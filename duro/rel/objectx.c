/*
 * RDB_object functions that involve storage
 *
 * Copyright (C) 2013-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "stable.h"
#include "qresult.h"
#include "delete.h"
#include "insert.h"
#include <gen/hashtabit.h>
#include <gen/strfns.h>
#include <obj/key.h>
#include <obj/objinternal.h>

#include <stdlib.h>
#include <stddef.h>
#include <string.h>

int
RDB_obj_ilen(const RDB_object *objp, size_t *lenp, RDB_exec_context *ecp)
{
    int ret;
    size_t len;
    RDB_type *impltyp = objp->store_typ;

    if (RDB_type_is_dummy(objp->store_typ)) {
        /* Add space for the type name size and the type name */
        impltyp = RDB_obj_impl_type(objp);
        *lenp = sizeof (size_t);
        *lenp += strlen(impltyp->name) + 1;
    } else {
        *lenp = 0;
    }

    switch(objp->kind) {
    case RDB_OB_TUPLE:
    {
        int i;
        RDB_type *tpltyp;

        if (RDB_type_is_dummy(objp->store_typ)) {
            tpltyp = RDB_type_is_dummy(objp->typ) ? objp->impl_typ : objp->typ;
        } else {
            tpltyp = objp->store_typ;
        }

        if (RDB_type_is_scalar(tpltyp))
            tpltyp = tpltyp->def.scalar.arep;

        for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
            RDB_type *attrtyp = tpltyp->def.tuple.attrv[i].typ;
            RDB_object *attrobjp = RDB_tuple_get(objp,
                    tpltyp->def.tuple.attrv[i].name);
            if (attrobjp == NULL) {
                RDB_raise_type_mismatch("missing attribute value", ecp);
                return RDB_ERROR;
            }

            attrobjp->store_typ = attrtyp;
            if (attrtyp->ireplen == RDB_VARIABLE_LEN)
                *lenp += sizeof (size_t);
            ret = RDB_obj_ilen(attrobjp, &len, ecp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            *lenp += len;
        }

        return RDB_OK;
    }
    case RDB_OB_TABLE:
    {
        size_t len;

        if (RDB_table_ilen(objp, &len, ecp) != RDB_OK)
            return RDB_ERROR;
        *lenp += len;
        return RDB_OK;
    }
    case RDB_OB_ARRAY:
    {
        RDB_type *arrtyp;
        RDB_object *elemp;
        int i = 0;

        if (RDB_type_is_dummy(objp->store_typ)) {
            arrtyp = RDB_type_is_dummy(objp->typ) ? objp->impl_typ : objp->typ;
        } else {
            arrtyp = objp->store_typ;
        }

        if (RDB_type_is_scalar(arrtyp))
            arrtyp = arrtyp->def.scalar.arep;

        while ((elemp = RDB_array_get((RDB_object *)objp, (RDB_int) i++,
                ecp)) != NULL) {
            elemp->store_typ = arrtyp->def.basetyp;
            if (elemp->store_typ->ireplen == RDB_VARIABLE_LEN)
                *lenp += sizeof (size_t);
            ret = RDB_obj_ilen(elemp, &len, ecp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            *lenp += len;
        }
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
            return RDB_ERROR;
        RDB_clear_err(ecp);
        return RDB_OK;
    }
    default: ;
    }
    if (impltyp->ireplen == RDB_VARIABLE_LEN)
        *lenp += objp->val.bin.len;
    else
        *lenp += impltyp->ireplen;
    return RDB_OK;
}

enum RDB_obj_kind
RDB_val_kind(const RDB_type *typ)
{
    switch (typ->kind) {
    case RDB_TP_SCALAR:
        if (typ == &RDB_BOOLEAN)
            return RDB_OB_BOOL;
        if (typ == &RDB_INTEGER)
            return RDB_OB_INT;
        if (typ == &RDB_FLOAT)
            return RDB_OB_FLOAT;
        if (typ == &RDB_DATETIME)
            return RDB_OB_TIME;
        if (typ->def.scalar.arep != NULL)
            return RDB_val_kind(typ->def.scalar.arep);
        return RDB_OB_BIN;
    case RDB_TP_TUPLE:
        return RDB_OB_TUPLE;
    case RDB_TP_RELATION:
        return RDB_OB_TABLE;
    case RDB_TP_ARRAY:
        return RDB_OB_ARRAY;
    }
    /* Should never be reached */
    abort();
}

static int
len_irep_to_obj(RDB_object *valp, RDB_type *typ, const void *datap,
        RDB_exec_context *ecp)
{
    int ret;
    size_t len;
    size_t llen = 0;
    RDB_byte *bp = (RDB_byte *) datap;

    if (typ->ireplen == RDB_VARIABLE_LEN) {
        memcpy(&len, bp, sizeof len);
        llen = sizeof (size_t);
        bp += sizeof (size_t);
    } else {
        len = (size_t) typ->ireplen;
    }

    ret = RDB_irep_to_obj(valp, typ, bp, len, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    return llen + len;
}

static int
irep_to_tuple(RDB_object *tplp, RDB_type *typ, const void *datap,
        RDB_exec_context *ecp)
{
    int i;
    int ret;
    size_t len = 0;
    const RDB_byte *bp = (RDB_byte *) datap;
    char *lastname = NULL;

    if (RDB_type_is_scalar(typ))
        typ = typ->def.scalar.arep;

    /*
     * Read attribute in the alphabetical order of their names
     */
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        RDB_object obj;
        size_t l;
        int attridx = RDB_next_attr_sorted(typ, lastname);

        RDB_init_obj(&obj);
        l = len_irep_to_obj(&obj, typ->def.tuple.attrv[attridx].typ, bp, ecp);
        if (l < 0) {
            RDB_destroy_obj(&obj, ecp);
            return l;
        }
        bp += l;
        len += l;
        ret = RDB_tuple_set(tplp, typ->def.tuple.attrv[attridx].name, &obj, ecp);
        RDB_destroy_obj(&obj, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        lastname =  typ->def.tuple.attrv[attridx].name;
    }
    return len;
}

static int
irep_to_table(RDB_object *tbp, RDB_type *typ, const void *datap, size_t len,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_object tpl;
    RDB_byte *bp = (RDB_byte *)datap;
    RDB_type *tbtyp;

    if (RDB_type_is_scalar(typ))
        typ = typ->def.scalar.arep;

    tbtyp = RDB_dup_nonscalar_type(typ, ecp);
    if (tbtyp == NULL) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        return RDB_ERROR;
    }
    if (RDB_init_table_i(tbp, NULL, RDB_FALSE,
            tbtyp, 0, NULL, 0, NULL, RDB_FALSE, NULL, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    while (len > 0) {
        int l;

        l = irep_to_tuple(&tpl, typ->def.basetyp, bp, ecp);
        if (l < 0) {
            RDB_destroy_obj(&tpl, ecp);
            return l;
        }
        ret = RDB_insert_real(tbp, &tpl, ecp, NULL);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        bp += l;
        len -= l;
    }
    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;
}

static int
irep_to_array(RDB_object *arrp, RDB_type *typ, const void *datap, size_t len,
        RDB_exec_context *ecp)
{
    int ret;
    int i;
    RDB_object tpl;
    int arrlen = 0;
    RDB_byte *bp = (RDB_byte *) datap;

    if (RDB_type_is_scalar(typ))
        typ = typ->def.scalar.arep;

    RDB_init_obj(&tpl);

    /* Determine array size */
    while (len > 0) {
        int l;

        l = len_irep_to_obj(&tpl, typ->def.basetyp, bp, ecp);
        if (l < 0) {
            RDB_destroy_obj(&tpl, ecp);
            return l;
        }
        bp += l;
        len -= l;
        arrlen++;
    }

    ret = RDB_set_array_length(arrp, (RDB_int) arrlen, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    bp = (RDB_byte *)datap;
    for (i = 0; i < arrlen; i++) {
        int l = len_irep_to_obj(&tpl, typ->def.basetyp, bp, ecp);

        ret = RDB_array_set(arrp, i, &tpl, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        bp += l;
    }

    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;
}

static void *
obj_to_irep(void *dstp, const void *srcp, size_t len)
{
    RDB_obj_to_irep(dstp, (const RDB_object *) srcp, len);
    return dstp;
}

static void *
obj_to_len_irep(void *dstp, const RDB_object *objp, RDB_type *typ,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_byte *bp = dstp;
    size_t len = typ->ireplen;

    if (len == RDB_VARIABLE_LEN) {
        ret = RDB_obj_ilen(objp, &len, ecp);
        if (ret != RDB_OK)
            return NULL;
        memcpy(bp, &len, sizeof (size_t));
        bp += sizeof (size_t);
    }
    obj_to_irep(bp, objp, len);
    bp += len;

    return bp;
}

static void
table_to_irep(void *dstp, RDB_object *tbp, size_t len)
{
    RDB_exec_context ec;
    RDB_object tpl;
    RDB_qresult *qrp;
    int ret;
    size_t l;
    RDB_byte *bp = dstp;

    RDB_init_exec_context(&ec);

    qrp = RDB_table_qresult(tbp, &ec, NULL);
    if (qrp == NULL) {
        RDB_destroy_exec_context(&ec);
        return;
    }

    RDB_init_obj(&tpl);

    while ((ret = RDB_next_tuple(qrp, &tpl, &ec, NULL)) == RDB_OK) {
        tpl.store_typ = RDB_type_is_scalar(tbp->typ) ?
                RDB_obj_impl_type(tbp)->def.scalar.arep->def.basetyp
                : tbp->typ->def.basetyp;
        if (RDB_obj_ilen(&tpl, &l, &ec) != RDB_OK) {
            RDB_destroy_exec_context(&ec);
            return;
        }
        obj_to_irep(bp, &tpl, l);
        bp += l;
    }
    RDB_destroy_obj(&tpl, &ec);
    if (RDB_obj_type(RDB_get_err(&ec)) != &RDB_NOT_FOUND_ERROR) {
        RDB_del_qresult(qrp, &ec, NULL);
        RDB_destroy_exec_context(&ec);
        return;
    }
    RDB_del_qresult(qrp, &ec, NULL);
    RDB_destroy_exec_context(&ec);
}

/*
 * Copy the internal representation of *objp to *dstp
 */
void
RDB_obj_to_irep(void *dstp, const RDB_object *objp, size_t len)
{
    RDB_exec_context ec;
    const void *srcp;
    RDB_type *impltyp;
    RDB_byte *bp = dstp;

    if (RDB_type_is_dummy(objp->store_typ)) {
        size_t typenamsz;
        impltyp = RDB_type_is_dummy(objp->typ) ?
                objp->impl_typ : objp->typ;
        typenamsz = strlen(impltyp->name) + 1;
        memcpy(bp, &typenamsz, sizeof(size_t));
        memcpy(bp + sizeof(size_t), impltyp->name, typenamsz);
        len -= sizeof(size_t) + typenamsz;
        bp += sizeof(size_t) + typenamsz;
    } else {
        impltyp = objp->store_typ;
    }

    switch (objp->kind) {
    /*
     * Tuples, tables, and arrays need special treatment
     * because the binary internal rep is not immediately available
     */
    case RDB_OB_TUPLE:
    {
        int i;
        char *lastwritten = NULL;

        RDB_init_exec_context(&ec);

        /* If the type is scalar, use internal rep */
        if (impltyp->kind == RDB_TP_SCALAR)
            impltyp = impltyp->def.scalar.arep;

        /*
         * Write attributes in alphabetical order of their names,
         * so the order corresponds with serializes tuple types.
         * See RDB_serialize_type().
         */
        for (i = 0; i < impltyp->def.tuple.attrc; i++) {
            RDB_object *attrp;
            int attridx = RDB_next_attr_sorted(impltyp, lastwritten);

            attrp = RDB_tuple_get(objp, impltyp->def.tuple.attrv[attridx].name);
            bp = obj_to_len_irep(bp, attrp, impltyp->def.tuple.attrv[attridx].typ,
                    &ec);
            lastwritten = impltyp->def.tuple.attrv[attridx].name;
        }
        RDB_destroy_exec_context(&ec);
        break;
    }
    case RDB_OB_TABLE:
        table_to_irep(bp, (RDB_object *) objp, len);
        break;
    case RDB_OB_ARRAY:
    {
        RDB_object *elemp;
        int i = 0;

        RDB_init_exec_context(&ec);

        if (impltyp->kind == RDB_TP_SCALAR)
            impltyp = impltyp->def.scalar.arep;

        while ((elemp = RDB_array_get((RDB_object *) objp, (RDB_int) i++,
                &ec)) != NULL) {
            elemp->typ = impltyp->def.basetyp;
            bp = obj_to_len_irep(bp, elemp, elemp->typ, &ec);
        }
        if (RDB_obj_type(RDB_get_err(&ec)) != &RDB_NOT_FOUND_ERROR) {
            return;
        }
        RDB_destroy_exec_context(&ec);
        break;
    }
    default:
        srcp = RDB_obj_irep((RDB_object *)objp, NULL);
        memcpy(bp, srcp, len);
    }
}

int
RDB_obj_to_field(RDB_field *fvp, RDB_object *objp, RDB_exec_context *ecp)
{
    /* Global tables cannot be converted to field values */
    if (objp->kind == RDB_OB_TABLE && RDB_table_is_persistent(objp)) {
        RDB_raise_invalid_argument(
                "global table cannot be used as field value", ecp);
        return RDB_ERROR;
    }

    fvp->datap = objp;
    fvp->copyfp = obj_to_irep;
    return RDB_obj_ilen(objp, &fvp->len, ecp);
}

/** @addtogroup type
 * @{
 */

/** @addtogroup typeimpl
 * @{
 */

/**
 * RDB_obj_irep returns a pointer to the binary internal representation of
the variable specified by <var>valp</var>.
If lenp is not NULL, the size of the internal representation
is stored at the location pointed to by <var>lenp</var>.

RDB_obj_irep only works for types with a binary internal representation.
These are built-in scalar types and user-defined types
which use a built-in scalar type or a byte array as physical representation.

@returns

A pointer to the internal representation.
 */
void *
RDB_obj_irep(RDB_object *valp, size_t *lenp)
{
    if (lenp != NULL) {
        *lenp = valp->typ->ireplen;
        if (*lenp == RDB_VARIABLE_LEN)
            *lenp = valp->val.bin.len;
    }

    switch (valp->kind) {
    case RDB_OB_BOOL:
        return &valp->val.bool_val;
    case RDB_OB_INT:
        return &valp->val.int_val;
    case RDB_OB_FLOAT:
        return &valp->val.float_val;
    case RDB_OB_TIME:
        return &valp->val.time;
    default:
        return valp->val.bin.datap;
    }
}

/**
 * Initialize the value pointed to by valp with the internal
 * representation given by <var>datap<var> and <var>len<var>.
 *
 * @arg len The length of the internal represenation in bytes.
 * @arg datap   A pointer to the internal representation.
 * If datap is NULL, len bytes are allocated but the internal representation
 * is undefined.
 *
 * @returns
 *
 * RDB_OK on success, RDB_ERROR on failure.
 */
int
RDB_irep_to_obj(RDB_object *valp, RDB_type *typ, const void *datap, size_t len,
        RDB_exec_context *ecp)
{
    int ret;
    enum RDB_obj_kind kind;

    if (valp->kind != RDB_OB_INITIAL) {
        if (RDB_destroy_obj(valp, ecp) != RDB_OK)
            return RDB_ERROR;
        RDB_init_obj(valp);
    }

    /*
     * No type information for non-scalar types
     * (except tables, in this case irep_to_table() will set the type)
     */
    kind = RDB_val_kind(typ);
    if (RDB_type_is_scalar(typ)) {
        valp->typ = typ;
        if (RDB_type_is_dummy(typ)) {
            size_t impltypsz;
            char *tnamp;

            memcpy(&impltypsz, datap, sizeof(size_t));
            tnamp = (char*) datap + sizeof(size_t);

            /* Check for terminating nullbyte */
            if (tnamp[impltypsz - 1] != '\0') {
                RDB_raise_data_corrupted("invalid type name", ecp);
                return RDB_ERROR;
            }
            valp->impl_typ = RDB_get_subtype(typ, tnamp);
            if (valp->impl_typ == NULL) {
                RDB_raise_type_not_found(tnamp, ecp);
                return RDB_ERROR;
            }
            datap = tnamp + impltypsz;
            len -= sizeof(size_t) + impltypsz;
            kind = RDB_val_kind(valp->impl_typ);
        }
    } else {
        valp->typ = NULL;
    }

    switch (kind) {
    case RDB_OB_INITIAL:
    case RDB_OB_BOOL:
        valp->val.bool_val = *(RDB_bool *) datap;
        break;
    case RDB_OB_INT:
        memcpy(&valp->val.int_val, datap, sizeof (RDB_int));
        break;
    case RDB_OB_FLOAT:
        memcpy(&valp->val.float_val, datap, sizeof (RDB_float));
        break;
    case RDB_OB_TIME:
        memcpy(&valp->val.time, datap, sizeof (RDB_time));
        break;
    case RDB_OB_BIN:
        valp->val.bin.len = len;
        if (len > 0) {
            valp->val.bin.datap = RDB_alloc(len, ecp);
            if (valp->val.bin.datap == NULL) {
                return RDB_ERROR;
            }
            if (datap != NULL)
                memcpy(valp->val.bin.datap, datap, len);
        }
        break;
    case RDB_OB_TUPLE:
        ret = irep_to_tuple(valp, RDB_type_is_dummy(typ) ? valp->impl_typ : typ,
                datap, ecp);
        if (ret > 0)
            ret = RDB_OK;
        return ret;
    case RDB_OB_TABLE:
    {
        if (irep_to_table(valp, RDB_type_is_dummy(typ) ? valp->impl_typ : typ,
                datap, len, ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_type_is_scalar(typ)) {
            if (valp->typ != NULL && !RDB_type_is_scalar(valp->typ)) {
                if (RDB_del_nonscalar_type(valp->typ, ecp) != RDB_OK)
                    return RDB_ERROR;
            }
            valp->typ = typ;
        }
        return RDB_OK;
    }
    case RDB_OB_ARRAY:
        return irep_to_array(valp, RDB_type_is_dummy(typ) ? valp->impl_typ : typ,
                datap, len, ecp);
    }
    valp->kind = kind;
    return RDB_OK;
}

/*@}*/

/*@}*/

/**@addtogroup generic
 * @{
 */

/**
 * Check two RDB_object variables for equality
and store the result in the variable pointed to by <var>resp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.
 */
int
RDB_obj_equals(const RDB_object *val1p, const RDB_object *val2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_object retval;
    RDB_object *argv[2];

    argv[0] = (RDB_object *) val1p;
    argv[1] = (RDB_object *) val2p;
    RDB_init_obj(&retval);
    ret = RDB_call_ro_op_by_name("=", 2, argv, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return RDB_ERROR;
    }
    *resp = RDB_obj_bool(&retval);
    return RDB_destroy_obj(&retval, ecp);
}

/**
 * Copy the value of property <var>propname</var>
 * of *<var>objp</var> *<var>propvalp</var>.
 *
 * There must be type information associated with *<var>objp</var>.

 * If <var>txp</var> is NULL and <var>envp</var> is not, <var>envp</var> is used
 * to look up the getter operator from memory.
 * If <var>txp</var> is not NULL, <var>envp</var> is ignored.

 * *<var>propvalp</var> will carry type information. If the property type is non-scalar and
 * not a relation type, it is managed by the type of *<var>objp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>invalid_argument_error
<dd>The type of *<var>valp</var> is not scalar, or it does not
have a property <var>propname</var>.
<dt>operator_not_found_error
<dd>The getter method for property <var>propname</var> has not been created.
</dl>

RDB_obj_property may also raise an error raised by a
user-provided getter function.

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_obj_property(const RDB_object *objp, const char *propname, RDB_object *propvalp,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_type *objtyp = objp->typ;
    if (RDB_type_is_dummy(objtyp))
        objtyp = objp->impl_typ;

    if (!RDB_type_is_scalar(objtyp) || objtyp->def.scalar.repc == 0) {
        RDB_raise_invalid_argument("property not found", ecp);
        return RDB_ERROR;
    }

    if (objtyp->def.scalar.sysimpl) {
        if (objtyp->def.scalar.repv[0].compc == 1) {
            RDB_type *comptyp;

            /* Actual rep is type of the only component - check component name */
            if (strcmp(propname, objtyp->def.scalar.repv[0].compv[0].name)
                    != 0) {
                RDB_raise_invalid_argument("component not found", ecp);
                return RDB_ERROR;
            }
            comptyp = objtyp->def.scalar.repv[0].compv[0].typ;

            /* If *propvalp carries a value, it must match the type */
            if (propvalp->kind != RDB_OB_INITIAL
                    && (propvalp->typ == NULL
                            || !RDB_type_equals(propvalp->typ, comptyp))) {
                RDB_raise_type_mismatch("invalid component type", ecp);
                return RDB_ERROR;
            }
            if (RDB_copy_obj_data(propvalp, objp, ecp, NULL) != RDB_OK)
                return RDB_ERROR;
            if (propvalp->typ != NULL && !RDB_type_is_scalar(propvalp->typ)) {
                if (RDB_del_nonscalar_type(propvalp->typ, ecp) != RDB_OK)
                    return RDB_ERROR;
            }
            if (RDB_type_is_relation(comptyp)) {
                propvalp->typ = RDB_dup_nonscalar_type(comptyp, ecp);
                if (propvalp->typ == NULL)
                    return RDB_ERROR;
            } else {
                propvalp->typ = comptyp;
            }
            ret = RDB_OK;
        } else {
            /* Actual rep is tuple */
            RDB_object *elemp = RDB_tuple_get(objp, propname);
            if (elemp == NULL) {
                RDB_raise_invalid_argument("component not found", ecp);
                return RDB_ERROR;
            }
            ret = RDB_copy_obj(propvalp, elemp, ecp);
        }
    } else {
        /* Getter is implemented by user */
        char *opname;
        RDB_object *argv[1];

        opname = RDB_alloc(strlen(objtyp->name) + strlen(propname)
                + strlen(RDB_GETTER_INFIX) + 1, ecp);
        if (opname == NULL) {
            return RDB_ERROR;
        }

        strcpy(opname, objtyp->name);
        strcat(opname, RDB_GETTER_INFIX);
        strcat(opname, propname);
        argv[0] = (RDB_object *) objp;

        ret = RDB_call_ro_op_by_name_e(opname, 1, argv, envp, ecp, txp, propvalp);
        RDB_free(opname);
    }
    return ret;
}

/**
 * Set the the value of property <var>propname</var>
of the RDB_object specified to by <var>objp</var>
to the value of *<var>propvalp</var>.

The RDB_object must be of a type which has a property
<var>propname</var>.

If <var>txp</var> is NULL and <var>envp</var> is not, <var>envp</var> is used
to look up the getter operator from memory.
If <var>txp</var> is not NULL, <var>envp</var> is ignored.

If an error occurs, an error value is left in *<var>ecp</var>.

If the call fails with a type_constraint_violation_error, the value of *dst
may be replaced by the init value of the type of *dst.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.
 */
int
RDB_obj_set_property(RDB_object *objp, const char *propname,
        const RDB_object *propvalp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (objp->typ->def.scalar.sysimpl) {
        /* Setter is implemented by the system */
        RDB_attr *compp;

        if (propvalp->typ == NULL) {
            RDB_raise_invalid_argument("missing type information", ecp);
            return RDB_ERROR;
        }

        compp = RDB_prop_attr(objp->typ, propname);
        if (compp == NULL) {
            RDB_raise_invalid_argument("invalid property", ecp);
            return RDB_ERROR;
        }

        if (!RDB_type_equals(propvalp->typ, compp->typ)) {
            RDB_raise_type_mismatch("source type does not match destination property type", ecp);
            return RDB_ERROR;
        }

        if (objp->typ->def.scalar.repv[0].compc == 1) {
            ret = RDB_copy_obj_data(objp, propvalp, ecp, NULL);
        } else {
            ret = RDB_tuple_set(objp, propname, propvalp, ecp);
        }
    } else {
        /* Setter is implemented by user */
        char *opname;
        RDB_object *argv[2];
        RDB_type *argtv[2];
        RDB_operator *op;

        opname = RDB_alloc(strlen(objp->typ->name) + strlen(propname)
                + strlen(RDB_SETTER_INFIX) + 1, ecp);
        if (opname == NULL) {
            return RDB_ERROR;
        }

        strcpy(opname, objp->typ->name);
        strcat(opname, RDB_SETTER_INFIX);
        strcat(opname, propname);
        argv[0] = objp;
        argv[1] = (RDB_object *) propvalp;
        argtv[0] = RDB_obj_type(argv[0]);
        argtv[1] = RDB_obj_type(argv[1]);

        op = RDB_get_update_op(opname, 2, argtv, envp, ecp, txp);
        RDB_free(opname);
        if (op == NULL)
            return RDB_ERROR;

        ret = RDB_call_update_op(op, 2, argv, ecp, txp);
    }

    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = RDB_check_type_constraint(objp, envp, ecp, txp);
    if (ret != RDB_OK) {
        /* Replace illegal value by init value */
        if (!objp->typ->def.scalar.init_val_is_valid) {
            RDB_raise_internal("missing init value", ecp);
            return RDB_ERROR;
        }
        RDB_copy_obj_data(objp, &objp->typ->def.scalar.init_val, ecp, NULL);
    }
    return ret;
}

/**
 * Set *<var>objp</var> to the initial value of type *<var>typ</var>.
 *
 * A user-defined type may have no INIT value if it has only one possrep.
 * In this case, the selector is called, and <var>txp</var> or,
 * if <var>txp</var> is NULL, <var>envp</var> is used to look up the selector.
 */
int
RDB_set_init_value(RDB_object *objp, RDB_type *typ, RDB_environment *envp,
        RDB_exec_context *ecp)
{
    switch (typ->kind) {
    case RDB_TP_RELATION:
        return RDB_init_table_from_type(objp, NULL, typ, 0, NULL,
                0, NULL, ecp);
    case RDB_TP_TUPLE:
    {
        RDB_type *attrtyp;
        int i;

        for (i = 0; i < typ->def.tuple.attrc; i++) {
            if (RDB_tuple_set(objp, typ->def.tuple.attrv[i].name,
                    NULL, ecp) != RDB_OK)
                return RDB_ERROR;
            attrtyp = typ->def.tuple.attrv[i].typ;

            if (RDB_type_is_relation(attrtyp)) {
                /*
                 * Duplicate the type, because RDB_init_table_from_type()
                 * will consume it
                 */
                attrtyp = RDB_dup_nonscalar_type(attrtyp, ecp);
                if (attrtyp == NULL)
                    return RDB_ERROR;
            }
            if (RDB_set_init_value(RDB_tuple_get(objp, typ->def.tuple.attrv[i].name),
                    attrtyp, envp, ecp) != RDB_OK) {
                if (RDB_type_is_relation(attrtyp))
                    RDB_del_nonscalar_type(typ, ecp);
                return RDB_ERROR;
            }
        }
        RDB_obj_set_typeinfo(objp, typ);
        break;
    }
    case RDB_TP_ARRAY:
        RDB_obj_set_typeinfo(objp, typ);
        if (RDB_set_array_length(objp, (RDB_int) 0, ecp) != RDB_OK)
            return RDB_ERROR;
        break;
    case RDB_TP_SCALAR:
        objp->typ = typ;

        if (typ->ireplen == RDB_NOT_IMPLEMENTED) {
            RDB_raise_invalid_argument("type is not implemented", ecp);
            return RDB_ERROR;
        }

        if (!typ->def.scalar.init_val_is_valid) {
            RDB_raise_internal("missing init value", ecp);
            return RDB_ERROR;
        }
        return RDB_copy_obj_data(objp, &typ->def.scalar.init_val, ecp, NULL);
    }
    return RDB_OK;
}

/**
 * Copy RDB_object, but not the type information, except for tables.
 * If the RDB_object wraps a table, it must be a real table.
 * No type checking takes place.
 */
int
RDB_copy_obj_data(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_int rc;
    void *datap;

    if (dstvalp->kind != RDB_OB_INITIAL && srcvalp->kind != dstvalp->kind) {
        ret = RDB_destroy_obj(dstvalp, ecp);
        RDB_init_obj(dstvalp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    switch (srcvalp->kind) {
    case RDB_OB_INITIAL:
        break;
    case RDB_OB_BOOL:
        dstvalp->kind = srcvalp->kind;
        dstvalp->val.bool_val = srcvalp->val.bool_val;
        break;
    case RDB_OB_INT:
        dstvalp->kind = srcvalp->kind;
        dstvalp->val.int_val = srcvalp->val.int_val;
        break;
    case RDB_OB_FLOAT:
        dstvalp->kind = srcvalp->kind;
        dstvalp->val.float_val = srcvalp->val.float_val;
        break;
    case RDB_OB_TIME:
        dstvalp->kind = srcvalp->kind;
        dstvalp->val.time = srcvalp->val.time;
        break;
    case RDB_OB_TUPLE:
        return RDB_copy_tuple(dstvalp, srcvalp, ecp);
    case RDB_OB_ARRAY:
        return RDB_copy_array(dstvalp, srcvalp, ecp);
    case RDB_OB_TABLE:
        if (dstvalp->kind == RDB_OB_INITIAL) {
            RDB_type *reltyp;

            /* Turn the RDB_object into a table */
            if (srcvalp->typ->kind == RDB_TP_RELATION) {
                reltyp = RDB_new_relation_type(
                        srcvalp->typ->def.basetyp->def.tuple.attrc,
                        srcvalp->typ->def.basetyp->def.tuple.attrv, ecp);
            } else {
                /* Type is scalar with relation type as actual rep */
                reltyp = RDB_dup_nonscalar_type(
                        RDB_obj_impl_type(srcvalp)->def.scalar.arep, ecp);
            }
            if (reltyp == NULL)
                return RDB_ERROR;

            ret = RDB_init_table_i(dstvalp, NULL, RDB_FALSE,
                    reltyp, 1, srcvalp->val.tb.keyv, 0, NULL, RDB_FALSE,
                    NULL, ecp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else {
            if (dstvalp->kind != RDB_OB_TABLE) {
                RDB_raise_type_mismatch("destination must be table", ecp);
                return RDB_ERROR;
            }
            /* Delete all tuples */
            ret = RDB_delete_real(dstvalp, NULL, NULL, NULL, ecp,
                    RDB_table_is_persistent(dstvalp) ? txp : NULL);
            if (ret == (RDB_int) RDB_ERROR)
                return RDB_ERROR;
        }
        rc = RDB_move_tuples(dstvalp, (RDB_object *) srcvalp, RDB_DISTINCT, ecp,
                RDB_table_is_persistent(srcvalp) || srcvalp->val.tb.exp != NULL
                || RDB_table_is_persistent(dstvalp) ? txp : NULL);
        if (rc == RDB_ERROR)
            return RDB_ERROR;
        return RDB_OK;
    case RDB_OB_BIN:
        if (dstvalp->kind == RDB_OB_BIN) {
            if (dstvalp->val.bin.len > 0) {
                if (srcvalp->val.bin.len > 0) {
                    datap = RDB_realloc(dstvalp->val.bin.datap,
                            srcvalp->val.bin.len, ecp);
                    if (datap == NULL)
                        return RDB_ERROR;
                    dstvalp->val.bin.datap = datap;
                    dstvalp->val.bin.len = srcvalp->val.bin.len;
                } else {
                    RDB_free(dstvalp->val.bin.datap);
                    dstvalp->val.bin.datap = NULL;
                    dstvalp->val.bin.len = 0;
                }
            } else {
                datap = RDB_alloc(srcvalp->val.bin.len, ecp);
                if (datap == NULL)
                    return RDB_ERROR;
                dstvalp->val.bin.datap = datap;
                dstvalp->val.bin.len = srcvalp->val.bin.len;
            }
        } else {
            if (srcvalp->val.bin.len == 0) {
                datap = NULL;
            } else {
                datap = RDB_alloc(srcvalp->val.bin.len, ecp);
                if (datap == NULL)
                    return RDB_ERROR;
            }
            dstvalp->kind = RDB_OB_BIN;
            dstvalp->val.bin.datap = datap;
            dstvalp->val.bin.len = srcvalp->val.bin.len;
        }
        if (dstvalp->val.bin.datap != NULL) {
            memcpy(dstvalp->val.bin.datap, srcvalp->val.bin.datap,
                    srcvalp->val.bin.len);
        }
        break;
    }
    return RDB_OK;
}

/*@}*/

/*
 * If *objp is non-scalar, create a RDB_type structure for the its type,
 * otherwise return the type of *objp.
 */
RDB_type *
RDB_new_nonscalar_obj_type(RDB_object *objp, RDB_exec_context *ecp)
{
    RDB_type *typ;
    RDB_type *elemtyp;
    RDB_object *elemp;

    switch (objp->kind) {
    case RDB_OB_TUPLE:
        return RDB_tuple_type(objp, ecp);
    case RDB_OB_ARRAY:
        if (RDB_array_length(objp, ecp) == 0) {
            RDB_raise_invalid_argument("Cannot get type of empty array", ecp);
            return NULL;
        }

        /*
         * Get first array element and create type from it
         */
        elemp = RDB_array_get(objp, (RDB_int) 0, ecp);
        if (elemp == NULL)
            return NULL;
        elemtyp = RDB_new_nonscalar_obj_type(elemp, ecp);
        if (elemtyp == NULL)
            return NULL;
        typ = RDB_new_array_type(elemtyp, ecp);
        if (typ == NULL) {
            RDB_del_nonscalar_type(elemtyp, ecp);
            return NULL;
        }
        return typ;
    default:
        typ = RDB_obj_type(objp);
        if (typ == NULL) {
            RDB_raise_internal("type of tuple attribute not found", ecp);
            return NULL;
        }
        return RDB_dup_nonscalar_type(typ, ecp);
    }
}
