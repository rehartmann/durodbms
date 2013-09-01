/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "key.h"
#include "stable.h"
#include "qresult.h"
#include "delete.h"
#include "insert.h"
#include <gen/hashtabit.h>
#include <gen/strfns.h>

#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <assert.h>

void *
RDB_alloc(size_t size, RDB_exec_context *ecp)
{
    void *p = malloc(size);
    if (p == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    return p;
}

void *
RDB_realloc(void *p, size_t size, RDB_exec_context *ecp)
{
    p = realloc(p, size);
    if (p == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    return p;
}

void
RDB_free(void *p)
{
    free(p);
}

RDB_object *
RDB_new_obj(RDB_exec_context *ecp)
{
    RDB_object *objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return NULL;
    }
    RDB_init_obj(objp);
    return objp;
}

int
RDB_free_obj(RDB_object *objp, RDB_exec_context *ecp)
{
    int ret = RDB_destroy_obj(objp, ecp);
    RDB_free(objp);
    return ret;
}

int
RDB_obj_ilen(const RDB_object *objp, size_t *lenp, RDB_exec_context *ecp)
{
    int ret;
    size_t len;

    *lenp = 0;

    switch(objp->kind) {
        case RDB_OB_TUPLE:
        {
            int i;
            RDB_type *tpltyp = objp->store_typ;

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
            return RDB_table_ilen(objp, lenp, ecp);
        case RDB_OB_ARRAY:
        {
            RDB_object *elemp;
            int i = 0;

            while ((elemp = RDB_array_get((RDB_object *)objp, (RDB_int) i++,
                    ecp)) != NULL) {
                elemp->store_typ = objp->store_typ->def.basetyp;
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
    *lenp = objp->store_typ->ireplen;
    if (*lenp == RDB_VARIABLE_LEN)
        *lenp = objp->val.bin.len;
    return RDB_OK;
}

static enum RDB_obj_kind
val_kind(const RDB_type *typ)
{
    switch (typ->kind) {
        case RDB_TP_SCALAR:
            if (typ == &RDB_BOOLEAN)
                return RDB_OB_BOOL;
            if (typ == &RDB_INTEGER)
                return RDB_OB_INT;
            if (typ == &RDB_FLOAT)
                return RDB_OB_FLOAT;
            if (typ->def.scalar.arep != NULL)
                return val_kind(typ->def.scalar.arep);
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
    RDB_byte *bp = (RDB_byte *)datap;

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
    RDB_byte *bp = (RDB_byte *)datap;

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
    if (RDB_type_is_scalar(typ))
        valp->typ = typ;
    else
        valp->typ = NULL;
    kind = val_kind(typ);

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
            ret = irep_to_tuple(valp, typ, datap, ecp);
            if (ret > 0)
                ret = RDB_OK;
            return ret;
        case RDB_OB_TABLE:
        {
            if (irep_to_table(valp, typ, datap, len, ecp) != RDB_OK)
                return RDB_ERROR;
            if (RDB_type_is_scalar(typ))
                valp->typ = typ;
            return RDB_OK;
        }
        case RDB_OB_ARRAY:
            return irep_to_array(valp, typ, datap, len, ecp);
    }
    valp->kind = kind;
    return RDB_OK;
}

/*@}*/

typedef struct {
    RDB_byte *bp;
    size_t len;
} irep_info;

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
        tpl.store_typ = RDB_type_is_scalar(tbp->store_typ) ?
                tbp->store_typ->def.scalar.arep->def.basetyp
                : tbp->store_typ->def.basetyp;
        ret = RDB_obj_ilen(&tpl, &l, &ec);
        if (ret != RDB_OK) {
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
    RDB_byte *bp = dstp;

    switch (objp->kind) {
        /*
         * Tuples, tables, and arrays need special treatment
         * because the binary internal rep is not immediately available
         */
        case RDB_OB_TUPLE:
        {
            RDB_type *tpltyp = objp->store_typ;
            int i;
            char *lastwritten = NULL;

            RDB_init_exec_context(&ec);

            /* If the type is scalar, use internal rep */
            if (tpltyp->kind == RDB_TP_SCALAR)
                tpltyp = tpltyp->def.scalar.arep;

            /*
             * Write attributes in alphabetical order of their names,
             * so the order corresponds with serializes tuple types.
             * See RDB_serialize_type().
             */
            for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
                RDB_object *attrp;
                int attridx = RDB_next_attr_sorted(tpltyp, lastwritten);

                attrp = RDB_tuple_get(objp, tpltyp->def.tuple.attrv[attridx].name);
                bp = obj_to_len_irep(bp, attrp, tpltyp->def.tuple.attrv[attridx].typ,
                        &ec);
                lastwritten = tpltyp->def.tuple.attrv[attridx].name;
            }
            RDB_destroy_exec_context(&ec);
            break;
        }
        case RDB_OB_TABLE:
            table_to_irep(dstp, (RDB_object *) objp, len);
            break;
        case RDB_OB_ARRAY:
        {
            RDB_object *elemp;
            int i = 0;

            RDB_init_exec_context(&ec);

            while ((elemp = RDB_array_get((RDB_object *) objp, (RDB_int) i++,
                    &ec)) != NULL) {
                elemp->typ = objp->store_typ->def.basetyp;
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

/**
 * Copy RDB_object, but not the type information.
 * If the RDB_object wraps a table, it must be a real table.
 */
int
RDB_copy_obj_data(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_int rc;

    if (dstvalp->kind != RDB_OB_INITIAL && srcvalp->kind != dstvalp->kind) {
        RDB_raise_type_mismatch("source type does not match destination type",
                ecp);
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
                            srcvalp->typ->def.scalar.arep, ecp);
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
                ret = RDB_delete_real(dstvalp, NULL, ecp,
                        RDB_table_is_persistent(dstvalp) ? txp : NULL);
                if (ret == (RDB_int) RDB_ERROR)
                    return RDB_ERROR;
            }
            rc = RDB_move_tuples(dstvalp, (RDB_object *) srcvalp, ecp,
                    RDB_table_is_persistent(srcvalp) || srcvalp->val.tb.exp != NULL
                    || RDB_table_is_persistent(dstvalp) ? txp : NULL);
            if (rc == RDB_ERROR)
                return RDB_ERROR;
            return RDB_OK;
        case RDB_OB_BIN:
            if (dstvalp->kind == RDB_OB_BIN) {
                if (dstvalp->val.bin.len > 0)
                    RDB_free(dstvalp->val.bin.datap);
            } else {
                dstvalp->kind = srcvalp->kind;
            }
            dstvalp->val.bin.len = srcvalp->val.bin.len;
            if (dstvalp->val.bin.len > 0) {
                dstvalp->val.bin.datap = RDB_alloc(srcvalp->val.bin.len, ecp);
                if (dstvalp->val.bin.datap == NULL) {
                    return RDB_ERROR;
                }
                memcpy(dstvalp->val.bin.datap, srcvalp->val.bin.datap,
                        srcvalp->val.bin.len);
            }
            break;
    }
    return RDB_OK;
}

/** @defgroup generic Scalar and generic functions 
 * @{
 */

/**
 * RDB_obj_equals checks two RDB_object variables for equality
and stores the result in the variable pointed to by <var>resp</var>.

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
 * RDB_init_obj initializes the RDB_object structure pointed to by
<var>valp</var>. RDB_init_obj must be called before any other
operation can be performed on a RDB_object variable.
 */
void
RDB_init_obj(RDB_object *valp)
{
    valp->kind = RDB_OB_INITIAL;
    valp->typ = NULL;
}

/**
 * RDB_destroy_obj releases all resources associated with a RDB_object
 * structure.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.
 */
int
RDB_destroy_obj(RDB_object *objp, RDB_exec_context *ecp)
{
    switch (objp->kind) {
        case RDB_OB_INITIAL:
        case RDB_OB_BOOL:
        case RDB_OB_INT:
        case RDB_OB_FLOAT:
            break;
        case RDB_OB_BIN:
            if (objp->val.bin.len > 0)
                RDB_free(objp->val.bin.datap);
            break;
        case RDB_OB_TUPLE:
        {
            RDB_hashtable_iter it;
            tuple_entry *entryp;

            RDB_init_hashtable_iter(&it, (RDB_hashtable *) &objp->val.tpl_tab);
            while ((entryp = RDB_hashtable_next(&it)) != NULL) {
                RDB_destroy_obj(&entryp->obj, ecp);
                RDB_free(entryp->key);
                RDB_free(entryp);
            }
            RDB_destroy_hashtable_iter(&it);
            RDB_destroy_hashtable(&objp->val.tpl_tab);
            break;
        }
        case RDB_OB_ARRAY:
        {
            int ret = RDB_OK;

            if (objp->val.arr.elemv != NULL) {
                int i;

                for (i = 0; i < objp->val.arr.elemc; i++)
                    RDB_destroy_obj(&objp->val.arr.elemv[i], ecp);
                RDB_free(objp->val.arr.elemv);
            }

            if (objp->val.arr.texp == NULL)
                return RDB_OK;

            if (objp->val.arr.qrp != NULL) {
                ret = RDB_del_qresult(objp->val.arr.qrp, ecp,
                        objp->val.arr.txp);
            }

            if (RDB_del_expr(objp->val.arr.texp, ecp) != RDB_OK)
                return RDB_ERROR;

            if (objp->val.arr.tplp != NULL) {
                if (RDB_destroy_obj(objp->val.arr.tplp, ecp) != RDB_OK)
                    return RDB_ERROR;
                RDB_free(objp->val.arr.tplp);
            }
            return ret;
        }
        case RDB_OB_TABLE:
            if (objp->val.tb.keyv != NULL) {
                RDB_free_keys(objp->val.tb.keyc, objp->val.tb.keyv);
            }

            /* It could be a scalar type with a relation actual rep */ 
            if (objp->typ != NULL && !RDB_type_is_scalar(objp->typ))
                RDB_del_nonscalar_type(objp->typ, ecp);
            
            RDB_free(objp->val.tb.name);
            
            if (objp->val.tb.exp != NULL) {
                if (RDB_del_expr(objp->val.tb.exp, ecp) != RDB_OK)
                    return RDB_ERROR;
            }

            /*
             * Delete recmap, if any
             */
            if (objp->val.tb.stp != NULL) {
                if (RDB_delete_stored_table(objp->val.tb.stp, ecp, NULL)
                        != RDB_OK)
                    return RDB_ERROR;
            }
            break;
    }
    return RDB_OK;
}

/**
 * RDB_bool_to_obj sets the RDB_object pointed to by <var>valp</var>
to the boolean value specified by <var>v</var>.

The RDB_object must either be newly initialized or of type
BOOLEAN.
 */
void
RDB_bool_to_obj(RDB_object *valp, RDB_bool v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_BOOLEAN);

    valp->typ = &RDB_BOOLEAN;
    valp->kind = RDB_OB_BOOL;
    valp->val.bool_val = v;
}

/**
 * RDB_int_to_obj sets the RDB_object pointed to by <var>valp</var>
to the integer value specified by <var>v</var>.

The RDB_object must either be newly initialized or of type
INTEGER.
 */
void
RDB_int_to_obj(RDB_object *valp, RDB_int v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_INTEGER);

    valp->typ = &RDB_INTEGER;
    valp->kind = RDB_OB_INT;
    valp->val.int_val = v;
}

/**
 * RDB_float_to_obj sets the RDB_object pointed to by <var>valp</var>
to the RDB_float value specified by <var>v</var>.

The RDB_object must either be newly initialized or of type
FLOAT.
 */
void
RDB_float_to_obj(RDB_object *valp, RDB_float v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_FLOAT);

    valp->typ = &RDB_FLOAT;
    valp->kind = RDB_OB_FLOAT;
    valp->val.float_val = v;
}

static int
set_str_obj_len(RDB_object *objp, size_t len, RDB_exec_context *ecp)
{
    void *datap;

    if (objp->kind != RDB_OB_INITIAL && objp->typ != &RDB_STRING) {
        RDB_raise_type_mismatch("not a string", ecp);
        return RDB_ERROR;
    }

    if (objp->kind == RDB_OB_INITIAL) {
        datap = RDB_alloc(len, ecp);
        if (datap == NULL) {
            return RDB_ERROR;
        }
        objp->typ = &RDB_STRING;
        objp->kind = RDB_OB_BIN;
    } else {
        datap = RDB_realloc(objp->val.bin.datap, len, ecp);
        if (datap == NULL) {
            return RDB_ERROR;
        }
    }
    objp->val.bin.len = len;
    objp->val.bin.datap = datap;
    return RDB_OK;
}

/**
 * RDB_string_to_obj sets the RDB_object pointed to by <var>valp</var>
to the string value specified by <var>str</var>.

The RDB_object must either be newly initialized or of type
string.
 */
int
RDB_string_to_obj(RDB_object *valp, const char *str, RDB_exec_context *ecp)
{
    if (set_str_obj_len(valp, strlen(str) + 1, ecp) != RDB_OK)
        return RDB_ERROR;

    strcpy(valp->val.bin.datap, str);
    return RDB_OK;
}

/**
 * Set *<var>valp</var> to the string that begins at <var>str</var>
 * limited to a length of <var>n</var> bytes.
<var>valp</var> must either be newly initialized or of type
string.
 */
int
RDB_string_n_to_obj(RDB_object *valp, const char *str, size_t n,
        RDB_exec_context *ecp)
{
    if (set_str_obj_len(valp, n + 1, ecp) != RDB_OK)
        return RDB_ERROR;

    strncpy(valp->val.bin.datap, str, n);
    ((char*) valp->val.bin.datap)[n] = '\0';
    return RDB_OK;
}

/**
 * Append the string <var>str</var> to *<var>objp</var>.

*<var>objp</var> must be of type string.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_append_string(RDB_object *objp, const char *str, RDB_exec_context *ecp)
{
    int len = objp->val.bin.len + strlen(str);
    char *nstr = RDB_realloc(objp->val.bin.datap, len, ecp);
    if (nstr == NULL) {
        return RDB_ERROR;
    }

    objp->val.bin.datap = nstr;
    strcpy(((char *)objp->val.bin.datap) + objp->val.bin.len - 1, str);
    objp->val.bin.len = len;
    return RDB_OK;
}

/**
 * Set *<var>objp</var> to the initial value of type *<var>typ</var>.
 *
 * If *<var>typ</var> is an array or tuple type, do nothing.
 *
 * A user-defined type may have no INIT value if it has only one possrep.
 * In this case, the selector is called, and <var>txp</var> or,
 * if <var>txp</var> is NULL, <var>envp</var> is used to look up the selector.
 */
int
RDB_set_init_value(RDB_object *objp, RDB_type *typ, RDB_environment *envp,
        RDB_exec_context *ecp)
{
    if (RDB_type_is_relation(typ)) {
        return RDB_init_table_from_type(objp, NULL, typ, 0, NULL,
                0, NULL, ecp);
    }

    if (RDB_type_is_scalar(typ)) {
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
RDB_obj_comp copies the value of component <var>compname</var>
of a possible representation of the variable pointed to by <var>valp</var>
to the variable pointed to by <var>comp</var>.

If <var>txp</var> is NULL and <var>envp</var> is not, <var>envp</var> is used
to look up the getter operator from memory.
If <var>txp</var> is not NULL, <var>envp</var> is ignored.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>invalid_argument_error
<dd>The type of *<var>valp</var> is not scalar, or it does not
have a possible representation with a component <var>compname</var>.
<dt>operator_not_found_error
<dd>The getter method for component <var>compname</var> has not been created.
</dl>

RDB_obj_comp may also raise an error raised by a
user-provided getter function.

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_obj_comp(const RDB_object *valp, const char *compname, RDB_object *compvalp,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (!RDB_type_is_scalar(valp->typ) || valp->typ->def.scalar.repc == 0) {
        RDB_raise_invalid_argument("component not found", ecp);
        return RDB_ERROR;
    }

    if (valp->typ->def.scalar.sysimpl) {
        if (valp->typ->def.scalar.repv[0].compc == 1) {
            RDB_type *comptyp;

            /* Actual rep is type of the only component - check component name */
            if (strcmp(compname, valp->typ->def.scalar.repv[0].compv[0].name)
                    != 0) {
                RDB_raise_invalid_argument("component not found", ecp);
                return RDB_ERROR;
            }            
            comptyp = valp->typ->def.scalar.repv[0].compv[0].typ;

            /* If *compvalp carries a value, it must match the type */
            if (compvalp->kind != RDB_OB_INITIAL
                     && (compvalp->typ == NULL
                         || !RDB_type_equals(compvalp->typ, comptyp))) {
                RDB_raise_type_mismatch("invalid component type", ecp);
                return RDB_ERROR;
            }
            ret = RDB_copy_obj_data(compvalp, valp, ecp, NULL);
            if (ret != RDB_OK)
                return RDB_ERROR;
            compvalp->typ = comptyp;
        } else {
            /* Actual rep is tuple */
            RDB_object *elemp = RDB_tuple_get(valp, compname);
            if (elemp == NULL) {
                RDB_raise_invalid_argument("component not found", ecp);
                return RDB_ERROR;
            }
            ret = RDB_copy_obj(compvalp, elemp, ecp);
        }
    } else {
        /* Getter is implemented by user */
        char *opname;
        RDB_object *argv[1];

        opname = RDB_alloc(strlen(valp->typ->name) + strlen(compname)
                + strlen(RDB_GETTER_INFIX) + 1, ecp);
        if (opname == NULL) {
            return RDB_ERROR;
        }

        strcpy(opname, valp->typ->name);
        strcat(opname, RDB_GETTER_INFIX);
        strcat(opname, compname);
        argv[0] = (RDB_object *) valp;

        ret = RDB_call_ro_op_by_name_e(opname, 1, argv, envp, ecp, txp, compvalp);
        RDB_free(opname);
    }
    return ret;
}

/**
 * RDB_obj_set_comp sets the the value of component <var>compname</var>
of a possible representation of the RDB_object specified to by <var>valp</var>
to the value of the variable pointed to by <var>comp</var>.

The RDB_object must be of a type which has a component
<var>compname</var>.

If <var>txp</var> is NULL and <var>envp</var> is not, <var>envp</var> is used
to look up the getter operator from memory.
If <var>txp</var> is not NULL, <var>envp</var> is ignored.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.
 */
int
RDB_obj_set_comp(RDB_object *valp, const char *compname,
        const RDB_object *compvalp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (valp->typ->def.scalar.sysimpl) {
        /* Setter is implemented by the system */
        if (valp->typ->def.scalar.repv[0].compc == 1) {
            ret = RDB_destroy_obj(valp, ecp);
            if (ret != RDB_OK)
                return RDB_ERROR;

            ret = RDB_copy_obj_data(valp, compvalp, ecp, NULL);
        } else {
            ret = RDB_tuple_set(valp, compname, compvalp, ecp);
        }
    } else {
        /* Setter is implemented by user */
        char *opname;
        RDB_object *argv[2];
        RDB_type *argtv[2];
        RDB_operator *op;

        opname = RDB_alloc(strlen(valp->typ->name) + strlen(compname)
                + strlen(RDB_SETTER_INFIX) + 1, ecp);
        if (opname == NULL) {
            return RDB_ERROR;
        }

        strcpy(opname, valp->typ->name);
        strcat(opname, RDB_SETTER_INFIX);
        strcat(opname, compname);
        argv[0] = valp;
        argv[1] = (RDB_object *) compvalp;
        argtv[0] = RDB_obj_type(argv[0]);
        argtv[1] = RDB_obj_type(argv[1]);

        op = RDB_get_update_op_e(opname, 2, argtv, envp, ecp, txp);
        RDB_free(opname);
        if (op == NULL)
            return RDB_ERROR;

        ret = RDB_call_update_op(op, 2, argv, ecp, txp);
    }

    if (ret != RDB_OK)
        return RDB_ERROR;
    
    ret = RDB_check_type_constraint(valp, envp, ecp, txp);
    if (ret != RDB_OK) {
        /* Destroy illegal value */
        RDB_destroy_obj(valp, ecp);
        RDB_init_obj(valp);
    }
    return ret;
}

/**
 * RDB_obj_bool returns the value of the RDB_object pointed to by
<var>valp</var> as a RDB_bool. The RDB_object must be of type
BOOLEAN.

@returns

The value of the RDB_object.
 */
RDB_bool
RDB_obj_bool(const RDB_object *valp)
{
    return valp->val.bool_val;
}

/**
 * RDB_obj_int returns the value of the RDB_object pointed to by
<var>valp</var> as a RDB_int. The RDB_object must be of type
INTEGER.

@returns

The value of the RDB_object.
 */
RDB_int
RDB_obj_int(const RDB_object *valp)
{
    return valp->val.int_val;
}

/**
 * RDB_obj_float returns the value of the RDB_object pointed to by
<var>valp</var> as a RDB_float. The RDB_object must be of type
FLOAT.

@returns

The value of the RDB_object.
 */
RDB_float
RDB_obj_float(const RDB_object *valp)
{
    return valp->val.float_val;
}

/**
 * RDB_obj_string returns a pointer to the value of the RDB_object pointed to by
<var>valp</var> as a char *. The RDB_object must be of type string.

@returns

The string value of the RDB_object.
 */
char *
RDB_obj_string(const RDB_object *valp)
{
    return valp->val.bin.datap;
}

/**
 * RDB_binary_set copies <var>len</var> bytes from srcp to
the position <var>pos</var> in the RDB_object pointed to by <var>valp</var>.
<var>valp</var> must point either to a new initialized RDB_object
or to a RDB_object of type BINARY.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_binary_set(RDB_object *valp, size_t pos, const void *srcp, size_t len,
        RDB_exec_context *ecp)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_BINARY);

    /* If the value is newly initialized, allocate memory */
    if (valp->kind == RDB_OB_INITIAL) {
        valp->val.bin.len = pos + len;
        if (valp->val.bin.len > 0) {
            valp->val.bin.datap = RDB_alloc(valp->val.bin.len, ecp);
            if (valp->val.bin.datap == NULL) {
                return RDB_ERROR;
            }
        }
        valp->typ = &RDB_BINARY;
        valp->kind = RDB_OB_BIN;
    }

    /* If the memory block is to small, reallocate */
    if (valp->val.bin.len < pos + len) {
        void *datap;

        if (valp->val.bin.len > 0)
            datap = RDB_realloc(valp->val.bin.datap, pos + len, ecp);
        else
            datap = RDB_alloc(pos + len, ecp);
        if (datap == NULL) {
            return RDB_ERROR;
        }
        valp->val.bin.datap = datap;
        valp->val.bin.len = pos + len;
    }

    /* Copy data */
    if (len > 0)
        memcpy(((RDB_byte *)valp->val.bin.datap) + pos, srcp, len);
    return RDB_OK;
}

/**
 * RDB_binary_get obtains a pointer to <var>len</var> bytes starting at position
<var>pos</var> of the RDB_object pointed to by <var>valp</var>
and stores this pointer at the location pointed to by <var>pp</var>.
If the sum of <var>pos</var> and <var>len</var> exceeds the length of the
object, the length of the byte block will be lower than requested.

If <var>alenp</var> is not NULL, the actual length of the byte block is stored
at the location pointed to by <var>alenp</var>.

<var>valp</var> must point to a RDB_object of type BINARY.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_binary_get(const RDB_object *objp, size_t pos, size_t len,
        RDB_exec_context *ecp, void **pp, size_t *alenp)
{
    if (pos > objp->val.bin.len) {
        RDB_raise_invalid_argument("index out of range", ecp);
        return RDB_ERROR;
    }
    if (alenp != NULL) {
        if (pos + len > objp->val.bin.len)
            *alenp = objp->val.bin.len - pos;
        else
            *alenp = len;
    }
    *pp = (RDB_byte *)objp->val.bin.datap + pos;
    return RDB_OK;
}

/**
 * RDB_binary_length returns the number of bytes stored in the
RDB_object pointed to by <var>objp</var>. The RDB_object
must be of type BINARY.

@returns

The length of the RDB_object.
 */
size_t
RDB_binary_length(const RDB_object *objp)
{
    return objp->val.bin.len;
}

/**
 * RDB_binary_resize sets the returns the number of bytes stored in the
RDB_object pointed to by <var>objp</var> to <var>len</var>.
If <var>len</var> is greater than the original length, the value of the
bytes added is undefined.

@returns

The length of the RDB_object.
 */
int
RDB_binary_resize(RDB_object *objp, size_t len, RDB_exec_context *ecp)
{
    void *datap;

    if (len > 0) {
        datap = RDB_realloc(objp->val.bin.datap, len, ecp);
        if (datap == NULL)
            return RDB_ERROR;
        objp->val.bin.datap = datap;
    } else {
        RDB_free(objp->val.bin.datap);
    }
    objp->val.bin.len = len;
    return RDB_OK;
}

/**
 * RDB_obj_type returns a pointer to the type of *<var>objp</var>.

@returns

A pointer to the type of the RDB_object.
 */
RDB_type *
RDB_obj_type(const RDB_object *objp)
{
    return objp->typ;
}

/**
 * Set the type information for *<var>objp</var>.
 * This should be used only for tuples and arrays, which, unlike scalars
 * and tables, do not to carry explicit type information by default.
 * The caller must manage the type; it is not automatically destroyed
 * when *<var>objp</var> is destroyed (except if *<var>objp</var> is embedded in an
 * expression, in which case the expression takes responsibility for destroying the type).
 */
void
RDB_obj_set_typeinfo(RDB_object *objp, RDB_type *typ)
{
    objp->typ = typ;
}

/*@}*/

/* Works only for scalar types */
void
RDB_set_obj_type(RDB_object *objp, RDB_type *typ)
{
    objp->typ = typ;
    objp->kind = val_kind(typ);
    if (objp->kind == RDB_OB_BIN)
        objp->val.bin.datap = NULL;
}

static void
dfloat_to_str(RDB_float f, char *bufp)
{
    size_t len;

    sprintf(bufp, "%.10f", (double) f);

    /* Remove trailing zeroes */
    len = strlen(bufp);
    while (len > 1 && bufp[len - 1] == '0' && bufp[len - 2] != '.') {
        bufp[--len] = '\0';
    }
}

int
RDB_obj_to_string(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp)
{
    char buf[64];

    if (srcp->typ == &RDB_INTEGER) {
        sprintf(buf, "%d", RDB_obj_int(srcp));
        if (RDB_string_to_obj(dstp, buf, ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (srcp->typ == &RDB_BOOLEAN) {
        if (RDB_string_to_obj(dstp, RDB_obj_bool(srcp) ? "TRUE" : "FALSE",
                ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (srcp->typ == &RDB_FLOAT) {
        dfloat_to_str(RDB_obj_float(srcp), buf);
        if (RDB_string_to_obj(dstp, buf, ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (srcp->typ == &RDB_STRING) {
        if (RDB_string_to_obj(dstp, RDB_obj_string(srcp), ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (srcp->typ == &RDB_BINARY) {
        if (RDB_string_n_to_obj(dstp, srcp->val.bin.datap, srcp->val.bin.len,
                ecp) != RDB_OK)
            return RDB_ERROR;
    } else {
        RDB_raise_invalid_argument("type cannot be converted to string", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}
