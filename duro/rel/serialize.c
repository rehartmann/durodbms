/*
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "serialize.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <gen/hashtabit.h>
#include <obj/objinternal.h>

#include <string.h>

/*
 * Functions for serializing/deserializing - needed for
 * storing expressions and types in the catalog
 */

enum {
    RDB_BUF_INITLEN = 256
};

static int
reserve_space(RDB_object *valp, int pos, size_t n, RDB_exec_context *ecp)
{
    /* If there is not enough buffer space, reserve more */
    if (valp->val.bin.len < pos + n) {
        /* Reserve more space than requested to reduce # of reallocations */
        int newlen = pos + n + RDB_BUF_INITLEN;
        void *newdatap = RDB_realloc(valp->val.bin.datap, newlen, ecp);

        if (newdatap == NULL) {
            return RDB_ERROR;
        }
        valp->val.bin.len = newlen;
        valp->val.bin.datap = newdatap;
    }

    return RDB_OK;
}

static int
RDB_serialize_str(RDB_object *valp, int *posp, const char *str,
        RDB_exec_context *ecp)
{
    size_t len = strlen(str);

    if (reserve_space(valp, *posp, len + sizeof len, ecp) != RDB_OK)
        return RDB_ERROR;

    memcpy(((uint8_t *) valp->val.bin.datap) + *posp, &len, sizeof len);
    *posp += sizeof len;
    memcpy(((uint8_t *) valp->val.bin.datap) + *posp, str, len);
    *posp += len;
    return RDB_OK;
}

static int
RDB_serialize_int(RDB_object *valp, int *posp, RDB_int v, RDB_exec_context *ecp)
{
    if (reserve_space(valp, *posp, sizeof v, ecp) != RDB_OK)
        return RDB_ERROR;

    memcpy(((uint8_t *) valp->val.bin.datap) + *posp, &v, sizeof v);
    *posp += sizeof v;
    return RDB_OK;
}

static int
serialize_size_t(RDB_object *valp, int *posp, size_t v, RDB_exec_context *ecp)
{
    int ret;

    ret = reserve_space(valp, *posp, sizeof v, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    memcpy(((uint8_t *) valp->val.bin.datap) + *posp, &v, sizeof v);
    *posp += sizeof v;
    return RDB_OK;
}

static int
RDB_serialize_byte(RDB_object *valp, int *posp, uint8_t b,
        RDB_exec_context *ecp)
{
    if (reserve_space(valp, *posp, 1, ecp) != RDB_OK)
        return RDB_ERROR;
    ((uint8_t *) valp->val.bin.datap)[(*posp)++] = b;
    return RDB_OK;
}

int
RDB_serialize_scalar_type(RDB_object *valp, int *posp, const char *name,
        RDB_exec_context *ecp)
{
    if (RDB_serialize_byte(valp, posp, (uint8_t) RDB_TP_SCALAR, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_serialize_str(valp, posp, name, ecp);
}

static int
RDB_serialize_type(RDB_object *valp, int *posp, const RDB_type *typ,
        RDB_exec_context *ecp)
{
    if (typ->kind == RDB_TP_SCALAR) {
        return RDB_serialize_scalar_type(valp, posp, RDB_type_name(typ), ecp);
    }

    if (RDB_serialize_byte(valp, posp, (uint8_t) typ->kind, ecp) != RDB_OK)
        return RDB_ERROR;

    switch (typ->kind) {
    case RDB_TP_TUPLE:
    {
        int i;
        int attridx;
        char *lastwritten = NULL;

        if (RDB_serialize_int(valp, posp, typ->def.tuple.attrc, ecp) != RDB_OK)
            return RDB_ERROR;

        /*
         * Write attributes in alphabetical order of their names,
         * to get a canonical representation of attribute types.
         * Necessary because types are compares bitwise when searching
         * for a matching operator.
         */
        for (i = 0; i < typ->def.tuple.attrc; i++) {
            attridx = RDB_next_attr_sorted(typ, lastwritten);

            if (typ->def.tuple.attrv[attridx].name != NULL) {
                if (RDB_serialize_str(valp, posp,
                        typ->def.tuple.attrv[attridx].name, ecp) != RDB_OK) {
                    return RDB_ERROR;
                }

                if (RDB_serialize_type(valp, posp,
                        typ->def.tuple.attrv[attridx].typ, ecp) != RDB_OK) {
                    return RDB_ERROR;
                }
            } else {
                if (RDB_serialize_str(valp, posp, "", ecp) != RDB_OK) {
                    return RDB_ERROR;
                }
            }
            lastwritten = typ->def.tuple.attrv[attridx].name != NULL ? typ->def.tuple.attrv[attridx].name : "";
        }
        return RDB_OK;
    }
    case RDB_TP_RELATION:
    case RDB_TP_ARRAY:
        return RDB_serialize_type(valp, posp, typ->def.basetyp, ecp);
    default:
        break;
    }
    RDB_raise_internal("RDB_serialize_type(): invalid type", ecp);
    return RDB_ERROR;
}

/*
 * Serialize transient RDB_objects.
 */
static int
RDB_serialize_obj(RDB_object *valp, int *posp, const RDB_object *argvalp,
        RDB_exec_context *ecp)
{
    size_t len;
    RDB_bool crtpltyp = RDB_FALSE;
    RDB_type *typ = RDB_obj_type(argvalp);

    if (typ == NULL) {
        if (argvalp->kind != RDB_OB_TUPLE) {
            RDB_raise_invalid_argument("type is missing for non-tuple", ecp);
            return RDB_ERROR;
        }

        typ = RDB_tuple_type(argvalp, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        crtpltyp = RDB_TRUE;
    }
    ((RDB_object *)argvalp)->store_typ = typ;

    if (RDB_serialize_type(valp, posp, typ, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    len = typ->ireplen;
    if (len == (size_t) RDB_VARIABLE_LEN) {
        /*
         * Store size
         */
        if (RDB_obj_ilen(argvalp, &len, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        if (serialize_size_t(valp, posp, len, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    if (reserve_space(valp, *posp, len, ecp) != RDB_OK)
        return RDB_ERROR;

    RDB_obj_to_irep(((uint8_t *) valp->val.bin.datap) + *posp, argvalp, len);
    *posp += len;
    if (crtpltyp) {
        RDB_del_nonscalar_type(typ, ecp);
        ((RDB_object *)argvalp)->typ = NULL;
    }
    return RDB_OK;
}

static int
serialize_trobj(RDB_object *valp, int *posp, const RDB_object *argvalp,
        RDB_exec_context *ecp)
{
    if (argvalp->kind == RDB_OB_TABLE && RDB_table_name(argvalp) != NULL) {
        RDB_raise_invalid_argument("cannot serialize named local table", ecp);
        return RDB_ERROR;
    }
    return RDB_serialize_obj(valp, posp, argvalp, ecp);
}

static int
serialize_table(RDB_object *valp, int *posp, RDB_object *tbp, RDB_exec_context *);

int
RDB_serialize_expr(RDB_object *valp, int *posp, RDB_expression *exp,
        RDB_exec_context *ecp)
{
    /* Expression kind (1 byte) */
    if (RDB_serialize_byte(valp, posp, (uint8_t) exp->kind, ecp) != RDB_OK)
        return RDB_ERROR;

    switch(exp->kind) {
    case RDB_EX_OBJ:
        return serialize_trobj(valp, posp, &exp->def.obj, ecp);
    case RDB_EX_TBP:
        return serialize_table(valp, posp, exp->def.tbref.tbp, ecp);
    case RDB_EX_VAR:
        return RDB_serialize_str(valp, posp, exp->def.varname, ecp);
    case RDB_EX_RO_OP:
    {
        RDB_expression *argp;

        /* Operator name */
        if (RDB_serialize_str(valp, posp, exp->def.op.name, ecp) != RDB_OK)
            return RDB_ERROR;

        /* # of arguments */
        if (RDB_serialize_int (valp, posp,
                RDB_expr_list_length(&exp->def.op.args), ecp) != RDB_OK) {
            return RDB_ERROR;
        }

        /* Write arg expressions */
        argp = exp->def.op.args.firstp;
        while (argp != NULL) {
            if (RDB_serialize_expr(valp, posp, argp, ecp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
        }

        /*
         * Serialize type to preserve the type of e.g. RELATION()
         * Only do this when the number of arguments is zero, because
         * RDB_expr_type() will not go deeper if type information is already present
         * and descendants may not get type information.
         * This can cause in RDB_qresult not working on virtual table
         * whose definition contain tuple-valued attribute names.
         */

        if (exp->typ != NULL && exp->def.op.args.firstp == NULL) {
            if (RDB_serialize_byte(valp, posp, (uint8_t) RDB_TRUE,
                    ecp) != RDB_OK)
                return RDB_ERROR;

            if (exp->typ != NULL) {
                if (RDB_serialize_type(valp, posp, exp->typ, ecp) != RDB_OK)
                    return RDB_ERROR;
            }
        } else {
            if (RDB_serialize_byte(valp, posp, (uint8_t) RDB_FALSE,
                    ecp) != RDB_OK)
                return RDB_ERROR;
        }

        return RDB_OK;
    }
    }
    /* should never be reached */
    abort();
} /* RDB_serialize_expr */

static int
serialize_table(RDB_object *valp, int *posp, RDB_object *tbp,
        RDB_exec_context *ecp)
{
    /* If the table is persistent, write name only */
    if (RDB_table_is_persistent(tbp))
        return RDB_serialize_str(valp, posp, RDB_table_name(tbp), ecp);
    if (RDB_serialize_str(valp, posp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    if (tbp->val.tbp->exp == NULL) {
        if (RDB_serialize_byte(valp, posp, (uint8_t) 1, ecp) != RDB_OK)
            return RDB_ERROR;
        return serialize_trobj(valp, posp, tbp, ecp);
    }
    if (RDB_serialize_byte(valp, posp, (uint8_t) 0, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_serialize_expr(valp, posp, tbp->val.tbp->exp, ecp);
}

int
RDB_vtable_to_binobj(RDB_object *valp, RDB_object *tbp, RDB_exec_context *ecp)
{
    int pos;
    int ret;

    RDB_destroy_obj(valp, ecp);
    valp->typ = &RDB_BINARY;
    valp->kind = RDB_OB_BIN;
    valp->val.bin.len = RDB_BUF_INITLEN;
    valp->val.bin.datap = RDB_alloc(RDB_BUF_INITLEN, ecp);
    if (valp->val.bin.datap == NULL) {
        return RDB_ERROR;
    }
    pos = 0;
    ret = RDB_serialize_expr(valp, &pos, tbp->val.tbp->exp, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    valp->val.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
RDB_type_to_bin(RDB_object *valp, const RDB_type *typ, RDB_exec_context *ecp)
{
    int pos;
    int ret;

    RDB_destroy_obj(valp, ecp);
    valp->typ = &RDB_BINARY;
    valp->kind = RDB_OB_BIN;
    valp->val.bin.len = RDB_BUF_INITLEN;
    valp->val.bin.datap = RDB_alloc(RDB_BUF_INITLEN, ecp);
    if (valp->val.bin.datap == NULL) {
        return RDB_ERROR;
    }
    pos = 0;
    ret = RDB_serialize_type(valp, &pos, typ, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    /* Set length to actual length */
    valp->val.bin.len = pos;
    return RDB_OK;
}

int
RDB_obj_to_bin(RDB_object *binobjp, const RDB_object *srcp,
        RDB_exec_context *ecp)
{
    int pos;

    RDB_destroy_obj(binobjp, ecp);
    binobjp->typ = &RDB_BINARY;
    binobjp->kind = RDB_OB_BIN;
    binobjp->val.bin.len = RDB_BUF_INITLEN;
    binobjp->val.bin.datap = RDB_alloc(RDB_BUF_INITLEN, ecp);
    if (binobjp->val.bin.datap == NULL) {
        return RDB_ERROR;
    }
    pos = 0;
    if (RDB_serialize_obj(binobjp, &pos, srcp, ecp) != RDB_OK)
        return RDB_ERROR;

    /* Set length to actual length */
    binobjp->val.bin.len = pos;
    return RDB_OK;
}

int
RDB_expr_to_bin(RDB_object *valp, RDB_expression *exp,
        RDB_exec_context *ecp)
{
    int pos = 0;

    RDB_destroy_obj(valp, ecp);
    valp->typ = &RDB_BINARY;
    valp->kind = RDB_OB_BIN;
    if (exp != NULL) {
        valp->val.bin.len = RDB_BUF_INITLEN;
        valp->val.bin.datap = RDB_alloc(RDB_BUF_INITLEN, ecp);
        if (valp->val.bin.datap == NULL) {
            return RDB_ERROR;
        }
        if (RDB_serialize_expr(valp, &pos, exp, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    } else {
        valp->val.bin.datap = NULL;
    }

    valp->val.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

static int
RDB_deserialize_str(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        char **strp)
{
    size_t len;
    
    if (*posp + sizeof (len) > valp->val.bin.len) {
        RDB_raise_internal("invalid string during deserialization", ecp);
        return RDB_ERROR;
    }
    memcpy (&len, ((uint8_t *)valp->val.bin.datap) + *posp, sizeof len);
    *posp += sizeof len;
    if (*posp + len > valp->val.bin.len) {
        RDB_raise_internal("invalid string during deserialization", ecp);
        return RDB_ERROR;
    }
    *strp = RDB_alloc(len + 1, ecp);
    if (*strp == NULL) {
        return RDB_ERROR;
    }
    memcpy(*strp, ((uint8_t *)valp->val.bin.datap) + *posp, len);
    (*strp)[len] = '\0';
    *posp += len;

    return RDB_OK;
}

static int
RDB_deserialize_int(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_int *vp)
{
    if (*posp + sizeof (RDB_int) > valp->val.bin.len) {
        RDB_raise_internal("invalid integer during deserialization", ecp);
        return RDB_ERROR;
    }
    memcpy(vp, ((uint8_t *)valp->val.bin.datap) + *posp, sizeof (RDB_int));
    *posp += sizeof (RDB_int);
    return RDB_OK;
}

static int
deserialize_size_t(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        size_t *vp)
{
    if (*posp + sizeof (RDB_int) > valp->val.bin.len) {
        RDB_raise_internal("invalid integer during deserialization", ecp);
        return RDB_ERROR;
    }
    memcpy(vp, ((uint8_t *)valp->val.bin.datap) + *posp, sizeof (size_t));
    *posp += sizeof (size_t);
    return RDB_OK;
}

static int
RDB_deserialize_byte(const RDB_object *valp, int *posp, RDB_exec_context *ecp)
{
    if (*posp + 1 > valp->val.bin.len) {
        RDB_raise_internal("invalid byte during deserialization", ecp);
        return RDB_ERROR;
    }

    return ((uint8_t *) valp->val.bin.datap)[(*posp)++];
}

RDB_type *
RDB_deserialize_type(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *namp;
    int ret;
    enum RDB_tp_kind kind;
    RDB_type *typ;

    ret = RDB_deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return NULL;
    kind = (enum RDB_tp_kind ) ret;

    switch (kind) {
    case RDB_TP_SCALAR:
        ret = RDB_deserialize_str(valp, posp, ecp, &namp);
        if (ret != RDB_OK)
            return NULL;
        typ = RDB_get_type(namp, ecp, txp);
        RDB_free(namp);
        return typ;
    case RDB_TP_TUPLE:
    {
        RDB_int attrc;
        int i;

        ret = RDB_deserialize_int(valp, posp, ecp, &attrc);
        typ = RDB_alloc(sizeof (RDB_type), ecp);
        if (typ == NULL) {
            return NULL;
        }
        typ->name = NULL;
        typ->kind = RDB_TP_TUPLE;
        typ->ireplen = RDB_VARIABLE_LEN;
        typ->cleanup_fp = NULL;

        typ->def.tuple.attrv = RDB_alloc(sizeof(RDB_attr) * attrc, ecp);
        if (typ->def.tuple.attrv == NULL) {
            RDB_free(typ);
            return NULL;
        }
        for (i = 0; i < attrc; i++) {
            ret = RDB_deserialize_str(valp, posp, ecp,
                    &typ->def.tuple.attrv[i].name);
            if (ret != RDB_OK) {
                RDB_free(typ->def.tuple.attrv);
                RDB_free(typ);
                return NULL;
            }
            if (*typ->def.tuple.attrv[i].name != '\0') {
                typ->def.tuple.attrv[i].typ = RDB_deserialize_type(valp, posp,
                        ecp, txp);
                if (typ->def.tuple.attrv[i].typ == NULL) {
                    RDB_free(typ->def.tuple.attrv);
                    RDB_free(typ);
                    return NULL;
                }
            } else {
                // Generic tuple type -- replace empty attribute name by NULL
                RDB_free(typ->def.tuple.attrv[i].name);
                typ->def.tuple.attrv[i].name = NULL;
            }
            typ->def.tuple.attrv[i].defaultp = NULL;
        }
        typ->def.tuple.attrc = attrc;
        return typ;
    }
    case RDB_TP_RELATION:
    case RDB_TP_ARRAY:
        typ = RDB_alloc(sizeof (RDB_type), ecp);
        if (typ == NULL) {
            return NULL;
        }
        typ->name = NULL;
        typ->kind = kind;
        typ->ireplen = RDB_VARIABLE_LEN;
        typ->cleanup_fp = NULL;

        typ->def.basetyp = RDB_deserialize_type(valp, posp, ecp, txp);
        if (typ->def.basetyp == NULL) {
            RDB_free(typ);
            return NULL;
        }
        return typ;
    }
    RDB_raise_internal("invalid type during deserialization", ecp);
    return NULL;
} /* RDB_deserialize_type */

static RDB_object *
deserialize_table(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp);

static int
RDB_deserialize_obj(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *argvalp)
{
    RDB_type *typ;
    size_t len;
    int ret;

    typ = RDB_deserialize_type(valp, posp, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    len = typ->ireplen;
    if (len == (size_t) RDB_VARIABLE_LEN) {
        if (deserialize_size_t(valp, posp, ecp, &len) != RDB_OK)
            return RDB_ERROR;
    }

    ret = RDB_irep_to_obj(argvalp, typ,
            ((uint8_t *)valp->val.bin.datap) + *posp, len, ecp);
    *posp += len;

    if (typ->kind == RDB_TP_TUPLE) {
        RDB_del_nonscalar_type(typ, ecp);
        argvalp->typ = NULL;
    }
    return ret;
}

RDB_type *
RDB_bin_to_type(const RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int pos = 0;

    return RDB_deserialize_type(valp, &pos, ecp, txp);
}

int
RDB_bin_to_obj(const RDB_object *binobjp, RDB_object *dstp,
        RDB_exec_context *ecp)
{
    int pos = 0;
    return RDB_deserialize_obj(binobjp, &pos, ecp, NULL, dstp);
}

int
RDB_deserialize_expr(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_expression **expp)
{
    enum RDB_expr_kind ekind;
    int ret;

    if (valp->val.bin.len == 0) {
        *expp = NULL;
        return RDB_OK;
    }

    ret = RDB_deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return ret;
    if (ret == 3) {
        RDB_raise_not_supported(
                "Storage format of expression no longer supported, must be deleted and recreated",
                ecp);
        return RDB_ERROR;
    }
    ekind = (enum RDB_expr_kind) ret;
    switch (ekind) {
    case RDB_EX_OBJ:
    {
        RDB_object val;

        RDB_init_obj(&val);
        ret = RDB_deserialize_obj(valp, posp, ecp, txp, &val);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&val, ecp);
            return ret;
        }
        *expp = RDB_obj_to_expr(&val, ecp);
        RDB_destroy_obj(&val, ecp);
        if (*expp == NULL) {
            return RDB_ERROR;
        }
    }
    break;
    case RDB_EX_TBP:
    {
        RDB_object *tbp = deserialize_table(valp, posp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        *expp = RDB_table_ref(tbp, ecp);
        if (*expp == NULL)
            return RDB_ERROR;
    }
    break;
    case RDB_EX_VAR:
    {
        char *attrnamp;

        ret = RDB_deserialize_str(valp, posp, ecp, &attrnamp);
        if (ret != RDB_OK)
            return ret;

        *expp = RDB_var_ref(attrnamp, ecp);
        RDB_free(attrnamp);
        if (*expp == NULL)
            return RDB_ERROR;
    }
    break;
    case RDB_EX_RO_OP:
    {
        char *name;
        RDB_int argc;
        int i;

        ret = RDB_deserialize_str(valp, posp, ecp, &name);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        ret = RDB_deserialize_int(valp, posp, ecp, &argc);
        if (ret != RDB_OK) {
            RDB_free(name);
            return RDB_ERROR;
        }

        *expp = RDB_ro_op(name, ecp);
        RDB_free(name);
        if (*expp == NULL)
            return RDB_ERROR;

        for (i = 0; i < argc; i++) {
            RDB_expression *argp;

            ret = RDB_deserialize_expr(valp, posp, ecp, txp, &argp);
            if (ret != RDB_OK) {
                return RDB_ERROR;
            }
            RDB_add_arg(*expp, argp);
        }

        /*
         * Restore type if it was stored to preserve
         * the type of e.g. RELATION()
         */
         ret = RDB_deserialize_byte(valp, posp, ecp);
        if (ret < 0)
            return RDB_ERROR;
        if (ret) {
            RDB_type *typ = RDB_deserialize_type(valp, posp, ecp, txp);
            if (typ == NULL)
                return RDB_ERROR;
            RDB_set_expr_type(*expp, typ);
        }
        break;
    }
    }
    return RDB_OK;
} /* RDB_deserialize_expr */

RDB_expression *
RDB_bin_to_expr(const RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp;
    int pos = 0;

    if (RDB_deserialize_expr(valp, &pos, ecp, txp, &exp) != RDB_OK)
        return NULL;
    return exp;
}

RDB_object *
deserialize_rtable(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *tbp = RDB_new_obj(ecp);
    if (tbp == NULL)
        return NULL;

     if (RDB_deserialize_obj(valp, posp, ecp, txp, tbp) != RDB_OK)
         return NULL;
     return tbp;
}

static RDB_object *
deserialize_table(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *namp;
    RDB_object *tbp;
    RDB_expression *exp;
    int ret = RDB_deserialize_str(valp, posp, ecp, &namp);
    if (ret != RDB_OK)
        return NULL;

    if (*namp != '\0') {
        tbp = RDB_get_table(namp, ecp, txp);
        RDB_free(namp);
        return tbp;
    }
    RDB_free(namp);

    ret = RDB_deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return NULL;
    if (ret) {
        return deserialize_rtable(valp, posp, ecp, txp);
    }
    ret = RDB_deserialize_expr(valp, posp, ecp, txp, &exp);
    if (ret != RDB_OK)
        return NULL;
    
    return RDB_expr_to_vtable(exp, ecp, txp);
}
