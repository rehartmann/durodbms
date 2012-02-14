/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "serialize.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <gen/hashtabit.h>
#include <string.h>
#include <assert.h>

/*
 * Functions for serializing/deserializing - needed for
 * persistent virtual tables (aka views)
 */

static int
reserve_space(RDB_object *valp, int pos, size_t n, RDB_exec_context *ecp)
{
    while (valp->val.bin.len < pos + n) {
        int newlen = valp->val.bin.len * 2;
        void *newdatap = RDB_realloc(valp->val.bin.datap, newlen, ecp);
        
        if (newdatap == NULL) {
            return RDB_ERROR;
        }
        valp->val.bin.len = newlen;
        valp->val.bin.datap = newdatap;
    }

    return RDB_OK;
}

int
_RDB_serialize_str(RDB_object *valp, int *posp, const char *str,
        RDB_exec_context *ecp)
{
    size_t len = strlen(str);

    if (reserve_space(valp, *posp, len + sizeof len, ecp) != RDB_OK)
        return RDB_ERROR;

    memcpy(((RDB_byte *) valp->val.bin.datap) + *posp, &len, sizeof len);
    *posp += sizeof len;
    memcpy(((RDB_byte *) valp->val.bin.datap) + *posp, str, len);
    *posp += len;
    return RDB_OK;
}

int
_RDB_serialize_int(RDB_object *valp, int *posp, RDB_int v, RDB_exec_context *ecp)
{
    if (reserve_space(valp, *posp, sizeof v, ecp) != RDB_OK)
        return RDB_ERROR;

    memcpy(((RDB_byte *) valp->val.bin.datap) + *posp, &v, sizeof v);
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
    memcpy(((RDB_byte *) valp->val.bin.datap) + *posp, &v, sizeof v);
    *posp += sizeof v;
    return RDB_OK;
}

int
_RDB_serialize_byte(RDB_object *valp, int *posp, RDB_byte b,
        RDB_exec_context *ecp)
{
    if (reserve_space(valp, *posp, 1, ecp) != RDB_OK)
        return RDB_ERROR;
    ((RDB_byte *) valp->val.bin.datap)[(*posp)++] = b;
    return RDB_OK;
}

int
_RDB_serialize_type(RDB_object *valp, int *posp, const RDB_type *typ,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_byte(valp, posp, (RDB_byte) typ->kind, ecp) != RDB_OK)
        return RDB_ERROR;

    switch (typ->kind) {
        case RDB_TP_SCALAR:
            return _RDB_serialize_str(valp, posp, typ->name, ecp);
        case RDB_TP_TUPLE:
        {
            int i;
            int attridx;
            char *lastwritten = NULL;

            if (_RDB_serialize_int(valp, posp, typ->def.tuple.attrc, ecp) != RDB_OK)
                return RDB_ERROR;

            /*
             * Write attributes in alphabetical order of their names,
             * to get a canonical representation of attribute types.
             * Necessary because types are compares bitwise when searching
             * for a matching operator.
             */
            for (i = 0; i < typ->def.tuple.attrc; i++) {
                attridx = RDB_next_attr_sorted(typ, lastwritten);

                if (_RDB_serialize_str(valp, posp,
                        typ->def.tuple.attrv[attridx].name, ecp) != RDB_OK) {
                    return RDB_ERROR;
                }

                if (_RDB_serialize_type(valp, posp,
                        typ->def.tuple.attrv[attridx].typ, ecp) != RDB_OK) {
                    return RDB_ERROR;
                }
                lastwritten = typ->def.tuple.attrv[attridx].name;
            }
            return RDB_OK;
        }
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            return _RDB_serialize_type(valp, posp, typ->def.basetyp, ecp);
    }
    abort();
}

/*
 * Serialize transient RDB_objects.
 */
static int
serialize_trobj(RDB_object *valp, int *posp, const RDB_object *argvalp,
        RDB_exec_context *ecp)
{
    size_t len;
    RDB_bool crtpltyp = RDB_FALSE;
    RDB_type *typ = RDB_obj_type(argvalp);

    if (argvalp->kind == RDB_OB_TABLE && RDB_table_name(argvalp) != NULL) {
        RDB_raise_invalid_argument("cannot serialize named local table", ecp);
        return RDB_ERROR;
    }

    if (typ == NULL) {
        assert(argvalp->kind == RDB_OB_TUPLE);

        typ = _RDB_tuple_type(argvalp, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        crtpltyp = RDB_TRUE;
    }
    ((RDB_object *)argvalp)->store_typ = typ;

    if (_RDB_serialize_type(valp, posp, typ, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    len = typ != NULL ? typ->ireplen : RDB_VARIABLE_LEN;
    if (len == (size_t) RDB_VARIABLE_LEN) {
        /*
         * Store size
         */
        if (_RDB_obj_ilen(argvalp, &len, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        if (serialize_size_t(valp, posp, len, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    if (reserve_space(valp, *posp, len, ecp) != RDB_OK)
        return RDB_ERROR;

    _RDB_obj_to_irep(((RDB_byte *) valp->val.bin.datap) + *posp, argvalp, len);
    *posp += len;
    if (crtpltyp) {
        RDB_del_nonscalar_type(typ, ecp);
        ((RDB_object *)argvalp)->typ = NULL;
    }
    return RDB_OK;
}

static int
serialize_table(RDB_object *valp, int *posp, RDB_object *tbp, RDB_exec_context *);

int
_RDB_serialize_expr(RDB_object *valp, int *posp, RDB_expression *exp,
        RDB_exec_context *ecp)
{
    /* Expression kind (1 byte) */
    if (_RDB_serialize_byte(valp, posp, (RDB_byte) exp->kind, ecp) != RDB_OK)
        return RDB_ERROR;

    switch(exp->kind) {
        case RDB_EX_OBJ:
            return serialize_trobj(valp, posp, &exp->def.obj, ecp);
        case RDB_EX_TBP:
            return serialize_table(valp, posp, exp->def.tbref.tbp, ecp);
        case RDB_EX_VAR:
            return _RDB_serialize_str(valp, posp, exp->def.varname, ecp);
        case RDB_EX_GET_COMP:
            if (_RDB_serialize_expr(valp, posp, exp->def.op.args.firstp, ecp)
                    != RDB_OK)
                return RDB_ERROR;
            return _RDB_serialize_str(valp, posp, exp->def.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp;

            /* Operator name */
            if (_RDB_serialize_str(valp, posp, exp->def.op.name, ecp) != RDB_OK)
                return RDB_ERROR;

            /* # of arguments */
            if (_RDB_serialize_int (valp, posp,
                    RDB_expr_list_length(&exp->def.op.args), ecp) != RDB_OK) {
                return RDB_ERROR;
            }

            /* Write arg expressions */
            argp = exp->def.op.args.firstp;
            while (argp != NULL) {
                if (_RDB_serialize_expr(valp, posp, argp, ecp) != RDB_OK)
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
                if (_RDB_serialize_byte(valp, posp, (RDB_byte) RDB_TRUE,
                        ecp) != RDB_OK)
                    return RDB_ERROR;

                if (exp->typ != NULL) {
                    if (_RDB_serialize_type(valp, posp, exp->typ, ecp) != RDB_OK)
                        return RDB_ERROR;
                }
            } else {
                if (_RDB_serialize_byte(valp, posp, (RDB_byte) RDB_FALSE,
                        ecp) != RDB_OK)
                    return RDB_ERROR;
            }

            return RDB_OK;
        }
        case RDB_EX_TUPLE_ATTR:
            if (_RDB_serialize_expr(valp, posp, exp->def.op.args.firstp, ecp)
                    != RDB_OK)
                return RDB_ERROR;
            return _RDB_serialize_str(valp, posp, exp->def.op.name, ecp);
    }
    /* should never be reached */
    abort();
} /* _RDB_serialize_expr */

static int
serialize_table(RDB_object *valp, int *posp, RDB_object *tbp,
        RDB_exec_context *ecp)
{
    /* If the table is persistent, write name only */
    if (tbp->val.tb.is_persistent)
        return _RDB_serialize_str(valp, posp, RDB_table_name(tbp), ecp);
    if (_RDB_serialize_str(valp, posp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    if (tbp->val.tb.exp == NULL) {
        if (_RDB_serialize_byte(valp, posp, (RDB_byte) 1, ecp) != RDB_OK)
            return RDB_ERROR;
        return serialize_trobj(valp, posp, tbp, ecp);
    }
    if (_RDB_serialize_byte(valp, posp, (RDB_byte) 0, ecp) != RDB_OK)
        return RDB_ERROR;
    return _RDB_serialize_expr(valp, posp, tbp->val.tb.exp, ecp);
}

int
_RDB_vtable_to_binobj(RDB_object *valp, RDB_object *tbp, RDB_exec_context *ecp)
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
    ret = _RDB_serialize_expr(valp, &pos, tbp->val.tb.exp, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    valp->val.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
_RDB_type_to_binobj(RDB_object *valp, const RDB_type *typ, RDB_exec_context *ecp)
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
    ret = _RDB_serialize_type(valp, &pos, typ, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    valp->val.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
_RDB_expr_to_binobj(RDB_object *valp, RDB_expression *exp,
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
        if (_RDB_serialize_expr(valp, &pos, exp, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    } else {
        valp->val.bin.datap = NULL;
    }

    valp->val.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
_RDB_deserialize_str(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        char **strp)
{
    size_t len;
    
    if (*posp + sizeof (len) > valp->val.bin.len) {
        RDB_raise_internal("invalid string during deserialization", ecp);
        return RDB_ERROR;
    }
    memcpy (&len, ((RDB_byte *)valp->val.bin.datap) + *posp, sizeof len);
    *posp += sizeof len;
    if (*posp + len > valp->val.bin.len) {
        RDB_raise_internal("invalid string during deserialization", ecp);
        return RDB_ERROR;
    }
    *strp = RDB_alloc(len + 1, ecp);
    if (*strp == NULL) {
        return RDB_ERROR;
    }
    memcpy(*strp, ((RDB_byte *)valp->val.bin.datap) + *posp, len);
    (*strp)[len] = '\0';
    *posp += len;

    return RDB_OK;
}

int
_RDB_deserialize_strobj(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_object *strobjp)
{
    char *str;
    if (RDB_destroy_obj(strobjp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_deserialize_str(valp, posp, ecp, &str) != RDB_OK)
        return RDB_ERROR;

    RDB_init_obj(strobjp);
    strobjp->typ = &RDB_STRING;
    strobjp->kind = RDB_OB_BIN;
    strobjp->val.bin.len = strlen(str);
    strobjp->val.bin.datap = str;
    return RDB_OK;
}

int
_RDB_deserialize_int(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_int *vp)
{
    if (*posp + sizeof (RDB_int) > valp->val.bin.len) {
        RDB_raise_internal("invalid integer during deserialization", ecp);
        return RDB_ERROR;
    }
    memcpy (vp, ((RDB_byte *)valp->val.bin.datap) + *posp, sizeof (RDB_int));
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
    memcpy (vp, ((RDB_byte *)valp->val.bin.datap) + *posp, sizeof (size_t));
    *posp += sizeof (RDB_int);
    return RDB_OK;
}

int
_RDB_deserialize_byte(RDB_object *valp, int *posp, RDB_exec_context *ecp)
{
    if (*posp + 1 > valp->val.bin.len) {
        RDB_raise_internal("invalid byte during deserialization", ecp);
        return RDB_ERROR;
    }

    return ((RDB_byte *) valp->val.bin.datap)[(*posp)++];
}

RDB_type *
_RDB_deserialize_type(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *namp;
    int ret;
    enum _RDB_tp_kind kind;
    RDB_type *typ;

    ret = _RDB_deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return NULL;
    kind = (enum _RDB_tp_kind ) ret;

    switch (kind) {
        case RDB_TP_SCALAR:
            ret = _RDB_deserialize_str(valp, posp, ecp, &namp);
            if (ret != RDB_OK)
                return NULL;
            typ = RDB_get_type(namp, ecp, txp);
            RDB_free(namp);
            return typ;
        case RDB_TP_TUPLE:
        {
            RDB_int attrc;
            int i;

            ret = _RDB_deserialize_int(valp, posp, ecp, &attrc);
            typ = RDB_alloc(sizeof (RDB_type), ecp);
            if (typ == NULL) {
                return NULL;
            }
            typ->name = NULL;
            typ->kind = RDB_TP_TUPLE;
            typ->ireplen = RDB_VARIABLE_LEN;

            typ->def.tuple.attrv = RDB_alloc(sizeof(RDB_attr) * attrc, ecp);
            if (typ->def.tuple.attrv == NULL) {
                RDB_free(typ);
                return NULL;
            }
            for (i = 0; i < attrc; i++) {
                ret = _RDB_deserialize_str(valp, posp, ecp,
                        &typ->def.tuple.attrv[i].name);
                if (ret != RDB_OK) {
                    RDB_free(typ->def.tuple.attrv);
                    RDB_free(typ);
                }
                typ->def.tuple.attrv[i].typ = _RDB_deserialize_type(valp, posp,
                        ecp, txp);
                if (typ->def.tuple.attrv[i].typ == NULL) {
                    RDB_free(typ->def.tuple.attrv);
                    RDB_free(typ);
                    return NULL;
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

            typ->def.basetyp = _RDB_deserialize_type(valp, posp, ecp, txp);
            if (typ->def.basetyp == NULL) {
                RDB_free(typ);
                return NULL;
            }
            return typ;
    }
    RDB_raise_internal("invalid type during deserialization", ecp);
    return NULL;
} /* _RDB_deserialize_type */

RDB_type *
_RDB_binobj_to_type(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int pos = 0;

    return _RDB_deserialize_type(valp, &pos, ecp, txp);
}

static RDB_object *
deserialize_table(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp);

static int
deserialize_trobj(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *argvalp)
{
    RDB_type *typ;
    size_t len;
    int ret;

    typ = _RDB_deserialize_type(valp, posp, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    len = typ->ireplen;
    if (len == (size_t) RDB_VARIABLE_LEN) {
        if (deserialize_size_t(valp, posp, ecp, &len) != RDB_OK)
            return RDB_ERROR;
    }

    ret = RDB_irep_to_obj(argvalp, typ,
            ((RDB_byte *)valp->val.bin.datap) + *posp, len, ecp);
    *posp += len;

    if (typ->kind == RDB_TP_TUPLE) {
        RDB_del_nonscalar_type(typ, ecp);
        argvalp->typ = NULL;
    }
    return ret;
}

int
_RDB_deserialize_expr(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_expression **expp)
{
    RDB_expression *ex1p;
    enum _RDB_expr_kind ekind;
    int ret;

    if (valp->val.bin.len == 0) {
        *expp = NULL;
        return RDB_OK;
    }

    ret = _RDB_deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return ret;
    ekind = (enum _RDB_expr_kind) ret;
    switch (ekind) {
        case RDB_EX_OBJ:
            {
               RDB_object val;

               RDB_init_obj(&val);
               ret = deserialize_trobj(valp, posp, ecp, txp, &val);
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
            
                ret = _RDB_deserialize_str(valp, posp, ecp, &attrnamp);
                if (ret != RDB_OK)
                    return ret;

                *expp = RDB_var_ref(attrnamp, ecp);
                RDB_free(attrnamp);
                if (*expp == NULL)
                    return RDB_ERROR;
            }
            break;
        case RDB_EX_GET_COMP:
        {
            char *name;
            
            ret = _RDB_deserialize_expr(valp, posp, ecp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_deserialize_str(valp, posp, ecp, &name);
            if (ret != RDB_OK) {
                RDB_drop_expr(ex1p, ecp);
                return ret;
            }
            *expp = _RDB_create_unexpr(ex1p, RDB_EX_GET_COMP, ecp);
            if (*expp == NULL)
                return RDB_ERROR;
            (*expp)->def.op.name = RDB_dup_str(name);
            if ((*expp)->def.op.name == NULL) {
                RDB_drop_expr(*expp, ecp);
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            break;
        }
        case RDB_EX_RO_OP:
        {
            char *name;
            RDB_int argc;
            int i;

            ret = _RDB_deserialize_str(valp, posp, ecp, &name);
            if (ret != RDB_OK) {
                return RDB_ERROR;
            }

            ret = _RDB_deserialize_int(valp, posp, ecp, &argc);
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

                ret = _RDB_deserialize_expr(valp, posp, ecp, txp, &argp);
                if (ret != RDB_OK) {
                    return RDB_ERROR;
                }
                RDB_add_arg(*expp, argp);
            }

            /*
             * Restore type if it was stored to preserve
             * the type of e.g. RELATION()
             */
            ret = _RDB_deserialize_byte(valp, posp, ecp);
            if (ret < 0)
                return RDB_ERROR;
            if (ret) {
                RDB_type *typ = _RDB_deserialize_type(valp, posp, ecp, txp);
                if (typ == NULL)
                    return RDB_ERROR;
                RDB_set_expr_type(*expp, typ);
            }
            break;
        }
        case RDB_EX_TUPLE_ATTR:
        {
            char *name;
            
            ret = _RDB_deserialize_expr(valp, posp, ecp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_deserialize_str(valp, posp, ecp, &name);
            if (ret != RDB_OK) {
                RDB_drop_expr(ex1p, ecp);
                return ret;
            }
            *expp = _RDB_create_unexpr(ex1p, RDB_EX_TUPLE_ATTR, ecp);
            if (*expp == NULL)
                return RDB_ERROR;
            (*expp)->def.op.name = RDB_dup_str(name);
            if ((*expp)->def.op.name == NULL) {
                RDB_raise_no_memory(ecp);
                RDB_drop_expr(*expp, ecp);
                return RDB_ERROR;
            }
            break;
        }
    }
    return RDB_OK;
} /* _RDB_deserialize_expr */

RDB_expression *
_RDB_binobj_to_expr(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp;
    int pos = 0;

    if (_RDB_deserialize_expr(valp, &pos, ecp, txp, &exp) != RDB_OK)
        return NULL;
    return exp;
}

RDB_object *
_RDB_binobj_to_vtable(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
	RDB_expression *exp;
    int pos = 0;
    int ret = _RDB_deserialize_expr(valp, &pos, ecp, txp, &exp);
    if (ret != RDB_OK)
        return NULL;

    return RDB_expr_to_vtable(exp, ecp, txp);
}

RDB_object *
deserialize_rtable(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object *tbp = _RDB_new_obj(ecp);
    if (tbp == NULL)
        return NULL;

     ret = deserialize_trobj(valp, posp, ecp, txp, tbp);
     if (ret != RDB_OK)
         return NULL;
     return tbp;
}

static RDB_object *
deserialize_table(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *namp;
    RDB_object *tbp;
    RDB_expression *exp;
    int ret = _RDB_deserialize_str(valp, posp, ecp, &namp);
    if (ret != RDB_OK)
        return NULL;

    if (*namp != '\0') {
        tbp = RDB_get_table(namp, ecp, txp);
        RDB_free(namp);
        return tbp;
    }
    RDB_free(namp);

    ret = _RDB_deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return NULL;
    if (ret) {
        return deserialize_rtable(valp, posp, ecp, txp);
    }
    ret = _RDB_deserialize_expr(valp, posp, ecp, txp, &exp);
    if (ret != RDB_OK)
        return NULL;
    
    return RDB_expr_to_vtable(exp, ecp, txp);
}
