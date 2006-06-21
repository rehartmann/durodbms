/*
 * $Id$
 *
 * Copyright (C) 2003-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "serialize.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <gen/hashtabit.h>
#include <string.h>
#include <assert.h>

enum {
    TABLE = 127
};

/*
 * Functions for serializing/deserializing - needed for
 * persistent virtual tables (aka views)
 */

static int
reserve_space(RDB_object *valp, int pos, size_t n, RDB_exec_context *ecp)
{
    while (valp->var.bin.len < pos + n) {
        int newlen = valp->var.bin.len * 2;
        void *newdatap = realloc(valp->var.bin.datap, newlen);
        
        if (newdatap == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        valp->var.bin.len = newlen;
        valp->var.bin.datap = newdatap;
    }

    return RDB_OK;
}

static int
serialize_str(RDB_object *valp, int *posp, const char *str,
        RDB_exec_context *ecp)
{
    size_t len = strlen(str);

    if (reserve_space(valp, *posp, len + sizeof len, ecp) != RDB_OK)
        return RDB_ERROR;

    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &len, sizeof len);
    *posp += sizeof len;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, str, len);
    *posp += len;
    return RDB_OK;
}

static int
serialize_int(RDB_object *valp, int *posp, RDB_int v, RDB_exec_context *ecp)
{
    int ret;

    ret = reserve_space(valp, *posp, sizeof v, ecp);
    if (ret != RDB_OK)
        return ret;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &v, sizeof v);
    *posp += sizeof v;
    return RDB_OK;
}

static int
serialize_size_t(RDB_object *valp, int *posp, size_t v, RDB_exec_context *ecp)
{
    int ret;

    ret = reserve_space(valp, *posp, sizeof v, ecp);
    if (ret != RDB_OK)
        return ret;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &v, sizeof v);
    *posp += sizeof v;
    return RDB_OK;
}

static int
serialize_byte(RDB_object *valp, int *posp, RDB_byte b, RDB_exec_context *ecp)
{
    int ret;

    ret = reserve_space(valp, *posp, 1, ecp);
    if (ret != RDB_OK)
        return ret;
    ((RDB_byte *) valp->var.bin.datap)[(*posp)++] = b;
    return RDB_OK;
}

static int
serialize_type(RDB_object *valp, int *posp, const RDB_type *typ,
        RDB_exec_context *ecp)
{
    int ret;
    
    ret = serialize_byte(valp, posp, (RDB_byte) typ->kind, ecp);
    if (ret != RDB_OK)
        return ret;

    switch (typ->kind) {
        case RDB_TP_SCALAR:
            return serialize_str(valp, posp, typ->name, ecp);
        case RDB_TP_TUPLE:
        {
            int i;

            ret = serialize_int(valp, posp, typ->var.tuple.attrc, ecp);
            if (ret != RDB_OK)
                return ret;

            for (i = 0; i < typ->var.tuple.attrc; i++) {
                ret = serialize_str(valp, posp, typ->var.tuple.attrv[i].name, ecp);
                if (ret != RDB_OK)
                    return ret;

                ret = serialize_type(valp, posp, typ->var.tuple.attrv[i].typ, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            return RDB_OK;
        }
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            return serialize_type(valp, posp, typ->var.basetyp, ecp);
    }
    abort();
}

static int
serialize_table(RDB_object *valp, int *posp, RDB_object *tbp, RDB_exec_context *);

static int
serialize_obj(RDB_object *valp, int *posp, const RDB_object *argvalp,
        RDB_exec_context *ecp)
{
    size_t len;
    RDB_bool crtpltyp = RDB_FALSE;
    RDB_type *typ = RDB_obj_type(argvalp);

/*
    if (typ != NULL && typ->kind == RDB_TP_RELATION) {
        abort();
        if (serialize_byte(valp, posp, (RDB_byte) TABLE, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
        return serialize_table(valp, posp, (RDB_object *) argvalp, ecp);
    }
        */

    if (typ == NULL) {
        assert(argvalp->kind == RDB_OB_TUPLE);

        typ = _RDB_tuple_type(argvalp, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        crtpltyp = RDB_TRUE;
        ((RDB_object *)argvalp)->typ = typ;
    }
    if (serialize_type(valp, posp, typ, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    len = typ != NULL ? typ->ireplen : RDB_VARIABLE_LEN;
    if (len == RDB_VARIABLE_LEN) {
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

    _RDB_obj_to_irep(((RDB_byte *) valp->var.bin.datap) + *posp, argvalp, len);
    *posp += len;
    if (crtpltyp) {
        RDB_drop_type(typ, ecp, NULL);
        ((RDB_object *)argvalp)->typ = NULL;
    }
    return RDB_OK;
}

static int
serialize_expr(RDB_object *valp, int *posp, const RDB_expression *exp,
        RDB_exec_context *ecp)
{
    int ret = serialize_byte(valp, posp, (RDB_byte) exp->kind, ecp);
    if (ret != RDB_OK)
        return ret;

    switch(exp->kind) {
        case RDB_EX_OBJ:
            return serialize_obj(valp, posp, &exp->var.obj, ecp);
        case RDB_EX_TBP:
            return serialize_table(valp, posp, exp->var.tbp, ecp);
        case RDB_EX_VAR:
            return serialize_str(valp, posp, exp->var.varname, ecp);
        case RDB_EX_GET_COMP:
            ret = serialize_expr(valp, posp, exp->var.op.argv[0], ecp);
            if (ret != RDB_OK)
                return ret;
            return serialize_str(valp, posp, exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            int i;
            int argc = exp->var.op.argc;

            ret = serialize_str(valp, posp, exp->var.op.name, ecp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            ret = serialize_int (valp, posp, argc, ecp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            for (i = 0; i < argc; i++) {
                ret = serialize_expr(valp, posp, exp->var.op.argv[i], ecp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
            }
            return RDB_OK;
        }
        case RDB_EX_TUPLE_ATTR:
            ret = serialize_expr(valp, posp, exp->var.op.argv[0], ecp);
            if (ret != RDB_OK)
                return ret;
            return serialize_str(valp, posp, exp->var.op.name, ecp);
    }
    /* should never be reached */
    abort();
}

static int
serialize_rtable(RDB_object *valp, int *posp, RDB_object *tbp,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_int len;

    if (RDB_table_name(tbp) != NULL) {
        RDB_raise_invalid_argument("cannot serialize named local table", ecp);
        return RDB_ERROR;
    }

    ret = serialize_type(valp, posp, RDB_obj_type(tbp), ecp);
    if (ret != RDB_OK)
        return ret;

    /*
     * Store size
     */
    if (_RDB_obj_ilen(tbp, &len, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (serialize_size_t(valp, posp, len, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    if (reserve_space(valp, *posp, len, ecp) != RDB_OK)
        return RDB_ERROR;

    _RDB_obj_to_irep(((RDB_byte *) valp->var.bin.datap) + *posp, tbp, len);
    *posp += len;
    return RDB_OK;
}

static int
serialize_table(RDB_object *valp, int *posp, RDB_object *tbp,
        RDB_exec_context *ecp)
{
    if (tbp->var.tb.is_persistent)
        return serialize_str(valp, posp, RDB_table_name(tbp), ecp);
    if (serialize_str(valp, posp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    if (tbp->var.tb.exp == NULL) {
        if (serialize_byte(valp, posp, (RDB_byte)1, ecp) != RDB_OK)
            return RDB_ERROR;
        return serialize_rtable(valp, posp, tbp, ecp);
    }
    if (serialize_byte(valp, posp, (RDB_byte) 0, ecp) != RDB_OK)
        return RDB_ERROR;
    return serialize_expr(valp, posp, tbp->var.tb.exp, ecp);
}

enum {
    RDB_BUF_INITLEN = 256
};

int
_RDB_vtable_to_binobj(RDB_object *valp, RDB_object *tbp, RDB_exec_context *ecp)
{
    int pos;
    int ret;

    RDB_destroy_obj(valp, ecp);
    valp->typ = &RDB_BINARY;
    valp->kind = RDB_OB_BIN;
    valp->var.bin.len = RDB_BUF_INITLEN;
    valp->var.bin.datap = malloc(RDB_BUF_INITLEN);
    if (valp->var.bin.datap == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    pos = 0;
    ret = serialize_expr(valp, &pos, tbp->var.tb.exp, ecp);
    if (ret != RDB_OK)
        return ret;

    valp->var.bin.len = pos; /* Only store actual length */
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
    valp->var.bin.len = RDB_BUF_INITLEN;
    valp->var.bin.datap = malloc(RDB_BUF_INITLEN);
    if (valp->var.bin.datap == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    pos = 0;
    ret = serialize_type(valp, &pos, typ, ecp);
    if (ret != RDB_OK)
        return ret;

    valp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
_RDB_expr_to_binobj(RDB_object *valp, const RDB_expression *exp,
        RDB_exec_context *ecp)
{
    int pos = 0;

    RDB_destroy_obj(valp, ecp);
    valp->typ = &RDB_BINARY;
    valp->kind = RDB_OB_BIN;
    if (exp != NULL) {
        valp->var.bin.len = RDB_BUF_INITLEN;
        valp->var.bin.datap = malloc(RDB_BUF_INITLEN);
        if (valp->var.bin.datap == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        if (serialize_expr(valp, &pos, exp, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    } else {
        valp->var.bin.datap = NULL;
    }

    valp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

static int
deserialize_str(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        char **strp)
{
    size_t len;
    
    if (*posp + sizeof (len) > valp->var.bin.len) {
        RDB_raise_internal("invalid string during deserialization", ecp);
        return RDB_ERROR;
    }
    memcpy (&len, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof len);
    *posp += sizeof len;
    if (*posp + len > valp->var.bin.len) {
        RDB_raise_internal("invalid string during deserialization", ecp);
        return RDB_ERROR;
    }
    *strp = malloc(len + 1);
    if (*strp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    memcpy(*strp, ((RDB_byte *)valp->var.bin.datap) + *posp, len);
    (*strp)[len] = '\0';
    *posp += len;

    return RDB_OK;
}

static int
deserialize_int(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_int *vp)
{
    if (*posp + sizeof (RDB_int) > valp->var.bin.len) {
        RDB_raise_internal("invalid integer during deserialization", ecp);
        return RDB_ERROR;
    }
    memcpy (vp, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof (RDB_int));
    *posp += sizeof (RDB_int);
    return RDB_OK;
}

static int
deserialize_size_t(const RDB_object *valp, int *posp, RDB_exec_context *ecp,
        size_t *vp)
{
    if (*posp + sizeof (RDB_int) > valp->var.bin.len) {
        RDB_raise_internal("invalid integer during deserialization", ecp);
        return RDB_ERROR;
    }
    memcpy (vp, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof (size_t));
    *posp += sizeof (RDB_int);
    return RDB_OK;
}

static int
deserialize_byte(RDB_object *valp, int *posp, RDB_exec_context *ecp)
{
    if (*posp + 1 > valp->var.bin.len) {
        RDB_raise_internal("invalid byte during deserialization", ecp);
        return RDB_ERROR;
    }

    return ((RDB_byte *) valp->var.bin.datap)[(*posp)++];
}

static RDB_type *
deserialize_type(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *namp;
    int ret;
    enum _RDB_tp_kind kind;
    RDB_type *typ;

    ret = deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return NULL;
    kind = (enum _RDB_tp_kind ) ret;

    switch (kind) {
        case RDB_TP_SCALAR:
            ret = deserialize_str(valp, posp, ecp, &namp);
            if (ret != RDB_OK)
                return NULL;
            typ = RDB_get_type(namp, ecp, txp);
            free(namp);
            return typ;
        case RDB_TP_TUPLE:
        {
            RDB_int attrc;
            int i;

            ret = deserialize_int(valp, posp, ecp, &attrc);
            typ = (RDB_type *) malloc(sizeof (RDB_type));
            if (typ == NULL) {
                RDB_raise_no_memory(ecp);
                return NULL;
            }
            typ->name = NULL;
            typ->kind = RDB_TP_TUPLE;
            typ->ireplen = RDB_VARIABLE_LEN;

            typ->var.tuple.attrv = malloc(sizeof(RDB_attr) * attrc);
            if (typ->var.tuple.attrv == NULL) {
                free(typ);
                RDB_raise_no_memory(ecp);
                return NULL;
            }
            for (i = 0; i < attrc; i++) {
                ret = deserialize_str(valp, posp, ecp,
                        &typ->var.tuple.attrv[i].name);
                if (ret != RDB_OK) {
                    free(typ->var.tuple.attrv);
                    free(typ);
                }
                typ->var.tuple.attrv[i].typ = deserialize_type(valp, posp,
                        ecp, txp);
                if (typ->var.tuple.attrv[i].typ == NULL) {
                    free(typ->var.tuple.attrv);
                    free(typ);
                    return NULL;
                }
                typ->var.tuple.attrv[i].defaultp = NULL;
            }
            typ->var.tuple.attrc = attrc;
            return typ;
        }
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            typ = (RDB_type *) malloc(sizeof (RDB_type));
            if (typ == NULL) {
                RDB_raise_no_memory(ecp);
                return NULL;
            }
            typ->name = NULL;
            typ->kind = kind;
            typ->ireplen = RDB_VARIABLE_LEN;

            typ->var.basetyp = deserialize_type(valp, posp, ecp, txp);
            if (typ->var.basetyp == NULL) {
                free(typ);
                return NULL;
            }
            return typ;
    }
    RDB_raise_internal("invalid type during deserialization", ecp);
    return NULL;
}

RDB_type *
_RDB_binobj_to_type(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int pos = 0;

    return deserialize_type(valp, &pos, ecp, txp);
}

static RDB_object *
deserialize_table(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp);

static int
deserialize_obj(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *argvalp)
{
    RDB_type *typ;
    size_t len;
    int ret;

    ret = deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return ret;

    if (ret == TABLE) {
        abort(); /*
        RDB_object *tbp;
        tbp = deserialize_table(valp, posp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        RDB_table_to_obj(argvalp, tbp, ecp);
        return RDB_OK;
        */
    }

    /* 1 byte back */
    (*posp)--;

    typ = deserialize_type(valp, posp, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    len = typ->ireplen;
    if (len == RDB_VARIABLE_LEN) {
        if (deserialize_size_t(valp, posp, ecp, &len) != RDB_OK)
            return RDB_ERROR;
    }

    ret = RDB_irep_to_obj(argvalp, typ,
            ((RDB_byte *)valp->var.bin.datap) + *posp, len, ecp);
    *posp += len;

    if (typ->kind == RDB_TP_TUPLE) {
        RDB_drop_type(typ, ecp, NULL);
        argvalp->typ = NULL;
    }
    return ret;
}

static int
deserialize_expr(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_expression **expp)
{
    RDB_expression *ex1p;
    enum _RDB_expr_kind ekind;
    int ret;

    if (valp->var.bin.len == 0) {
        *expp = NULL;
        return RDB_OK;
    }

    ret = deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return ret;
    ekind = (enum _RDB_expr_kind) ret;
    switch (ekind) {
        case RDB_EX_OBJ:
            {
               RDB_object val;

               RDB_init_obj(&val);
               ret = deserialize_obj(valp, posp, ecp, txp, &val);
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
               	*expp = RDB_table_ref_to_expr(tbp, ecp);
               	if (*expp == NULL)
               	    return RDB_ERROR;
            }
            break;
        case RDB_EX_VAR:
            {
                char *attrnamp;
            
                ret = deserialize_str(valp, posp, ecp, &attrnamp);
                if (ret != RDB_OK)
                    return ret;

                *expp = RDB_expr_var(attrnamp, ecp);
                free(attrnamp);
                if (*expp == NULL)
                    return RDB_ERROR;
            }
            break;
        case RDB_EX_GET_COMP:
        {
            char *name;
            
            ret = deserialize_expr(valp, posp, ecp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_str(valp, posp, ecp, &name);
            if (ret != RDB_OK) {
                RDB_drop_expr(ex1p, ecp);
                return ret;
            }
            *expp = _RDB_create_unexpr(ex1p, RDB_EX_GET_COMP, ecp);
            if (*expp == NULL)
                return RDB_ERROR;
            (*expp)->var.op.name = RDB_dup_str(name);
            if ((*expp)->var.op.name == NULL) {
                RDB_drop_expr(*expp, ecp);
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            break;
        }
        case RDB_EX_RO_OP:
        {
            char *name;
            int argc;
            int i;
            RDB_expression **argv;
        
            ret = deserialize_str(valp, posp, ecp, &name);
            if (ret != RDB_OK) {
                return ret;
            }

            ret = deserialize_int(valp, posp, ecp, &argc);
            if (ret != RDB_OK)
                return ret;

            argv = malloc(argc * sizeof (RDB_expression *));
            if (argv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }

            for (i = 0; i < argc; i++) {
                ret = deserialize_expr(valp, posp, ecp, txp, &argv[i]);
                if (ret != RDB_OK) {
                    return RDB_ERROR;
                }
            }
            *expp = RDB_ro_op(name, argc, argv, ecp);
            free(argv);
            if (*expp == NULL)
                return RDB_ERROR;
            break;
        }
        case RDB_EX_TUPLE_ATTR:
        {
            char *name;
            
            ret = deserialize_expr(valp, posp, ecp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_str(valp, posp, ecp, &name);
            if (ret != RDB_OK) {
                RDB_drop_expr(ex1p, ecp);
                return ret;
            }
            *expp = _RDB_create_unexpr(ex1p, RDB_EX_TUPLE_ATTR, ecp);
            if (*expp == NULL)
                return RDB_ERROR;
            (*expp)->var.op.name = RDB_dup_str(name);
            if ((*expp)->var.op.name == NULL) {
                RDB_raise_no_memory(ecp);
                RDB_drop_expr(*expp, ecp);
                return RDB_ERROR;
            }
            break;
        }
    }
    return RDB_OK;
}

RDB_expression *
_RDB_binobj_to_expr(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp;
    int pos = 0;

    if (deserialize_expr(valp, &pos, ecp, txp, &exp) != RDB_OK)
        return NULL;
    return exp;
}

RDB_object *
_RDB_binobj_to_vtable(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
	RDB_expression *exp;
    int pos = 0;
    int ret = deserialize_expr(valp, &pos, ecp, txp, &exp);
    if (ret != RDB_OK)
        return NULL;

    return _RDB_expr_to_vtable(exp, ecp, txp);
}

RDB_object *
deserialize_rtable(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *typ;
    RDB_int len;
    int ret;
    RDB_object *tbp;

    typ = deserialize_type(valp, posp, ecp, txp);
    if (typ == NULL)
        return NULL;

    if (deserialize_size_t(valp, posp, ecp, &len) != RDB_OK)
        return NULL;

    tbp = _RDB_new_obj(ecp);
    if (tbp == NULL)
        return NULL;

    ret = _RDB_irep_to_table(tbp, typ,
            ((RDB_byte *)valp->var.bin.datap) + *posp, len, ecp);
    if (ret != RDB_OK) {
    	free(tbp);
        return NULL;
    }
    *posp += len;
    return tbp;
}

static RDB_object *
deserialize_table(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *namp;
    RDB_object *tbp;
    RDB_expression *exp;
    int ret = deserialize_str(valp, posp, ecp, &namp);
    if (ret != RDB_OK)
        return NULL;

    if (*namp != '\0') {
        tbp = RDB_get_table(namp, ecp, txp);
        free(namp);
        return tbp;
    }
    free(namp);

    ret = deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return NULL;
    if (ret) {
        return deserialize_rtable(valp, posp, ecp, txp);
    }
    ret = deserialize_expr(valp, posp, ecp, txp, &exp);
    if (ret != RDB_OK)
        return NULL;
    
    return _RDB_expr_to_vtable(exp, ecp, txp);
}
