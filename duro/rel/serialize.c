/*
 * $Id$
 *
 * Copyright (C) 2003-2005 Ren� Hartmann.
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
serialize_table(RDB_object *valp, int *posp, RDB_table *tbp, RDB_exec_context *);

static int
serialize_obj(RDB_object *valp, int *posp, const RDB_object *argvalp,
        RDB_exec_context *ecp)
{
    size_t len;
    RDB_bool crtpltyp = RDB_FALSE;
    RDB_type *typ = RDB_obj_type(argvalp);

    if (typ != NULL && typ->kind == RDB_TP_ARRAY) {
        RDB_raise_not_supported("serializing array types is not supported",
                ecp);
        return RDB_ERROR;
    }

    if (typ != NULL && typ->kind == RDB_TP_RELATION) {
        if (serialize_byte(valp, posp, (RDB_byte) TABLE, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
        return serialize_table(valp, posp, argvalp->var.tbp, ecp);
    }

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
    int ret = serialize_byte(valp, posp, (RDB_byte)exp->kind, ecp);
    if (ret != RDB_OK)
        return ret;

    switch(exp->kind) {
        case RDB_EX_OBJ:
            return serialize_obj(valp, posp, &exp->var.obj, ecp);
        case RDB_EX_ATTR:
            return serialize_str(valp, posp, exp->var.attrname, ecp);
        case RDB_EX_GET_COMP:
            ret = serialize_expr(valp, posp, exp->var.op.argv[0], ecp);
            if (ret != RDB_OK)
                return ret;
            return serialize_str(valp, posp, exp->var.op.name, ecp);
        case RDB_EX_AGGREGATE:
            ret = serialize_expr(valp, posp, exp->var.op.argv[0], ecp);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_byte(valp, posp, (RDB_byte) exp->var.op.op, ecp);
            if (ret != RDB_OK)
                return ret;
            return serialize_str(valp, posp, exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            int i;
            int argc = exp->var.op.argc;

            ret = serialize_str(valp, posp, exp->var.op.name, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_int (valp, posp, argc, ecp);
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < argc; i++) {
                ret = serialize_expr(valp, posp, exp->var.op.argv[i], ecp);
                if (ret != RDB_OK)
                    return ret;
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
serialize_project(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.project.tbp, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tuptyp->var.tuple.attrc, ecp);
    if (ret != RDB_OK)
        return ret;
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        ret = serialize_str(valp, posp, tuptyp->var.tuple.attrv[i].name, ecp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}      

static int
serialize_extend(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.extend.tbp, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.extend.attrc, ecp);
    if (ret != RDB_OK)
        return ret;
    for (i = 0; i < tbp->var.extend.attrc; i++) {
        ret = serialize_str(valp, posp, tbp->var.extend.attrv[i].name, ecp);
        if (ret != RDB_OK)
            return ret;
        ret = serialize_expr(valp, posp, tbp->var.extend.attrv[i].exp, ecp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}      

static int
serialize_summarize(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.summarize.tb1p, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_table(valp, posp, tbp->var.summarize.tb2p, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.summarize.addc, ecp);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.summarize.addc; i++) {
        ret = serialize_byte(valp, posp,
                             (RDB_byte)tbp->var.summarize.addv[i].op, ecp);
        if (ret != RDB_OK)
            return ret;

        if (tbp->var.summarize.addv[i].op != RDB_COUNT) {
            ret = serialize_expr(valp, posp, tbp->var.summarize.addv[i].exp, ecp);
            if (ret != RDB_OK)
                return ret;
        }

        ret = serialize_str(valp, posp, tbp->var.summarize.addv[i].name, ecp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}

static int
serialize_rename(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.rename.tbp, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.rename.renc, ecp);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.rename.renc; i++) {
        ret = serialize_str(valp, posp, tbp->var.rename.renv[i].from, ecp);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_str(valp, posp, tbp->var.rename.renv[i].to, ecp);
        if (ret != RDB_OK)
            return ret;
    }

    return RDB_OK;
}

static int
serialize_wrap(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int i, j;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.wrap.tbp, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.wrap.wrapc, ecp);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.wrap.wrapc; i++) {
        ret = serialize_str(valp, posp, tbp->var.wrap.wrapv[i].attrname, ecp);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_int(valp, posp, tbp->var.wrap.wrapv[i].attrc, ecp);
        if (ret != RDB_OK)
            return ret;

        for (j = 0; j < tbp->var.wrap.wrapv[i].attrc; j++) {
            ret = serialize_str(valp, posp, tbp->var.wrap.wrapv[i].attrv[j], ecp);
            if (ret != RDB_OK)
                return ret;
        }
    }

    return RDB_OK;
}

static int
serialize_unwrap(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.unwrap.tbp, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.unwrap.attrc, ecp);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.unwrap.attrc; i++) {
        ret = serialize_str(valp, posp, tbp->var.unwrap.attrv[i], ecp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}

static int
serialize_group(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.group.tbp, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.group.attrc, ecp);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.group.attrc; i++) {
        ret = serialize_str(valp, posp, tbp->var.group.attrv[i], ecp);
        if (ret != RDB_OK)
            return ret;
    }
    ret = serialize_str(valp, posp, tbp->var.group.gattr, ecp);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}

static int
serialize_ungroup(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int ret;

    ret = serialize_table(valp, posp, tbp->var.ungroup.tbp, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_str(valp, posp, tbp->var.ungroup.attr, ecp);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}

static int
serialize_sdivide(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int ret;

    ret = serialize_table(valp, posp, tbp->var.sdivide.tb1p, ecp);
    if (ret != RDB_OK)
        return ret;
    ret = serialize_table(valp, posp, tbp->var.sdivide.tb2p, ecp);
    if (ret != RDB_OK)
        return ret;
    ret = serialize_table(valp, posp, tbp->var.sdivide.tb3p, ecp);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}

static int
serialize_rtable(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_int len;

    if (RDB_table_name(tbp) != NULL) {
        RDB_raise_invalid_argument("cannot serialize named local table", ecp);
        return RDB_ERROR;
    }

    ret = serialize_type(valp, posp, RDB_table_type(tbp), ecp);
    if (ret != RDB_OK)
        return ret;

    /*
     * Store size
     */
    if (_RDB_table_ilen(tbp, &len, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (serialize_size_t(valp, posp, len, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    if (reserve_space(valp, *posp, len, ecp) != RDB_OK)
        return RDB_ERROR;

    _RDB_table_to_irep(((RDB_byte *) valp->var.bin.datap) + *posp, tbp, len);
    *posp += len;
    return RDB_OK;
}

static int
serialize_vtable(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int ret = serialize_byte(valp, posp, (RDB_byte)tbp->kind, ecp);
    if (ret != RDB_OK)
        return ret;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            abort();
        case RDB_TB_SELECT:
            ret = serialize_table(valp, posp, tbp->var.select.tbp, ecp);
            if (ret != RDB_OK)
                return ret;
            return serialize_expr(valp, posp, tbp->var.select.exp, ecp);
        case RDB_TB_UNION:
            ret = serialize_table(valp, posp, tbp->var._union.tb1p, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_table(valp, posp, tbp->var._union.tb2p, ecp);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_MINUS:
            ret = serialize_table(valp, posp, tbp->var.minus.tb1p, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_table(valp, posp, tbp->var.minus.tb2p, ecp);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_INTERSECT:
            ret = serialize_table(valp, posp, tbp->var.intersect.tb1p, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_table(valp, posp, tbp->var.intersect.tb2p, ecp);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_JOIN:
            ret = serialize_table(valp, posp, tbp->var.join.tb1p, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_table(valp, posp, tbp->var.join.tb2p, ecp);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_EXTEND:
            return serialize_extend(valp, posp, tbp, ecp);
        case RDB_TB_PROJECT:
            return serialize_project(valp, posp, tbp, ecp);
        case RDB_TB_SUMMARIZE:
            return serialize_summarize(valp, posp, tbp, ecp);
        case RDB_TB_RENAME:
            return serialize_rename(valp, posp, tbp, ecp);
        case RDB_TB_WRAP:
            return serialize_wrap(valp, posp, tbp, ecp);
        case RDB_TB_UNWRAP:
            return serialize_unwrap(valp, posp, tbp, ecp);
        case RDB_TB_GROUP:
            return serialize_group(valp, posp, tbp, ecp);
        case RDB_TB_UNGROUP:
            return serialize_ungroup(valp, posp, tbp, ecp);
        case RDB_TB_SDIVIDE:
            return serialize_sdivide(valp, posp, tbp, ecp);
    }
    abort();
}


static int
serialize_table(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    if (tbp->is_persistent)
        return serialize_str(valp, posp, RDB_table_name(tbp), ecp);
    if (serialize_str(valp, posp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    if (tbp->kind == RDB_TB_REAL) {
        if (serialize_byte(valp, posp, (RDB_byte)tbp->kind, ecp) != RDB_OK)
            return RDB_ERROR;
        return serialize_rtable(valp, posp, tbp, ecp);
    }
    return serialize_vtable(valp, posp, tbp, ecp);
}

enum {
    RDB_BUF_INITLEN = 256
};

int
_RDB_vtable_to_binobj(RDB_object *valp, RDB_table *tbp, RDB_exec_context *ecp)
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
    ret = serialize_vtable(valp, &pos, tbp, ecp);
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

static RDB_table *
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
        RDB_table *tbp;

        tbp = deserialize_table(valp, posp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        RDB_table_to_obj(argvalp, tbp, ecp);
        return RDB_OK;
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

    ret = RDB_irep_to_obj(argvalp, typ, ((RDB_byte *)valp->var.bin.datap) + *posp,
            len, ecp);
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
                   RDB_raise_no_memory(ecp);
                   return RDB_ERROR;
               }
            }
            break;
        case RDB_EX_ATTR:
            {
                char *attrnamp;
            
                ret = deserialize_str(valp, posp, ecp, &attrnamp);
                if (ret != RDB_OK)
                    return ret;

                *expp = RDB_expr_attr(attrnamp, ecp);
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
        case RDB_EX_AGGREGATE:
        {
            RDB_expression *exp;
            RDB_aggregate_op op;
            char *name;

            ret = deserialize_expr(valp, posp, ecp, txp, &exp);
            if (ret != RDB_OK)
                return ret;

            ret = deserialize_byte(valp, posp, ecp);
            if (ret < 0)
                return ret;
            op = (RDB_aggregate_op) ret;

            ret = deserialize_str(valp, posp, ecp, &name);
            if (ret != RDB_OK) {
                return ret;
            }

            *expp = RDB_expr_aggregate(exp, op, name, ecp);
            if (*expp == NULL)
                return ret;
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

RDB_table *
deserialize_project(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_table *tbp;
    RDB_int ac;
    char **av;
    int i;
    int ret;

    tbp = deserialize_table(valp, posp, ecp, txp);
    if (tbp == NULL)
        return NULL;
    ret = deserialize_int(valp, posp, ecp, &ac);
    if (ret != RDB_OK)
        return NULL;
    av = malloc(ac * sizeof(char *));
    if (av == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < ac; i++)
       av[i] = NULL;
    for (i = 0; i < ac; i++) {
        ret = deserialize_str(valp, posp, ecp, av + i);
        if (ret != RDB_OK)
            goto error;
    }
    tbp = RDB_project(tbp, ac, av, ecp);
    if (tbp == NULL)
        goto error;
    RDB_free_strvec(ac, av);
    return tbp;

error:
    RDB_free_strvec(ac, av);
    return NULL;
}

RDB_table *
deserialize_extend(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_table *tbp;
    RDB_int ac;
    RDB_virtual_attr *av;
    int i;
    int ret;

    tbp = deserialize_table(valp, posp, ecp, txp);
    if (tbp == NULL)
        return NULL;
    ret = deserialize_int(valp, posp, ecp, &ac);
    av = malloc(ac * sizeof(RDB_virtual_attr));
    if (av == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < ac; i++) {
        av[i].name = NULL;
    }
    for (i = 0; i < ac; i++) {
        ret = deserialize_str(valp, posp, ecp, &av[i].name);
        if (ret != RDB_OK)
            goto error;
        ret = deserialize_expr(valp, posp, ecp, txp, &av[i].exp);
        if (ret != RDB_OK)
            goto error;
    }
    tbp = RDB_extend(tbp, ac, av, ecp, txp);
    if (tbp == NULL)
        goto error;
    for (i = 0; i < ac; i++) {
        free(av[i].name);
    }
    free(av);
    return tbp;

error:
    for (i = 0; i < ac; i++) {
        free(av[i].name);
    }
    free(av);
    return NULL;
}

RDB_table *
deserialize_summarize(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_table *tb1p;
    RDB_table *tb2p;
    RDB_table *rtbp;
    RDB_summarize_add *addv;
    RDB_int addc;
    int ret;
    int i;

    tb1p = deserialize_table(valp, posp, ecp, txp);
    if (tb1p == NULL)
        return NULL;

    tb2p = deserialize_table(valp, posp, ecp, txp);
    if (tb2p == NULL)
        return NULL;

    ret = deserialize_int(valp, posp, ecp, &addc);
    if (ret != RDB_OK)
        return NULL;

    addv = malloc(addc * sizeof (RDB_summarize_add));
    if (addv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < addc; i++) {
        addv[i].exp = NULL;
        addv[i].name = NULL;
    }

    for (i = 0; i < addc; i++) {
        ret = deserialize_byte(valp, posp, ecp);
        if (ret < 0)
            goto error;
        addv[i].op = (RDB_aggregate_op)ret;
        if (addv[i].op != RDB_COUNT) {
            ret = deserialize_expr(valp, posp, ecp, txp, &addv[i].exp);
            if (ret != RDB_OK)
                goto error;
        } else {
            addv[i].exp = NULL;
        }
        ret = deserialize_str(valp, posp, ecp, &addv[i].name);
        if (ret != RDB_OK)
            goto error;
    }

    rtbp = RDB_summarize(tb1p, tb2p, addc, addv, ecp, txp);
    if (rtbp == NULL) {
        goto error;
    }

    for (i = 0; i < addc; i++) {
        free(addv[i].name);
    }
    free(addv);

    return rtbp;

error:
    if (ret != RDB_OK) {
        if (RDB_table_name(tb1p) == NULL)
            RDB_drop_table(tb1p, ecp, NULL);
        if (RDB_table_name(tb2p) == NULL)
            RDB_drop_table(tb2p, ecp, NULL);
    }

    for (i = 0; i < addc; i++) {
        if (addv[i].exp != NULL)
            RDB_drop_expr(addv[i].exp, ecp);
        free(addv[i].name);
    }
    free(addv);
    return NULL;
}

RDB_table *
deserialize_rename(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_table *tbp;
    RDB_int renc;
    RDB_renaming *renv;
    int ret;
    int i;

    tbp = deserialize_table(valp, posp, ecp, txp);
    if (tbp == NULL)
        return NULL;

    ret = deserialize_int(valp, posp, ecp, &renc);
    if (ret != RDB_OK)
        return NULL;

    renv = malloc(renc * sizeof (RDB_renaming));
    if (renv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < renc; i++) {
        renv[i].from = NULL;
        renv[i].to = NULL;
    }

    for (i = 0; i < renc; i++) {
        ret = deserialize_str(valp, posp, ecp, &renv[i].from);
        if (ret != RDB_OK) {
            tbp = NULL;
            goto cleanup;
        }
        ret = deserialize_str(valp, posp, ecp, &renv[i].to);
        if (ret != RDB_OK) {
            tbp = NULL;
            goto cleanup;
        }
    }

    tbp = RDB_rename(tbp, renc, renv, ecp);

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    for (i = 0; i < renc; i++) {
        free(renv[i].from);
        free(renv[i].to);
    }
    free(renv);

    return tbp;
}

RDB_table *
deserialize_wrap(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i, j;
    int ret;
    RDB_table *tbp;
    int wrapc;
    RDB_wrapping *wrapv = NULL;
    RDB_table *rtbp = NULL;

    tbp = deserialize_table(valp, posp, ecp, txp);
    if (tbp == NULL)
        return NULL;

    ret = deserialize_int(valp, posp, ecp, &wrapc);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    wrapv = malloc(wrapc * sizeof (RDB_wrapping));
    if (wrapv == NULL)
        goto cleanup;

    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrname = NULL;
        wrapv[i].attrv = NULL;
    }
    for (i = 0; i < wrapc; i++) {
        ret = deserialize_str(valp, posp, ecp, &wrapv[i].attrname);
        if (ret != RDB_OK)
            goto cleanup;

        ret = deserialize_int(valp, posp, ecp, &wrapv[i].attrc);
        if (ret != RDB_OK)
            goto cleanup;
        for (j = 0; j < wrapv[i].attrc; j++)
            wrapv[j].attrv[j] = NULL;
        for (j = 0; j < wrapv[i].attrc; j++) {
            ret = deserialize_str(valp, posp, ecp, &wrapv[i].attrv[j]);
            if (ret != RDB_OK)
                goto cleanup;
        }
    }
     
    rtbp = RDB_wrap(tbp, wrapc, wrapv, ecp);

cleanup:
    if (rtbp == NULL && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    if (wrapv != NULL) {
        for (i = 0; i < wrapc; i++) {
            free(wrapv[i].attrname);
            if (wrapv[i].attrv != NULL) {
                for (j = 0; j < wrapv[i].attrc; i++)
                    free(wrapv[i].attrv[j]);
            }
        }
    }

    return rtbp;
}

RDB_table *
deserialize_unwrap(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_table *tbp;
    int attrc;
    char **attrv;
    int ret;
    int i;
    RDB_table *rtbp = NULL;

    tbp = deserialize_table(valp, posp, ecp, txp);
    if (tbp == NULL)
        return NULL;

    ret = deserialize_int(valp, posp, ecp, &attrc);
    if (ret != RDB_OK)
        return NULL;

    attrv = malloc(attrc * sizeof (char *));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < attrc; i++) {
        attrv[i] = NULL;
    }

    for (i = 0; i < attrc; i++) {
        ret = deserialize_str(valp, posp, ecp, &attrv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }

    rtbp = RDB_unwrap(tbp, attrc, attrv, ecp);

cleanup:
    if (rtbp == NULL && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    for (i = 0; i < attrc; i++) {
        free(attrv[i]);
    }
    free(attrv);

    return rtbp;
}

RDB_table *
deserialize_group(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_table *tbp;
    int attrc;
    RDB_table *rtbp;
    char **attrv = NULL;
    char *gattr = NULL;

    tbp = deserialize_table(valp, posp, ecp, txp);
    if (tbp == NULL)
        return NULL;

    ret = deserialize_int(valp, posp, ecp, &attrc);
    if (ret != RDB_OK)
        goto cleanup;

    attrv = malloc(attrc * sizeof (char *));
    if (attrv == NULL)
        goto cleanup;

    for (i = 0; i < attrc; i++)
        attrv[i] = NULL;

    for (i = 0; i < attrc; i++) {
        ret = deserialize_str(valp, posp, ecp, &attrv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = deserialize_str(valp, posp, ecp, &gattr);
    if (ret != RDB_OK)
        goto cleanup;

    rtbp = RDB_group(tbp, attrc, attrv, gattr, ecp);

cleanup:
    if (rtbp == NULL && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    if (attrv != NULL) {
        for (i = 0; i < attrc; i++)
            free(attrv[i]);
    }
    free(gattr);

    return rtbp;
}

RDB_table *
deserialize_ungroup(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_table *tbp;
    char *attrname = NULL;
    int ret;
    RDB_table *rtbp = NULL;

    tbp = deserialize_table(valp, posp, ecp, txp);
    if (tbp == NULL)
        return NULL;

    ret = deserialize_str(valp, posp, ecp, &attrname);
    if (ret != RDB_OK)
        goto cleanup;

    rtbp = RDB_ungroup(tbp, attrname, ecp);

cleanup:
    if (rtbp == NULL && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    if (attrname != NULL)
        free(attrname);

    return rtbp;
}

RDB_table *
deserialize_sdivide(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_table *tb1p, *tb2p, *tb3p;
    RDB_table *rtbp;

    tb1p = deserialize_table(valp, posp, ecp, txp);
    if (tb1p == NULL)
        return NULL;

    tb2p = deserialize_table(valp, posp, ecp, txp);
    if (tb2p == NULL)
        goto cleanup;

    tb3p = deserialize_table(valp, posp, ecp, txp);
    if (tb3p == NULL)
        goto cleanup;

    rtbp = RDB_sdivide(tb1p, tb2p, tb3p, ecp);

cleanup:
    if (rtbp == NULL) {
        if (RDB_table_name(tb1p) == NULL)
            RDB_drop_table(tb1p, ecp, NULL);
        if (RDB_table_name(tb2p) == NULL)
            RDB_drop_table(tb2p, ecp, NULL);
        if (RDB_table_name(tb3p) == NULL)
            RDB_drop_table(tb3p, ecp, NULL);
    }

    return rtbp;
}

static RDB_table *
deserialize_vtable(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_table *tb1p, *tb2p;
    RDB_expression *exprp;

    ret = deserialize_byte(valp, posp, ecp);
    if (ret < 0)
        return NULL;
    switch ((enum _RDB_tb_kind) ret) {
        case RDB_TB_REAL:
            abort();
        case RDB_TB_SELECT:
            tb1p = deserialize_table(valp, posp, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            ret = deserialize_expr(valp, posp, ecp, txp, &exprp);
            if (ret != RDB_OK)
                goto error;
            return RDB_select(tb1p, exprp, ecp, txp);
        case RDB_TB_UNION:
            tb1p = deserialize_table(valp, posp, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = deserialize_table(valp, posp, ecp, txp);
            if (tb2p == NULL)
                return NULL;
            return RDB_union(tb1p, tb2p, ecp);
        case RDB_TB_MINUS:
            tb1p = deserialize_table(valp, posp, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = deserialize_table(valp, posp, ecp, txp);
            if (tb2p == NULL)
                return NULL;
            return RDB_minus(tb1p, tb2p, ecp);
        case RDB_TB_INTERSECT:
            tb1p = deserialize_table(valp, posp, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = deserialize_table(valp, posp, ecp, txp);
            if (tb2p == NULL)
                return NULL;
            return RDB_intersect(tb1p, tb2p, ecp);
        case RDB_TB_JOIN:
            tb1p = deserialize_table(valp, posp, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = deserialize_table(valp, posp, ecp, txp);
            if (tb2p == NULL)
                return NULL;
            return RDB_join(tb1p, tb2p, ecp);
        case RDB_TB_EXTEND:
            return deserialize_extend(valp, posp, ecp, txp);
        case RDB_TB_PROJECT:
            return deserialize_project(valp, posp, ecp, txp);
        case RDB_TB_SUMMARIZE:
            return deserialize_summarize(valp, posp, ecp, txp);
        case RDB_TB_RENAME:
            return deserialize_rename(valp, posp, ecp, txp);
        case RDB_TB_WRAP:
            return deserialize_wrap(valp, posp, ecp, txp);
        case RDB_TB_UNWRAP:
            return deserialize_unwrap(valp, posp, ecp, txp);
        case RDB_TB_GROUP:
            return deserialize_group(valp, posp, ecp, txp);
        case RDB_TB_UNGROUP:
            return deserialize_ungroup(valp, posp, ecp, txp);
        case RDB_TB_SDIVIDE:
            return deserialize_sdivide(valp, posp, ecp, txp);
    }
    abort();

error:
    return NULL;
}

RDB_table *
_RDB_binobj_to_vtable(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int pos = 0;

    return deserialize_vtable(valp, &pos, ecp, txp);
}

RDB_table *
deserialize_rtable(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *typ;
    RDB_int len;
    int ret;
    RDB_table *tbp;

    typ = deserialize_type(valp, posp, ecp, txp);
    if (typ == NULL)
        return NULL;

    if (deserialize_size_t(valp, posp, ecp, &len) != RDB_OK)
        return NULL;

    ret = _RDB_irep_to_table(&tbp, typ,
            ((RDB_byte *)valp->var.bin.datap) + *posp, len, ecp);
    if (ret != RDB_OK)
        return NULL;
    *posp += len;
    return tbp;
}

static RDB_table *
deserialize_table(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *namp;
    RDB_table *tbp;
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
    if (((enum _RDB_tb_kind) ret) ==  RDB_TB_REAL) {
        return deserialize_rtable(valp, posp, ecp, txp);
    }
    (*posp)--;
    return deserialize_vtable(valp, posp, ecp, txp);
}
