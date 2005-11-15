/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "serialize.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>
#include <gen/hashtabit.h>

/*
 * Functions for serializing/deserializing - needed for
 * persistent virtual tables (views)
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
    int ret;
    size_t len = strlen(str);

    ret = reserve_space(valp, *posp, len + sizeof len, ecp);
    if (ret != RDB_OK)
        return ret;
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
    void *datap;
    int ret;

    ret = serialize_byte(valp, posp, argvalp->kind, ecp);
    if (ret != RDB_OK)
        return ret;

    switch (argvalp->kind) {
        case RDB_OB_TABLE:
            return serialize_table(valp, posp, argvalp->var.tbp, ecp);
        case RDB_OB_TUPLE:
        {
            tuple_entry *entryp;
            RDB_hashtable_iter hiter;
        
            ret = serialize_int(valp, posp, RDB_tuple_size(argvalp), ecp);
            if (ret != RDB_OK)
                return ret;

            RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &argvalp->var.tpl_tab);
            while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
                /* Write attribute name */
                ret = serialize_str(valp, posp, entryp->key, ecp);
                if (ret != RDB_OK)
                    return ret;

                /* Write attribute value */
                ret = serialize_obj(valp, posp, &entryp->obj, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_destroy_hashtable_iter(&hiter);
            return RDB_OK;
        }
        case RDB_OB_ARRAY:
            RDB_raise_not_supported("cannot serialize array", ecp);
            return RDB_ERROR;
        case RDB_OB_INITIAL:
            return RDB_OK;
        case RDB_OB_BOOL:
        case RDB_OB_INT:
        case RDB_OB_RATIONAL:
        case RDB_OB_BIN:
            ret = serialize_type(valp, posp, argvalp->typ, ecp);
            if (ret != RDB_OK)
                return ret;

            len = argvalp->typ->ireplen;
            if (len == RDB_VARIABLE_LEN) {
                len = argvalp->var.bin.len;
                ret = reserve_space(valp, *posp, len + sizeof len, ecp);
                if (ret != RDB_OK)
                    return ret;
                memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &len, sizeof len);
                *posp += sizeof len;
            } else {
                ret = reserve_space(valp, *posp, len, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            datap = RDB_obj_irep((RDB_object *) argvalp, NULL);
            memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, datap, len);
            *posp += len;

            return RDB_OK;
    }
    abort();
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
    RDB_object arr;
    int i;
    RDB_int len;
    RDB_object *tplp;

    if (tbp->is_persistent)
        return serialize_str(valp, posp, tbp->name != NULL ? tbp->name : "", ecp);

    if (RDB_table_name(tbp) != NULL) {
        RDB_raise_invalid_argument("cannot serialize named local table", ecp);
        return RDB_ERROR;
    }

    ret = serialize_str(valp, posp, "", ecp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_type(valp, posp, RDB_table_type(tbp), ecp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&arr);
    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, NULL);
    if (ret != RDB_OK)
        return ret;
    len = RDB_array_length(&arr, ecp);
    if (len < 0) {
        RDB_destroy_obj(&arr, ecp);
        return len;
    }

    ret = serialize_int(valp, posp, len, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }
    for (i = 0; i < len; i++) {
        tplp = RDB_array_get(&arr, (RDB_int) i, ecp);
        if (tplp == NULL) {
            RDB_destroy_obj(&arr, ecp);
            return RDB_ERROR;
        }
        ret = serialize_obj(valp, posp, tplp, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&arr, ecp);
            return ret;
        }
    }
    return RDB_destroy_obj(&arr, ecp);
}

static int
serialize_table(RDB_object *valp, int *posp, RDB_table *tbp,
        RDB_exec_context *ecp)
{
    int ret = serialize_byte(valp, posp, (RDB_byte)tbp->kind, ecp);

    if (ret != RDB_OK)
        return ret;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            return serialize_rtable(valp, posp, tbp, ecp);
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

enum {
    RDB_BUF_INITLEN = 256
};

int
_RDB_table_to_obj(RDB_object *valp, RDB_table *tbp, RDB_exec_context *ecp)
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
    ret = serialize_table(valp, &pos, tbp, ecp);
    if (ret != RDB_OK)
        return ret;

    valp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
_RDB_type_to_obj(RDB_object *valp, const RDB_type *typ, RDB_exec_context *ecp)
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
_RDB_expr_to_obj(RDB_object *valp, const RDB_expression *exp,
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
    
    if (*posp + sizeof (len) > valp->var.bin.len)
        return RDB_INTERNAL; /* !! */
    memcpy (&len, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof len);
    *posp += sizeof len;
    if (*posp + len > valp->var.bin.len)
        return RDB_INTERNAL;
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
deserialize_int(RDB_object *valp, int *posp, RDB_int *vp)
{
    if (*posp + sizeof (RDB_int) > valp->var.bin.len)
        return RDB_INTERNAL;
    memcpy (vp, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof (RDB_int));
    *posp += sizeof (RDB_int);
    return RDB_OK;
}

static int
deserialize_byte(RDB_object *valp, int *posp)
{
    if (*posp + 1 > valp->var.bin.len)
        return RDB_INTERNAL;

    return ((RDB_byte *) valp->var.bin.datap)[(*posp)++];
}

static int
deserialize_type(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_type **typp)
{
    char *namp;
    int ret;
    enum _RDB_tp_kind kind;

    ret = deserialize_byte(valp, posp);
    if (ret < 0)
        return ret;
    kind = (enum _RDB_tp_kind ) ret;

    switch (kind) {
        case RDB_TP_SCALAR:
            ret = deserialize_str(valp, posp, ecp, &namp);
            if (ret != RDB_OK)
                return ret;
            *typp = RDB_get_type(namp, ecp, txp);
            free(namp);
            return *typp != NULL ? RDB_OK : RDB_ERROR;
        case RDB_TP_TUPLE:
        {
            RDB_int attrc;
            int i;

            ret = deserialize_int(valp, posp, &attrc);
            *typp = (RDB_type *) malloc(sizeof (RDB_type));
            if (*typp == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            (*typp)->name = NULL;
            (*typp)->kind = RDB_TP_TUPLE;
            (*typp)->ireplen = RDB_VARIABLE_LEN;

            (*typp)->var.tuple.attrv = malloc(sizeof(RDB_attr) * attrc);
            if ((*typp)->var.tuple.attrv == NULL) {
                free(*typp);
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            for (i = 0; i < attrc; i++) {
                ret = deserialize_str(valp, posp, ecp,
                        &(*typp)->var.tuple.attrv[i].name);
                if (ret != RDB_OK) {
                    free((*typp)->var.tuple.attrv);
                    free(*typp);
                }
                ret = deserialize_type(valp, posp, ecp, txp,
                        &(*typp)->var.tuple.attrv[i].typ);
                if (ret != RDB_OK) {
                    free((*typp)->var.tuple.attrv);
                    free(*typp);
                }
                (*typp)->var.tuple.attrv[i].defaultp = NULL;
            }
            (*typp)->var.tuple.attrc = attrc;
            return RDB_OK;
        }
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            *typp = (RDB_type *) malloc(sizeof (RDB_type));
            if (*typp == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            (*typp)->name = NULL;
            (*typp)->kind = kind;
            (*typp)->ireplen = RDB_VARIABLE_LEN;

            ret = deserialize_type(valp, posp, ecp, txp,
                        &(*typp)->var.basetyp);
            if (ret != RDB_OK)
                free(*typp);
            return RDB_OK;
    }
    return RDB_INTERNAL;
}

int
_RDB_deserialize_type(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_type **typp)
{
    int pos = 0;

    return deserialize_type(valp, &pos, ecp, txp, typp);
}

static int
deserialize_table(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp);

static int
deserialize_obj(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *argvalp)
{
    RDB_type *typ;
    size_t len;
    int ret;
    RDB_table *tbp;
    enum _RDB_obj_kind kind;

    ret = deserialize_byte(valp, posp);
    if (ret < 0)
        return ret;

    kind = (enum _RDB_obj_kind) ret;
    switch (kind) {
        case RDB_OB_TABLE:
            ret = deserialize_table(valp, posp, ecp, txp, &tbp);
            if (ret != RDB_OK)
                return ret;
            RDB_table_to_obj(argvalp, tbp, ecp);
            return RDB_OK;
        case RDB_OB_INITIAL:
            return RDB_OK;
        case RDB_OB_TUPLE:
        {
            RDB_int size;
            int i;
            char *attrname;
            RDB_object attrobj;

            ret = deserialize_int(valp, posp, &size);
            if (ret != RDB_OK)
                return ret;

            RDB_init_obj(&attrobj);
            for (i = 0; i < size; i++) {
                ret = deserialize_str(valp, posp, ecp, &attrname);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&attrobj, ecp);
                    return ret;
                }
                ret = deserialize_obj(valp, posp, ecp, txp, &attrobj);
                if (ret != RDB_OK) {
                    free(attrname);
                    RDB_destroy_obj(&attrobj, ecp);
                    return ret;
                }
                ret = RDB_tuple_set(argvalp, attrname, &attrobj, ecp);
                free(attrname);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&attrobj, ecp);
                    return ret;
                }
            }
            return RDB_destroy_obj(&attrobj, ecp);
        }
        case RDB_OB_ARRAY:
            RDB_raise_not_supported("cannot deserialize array", ecp);
            return RDB_ERROR;
        case RDB_OB_BOOL:
        case RDB_OB_INT:
        case RDB_OB_RATIONAL:
        case RDB_OB_BIN:
            ret = deserialize_type(valp, posp, ecp, txp, &typ);
            if (ret != RDB_OK)
                return ret;

            len = typ->ireplen;
            if (len == RDB_VARIABLE_LEN) {
                if (*posp + sizeof len > valp->var.bin.len)
                    return RDB_INTERNAL;
                memcpy (&len, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof len);
                *posp += sizeof len;
            }

            if (*posp + len > valp->var.bin.len)
                return RDB_INTERNAL;
            ret = RDB_irep_to_obj(argvalp, typ,
                    ((RDB_byte *)valp->var.bin.datap) + *posp, len, ecp);
            if (ret != RDB_OK)
                return ret;
            *posp += len;

            return RDB_OK;
    }
    abort();
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

    ret = deserialize_byte(valp, posp);
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

            ret = deserialize_int(valp, posp, &argc);
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

            ret = deserialize_byte(valp, posp);
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

int
_RDB_deserialize_expr(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_expression **expp)
{
    int pos = 0;

    return deserialize_expr(valp, &pos, ecp, txp, expp);
}

int
deserialize_project(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int ac;
    char **av;
    int i;
    int ret;

    ret = deserialize_table(valp, posp, ecp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;
    ret = deserialize_int(valp, posp, &ac);
    if (ret != RDB_OK)
        return ret;
    av = malloc(ac * sizeof(char *));
    if (av == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < ac; i++)
       av[i] = NULL;
    for (i = 0; i < ac; i++) {
        ret = deserialize_str(valp, posp, ecp, av + i);
        if (ret != RDB_OK)
            goto error;
    }
    *tbpp = RDB_project(tbp, ac, av, ecp);
    if (*tbpp == NULL) /* !! ret */
        goto error;
    RDB_free_strvec(ac, av);
    return RDB_OK;
error:
    RDB_free_strvec(ac, av);
    return ret;
}

int
deserialize_extend(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int ac;
    RDB_virtual_attr *av;
    int i;
    int ret;

    ret = deserialize_table(valp, posp, ecp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;
    ret = deserialize_int(valp, posp, &ac);
    av = malloc(ac * sizeof(RDB_virtual_attr));
    if (av == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
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
    *tbpp = RDB_extend(tbp, ac, av, ecp, txp);
    if (*tbpp == NULL)
        goto error;
    for (i = 0; i < ac; i++) {
        free(av[i].name);
    }
    free(av);
    return RDB_OK;

error:
    for (i = 0; i < ac; i++) {
        free(av[i].name);
    }
    free(av);
    return RDB_ERROR;
}

int
deserialize_summarize(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tb1p;
    RDB_table *tb2p;
    RDB_summarize_add *addv;
    RDB_int addc;
    int ret;
    int i;

    ret = deserialize_table(valp, posp, ecp, txp, &tb1p);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_table(valp, posp, ecp, txp, &tb2p);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &addc);
    if (ret != RDB_OK)
        return ret;

    addv = malloc(addc * sizeof (RDB_summarize_add));
    if (addv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < addc; i++) {
        addv[i].exp = NULL;
        addv[i].name = NULL;
    }

    for (i = 0; i < addc; i++) {
        ret = deserialize_byte(valp, posp);
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

    *tbpp = RDB_summarize(tb1p, tb2p, addc, addv, ecp, txp);
    if (*tbpp == NULL) {
        goto error;
    }

    for (i = 0; i < addc; i++) {
        free(addv[i].name);
    }
    free(addv);

    return RDB_OK;

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
    return RDB_ERROR;
}

int
deserialize_rename(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int renc;
    RDB_renaming *renv;
    int ret;
    int i;

    ret = deserialize_table(valp, posp, ecp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &renc);
    if (ret != RDB_OK)
        return ret;

    renv = malloc(renc * sizeof (RDB_renaming));
    if (renv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < renc; i++) {
        renv[i].from = NULL;
        renv[i].to = NULL;
    }

    for (i = 0; i < renc; i++) {
        ret = deserialize_str(valp, posp, ecp, &renv[i].from);
        if (ret != RDB_OK)
            goto cleanup;
        ret = deserialize_str(valp, posp, ecp, &renv[i].to);
        if (ret != RDB_OK)
            goto cleanup;
    }

    *tbpp = RDB_rename(tbp, renc, renv, ecp);
    if (*tbpp == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
    } else {
        ret = RDB_OK;
    }

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    for (i = 0; i < renc; i++) {
        free(renv[i].from);
        free(renv[i].to);
    }
    free(renv);

    return ret;
}

int
deserialize_wrap(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    int i, j;
    int ret;
    RDB_table *tbp;
    int wrapc;
    RDB_wrapping *wrapv = NULL;

    ret = deserialize_table(valp, posp, ecp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &wrapc);
    if (ret != RDB_OK)
        goto cleanup;

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

        ret = deserialize_int(valp, posp, &wrapv[i].attrc);
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
     
    *tbpp = RDB_wrap(tbp, wrapc, wrapv, ecp);
    ret = *tbpp != NULL ? RDB_OK : RDB_ERROR;

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
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

    return ret;
}

int
deserialize_unwrap(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tbp;
    int attrc;
    char **attrv;
    int ret;
    int i;

    ret = deserialize_table(valp, posp, ecp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &attrc);
    if (ret != RDB_OK)
        return ret;

    attrv = malloc(attrc * sizeof (char *));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < attrc; i++) {
        attrv[i] = NULL;
    }

    for (i = 0; i < attrc; i++) {
        ret = deserialize_str(valp, posp, ecp, &attrv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }

    *tbpp = RDB_unwrap(tbp, attrc, attrv, ecp);
    if (*tbpp == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
    } else {
        ret = RDB_OK;
    }

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    for (i = 0; i < attrc; i++) {
        free(attrv[i]);
    }
    free(attrv);

    return ret;
}

int
deserialize_group(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    int i;
    int ret;
    RDB_table *tbp;
    int attrc;
    char **attrv = NULL;
    char *gattr = NULL;

    ret = deserialize_table(valp, posp, ecp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &attrc);
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

    *tbpp = RDB_group(tbp, attrc, attrv, gattr, ecp);
    if (*tbpp == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
    } else {
        ret = RDB_OK;
    }

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    if (attrv != NULL) {
        for (i = 0; i < attrc; i++)
            free(attrv[i]);
    }
    free(gattr);

    return ret;
}

int
deserialize_ungroup(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tbp;
    char *attrname = NULL;
    int ret;

    ret = deserialize_table(valp, posp, ecp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_str(valp, posp, ecp, &attrname);
    if (ret != RDB_OK)
        goto cleanup;

    *tbpp = RDB_ungroup(tbp, attrname, ecp);
    if (*tbpp == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
    } else {
        ret = RDB_OK;
    }

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, ecp, NULL);

    if (attrname != NULL)
        free(attrname);

    return ret;
}

int
deserialize_sdivide(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tb1p, *tb2p, *tb3p;
    int ret;

    ret = deserialize_table(valp, posp, ecp, txp, &tb1p);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_table(valp, posp, ecp, txp, &tb2p);
    if (ret != RDB_OK)
        goto cleanup;

    ret = deserialize_table(valp, posp, ecp, txp, &tb3p);
    if (ret != RDB_OK)
        goto cleanup;

    *tbpp = RDB_sdivide(tb1p, tb2p, tb3p, ecp);
    if (*tbpp == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
    } else {
        ret = RDB_OK;
    }

cleanup:
    if (ret != RDB_OK) {
        if (RDB_table_name(tb1p) == NULL)
            RDB_drop_table(tb1p, ecp, NULL);
        if (RDB_table_name(tb2p) == NULL)
            RDB_drop_table(tb2p, ecp, NULL);
        if (RDB_table_name(tb3p) == NULL)
            RDB_drop_table(tb3p, ecp, NULL);
    }

    return ret;
}

int
_RDB_deserialize_table(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    int pos = 0;

    return deserialize_table(valp, &pos, ecp, txp, tbpp);
}

static int
deserialize_rtable(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    char *namp;
    RDB_type *typ;
    RDB_int len;
    int ret;
    int i;
    RDB_object tpl;

    ret = deserialize_str(valp, posp, ecp, &namp);
    if (ret != RDB_OK)
        return ret;
    if (*namp != '\0') {
        *tbpp = RDB_get_table(namp, ecp, txp);
        free(namp);
        return *tbpp != NULL ? RDB_OK : RDB_ERROR;
    }
    free(namp);
    ret = deserialize_type(valp, posp, ecp, txp, &typ);
    if (ret != RDB_OK)
        return ret;
    *tbpp = RDB_create_table(NULL, RDB_FALSE, typ->var.basetyp->var.tuple.attrc,
            typ->var.basetyp->var.tuple.attrv, 0, NULL, ecp, NULL);
    RDB_drop_type(typ, ecp, NULL);
    if (*tbpp == NULL) {
       return RDB_ERROR;
    }
    
    ret = deserialize_int(valp, posp, &len);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);
    for (i = 0; i < len ; i++) {
        ret = deserialize_obj(valp, posp, ecp, NULL, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        ret = RDB_insert(*tbpp, &tpl, ecp, NULL);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
    }
    return RDB_destroy_obj(&tpl, ecp);
}

static int
deserialize_table(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    RDB_table *tb1p, *tb2p;
    RDB_expression *exprp;

    ret = deserialize_byte(valp, posp);
    if (ret < 0)
        return ret;
    switch ((enum _RDB_tb_kind) ret) {
        case RDB_TB_REAL:
            return deserialize_rtable(valp, posp, ecp, txp, tbpp);
        case RDB_TB_SELECT:
            ret = deserialize_table(valp, posp, ecp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_expr(valp, posp, ecp, txp, &exprp);
            if (ret != RDB_OK)
                goto error;
            *tbpp = RDB_select(tb1p, exprp, ecp, txp);
            return *tbpp != NULL ? RDB_OK : RDB_ERROR;
        case RDB_TB_UNION:
            ret = deserialize_table(valp, posp, ecp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_table(valp, posp, ecp, txp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            *tbpp = RDB_union(tb1p, tb2p, ecp);
            if (*tbpp == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            return RDB_OK;
        case RDB_TB_MINUS:
            ret = deserialize_table(valp, posp, ecp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_table(valp, posp, ecp, txp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            *tbpp = RDB_minus(tb1p, tb2p, ecp);
            if (*tbpp == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            return RDB_OK;
        case RDB_TB_INTERSECT:
            ret = deserialize_table(valp, posp, ecp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_table(valp, posp, ecp, txp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            *tbpp =  RDB_intersect(tb1p, tb2p, ecp);
            if (*tbpp == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            return RDB_OK;
        case RDB_TB_JOIN:
            ret = deserialize_table(valp, posp, ecp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_table(valp, posp, ecp, txp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            *tbpp = RDB_join(tb1p, tb2p, ecp);
            if (*tbpp == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            return RDB_OK;
        case RDB_TB_EXTEND:
            return deserialize_extend(valp, posp, ecp, txp, tbpp);
        case RDB_TB_PROJECT:
            return deserialize_project(valp, posp, ecp, txp, tbpp);
        case RDB_TB_SUMMARIZE:
            return deserialize_summarize(valp, posp, ecp, txp, tbpp);
        case RDB_TB_RENAME:
            return deserialize_rename(valp, posp, ecp, txp, tbpp);
        case RDB_TB_WRAP:
            return deserialize_wrap(valp, posp, ecp, txp, tbpp);
        case RDB_TB_UNWRAP:
            return deserialize_unwrap(valp, posp, ecp, txp, tbpp);
        case RDB_TB_GROUP:
            return deserialize_group(valp, posp, ecp, txp, tbpp);
        case RDB_TB_UNGROUP:
            return deserialize_ungroup(valp, posp, ecp, txp, tbpp);
        case RDB_TB_SDIVIDE:
            return deserialize_sdivide(valp, posp, ecp, txp, tbpp);
    }
    abort();
error:
    return ret;
}
