/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "serialize.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>
#include <gen/hashmapit.h>

/*
 * Functions for serializing/deserializing - needed for
 * persistent virtual tables (views)
 */

static int
reserve_space(RDB_object *valp, int pos, size_t n)
{
    while (valp->var.bin.len < pos + n) {
        int newlen = valp->var.bin.len * 2;
        void *newdatap = realloc(valp->var.bin.datap, newlen);
        
        if (newdatap == NULL)
            return RDB_NO_MEMORY;
        valp->var.bin.len = newlen;
        valp->var.bin.datap = newdatap;
    }

    return RDB_OK;
}

static int
serialize_str(RDB_object *valp, int *posp, const char *str)
{
    int ret;
    size_t len = strlen(str);

    ret = reserve_space(valp, *posp, len + sizeof len);
    if (ret != RDB_OK)
        return ret;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &len, sizeof len);
    *posp += sizeof len;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, str, len);
    *posp += len;
    return RDB_OK;
}

static int
serialize_int(RDB_object *valp, int *posp, RDB_int v)
{
    int ret;

    ret = reserve_space(valp, *posp, sizeof v);
    if (ret != RDB_OK)
        return ret;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &v, sizeof v);
    *posp += sizeof v;
    return RDB_OK;
}

static int
serialize_byte(RDB_object *valp, int *posp, RDB_byte b)
{
    int ret;

    ret = reserve_space(valp, *posp, 1);
    if (ret != RDB_OK)
        return ret;
    ((RDB_byte *) valp->var.bin.datap)[(*posp)++] = b;
    return RDB_OK;
}

static int
serialize_type(RDB_object *valp, int *posp, const RDB_type *typ)
{
    int ret;
    
    ret = serialize_byte(valp, posp, (RDB_byte) typ->kind);
    if (ret != RDB_OK)
        return ret;

    switch (typ->kind) {
        case RDB_TP_SCALAR:
            return serialize_str(valp, posp, typ->name);
        case RDB_TP_TUPLE:
        {
            int i;

            ret = serialize_int(valp, posp, typ->var.tuple.attrc);
            if (ret != RDB_OK)
                return ret;

            for (i = 0; i < typ->var.tuple.attrc; i++) {
                ret = serialize_str(valp, posp, typ->var.tuple.attrv[i].name);
                if (ret != RDB_OK)
                    return ret;

                ret = serialize_type(valp, posp, typ->var.tuple.attrv[i].typ);
                if (ret != RDB_OK)
                    return ret;
            }
            return RDB_OK;
        }
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            return serialize_type(valp, posp, typ->var.basetyp);
    }
    abort();
}

static int
serialize_table(RDB_object *valp, int *posp, RDB_table *tbp);

static int
serialize_obj(RDB_object *valp, int *posp, const RDB_object *argvalp)
{
    size_t len;
    void *datap;
    int ret;

    ret = serialize_byte(valp, posp, argvalp->kind);
    if (ret != RDB_OK)
        return ret;

    switch (argvalp->kind) {
        case RDB_OB_TABLE:
            return serialize_table(valp, posp, argvalp->var.tbp);
        case RDB_OB_TUPLE:
        {
            RDB_object *attrp;
            char *key;
            RDB_hashmap_iter hiter;
        
            ret = serialize_int(valp, posp, RDB_tuple_size(argvalp));
            if (ret != RDB_OK)
                return ret;

            RDB_init_hashmap_iter(&hiter, (RDB_hashmap *) &argvalp->var.tpl_map);
            while ((attrp = (RDB_object *) RDB_hashmap_next(&hiter, &key, NULL))
                    != NULL)
            {
                /* Write attribute name */
                ret = serialize_str(valp, posp, key);
                if (ret != RDB_OK)
                    return ret;

                /* Write attribute value */
                ret = serialize_obj(valp, posp, attrp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_destroy_hashmap_iter(&hiter);
            return RDB_OK;
        }
        case RDB_OB_ARRAY:
            return RDB_NOT_SUPPORTED;
        case RDB_OB_INITIAL:
            return RDB_OK;
        case RDB_OB_BOOL:
        case RDB_OB_INT:
        case RDB_OB_RATIONAL:
        case RDB_OB_BIN:
            ret = serialize_type(valp, posp, argvalp->typ);
            if (ret != RDB_OK)
                return ret;

            len = argvalp->typ->ireplen;
            if (len == RDB_VARIABLE_LEN) {
                len = argvalp->var.bin.len;
                ret = reserve_space(valp, *posp, len + sizeof len);
                if (ret != RDB_OK)
                    return ret;
                memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &len, sizeof len);
                *posp += sizeof len;
            } else {
                ret = reserve_space(valp, *posp, len);
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
serialize_expr(RDB_object *valp, int *posp, const RDB_expression *exp)
{
    int ret = serialize_byte(valp, posp, (RDB_byte)exp->kind);
    if (ret != RDB_OK)
        return ret;

    switch(exp->kind) {
        case RDB_EX_OBJ:
            return serialize_obj(valp, posp, &exp->var.obj);
        case RDB_EX_ATTR:
            return serialize_str(valp, posp, exp->var.attr.name);
        case RDB_EX_NOT:
        case RDB_EX_NEGATE:
        case RDB_EX_IS_EMPTY:
        case RDB_EX_STRLEN:
            return serialize_expr(valp, posp, exp->var.op.arg1);
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
        case RDB_EX_AND:
        case RDB_EX_OR:
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
        case RDB_EX_REGMATCH:
        case RDB_EX_CONTAINS:
        case RDB_EX_CONCAT:
        case RDB_EX_SUBSET:
            ret = serialize_expr(valp, posp, exp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
            return serialize_expr(valp, posp, exp->var.op.arg2);
        case RDB_EX_GET_COMP:
            ret = serialize_expr(valp, posp, exp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
            return serialize_str(valp, posp, exp->var.op.name);
        case RDB_EX_AGGREGATE:
            ret = serialize_expr(valp, posp, exp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_byte(valp, posp, (RDB_byte) exp->var.op.op);
            if (ret != RDB_OK)
                return ret;
            return serialize_str(valp, posp, exp->var.op.name);
        case RDB_EX_USER_OP:
        {
            int i;
            int argc = exp->var.user_op.argc;

            ret = serialize_str(valp, posp, exp->var.user_op.name);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_int (valp, posp, argc);
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < argc; i++) {
                ret = serialize_expr(valp, posp, exp->var.user_op.argv[i]);
                if (ret != RDB_OK)
                    return ret;
            }
            return RDB_OK;
        }
        case RDB_EX_TUPLE_ATTR:
            ret = serialize_expr(valp, posp, exp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
            return serialize_str(valp, posp, exp->var.op.name);
    }
    /* should never be reached */
    abort();
}

static int
serialize_project(RDB_object *valp, int *posp, RDB_table *tbp)
{
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.project.tbp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tuptyp->var.tuple.attrc);
    if (ret != RDB_OK)
        return ret;
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        ret = serialize_str(valp, posp, tuptyp->var.tuple.attrv[i].name);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}      

static int
serialize_extend(RDB_object *valp, int *posp, RDB_table *tbp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.extend.tbp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.extend.attrc);
    if (ret != RDB_OK)
        return ret;
    for (i = 0; i < tbp->var.extend.attrc; i++) {
        ret = serialize_str(valp, posp, tbp->var.extend.attrv[i].name);
        if (ret != RDB_OK)
            return ret;
        ret = serialize_expr(valp, posp, tbp->var.extend.attrv[i].exp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}      

static int
serialize_summarize(RDB_object *valp, int *posp, RDB_table *tbp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.summarize.tb1p);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_table(valp, posp, tbp->var.summarize.tb2p);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.summarize.addc);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.summarize.addc; i++) {
        ret = serialize_byte(valp, posp,
                             (RDB_byte)tbp->var.summarize.addv[i].op);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_expr(valp, posp, tbp->var.summarize.addv[i].exp);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_str(valp, posp, tbp->var.summarize.addv[i].name);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}

static int
serialize_rename(RDB_object *valp, int *posp, RDB_table *tbp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.rename.tbp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.rename.renc);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.rename.renc; i++) {
        ret = serialize_str(valp, posp, tbp->var.rename.renv[i].from);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_str(valp, posp, tbp->var.rename.renv[i].to);
        if (ret != RDB_OK)
            return ret;
    }

    return RDB_OK;
}

static int
serialize_wrap(RDB_object *valp, int *posp, RDB_table *tbp)
{
    int i, j;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.wrap.tbp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.wrap.wrapc);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.wrap.wrapc; i++) {
        ret = serialize_str(valp, posp, tbp->var.wrap.wrapv[i].attrname);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_int(valp, posp, tbp->var.wrap.wrapv[i].attrc);
        if (ret != RDB_OK)
            return ret;

        for (j = 0; j < tbp->var.wrap.wrapv[i].attrc; j++) {
            ret = serialize_str(valp, posp, tbp->var.wrap.wrapv[i].attrv[j]);
            if (ret != RDB_OK)
                return ret;
        }
    }

    return RDB_OK;
}

static int
serialize_unwrap(RDB_object *valp, int *posp, RDB_table *tbp)
{
    int i;
    int ret;

    ret = serialize_table(valp, posp, tbp->var.unwrap.tbp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.unwrap.attrc);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.unwrap.attrc; i++) {
        ret = serialize_str(valp, posp, tbp->var.unwrap.attrv[i]);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}

static int
serialize_sdivide(RDB_object *valp, int *posp, RDB_table *tbp)
{
    int ret;

    ret = serialize_table(valp, posp, tbp->var.sdivide.tb1p);
    if (ret != RDB_OK)
        return ret;
    ret = serialize_table(valp, posp, tbp->var.sdivide.tb2p);
    if (ret != RDB_OK)
        return ret;
    ret = serialize_table(valp, posp, tbp->var.sdivide.tb3p);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}

static int
serialize_table(RDB_object *valp, int *posp, RDB_table *tbp)
{
    int ret = serialize_byte(valp, posp, (RDB_byte)tbp->kind);

    if (ret != RDB_OK)
        return ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            if (tbp->name == NULL)
                return RDB_INVALID_ARGUMENT;
            return serialize_str(valp, posp, tbp->name);
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_INDEX:
            ret = serialize_table(valp, posp, tbp->var.select.tbp);
            if (ret != RDB_OK)
                return ret;
            return serialize_expr(valp, posp, tbp->var.select.exprp);
        case RDB_TB_UNION:
            ret = serialize_table(valp, posp, tbp->var._union.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_table(valp, posp, tbp->var._union.tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_MINUS:
            ret = serialize_table(valp, posp, tbp->var.minus.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_table(valp, posp, tbp->var.minus.tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_INTERSECT:
            ret = serialize_table(valp, posp, tbp->var.intersect.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_table(valp, posp, tbp->var.intersect.tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_JOIN:
            ret = serialize_table(valp, posp, tbp->var.join.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = serialize_table(valp, posp, tbp->var.join.tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_EXTEND:
            return serialize_extend(valp, posp, tbp);
        case RDB_TB_PROJECT:
            return serialize_project(valp, posp, tbp);
        case RDB_TB_SUMMARIZE:
            return serialize_summarize(valp, posp, tbp);
        case RDB_TB_RENAME:
            return serialize_rename(valp, posp, tbp);
        case RDB_TB_WRAP:
            return serialize_wrap(valp, posp, tbp);
        case RDB_TB_UNWRAP:
            return serialize_unwrap(valp, posp, tbp);
        case RDB_TB_SDIVIDE:
            return serialize_sdivide(valp, posp, tbp);
    }
    abort();
}

enum {
    RDB_BUF_INITLEN = 256
};

int
_RDB_table_to_obj(RDB_object *valp, RDB_table *tbp)
{
    int pos;
    int ret;

    RDB_destroy_obj(valp);
    valp->typ = &RDB_BINARY;
    valp->kind = RDB_OB_BIN;
    valp->var.bin.len = RDB_BUF_INITLEN;
    valp->var.bin.datap = malloc(RDB_BUF_INITLEN);
    if (valp->var.bin.datap == NULL) {
        return RDB_NO_MEMORY;
    }
    pos = 0;
    ret = serialize_table(valp, &pos, tbp);
    if (ret != RDB_OK)
        return ret;

    valp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
_RDB_type_to_obj(RDB_object *valp, const RDB_type *typ)
{
    int pos;
    int ret;

    RDB_destroy_obj(valp);
    valp->typ = &RDB_BINARY;
    valp->kind = RDB_OB_BIN;
    valp->var.bin.len = RDB_BUF_INITLEN;
    valp->var.bin.datap = malloc(RDB_BUF_INITLEN);
    if (valp->var.bin.datap == NULL) {
        return RDB_NO_MEMORY;
    }
    pos = 0;
    ret = serialize_type(valp, &pos, typ);
    if (ret != RDB_OK)
        return ret;

    valp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
_RDB_expr_to_obj(RDB_object *valp, const RDB_expression *exp)
{
    int pos = 0;
    int ret;

    RDB_destroy_obj(valp);
    valp->typ = &RDB_BINARY;
    valp->kind = RDB_OB_BIN;
    if (exp != NULL) {
        valp->var.bin.len = RDB_BUF_INITLEN;
        valp->var.bin.datap = malloc(RDB_BUF_INITLEN);
        if (valp->var.bin.datap == NULL) {
            return RDB_NO_MEMORY;
        }
        ret = serialize_expr(valp, &pos, exp);
        if (ret != RDB_OK)
            return ret;
    } else {
        valp->var.bin.datap = NULL;
    }

    valp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

static int
deserialize_str(RDB_object *valp, int *posp, char **strp)
{
    size_t len;
    
    if (*posp + sizeof (len) > valp->var.bin.len)
        return RDB_INTERNAL;
    memcpy (&len, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof len);
    *posp += sizeof len;
    if (*posp + len > valp->var.bin.len)
        return RDB_INTERNAL;
    *strp = malloc(len + 1);
    if (*strp == NULL)
        return RDB_NO_MEMORY;
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
deserialize_type(RDB_object *valp, int *posp, RDB_transaction *txp,
                 RDB_type **typp)
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
            ret = deserialize_str(valp, posp, &namp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_get_type(namp, txp, typp);
            free(namp);
            return ret;
        case RDB_TP_TUPLE:
        {
            RDB_int attrc;
            int i;

            ret = deserialize_int(valp, posp, &attrc);
            *typp = (RDB_type *) malloc(sizeof (RDB_type));
            if (*typp == NULL)
                return RDB_NO_MEMORY;
            (*typp)->name = NULL;
            (*typp)->kind = RDB_TP_TUPLE;
            (*typp)->ireplen = RDB_VARIABLE_LEN;

            (*typp)->var.tuple.attrv = malloc(sizeof(RDB_attr) * attrc);
            if ((*typp)->var.tuple.attrv == NULL) {
                free(*typp);
                return RDB_NO_MEMORY;
            }
            for (i = 0; i < attrc; i++) {
                ret = deserialize_str(valp, posp,
                        &(*typp)->var.tuple.attrv[i].name);
                if (ret != RDB_OK) {
                    free((*typp)->var.tuple.attrv);
                    free(*typp);
                }
                ret = deserialize_type(valp, posp, txp,
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
            if (*typp == NULL)
                return RDB_NO_MEMORY;
            (*typp)->name = NULL;
            (*typp)->kind = kind;
            (*typp)->ireplen = RDB_VARIABLE_LEN;

            ret = deserialize_type(valp, posp, txp,
                        &(*typp)->var.basetyp);
            if (ret != RDB_OK)
                free(*typp);
            return RDB_OK;
    }
    return RDB_INTERNAL;
}

int
_RDB_deserialize_type(RDB_object *valp, RDB_transaction *txp,
                 RDB_type **typp)
{
    int pos = 0;

    return deserialize_type(valp, &pos, txp, typp);
}

static int
deserialize_table(RDB_object *valp, int *posp, RDB_transaction *txp,
                   RDB_table **tbpp);

static int
deserialize_obj(RDB_object *valp, int *posp, RDB_transaction *txp,
                  RDB_object *argvalp)
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
            ret = deserialize_table(valp, posp, txp, &tbp);
            if (ret != RDB_OK)
                return ret;
            RDB_table_to_obj(valp, tbp);
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
                ret = deserialize_str(valp, posp, &attrname);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&attrobj);
                    return ret;
                }
                ret = deserialize_obj(valp, posp, txp, &attrobj);
                if (ret != RDB_OK) {
                    free(attrname);
                    RDB_destroy_obj(&attrobj);
                    return ret;
                }
                ret = RDB_tuple_set(argvalp, attrname, &attrobj);
                free(attrname);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&attrobj);
                    return ret;
                }
            }
            RDB_destroy_obj(&attrobj);
            return RDB_OK;
        }
        case RDB_OB_ARRAY:
            /* !! */
            return RDB_NOT_SUPPORTED;
        case RDB_OB_BOOL:
        case RDB_OB_INT:
        case RDB_OB_RATIONAL:
        case RDB_OB_BIN:
            ret = deserialize_type(valp, posp, txp, &typ);
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
                    ((RDB_byte *)valp->var.bin.datap) + *posp, len);
            if (ret != RDB_OK)
                return ret;
            *posp += len;

            return RDB_OK;
    }
    abort();
}

static int
deserialize_expr(RDB_object *valp, int *posp, RDB_transaction *txp,
                 RDB_expression **expp)
{
    RDB_expression *ex1p, *ex2p;
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
               RDB_expression *exp;

               RDB_init_obj(&val);
               ret = deserialize_obj(valp, posp, txp, &val);
               if (ret != RDB_OK) {
                   RDB_destroy_obj(&val);
                   return ret;
               }
               *expp = RDB_obj_to_expr(&val);
               RDB_destroy_obj(&val);
               if (exp == NULL)
                   return RDB_NO_MEMORY;
            }
            break;
        case RDB_EX_ATTR:
            {
                char *attrnamp;
            
                ret = deserialize_str(valp, posp, &attrnamp);
                if (ret != RDB_OK)
                    return ret;

                *expp = RDB_expr_attr(attrnamp);
                free(attrnamp);
                if (*expp == NULL)
                    return RDB_NO_MEMORY;
            }
            break;
        case RDB_EX_NOT:
        case RDB_EX_NEGATE:
        case RDB_EX_STRLEN:
            ret = deserialize_expr(valp, posp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            *expp = _RDB_create_unexpr(ex1p, ekind);
            if (*expp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_EX_IS_EMPTY:
            ret = deserialize_expr(valp, posp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            *expp = RDB_expr_is_empty(ex1p);
            if (*expp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
        case RDB_EX_AND:
        case RDB_EX_OR:
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
        case RDB_EX_REGMATCH:
        case RDB_EX_CONTAINS:
        case RDB_EX_CONCAT:
        case RDB_EX_SUBSET:
            ret = deserialize_expr(valp, posp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_expr(valp, posp, txp, &ex2p);
            if (ret != RDB_OK)
                return ret;
            *expp = _RDB_create_binexpr(ex1p, ex2p, ekind);
            if (*expp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_EX_GET_COMP:
        {
            char *name;
            
            ret = deserialize_expr(valp, posp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_str(valp, posp, &name);
            if (ret != RDB_OK) {
                RDB_drop_expr(ex1p);
                return ret;
            }
            *expp = _RDB_create_unexpr(ex1p, RDB_EX_GET_COMP);
            if (*expp == NULL)
                return RDB_NO_MEMORY;
            (*expp)->var.op.name = RDB_dup_str(name);
            if ((*expp)->var.op.name == NULL) {
                RDB_drop_expr(*expp);
                return RDB_NO_MEMORY;
            }
            break;
        }
        case RDB_EX_USER_OP:
        {
            char *name;
            int argc;
            int i;
            RDB_expression **argv;
        
            ret = deserialize_str(valp, posp, &name);
            if (ret != RDB_OK) {
                return ret;
            }

            ret = deserialize_int(valp, posp, &argc);
            if (ret != RDB_OK)
                return ret;

            argv = malloc(argc * sizeof (RDB_expression *));
            if (argv == NULL)
                return RDB_NO_MEMORY;

            for (i = 0; i < argc; i++) {
                ret = deserialize_expr(valp, posp, txp, &argv[i]);
                if (ret != RDB_OK) {
                    return ret;
                }
            }
            ret = RDB_user_op(name, argc, argv, txp, expp);
            free(argv);
            if (ret != RDB_OK)
                return ret;
            break;
        }
        case RDB_EX_AGGREGATE:
        {
            RDB_expression *exp;
            RDB_aggregate_op op;
            char *name;

            ret = deserialize_expr(valp, posp, txp, &exp);
            if (ret != RDB_OK)
                return ret;

            ret = deserialize_byte(valp, posp);
            if (ret < 0)
                return ret;
            op = (RDB_aggregate_op) ret;

            ret = deserialize_str(valp, posp, &name);
            if (ret != RDB_OK) {
                return ret;
            }

            *expp = RDB_expr_aggregate(exp, op, name);
            if (*expp == NULL)
                return ret;
            break;
        }
        case RDB_EX_TUPLE_ATTR:
        {
            char *name;
            
            ret = deserialize_expr(valp, posp, txp, &ex1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_str(valp, posp, &name);
            if (ret != RDB_OK) {
                RDB_drop_expr(ex1p);
                return ret;
            }
            *expp = _RDB_create_unexpr(ex1p, RDB_EX_TUPLE_ATTR);
            if (*expp == NULL)
                return RDB_NO_MEMORY;
            (*expp)->var.op.name = RDB_dup_str(name);
            if ((*expp)->var.op.name == NULL) {
                RDB_drop_expr(*expp);
                return RDB_NO_MEMORY;
            }
            break;
        }
    }
    return RDB_OK;
}

int
_RDB_deserialize_expr(RDB_object *valp, RDB_transaction *txp,
                      RDB_expression **expp)
{
    int pos = 0;

    return deserialize_expr(valp, &pos, txp, expp);
}

int
deserialize_project(RDB_object *valp, int *posp, RDB_transaction *txp,
                    RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int ac;
    char **av;
    int i;
    int ret;

    ret = deserialize_table(valp, posp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;
    ret = deserialize_int(valp, posp, &ac);
    if (ret != RDB_OK)
        return ret;
    av = malloc(ac * sizeof(char *));
    if (av == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < ac; i++)
       av[i] = NULL;
    for (i = 0; i < ac; i++) {
        ret = deserialize_str(valp, posp, av + i);
        if (ret != RDB_OK)
            goto error;
    }
    ret = RDB_project(tbp, ac, av, tbpp);
    if (ret != RDB_OK)
        goto error;
    RDB_free_strvec(ac, av);
    return RDB_OK;
error:
    RDB_free_strvec(ac, av);
    return ret;
}

int
deserialize_extend(RDB_object *valp, int *posp, RDB_transaction *txp,
                    RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int ac;
    RDB_virtual_attr *av;
    int i;
    int ret;

    ret = deserialize_table(valp, posp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;
    ret = deserialize_int(valp, posp, &ac);
    av = malloc(ac * sizeof(RDB_virtual_attr));
    if (av == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < ac; i++) {
        av[i].name = NULL;
    }
    for (i = 0; i < ac; i++) {
        ret = deserialize_str(valp, posp, &av[i].name);
        if (ret != RDB_OK)
            goto error;
        ret = deserialize_expr(valp, posp, txp, &av[i].exp);
        if (ret != RDB_OK)
            goto error;
    }
    ret = RDB_extend(tbp, ac, av, tbpp);
    if (ret != RDB_OK)
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
    return ret;
}

int
deserialize_summarize(RDB_object *valp, int *posp, RDB_transaction *txp,
                    RDB_table **tbpp)
{
    RDB_table *tb1p;
    RDB_table *tb2p;
    RDB_summarize_add *addv;
    RDB_int addc;
    int ret;
    int i;

    ret = deserialize_table(valp, posp, txp, &tb1p);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_table(valp, posp, txp, &tb2p);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &addc);
    if (ret != RDB_OK)
        return ret;

    addv = malloc(addc * sizeof (RDB_summarize_add));
    if (addv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < addc; i++) {
        addv[i].exp = NULL;
        addv[i].name = NULL;
    }

    for (i = 0; i < addc; i++) {
        ret = deserialize_byte(valp, posp);
        if (ret < 0)
            goto error;
        addv[i].op = (RDB_aggregate_op)ret;
        ret = deserialize_expr(valp, posp, txp, &addv[i].exp);
        if (ret != RDB_OK)
            goto error;
        ret = deserialize_str(valp, posp, &addv[i].name);
        if (ret != RDB_OK)
            goto error;
    }

    ret = RDB_summarize(tb1p, tb2p, addc, addv, tbpp);
    if (ret != RDB_OK) {
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
            RDB_drop_table(tb1p, NULL);
        if (RDB_table_name(tb2p) == NULL)
            RDB_drop_table(tb2p, NULL);
    }

    for (i = 0; i < addc; i++) {
        if (addv[i].exp != NULL)
            RDB_drop_expr(addv[i].exp);
        free(addv[i].name);
    }
    free(addv);
    return ret;
}

int
deserialize_rename(RDB_object *valp, int *posp, RDB_transaction *txp,
                   RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int renc;
    RDB_renaming *renv;
    int ret;
    int i;

    ret = deserialize_table(valp, posp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &renc);
    if (ret != RDB_OK)
        return ret;

    renv = malloc(renc * sizeof (RDB_renaming));
    if (renv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < renc; i++) {
        renv[i].from = NULL;
        renv[i].to = NULL;
    }

    for (i = 0; i < renc; i++) {
        ret = deserialize_str(valp, posp, &renv[i].from);
        if (ret != RDB_OK)
            goto cleanup;
        ret = deserialize_str(valp, posp, &renv[i].to);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_rename(tbp, renc, renv, tbpp);

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, NULL);

    for (i = 0; i < renc; i++) {
        free(renv[i].from);
        free(renv[i].to);
    }
    free(renv);

    return ret;
}

int
deserialize_wrap(RDB_object *valp, int *posp, RDB_transaction *txp,
                   RDB_table **tbpp)
{
    int i, j;
    int ret;
    RDB_table *tbp;
    int wrapc;
    RDB_wrapping *wrapv;
    
    ret = deserialize_table(valp, posp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &wrapc);
    if (ret != RDB_OK)
        return ret;

    wrapv = malloc(wrapc * sizeof (RDB_wrapping));
    if (wrapv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrname = NULL;
        wrapv[i].attrv = NULL;
    }
    for (i = 0; i < wrapc; i++) {
        ret = deserialize_str(valp, posp, &wrapv[i].attrname);
        if (ret != RDB_OK)
            goto cleanup;

        ret = deserialize_int(valp, posp, &wrapv[i].attrc);
        if (ret != RDB_OK)
            goto cleanup;
        for (j = 0; j < wrapv[i].attrc; j++)
            wrapv[j].attrv[j] = NULL;
        for (j = 0; j < wrapv[i].attrc; j++) {
            ret = deserialize_str(valp, posp, &wrapv[i].attrv[j]);
            if (ret != RDB_OK)
                goto cleanup;
        }
    }
     
    ret = RDB_wrap(tbp, wrapc, wrapv, tbpp);

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, NULL);

    for (i = 0; i < wrapc; i++) {
        free(wrapv[i].attrname);
        if (wrapv[i].attrv != NULL) {
            for (j = 0; j < wrapv[i].attrc; i++)
                free(wrapv[i].attrv[j]);
        }
    }

    return ret;
}

int
deserialize_unwrap(RDB_object *valp, int *posp, RDB_transaction *txp,
                   RDB_table **tbpp)
{
    RDB_table *tbp;
    int attrc;
    char **attrv;
    int ret;
    int i;

    ret = deserialize_table(valp, posp, txp, &tbp);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_int(valp, posp, &attrc);
    if (ret != RDB_OK)
        return ret;

    attrv = malloc(attrc * sizeof (char *));
    if (attrv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < attrc; i++) {
        attrv[i] = NULL;
    }

    for (i = 0; i < attrc; i++) {
        ret = deserialize_str(valp, posp, &attrv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_unwrap(tbp, attrc, attrv, tbpp);

cleanup:
    if (ret != RDB_OK && RDB_table_name(tbp) == NULL)
        RDB_drop_table(tbp, NULL);

    for (i = 0; i < attrc; i++) {
        free(attrv[i]);
    }
    free(attrv);

    return ret;
}

int
deserialize_sdivide(RDB_object *valp, int *posp, RDB_transaction *txp,
                   RDB_table **tbpp)
{
    RDB_table *tb1p, *tb2p, *tb3p;
    int ret;

    ret = deserialize_table(valp, posp, txp, &tb1p);
    if (ret != RDB_OK)
        return ret;

    ret = deserialize_table(valp, posp, txp, &tb2p);
    if (ret != RDB_OK)
        goto cleanup;

    ret = deserialize_table(valp, posp, txp, &tb3p);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_sdivide(tb1p, tb2p, tb3p, tbpp);

cleanup:
    if (ret != RDB_OK) {
        if (RDB_table_name(tb1p) == NULL)
            RDB_drop_table(tb1p, NULL);
        if (RDB_table_name(tb2p) == NULL)
            RDB_drop_table(tb2p, NULL);
        if (RDB_table_name(tb3p) == NULL)
            RDB_drop_table(tb3p, NULL);
    }

    return ret;
}

int
_RDB_deserialize_table(RDB_object *valp, RDB_transaction *txp, RDB_table **tbpp)
{
    int pos = 0;

    return deserialize_table(valp, &pos, txp, tbpp);
}

static int
deserialize_table(RDB_object *valp, int *posp, RDB_transaction *txp,
                   RDB_table **tbpp)
{
    int ret;
    char *namp;
    RDB_table *tb1p, *tb2p;
    RDB_expression *exprp;

    ret = deserialize_byte(valp, posp);
    if (ret < 0)
        return ret;
    switch ((enum _RDB_tb_kind) ret) {
        case RDB_TB_STORED:
            ret = deserialize_str(valp, posp, &namp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_get_table(namp, txp, tbpp);
            free(namp);
            return ret;
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_INDEX:
            ret = deserialize_table(valp, posp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_expr(valp, posp, txp, &exprp);
            if (ret != RDB_OK)
                goto error;
            return RDB_select(tb1p, exprp, tbpp);
            break;
        case RDB_TB_UNION:
            ret = deserialize_table(valp, posp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_table(valp, posp, txp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_union(tb1p, tb2p, tbpp);
        case RDB_TB_MINUS:
            ret = deserialize_table(valp, posp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_table(valp, posp, txp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_minus(tb1p, tb2p, tbpp);
        case RDB_TB_INTERSECT:
            ret = deserialize_table(valp, posp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_table(valp, posp, txp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_union(tb1p, tb2p, tbpp);
        case RDB_TB_JOIN:
            ret = deserialize_table(valp, posp, txp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_table(valp, posp, txp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_join(tb1p, tb2p, tbpp);
        case RDB_TB_EXTEND:
            return deserialize_extend(valp, posp, txp, tbpp);
        case RDB_TB_PROJECT:
            return deserialize_project(valp, posp, txp, tbpp);
        case RDB_TB_SUMMARIZE:
            return deserialize_summarize(valp, posp, txp, tbpp);
        case RDB_TB_RENAME:
            return deserialize_rename(valp, posp, txp, tbpp);
        case RDB_TB_WRAP:
            return deserialize_wrap(valp, posp, txp, tbpp);
        case RDB_TB_UNWRAP:
            return deserialize_unwrap(valp, posp, txp, tbpp);
        case RDB_TB_SDIVIDE:
            return deserialize_sdivide(valp, posp, txp, tbpp);
    }
    abort();
error:
    return ret;
}
