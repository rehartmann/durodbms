/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "tabletostr.h"
#include <string.h>
#include <rel/internal.h>
#include <gen/hashmapit.h>

static int
append_str(RDB_object *objp, const char *str)
{
    int len = objp->var.bin.len + strlen(str);
    char *nstr = realloc(objp->var.bin.datap, len);
    if (nstr == NULL)
        return RDB_NO_MEMORY;

    objp->var.bin.datap = nstr;
    strcpy(((char *)objp->var.bin.datap) + objp->var.bin.len - 1, str);
    objp->var.bin.len = len;
    return RDB_OK;
}

static int
append_aggr_op(RDB_object *objp, RDB_aggregate_op op)
{
    switch (op) {
        case RDB_COUNT:
            return append_str(objp, "COUNT");
        case RDB_SUM:
            return append_str(objp, "SUM");
        case RDB_AVG:
            return append_str(objp, "AVG");
        case RDB_MAX:
            return append_str(objp, "MAX");
        case RDB_MIN:
            return append_str(objp, "MIN");
        case RDB_ALL:
            return append_str(objp, "ALL");
        case RDB_ANY:
            return append_str(objp, "ANY");
        case RDB_COUNTD:
            return append_str(objp, "COUNTD");
        case RDB_SUMD:
            return append_str(objp, "SUMD");
        case RDB_AVGD:
            return append_str(objp, "AVGD");
    }
    abort();
}

static int
append_obj(RDB_object *objp, const RDB_object *srcp);

static int
append_table(RDB_object *, RDB_table *, int options);

static int
append_tuple(RDB_object *objp, const RDB_object *tplp)
{
    int ret;
    RDB_hashmap_iter hiter;
    RDB_object *attrp;
    char *key;
    RDB_bool start = RDB_TRUE;

    ret = append_str(objp, "TUPLE { ");
    if (ret != RDB_OK)
        return ret;

    RDB_init_hashmap_iter(&hiter, (RDB_hashmap *) &tplp->var.tpl_map);
    for (;;) {
        /* Get next attribute */
        attrp = (RDB_object *) RDB_hashmap_next(&hiter, &key, NULL);
        if (attrp == NULL)
            break;

        if (start) {
            start = RDB_FALSE;
        } else {
            ret = append_str(objp, ", ");
            if (ret != RDB_OK)
                goto error;
        }

        ret = append_str(objp, key);
        if (ret != RDB_OK)
            goto error;

        ret = append_str(objp, " ");
        if (ret != RDB_OK)
            goto error;

        ret = append_obj(objp, attrp);
        if (ret != RDB_OK)
            goto error;
    }
    ret = append_str(objp, " }");
    if (ret != RDB_OK)
        return ret;

    RDB_destroy_hashmap_iter(&hiter);
    return RDB_OK;

error:
    RDB_destroy_hashmap_iter(&hiter);
    return ret;
}


static int
append_obj(RDB_object *objp, const RDB_object *srcp)
{
    int ret;
    RDB_object dst;
    RDB_type *typ = RDB_obj_type(srcp);

    if (typ != NULL && RDB_type_is_scalar(typ)) {
         if (!RDB_type_is_builtin(srcp->typ))
             return RDB_NOT_SUPPORTED; /* !! */
         if (srcp->typ == &RDB_STRING) {
             ret = append_str(objp, "\"");
             if (ret != RDB_OK)
                 return ret;
         }
         RDB_init_obj(&dst);
         ret = RDB_obj_to_string(&dst, srcp);
         if (ret != RDB_OK) {
             RDB_destroy_obj(&dst);
             return ret;
         }
         ret = append_str(objp, dst.var.bin.datap);
         RDB_destroy_obj(&dst);
         if (ret != RDB_OK)
             return ret;
         if (srcp->typ == &RDB_STRING) {
             ret = append_str(objp, "\"");
             if (ret != RDB_OK)
                 return ret;
         }
    } else {
        switch (srcp->kind) {
            case RDB_OB_TUPLE:
                ret = append_tuple(objp, srcp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_OB_TABLE:
                ret = append_table(objp, srcp->var.tbp, 0);
                if (ret != RDB_OK)
                    return ret;
                break;
            default:
                return RDB_NOT_SUPPORTED;
        }
    }
    return RDB_OK;
}

static int
append_ex(RDB_object *objp, RDB_expression *exp)
{
    int ret;
    int i;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            ret = append_obj(objp, &exp->var.obj);
             if (ret != RDB_OK)
                 return ret;
            break;
        case RDB_EX_ATTR:
             ret = append_str(objp, exp->var.attrname);
             if (ret != RDB_OK)
                 return ret;
            break;
        case RDB_EX_TUPLE_ATTR:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ").");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, exp->var.op.name);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_GET_COMP:
            ret = append_str(objp, "THE_");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, exp->var.op.name);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_RO_OP:
            if (strcmp(exp->var.op.name, "=") == 0
                    || strcmp(exp->var.op.name, "<>") == 0
                    || strcmp(exp->var.op.name, "<") == 0
                    || strcmp(exp->var.op.name, ">") == 0
                    || strcmp(exp->var.op.name, "<=") == 0
                    || strcmp(exp->var.op.name, ">=") == 0
                    || strcmp(exp->var.op.name, "+") == 0
                    || strcmp(exp->var.op.name, "-") == 0
                    || strcmp(exp->var.op.name, "||") == 0
                    || strcmp(exp->var.op.name, "MATCHES") == 0
                    || strcmp(exp->var.op.name, "AND") == 0
                    || strcmp(exp->var.op.name, "OR") == 0
                    || strcmp(exp->var.op.name, "IN") == 0
                    || strcmp(exp->var.op.name, "SUBSET_OF") == 0) {
                ret = append_str(objp, "(");
                if (ret != RDB_OK)
                    return ret;
                ret = append_ex(objp, exp->var.op.argv[0]);
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, ") ");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, exp->var.op.name);
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, " (");
                if (ret != RDB_OK)
                    return ret;
                ret = append_ex(objp, exp->var.op.argv[1]);
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, ")");
                if (ret != RDB_OK)
                    return ret;
            } else {
                ret = append_str(objp, exp->var.op.name);
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, "(");
                if (ret != RDB_OK)
                    return ret;
                for (i = 0; i < exp->var.op.argc; i++) {
                    if (i > 0) {
                        ret = append_str(objp, ", ");
                        if (ret != RDB_OK)
                            return ret;
                    }
                    ret = append_ex(objp, exp->var.op.argv[i]);
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, ")");
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_EX_AGGREGATE:
            ret = append_aggr_op(objp, exp->var.op.op);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " (");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            if (exp->var.op.op != RDB_COUNT) {
                ret = append_str(objp, " ,");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, exp->var.op.name);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
    }
    return RDB_OK;
}

static int
append_table(RDB_object *objp, RDB_table *tbp, int options)
{
    int ret;
    int i;

    switch (tbp->kind) {
        case RDB_TB_REAL:
        {
            if (tbp->is_persistent) {
                ret = append_str(objp, RDB_table_name(tbp));
                if (ret != RDB_OK)
                    return ret;
            } else {
                RDB_object arr;
                RDB_object *tplp;

                if (RDB_table_name(tbp) != NULL)
                    return RDB_INVALID_ARGUMENT;
                ret = append_str(objp, "RELATION { ");
                if (ret != RDB_OK)
                    return ret;

                RDB_init_obj(&arr);
                ret = RDB_table_to_array(&arr, tbp, 0, NULL, NULL);
                if (ret != RDB_OK)
                    return ret;

                for (i = 0;
                        (ret = RDB_array_get(&arr, (RDB_int) i, &tplp)) == RDB_OK;
                        i++) {
                    if (i > 0) {
                        ret = append_str(objp, ", ");
                        if (ret != RDB_OK) {
                            RDB_destroy_obj(&arr);
                            return ret;
                        }
                    }

                    ret = append_tuple(objp, tplp);
                    if (ret != RDB_OK) {
                        RDB_destroy_obj(&arr);
                        return ret;
                    }
                }
                RDB_destroy_obj(&arr);
                if (ret != RDB_NOT_FOUND)
                    return ret;
                ret = append_str(objp, "}");
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        }
        case RDB_TB_SELECT:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.select.tbp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") WHERE (");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, tbp->var.select.exp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNION:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var._union.tb1p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") UNION (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var._union.tb2p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_MINUS:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.minus.tb1p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") MINUS (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.minus.tb2p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_INTERSECT:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.intersect.tb1p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") INTERSECT (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.intersect.tb2p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_JOIN:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.join.tb1p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") JOIN (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.join.tb2p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_EXTEND:
            ret = append_str(objp, "EXTEND ");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.extend.tbp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " ADD (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.extend.attrc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                    return ret;
                }
                ret = append_ex(objp, tbp->var.extend.attrv[i].exp);
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, " AS ");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, tbp->var.extend.attrv[i].name);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.project.tbp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") {");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->typ->var.basetyp->var.tuple.attrc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                    return ret;
                }
                ret = append_str(objp,
                        tbp->typ->var.basetyp->var.tuple.attrv[i].name);
                if (ret != RDB_OK)
                return ret;
            }
            ret = append_str(objp, "}");
            if (ret != RDB_OK)
                return ret;
            if (RDB_SHOW_INDEX & options) {
                if (tbp->var.project.indexp != NULL) {
                    ret = append_str(objp, " INDEX ");
                    if (ret != RDB_OK)
                        return ret;
                    ret = append_str(objp, tbp->var.project.indexp->name);
                    if (ret != RDB_OK)
                        return ret;
                }
            }
            break;
        case RDB_TB_RENAME:
            ret = append_table(objp, tbp->var.rename.tbp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " RENAME (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.rename.renc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                    return ret;
                }
                ret = append_str(objp, tbp->var.rename.renv[i].from);
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, " AS ");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, tbp->var.rename.renv[i].to);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SUMMARIZE:
            ret = append_str(objp, "SUMMARIZE ");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.summarize.tb1p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " PER ");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.summarize.tb2p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " ADD (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.summarize.addc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_aggr_op(objp, tbp->var.summarize.addv[i].op);
                if (ret != RDB_OK)
                    return ret;
                if (tbp->var.summarize.addv[i].op != RDB_COUNT) {
                    ret = append_str(objp, " (");
                    if (ret != RDB_OK)
                        return ret;
                    ret = append_ex(objp, tbp->var.summarize.addv[i].exp);
                    if (ret != RDB_OK)
                        return ret;
                    ret = append_str(objp, ")");
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, " AS ");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, tbp->var.summarize.addv[i].name);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_WRAP:
            ret = append_table(objp, tbp->var.wrap.tbp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " WRAP (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.wrap.wrapc; i++) {
                int j;

                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, "{");
                if (ret != RDB_OK)
                    return ret;
                for (j = 0; j < tbp->var.wrap.wrapv[i].attrc; j++) {
                    if (j > 0) {
                        ret = append_str(objp, ", ");
                        if (ret != RDB_OK)
                            return ret;
                    }
                    ret = append_str(objp, tbp->var.wrap.wrapv[i].attrv[j]);
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, "} AS ");
                if (ret != RDB_OK)
                    return ret;                    
                ret = append_str(objp, tbp->var.wrap.wrapv[i].attrname);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNWRAP:
            ret = append_table(objp, tbp->var.unwrap.tbp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " UNWRAP (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.unwrap.attrc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, tbp->var.unwrap.attrv[i]);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SDIVIDE:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.sdivide.tb1p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") DIVIDEBY (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.sdivide.tb2p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") PER (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.sdivide.tb1p, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_GROUP:
            ret = append_table(objp, tbp->var.group.tbp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " GROUP {");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.group.attrc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                    return ret;
                }
                ret = append_str(objp, tbp->var.group.attrv[i]);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, "} AS ");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, tbp->var.group.gattr);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNGROUP:
            ret = append_table(objp, tbp->var.ungroup.tbp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " UNGROUP ");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, tbp->var.ungroup.attr);
            if (ret != RDB_OK)
                return ret;
            break;
    }
    return RDB_OK;
}

int
_RDB_table_to_str(RDB_object *objp, RDB_table *tbp, int options)
{
    int ret;

    ret = RDB_string_to_obj(objp, "");
    if (ret != RDB_OK)
        return ret;

    return append_table(objp, tbp, options);
}
