/*
 * $Id$
 *
 * Copyright (C) 2004, 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "tabletostr.h"
#include <string.h>
#include <errno.h>
#include <rel/internal.h>
#include <gen/hashtabit.h>

static int
append_str(RDB_object *objp, const char *str)
{
    int len = objp->var.bin.len + strlen(str);
    char *nstr = realloc(objp->var.bin.datap, len);
    if (nstr == NULL)
        return ENOMEM;

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
append_obj(RDB_object *objp, const RDB_object *srcp, RDB_exec_context *,
        RDB_transaction *);

static int
append_table(RDB_object *, RDB_table *, RDB_exec_context *,
        RDB_transaction *, int options);

static int
append_tuple(RDB_object *objp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_hashtable_iter hiter;
    tuple_entry *entryp;
    RDB_bool start = RDB_TRUE;

    ret = append_str(objp, "TUPLE { ");
    if (ret != RDB_OK)
        return ret;

    RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tplp->var.tpl_tab);
    for (;;) {
        /* Get next attribute */
        entryp = RDB_hashtable_next(&hiter);
        if (entryp == NULL)
            break;

        if (start) {
            start = RDB_FALSE;
        } else {
            ret = append_str(objp, ", ");
            if (ret != RDB_OK)
                goto error;
        }

        ret = append_str(objp, entryp->key);
        if (ret != RDB_OK)
            goto error;

        ret = append_str(objp, " ");
        if (ret != RDB_OK)
            goto error;

        ret = append_obj(objp, &entryp->obj, ecp, txp);
        if (ret != RDB_OK)
            goto error;
    }
    ret = append_str(objp, " }");
    if (ret != RDB_OK)
        return ret;

    RDB_destroy_hashtable_iter(&hiter);
    return RDB_OK;

error:
    RDB_destroy_hashtable_iter(&hiter);
    return ret;
}

static int
append_quoted_string(RDB_object *objp, const RDB_object *strp)
{
    int ret;
    int i;
    size_t qlen;
    char *qstr = malloc((strp->var.bin.len + 2) * 2);

    if (qstr == NULL)
        return ENOMEM;

    qstr[0] = '\"';
    qlen = 1;
    for (i = 0; i < strp->var.bin.len - 1; i++) {
        switch (((char *)strp->var.bin.datap)[i]) {
            case '\"':
                qstr[qlen++] = '\\';
                qstr[qlen++] = '\"';
                break;
            case '\\':
                qstr[qlen++] = '\\';
                qstr[qlen++] = '\\';
                break;
            case '\n':
                qstr[qlen++] = '\\';
                qstr[qlen++] = 'n';
                break;
            case '\r':
                qstr[qlen++] = '\\';
                qstr[qlen++] = 'r';
                break;
            case '\t':
                qstr[qlen++] = '\\';
                qstr[qlen++] = 't';
                break;
            default:
                qstr[qlen++] = ((char *)strp->var.bin.datap)[i];
        }
    }
    qstr[qlen++] = '\"';
    qstr[qlen] = '\0';

    ret = append_str(objp, qstr);
    free(qstr);
    return ret;
}

static int
append_utype_obj(RDB_object *objp, const RDB_object *srcp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int compc;
    int i;
    int ret;
    RDB_possrep *possrep = &srcp->typ->var.scalar.repv[0]; /* Take 1st possrep */

    ret = append_str(objp, possrep->name);
    if (ret != RDB_OK)
        return ret;
    ret = append_str(objp, "(");
    if (ret != RDB_OK)
        return ret;

    compc = possrep->compc;
    for (i = 0; i < compc; i++) {
        RDB_object compobj;

        if (i > 0) {
            ret = append_str(objp, ", ");
            if (ret != RDB_OK)
                return ret;
        }

        RDB_init_obj(&compobj);
        ret = RDB_obj_comp(srcp, possrep->compv[i].name, &compobj, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&compobj, ecp);
            return ret;
        }
        ret = append_obj(objp, &compobj, ecp, txp);
        RDB_destroy_obj(&compobj, ecp);
        if (ret != RDB_OK)
            return ret;
    }
    return append_str(objp, ")");
}

static int
append_obj(RDB_object *objp, const RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_type *typ = RDB_obj_type(srcp);

    if (typ != NULL && RDB_type_is_scalar(typ)) {
         if (!RDB_type_is_builtin(srcp->typ))
             ret = append_utype_obj(objp, srcp, ecp, txp);
         else if (srcp->typ == &RDB_STRING)
             ret = append_quoted_string(objp, srcp);
         else {
             RDB_object dst;

             RDB_init_obj(&dst);
             ret = RDB_obj_to_string(&dst, srcp, ecp);
             if (ret != RDB_OK) {
                 RDB_destroy_obj(&dst, ecp);
                 return ret;
             }
             ret = append_str(objp, dst.var.bin.datap);
             RDB_destroy_obj(&dst, ecp);
         }
         if (ret != RDB_OK) {
             return RDB_ERROR;
         }
    } else {
        switch (srcp->kind) {
            case RDB_OB_TUPLE:
                ret = append_tuple(objp, srcp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_OB_TABLE:
                ret = append_table(objp, srcp->var.tbp, ecp, txp, 0);
                if (ret != RDB_OK)
                    return ret;
                break;
            default:
                RDB_raise_not_supported("", ecp);
                return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
append_ex(RDB_object *objp, const RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            ret = append_obj(objp, &exp->var.obj, ecp, txp);
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
            ret = append_ex(objp, exp->var.op.argv[0], ecp, txp);
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
            ret = append_ex(objp, exp->var.op.argv[0], ecp, txp);
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
                ret = append_ex(objp, exp->var.op.argv[0], ecp, txp);
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
                ret = append_ex(objp, exp->var.op.argv[1], ecp, txp);
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
                    ret = append_ex(objp, exp->var.op.argv[i], ecp, txp);
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
            ret = append_ex(objp, exp->var.op.argv[0], ecp, txp);
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
append_table(RDB_object *objp, RDB_table *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, int options)
{
    int ret;
    int i;

    switch (tbp->kind) {
        case RDB_TB_REAL:
        {
            if (tbp->name != NULL) {
                ret = append_str(objp, RDB_table_name(tbp));
                if (ret != RDB_OK)
                    return ret;
            } else {
                RDB_object arr;
                RDB_object *tplp;

                if (RDB_table_name(tbp) != NULL) {
                    RDB_raise_invalid_argument("real table must be named", ecp);
                    return RDB_ERROR;
                }
                ret = append_str(objp, "RELATION { ");
                if (ret != RDB_OK)
                    return RDB_ERROR;

                RDB_init_obj(&arr);
                ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, NULL);
                if (ret != RDB_OK)
                    return RDB_ERROR;

                for (i = 0;
                        (tplp = RDB_array_get(&arr, (RDB_int) i, ecp)) != NULL;
                        i++) {
                    if (i > 0) {
                        ret = append_str(objp, ", ");
                        if (ret != RDB_OK) {
                            RDB_destroy_obj(&arr, ecp);
                            return ret;
                        }
                    }

                    ret = append_tuple(objp, tplp, ecp, txp);
                    if (ret != RDB_OK) {
                        RDB_destroy_obj(&arr, ecp);
                        return ret;
                    }
                }
                RDB_destroy_obj(&arr, ecp);
                if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                    return RDB_ERROR;
                RDB_clear_err(ecp);
                ret = append_str(objp, "}");
                if (ret != RDB_OK)
                    return RDB_ERROR;
            }
            break;
        }
        case RDB_TB_SELECT:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.select.tbp, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") WHERE (");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, tbp->var.select.exp, ecp, txp);
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
            ret = append_table(objp, tbp->var._union.tb1p, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") UNION (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var._union.tb2p, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.minus.tb1p, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") MINUS (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.minus.tb2p, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.intersect.tb1p, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") INTERSECT (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.intersect.tb2p, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.join.tb1p, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") JOIN (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.join.tb2p, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.extend.tbp, ecp, txp, options);
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
                ret = append_ex(objp, tbp->var.extend.attrv[i].exp, ecp, txp);
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
            ret = append_table(objp, tbp->var.project.tbp, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.rename.tbp, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.summarize.tb1p, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " PER ");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.summarize.tb2p, ecp, txp,
                    options);
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
                    ret = append_ex(objp, tbp->var.summarize.addv[i].exp,
                            ecp, txp);
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
            ret = append_table(objp, tbp->var.wrap.tbp, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.unwrap.tbp, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.sdivide.tb1p, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") DIVIDEBY (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.sdivide.tb2p, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") PER (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.sdivide.tb1p, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_GROUP:
            ret = append_table(objp, tbp->var.group.tbp, ecp, txp, options);
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
            ret = append_table(objp, tbp->var.ungroup.tbp, ecp, txp, options);
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
_RDB_table_to_str(RDB_object *objp, RDB_table *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, int options)
{
    int ret;

    ret = RDB_string_to_obj(objp, "", ecp);
    if (ret != RDB_OK)
        return ret;

    return append_table(objp, tbp, ecp, txp, options);
}

int
_RDB_obj_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    ret = RDB_string_to_obj(dstp, "", ecp);
    if (ret != RDB_OK)
        return ret;

    return append_obj(dstp, srcp, ecp, txp);
}

int
_RDB_expr_to_str(RDB_object *dstp, const RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    ret = RDB_string_to_obj(dstp, "", ecp);
    if (ret != RDB_OK)
        return ret;

    return append_ex(dstp, exp, ecp, txp);
}
