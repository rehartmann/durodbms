/*
 * $Id$
 *
 * Copyright (C) 2004-2009 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "tabletostr.h"
#include <string.h>
#include <errno.h>
#include <rel/internal.h>
#include <rel/stable.h>
#include <gen/hashtabit.h>

static int
append_obj(RDB_object *objp, const RDB_object *srcp, RDB_exec_context *,
        RDB_transaction *);

static int
append_table_def(RDB_object *, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, int options);

static int
append_tuple(RDB_object *objp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_hashtable_iter hiter;
    tuple_entry *entryp;
    RDB_bool start = RDB_TRUE;

    if (RDB_append_string(objp, "TUPLE { ", ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    /* Tuple can be empty */
    if (tplp->kind != RDB_OB_INITIAL) {
        RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tplp->var.tpl_tab);
        for (;;) {
            /* Get next attribute */
            entryp = RDB_hashtable_next(&hiter);
            if (entryp == NULL)
                break;
    
            if (start) {
                start = RDB_FALSE;
            } else {
                ret = RDB_append_string(objp, ", ", ecp);
                if (ret != RDB_OK) {
                    goto error;
                }
            }
    
            ret = RDB_append_string(objp, entryp->key, ecp);
            if (ret != RDB_OK) {
                goto error;
            }
    
            ret = RDB_append_string(objp, " ", ecp);
            if (ret != RDB_OK) {
                goto error;
            }
    
            ret = append_obj(objp, &entryp->obj, ecp, txp);
            if (ret != RDB_OK)
                goto error;
        }
    }
    ret = RDB_append_string(objp, " }", ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    RDB_destroy_hashtable_iter(&hiter);
    return RDB_OK;

error:
    if (objp->kind != RDB_OB_INITIAL)
        RDB_destroy_hashtable_iter(&hiter);
    return RDB_ERROR;
}

static int
append_quoted_string(RDB_object *objp, const RDB_object *strp,
        RDB_exec_context *ecp)
{
    int ret;
    int i;
    size_t qlen;
    char *qstr = RDB_alloc((strp->var.bin.len + 2) * 2, ecp);

    if (qstr == NULL)
        return RDB_ERROR;

    qstr[0] = '\'';
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
    qstr[qlen++] = '\'';
    qstr[qlen] = '\0';

    ret = RDB_append_string(objp, qstr, ecp);
    RDB_free(qstr);
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

    ret = RDB_append_string(objp, possrep->name, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    ret = RDB_append_string(objp, "(", ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    compc = possrep->compc;
    for (i = 0; i < compc; i++) {
        RDB_object compobj;

        if (i > 0) {
            ret = RDB_append_string(objp, ", ", ecp);
            if (ret != RDB_OK)
                return RDB_ERROR;
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
    return RDB_append_string(objp, ")", ecp);
}

static int
append_table_val(RDB_object *objp, const RDB_object *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    RDB_object arr;
    RDB_object *tplp;

    if (RDB_append_string(objp, "RELATION { ", ecp) != RDB_OK)
        return RDB_ERROR;

    RDB_init_obj(&arr);
    if (RDB_table_to_array(&arr, (RDB_object *) tbp, 0, NULL, 0, ecp, txp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    for (i = 0;
            (tplp = RDB_array_get(&arr, (RDB_int) i, ecp)) != NULL;
            i++) {
        if (i > 0) {
            if (RDB_append_string(objp, ", ", ecp) != RDB_OK) {
                RDB_destroy_obj(&arr, ecp);
                return RDB_ERROR;
            }
        }

        if (append_tuple(objp, tplp, ecp, txp) != RDB_OK) {
            RDB_destroy_obj(&arr, ecp);
            return RDB_ERROR;
        }
    }
    RDB_destroy_obj(&arr, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        return RDB_ERROR;
    RDB_clear_err(ecp);
    return RDB_append_string(objp, "}", ecp);
}

static int
append_obj(RDB_object *objp, const RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_type *typ = RDB_obj_type(srcp);

    if (typ != NULL && RDB_type_is_scalar(typ)) {
         if (srcp->typ->var.scalar.repc > 0)
             ret = append_utype_obj(objp, srcp, ecp, txp);
         else if (srcp->typ == &RDB_STRING)
             ret = append_quoted_string(objp, srcp, ecp);
         else {
             RDB_object dst;

             RDB_init_obj(&dst);
             ret = RDB_obj_to_string(&dst, srcp, ecp);
             if (ret != RDB_OK) {
                 RDB_destroy_obj(&dst, ecp);
                 return ret;
             }
             ret = RDB_append_string(objp, dst.var.bin.datap, ecp);
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
                ret = append_table_val(objp, srcp, ecp, txp);
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
        RDB_transaction *txp, int options)
{
    int ret;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            ret = append_obj(objp, &exp->var.obj, ecp, txp);
            if (ret != RDB_OK)
                 return ret;
            break;
        case RDB_EX_TBP:
            ret = append_table_def(objp, exp->var.tbref.tbp, ecp, txp, options);
            if (ret != RDB_OK)
                 return ret;
            if (exp->var.tbref.indexp != NULL) {
                ret = RDB_append_string(objp, " INDEX ", ecp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
                ret = RDB_append_string(objp, exp->var.tbref.indexp->name, ecp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
            }            
            break;            
        case RDB_EX_VAR:
             ret = RDB_append_string(objp, exp->var.varname, ecp);
             if (ret != RDB_OK)
                 return ret;
            break;
        case RDB_EX_TUPLE_ATTR:
            ret = RDB_append_string(objp, "(", ecp);
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.args.firstp, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, ").", ecp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, exp->var.op.name, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, ")", ecp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_GET_COMP:
            ret = RDB_append_string(objp, "THE_", ecp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, exp->var.op.name, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, "(", ecp);
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.args.firstp, ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, ")", ecp);
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
                if (RDB_append_string(objp, "(", ecp) != RDB_OK)
                    return RDB_ERROR;
                if (append_ex(objp, exp->var.op.args.firstp, ecp, txp,
                        options) != RDB_OK)
                    return RDB_ERROR;
                ret = RDB_append_string(objp, ") ", ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = RDB_append_string(objp, exp->var.op.name, ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = RDB_append_string(objp, " (", ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = append_ex(objp, exp->var.op.args.firstp->nextp, ecp,
                        txp, options);
                if (ret != RDB_OK)
                    return ret;
                ret = RDB_append_string(objp, ")", ecp);
                if (ret != RDB_OK)
                    return ret;
            } else {
                RDB_expression *argp;

                if (RDB_append_string(objp, exp->var.op.name, ecp) != RDB_OK)
                    return RDB_ERROR;
                if (RDB_append_string(objp, "(", ecp) != RDB_OK)
                    return RDB_ERROR;
                argp = exp->var.op.args.firstp;
                while (argp != NULL) {
                    if (append_ex(objp, argp, ecp, txp, options) != RDB_OK)
                        return RDB_ERROR;
                    argp = argp->nextp;
                    if (argp != NULL) {
                        if (RDB_append_string(objp, ", ", ecp) != RDB_OK)
                            return RDB_ERROR;
                    }
                }
                if (RDB_append_string(objp, ")", ecp) != RDB_OK)
                    return RDB_ERROR;
            }
            break;
    }
    return RDB_OK;
}

static int
append_table_def(RDB_object *objp, const RDB_object *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, int options)
{
    if (RDB_table_name(tbp) != NULL) {
        return RDB_append_string(objp, RDB_table_name(tbp), ecp);
    }
    if (tbp->var.tb.exp == NULL) {
        return append_table_val(objp, tbp, ecp, txp);
    }
    if (RDB_append_string(objp, "(", ecp) != RDB_OK)
        return RDB_ERROR;

    if (append_ex(objp, tbp->var.tb.exp, ecp, txp, options) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(objp, ")", ecp);
}

int
_RDB_obj_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (RDB_string_to_obj(dstp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    return append_obj(dstp, srcp, ecp, txp);
}

int
_RDB_table_def_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp, RDB_transaction *txp, int options)
{
    if (RDB_string_to_obj(dstp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    return append_table_def(dstp, srcp, ecp, txp, options);
}

int
_RDB_expr_to_str(RDB_object *dstp, const RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp, int options)
{
    if (RDB_string_to_obj(dstp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    return append_ex(dstp, exp, ecp, txp, options);
}
