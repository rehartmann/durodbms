/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include <string.h>
#include <stdlib.h>

static void
unbalance_and(RDB_expression *exp)
{
    RDB_expression *axp;

    if (exp->kind != RDB_EX_AND)
        return;

    if (exp->var.op.arg1->kind == RDB_EX_AND)
        unbalance_and(exp->var.op.arg1);
        
    if (exp->var.op.arg2->kind == RDB_EX_AND) {
        unbalance_and(exp->var.op.arg2);
        if (exp->var.op.arg1->kind == RDB_EX_AND) {
            RDB_expression *ax2p;

            /* find leftmost factor */
            axp = exp->var.op.arg1;
            while (axp->var.op.arg1->kind == RDB_EX_AND)
                axp = axp->var.op.arg1;

            /* switch leftmost factor and right child */
            ax2p = exp->var.op.arg2;
            exp->var.op.arg2 = axp->var.op.arg1;
            axp->var.op.arg1 = ax2p;
        } else {
            /* swap children */
            axp = exp->var.op.arg1;
            exp->var.op.arg1 = exp->var.op.arg2;
            exp->var.op.arg2 = axp;
        }
    }
}

static RDB_bool
expr_eq_attr(RDB_expression *exp, const char *attrname)
{
    if (exp->kind == RDB_EX_ATTR
            && strcmp(exp->var.attr.name, attrname) == 0)
        return RDB_TRUE;
    if (exp->kind == RDB_EX_EQ) {
        if (exp->var.op.arg1->kind == RDB_EX_ATTR
                && strcmp(exp->var.op.arg1->var.attr.name, attrname) == 0)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

static RDB_expression *
attr_exp(RDB_expression *exp, const char *attrname)
{
    if (exp->kind != RDB_EX_AND) {
        if (expr_eq_attr(exp, attrname))
           return exp;
        else
           return NULL;
    }

    for (;;) {    
        if (expr_eq_attr(exp->var.op.arg2, attrname))
            return exp;
        if (exp->var.op.arg1->kind != RDB_EX_AND) {
            if (expr_eq_attr(exp->var.op.arg1, attrname))
                return exp;
            else
                return NULL;
        }
        exp = exp->var.op.arg1;        
    }
}

/* 
 * Check if the index specified by indexp can be used for the selection
 * specified by tbp. If yes, return the estimated cost.
 * If no, return INT_MAX.
 */
static int
eval_index(RDB_table *tbp, _RDB_tbindex *indexp)
{
    int i;
    RDB_expression *exp;

    for (i = 0; i < indexp->attrc; i++) {
        if (attr_exp(exp = tbp->var.select.exp,
                indexp->attrv[i].attrname) == NULL)
            return INT_MAX;
    }
    return indexp->idxp == NULL ? 1 : 2;
}

static int
split_by_index(RDB_table *tbp, _RDB_tbindex *indexp)
{
    int ret;
    RDB_expression *exp;
    RDB_expression *ixexp;
    RDB_bool dosplit;

    exp = attr_exp(tbp->var.select.exp, indexp->attrv[0].attrname);
    if (exp->kind != RDB_EX_AND) {        
        dosplit = RDB_FALSE;
        ixexp = exp;
    } else {
        RDB_expression *prevp;

        dosplit = RDB_TRUE;
        if (exp == tbp->var.select.exp) {
            prevp = NULL;
        } else {
            prevp = tbp->var.select.exp;
            while (prevp->var.op.arg1 != exp)
                prevp = prevp->var.op.arg1;
        }

        if (expr_eq_attr(exp->var.op.arg1, indexp->attrv[0].attrname)) {
            ixexp = exp->var.op.arg1;
            if (prevp != NULL) {
                prevp->var.op.arg1 = exp->var.op.arg2;
            } else {
                tbp->var.select.exp = exp->var.op.arg2;
            }
            free(exp);            
        } else {
            ixexp = exp->var.op.arg2;
            if (prevp != NULL) {
                prevp->var.op.arg1 = exp->var.op.arg1;
            } else {
                tbp->var.select.exp = exp->var.op.arg1;
            }
            free(exp);
        }
    }

    if (dosplit) {
        RDB_table *sitbp;

        ret = RDB_select(tbp->var.select.tbp, ixexp, &sitbp);
        if (ret != RDB_OK)
            return ret;

        sitbp->kind = RDB_TB_SELECT_INDEX;
        sitbp->var.select.indexp = indexp;
        sitbp->optimized = RDB_TRUE;
        tbp->var.select.tbp = sitbp;
    } else {
        tbp->kind = RDB_TB_SELECT_INDEX;
        tbp->var.select.indexp = indexp;
        tbp->var.select.exp = ixexp;
    }
    return RDB_OK;
}

static int
optimize_select(RDB_table *tbp, RDB_transaction *txp)
{
    int i;
    RDB_table *stbp;
    int idx;
    int bestcost = INT_MAX;

    if (tbp->var.select.tbp->kind != RDB_TB_STORED)
        return RDB_OK;

    stbp = tbp->var.select.tbp;

    /*
     * Convert condition into 'unbalanced' form
     */
    unbalance_and(tbp->var.select.exp);

    /*
     * Try to find best index
     */
    for (i = 0; i < stbp->var.stored.indexc; i++) {
        if (stbp->var.stored.indexv[i].attrc > 1
                || !stbp->var.stored.indexv[i].unique)
            continue;

        int cost = eval_index(tbp, &stbp->var.stored.indexv[i]);

        if (cost < bestcost) {
            bestcost = cost;
            idx = i;
        }
    }

    if (bestcost == INT_MAX) {
        return RDB_OK;
    }

    return split_by_index(tbp, &stbp->var.stored.indexv[idx]);
}

int
_RDB_optimize(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            break;
        case RDB_TB_MINUS:
            ret = _RDB_optimize(tbp->var.minus.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_optimize(tbp->var.minus.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNION:
            ret = _RDB_optimize(tbp->var._union.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_optimize(tbp->var._union.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_INTERSECT:
            ret = _RDB_optimize(tbp->var.intersect.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_optimize(tbp->var.intersect.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_INDEX:
            ret = optimize_select(tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_JOIN:
            ret = _RDB_optimize(tbp->var.join.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_optimize(tbp->var.join.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_EXTEND:
            ret = _RDB_optimize(tbp->var.extend.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            ret = _RDB_optimize(tbp->var.project.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SUMMARIZE:
            ret = _RDB_optimize(tbp->var.summarize.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_optimize(tbp->var.summarize.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_RENAME:
            ret = _RDB_optimize(tbp->var.rename.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_WRAP:
            ret = _RDB_optimize(tbp->var.wrap.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNWRAP:
            ret = _RDB_optimize(tbp->var.unwrap.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_GROUP:
            ret = _RDB_optimize(tbp->var.group.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNGROUP:
            ret = _RDB_optimize(tbp->var.ungroup.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SDIVIDE:
            ret = _RDB_optimize(tbp->var.sdivide.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_optimize(tbp->var.sdivide.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_optimize(tbp->var.sdivide.tb3p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
    }
    tbp->optimized = RDB_TRUE;
    return RDB_OK;
}
