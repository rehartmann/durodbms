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
                && strcmp(exp->var.op.arg1->var.attr.name, attrname) == 0
                && exp->var.op.arg2->kind == RDB_EX_OBJ)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

RDB_expression *
_RDB_attr_node(RDB_expression *exp, const char *attrname)
{
    while (exp->kind == RDB_EX_AND) {
        if (expr_eq_attr(exp->var.op.arg2, attrname))
            return exp;
        exp = exp->var.op.arg1;
    }
    if (expr_eq_attr(exp, attrname))
        return exp;
    return NULL;
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
        if (_RDB_attr_node(exp = tbp->var.select.exp,
                indexp->attrv[i].attrname) == NULL)
            return INT_MAX;
    }
    return indexp->idxp == NULL ? 1 : 2;
}

static int
split_by_index(RDB_table *tbp, _RDB_tbindex *indexp)
{
    int ret;
    int i;
    RDB_expression *prevp;
    RDB_expression *nodep;
    RDB_expression *ixexp = NULL;

    for (i = 0; i < indexp->attrc; i++) {
        nodep = _RDB_attr_node(tbp->var.select.exp, indexp->attrv[i].attrname);

        /* Get previous node */
        if (nodep == tbp->var.select.exp) {
            prevp = NULL;
        } else {
            prevp = tbp->var.select.exp;
            while (prevp->var.op.arg1 != nodep)
                prevp = prevp->var.op.arg1;
        }

        if (nodep->kind != RDB_EX_AND) {
            if (ixexp == NULL)
                ixexp = nodep;
            else
                ixexp = RDB_and(ixexp, nodep);
            if (prevp == NULL) {
                tbp->var.select.exp = NULL;
            } else {
                if (prevp == tbp->var.select.exp) {
                    tbp->var.select.exp = prevp->var.op.arg2;
                } else {
                    RDB_expression *pprevp = tbp->var.select.exp;

                    while (pprevp->var.op.arg1 != prevp)
                        pprevp = pprevp->var.op.arg1;
                    pprevp->var.op.arg1 = prevp->var.op.arg2;
                }
                free(prevp);
            }
        } else {
            if (ixexp == NULL)
                ixexp = nodep->var.op.arg2;
            else
                ixexp = RDB_and(ixexp, nodep->var.op.arg2);
            if (prevp == NULL)
                tbp->var.select.exp = nodep->var.op.arg1;
            else
                prevp->var.op.arg1 = nodep->var.op.arg1;
            free(nodep);
        }
    }

    if (tbp->var.select.exp != NULL) {
        RDB_table *sitbp;

        /*
         * Split table into two
         */
        ret = RDB_select(tbp->var.select.tbp, ixexp, &sitbp);
        if (ret != RDB_OK)
            return ret;

        sitbp->kind = RDB_TB_SELECT_INDEX;
        sitbp->var.select.indexp = indexp;
        sitbp->optimized = RDB_TRUE;
        tbp->var.select.tbp = sitbp;
    } else {
        /*
         * Convert table to RDB_SELECT_INDEX
         */
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
        if (!stbp->var.stored.indexv[i].unique)
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

static int
resolve_views(RDB_table *tbp);

static int
dup_named(RDB_table **tbpp)
{
    RDB_table *newtbp;

    /*
     * Duplicate table, if it has a name, otherwise call resolve_views().
     */
    if (RDB_table_name(*tbpp) != NULL) {
        newtbp = _RDB_dup_vtable(*tbpp);
        if (newtbp == NULL)
            return RDB_NO_MEMORY;
        *tbpp = newtbp;
        return RDB_OK;
    }
    return resolve_views(*tbpp);
}

static int
resolve_views(RDB_table *tbp)
{
    int ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            break;
        case RDB_TB_MINUS:
            ret = dup_named(&tbp->var.minus.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = dup_named(&tbp->var.minus.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNION:
            ret = dup_named(&tbp->var._union.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = dup_named(&tbp->var._union.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_INTERSECT:
            ret = dup_named(&tbp->var.intersect.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = dup_named(&tbp->var.intersect.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_INDEX:
            ret = dup_named(&tbp->var.select.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_JOIN:
            ret = dup_named(&tbp->var.join.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = dup_named(&tbp->var.join.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_EXTEND:
            ret = dup_named(&tbp->var.extend.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            ret = dup_named(&tbp->var.project.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SUMMARIZE:
            ret = dup_named(&tbp->var.summarize.tb1p);
            if (ret != RDB_OK)
                return ret;
            break;
            ret = dup_named(&tbp->var.summarize.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_RENAME:
            ret = dup_named(&tbp->var.rename.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_WRAP:
            ret = dup_named(&tbp->var.wrap.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNWRAP:
            ret = dup_named(&tbp->var.unwrap.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_GROUP:
            ret = dup_named(&tbp->var.group.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNGROUP:
            ret = dup_named(&tbp->var.ungroup.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SDIVIDE:
            ret = dup_named(&tbp->var.sdivide.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = dup_named(&tbp->var.sdivide.tb2p);
            if (ret != RDB_OK)
                return ret;
            ret = dup_named(&tbp->var.sdivide.tb3p);
            if (ret != RDB_OK)
                return ret;
            break;
    }
    return RDB_OK;
}

static int
optimize(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            break;
        case RDB_TB_MINUS:
            ret = optimize(tbp->var.minus.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = optimize(tbp->var.minus.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNION:
            ret = optimize(tbp->var._union.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = optimize(tbp->var._union.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_INTERSECT:
            ret = optimize(tbp->var.intersect.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = optimize(tbp->var.intersect.tb2p, txp);
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
            ret = optimize(tbp->var.join.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = optimize(tbp->var.join.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_EXTEND:
            ret = optimize(tbp->var.extend.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            ret = optimize(tbp->var.project.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SUMMARIZE:
            ret = optimize(tbp->var.summarize.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = optimize(tbp->var.summarize.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_RENAME:
            ret = optimize(tbp->var.rename.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_WRAP:
            ret = optimize(tbp->var.wrap.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNWRAP:
            ret = optimize(tbp->var.unwrap.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_GROUP:
            ret = optimize(tbp->var.group.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNGROUP:
            ret = optimize(tbp->var.ungroup.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SDIVIDE:
            ret = optimize(tbp->var.sdivide.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = optimize(tbp->var.sdivide.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = optimize(tbp->var.sdivide.tb3p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
    }
    tbp->optimized = RDB_TRUE;

    return RDB_OK;
}

int
_RDB_optimize(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    ret = resolve_views(tbp);
    if (ret != RDB_OK)
        return ret;

    ret = _RDB_transform(tbp);
    if (ret != RDB_OK)
        return ret;

    return optimize(tbp, txp);
}
