/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>
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
expr_attr(RDB_expression *exp, const char *attrname, enum _RDB_expr_kind kind)
{
    if (exp->kind == RDB_EX_ATTR
            && strcmp(exp->var.attrname, attrname) == 0)
        return RDB_TRUE;
    if (exp->kind == kind) {
        if (exp->var.op.arg1->kind == RDB_EX_ATTR
                && strcmp(exp->var.op.arg1->var.attrname, attrname) == 0
                && exp->var.op.arg2->kind == RDB_EX_OBJ)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

static RDB_bool
expr_cmp_attr(RDB_expression *exp, const char *attrname)
{
    if (exp->kind == RDB_EX_ATTR
            && strcmp(exp->var.attrname, attrname) == 0)
        return RDB_TRUE;
    switch(exp->kind) {
        case RDB_EX_EQ:
        case RDB_EX_GT:
        case RDB_EX_LT:
        case RDB_EX_GET:
        case RDB_EX_LET:
            if (exp->var.op.arg1->kind == RDB_EX_ATTR
                    && strcmp(exp->var.op.arg1->var.attrname, attrname) == 0
                    && exp->var.op.arg2->kind == RDB_EX_OBJ)
                return RDB_TRUE;
        default: ;
    }
    return RDB_FALSE;
}

RDB_expression *
attr_node(RDB_expression *exp, const char *attrname, enum _RDB_expr_kind kind)
{
    while (exp->kind == RDB_EX_AND) {
        if (expr_attr(exp->var.op.arg2, attrname, kind))
            return exp;
        exp = exp->var.op.arg1;
    }
    if (expr_attr(exp, attrname, kind))
        return exp;
    return NULL;
}

/* 
 * Check if the index specified by indexp can be used for the selection
 * specified by tbp. If yes, return the estimated cost.
 * If no, return INT_MAX.
 */
static int
eval_index_exp(RDB_expression *exp, _RDB_tbindex *indexp)
{
    int i;

    if (indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp)) {
        RDB_expression *iexp = exp;

        /*
         * The index is ordered, so the expression must refer at least to the
         * first attribute
         */
        if (expr_cmp_attr(iexp, indexp->attrv[0].attrname))
            return 4;
        iexp = exp;
        while (iexp != NULL && iexp->kind == RDB_EX_AND) {
            if (expr_cmp_attr(iexp->var.op.arg2, indexp->attrv[0].attrname))
                return 4;
            iexp = iexp->var.op.arg1;
        }
        return INT_MAX;
    }

    /*
     * The index is not ordered, so the expression must cover
     * all index attributes
     */
    for (i = 0; i < indexp->attrc; i++) {
        if (attr_node(exp, indexp->attrv[i].attrname, RDB_EX_EQ) == NULL)
            return INT_MAX;
    }
    if (indexp->idxp == NULL)
        return 1;
    return indexp->unique ? 2 : 3;
}

static int
eval_index_attrs(int attrc, char *attrv[], _RDB_tbindex *indexp)
{
    int i;

    /* Check if all index attributes appear in attrv */
    for (i = 0; i < indexp->attrc; i++) {
        if (RDB_find_str(attrc, attrv, indexp->attrv[i].attrname) == -1)
            return INT_MAX;
    }
    if (indexp->idxp == NULL)
        return 1;
    return indexp->unique ? 2 : 3;
}

static int
split_by_index(RDB_table *tbp, _RDB_tbindex *indexp)
{
    int ret;
    int i;
    RDB_expression *prevp;
    RDB_expression *nodep;
    RDB_expression *ixexp = NULL;
    RDB_bool all_eq = RDB_TRUE;
    int objpc = 0;
    RDB_object **objpv = malloc(sizeof (RDB_object *) * indexp->attrc);

    if (objpv == NULL)
        return RDB_NO_MEMORY;
    
    for (i = 0; i < indexp->attrc && all_eq; i++) {
        RDB_expression *attrexp;

        if (indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp)) {
            nodep = attr_node(tbp->var.select.exp, indexp->attrv[i].attrname,
                    RDB_EX_EQ);
            if (nodep == NULL) {
                nodep = attr_node(tbp->var.select.exp, indexp->attrv[i].attrname,
                    RDB_EX_GET);
                if (nodep == NULL) {
                    nodep = attr_node(tbp->var.select.exp, indexp->attrv[i].attrname,
                            RDB_EX_GT);
                    if (nodep == NULL) {
                        nodep = attr_node(tbp->var.select.exp,
                                indexp->attrv[i].attrname, RDB_EX_LET);
                        if (nodep == NULL) {
                            nodep = attr_node(tbp->var.select.exp,
                                    indexp->attrv[i].attrname, RDB_EX_LT);
                            if (nodep == NULL)
                                break;
                        }
                    }
                }
                all_eq = RDB_FALSE;
            }
        } else {
            nodep = attr_node(tbp->var.select.exp, indexp->attrv[i].attrname,
                    RDB_EX_EQ);
        }
        attrexp = nodep;
        if (attrexp->kind == RDB_EX_AND)
            attrexp = attrexp->var.op.arg2;
        if (attrexp->var.op.arg2->var.obj.typ == NULL
                && (attrexp->var.op.arg2->var.obj.kind == RDB_OB_TUPLE
                || attrexp->var.op.arg2->var.obj.kind == RDB_OB_ARRAY))
            _RDB_set_nonsc_type(&attrexp->var.op.arg2->var.obj,
                    RDB_type_attr_type(tbp->typ, indexp->attrv[i].attrname));
        if (attrexp->kind == RDB_EX_EQ || attrexp->kind == RDB_EX_GET
                || attrexp->kind == RDB_EX_GT)
            objpv[objpc++] = &attrexp->var.op.arg2->var.obj;

        /*
         * Delete node
         */

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
        sitbp->var.select.objpv = objpv;
        sitbp->var.select.objpc = objpc;
        sitbp->var.select.all_eq = all_eq;
        sitbp->optimized = RDB_TRUE;
        tbp->var.select.tbp = sitbp;
    } else {
        /*
         * Convert table to RDB_SELECT_INDEX
         */
        tbp->kind = RDB_TB_SELECT_INDEX;
        tbp->var.select.indexp = indexp;
        tbp->var.select.objpv = objpv;
        tbp->var.select.objpc = objpc;
        tbp->var.select.all_eq = all_eq;
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
        int cost = eval_index_exp(tbp->var.select.exp,
                &stbp->var.stored.indexv[i]);

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

/*
 * Make a copy of all named virtual tables,
 * so the tree may be safely modified.
 */
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
optimize(RDB_table *, RDB_transaction *);

static int
best_index(RDB_table *tbp, int attrc, char **attrv,
        _RDB_tbindex **indexpp)
{
    int i;
    int bestcost = INT_MAX;

    if (tbp->kind != RDB_TB_STORED)
        return INT_MAX;

    for (i = 0; i < tbp->var.stored.indexc; i++) {
        int cost = eval_index_attrs(attrc, attrv, &tbp->var.stored.indexv[i]);

        if (cost < bestcost) {
            bestcost = cost;
            *indexpp = &tbp->var.stored.indexv[i];
        }
    }
    
    return bestcost;
}

static int
common_attrs(RDB_type *tpltyp1, RDB_type *tpltyp2, char **cmattrv)
{
    int i, j;
    int cattrc = 0;

    for (i = 0; i < tpltyp1->var.tuple.attrc; i++) {
        for (j = 0;
             j < tpltyp2->var.tuple.attrc
                     && strcmp(tpltyp1->var.tuple.attrv[i].name,
                     tpltyp2->var.tuple.attrv[j].name) != 0;
             j++);
        if (j < tpltyp2->var.tuple.attrc)
            cmattrv[cattrc++] = tpltyp1->var.tuple.attrv[i].name;
    }
    return cattrc;
}

static int
optimize_join(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    int cmattrc;
    char **cmattrv;

    cmattrv = malloc(sizeof(char *)
            * tbp->var.join.tb1p->typ->var.basetyp->var.tuple.attrc);
    if (cmattrv == NULL)
        return RDB_NO_MEMORY;
    cmattrc = common_attrs(tbp->var.join.tb1p->typ->var.basetyp,
            tbp->var.join.tb2p->typ->var.basetyp, cmattrv);

    if (cmattrc >= 1) {
        int cost1, cost2;
        _RDB_tbindex *index1p;
        _RDB_tbindex *index2p;

        cost1 = best_index(tbp->var.join.tb1p, cmattrc, cmattrv, &index1p);
        cost2 = best_index(tbp->var.join.tb2p, cmattrc, cmattrv, &index2p);
        if (cost2 < cost1 || (cost1 == cost2 && cost2 < INT_MAX)) {
            tbp->var.join.indexp = index2p;
        } else if (cost1 < cost2) {
            /* Use index for table #1, swap tables */
            RDB_table *ttbp = tbp->var.join.tb1p;
            tbp->var.join.tb1p = tbp->var.join.tb2p;
            tbp->var.join.tb2p = ttbp;

            tbp->var.join.indexp = index1p;
        }
    }

    free(cmattrv);

    ret = optimize(tbp->var.join.tb1p, txp);
    if (ret != RDB_OK)
        return ret;
    if (tbp->var.join.indexp == NULL) {
        ret = optimize(tbp->var.join.tb2p, txp);
        if (ret != RDB_OK)
            return ret;
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
            ret = optimize_join(tbp, txp);
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
