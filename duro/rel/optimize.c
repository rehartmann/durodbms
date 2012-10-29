/*
 * $Id$
 *
 * Copyright (C) 2004-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "optimize.h"
#include "transform.h"
#include "internal.h"
#include "stable.h"
#include "tostr.h"

#include <gen/strfns.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>

static RDB_bool RDB_optimize_enabled = RDB_TRUE;

RDB_bool
RDB_index_sorts(struct RDB_tbindex *indexp, int seqitc,
        const RDB_seq_item seqitv[])
{
    int i;

    if (indexp->idxp == NULL || !RDB_index_is_ordered(indexp->idxp)
            || indexp->attrc < seqitc)
        return RDB_FALSE;

    for (i = 0; i < seqitc; i++) {
        if (strcmp(indexp->attrv[i].attrname, seqitv[i].attrname) != 0
                || indexp->attrv[i].asc != seqitv[i].asc)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

enum {
    tbpv_cap = 256
};

static RDB_bool is_and(const RDB_expression *exp)
{
    return (RDB_bool) (exp->kind == RDB_EX_RO_OP
            && strcmp (exp->def.op.name, "and") == 0);
}

static int
alter_op(RDB_expression *exp, const char *name, RDB_exec_context *ecp)
{
    char *newname;

    newname = RDB_realloc(exp->def.op.name, strlen(name) + 1, ecp);
    if (newname == NULL) {
        return RDB_ERROR;
    }
    strcpy(newname, name);
    exp->def.op.name = newname;

    return RDB_OK;
}

/**
 * Remove the (only) child of exp and turn the grandchildren of exp into children
 */
static int
eliminate_child (RDB_expression *exp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *hexp = exp->def.op.args.firstp;
    if (alter_op(exp, name, ecp) != RDB_OK)
        return RDB_ERROR;

    exp->def.op.args.firstp = hexp->def.op.args.firstp;
    RDB_free(hexp->def.op.name);
    RDB_free(hexp);
    return RDB_transform(exp->def.op.args.firstp, NULL, NULL, ecp, txp);
}

/* Try to eliminate NOT operator */
static int
eliminate_not(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *hexp;

    if (exp->kind != RDB_EX_RO_OP)
        return RDB_OK;

    if (strcmp(exp->def.op.name, "not") != 0) {
        RDB_expression *argp = exp->def.op.args.firstp;

        while (argp != NULL) {
            if (eliminate_not(argp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
        }
        return RDB_OK;
    }

    if (exp->def.op.args.firstp->kind != RDB_EX_RO_OP)
        return RDB_OK;

    if (strcmp(exp->def.op.args.firstp->def.op.name, "and") == 0) {
        hexp = RDB_ro_op("not", ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        RDB_add_arg(hexp, exp->def.op.args.firstp->def.op.args.firstp->nextp);
        ret = alter_op(exp, "or", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->nextp = hexp;

        ret = alter_op(exp->def.op.args.firstp, "not", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->def.op.args.firstp->nextp = NULL;

        ret = eliminate_not(exp->def.op.args.firstp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
        return eliminate_not(exp->def.op.args.firstp->nextp, ecp, txp);
    }
    if (strcmp(exp->def.op.args.firstp->def.op.name, "or") == 0) {
        hexp = RDB_ro_op("not", ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        hexp->nextp = NULL;
        RDB_add_arg(hexp, exp->def.op.args.firstp->def.op.args.firstp->nextp);
        ret = alter_op(exp, "and", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->nextp = hexp;

        ret = alter_op(exp->def.op.args.firstp, "not", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->def.op.args.firstp->nextp = NULL;

        ret = eliminate_not(exp->def.op.args.firstp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
        return eliminate_not(exp->def.op.args.firstp->nextp, ecp, txp);
    }
    if (strcmp(exp->def.op.args.firstp->def.op.name, "=") == 0)
        return eliminate_child(exp, "<>", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, "<>") == 0)
        return eliminate_child(exp, "=", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, "<") == 0)
        return eliminate_child(exp, ">=", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, ">") == 0)
        return eliminate_child(exp, "<=", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, "<=") == 0)
        return eliminate_child(exp, ">", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, ">=") == 0)
        return eliminate_child(exp, "<", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, "not") == 0) {
        hexp = exp->def.op.args.firstp;
        memcpy(exp, hexp->def.op.args.firstp, sizeof (RDB_expression));
        RDB_free(hexp->def.op.args.firstp->def.op.name);
        RDB_free(hexp->def.op.args.firstp);
        RDB_free(hexp->def.op.name);
        RDB_free(hexp);
        return eliminate_not(exp->def.op.args.firstp, ecp, txp);;
    }

    return eliminate_not(exp->def.op.args.firstp, ecp, txp);
}

static void
unbalance_and(RDB_expression *exp)
{
    RDB_expression *axp;

    if (!is_and(exp))
        return;

    if (is_and(exp->def.op.args.firstp))
        unbalance_and(exp->def.op.args.firstp);

    if (is_and(exp->def.op.args.firstp->nextp)) {
        unbalance_and(exp->def.op.args.firstp->nextp);
        if (is_and(exp->def.op.args.firstp)) {
            RDB_expression *ax2p;

            /* Find leftmost factor */
            axp = exp->def.op.args.firstp;
            while (is_and(axp->def.op.args.firstp))
                axp = axp->def.op.args.firstp;

            /* Swap leftmost factor and right child */
            ax2p = exp->def.op.args.firstp->nextp;
            ax2p = axp->nextp;
            axp->nextp = NULL;
            exp->def.op.args.firstp->nextp = axp->def.op.args.firstp;
            axp->def.op.args.firstp = ax2p;
        } else {
            /* Swap children */
            axp = exp->def.op.args.firstp;
            exp->def.op.args.firstp = axp->nextp;
            axp->nextp = exp->def.op.args.firstp->nextp;
            exp->def.op.args.firstp->nextp = axp;
        }
    }
}

/*
 * Check if the expression covers all index attributes.
 */
static int
expr_covers_index(RDB_expression *exp, RDB_tbindex *indexp)
{
    int i;

    for (i = 0; i < indexp->attrc
            && RDB_attr_node(exp, indexp->attrv[i].attrname, "=") != NULL;
            i++);
    return i;
}

/*
 * Check if attrv covers all index attributes.
 */
static int
table_covers_index(RDB_type *reltyp, RDB_tbindex *indexp)
{
    int i;

    /* Check if all index attributes appear in attrv */
    for (i = 0; i < indexp->attrc; i++) {
        if (RDB_tuple_type_attr(reltyp->def.basetyp,
                indexp->attrv[i].attrname) == NULL)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/**
 * Move node *nodep, which belongs to WHERE expression *texp, to **dstpp.
 */
static int
move_node(RDB_expression *texp, RDB_expression **dstpp, RDB_expression *nodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *prevp;

    /* Get previous node */
    if (nodep == texp->def.op.args.firstp->nextp) {
        prevp = NULL;
    } else {
        prevp = texp->def.op.args.firstp->nextp;
        while (prevp->def.op.args.firstp != nodep)
            prevp = prevp->def.op.args.firstp;
    }

    if (!is_and(nodep)) {
        /* Remove *nodep from source */
        if (prevp == NULL) {
            texp->def.op.args.firstp->nextp = NULL;
        } else {
            if (prevp == texp->def.op.args.firstp->nextp) {
                texp->def.op.args.firstp->nextp = prevp->def.op.args.firstp->nextp;
                texp->def.op.args.firstp->nextp->nextp = NULL;
            } else {
                RDB_expression *pprevp = texp->def.op.args.firstp->nextp;

                while (pprevp->def.op.args.firstp != prevp)
                    pprevp = pprevp->def.op.args.firstp;
                prevp->def.op.args.firstp->nextp->nextp = pprevp->def.op.args.firstp->nextp;
                pprevp->def.op.args.firstp = prevp->def.op.args.firstp->nextp;
            }
            RDB_free(prevp->def.op.name);
            RDB_free(prevp);
        }

        if (*dstpp == NULL)
            *dstpp = nodep;            
        else {
            RDB_expression *exp = RDB_ro_op("and", ecp);
            if (exp == NULL)
                return RDB_ERROR;
            RDB_add_arg(exp, *dstpp);
            RDB_add_arg(exp, nodep);
            *dstpp = exp;
        }
    } else {
        RDB_expression *arg2p = nodep->def.op.args.firstp->nextp;
        
        nodep->def.op.args.firstp->nextp = NULL;
        if (prevp == NULL) {
            texp->def.op.args.firstp->nextp = nodep->def.op.args.firstp;
        } else {
            prevp->def.op.args.firstp = nodep->def.op.args.firstp;
        }

        if (*dstpp == NULL)
            *dstpp = arg2p;
        else {
            RDB_expression *exp = RDB_ro_op("and", ecp);
            if (exp == NULL)
                return RDB_ERROR;
            RDB_add_arg(exp, *dstpp);
            RDB_add_arg(exp, arg2p);
            *dstpp = exp;
        }

        RDB_free(nodep->def.op.name);
        RDB_free(nodep);
    }
    return RDB_OK;
}

/**
 * Split a WHERE expression into two: one that uses the index specified
 * by indexp (the child) and one which does not (the parent).
 * If the parent condition becomes TRUE, simply convert
 * the selection into a selection which uses the index.
 */
static int
split_by_index(RDB_expression *texp, RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *nodep;
    RDB_expression *ixexp = NULL;
    RDB_expression *stopexp = NULL;
    RDB_bool asc = RDB_TRUE;
    RDB_bool all_eq = RDB_TRUE;
    int objpc = 0;
    RDB_object **objpv;

    for (i = 0; i < indexp->attrc && all_eq; i++) {
        RDB_expression *attrexp;

        if (indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp)) {
            nodep = RDB_attr_node(texp->def.op.args.firstp->nextp,
                    indexp->attrv[i].attrname, "=");
            if (nodep == NULL) {
                nodep = RDB_attr_node(texp->def.op.args.firstp->nextp,
                        indexp->attrv[i].attrname, ">=");
                if (nodep == NULL) {
                    nodep = RDB_attr_node(texp->def.op.args.firstp->nextp,
                            indexp->attrv[i].attrname, ">");
                    if (nodep == NULL) {
                        nodep = RDB_attr_node(texp->def.op.args.firstp->nextp,
                                indexp->attrv[i].attrname, "<=");
                        if (nodep == NULL) {
                            nodep = RDB_attr_node(texp->def.op.args.firstp->nextp,
                                    indexp->attrv[i].attrname, "<");
                            if (nodep == NULL)
                                break;
                        }
                    }
                }
                if (strcmp(nodep->def.op.name, ">=") == 0
                        || strcmp(nodep->def.op.name, ">") == 0) {
                    stopexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                            indexp->attrv[i].attrname, "<=");
                    if (stopexp == NULL) {
                        stopexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                                indexp->attrv[i].attrname, "<");
                    }
                    if (stopexp != NULL) {
                        attrexp = stopexp;
                        if (is_and(attrexp))
                            attrexp = attrexp->def.op.args.firstp->nextp;
                        if (move_node(texp, &ixexp, stopexp, ecp, txp) != RDB_OK)
                            return RDB_ERROR;
                        stopexp = attrexp;
                    }
                }
                all_eq = RDB_FALSE;
            }
        } else {
            nodep = RDB_attr_node(texp->def.op.args.firstp->nextp,
                    indexp->attrv[i].attrname, "=");
        }
        attrexp = nodep;
        if (is_and(attrexp))
            attrexp = attrexp->def.op.args.firstp->nextp;

        objpc++;

        if (indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp)) {
            if (strcmp(attrexp->def.op.name, "=") == 0
                    || strcmp(attrexp->def.op.name, ">=") == 0
                    || strcmp(attrexp->def.op.name, ">") == 0)
                asc = indexp->attrv[i].asc;
            else
                asc = (RDB_bool) !indexp->attrv[i].asc;
        }
        if (move_node(texp, &ixexp, nodep, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }
    if (objpc > 0) {
        if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
            objpv = RDB_index_objpv(indexp, ixexp, texp->def.op.args.firstp->def.tbref.tbp->typ,
                    objpc, all_eq, asc, ecp);
        } else {
            objpv = RDB_index_objpv(indexp, ixexp,
                    texp->def.op.args.firstp->def.op.args.firstp->def.tbref.tbp->typ,
                    objpc, all_eq, asc, ecp);
        }
        if (objpv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    if (texp->def.op.args.firstp->nextp != NULL) {
        RDB_expression *sitexp, *arg2p;

        /*
         * Split table into two
         */
        if (ixexp == NULL) {
            ixexp = RDB_bool_to_expr(RDB_TRUE, ecp);
            if (ixexp == NULL)
                return RDB_ERROR;
        }

        sitexp = RDB_ro_op("where", ecp);
        if (sitexp == NULL)
            return RDB_ERROR;
        arg2p = texp->def.op.args.firstp->nextp;
        RDB_add_arg(sitexp, texp->def.op.args.firstp);
        RDB_add_arg(sitexp, ixexp);

        assert(sitexp->def.op.optinfo.objc == 0);
        sitexp->def.op.optinfo.objc = objpc;
        sitexp->def.op.optinfo.objpv = objpv;
        sitexp->def.op.optinfo.asc = asc;
        sitexp->def.op.optinfo.all_eq = all_eq;
        sitexp->def.op.optinfo.stopexp = stopexp;

        texp->def.op.args.firstp = sitexp;
        sitexp->nextp = arg2p;
    } else {
        /*
         * Convert table to index select
         */
        texp->def.op.args.firstp->nextp = ixexp;
        ixexp->nextp = NULL;
        assert(texp->def.op.optinfo.objc == 0);
        texp->def.op.optinfo.objc = objpc;
        texp->def.op.optinfo.objpv = objpv;
        texp->def.op.optinfo.asc = asc;
        texp->def.op.optinfo.all_eq = all_eq;
        texp->def.op.optinfo.stopexp = stopexp;
    }
    texp->def.op.args.lastp = texp->def.op.args.firstp->nextp;
    return RDB_OK;
}

static unsigned
table_cost(RDB_expression *exp)
{
    RDB_tbindex *indexp;

    switch(exp->kind) {
        case RDB_EX_TBP:
            return exp->def.tbref.tbp->val.tb.stp != NULL ?
                    exp->def.tbref.tbp->val.tb.stp->est_cardinality : 0;
        case RDB_EX_OBJ:
            if (exp->def.obj.kind != RDB_OB_TABLE)
                return 0;
            return exp->def.obj.val.tb.stp != NULL ?
                    exp->def.obj.val.tb.stp->est_cardinality : 0;
        case RDB_EX_VAR:
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return 0; /* !! */
        case RDB_EX_RO_OP:
            break;
    }

    if (strcmp(exp->def.op.name, "semiminus") == 0)
        return table_cost(exp->def.op.args.firstp); /* !! */

    if (strcmp(exp->def.op.name, "minus") == 0)
        return table_cost(exp->def.op.args.firstp); /* !! */

    if (strcmp(exp->def.op.name, "union") == 0)
        return table_cost(exp->def.op.args.firstp)
                + table_cost(exp->def.op.args.firstp->nextp);

    if (strcmp(exp->def.op.name, "semijoin") == 0)
        return table_cost(exp->def.op.args.firstp); /* !! */

    if (strcmp(exp->def.op.name, "intersect") == 0)
        return table_cost(exp->def.op.args.firstp); /* !! */

    if (strcmp(exp->def.op.name, "where") == 0) {
        if (exp->def.op.optinfo.objc == 0)
            return table_cost(exp->def.op.args.firstp);
        if (exp->def.op.args.firstp->kind == RDB_EX_TBP) {
            indexp = exp->def.op.args.firstp->def.tbref.indexp;
        } else {
            indexp = exp->def.op.args.firstp->def.op.args.firstp->def.tbref.indexp;
        }
        if (indexp == NULL) {
            return table_cost(exp->def.op.args.firstp);
        }
        if (indexp->idxp == NULL)
            return 1;
        if (indexp->unique)
            return 2;
        if (!RDB_index_is_ordered(indexp->idxp))
            return 3;
        return 4;
    }
    if (strcmp(exp->def.op.name, "join") == 0) {
        if (exp->def.op.args.firstp->nextp->kind == RDB_EX_TBP
                && exp->def.op.args.firstp->nextp->def.tbref.indexp != NULL) {
            indexp = exp->def.op.args.firstp->nextp->def.tbref.indexp;
            if (indexp->idxp == NULL)
                return table_cost(exp->def.op.args.firstp);
            if (indexp->unique)
                return table_cost(exp->def.op.args.firstp) * 2;
            if (!RDB_index_is_ordered(indexp->idxp))
                return table_cost(exp->def.op.args.firstp) * 3;
            return table_cost(exp->def.op.args.firstp) * 4;
        }
        return table_cost(exp->def.op.args.firstp)
                * table_cost(exp->def.op.args.firstp->nextp);
    }
    if (strcmp(exp->def.op.name, "extend") == 0
             || strcmp(exp->def.op.name, "project") == 0
             || strcmp(exp->def.op.name, "remove") == 0
             || strcmp(exp->def.op.name, "summarize") == 0
             || strcmp(exp->def.op.name, "rename") == 0
             || strcmp(exp->def.op.name, "wrap") == 0
             || strcmp(exp->def.op.name, "unwrap") == 0
             || strcmp(exp->def.op.name, "group") == 0
             || strcmp(exp->def.op.name, "ungroup") == 0)
        return table_cost(exp->def.op.args.firstp);
    if (strcmp(exp->def.op.name, "divide") == 0) {
        return table_cost(exp->def.op.args.firstp)
                * table_cost(exp->def.op.args.firstp->nextp); /* !! */
    }
    if (strcmp(exp->def.op.name, "is_empty") == 0
            || strcmp(exp->def.op.name, "count") == 0)
        return table_cost(exp->def.op.args.firstp);

    /* Other operator */
    if (exp->def.op.args.firstp != NULL)
        return table_cost(exp->def.op.args.firstp);

    return 0;
}

static int
mutate(RDB_expression *exp, RDB_expression **tbpv, int cap, RDB_expression *,
        RDB_exec_context *, RDB_transaction *);

static int
mutate_where(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int tbc;
    RDB_expression *chexp = texp->def.op.args.firstp;
    RDB_expression *condp = texp->def.op.args.firstp->nextp;

    if (chexp->kind == RDB_EX_TBP
        || (chexp->kind == RDB_EX_RO_OP
            && strcmp(chexp->def.op.name, "project") == 0
            && chexp->def.op.args.firstp->kind == RDB_EX_TBP)) {
        if (eliminate_not(condp, ecp, txp) != RDB_OK)
            return RDB_ERROR;

        /* Convert condition into 'unbalanced' form */
        unbalance_and(condp);
    }

    tbc = mutate(chexp, tbpv, cap, empty_exp, ecp, txp);
    if (tbc < 0)
        return tbc;

    for (i = 0; i < tbc; i++) {
        RDB_expression *nexp;
        RDB_expression *exp = RDB_dup_expr(condp, ecp);
        if (exp == NULL)
            return RDB_ERROR;

        nexp = RDB_ro_op("where", ecp);
        if (nexp == NULL) {
            RDB_del_expr(exp, ecp);
            return RDB_ERROR;
        }
        RDB_add_arg(nexp, tbpv[i]);
        RDB_add_arg(nexp, exp);

        /*
         * If the child table is a stored table or a
         * projection of a stored table, try to use an index
         */
        if (tbpv[i]->kind == RDB_EX_TBP
                && tbpv[i]->def.tbref.indexp != NULL)
        {
            RDB_tbindex *indexp = tbpv[i]->def.tbref.indexp;
            if ((indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp))
                    || expr_covers_index(exp, indexp) == indexp->attrc) {
                if (split_by_index(nexp, indexp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }
        } else if (tbpv[i]->kind == RDB_EX_RO_OP
                && strcmp(tbpv[i]->def.op.name, "project") == 0
                && tbpv[i]->def.op.args.firstp->kind == RDB_EX_TBP
                && tbpv[i]->def.op.args.firstp->def.tbref.indexp != NULL) {
            RDB_tbindex *indexp = tbpv[i]->def.op.args.firstp->def.tbref.indexp;
            if ((indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp))
                    || expr_covers_index(exp, indexp)
                            == indexp->attrc) {
                if (split_by_index(nexp, indexp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }
        }
        tbpv[i] = nexp;
    }
    return tbc;
}

static int
mutate_unary(RDB_expression *exp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int tbc = mutate(exp->def.op.args.firstp, tbpv, cap, empty_exp, ecp, txp);
    if (tbc <= 0)
        return tbc;

    for (i = 0; i < tbc; i++) {
        RDB_expression *nexp = RDB_ro_op(exp->def.op.name, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        RDB_add_arg(nexp, tbpv[i]);
        tbpv[i] = nexp;
    }
    return tbc;
}

/*
 * Copy expression, resolving table names and making a copy
 * of virtual tables (recursively)
 */
static RDB_expression *
dup_expr_deep(const RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *newexp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = dup_expr_deep(exp->def.op.args.firstp, ecp, txp);
            if (newexp == NULL)
                return NULL;
            newexp = RDB_tuple_attr(newexp, exp->def.op.name, ecp);
            break;
        case RDB_EX_GET_COMP:
            newexp = dup_expr_deep(exp->def.op.args.firstp, ecp, txp);
            if (newexp == NULL)
                return NULL;
            newexp = RDB_expr_comp(newexp, exp->def.op.name, ecp);
            break;
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp;

            newexp = RDB_ro_op(exp->def.op.name, ecp);
            argp = exp->def.op.args.firstp;
            while (argp != NULL) {
                RDB_expression *nargp = dup_expr_deep(argp, ecp, txp);
                if (nargp == NULL)
                    return NULL;
                RDB_add_arg(newexp, nargp);
                argp = argp->nextp;
            }
            break;
        }
        case RDB_EX_OBJ:
            newexp = RDB_obj_to_expr(&exp->def.obj, ecp);
            break;
        case RDB_EX_TBP:
            if (RDB_table_is_real(exp->def.tbref.tbp)) {
                newexp = RDB_table_ref(exp->def.tbref.tbp, ecp);
            } else {
                newexp = dup_expr_deep(RDB_vtable_expr(exp->def.tbref.tbp),
                        ecp, txp);
            }
            break;
        case RDB_EX_VAR:
            /*
             * Resolve table name.
             * If the name refers to a virtual table, convert it
             * to its defining expression
             */
            newexp = NULL;
            if (txp != NULL
                    && (exp->typ == NULL || RDB_type_is_relation(exp->typ))) {
                RDB_object *tbp = RDB_get_table(RDB_expr_var_name(exp),
                        ecp, txp);
                if (tbp != NULL) {
                    if (RDB_table_is_real(tbp)) {
                        newexp = RDB_table_ref(tbp, ecp);
                    } else {
                        newexp = dup_expr_deep(RDB_vtable_expr(tbp), ecp, txp);
                    }
                    if (newexp == NULL)
                        return NULL;
                } else {
                    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NAME_ERROR) {
                        /* Ignore error */
                        RDB_clear_err(ecp);
                    } else {
                        return NULL;
                    }
                }
            }
            if (newexp == NULL)
                newexp = RDB_var_ref(RDB_expr_var_name(exp), ecp);
            break;
    }
    if (newexp == NULL)
        return NULL;
        
    if (exp->typ != NULL) {
        newexp->typ = RDB_dup_nonscalar_type(exp->typ, ecp);
        if (newexp->typ == NULL) {
            RDB_del_expr(newexp, ecp);
            return NULL;
        }
    }
    return newexp;
}

static int
mutate_vt(RDB_expression *texp, int nargc, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j, k;
    RDB_expression *argp;
    int ntbc;
    int otbc = 0;
    int argc = RDB_expr_list_length(&texp->def.op.args);

    argp = texp->def.op.args.firstp;
    for (j = 0; j < nargc; j++) {
        ntbc = mutate(argp, &tbpv[otbc], cap - otbc, empty_exp, ecp, txp);
        if (ntbc < 0)
            return ntbc;
        for (i = otbc; i < otbc + ntbc; i++) {
            RDB_expression *argp2;
            RDB_expression *nexp = RDB_ro_op(texp->def.op.name, ecp);
            if (nexp == NULL)
                return RDB_ERROR;

            argp2 = texp->def.op.args.firstp;
            for (k = 0; k < argc; k++) {
                if (k == j) {
                    RDB_add_arg(nexp, tbpv[i]);
                } else {
                    RDB_expression *otexp = RDB_dup_expr(argp2, ecp);
                    if (otexp == NULL)
                        return RDB_ERROR;
                    RDB_add_arg(nexp, otexp);
                }
                argp2 = argp2->nextp;
            }
            tbpv[i] = nexp;
        }
        otbc += ntbc;
        argp = argp->nextp;
    }
    return otbc;
}

static int
mutate_full_vt(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    return mutate_vt(texp, RDB_expr_list_length(&texp->def.op.args), tbpv,
            cap, empty_exp, ecp, txp);
}

/*
 * If *exp or a subexpression of *exp is equal to *empty_exp, replace it
 * with an empty relation.
 * *empty_exp must not be NULL.
 */
static int
replace_empty(RDB_expression *exp, RDB_expression *empty_exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_bool eqempty;
    /* Check if there is a constraint that says the table is empty */
    if (RDB_expr_equals(exp, empty_exp, ecp, txp, &eqempty) != RDB_OK)
        return RDB_ERROR;

    if (eqempty) {
        return RDB_expr_to_empty_table(exp, ecp, txp);
    }
    if (exp->kind == RDB_EX_RO_OP) {
        RDB_expression *argp = exp->def.op.args.firstp;

        while (argp != NULL) {
            if (replace_empty(argp, empty_exp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
    	}
    }
    return RDB_OK;
}

/*
 * (T1 UNION T2) [SEMI]MINUS T3 -> (T1 [SEMI]MINUS T2) UNION (T1 [SEMI]MINUS T3)
 * Returns the result.
 * Useful when there is a constraint of the form IS_EMPTY(T1 SEMIMINUS T2)
 * so one of the children can be optimized away.
 * empty_exp points to an expression declared to be empty
 * by a constraint.
 */
static RDB_expression *
transform_semi_minus_union1(RDB_expression *texp, RDB_expression *empty_exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *ex1p, *ex2p, *ex3p, *ex4p;
    RDB_expression *resexp;

    ex1p = RDB_dup_expr(texp->def.op.args.firstp->def.op.args.firstp, ecp);
    if (ex1p == NULL) {
        return NULL;
    }

    ex2p = RDB_dup_expr(texp->def.op.args.firstp->nextp, ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex1p, ecp);
        return NULL;
    }

    ex3p = RDB_ro_op(texp->def.op.name, ecp);
    if (ex3p == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex2p, ecp);
        return NULL;
    }
    RDB_add_arg(ex3p, ex1p);
    RDB_add_arg(ex3p, ex2p);

    ex1p = RDB_dup_expr(texp->def.op.args.firstp->def.op.args.firstp->nextp, ecp);
    if (ex1p == NULL) {
        RDB_del_expr(ex3p, ecp);
        return NULL;
    }

    ex2p = RDB_dup_expr(texp->def.op.args.firstp->nextp, ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex3p, ecp);
        return NULL;
    }

    ex4p = RDB_ro_op(texp->def.op.name, ecp);
    if (ex4p == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex2p, ecp);
        RDB_del_expr(ex3p, ecp);
        return NULL;
    }
    RDB_add_arg(ex4p, ex1p);
    RDB_add_arg(ex4p, ex2p);

    resexp = RDB_ro_op("union", ecp);
    if (resexp == NULL) {
        RDB_del_expr(ex3p, ecp);
        RDB_del_expr(ex4p, ecp);
        return NULL;
    }
    RDB_add_arg(resexp, ex3p);
    RDB_add_arg(resexp, ex4p);

    ret = replace_empty(resexp, empty_exp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_del_expr(resexp, ecp);
        return NULL;
    }
    return resexp;
}

/*
 * T! [SEMI]MINUS (T2 UNION T3) -> (T1 [SEMI]MINUS T2) [SEMI]MINUS T3
 * Returns the result.
 * Useful when there is a constraint of the form IS_EMPTY(T1 SEMIMINUS T2)
 * to optimize the constraint checking of an insert into T2.
 */
static RDB_expression *
transform_semi_minus_union2(RDB_expression *texp, RDB_expression *empty_exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *ex1p, *ex2p, *ex3p;

    ex1p = RDB_dup_expr(texp->def.op.args.firstp, ecp);
    if (ex1p == NULL)
        return NULL;
    ex2p = RDB_dup_expr(texp->def.op.args.firstp->nextp->def.op.args.firstp,
            ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex1p, ecp);
        return NULL;
    }
    ex3p = RDB_ro_op(texp->def.op.name, ecp);
    if (ex3p == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex2p, ecp);
        return NULL;
    }
    RDB_add_arg(ex3p, ex1p);
    RDB_add_arg(ex3p, ex2p);

    ex2p = RDB_dup_expr(
            texp->def.op.args.firstp->nextp->def.op.args.firstp->nextp,
            ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex3p, ecp);
        return NULL;
    }

    ex1p = RDB_ro_op(texp->def.op.name, ecp);
    if (ex1p == NULL) {
        RDB_del_expr(ex3p, ecp);
        RDB_del_expr(ex2p, ecp);
        return NULL;
    }

    RDB_add_arg(ex1p, ex3p);
    RDB_add_arg(ex1p, ex2p);

    ret = replace_empty(ex1p, empty_exp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_del_expr(ex1p, ecp);
        return NULL;
    }
    return ex1p;
}

/*
 * empty_exp must not be NULL.
 */
static int
mutate_semi_minus(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc = mutate_full_vt(texp, tbpv, cap, empty_exp, ecp, txp);
    if (tbc < 0)
        return RDB_ERROR;

    if (tbc < cap && texp->def.op.args.firstp->kind == RDB_EX_RO_OP
            && strcmp(texp->def.op.args.firstp->def.op.name, "union") == 0) {
        tbpv[tbc] = transform_semi_minus_union1(texp, empty_exp, ecp, txp);
        if (tbpv[tbc] == NULL)
            return RDB_ERROR;
        tbc++;
    }
    if (tbc < cap && texp->def.op.args.firstp->nextp->kind == RDB_EX_RO_OP
            && strcmp(texp->def.op.args.firstp->nextp->def.op.name, "union")
               == 0) {
        tbpv[tbc] = transform_semi_minus_union2(texp, empty_exp, ecp, txp);
        if (tbpv[tbc] == NULL)
            return RDB_ERROR;
        tbc++;
    }
    return tbc;
}

static int
index_joins(RDB_expression *otexp, RDB_expression *itexp, 
        RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;
    int tbc;
    int i;
    RDB_type *ottyp = RDB_expr_type(otexp, NULL, NULL, ecp, txp);
    if (ottyp == NULL)
        return RDB_ERROR;

    /*
     * Use indexes of table #2 which cover table #1
     */

    tbp = itexp->def.tbref.tbp;
    tbc = 0;
    for (i = 0; i < tbp->val.tb.stp->indexc && tbc < cap; i++) {
        if (table_covers_index(ottyp, &tbp->val.tb.stp->indexv[i])) {
            RDB_expression *arg1p, *ntexp;
            RDB_expression *refargp = RDB_table_ref(tbp, ecp);
            if (refargp == NULL) {
                return RDB_ERROR;
            }

            refargp->def.tbref.indexp = &tbp->val.tb.stp->indexv[i];
            ntexp = RDB_ro_op("join", ecp);
            if (ntexp == NULL) {
                return RDB_ERROR;
            }

            arg1p = RDB_dup_expr(otexp, ecp);
            if (arg1p == NULL) {
                RDB_del_expr(ntexp, ecp);
                return RDB_ERROR;
            }

            RDB_add_arg(ntexp, arg1p);
            RDB_add_arg(ntexp, refargp);
            tbpv[tbc++] = ntexp;
        }
    }

    return tbc;
}

static int
mutate_join(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc = 0;

    if (texp->def.op.args.firstp->kind != RDB_EX_TBP
            && texp->def.op.args.firstp->nextp->kind != RDB_EX_TBP) {
        return mutate_full_vt(texp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (texp->def.op.args.firstp->nextp->kind == RDB_EX_TBP) {
        tbc = index_joins(texp->def.op.args.firstp, texp->def.op.args.firstp->nextp,
                tbpv, cap, ecp, txp);
        if (tbc == RDB_ERROR)
            return RDB_ERROR;
    }

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        int ret = index_joins(texp->def.op.args.firstp->nextp, texp->def.op.args.firstp,
                tbpv + tbc, cap - tbc, ecp, txp);
        if (ret == RDB_ERROR)
            return RDB_ERROR;
        tbc += ret;
    }
    return tbc;
}

static int
mutate_tbref(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (texp->def.tbref.tbp->kind == RDB_OB_TABLE
            && texp->def.tbref.tbp->val.tb.stp != NULL
            && texp->def.tbref.tbp->val.tb.stp->indexc > 0) {
        int i;
        int tbc = texp->def.tbref.tbp->val.tb.stp->indexc;
        if (tbc > cap)
            tbc = cap;

        /* For each index, generate an expression that uses the index */
        for (i = 0; i < tbc; i++) {
            RDB_tbindex *indexp = &texp->def.tbref.tbp->val.tb.stp->indexv[i];
            RDB_expression *tiexp = RDB_table_ref(
                    texp->def.tbref.tbp, ecp);
            if (tiexp == NULL)
                return RDB_ERROR;
            tiexp->def.tbref.indexp = indexp;
            tbpv[i] = tiexp;
        }
        return tbc;
    } else {
        return 0;
    }
}

/*
 * Create equivalents of *exp and store them in *tbpv.
 */
static int
mutate(RDB_expression *exp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (exp->kind == RDB_EX_TBP) {
        return mutate_tbref(exp, tbpv, cap, ecp, txp);
    }

    if (exp->kind != RDB_EX_RO_OP)
        return 0;

    if (strcmp(exp->def.op.name, "where") == 0) {
        return mutate_where(exp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (strcmp(exp->def.op.name, "join") == 0) {
        return mutate_join(exp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (strcmp(exp->def.op.name, "minus") == 0
            || strcmp(exp->def.op.name, "semiminus") == 0) {
        if (empty_exp != NULL)
            return mutate_semi_minus(exp, tbpv, cap, empty_exp, ecp, txp);
        return 0;
    }

    if (strcmp(exp->def.op.name, "union") == 0
            || strcmp(exp->def.op.name, "minus") == 0
            || strcmp(exp->def.op.name, "intersect") == 0
            || strcmp(exp->def.op.name, "semijoin") == 0) {
        return mutate_full_vt(exp, tbpv, cap, empty_exp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "extend") == 0
            || strcmp(exp->def.op.name, "project") == 0
            || strcmp(exp->def.op.name, "remove") == 0
            || strcmp(exp->def.op.name, "rename") == 0
            || strcmp(exp->def.op.name, "summarize") == 0
            || strcmp(exp->def.op.name, "wrap") == 0
            || strcmp(exp->def.op.name, "unwrap") == 0
            || strcmp(exp->def.op.name, "group") == 0
            || strcmp(exp->def.op.name, "ungroup") == 0) {
        return mutate_vt(exp, 1, tbpv, cap, empty_exp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "divide") == 0) {
        return mutate_vt(exp, 2, tbpv, cap, empty_exp, ecp, txp);
    }
    if (exp->def.op.args.firstp != NULL
            && exp->def.op.args.firstp->nextp == NULL) {
        return mutate_unary(exp, tbpv, cap, empty_exp, ecp, txp);
    }
    return 0;
}

/*
 * Estimate cost for reading all tuples of the table in the order
 * specified by seqitc/seqitv.
 */
static unsigned
sorted_table_cost(RDB_expression *texp, int seqitc,
        const RDB_seq_item seqitv[])
{
    int cost = table_cost(texp);

    /* Check if the result must be sorted */
    if (seqitc > 0) {
        RDB_tbindex *indexp = RDB_expr_sortindex(texp);
        if (indexp == NULL || !RDB_index_sorts(indexp, seqitc, seqitv))
        {
            int scost = (((double) cost) /* !! * log10(cost) */ / 7);

            if (scost == 0)
                scost = 1;
            cost += scost;
        }
    }

    return cost;
}

RDB_expression *
RDB_optimize(RDB_object *tbp, int seqitc, const RDB_seq_item seqitv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *nexp;

    if (tbp->kind != RDB_OB_TABLE) {
        RDB_raise_invalid_argument("not a table", ecp);
        return NULL;
    }

    if (tbp->val.tb.exp == NULL) {
        /* It's a real table - no optimization possible */
        if (seqitc > 0 && tbp->val.tb.stp != NULL) {
            /*
             * Check if an index can be used for sorting
             */

            for (i = 0; i < tbp->val.tb.stp->indexc
                    && !RDB_index_sorts(&tbp->val.tb.stp->indexv[i],
                            seqitc, seqitv);
                    i++);
            /* If yes, create reference */
            if (i < tbp->val.tb.stp->indexc) {
                nexp = RDB_table_ref(tbp, ecp);
                if (nexp == NULL)
                    return NULL;
                nexp->def.tbref.indexp = &tbp->val.tb.stp->indexv[i];
                return nexp;
            }
        }
        return RDB_table_ref(tbp, ecp);
    }
    if (!RDB_optimize_enabled) {
        return dup_expr_deep(tbp->val.tb.exp, ecp, txp);
    }

    /* Set expression types so it is known which names cannot refer to tables */
    if (RDB_expr_type(tbp->val.tb.exp, NULL, NULL, ecp, txp) == NULL)
        return NULL;
    return RDB_optimize_expr(tbp->val.tb.exp, seqitc, seqitv, NULL,
            ecp, txp);
}

static void
trace_plan_cost(RDB_expression *exp, int cost, const char *txt,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_database *dbp = RDB_tx_db(txp);
    /* dbp could be NULL, e.g. when RDB_get_dbs() is executed */
    if (dbp != NULL && RDB_env_trace(RDB_db_env(dbp)) > 0) {
        /*
         * Write expression (with index info) and cost (if not -1) to stderr
         */
        RDB_object strobj;

        RDB_init_obj(&strobj);
        if (RDB_expr_to_str(&strobj, exp, ecp, txp, RDB_SHOW_INDEX) != RDB_OK) {
            RDB_destroy_obj(&strobj, ecp);
            return;
        }

        fprintf(stderr, "%s: %s", txt, RDB_obj_string(&strobj));
        if (cost != -1)
            fprintf(stderr, ", cost: %d\n", cost);
        else
            fputs("\n", stderr);
        RDB_destroy_obj(&strobj, ecp);
    }
}

/*
 * Return an optimized version of exp or exp itself, if a cheaper
 * version could not be found
 */
static RDB_expression *
mutate_select(RDB_expression *exp, int seqitc, const RDB_seq_item seqitv[],
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    unsigned obestcost, bestcost;
    int bestn;
    int tbc;
    RDB_expression *texpv[tbpv_cap];

    bestcost = sorted_table_cost(exp, seqitc, seqitv);

    trace_plan_cost(exp, bestcost, "plan after transformation", ecp, txp);

    /* Perform rounds of optimization until there is no improvemen */
    do {
        obestcost = bestcost;

        trace_plan_cost(exp, obestcost, "original plan", ecp, txp);

        tbc = mutate(exp, texpv, tbpv_cap, empty_exp, ecp, txp);
        if (tbc < 0)
            return NULL;

        bestn = -1;

        for (i = 0; i < tbc; i++) {
            int cost = sorted_table_cost(texpv[i], seqitc, seqitv);

            trace_plan_cost(texpv[i], cost, "alternative plan", ecp, txp);

            if (cost < bestcost) {
                bestcost = cost;
                bestn = i;
            }
        }
        if (bestn == -1) {
            for (i = 0; i < tbc; i++)
                RDB_del_expr(texpv[i], ecp);
        } else {
            exp = texpv[bestn];
            for (i = 0; i < tbc; i++) {
                if (i != bestn)
                    RDB_del_expr(texpv[i], ecp);
            }
        }
    } while (bestcost < obestcost);
    trace_plan_cost(exp, bestcost, "winning plan", ecp, txp);
    exp->optimized = RDB_TRUE;
    return exp;
}

RDB_expression *
RDB_optimize_expr(RDB_expression *texp, int seqitc, const RDB_seq_item seqitv[],
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *nexp;
    RDB_expression *optexp;

    if (txp != NULL) {
        trace_plan_cost(texp, -1, "plan before transformation", ecp, txp);
    }

    /*
     * Make a deep copy of the table, so it can be transformed freely
     */
    nexp = dup_expr_deep(texp, ecp, txp);

    /*
     * Algebraic optimization
     */
    if (RDB_transform(nexp, NULL, NULL, ecp, txp) != RDB_OK)
        return NULL;

    /*
     * If no tx, optimization ends here
     */
    if (txp == NULL) {
        nexp->optimized = RDB_TRUE;
        return nexp;
    }

    /*
     * Replace tables which are declared to be empty
     * by a constraint
     */
    if (RDB_tx_db(txp) != NULL) {
        if (empty_exp != NULL) {
            if (replace_empty(nexp, empty_exp, ecp, txp) != RDB_OK)
                return NULL;
        }
    }

    /*
     * Try to find cheapest table
     */

    optexp = mutate_select(nexp, seqitc, seqitv, empty_exp, ecp, txp);

    /* If a better expression has been found, destroy the original one */
    if (optexp != nexp) {
        RDB_del_expr(nexp, ecp);
    }

    return optexp;
}
