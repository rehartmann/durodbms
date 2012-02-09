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

#include <gen/strfns.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>

static RDB_bool _RDB_optimize_enabled = RDB_TRUE;

RDB_bool
_RDB_index_sorts(struct _RDB_tbindex *indexp, int seqitc,
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
            && strcmp (exp->def.op.name, "AND") == 0);
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
 * Remove the (only child) of exp and turn the grandchildren of exp into children
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
    return _RDB_transform(exp->def.op.args.firstp, NULL, NULL, ecp, txp);
}

/* Try to eliminate NOT operator */
static int
eliminate_not(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *hexp;

    if (exp->kind != RDB_EX_RO_OP)
        return RDB_OK;

    if (strcmp(exp->def.op.name, "NOT") != 0) {
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

    if (strcmp(exp->def.op.args.firstp->def.op.name, "AND") == 0) {
        hexp = RDB_ro_op("NOT", ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        RDB_add_arg(hexp, exp->def.op.args.firstp->def.op.args.firstp->nextp);
        ret = alter_op(exp, "OR", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->nextp = hexp;

        ret = alter_op(exp->def.op.args.firstp, "NOT", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->def.op.args.firstp->nextp = NULL;

        ret = eliminate_not(exp->def.op.args.firstp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
        return eliminate_not(exp->def.op.args.firstp->nextp, ecp, txp);
    }
    if (strcmp(exp->def.op.args.firstp->def.op.name, "OR") == 0) {
        hexp = RDB_ro_op("NOT", ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        hexp->nextp = NULL;
        RDB_add_arg(hexp, exp->def.op.args.firstp->def.op.args.firstp->nextp);
        ret = alter_op(exp, "AND", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->nextp = hexp;

        ret = alter_op(exp->def.op.args.firstp, "NOT", ecp);
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
    if (strcmp(exp->def.op.args.firstp->def.op.name, "NOT") == 0) {
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
expr_covers_index(RDB_expression *exp, _RDB_tbindex *indexp)
{
    int i;

    for (i = 0; i < indexp->attrc
            && _RDB_attr_node(exp, indexp->attrv[i].attrname, "=") != NULL;
            i++);
    return i;
}

/*
 * Check if attrv covers all index attributes.
 */
static int
table_covers_index(RDB_type *reltyp, _RDB_tbindex *indexp)
{
    int i;

    /* Check if all index attributes appear in attrv */
    for (i = 0; i < indexp->attrc; i++) {
        if (_RDB_tuple_type_attr(reltyp->def.basetyp,
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
            RDB_expression *exp = RDB_ro_op("AND", ecp);
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
            RDB_expression *exp = RDB_ro_op("AND", ecp);
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
split_by_index(RDB_expression *texp, _RDB_tbindex *indexp,
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
            nodep = _RDB_attr_node(texp->def.op.args.firstp->nextp,
                    indexp->attrv[i].attrname, "=");
            if (nodep == NULL) {
                nodep = _RDB_attr_node(texp->def.op.args.firstp->nextp,
                        indexp->attrv[i].attrname, ">=");
                if (nodep == NULL) {
                    nodep = _RDB_attr_node(texp->def.op.args.firstp->nextp,
                            indexp->attrv[i].attrname, ">");
                    if (nodep == NULL) {
                        nodep = _RDB_attr_node(texp->def.op.args.firstp->nextp,
                                indexp->attrv[i].attrname, "<=");
                        if (nodep == NULL) {
                            nodep = _RDB_attr_node(texp->def.op.args.firstp->nextp,
                                    indexp->attrv[i].attrname, "<");
                            if (nodep == NULL)
                                break;
                        }
                    }
                }
                if (strcmp(nodep->def.op.name, ">=") == 0
                        || strcmp(nodep->def.op.name, ">") == 0) {
                    stopexp = _RDB_attr_node(texp->def.op.args.firstp->nextp,
                            indexp->attrv[i].attrname, "<=");
                    if (stopexp == NULL) {
                        stopexp = _RDB_attr_node(texp->def.op.args.firstp->nextp,
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
            nodep = _RDB_attr_node(texp->def.op.args.firstp->nextp,
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
            objpv = _RDB_index_objpv(indexp, ixexp, texp->def.op.args.firstp->def.tbref.tbp->typ,
                    objpc, all_eq, asc, ecp);
        } else {
            objpv = _RDB_index_objpv(indexp, ixexp,
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

        sitexp = RDB_ro_op("WHERE", ecp);
        if (sitexp == NULL)
            return RDB_ERROR;
        arg2p = texp->def.op.args.firstp->nextp;
        RDB_add_arg(sitexp, texp->def.op.args.firstp);
        RDB_add_arg(sitexp, ixexp);

        assert(sitexp->def.op.optinfo.objpc == 0);
        sitexp->def.op.optinfo.objpc = objpc;
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
        assert(texp->def.op.optinfo.objpc == 0);
        texp->def.op.optinfo.objpc = objpc;
        texp->def.op.optinfo.objpv = objpv;
        texp->def.op.optinfo.asc = asc;
        texp->def.op.optinfo.all_eq = all_eq;
        texp->def.op.optinfo.stopexp = stopexp;
    }
    texp->def.op.args.lastp = texp->def.op.args.firstp->nextp;
    return RDB_OK;
}

static unsigned
table_cost(RDB_expression *texp)
{
    _RDB_tbindex *indexp;

    switch(texp->kind) {
        case RDB_EX_TBP:
            return texp->def.tbref.tbp->val.tb.stp != NULL ?
                    texp->def.tbref.tbp->val.tb.stp->est_cardinality : 0;
        case RDB_EX_OBJ:
            return texp->def.obj.val.tb.stp != NULL ?
                    texp->def.obj.val.tb.stp->est_cardinality : 0;
        case RDB_EX_VAR:
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            abort();
        case RDB_EX_RO_OP:
            ;
    }

    if (strcmp(texp->def.op.name, "SEMIMINUS") == 0)
        return table_cost(texp->def.op.args.firstp); /* !! */

    if (strcmp(texp->def.op.name, "MINUS") == 0)
        return table_cost(texp->def.op.args.firstp); /* !! */

    if (strcmp(texp->def.op.name, "UNION") == 0)
        return table_cost(texp->def.op.args.firstp)
                + table_cost(texp->def.op.args.firstp->nextp);

    if (strcmp(texp->def.op.name, "SEMIJOIN") == 0)
        return table_cost(texp->def.op.args.firstp); /* !! */

    if (strcmp(texp->def.op.name, "INTERSECT") == 0)
        return table_cost(texp->def.op.args.firstp); /* !! */

    if (strcmp(texp->def.op.name, "WHERE") == 0) {
        if (texp->def.op.optinfo.objpc == 0)
            return table_cost(texp->def.op.args.firstp);
        if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
            indexp = texp->def.op.args.firstp->def.tbref.indexp;
        } else {
            indexp = texp->def.op.args.firstp->def.op.args.firstp->def.tbref.indexp;
        }
        if (indexp->idxp == NULL)
            return 1;
        if (indexp->unique)
            return 2;
        if (!RDB_index_is_ordered(indexp->idxp))
            return 3;
        return 4;
    }
    if (strcmp(texp->def.op.name, "JOIN") == 0) {
        if (texp->def.op.args.firstp->nextp->kind == RDB_EX_TBP
                && texp->def.op.args.firstp->nextp->def.tbref.indexp != NULL) {
            indexp = texp->def.op.args.firstp->nextp->def.tbref.indexp;
            if (indexp->idxp == NULL)
                return table_cost(texp->def.op.args.firstp);
            if (indexp->unique)
                return table_cost(texp->def.op.args.firstp) * 2;
            if (!RDB_index_is_ordered(indexp->idxp))
                return table_cost(texp->def.op.args.firstp) * 3;
            return table_cost(texp->def.op.args.firstp) * 4;
        }
        return table_cost(texp->def.op.args.firstp)
                * table_cost(texp->def.op.args.firstp->nextp);
    }
    if (strcmp(texp->def.op.name, "EXTEND") == 0
             || strcmp(texp->def.op.name, "PROJECT") == 0
             || strcmp(texp->def.op.name, "REMOVE") == 0
             || strcmp(texp->def.op.name, "SUMMARIZE") == 0
             || strcmp(texp->def.op.name, "RENAME") == 0
             || strcmp(texp->def.op.name, "WRAP") == 0
             || strcmp(texp->def.op.name, "UNWRAP") == 0
             || strcmp(texp->def.op.name, "GROUP") == 0
             || strcmp(texp->def.op.name, "UNGROUP") == 0)
        return table_cost(texp->def.op.args.firstp);
    if (strcmp(texp->def.op.name, "DIVIDE") == 0) {
        return table_cost(texp->def.op.args.firstp)
                * table_cost(texp->def.op.args.firstp->nextp); /* !! */
    }
    /* Other operator */
    return 0;
}

static int
mutate(RDB_expression *texp, RDB_expression **tbpv, int cap, RDB_exec_context *,
        RDB_transaction *);

static int
mutate_where(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int tbc;
    RDB_expression *chexp = texp->def.op.args.firstp;
    RDB_expression *condp = texp->def.op.args.firstp->nextp;

    if (chexp->kind == RDB_EX_TBP
        || (chexp->kind == RDB_EX_RO_OP
            && strcmp(chexp->def.op.name, "PROJECT") == 0
            && chexp->def.op.args.firstp->kind == RDB_EX_TBP)) {
        if (eliminate_not(condp, ecp, txp) != RDB_OK)
            return RDB_ERROR;

        /* Convert condition into 'unbalanced' form */
        unbalance_and(condp);
    }

    tbc = mutate(chexp, tbpv, cap, ecp, txp);
    if (tbc < 0)
        return tbc;

    for (i = 0; i < tbc; i++) {
        RDB_expression *nexp;
        RDB_expression *exp = RDB_dup_expr(condp, ecp);
        if (exp == NULL)
            return RDB_ERROR;

        nexp = RDB_ro_op("WHERE", ecp);
        if (nexp == NULL) {
            RDB_drop_expr(exp, ecp);
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
            _RDB_tbindex *indexp = tbpv[i]->def.tbref.indexp;
            if ((indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp))
                    || expr_covers_index(exp, indexp) == indexp->attrc) {
                if (split_by_index(nexp, indexp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }
        } else if (tbpv[i]->kind == RDB_EX_RO_OP
                && strcmp(tbpv[i]->def.op.name, "PROJECT") == 0
                && tbpv[i]->def.op.args.firstp->kind == RDB_EX_TBP
                && tbpv[i]->def.op.args.firstp->def.tbref.indexp != NULL) {
            _RDB_tbindex *indexp = tbpv[i]->def.op.args.firstp->def.tbref.indexp;
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

/*
 * Copy expression, making a copy of virtual tables
 */
static RDB_expression *
dup_expr_vt(const RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_expression *newexp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = dup_expr_vt(exp->def.op.args.firstp, ecp);
            if (newexp == NULL)
                return NULL;
            newexp = RDB_tuple_attr(newexp, exp->def.op.name, ecp);
            break;
        case RDB_EX_GET_COMP:
            newexp = dup_expr_vt(exp->def.op.args.firstp, ecp);
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
                RDB_expression *nargp = dup_expr_vt(argp, ecp);
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
            if (exp->def.tbref.tbp->val.tb.exp == NULL) {
                newexp = RDB_table_ref(exp->def.tbref.tbp, ecp);
            } else {
                newexp = dup_expr_vt(exp->def.tbref.tbp->val.tb.exp, ecp);
            }
            break;
        case RDB_EX_VAR:
            newexp = RDB_var_ref(exp->def.varname, ecp);
            break;
    }
    if (newexp == NULL)
        return NULL;
        
    if (exp->typ != NULL) {
        newexp->typ = RDB_dup_nonscalar_type(exp->typ, ecp);
        if (newexp->typ == NULL) {
            RDB_drop_expr(newexp, ecp);
            return NULL;
        }
    }
    return newexp;
}

static int
mutate_vt(RDB_expression *texp, int nargc, RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j, k;
    RDB_expression *argp;
    int ntbc;
    int otbc = 0;
    int argc = RDB_expr_list_length(&texp->def.op.args);

    argp = texp->def.op.args.firstp;
    for (j = 0; j < nargc; j++) {
        ntbc = mutate(argp, &tbpv[otbc], cap - otbc, ecp, txp);
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
                    RDB_expression *otexp = dup_expr_vt(argp2, ecp);
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
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    return mutate_vt(texp, RDB_expr_list_length(&texp->def.op.args), tbpv,
            cap, ecp, txp);
}

static int
replace_empty(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    struct _RDB_tx_and_ec te;

    te.txp = txp;
    te.ecp = ecp;

    /* Check if there is a constraint that says the table is empty */
    if (txp->dbp != NULL
            && RDB_hashtable_get(&txp->dbp->dbrootp->empty_tbtab,
                    exp, &te) != NULL) {
        return _RDB_expr_to_empty_table(exp, ecp, txp);
    } else if (exp->kind == RDB_EX_RO_OP) {
        RDB_expression *argp = exp->def.op.args.firstp;

        while (argp != NULL) {
            if (replace_empty(argp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
    	}
    }
    return RDB_OK;
}

static RDB_expression *
transform_semiminus_union(RDB_expression *texp, RDB_exec_context *ecp,
        RDB_transaction *txp)
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
        RDB_drop_expr(ex1p, ecp);
        return NULL;
    }

    ex3p = RDB_ro_op("SEMIMINUS", ecp);
    if (ex3p == NULL) {
        RDB_drop_expr(ex1p, ecp);
        RDB_drop_expr(ex2p, ecp);
        return NULL;
    }
    RDB_add_arg(ex3p, ex1p);
    RDB_add_arg(ex3p, ex2p);

    ex1p = RDB_dup_expr(texp->def.op.args.firstp->def.op.args.firstp->nextp, ecp);
    if (ex1p == NULL) {
        RDB_drop_expr(ex3p, ecp);
        return NULL;
    }

    ex2p = RDB_dup_expr(texp->def.op.args.firstp->nextp, ecp);
    if (ex2p == NULL) {
        RDB_drop_expr(ex1p, ecp);
        RDB_drop_expr(ex3p, ecp);
        return NULL;
    }

    ex4p = RDB_ro_op("SEMIMINUS", ecp);
    if (ex4p == NULL) {
        RDB_drop_expr(ex1p, ecp);
        RDB_drop_expr(ex2p, ecp);
        RDB_drop_expr(ex3p, ecp);
        return NULL;
    }
    RDB_add_arg(ex4p, ex1p);
    RDB_add_arg(ex4p, ex2p);

    resexp = RDB_ro_op("UNION", ecp);
    if (resexp == NULL) {
        RDB_drop_expr(ex3p, ecp);
        RDB_drop_expr(ex4p, ecp);
        return NULL;
    }
    RDB_add_arg(resexp, ex3p);
    RDB_add_arg(resexp, ex4p);

    ret = replace_empty(resexp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_expr(resexp, ecp);
        return NULL;
    }
    return resexp;
}

static int
mutate_semiminus(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc = mutate_full_vt(texp, tbpv, cap, ecp, txp);
    if (tbc < 0)
        return RDB_ERROR;

    if (tbc < cap && texp->def.op.args.firstp->kind == RDB_EX_RO_OP
            && strcmp(texp->def.op.args.firstp->def.op.name, "UNION") == 0) {
        tbpv[tbc] = transform_semiminus_union(texp, ecp, txp);
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
            ntexp = RDB_ro_op("JOIN", ecp);
            if (ntexp == NULL) {
                return RDB_ERROR;
            }

            arg1p = RDB_dup_expr(otexp, ecp);
            if (arg1p == NULL) {
                RDB_drop_expr(ntexp, ecp);
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
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc = 0;

    if (texp->def.op.args.firstp->kind != RDB_EX_TBP
            && texp->def.op.args.firstp->nextp->kind != RDB_EX_TBP) {
        return mutate_full_vt(texp, tbpv, cap, ecp, txp);
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

        for (i = 0; i < tbc; i++) {
            _RDB_tbindex *indexp = &texp->def.tbref.tbp->val.tb.stp->indexv[i];
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

static int
mutate(RDB_expression *texp, RDB_expression **tbpv, int cap, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (texp->kind == RDB_EX_TBP) {
        return mutate_tbref(texp, tbpv, cap, ecp, txp);
    }

    if (texp->kind != RDB_EX_RO_OP)
        return 0;

    if (strcmp(texp->def.op.name, "WHERE") == 0) {
        return mutate_where(texp, tbpv, cap, ecp, txp);
    }

    if (strcmp(texp->def.op.name, "JOIN") == 0) {
        return mutate_join(texp, tbpv, cap, ecp, txp);
    }

    if (strcmp(texp->def.op.name, "SEMIMINUS") == 0) {
        return mutate_semiminus(texp, tbpv, cap, ecp, txp);       
    }

    if (strcmp(texp->def.op.name, "UNION") == 0
            || strcmp(texp->def.op.name, "MINUS") == 0
            || strcmp(texp->def.op.name, "INTERSECT") == 0
            || strcmp(texp->def.op.name, "SEMIJOIN") == 0
            || strcmp(texp->def.op.name, "JOIN") == 0) {
        return mutate_full_vt(texp, tbpv, cap, ecp, txp);
    }
    if (strcmp(texp->def.op.name, "EXTEND") == 0
            || strcmp(texp->def.op.name, "PROJECT") == 0
            || strcmp(texp->def.op.name, "REMOVE") == 0
            || strcmp(texp->def.op.name, "RENAME") == 0
            || strcmp(texp->def.op.name, "SUMMARIZE") == 0
            || strcmp(texp->def.op.name, "WRAP") == 0
            || strcmp(texp->def.op.name, "UNWRAP") == 0
            || strcmp(texp->def.op.name, "GROUP") == 0
            || strcmp(texp->def.op.name, "UNGROUP") == 0) {
        return mutate_vt(texp, 1, tbpv, cap, ecp, txp);
    }
    if (strcmp(texp->def.op.name, "DIVIDE") == 0) {
        return mutate_vt(texp, 2, tbpv, cap, ecp, txp);
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

    /* Check if the index must be sorted */
    if (seqitc > 0) {
        _RDB_tbindex *indexp = _RDB_expr_sortindex(texp);
        if (indexp == NULL || !_RDB_index_sorts(indexp, seqitc, seqitv))
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
_RDB_optimize(RDB_object *tbp, int seqitc, const RDB_seq_item seqitv[],
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
                    && !_RDB_index_sorts(&tbp->val.tb.stp->indexv[i],
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
    return _RDB_optimize_enabled ?
            _RDB_optimize_expr(tbp->val.tb.exp, seqitc, seqitv, ecp, txp)
            : dup_expr_vt(tbp->val.tb.exp, ecp);
}

RDB_expression *
_RDB_optimize_expr(RDB_expression *texp, int seqitc, const RDB_seq_item seqitv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *nexp;
    unsigned obestcost, bestcost;
    int bestn;
    int tbc;
    RDB_expression *texpv[tbpv_cap];

    /*
     * Make a copy of the table, so it can be transformed freely
     */
    nexp = dup_expr_vt(texp, ecp);
    if (nexp == NULL)
        return NULL;

    if (nexp->kind == RDB_EX_VAR && txp != NULL) {
        /* Transform into table ref */
        RDB_object *tbp = RDB_get_table(nexp->def.varname, ecp, txp);
        if (tbp == NULL)
            return NULL;

        RDB_free(nexp->def.varname);
        nexp->kind = RDB_EX_TBP;
        nexp->def.tbref.tbp = tbp;
        nexp->def.tbref.indexp = NULL;
    }

    /*
     * Algebraic optimization
     */
    if (_RDB_transform(nexp, NULL, NULL, ecp, txp) != RDB_OK)
        return NULL;

    /*
     * If no tx, optimization ends here
     */
    if (txp == NULL)
        return nexp;

    /*
     * Replace tables which are declared to be empty
     * by a constraint
     */
    if (RDB_tx_db(txp) != NULL) {
        if (replace_empty(nexp, ecp, txp) != RDB_OK)
            return NULL;
    }

    /*
     * Try to find cheapest table
     */

    bestcost = sorted_table_cost(nexp, seqitc, seqitv);
    do {
        obestcost = bestcost;

        tbc = mutate(nexp, texpv, tbpv_cap, ecp, txp);
        if (tbc < 0)
            return NULL;

        bestn = -1;

        for (i = 0; i < tbc; i++) {
            int cost = sorted_table_cost(texpv[i], seqitc, seqitv);

            if (cost < bestcost) {
                bestcost = cost;
                bestn = i;
            }
        }
        if (bestn == -1) {
            for (i = 0; i < tbc; i++)
                RDB_drop_expr(texpv[i], ecp);
        } else {
            RDB_drop_expr(nexp, ecp);
            nexp = texpv[bestn];
            for (i = 0; i < tbc; i++) {
                if (i != bestn)
                    RDB_drop_expr(texpv[i], ecp);
            }
        }
    } while (bestcost < obestcost);
    return nexp;
}
