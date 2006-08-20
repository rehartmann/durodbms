/*
 * $Id$
 *
 * Copyright (C) 2004-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

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

static RDB_bool is_and(RDB_expression *exp) {
    return (RDB_bool) exp->kind == RDB_EX_RO_OP
            && strcmp (exp->var.op.name, "AND") == 0;
}

static void
unbalance_and(RDB_expression *exp)
{
    RDB_expression *axp;

    if (!is_and(exp))
        return;

    if (is_and(exp->var.op.argv[0]))
        unbalance_and(exp->var.op.argv[0]);
        
    if (is_and(exp->var.op.argv[1])) {
        unbalance_and(exp->var.op.argv[1]);
        if (is_and(exp->var.op.argv[0])) {
            RDB_expression *ax2p;

            /* Find leftmost factor */
            axp = exp->var.op.argv[0];
            while (is_and(axp->var.op.argv[0]))
                axp = axp->var.op.argv[0];

            /* Swap leftmost factor and right child */
            ax2p = exp->var.op.argv[1];
            exp->var.op.argv[1] = axp->var.op.argv[0];
            axp->var.op.argv[0] = ax2p;
        } else {
            /* Swap children */
            axp = exp->var.op.argv[0];
            exp->var.op.argv[0] = exp->var.op.argv[1];
            exp->var.op.argv[1] = axp;
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
        if (_RDB_tuple_type_attr(reltyp->var.basetyp,
                indexp->attrv[i].attrname) == NULL)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

static int
move_node(RDB_expression *texp, RDB_expression **dstpp, RDB_expression *nodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *prevp;

    /*
     * Move node
     */

    /* Get previous node */
    if (nodep == texp->var.op.argv[1]) {
        prevp = NULL;
    } else {
        prevp = texp->var.op.argv[1];
        while (prevp->var.op.argv[0] != nodep)
            prevp = prevp->var.op.argv[0];
    }

    if (!is_and(nodep)) {
        if (*dstpp == NULL)
            *dstpp = nodep;
        else {
            RDB_expression *exp = RDB_ro_op("AND", 2, ecp);
            if (exp == NULL)
                return RDB_ERROR;
            RDB_add_arg(exp, *dstpp);
            RDB_add_arg(exp, nodep);
            *dstpp = exp;
        }
        if (prevp == NULL) {
            texp->var.op.argv[1] = NULL;
        } else {
            if (prevp == texp->var.op.argv[1]) {
                texp->var.op.argv[1] = prevp->var.op.argv[1];
            } else {
                RDB_expression *pprevp = texp->var.op.argv[1];

                while (pprevp->var.op.argv[0] != prevp)
                    pprevp = pprevp->var.op.argv[0];
                pprevp->var.op.argv[0] = prevp->var.op.argv[1];
            }
            free(prevp->var.op.name);
            free(prevp->var.op.argv);
            free(prevp);
        }
    } else {
        if (*dstpp == NULL)
            *dstpp = nodep->var.op.argv[1];
        else {
            RDB_expression *exp = RDB_ro_op("AND", 2, ecp);
            if (exp == NULL)
                return RDB_ERROR;
            RDB_add_arg(exp, *dstpp);
            RDB_add_arg(exp, nodep->var.op.argv[1]);
            *dstpp = exp;
        }
        if (prevp == NULL)
            texp->var.op.argv[1] = nodep->var.op.argv[0];
        else
            prevp->var.op.argv[0] = nodep->var.op.argv[0];
        free(nodep->var.op.name);
        free(nodep->var.op.argv);
        free(nodep);
    }
    return RDB_OK;
}

/*
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
            nodep = _RDB_attr_node(texp->var.op.argv[1],
                    indexp->attrv[i].attrname, "=");
            if (nodep == NULL) {
                nodep = _RDB_attr_node(texp->var.op.argv[1],
                        indexp->attrv[i].attrname, ">=");
                if (nodep == NULL) {
                    nodep = _RDB_attr_node(texp->var.op.argv[1],
                            indexp->attrv[i].attrname, ">");
                    if (nodep == NULL) {
                        nodep = _RDB_attr_node(texp->var.op.argv[1],
                                indexp->attrv[i].attrname, "<=");
                        if (nodep == NULL) {
                            nodep = _RDB_attr_node(texp->var.op.argv[1],
                                    indexp->attrv[i].attrname, "<");
                            if (nodep == NULL)
                                break;
                        }
                    }
                }
                if (strcmp(nodep->var.op.name, ">=") == 0
                        || strcmp(nodep->var.op.name, ">") == 0) {
                    stopexp = _RDB_attr_node(texp->var.op.argv[1],
                            indexp->attrv[i].attrname, "<=");
                    if (stopexp == NULL) {
                        stopexp = _RDB_attr_node(texp->var.op.argv[1],
                                indexp->attrv[i].attrname, "<");
                    }
                    if (stopexp != NULL) {
                        attrexp = stopexp;
                        if (is_and(attrexp))
                            attrexp = attrexp->var.op.argv[1];
                        if (move_node(texp, &ixexp, stopexp, ecp, txp) != RDB_OK)
                            return RDB_ERROR;
                        stopexp = attrexp;
                    }
                }
                all_eq = RDB_FALSE;
            }
        } else {
            nodep = _RDB_attr_node(texp->var.op.argv[1],
                    indexp->attrv[i].attrname, "=");
        }
        attrexp = nodep;
        if (is_and(attrexp))
            attrexp = attrexp->var.op.argv[1];

        objpc++;

        if (indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp)) {
            if (strcmp(attrexp->var.op.name, "=") == 0
                    || strcmp(attrexp->var.op.name, ">=") == 0
                    || strcmp(attrexp->var.op.name, ">") == 0)
                asc = indexp->attrv[i].asc;
            else
                asc = (RDB_bool) !indexp->attrv[i].asc;
        }
        
        if (move_node(texp, &ixexp, nodep, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }

    if (texp->var.op.argv[0]->kind == RDB_EX_TBP) {
        objpv = _RDB_index_objpv(indexp, ixexp, texp->var.op.argv[0]->var.tbref.tbp->typ,
                objpc, all_eq, asc);
    } else {
        objpv = _RDB_index_objpv(indexp, ixexp,
                texp->var.op.argv[0]->var.op.argv[0]->var.tbref.tbp->typ,
                objpc, all_eq, asc);        
    }
    if (objpv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    if (texp->var.op.argv[1] != NULL) {
        RDB_expression *sitexp;

        /*
         * Split table into two
         */
        if (ixexp == NULL) {
            ixexp = RDB_bool_to_expr(RDB_TRUE, ecp);
            if (ixexp == NULL)
                return RDB_ERROR;
        }
        sitexp = RDB_ro_op("WHERE", 2, ecp);
        if (sitexp == NULL)
            return RDB_ERROR;
        RDB_add_arg(sitexp, texp->var.op.argv[0]);
        RDB_add_arg(sitexp, ixexp);
        
        sitexp->var.op.optinfo.objpc = objpc;
        sitexp->var.op.optinfo.objpv = objpv;
        sitexp->var.op.optinfo.asc = asc;
        sitexp->var.op.optinfo.all_eq = all_eq;
        sitexp->var.op.optinfo.stopexp = stopexp;
    } else {
        /*
         * Convert table to index select
         */
        texp->var.op.argv[1] = ixexp;
        texp->var.op.optinfo.objpc = objpc;
        texp->var.op.optinfo.objpv = objpv;
        texp->var.op.optinfo.asc = asc;
        texp->var.op.optinfo.all_eq = all_eq;
        texp->var.op.optinfo.stopexp = stopexp;
    }
    return RDB_OK;
}

static unsigned
table_cost(RDB_expression *texp)
{
    _RDB_tbindex *indexp;

    if (texp->kind == RDB_EX_TBP)
        return texp->var.tbref.tbp->var.tb.stp != NULL ?
                texp->var.tbref.tbp->var.tb.stp->est_cardinality : 0;

    if (texp->kind == RDB_EX_OBJ)
        return texp->var.obj.var.tb.stp->est_cardinality;

    if (strcmp(texp->var.op.name, "SEMIMINUS") == 0)
        return table_cost(texp->var.op.argv[0]); /* !! */

    if (strcmp(texp->var.op.name, "MINUS") == 0)
        return table_cost(texp->var.op.argv[0]); /* !! */

    if (strcmp(texp->var.op.name, "UNION") == 0)
        return table_cost(texp->var.op.argv[0])
                + table_cost(texp->var.op.argv[1]);

    if (strcmp(texp->var.op.name, "SEMIJOIN") == 0)
        return table_cost(texp->var.op.argv[0]); /* !! */

    if (strcmp(texp->var.op.name, "INTERSECT") == 0)
        return table_cost(texp->var.op.argv[0]); /* !! */

    if (strcmp(texp->var.op.name, "WHERE") == 0) {
        if (texp->var.op.optinfo.objpc == 0)
            return table_cost(texp->var.op.argv[0]);
        if (texp->var.op.argv[0]->kind == RDB_EX_TBP) {
            indexp = texp->var.op.argv[0]->var.tbref.indexp;
        } else {
            indexp = texp->var.op.argv[0]->var.op.argv[0]->var.tbref.indexp;
        }
        if (indexp->idxp == NULL)
            return 1;
        if (indexp->unique)
            return 2;
        if (!RDB_index_is_ordered(indexp->idxp))
            return 3;
        return 4;
    }
    if (strcmp(texp->var.op.name, "JOIN") == 0) {
        if (texp->var.op.argv[1]->kind == RDB_EX_TBP
                && texp->var.op.argv[1]->var.tbref.indexp != NULL) {
            indexp = texp->var.op.argv[1]->var.tbref.indexp;
            if (indexp->idxp == NULL)
                return table_cost(texp->var.op.argv[0]);
            if (indexp->unique)
                return table_cost(texp->var.op.argv[0]) * 2;
            if (!RDB_index_is_ordered(indexp->idxp))
                return table_cost(texp->var.op.argv[0]) * 3;
            return table_cost(texp->var.op.argv[0]) * 4;
        }
        return table_cost(texp->var.op.argv[0])
                * table_cost(texp->var.op.argv[1]);
    }
    if (strcmp(texp->var.op.name, "EXTEND") == 0
             || strcmp(texp->var.op.name, "PROJECT") == 0
             || strcmp(texp->var.op.name, "REMOVE") == 0
             || strcmp(texp->var.op.name, "SUMMARIZE") == 0
             || strcmp(texp->var.op.name, "RENAME") == 0
             || strcmp(texp->var.op.name, "WRAP") == 0
             || strcmp(texp->var.op.name, "UNWRAP") == 0
             || strcmp(texp->var.op.name, "GROUP") == 0
             || strcmp(texp->var.op.name, "UNGROUP") == 0)
        return table_cost(texp->var.op.argv[0]);
    if (strcmp(texp->var.op.name, "DIVIDE_BY_PER") == 0) {
        return table_cost(texp->var.op.argv[0])
                * table_cost(texp->var.op.argv[1]); /* !! */
    }
    abort();
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
    RDB_expression *chexp = texp->var.op.argv[0];

    if (_RDB_transform(chexp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (chexp->kind == RDB_EX_TBP
        || (chexp->kind == RDB_EX_RO_OP
            && strcmp(chexp->var.op.name, "PROJECT") == 0
            && chexp->var.op.argv[0]->kind == RDB_EX_TBP)) {
        /* Convert condition into 'unbalanced' form */
        unbalance_and(texp->var.op.argv[1]);
    }

    tbc = mutate(chexp, tbpv, cap, ecp, txp);
    if (tbc < 0)
        return tbc;

    for (i = 0; i < tbc; i++) {
        RDB_expression *nexp;
        RDB_expression *exp = RDB_dup_expr(texp->var.op.argv[1], ecp);
        if (exp == NULL)
            return RDB_ERROR;

        nexp = RDB_ro_op("WHERE", 2, ecp);
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
                && tbpv[i]->var.tbref.indexp != NULL)
        {
            _RDB_tbindex *indexp = tbpv[i]->var.tbref.indexp;
            if ((indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp))
                    || expr_covers_index(exp, indexp)
                            == indexp->attrc) {
                if (split_by_index(nexp, indexp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }
        } else if (tbpv[i]->kind == RDB_EX_RO_OP
                && strcmp(tbpv[i]->var.op.name, "PROJECT") == 0
                && tbpv[i]->var.op.argv[0]->kind == RDB_EX_TBP
                && tbpv[i]->var.op.argv[0]->var.tbref.indexp != NULL) {
            _RDB_tbindex *indexp = tbpv[i]->var.op.argv[0]->var.tbref.indexp;
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
            newexp = dup_expr_vt(exp->var.op.argv[0], ecp);
            if (newexp == NULL)
                return NULL;
            return RDB_tuple_attr(newexp, exp->var.op.name, ecp);
        case RDB_EX_GET_COMP:
            newexp = dup_expr_vt(exp->var.op.argv[0], ecp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_comp(newexp, exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            int i;

            newexp = RDB_ro_op(exp->var.op.name, exp->var.op.argc,
                    ecp);
            for (i = 0; i < exp->var.op.argc; i++) {
                RDB_expression *argp = dup_expr_vt(exp->var.op.argv[i], ecp);
                if (argp == NULL)
                    return NULL;
                RDB_add_arg(newexp, argp);
            }
            return newexp;
        }
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj, ecp);
        case RDB_EX_TBP:
            if (exp->var.tbref.tbp->var.tb.exp == NULL)
                return RDB_table_ref_to_expr(exp->var.tbref.tbp, ecp);
            return dup_expr_vt(exp->var.tbref.tbp->var.tb.exp, ecp);
        case RDB_EX_VAR:
            return RDB_expr_var(exp->var.varname, ecp);
    }
    abort();
}

static int
mutate_vt(RDB_expression *texp, int nargc, RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j, k;
    int ntbc;
    int otbc = 0;

    for (j = 0; j < nargc; j++) {
        ntbc = mutate(texp->var.op.argv[j], &tbpv[otbc], cap - otbc, ecp, txp);
        if (ntbc < 0)
            return ntbc;
        for (i = otbc; i < otbc + ntbc; i++) {
            RDB_expression *nexp = RDB_ro_op(texp->var.op.name,
                    texp->var.op.argc, ecp);
            if (nexp == NULL)
                return RDB_ERROR;

            for (k = 0; k < texp->var.op.argc; k++) {
                if (k == j) {
                    RDB_add_arg(nexp, tbpv[i]);
                } else {
                    RDB_expression *otexp = dup_expr_vt(texp->var.op.argv[k], ecp);
                    if (otexp == NULL)
                        return RDB_ERROR;
                    RDB_add_arg(nexp, otexp);
                }
            }
            tbpv[i] = nexp;
        }
        otbc += ntbc;
    }
    return otbc;
}

static int
mutate_full_vt(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    return mutate_vt(texp, texp->var.op.argc, tbpv, cap, ecp, txp);
}

static void
table_to_empty(RDB_expression *exp, RDB_exec_context *ecp)
{
    /* !!
    switch (tbp->kind) {
        case RDB_TB_SELECT:
            RDB_drop_expr(tbp->var.select.exp, ecp);
            if (!tbp->var.select.tbp->is_persistent) {
                RDB_drop_table(tbp->var.select.tbp, ecp, NULL);
            }
            tbp->kind = RDB_TB_REAL;
            break;
        case RDB_TB_SEMIMINUS:
            if (!tbp->var.semiminus.tb1p->is_persistent) {
                RDB_drop_table(tbp->var.semiminus.tb1p, ecp, NULL);
            }
            if (!tbp->var.semiminus.tb2p->is_persistent) {
                RDB_drop_table(tbp->var.semiminus.tb2p, ecp, NULL);
            }
            tbp->kind = RDB_TB_REAL;
            break;
        default: ;
    }
    */
}

static int
replace_empty(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
	int ret;
    struct _RDB_tx_and_ec te;

    te.txp = txp;
    te.ecp = ecp;

    /* Check if there is a constraint that says the table is empty */
    if (txp->dbp != NULL
            && RDB_hashtable_get(&txp->dbp->dbrootp->empty_tbtab,
                    exp, &te) != NULL) {
            table_to_empty(exp, ecp);
    } else if (exp->kind == RDB_EX_RO_OP) {
    	int i;
    	
    	for (i = 0; i < exp->var.op.argc; i++) {
            ret = replace_empty(exp->var.op.argv[0], ecp, txp);
            if (ret != RDB_OK)
                return ret;
    	}
    }
    return RDB_OK;
}

#ifdef REMOVED
static int
transform_semiminus_union(RDB_object *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object **restbpp)
{
    int ret;
    RDB_object *tb1p, *tb2p, *tb3p, *tb4p;

    tb1p = _RDB_dup_vtable(tbp->var.semiminus.tb1p->var._union.tb1p, ecp);
    if (tb1p == NULL) {
        return RDB_ERROR;
    }

    tb2p = _RDB_dup_vtable(tbp->var.semiminus.tb2p, ecp);
    if (tb2p == NULL) {
        RDB_drop_table (tb1p, ecp, NULL);
        return RDB_ERROR;
    }

    tb3p = RDB_semiminus(tb1p, tb2p, ecp);
    if (tb3p == NULL) {
        RDB_drop_table(tb1p, ecp, NULL);
        RDB_drop_table(tb2p, ecp, NULL);
        return RDB_ERROR;
    }

    tb1p = _RDB_dup_vtable(tbp->var.semiminus.tb1p->var._union.tb2p, ecp);
    if (tb1p == NULL) {
        RDB_drop_table(tb3p, ecp, NULL);
        return RDB_ERROR;
    }
    tb2p = _RDB_dup_vtable(tbp->var.semiminus.tb2p, ecp);
    if (tb2p == NULL) {
        RDB_drop_table(tb1p, ecp, NULL);
        RDB_drop_table(tb3p, ecp, NULL);
        return RDB_ERROR;
    }

    tb4p = RDB_semiminus(tb1p, tb2p, ecp);
    if (tb4p == NULL) {
        RDB_drop_table(tb1p, ecp, NULL);
        RDB_drop_table(tb2p, ecp, NULL);
        RDB_drop_table(tb3p, ecp, NULL);        
        return RDB_ERROR;
    }

    *restbpp = RDB_union(tb3p, tb4p, ecp);
    if (*restbpp == NULL) {
        RDB_drop_table(tb3p, ecp, NULL);
        RDB_drop_table(tb4p, ecp, NULL);
        return RDB_ERROR;
    }
    ret = replace_empty(*restbpp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(*restbpp, ecp, NULL);
        return ret;
    }
    return RDB_OK;
}

static int
mutate_semiminus(RDB_expression *texp, RDB_object **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc;
    int i;
    int ret;

    tbc = mutate(tbp->var.semiminus.tb1p, tbpv, cap, ecp, txp);
    if (tbc < 0)
        return tbc;
    for (i = 0; i < tbc; i++) {
        RDB_object *ntbp;
        RDB_object *otbp = _RDB_dup_vtable(tbp->var.semiminus.tb2p, ecp);
        if (otbp == NULL)
            return RDB_ERROR;

        ntbp = RDB_semiminus(tbpv[i], otbp, ecp);
        if (ntbp == NULL)
            return RDB_ERROR;
        tbpv[i] = ntbp;
    }
    if (tbc < cap && tbp->var.semiminus.tb1p->kind == RDB_TB_UNION) {
        ret = transform_semiminus_union(tbp, ecp, txp, &tbpv[tbc]);
        if (ret != RDB_OK)
            return RDB_ERROR;
        tbc++;
        /* !! mutate_union */
    }
    return tbc;
}

static int
mutate_semijoin(RDB_expression *texp, RDB_object **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc1, tbc2;
    int i;

    tbc1 = mutate(tbp->var.semijoin.tb1p, tbpv, cap, ecp, txp);
    if (tbc1 < 0)
        return tbc1;
    for (i = 0; i < tbc1; i++) {
        RDB_object *ntbp;
        RDB_object *otbp = _RDB_dup_vtable(tbp->var.semijoin.tb2p, ecp);
        if (otbp == NULL)
            return RDB_ERROR;

        ntbp = RDB_semijoin(tbpv[i], otbp, ecp);
        if (ntbp == NULL)
            return RDB_ERROR;
        tbpv[i] = ntbp;
    }
    tbc2 = mutate(tbp->var.semijoin.tb2p, &tbpv[tbc1], cap - tbc1, ecp, txp);
    if (tbc2 < 0)
        return tbc2;
    for (i = tbc1; i < tbc1 + tbc2; i++) {
        RDB_object *ntbp;
        RDB_object *otbp = _RDB_dup_vtable(tbp->var.semijoin.tb1p, ecp);
        if (otbp == NULL)
            return RDB_ERROR;

        ntbp = RDB_semijoin(otbp, tbpv[i], ecp);
        if (ntbp == NULL)
            return RDB_ERROR;
        tbpv[i] = ntbp;
    }
    return tbc1 + tbc2;
}

static int
mutate_extend(RDB_expression *texp, RDB_object **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc;
    int i, j;
    RDB_virtual_attr *extv;

    tbc = mutate(tbp->var.op.argv[0], tbpv, cap, ecp, txp);
    if (tbc < 0)
        return tbc;

    extv = malloc(sizeof (RDB_virtual_attr)
            * tbp->var.extend.attrc);
    if (extv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    for (i = 0; i < tbp->var.extend.attrc; i++)
        extv[i].name = tbp->var.extend.attrv[i].name;

    for (i = 0; i < tbc; i++) {
        RDB_object *ntbp;

        for (j = 0; j < tbp->var.extend.attrc; j++) {
            extv[j].exp = RDB_dup_expr(tbp->var.extend.attrv[j].exp, ecp);
            if (extv[j].exp == NULL)
                return RDB_ERROR;
        }
        ntbp = _RDB_extend(tbpv[i], tbp->var.extend.attrc, extv, ecp, txp);
        if (ntbp == NULL) {
            for (j = 0; j < tbp->var.extend.attrc; j++)
                RDB_drop_expr(extv[j].exp, ecp);
            free(extv);
            return RDB_ERROR;
        }
        tbpv[i] = ntbp;
    }
    free(extv);
    return tbc;
}

static int
mutate_summarize(RDB_expression *texp, RDB_object **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc;
    int i, j;
    RDB_summarize_add *addv;

    tbc = mutate(tbp->var.op.argv[0], tbpv, cap, ecp, txp);
    if (tbc < 0)
        return tbc;

    addv = malloc(sizeof (RDB_summarize_add) * tbp->var.summarize.addc);
    if (addv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    for (i = 0; i < tbp->var.summarize.addc; i++) {
        addv[i].op = tbp->var.summarize.addv[i].op;
        addv[i].name = tbp->var.summarize.addv[i].name;
    }

    for (i = 0; i < tbc; i++) {
        RDB_object *ntbp;
        RDB_object *otbp = _RDB_dup_vtable(tbp->var.summarize.tb2p, ecp);
        if (otbp == NULL) {
            free(addv);
            return RDB_ERROR;
        }

        for (j = 0; j < tbp->var.summarize.addc; j++) {
            if (tbp->var.summarize.addv[j].exp != NULL) {
                addv[j].exp = RDB_dup_expr(tbp->var.summarize.addv[j].exp, ecp);
                if (addv[j].exp == NULL)
                    return RDB_ERROR;
            } else {
                addv[j].exp = NULL;
            }
        }

        ntbp = RDB_summarize(tbpv[i], otbp, tbp->var.summarize.addc, addv,
                ecp, txp);
        if (ntbp == NULL) {
            for (j = 0; j < tbp->var.summarize.addc; j++)
                RDB_drop_expr(addv[j].exp, ecp);
            free(addv);
            return RDB_ERROR;
        }
        tbpv[i] = ntbp;
    }
    free(addv);
    return tbc;
}

static int
mutate_project(RDB_expression *texp, RDB_object **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc;
    int i;
    char **namev;

    if (tbp->var.project.tbp->kind == RDB_TB_REAL
            && tbp->var.project.tbp->stp != NULL
            && tbp->var.project.tbp->stp->indexc > 0) {
        tbc = tbp->var.project.tbp->stp->indexc;
        for (i = 0; i < tbc; i++) {
            _RDB_tbindex *indexp = &tbp->var.project.tbp->stp->indexv[i];
            RDB_object *ptbp = _RDB_dup_vtable(tbp, ecp);
            if (ptbp == NULL)
                return RDB_ERROR;
            ptbp->var.project.indexp = indexp;
            tbpv[i] = ptbp;
        }
    } else {
        tbc = mutate(texp->var.op.argv[0], tbpv, cap, ecp, txp);
        if (tbc < 0)
            return tbc;

        namev = malloc(sizeof (char *) * tbp->typ->var.basetyp->var.tuple.attrc);
        if (namev == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        for (i = 0; i < tbp->typ->var.basetyp->var.tuple.attrc; i++)
            namev[i] = tbp->typ->var.basetyp->var.tuple.attrv[i].name;

        for (i = 0; i < tbc; i++) {
            RDB_object *ntbp;

            ntbp = RDB_project(tbpv[i], tbp->typ->var.basetyp->var.tuple.attrc,
                    namev, ecp);
            if (ntbp == NULL) {
                free(namev);
                return RDB_ERROR;
            }
            tbpv[i] = ntbp;
        }
        free(namev);
    }
    return tbc;
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
#endif

static int
index_joins(RDB_expression *otexp, RDB_expression *itexp, 
        RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;
    int tbc;
    int i;
    RDB_type *ottyp = RDB_expr_type(otexp, NULL, ecp, txp);
    if (ottyp == NULL)
        return RDB_ERROR;

    /*
     * Use indexes of table #2 which cover table #1
     */

    tbp = itexp->var.tbref.tbp;
    tbc = 0;
    for (i = 0; i < tbp->var.tb.stp->indexc && tbc < cap; i++) {
        if (table_covers_index(ottyp, &tbp->var.tb.stp->indexv[i])) {
            RDB_expression *arg1p, *ntexp;
            RDB_expression *refargp = RDB_table_ref_to_expr(tbp, ecp);
            if (refargp == NULL) {
                RDB_drop_type(ottyp, ecp, NULL);
                return RDB_ERROR;
            }

            refargp->var.tbref.indexp = &tbp->var.tb.stp->indexv[i];
            ntexp = RDB_ro_op("JOIN", 2, ecp);
            if (ntexp == NULL) {
                RDB_drop_type(ottyp, ecp, NULL);
                return RDB_ERROR;
            }

            arg1p = RDB_dup_expr(otexp, ecp);
            if (arg1p == NULL) {
                RDB_drop_type(ottyp, ecp, NULL);
                RDB_drop_expr(ntexp, ecp);
                return RDB_ERROR;
            }

            RDB_add_arg(ntexp, arg1p);
            RDB_add_arg(ntexp, refargp);
            tbpv[tbc++] = ntexp;
        }
    }

    RDB_drop_type(ottyp, ecp, NULL);
    return tbc;
}

static int
mutate_join(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc = 0;

    if (texp->var.op.argv[0]->kind != RDB_EX_TBP
            && texp->var.op.argv[1]->kind != RDB_EX_TBP) {
        return mutate_full_vt(texp, tbpv, cap, ecp, txp);
    }
    
    if (texp->var.op.argv[1]->kind == RDB_EX_TBP) {
        tbc = index_joins(texp->var.op.argv[0], texp->var.op.argv[1],
                tbpv, cap, ecp, txp);
        if (tbc == RDB_ERROR)
            return RDB_ERROR;
    }

    if (texp->var.op.argv[0]->kind == RDB_EX_TBP) {
        int ret = index_joins(texp->var.op.argv[1], texp->var.op.argv[0],
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
    if (texp->var.tbref.tbp->kind == RDB_OB_TABLE
            && texp->var.tbref.tbp->var.tb.stp != NULL
            && texp->var.tbref.tbp->var.tb.stp->indexc > 0) {
        int i;
        int tbc = texp->var.tbref.tbp->var.tb.stp->indexc;
        if (tbc > cap)
            tbc = cap;

        for (i = 0; i < tbc; i++) {
            _RDB_tbindex *indexp = &texp->var.tbref.tbp->var.tb.stp->indexv[i];
            RDB_expression *tiexp = RDB_table_ref_to_expr(
                    texp->var.tbref.tbp, ecp);
            if (tiexp == NULL)
                return RDB_ERROR;
            tiexp->var.tbref.indexp = indexp;
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

    if (strcmp(texp->var.op.name, "WHERE") == 0) {
        return mutate_where(texp, tbpv, cap, ecp, txp);
    }

    if (strcmp(texp->var.op.name, "JOIN") == 0) {
        return mutate_join(texp, tbpv, cap, ecp, txp);
    }

    if (strcmp(texp->var.op.name, "UNION") == 0
            || strcmp(texp->var.op.name, "MINUS") == 0
            || strcmp(texp->var.op.name, "SEMIMINUS") == 0
            || strcmp(texp->var.op.name, "INTERSECT") == 0
            || strcmp(texp->var.op.name, "SEMIJOIN") == 0
            || strcmp(texp->var.op.name, "JOIN") == 0) {
        return mutate_full_vt(texp, tbpv, cap, ecp, txp);
    }
    if (strcmp(texp->var.op.name, "EXTEND") == 0
            || strcmp(texp->var.op.name, "PROJECT") == 0
            || strcmp(texp->var.op.name, "REMOVE") == 0
            || strcmp(texp->var.op.name, "RENAME") == 0
            || strcmp(texp->var.op.name, "SUMMARIZE") == 0
            || strcmp(texp->var.op.name, "WRAP") == 0
            || strcmp(texp->var.op.name, "UNWRAP") == 0
            || strcmp(texp->var.op.name, "GROUP") == 0
            || strcmp(texp->var.op.name, "UNGROUP") == 0) {
        return mutate_vt(texp, 1, tbpv, cap, ecp, txp);
    }
    if (strcmp(texp->var.op.name, "DIVIDE_BY_PER") == 0) {
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

#ifdef REMOVED
    /* Check if the index must be sorted */
    if (seqitc > 0) {
        _RDB_tbindex *indexp = _RDB_sortindex(tbp);
        if (indexp == NULL || !_RDB_index_sorts(indexp, seqitc, seqitv))
        {
            int scost = (((double) cost) /* !! * log10(cost) */ / 7);

            if (scost == 0)
                scost = 1;
            cost += scost;
        }
    }
#endif

    return cost;
}

int
_RDB_optimize(RDB_object *tbp, int seqitc, const RDB_seq_item seqitv[],
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object **ntbpp)
{
    int i;
    RDB_expression *nexp;

    if (tbp->var.tb.exp == NULL) {
        if (seqitc > 0 && tbp->var.tb.stp != NULL) {
            /*
             * Check if an index can be used for sorting
             */

            for (i = 0; i < tbp->var.tb.stp->indexc
                    && !_RDB_index_sorts(&tbp->var.tb.stp->indexv[i],
                            seqitc, seqitv);
                    i++);
            /* If yes, create reference */
            if (i < tbp->var.tb.stp->indexc) {
                nexp = RDB_table_ref_to_expr(tbp, ecp);
                if (nexp == NULL)
                    return RDB_ERROR;
                nexp->var.tbref.indexp = &tbp->var.tb.stp->indexv[i];
                *ntbpp = RDB_expr_to_vtable(nexp, ecp, txp);
                if (*ntbpp == NULL)
                    return RDB_ERROR;
                return RDB_OK;
            }
        }
        *ntbpp = tbp;
    } else {
        nexp = _RDB_optimize_expr(tbp->var.tb.exp,
                seqitc, seqitv, ecp, txp);
        if (nexp == NULL)
            return RDB_ERROR;
        *ntbpp = RDB_expr_to_vtable(nexp, ecp, txp);
        if (*ntbpp == NULL)
            return RDB_ERROR;
    }

    return RDB_OK;
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

    /*
     * Algebraic optimization
     */

    if (_RDB_transform(nexp, ecp, txp) != RDB_OK)
        return NULL;

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
