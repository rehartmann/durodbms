/*
 * $Id$
 *
 * Copyright (C) 2005 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

/*
 * Functions for declarative integrity constraints
 */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>

/*
 * If the constraint is of the form IS_EMPTY(table), add table to
 * hashtable of empty tables
 */
static int
add_empty_tb(RDB_constraint *constrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    struct _RDB_tx_and_ec te;

    te.txp = txp;
    te.ecp = ecp;

    if (constrp->exp->kind == RDB_EX_RO_OP
            && constrp->exp->var.op.argc == 1
            && strcmp(constrp->exp->var.op.name, "IS_EMPTY") == 0) {
        RDB_object *ptbp;
        RDB_expression *pexp;
        RDB_expression *argexp = RDB_dup_expr(constrp->exp->var.op.argv[0],
                NULL);
        if (argexp == NULL)
            return RDB_ERROR;

        pexp = RDB_ro_op_va("PROJECT", ecp, argexp, (RDB_expression *) NULL);
        if (pexp == NULL) {
            RDB_drop_expr(argexp, ecp);
            return RDB_ERROR;
        }

        ret = _RDB_transform(pexp, ecp, txp);
        if (ret != RDB_OK) {
            RDB_drop_table(ptbp, ecp, NULL);
            return RDB_ERROR;
        }
/* !!
        if (ptbp->kind == RDB_TB_PROJECT) {
            ctbp = ptbp->var.project.tbp;
            _RDB_free_table(ptbp, ecp);
        } else {
            ctbp = ptbp;
        }
*/
        ptbp = _RDB_expr_to_vtable(pexp, ecp, txp);
        if (ptbp != NULL) {
            RDB_drop_expr(pexp, ecp);
            return ret;
        }
        ret = RDB_hashtable_put(&txp->dbp->dbrootp->empty_tbtab,
                ptbp, &te);
        if (ret != RDB_OK) {
            RDB_drop_table(ptbp, ecp, NULL);
            return ret;
        }
    }
    return RDB_OK;
}

/*
 * Read constraints from catalog
 */
int
_RDB_read_constraints(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object constrs;
    RDB_object *tplp;
    RDB_dbroot *dbrootp = RDB_tx_db(txp)->dbrootp;

    RDB_init_obj(&constrs);

    ret = RDB_table_to_array(&constrs, dbrootp->constraints_tbp, 0, NULL, ecp,
            txp);
    if (ret != RDB_OK)
        goto cleanup;

    for (i = 0; (tplp = RDB_array_get(&constrs, i, ecp)) != NULL; i++) {
        RDB_constraint *constrp = malloc(sizeof(RDB_constraint));

        if (constrp == NULL) {
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        constrp->name = RDB_dup_str(RDB_obj_string(RDB_tuple_get(tplp,
                "CONSTRAINTNAME")));
        if (constrp->name == NULL) {
            free(constrp);
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        constrp->exp = _RDB_binobj_to_expr(RDB_tuple_get(tplp, "I_EXPR"), ecp,
                txp);
        if (constrp->exp == NULL) {
            free(constrp->name);
            free(constrp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        add_empty_tb(constrp, ecp, txp);
        constrp->nextp = dbrootp->first_constrp;
        dbrootp->first_constrp = constrp;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        ret = RDB_OK;
    } else {
        ret = RDB_ERROR;
    }

cleanup:
    RDB_destroy_obj(&constrs, ecp);
    return ret;
}

int
RDB_create_constraint(const char *name, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_dbroot *dbrootp;
    RDB_bool res;
    RDB_constraint *constrp;

    /* Check constraint */
    ret = RDB_evaluate_bool(exp, NULL, ecp, txp, &res);
    if (ret != RDB_OK)
        return ret;
    if (!res) {
        RDB_raise_predicate_violation(name, ecp);
        return RDB_ERROR;
    }

    if (!RDB_tx_db(txp)->dbrootp->constraints_read) {
        ret = _RDB_read_constraints(ecp, txp);
        if (ret != RDB_OK)
            return ret;
        RDB_tx_db(txp)->dbrootp->constraints_read = RDB_TRUE;
    }

    constrp = malloc(sizeof (RDB_constraint));
    if (constrp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    constrp->exp = exp;
    add_empty_tb(constrp, ecp, txp);

    constrp->name = RDB_dup_str(name);
    if (constrp->name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    /* Create constraint in catalog */
    ret = _RDB_cat_create_constraint(name, exp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    /* Insert constraint into list */
    dbrootp = RDB_tx_db(txp)->dbrootp;
    constrp->nextp = dbrootp->first_constrp;
    dbrootp->first_constrp = constrp;
    
    return RDB_OK;

error:
    free(constrp->name);
    free(constrp);

    return RDB_ERROR;
}

int
RDB_drop_constraint(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *condp;
    RDB_dbroot *dbrootp = RDB_tx_db(txp)->dbrootp;

    if (dbrootp->constraints_read) {
        /* Delete constraint from list */
        RDB_constraint *constrp = dbrootp->first_constrp;
        if (constrp == NULL) {
            RDB_raise_not_found(name, ecp);
            return RDB_ERROR;
        }

        if (strcmp(constrp->name, name) == 0) {
            dbrootp->first_constrp = constrp->nextp;
            RDB_drop_expr(constrp->exp, ecp);
            free(constrp->name);
            free(constrp);
        } else {
            RDB_constraint *hconstrp;

            while (constrp->nextp != NULL
                    && strcmp(constrp->nextp->name, name) !=0) {
                constrp = constrp->nextp;
            }
            if (constrp->nextp == NULL) {
                 RDB_raise_not_found(name, ecp);
                 return RDB_ERROR;
             }

            hconstrp = constrp->nextp;
            constrp->nextp = constrp->nextp->nextp;
            RDB_drop_expr(hconstrp->exp, ecp);
            free(hconstrp->name);
            free(hconstrp);
        }
    }

    /* Delete constraint from catalog */
    condp = RDB_eq(RDB_expr_var("CONSTRAINTNAME", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (condp == NULL) {
        return RDB_ERROR;
    }
    ret = RDB_delete(dbrootp->constraints_tbp, condp, ecp, txp);
    RDB_drop_expr(condp, ecp);

    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

int
_RDB_check_constraints(const RDB_constraint *constrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_bool b;
    int ret;

    while (constrp != NULL) {
        ret = RDB_evaluate_bool(constrp->exp, NULL, ecp, txp, &b);
        if (ret != RDB_OK) {
            return ret;
        }
        if (!b) {
            RDB_raise_predicate_violation(constrp->name, ecp);
            return RDB_ERROR;
        }
        constrp = constrp->nextp;
    }
    return RDB_OK;
}
