/*
 * $Id$
 *
 * Copyright (C) 2005 René Hartmann.
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

    if (constrp->exp->kind == RDB_EX_RO_OP
            && constrp->exp->var.op.argc == 1
            && strcmp(constrp->exp->var.op.name, "IS_EMPTY") == 0) {
        RDB_object resobj;
        RDB_table *ptbp, *ctbp;

        RDB_init_obj(&resobj);
        ret = RDB_evaluate(constrp->exp->var.op.argv[0], NULL, ecp, txp,
                &resobj);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&resobj, ecp);
            return ret;
        }
        ptbp = RDB_project(RDB_obj_table(&resobj), 0, NULL, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&resobj, ecp);
            return ret;
        }
        resobj.var.tbp = NULL;
        RDB_destroy_obj(&resobj, ecp);

        ret = _RDB_transform(ptbp, ecp, txp);
        if (ret != RDB_OK) {
            RDB_drop_table(ptbp, ecp, NULL);
            return ret;
        }
        if (ptbp->kind == RDB_TB_PROJECT) {
            ctbp = ptbp->var.project.tbp;
            _RDB_free_table(ptbp, ecp);
        } else {
            ctbp = ptbp;
        }
        ret = RDB_hashtable_put(&txp->dbp->dbrootp->empty_tbtab,
                ctbp, txp);
        if (ret != RDB_OK) {
            RDB_drop_table(ctbp, ecp, NULL);
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
        ret = _RDB_deserialize_expr(RDB_tuple_get(tplp, "I_EXPR"), ecp, txp,
                &constrp->exp);
        if (ret != RDB_OK) {
            free(constrp->name);
            free(constrp);
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
        return RDB_PREDICATE_VIOLATION;
    }

    if (!RDB_tx_db(txp)->dbrootp->constraints_read) {
        ret = _RDB_read_constraints(ecp, txp);
        if (ret != RDB_OK)
            return ret;
        RDB_tx_db(txp)->dbrootp->constraints_read = RDB_TRUE;
    }

    constrp = malloc(sizeof (RDB_constraint));
    if (constrp == NULL)
        return RDB_NO_MEMORY;

    constrp->exp = exp;
    add_empty_tb(constrp, ecp, txp);

    constrp->name = RDB_dup_str(name);
    if (constrp->name == NULL) {
        ret = RDB_NO_MEMORY;
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

    _RDB_handle_syserr(txp, ret);
    return ret;
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
        if (constrp == NULL)
            return RDB_NOT_FOUND;

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
            if (constrp->nextp == NULL)
                return RDB_NOT_FOUND;
            hconstrp = constrp->nextp;
            constrp->nextp = constrp->nextp->nextp;
            RDB_drop_expr(hconstrp->exp, ecp);
            free(hconstrp->name);
            free(hconstrp);
        }
    }

    /* Delete constraint from catalog */
    condp = RDB_eq(RDB_expr_attr("CONSTRAINTNAME"), RDB_string_to_expr(name));
    if (condp == NULL)
        return RDB_NO_MEMORY;
    ret = RDB_delete(dbrootp->constraints_tbp, condp, ecp, txp);
    RDB_drop_expr(condp, ecp);

    _RDB_handle_syserr(txp, ret);
    return ret;
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
            return RDB_PREDICATE_VIOLATION;
        }
        constrp = constrp->nextp;
    }
    return RDB_OK;
}
