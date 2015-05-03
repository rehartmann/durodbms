/*
 * iinterp_eval.c
 *
 *  Created on: 15.03.2014
 *      Author: Rene Hartmann
 */

#include <rel/rdb.h>
#include "interp_eval.h"
#include "iinterp.h"
#include "interp_core.h"

/**
 * Evaluate expression.
 * If evaluation fails with OPERATOR_NOT_FOUND_ERROR ad no transaction is running
 * but a environment is available, start a transaction and try again.
 */
int
Duro_evaluate_retry(RDB_expression *exp, Duro_interp *interp,
        RDB_exec_context *ecp, RDB_object *resultp)
{
    RDB_transaction tx;
    RDB_database *dbp;
    RDB_exec_context ec;
    int ret;

    ret = RDB_evaluate(exp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL, resultp);
    /*
     * Success or error different from OPERATOR_NOT_FOUND_ERROR
     * -> return
     */
    if (ret == RDB_OK
            || RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR)
        return ret;
    /*
     * If a transaction is already active or no environment is
     * available, stop
     */
    if (interp->txnp != NULL || interp->envp == NULL)
        return ret;
    /*
     * Start transaction and retry.
     * If this succeeds, the operator will be in memory next time
     * so no transaction will be needed.
     */
    RDB_init_exec_context(&ec);
    dbp = Duro_get_db(interp, &ec);
    RDB_destroy_exec_context(&ec);
    if (dbp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
        return RDB_ERROR;
    ret = RDB_evaluate(exp, &Duro_get_var, interp, interp->envp, ecp, &tx, resultp);
    if (ret != RDB_OK) {
        RDB_commit(ecp, &tx);
        return ret;
    }
    return RDB_commit(ecp, &tx);
}

RDB_type *
Duro_expr_type_retry(RDB_expression *exp, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_database *dbp;
    RDB_exec_context ec;
    RDB_type *typ = RDB_expr_type(exp, &Duro_get_var_type, interp,
            interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    /*
     * Success or error different from OPERATOR_NOT_FOUND_ERROR
     * -> return
     */
    if (typ != NULL
            || RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR)
        return typ;

    /*
     * If a transaction is already active or no environment is
     * available, give up
     */
    if (interp->txnp != NULL || interp->envp == NULL)
        return typ;
    /*
     * Start transaction and retry.
     */
    RDB_init_exec_context(&ec);
    dbp = Duro_get_db(interp, &ec);
    RDB_destroy_exec_context(&ec);
    if (dbp == NULL) {
        return NULL;
    }

    if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
        return NULL;
    typ = RDB_expr_type(exp, &Duro_get_var_type, interp, interp->envp, ecp, &tx);
    if (typ != NULL) {
        RDB_commit(ecp, &tx);
        return typ;
    }
    return RDB_commit(ecp, &tx) == RDB_OK ? typ : NULL;
}
