/*
 * Function for getting the action operator.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "getaction.h"

RDB_operator *
Dr_get_action_op(Duro_interp *interpp, RDB_exec_context *ecp)
{
    static RDB_expression *query_exp = NULL;

    RDB_object tpl;
    RDB_type *typev[2];
    RDB_operator *op;

    /* Get action operator */

    RDB_init_obj(&tpl);

    if (Duro_dt_execute_str("begin tx;", interpp, ecp) != RDB_OK)
        goto error;

    if (query_exp == NULL) {
        query_exp = Duro_dt_parse_expr_str("tuple from net_actions "
                "where path like net.get_request_header('PATH_INFO') "
                "and method = net.get_request_header('REQUEST_METHOD')",
                interpp, ecp);
        if (query_exp == NULL)
            goto error;
    }

    if (Duro_evaluate(query_exp, interpp, ecp, &tpl) != RDB_OK)
        goto error;

    typev[0] = NULL;
    typev[1] = &RDB_STRING;

    op = RDB_get_update_op(RDB_obj_string(RDB_tuple_get(&tpl, "opname")),
            2, typev, NULL, ecp, &interpp->txnp->tx);
    if (op == NULL)
        goto error;

    if (Duro_dt_execute_str("commit;", interpp, ecp) != RDB_OK)
        return NULL;

    RDB_destroy_obj(&tpl, ecp);
    return op;

error:
    if (Duro_dt_tx(interpp) != NULL
            && RDB_tx_is_running(Duro_dt_tx(interpp))) {
        /* Preserve *ecp */
        RDB_exec_context ec;
        RDB_init_exec_context(&ec);

        Duro_dt_execute_str("commit;", interpp, &ec);

        RDB_destroy_exec_context(&ec);
    }

    RDB_destroy_obj(&tpl, ecp);
    return NULL;
}

