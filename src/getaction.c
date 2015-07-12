/*
 * Function for getting the action operator.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "getaction.h"

RDB_operator *
Dr_get_action_op(Duro_interp *interpp, RDB_type *resptyp, RDB_exec_context *ecp)
{
    static RDB_expression *query_exp = NULL;

    RDB_object tpl;
    RDB_type *typev[3];
    RDB_operator *op;

    /* Get action operator */

    RDB_init_obj(&tpl);

    if (Duro_dt_execute_str("begin tx;", interpp, ecp) != RDB_OK)
        goto error;

    if (query_exp == NULL) {
        /* If the request method is HEAD get the GET entry */
        query_exp = Duro_dt_parse_expr_str("tuple from net_actions "
                "where path like http.get_request_header(dreisam_req, 'PATH_INFO') "
                "and with (request_method := the_method(dreisam_req)):"
                     "method = if request_method = 'HEAD' then 'GET' else request_method",
                interpp, ecp);
        if (query_exp == NULL)
            goto error;
    }

    if (Duro_evaluate(query_exp, interpp, ecp, &tpl) != RDB_OK)
        goto error;

    typev[0] = NULL;
    typev[1] = &RDB_STRING;
    typev[2] = NULL;
    typev[3] = NULL;

    op = RDB_get_update_op(RDB_obj_string(RDB_tuple_get(&tpl, "opname")),
            2, typev, NULL, ecp, &interpp->txnp->tx);
    if (op == NULL) {
        op = RDB_get_update_op(RDB_obj_string(RDB_tuple_get(&tpl, "opname")),
                3, typev, NULL, ecp, &interpp->txnp->tx);
        if (op == NULL) {
            op = RDB_get_update_op(RDB_obj_string(RDB_tuple_get(&tpl, "opname")),
                    4, typev, NULL, ecp, &interpp->txnp->tx);
            if (op == NULL)
                goto error;
        }
    }

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

