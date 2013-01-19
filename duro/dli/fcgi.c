/*
 * fcgi.c
 *
 *  Created on: 17.01.2013
 *      Author: Rene Hartmann
 */

#include <rel/rdb.h>
#include <rel/opmap.h>

#include <fcgi_stdio.h>

static int
op_fcgi_accept(int argc, RDB_object *argv[],
        RDB_operator *op, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int result = FCGI_Accept();

    RDB_bool_to_obj(argv[0], (RDB_bool) result >= 0);
    return RDB_OK;
}

static int
op_fcgi_set_exit_status(int argc, RDB_object *argv[],
        RDB_operator *op, RDB_exec_context *ecp, RDB_transaction *txp)
{
    FCGI_SetExitStatus(RDB_obj_int(argv[0]));
    return RDB_OK;
}

static int
op_fcgi_finish(int argc, RDB_object *argv[],
        RDB_operator *op, RDB_exec_context *ecp, RDB_transaction *txp)
{
    FCGI_Finish();
    return RDB_OK;
}

/*
 * Add the operators fcgi_accept(), fcgi_set_exit_status()
 * and fcgi_finish() which call their fcgi_stdio counterparts.
 */
int
RDB_add_fcgi_ops(RDB_op_map *opmapp, RDB_exec_context *ecp)
{
    static RDB_parameter fcgi_accept_params[1];
    static RDB_parameter fcgi_set_exit_status_params[1];

    fcgi_accept_params[0].typ = &RDB_BOOLEAN;
    fcgi_accept_params[0].update = RDB_TRUE;

    if (RDB_put_upd_op(opmapp, "fcgi_accept", 1, fcgi_accept_params,
            &op_fcgi_accept, ecp) != RDB_OK)
        return RDB_ERROR;

    fcgi_set_exit_status_params[0].typ = &RDB_INTEGER;
    fcgi_set_exit_status_params[0].update = RDB_FALSE;

    if (RDB_put_upd_op(opmapp, "fcgi_set_exit_status", 1,
            fcgi_set_exit_status_params,
            &op_fcgi_set_exit_status, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "fcgi_finish", 0, NULL,
            &op_fcgi_finish, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}
