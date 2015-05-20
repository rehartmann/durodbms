/*
 * Interpreter evaluation functions.
 *
 * Copyright (C) 2014-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>
#include "iinterp.h"
#include "interp_core.h"

/**
 * Evaluate expression in the context of the interpreter *<var>interp</var>.
 */
int
Duro_evaluate(RDB_expression *exp, Duro_interp *interp,
        RDB_exec_context *ecp, RDB_object *resultp)
{
    return RDB_evaluate(exp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL, resultp);
}

RDB_type *
Duro_expr_type(RDB_expression *exp, Duro_interp *interp, RDB_exec_context *ecp)
{
    return RDB_expr_type(exp, &Duro_get_var_type, interp,
            interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
}
