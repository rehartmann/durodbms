/*
 * $Id$
 *
 * Copyright (C) 2003-2005 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"
#include <rel/internal.h>
#include <string.h>

int
Duro_expr_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_expression *exprp;
    RDB_object val;
    Tcl_Obj *tobjp;
    TclState *statep = (TclState *) data;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "expression tx");
        return TCL_ERROR;
    }

    RDB_clear_err(statep->current_ecp);

    txstr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    exprp = Duro_parse_expr_utf(interp, Tcl_GetString(objv[1]), statep,
            statep->current_ecp, txp);
    if (exprp == NULL) {
        return TCL_ERROR;
    }

    RDB_init_obj(&val);
    if (RDB_evaluate(exprp, Duro_get_ltable, statep, NULL, statep->current_ecp,
            txp, &val) != RDB_OK) {
        RDB_del_expr(exprp, statep->current_ecp);
        RDB_destroy_obj(&val, statep->current_ecp);
        Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
        return TCL_ERROR;
    }
    RDB_del_expr(exprp, statep->current_ecp);

    tobjp = Duro_to_tcl(interp, &val, statep->current_ecp, txp);
    RDB_destroy_obj(&val, statep->current_ecp);
    if (tobjp == NULL)
        return TCL_ERROR;

    Tcl_SetObjResult(interp, tobjp);
    return TCL_OK;
}
