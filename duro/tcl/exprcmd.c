/*
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <rel/internal.h>
#include <string.h>

int
Duro_expr_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int ret;
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

    txstr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_parse_expr_utf(interp, Tcl_GetString(objv[1]), statep, txp,
            &exprp);
    if (ret != TCL_OK) {
        return ret;
    }

    RDB_init_obj(&val);
    txp->user_data = interp;
    ret = RDB_evaluate(exprp, NULL, txp, &val);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val);
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }
    RDB_drop_expr(exprp);

    tobjp = Duro_to_tcl(interp, &val, txp);
    RDB_destroy_obj(&val);
    if (tobjp == NULL)
        return TCL_ERROR;

    Tcl_SetObjResult(interp, tobjp);
    return TCL_OK;
}
