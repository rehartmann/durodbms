/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <rel/internal.h>
#include <dli/parse.h>
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
        Tcl_WrongNumArgs(interp, 1, objv,
                "expression tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = RDB_parse_expr(Tcl_GetStringFromObj(objv[1], NULL),
            Duro_get_ltable, statep, txp, &exprp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    RDB_init_obj(&val);
    ret = RDB_evaluate(exprp, NULL, txp, &val);
    RDB_drop_expr(exprp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val);
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    tobjp = Duro_to_tcl(interp, &val);
    RDB_destroy_obj(&val);
    if (tobjp == NULL)
        return TCL_ERROR;

    Tcl_SetObjResult(interp, tobjp);
    return TCL_OK;
}
