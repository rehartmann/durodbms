/*
 * $Id$
 *
 * Copyright (C) 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"
#include <rel/internal.h>
#include <dli/parse.h>
#include <dli/tabletostr.h>
#include <gen/strfns.h>
#include <string.h>
#include <ctype.h>

static int
constraint_create_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_expression *exp;

    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "constraintName expression tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[4], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_parse_expr_utf(interp, Tcl_GetString(objv[3]), statep, txp,
            &exp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    ret = RDB_create_constraint(Tcl_GetString(objv[2]), exp, txp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exp);
        Duro_dberror(interp, txp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

static int
constraint_drop_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "constraintName tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = RDB_drop_constraint(Tcl_GetString(objv[2]), txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, txp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

int
Duro_constraint_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    TclState *statep = (TclState *) data;

    const char *sub_cmds[] = {
        "create", "drop", NULL
    };
    enum table_ix {
        create_ix, drop_ix
    };
    int index;

    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "arg ?arg ...?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj(interp, objv[1], sub_cmds, "option", 0, &index)
            != TCL_OK) {
        return TCL_ERROR;
    }

    switch (index) {
        case create_ix:
            return constraint_create_cmd(statep, interp, objc, objv);
        case drop_ix:
            return constraint_drop_cmd(statep, interp, objc, objv);
    }
    return TCL_ERROR;
}
