/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"

int
Duro_insert_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    int attrcount;
    RDB_object tpl;
    TclState *statep = (TclState *) data;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "table tuple tx");
        return TCL_ERROR;
    }

    name = Tcl_GetStringFromObj(objv[1], NULL);

    txstr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    Tcl_ListObjLength(interp, objv[2], &attrcount);
    if (attrcount % 2 != 0) {
        Tcl_SetResult(interp, "Invalid tuple value", TCL_STATIC);
        return TCL_ERROR;
    }

    RDB_init_obj(&tpl);
    ret = Duro_tcl_to_duro(interp, objv[2], RDB_table_type(tbp)->var.basetyp,
            &tpl, txp);
    if (ret != TCL_OK) {
        goto cleanup;
    }
    ret = RDB_insert(tbp, &tpl, txp);
    if (ret == RDB_OK) {
        ret = TCL_OK;
    } else {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
    }

cleanup:
    RDB_destroy_obj(&tpl);

    return ret;
}
