/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"

static int
index_create_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_table *tbp;
    RDB_seq_item *idxattrv;
    int attrc;
    RDB_bool ordered;

    if (objc != 6) {
        Tcl_WrongNumArgs(interp, 2, objv, "indexname tablename attrs txId");
        return TCL_ERROR;
    }

    txstr = Tcl_GetString(objv[5]);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, Tcl_GetString(objv[3]), txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    idxattrv = Duro_tobj_to_seq_items(interp, objv[4], &attrc, RDB_FALSE, &ordered);
    if (idxattrv == NULL) {
        return TCL_ERROR;
    }

    ret = RDB_create_table_index(Tcl_GetString(objv[2]), tbp, attrc,
        idxattrv, ordered ? RDB_ORDERED : 0, txp);
    free(idxattrv);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }
    return TCL_OK;
}

static int
index_drop_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "indexname txId");
        return TCL_ERROR;
    }

    txstr = Tcl_GetString(objv[3]);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = RDB_drop_table_index(Tcl_GetString(objv[2]), txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

int
Duro_index_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
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
            return index_create_cmd(statep, interp, objc, objv);
        case drop_ix:
            return index_drop_cmd(statep, interp, objc, objv);
    }
    return TCL_ERROR;
}
