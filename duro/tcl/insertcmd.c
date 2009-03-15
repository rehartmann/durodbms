/*
 * $Id$
 *
 * Copyright (C) 2003-2009 René Hartmann.
 * See the file COPYING for redistribution information.
 */

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
    RDB_object *tbp;
    RDB_object tpl;
    TclState *statep = (TclState *) data;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "table tuple tx");
        return TCL_ERROR;
    }

    RDB_clear_err(statep->current_ecp);

    name = Tcl_GetString(objv[1]);

    txstr = Tcl_GetString(objv[3]);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    tbp = Duro_get_table(statep, interp, name, txp);
    if (tbp == NULL) {
        return TCL_ERROR;
    }

    RDB_init_obj(&tpl);
    ret = Duro_tcl_to_duro(interp, objv[2], RDB_base_type(RDB_obj_type(tbp)),
            &tpl, statep->current_ecp, txp);
    if (ret != TCL_OK) {
        goto cleanup;
    }
    ret = RDB_insert(tbp, &tpl, statep->current_ecp, txp);
    if (ret == RDB_OK) {
        ret = TCL_OK;
    } else {
        /*
         * Must reset result, because Duro_tcl_to_duro may have invoked a script
         */
        Tcl_ResetResult(interp);

        Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
        ret = TCL_ERROR;
    }

cleanup:
    RDB_destroy_obj(&tpl, statep->current_ecp);

    return ret;
}
