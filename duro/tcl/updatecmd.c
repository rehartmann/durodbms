/*
 * Copyright (C) 2004 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <dli/parse.h>

int
Duro_update_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    int i;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    int updc;
    RDB_attr_update *updv;
    RDB_expression *wherep;
    int upd_arg_idx;
    TclState *statep = (TclState *) data;

    if (objc < 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "tablename ?where?"
                " attrname expression ?attrname expression ...? txId");
        return TCL_ERROR;
    }

    name = Tcl_GetStringFromObj(objv[1], NULL);

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
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

    if (objc % 2 == 0) {
        /* Read where conditon */
        ret = RDB_parse_expr(Tcl_GetStringFromObj(objv[2], NULL),
                Duro_get_ltable, statep, txp, &wherep);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
        upd_arg_idx = 3;
    } else {
        wherep = NULL;
        upd_arg_idx = 2;
    }

    updc = (objc - 3) / 2;
    updv = (RDB_attr_update *) Tcl_Alloc(updc * sizeof (RDB_attr_update));
    for (i = 0; i < updc; i++)
        updv[i].exp = NULL;
    for (i = 0; i < updc; i++) {
        updv[i].name = Tcl_GetStringFromObj(objv[upd_arg_idx + i * 2], NULL);
        ret = RDB_parse_expr(
                Tcl_GetStringFromObj(objv[upd_arg_idx + i * 2 + 1], NULL),
                &Duro_get_ltable, statep, txp, &updv[i].exp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            ret = TCL_ERROR;
            goto cleanup;
        }
    }
    ret = RDB_update(tbp, wherep, updc, updv, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
        goto cleanup;
    }

    ret = TCL_OK;

cleanup:
    for (i = 0; i < updc; i++) {
        if (updv[i].exp != NULL)
            RDB_drop_expr(updv[i].exp);
    }
    Tcl_Free((char *) updv);
    return ret;                    
}

