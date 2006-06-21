/*
 * $Id$
 *
 * Copyright (C) 2004-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"

int
Duro_update_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    RDB_int count;
    int ret;
    int i;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_object *tbp;
    int updc;
    RDB_attr_update *updv;
    RDB_expression *wherep;
    int upd_arg_idx;
    Tcl_Obj *restobjp;
    TclState *statep = (TclState *) data;

    if (objc < 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "tablename ?where?"
                " attrname expression ?attrname expression ...? txId");
        return TCL_ERROR;
    }

    RDB_clear_err(statep->current_ecp);

    name = Tcl_GetStringFromObj(objv[1], NULL);

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
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

    if (objc % 2 == 0) {
        /* Read where conditon */
        wherep = Duro_parse_expr_utf(interp, Tcl_GetString(objv[2]),
                    statep, statep->current_ecp, txp);
        if (wherep == NULL) {
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
        updv[i].exp = Duro_parse_expr_utf(interp,
                Tcl_GetString(objv[upd_arg_idx + i * 2 + 1]), statep,
                statep->current_ecp, txp);
        if (updv[i].exp == NULL) {
            ret = TCL_ERROR;
            goto cleanup;
        }
    }
    count = RDB_update(tbp, wherep, updc, updv, statep->current_ecp, txp);
    if (count == RDB_ERROR) {
        Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
        ret = TCL_ERROR;
        goto cleanup;
    }

    restobjp = Tcl_NewIntObj(count);
    if (restobjp == NULL) {
        ret = TCL_ERROR;
        goto cleanup;
    }

    Tcl_SetObjResult(interp, restobjp);
    ret = TCL_OK;

cleanup:
    for (i = 0; i < updc; i++) {
        if (updv[i].exp != NULL)
            RDB_drop_expr(updv[i].exp, statep->current_ecp);
    }
    Tcl_Free((char *) updv);
    return ret;                    
}

