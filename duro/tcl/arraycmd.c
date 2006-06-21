/*
 * $Id$
 *
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"
#include <string.h>

int
Duro_tcl_drop_array(RDB_object *arrayp, Tcl_HashEntry *entryp,
        RDB_exec_context *ecp)
{
    int ret;

    Tcl_DeleteHashEntry(entryp);
    ret = RDB_destroy_obj(arrayp, ecp);
    Tcl_Free((char *) arrayp);
    return ret;
}

static int
array_create_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *tbname;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_object *tbp;
    RDB_object *arrayp;
    int new;
    char handle[20];
    RDB_bool ordered;
    RDB_seq_item *seqitv = NULL;
    int seqitc = 0;

    if (objc < 4 || objc > 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "table ?{ ?attr dir ...? }? tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    tbname = Tcl_GetStringFromObj(objv[2], NULL);
    tbp = Duro_get_table(statep, interp, tbname, txp);
    if (tbp == NULL) {
        return TCL_ERROR;
    }

    if (objc == 5) {
        seqitv = Duro_tobj_to_seq_items(interp, objv[3], &seqitc, RDB_TRUE,
                &ordered);
        if (seqitv == NULL)
            return TCL_ERROR;
    }

    arrayp = (RDB_object *) Tcl_Alloc(sizeof (RDB_object));
    RDB_init_obj(arrayp);

    ret = RDB_table_to_array(arrayp, tbp, seqitc, seqitv, statep->current_ecp,
            txp);
    if (seqitv != NULL)
        Tcl_Free((char *) seqitv);
    if (ret != RDB_OK) {
        Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
        return TCL_ERROR;
    }

    statep->array_uid++;
    sprintf(handle, "a%d", statep->array_uid);
    entryp = Tcl_CreateHashEntry(&statep->arrays, handle, &new);
    Tcl_SetHashValue(entryp, (ClientData)arrayp);

    Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);
    return TCL_OK;
}

static int
array_drop_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    Tcl_HashEntry *entryp;
    RDB_object *arrayp;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 2, objv, "arrayname");
        return TCL_ERROR;
    }

    arraystr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);
    ret = Duro_tcl_drop_array(arrayp, entryp, statep->current_ecp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, RDB_get_err(statep->current_ecp), NULL);
        return TCL_ERROR;
    }      

    return TCL_OK;
}

static int
array_index_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_object *arrayp;
    RDB_object *tplp;
    int idx;
    Tcl_Obj *listobjp;

    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "arrayname idx tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    arraystr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    ret = Tcl_GetIntFromObj(interp, objv[3], &idx);
    if (ret != TCL_OK)
        return ret;

    tplp = RDB_array_get(arrayp, (RDB_int) idx, statep->current_ecp);
    if (tplp == NULL) {
        Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
        return TCL_ERROR;
    }

    listobjp = Duro_to_tcl(interp, tplp, statep->current_ecp, txp);
    if (listobjp == NULL)
        return TCL_ERROR;

    Tcl_SetObjResult(interp, listobjp);
    return TCL_OK;
}

static int
array_foreach_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_object *arrayp;
    RDB_object *tplp;
    int i;
    int len;
    Tcl_Obj *elemobjp;

    if (objc != 6) {
        Tcl_WrongNumArgs(interp, 2, objv, "varname arrayname body tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[5], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    /* Get duro array */
    arraystr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    len = RDB_array_length(arrayp, statep->current_ecp);
    if (len == RDB_ERROR) {
        Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
        return TCL_ERROR;
    }

    for (i = 0; i < len; i++) {
        tplp = RDB_array_get(arrayp, i, statep->current_ecp);
        if (tplp == NULL) {
            Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
            return TCL_ERROR;
        }

        /* Set variable */
        elemobjp = Duro_to_tcl(interp, tplp, statep->current_ecp, txp);
        if (elemobjp == NULL)
            return TCL_ERROR;
        
        Tcl_ObjSetVar2(interp, objv[2], NULL, elemobjp, 0);

        /* Invoke script */
        ret = Tcl_EvalObjEx(interp, objv[4], 0);
        if (ret != TCL_OK)
            return ret;
    }
        
    return TCL_OK;
}

static int
array_length_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    char *arraystr;
    RDB_object *arrayp;
    RDB_int len;
    Tcl_HashEntry *entryp;
    Tcl_Obj *robjp;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 2, objv, "array");
        return TCL_ERROR;
    }

    arraystr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    len = RDB_array_length(arrayp, statep->current_ecp);
    if (len < 0) {
        Duro_dberror(interp, RDB_get_err(statep->current_ecp), NULL);
        return TCL_ERROR;
    }

    robjp = Tcl_NewIntObj((int) len);
    Tcl_SetObjResult(interp, robjp);

    return TCL_OK;
}

static int
array_set_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_object *arrayp;
    int idx;
    RDB_type *typ;
    RDB_object obj;

    if (objc != 7) {
        Tcl_WrongNumArgs(interp, 2, objv, "arrayname idx value type tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    arraystr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    ret = Tcl_GetIntFromObj(interp, objv[3], &idx);
    if (ret != TCL_OK)
        return ret;

    typ = Duro_get_type(objv[5], interp, statep->current_ecp, txp);
    if (typ == NULL)
        return TCL_ERROR;

    RDB_init_obj(&obj);
    ret = Duro_tcl_to_duro(interp, objv[4], typ, &obj, statep->current_ecp,
            txp);
    if (ret != TCL_OK) {
        RDB_destroy_obj(&obj, statep->current_ecp);
        return ret;
    }

    ret = RDB_array_set(arrayp, (RDB_int) idx, &obj, statep->current_ecp);
    RDB_destroy_obj(&obj, statep->current_ecp);
    if (ret != RDB_OK) {
        /*
         * Must Rest Result, because Duro_tcl_to_duro may have invoked a script
         */
        Tcl_ResetResult(interp);

        Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
        return TCL_ERROR;
    }

    return TCL_OK;
}

int
Duro_array_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    TclState *statep = (TclState *) data;

    const char *sub_cmds[] = {
        "create", "drop", "length", "index", "foreach", "set", NULL
    };
    enum array_ix {
        create_ix, drop_ix, length_ix, index_ix, foreach_ix, set_ix
    };
    int index;

    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "option ?arg ...?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj(interp, objv[1], sub_cmds, "option", 0, &index)
            != RDB_OK) {
        return TCL_ERROR;
    }

    RDB_clear_err(statep->current_ecp);

    switch (index) {
        case create_ix:
            return array_create_cmd(statep, interp, objc, objv);
        case drop_ix:
            return array_drop_cmd(statep, interp, objc, objv);
        case length_ix:
            return array_length_cmd(statep, interp, objc, objv);
        case index_ix:
            return array_index_cmd(statep, interp, objc, objv);
        case foreach_ix:
            return array_foreach_cmd(statep, interp, objc, objv);
        case set_ix:
            return array_set_cmd(statep, interp, objc, objv);
    }
    return TCL_ERROR;
}
