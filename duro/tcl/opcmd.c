/* $Id$ */

#include "duro.h"
#include <string.h>

int
Duro_operator_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    RDB_bool update;
    int ret;
    int i;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    int argc;
    RDB_type **argtv;
    char *txtp;
    int len;
    TclState *statep = (TclState *) data;

    if (objc != 7) {
        Tcl_WrongNumArgs(interp, 1, objv,
                "name [-returns rtype | -updates updlist] tx args body");
        return TCL_ERROR;
    }
    if (strcmp (Tcl_GetStringFromObj(objv[2], NULL), "-updates") == 0)
        update = RDB_TRUE;
    if (strcmp (Tcl_GetStringFromObj(objv[2], NULL), "-returns") == 0)
        update = RDB_FALSE;
    else
        Tcl_SetResult(interp, "invalid option, must be -updates or -returns",
                TCL_STATIC);

    txstr = Tcl_GetStringFromObj(objv[4], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    Tcl_ListObjLength(interp, objv[5], &argc);
    argtv = (RDB_type **) Tcl_Alloc(sizeof(RDB_type *) * argc);
    for (i = 0; i < argc; i++) {
        Tcl_Obj *argtobjp;
        Tcl_Obj *typeobjp;

        Tcl_ListObjIndex(interp, objv[5], i, &argtobjp);
        ret = Tcl_ListObjIndex(interp, argtobjp, 1, &typeobjp);
        if (ret != TCL_OK) {
            Tcl_Free((char *) argtv);
            return ret;
        }
        if (typeobjp == NULL) {
            Tcl_SetResult(interp, "Type missing", TCL_STATIC);
            return TCL_ERROR;
        }

        ret = RDB_get_type(Tcl_GetStringFromObj(typeobjp, NULL), txp,
                &argtv[i]);
        if (ret != RDB_OK) {
            Tcl_Free((char *) argtv);
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
    }

    txtp = Tcl_GetStringFromObj(objv[6], &len);

    if (update) {
        ret = RDB_create_update_op(Tcl_GetStringFromObj(objv[1], NULL),
                argc, argtv, NULL, "libdurotcl", "Duro_invoke_update_op",
                txtp, (size_t) len, txp);
    }
    Tcl_Free((char *) argtv);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

int
Duro_call_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int ret;
    int i;
    int argc;
    RDB_object **argv;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (objc < 3) {
        Tcl_WrongNumArgs(interp, 1, objv,
                "name tx arg ?arg ...?");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    argc = objc - 3;
    argv = (RDB_object **) Tcl_Alloc(sizeof (RDB_object *) * argc);

    for (i = 0; i < argc; i++) {
        argv[i] = (RDB_object *) Tcl_Alloc(sizeof (RDB_object));
        /* ... */
    }

    ret = RDB_call_update_op(Tcl_GetStringFromObj(objv[1], NULL),
            argc, argv, txp);
    for (i = 0; i < argc; i++) {
        RDB_destroy_obj(argv[i]);
        Tcl_Free((char *) argv[i]);
    }
    Tcl_Free((char *) argv);
    
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

int
Duro_invoke_update_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_transaction *txp)
{
    /* ... */

    Tcl_EvalEx((Tcl_Interp *)RDB_db_env(RDB_tx_db(txp))->user_data,
            (CONST char *)iargp, iarglen, TCL_EVAL_GLOBAL);

    return RDB_OK;
}
