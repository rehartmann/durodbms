/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <rel/internal.h>
#include <string.h>

static int
operator_create_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
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
    Tcl_Obj *namelistp, *opdatap;
    TclState *statep = (TclState *) data;

    if (objc != 8) {
        Tcl_WrongNumArgs(interp, 2, objv,
                "name [-returns rtype | -updates updlist] "
                "arglist body txId");
        return TCL_ERROR;
    }
    if (strcmp (Tcl_GetStringFromObj(objv[3], NULL), "-updates") == 0)
        update = RDB_TRUE;
    if (strcmp (Tcl_GetStringFromObj(objv[3], NULL), "-returns") == 0)
        update = RDB_FALSE;
    else
        Tcl_SetResult(interp, "invalid option, must be -updates or -returns",
                TCL_STATIC);

    txstr = Tcl_GetStringFromObj(objv[7], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    namelistp = Tcl_NewListObj(0, NULL);

    Tcl_ListObjLength(interp, objv[5], &argc);
    if (argc % 2 > 0) {
        Tcl_SetResult(interp, "invalid argument list", TCL_STATIC);
        return TCL_ERROR;
    }
    argc /= 2;
        
    argtv = (RDB_type **) Tcl_Alloc(sizeof(RDB_type *) * argc);

    for (i = 0; i < argc; i++) {
        Tcl_Obj *typeobjp;
        Tcl_Obj *nameobjp;

        /*
         * Get argument and append it to list
         */
        ret = Tcl_ListObjIndex(interp, objv[5], i * 2, &nameobjp);
        if (ret != TCL_OK) {
            goto cleanup;
        }
        ret = Tcl_ListObjAppendElement(interp, namelistp, nameobjp);
        if (ret != TCL_OK)
            goto cleanup;

        ret = Tcl_ListObjIndex(interp, objv[5], i * 2 + 1, &typeobjp);
        if (ret != TCL_OK) {
            goto cleanup;
        }
        if (typeobjp == NULL) {
            Tcl_SetResult(interp, "Type missing", TCL_STATIC);
            ret = TCL_ERROR;
            goto cleanup;
        }

        ret = RDB_get_type(Tcl_GetStringFromObj(typeobjp, NULL), txp,
                &argtv[i]);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            ret = TCL_ERROR;
            goto cleanup;
        }
    }

    /*
     * Create list of argument names and op body. This is then stored
     * in the catalog.
     */
    opdatap = Tcl_NewListObj(1, &namelistp);
    ret = Tcl_ListObjAppendElement(interp, opdatap, objv[6]);
    if (ret != TCL_OK)
            goto cleanup;

    txtp = Tcl_GetStringFromObj(opdatap, &len);

    if (update) {
        int updlen;
        Tcl_Obj *argobjp;
        RDB_bool *updv = (RDB_bool *) Tcl_Alloc(argc);

        for (i = 0; i < argc; i++)
            updv[i] = RDB_FALSE;

        ret = Tcl_ListObjLength(interp, objv[4], &updlen);
        if (ret != TCL_OK) {
            Tcl_Free(updv);
            goto cleanup;
        }

        for (i = 0; i < updlen; i++) {
            int j;
            char *argp;

            ret = Tcl_ListObjIndex(interp, objv[4], i, &argobjp);
            if (ret != TCL_OK) {
                Tcl_Free(updv);
                goto cleanup;
            }
            argp = Tcl_GetString(argobjp);

            /*
             * Search arg name in list, set updv[j] to RDB_TRUE if found.
             */
            for (j = 0; j < argc; j++) {
                Tcl_Obj *updobjp;

                ret = Tcl_ListObjIndex(interp, namelistp, i, &updobjp);
                if (ret != TCL_OK) {
                    Tcl_Free(updv);
                    goto cleanup;
                }
                if (strcmp(Tcl_GetString(updobjp), argp) == 0) {
                    updv[j] = RDB_TRUE;
                    break;
                }
            }
            if (j >= argc) {
                Tcl_AppendResult(interp, "Invalid argument: ", argp, NULL);
                Tcl_Free(updv);
                ret = TCL_ERROR;
                goto cleanup;
            }
        }

        ret = RDB_create_update_op(Tcl_GetStringFromObj(objv[2], NULL),
                argc, argtv, updv, "libdurotcl", "Duro_invoke_update_op",
                txtp, (size_t) len, txp);
        Tcl_Free(updv);
    } else {
        RDB_type *rtyp;

        ret = RDB_get_type(Tcl_GetStringFromObj(objv[4], NULL), txp, &rtyp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            ret = TCL_ERROR;
            goto cleanup;
        }

        ret = RDB_create_ro_op(Tcl_GetStringFromObj(objv[2], NULL),
                 argc, argtv, rtyp, "libdurotcl", "Duro_invoke_ro_op",
                 txtp, (size_t) len, txp);
    }
    
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
        goto cleanup;
    }

    ret = TCL_OK;

cleanup:
    Tcl_Free((char *) argtv);
    return ret;
}

static int
operator_drop_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int ret;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "name txId");
        return TCL_ERROR;
    }

    txstr = Tcl_GetString(objv[3]);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = RDB_drop_op(Tcl_GetString(objv[2]), txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

int
Duro_operator_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
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
            != RDB_OK) {
        return TCL_ERROR;
    }

    switch (index) {
        case create_ix:
            return operator_create_cmd(statep, interp, objc, objv);
        case drop_ix:
            return operator_drop_cmd(statep, interp, objc, objv);
    }
    return TCL_ERROR;
}

int
Duro_call_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int ret;
    int i;
    int argc;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_upd_op *op;
    RDB_type **argtv = NULL;
    RDB_object **argv = NULL;
    TclState *statep = (TclState *) data;

    if (objc < 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "name ?arg argtype ...? txId");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    argc = (objc - 3) / 2;
    argtv = (RDB_type **) Tcl_Alloc(sizeof (RDB_type *) * argc);

    for (i = 0; i < argc; i++) {
        char *typename = Tcl_GetString(objv[3 + i * 2]);
        ret = RDB_get_type(typename, txp, &argtv[i]);
        if (ret != RDB_OK) {
            Tcl_AppendResult(interp, "Unknown type: ", typename, NULL);
            ret = TCL_ERROR;
            goto cleanup;
        }
    }

    ret = _RDB_get_upd_op(Tcl_GetString(objv[1]), argc, argtv, txp, &op);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
        goto cleanup;
    }

    argv = (RDB_object **) Tcl_Alloc(sizeof (RDB_object *) * argc);
    for (i = 0; i < argc; i++)
        argv[i] = NULL;

    for (i = 0; i < argc; i++) {
        Tcl_Obj *valobjp = objv[2 + i * 2];

        if (op->updv[i]) {
            /* It's an update argument - read variable */
            valobjp = Tcl_ObjGetVar2(interp, valobjp, NULL, 0);
        }

        argv[i] = (RDB_object *) Tcl_Alloc(sizeof (RDB_object));
        RDB_init_obj(argv[i]);
        ret = Duro_tcl_to_duro(interp, valobjp, argtv[i], argv[i], txp);
        if (ret != TCL_OK) {
            RDB_destroy_obj(argv[i]);
            Tcl_Free((char *) argv[i]);
            argv[i] = 0;
            ret = TCL_ERROR;
            goto cleanup;
        }
    }

    txp->user_data = interp;
    ret = RDB_call_update_op(Tcl_GetStringFromObj(objv[1], NULL),
            argc, argv, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    /* Store updated values */
    for (i = 0; i < argc; i++) {
        if (op->updv[i]) {
            Tcl_Obj *valobjp = Duro_to_tcl(interp, argv[i], txp);

            if (valobjp == NULL) {
                ret = TCL_ERROR;
                goto cleanup;
            }
            Tcl_ObjSetVar2(interp, objv[2 + i * 2], NULL, valobjp, 0);
        }
    }   

    ret = TCL_OK;

cleanup:
    if (argv != NULL) {
        for (i = 0; i < argc; i++) {
            if (argv[i] != NULL) {
                RDB_destroy_obj(argv[i]);
                Tcl_Free((char *) argv[i]);
            }
        }
        Tcl_Free((char *) argv);
    }
    Tcl_Free((char *) argtv);

    return ret;
}

int
Duro_invoke_update_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_transaction *txp)
{
    int ret;
    int i;
    Tcl_Obj *opdatap;
    Tcl_Obj *namelistp;
    Tcl_Obj *bodyp;
    Tcl_Interp *slinterp;
    RDB_environment *envp = RDB_db_env(RDB_tx_db(txp));
    Tcl_Interp *interp = txp->user_data;

    /* Get operator data (arg names + body) */
    opdatap = Tcl_NewStringObj((CONST char *) iargp, iarglen);

    /*
     * Evaluate operator script in a slave interpreter,
     * so it runs within its own scope
     */

    slinterp = Tcl_CreateSlave(interp, "duro::slinterp", 0);

    /*
     * Get argument names and set arguments
     */

    ret = Tcl_ListObjIndex(slinterp, opdatap, 0, &namelistp);
    if (ret != TCL_OK) {
        Tcl_DeleteInterp(slinterp);
        return RDB_INTERNAL;
    }

    for (i = 0; i < argc; i++) {
        Tcl_Obj *argnamep;

        ret = Tcl_ListObjIndex(slinterp, namelistp, i, &argnamep);
        if (ret != TCL_OK) {
            Tcl_DeleteInterp(slinterp);
            return RDB_INTERNAL;
        }

        if (Tcl_ObjSetVar2(slinterp, argnamep, NULL,
                Duro_to_tcl(slinterp, argv[i], txp), 0) == NULL) {
            Tcl_DeleteInterp(slinterp);
            return RDB_INTERNAL;
        }
        if (ret != TCL_OK) {
            Tcl_DeleteInterp(slinterp);
            return RDB_INTERNAL;
        }
    }

    /* Get body */
    ret = Tcl_ListObjIndex(slinterp, opdatap, 1, &bodyp);
    if (ret != TCL_OK) {
        Tcl_DeleteInterp(slinterp);
        return RDB_INTERNAL;
    }

    /* Execute operator */
    ret = Tcl_EvalObjEx(slinterp, bodyp, 0);
    
    switch (ret) {
        case TCL_ERROR:
            RDB_errmsg(envp, "%s", Tcl_GetStringResult(slinterp));
            Tcl_DeleteInterp(slinterp);
            return RDB_INVALID_ARGUMENT;
        case TCL_BREAK:
            RDB_errmsg(envp, "invoked \"break\" outside of a loop");
            Tcl_DeleteInterp(slinterp);
            return RDB_INVALID_ARGUMENT;
        case TCL_CONTINUE:
            RDB_errmsg(envp, "invoked \"continue\" outside of a loop");
            Tcl_DeleteInterp(slinterp);
            return RDB_INVALID_ARGUMENT;
    }

    /* Store updated values */
    for (i = 0; i < argc; i++) {
        if (updv[i]) {
            Tcl_Obj *argnamep;
            Tcl_Obj *valobjp;

            ret = Tcl_ListObjIndex(slinterp, namelistp, i, &argnamep);
            if (ret != TCL_OK) {
                Tcl_DeleteInterp(slinterp);
                return RDB_INTERNAL;
            }

            valobjp = Tcl_ObjGetVar2(slinterp, argnamep, NULL, 0);

            ret = Duro_tcl_to_duro(slinterp, valobjp, RDB_obj_type(argv[i]),
                    argv[i], txp);
            if (ret != TCL_OK) {
                Tcl_DeleteInterp(slinterp);
                return RDB_INTERNAL;
            }
        }
    }

    Tcl_DeleteInterp(slinterp);
    return RDB_OK;
}

int
Duro_invoke_ro_op(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i;
    Tcl_Obj *opdatap;
    Tcl_Obj *namelistp;
    Tcl_Obj *bodyp;
    Tcl_Interp *slinterp;
    RDB_environment *envp = RDB_db_env(RDB_tx_db(txp));
    Tcl_Interp *interp = txp->user_data;

    /* Get operator data (arg names + body) */
    opdatap = Tcl_NewStringObj((CONST char *) iargp, iarglen);

    /*
     * Evaluate operator script in a slave interpreter,
     * so it runs within its own scope
     */

    slinterp = Tcl_CreateSlave(interp, "duro::slinterp", 0);

    /*
     * Get argument names and set arguments
     */

    ret = Tcl_ListObjIndex(slinterp, opdatap, 0, &namelistp);
    if (ret != TCL_OK) {
        RDB_errmsg(envp, "%s", Tcl_GetStringResult(slinterp));
        Tcl_DeleteInterp(slinterp);
        return RDB_INTERNAL;
    }

    for (i = 0; i < argc; i++) {
        Tcl_Obj *argnamep;

        ret = Tcl_ListObjIndex(slinterp, namelistp, i, &argnamep);
        if (ret != TCL_OK) {
            RDB_errmsg(envp, "%s", Tcl_GetStringResult(slinterp));
            Tcl_DeleteInterp(slinterp);
            return RDB_INTERNAL;
        }

        if (Tcl_ObjSetVar2(slinterp, argnamep, NULL,
                Duro_to_tcl(slinterp, argv[i], txp), 0) == NULL) {
            RDB_errmsg(envp, "%s", Tcl_GetStringResult(slinterp));
            Tcl_DeleteInterp(slinterp);
            return RDB_INTERNAL;
        }
        if (ret != TCL_OK) {
            RDB_errmsg(envp, "%s", Tcl_GetStringResult(slinterp));
            Tcl_DeleteInterp(slinterp);
            return RDB_INTERNAL;
        }
    }

    /* Get body */
    ret = Tcl_ListObjIndex(slinterp, opdatap, 1, &bodyp);
    if (ret != TCL_OK) {
        RDB_errmsg(envp, "%s", Tcl_GetStringResult(slinterp));
        Tcl_DeleteInterp(slinterp);
        return RDB_INTERNAL;
    }

    /* Execute operator */
    ret = Tcl_EvalObjEx(slinterp, bodyp, 0);
    switch (ret) {
        case TCL_ERROR:
            RDB_errmsg(envp, "%s", Tcl_GetStringResult(slinterp));
            Tcl_DeleteInterp(slinterp);
            return RDB_INVALID_ARGUMENT;
        case TCL_BREAK:
            RDB_errmsg(envp, "invoked \"break\" outside of a loop");
            Tcl_DeleteInterp(slinterp);
            return RDB_INVALID_ARGUMENT;
        case TCL_CONTINUE:
            RDB_errmsg(envp, "invoked \"continue\" outside of a loop");
            Tcl_DeleteInterp(slinterp);
            return RDB_INVALID_ARGUMENT;
    }

    /* Convert result */
    ret = Duro_tcl_to_duro(slinterp, Tcl_GetObjResult(slinterp),
            RDB_obj_type(retvalp), retvalp, txp);
    Tcl_DeleteInterp(slinterp);
    return ret;
}
