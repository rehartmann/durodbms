/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <rel/internal.h>
#include <string.h>

static int
operator_create_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
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
                "name [-returns rtype | -updates updlist] arglist body txId");
        return TCL_ERROR;
    }
    if (strcmp (Tcl_GetString(objv[3]), "-updates") == 0) {
        update = RDB_TRUE;
    } else if (strcmp (Tcl_GetString(objv[3]), "-returns") == 0) {
        update = RDB_FALSE;
    } else {
        Tcl_SetResult(interp, "invalid option, must be -updates or -returns",
                TCL_STATIC);
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[7], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "unknown transaction: ", txstr, NULL);
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

        ret = Duro_get_type(typeobjp, interp, txp, &argtv[i]);
        if (ret != TCL_OK) {
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
                Tcl_AppendResult(interp, "invalid argument: ", argp, NULL);
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

        ret = Duro_get_type(objv[4], interp, txp, &rtyp);
        if (ret != TCL_OK) {
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
        Tcl_AppendResult(interp, "unknown transaction: ", txstr, NULL);
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
        Tcl_AppendResult(interp, "unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    argc = (objc - 3) / 2;
    argtv = (RDB_type **) Tcl_Alloc(sizeof (RDB_type *) * argc);

    for (i = 0; i < argc; i++) {
        ret = Duro_get_type(objv[3 + i * 2], interp, txp, &argtv[i]);
        if (ret != TCL_OK) {
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

        if (op->updv[i] && argtv[i]->kind != RDB_TP_RELATION) {
            /* It's a non-relation update argument - read variable */
            valobjp = Tcl_ObjGetVar2(interp, valobjp, NULL, 0);
            if (valobjp == NULL) {
                ret = TCL_ERROR;
                Tcl_AppendResult(interp, "can't read \"",
                        Tcl_GetString(objv[2 + i * 2]),
                        "\": no such variable", NULL);
                goto cleanup;
            }
        }

        argv[i] = (RDB_object *) Tcl_Alloc(sizeof (RDB_object));
        RDB_init_obj(argv[i]);
        if (op->updv[i] && argtv[i]->kind == RDB_TP_RELATION) {
            RDB_table *tbp;

            /* Updated relation argument - pass table */
            ret = Duro_get_table(statep, interp, Tcl_GetString(valobjp), txp,
                    &tbp);
            if (ret != TCL_OK) {
                RDB_destroy_obj(argv[i]);
                Tcl_Free((char *) argv[i]);
                argv[i] = 0;
                ret = TCL_ERROR;
                goto cleanup;
            }
            RDB_table_to_obj(argv[i], tbp);
        } else {
            ret = Duro_tcl_to_duro(interp, valobjp, argtv[i], argv[i], txp);
            if (ret != TCL_OK) {
                RDB_destroy_obj(argv[i]);
                Tcl_Free((char *) argv[i]);
                argv[i] = 0;
                ret = TCL_ERROR;
                goto cleanup;
            }
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

static Tcl_Obj *
find_txid(RDB_transaction *txp, TclState *statep)
{
    Tcl_HashSearch search;

    Tcl_HashEntry *entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    while (entryp != NULL) {
        if ((RDB_transaction *) Tcl_GetHashValue(entryp) == txp) {
            char *txid = Tcl_GetHashKey(&statep->txs, entryp);
            return Tcl_NewStringObj(txid, strlen(txid));
        }
        entryp = Tcl_NextHashEntry(&search);
    }
    return NULL;
}

int
Duro_invoke_update_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_transaction *txp)
{
    int ret;
    int i;
    Tcl_Obj *opdatap;
    Tcl_Obj *nametop;
    Tcl_Obj *namelistp;
    Tcl_Obj *bodytop;
    Tcl_Obj *txtop;
    Tcl_Obj *procargv[5];
    Tcl_Obj **opargv;
    Tcl_CmdInfo cmdinfo;
    RDB_environment *envp = RDB_db_env(RDB_tx_db(txp));
    Tcl_Interp *interp = txp->user_data;
    int issetter = argc == 2 && strstr(name, "_set_") != NULL;

    nametop = Tcl_NewStringObj(name, strlen(name));

    /* Get operator data (arg names + body) */
    opdatap = Tcl_NewStringObj((CONST char *) iargp, iarglen);

    /* Get argument name list */
    ret = Tcl_ListObjIndex(interp, opdatap, 0, &namelistp);
    if (ret != TCL_OK) {
        return RDB_INTERNAL;
    }

    /* Add tx argument */
    ret = Tcl_ListObjAppendElement(interp, namelistp, Tcl_NewStringObj("tx", 2));
    if (ret != TCL_OK)
        return RDB_INTERNAL;

    if (!Tcl_GetCommandInfo(interp, "duro::operator", &cmdinfo))
        return RDB_INTERNAL;

    /* Find tx id */
    txtop = find_txid(txp, (TclState *) cmdinfo.objClientData);
    if (txtop == NULL) {
        RDB_errmsg(envp, "transaction not found");
        return RDB_INTERNAL;
    }

    /* Get body */
    ret = Tcl_ListObjIndex(interp, opdatap, 1, &bodytop);
    if (ret != TCL_OK) {
        return RDB_INTERNAL;
    }

    /*
     * Create a command by executing the 'proc' command
     */
    procargv[0] = Tcl_NewStringObj("proc", 4);
    procargv[1] = nametop;
    procargv[2] = namelistp;
    procargv[3] = bodytop;
    ret = Tcl_EvalObjv(interp, 4, procargv, 0);
    if (ret != TCL_OK) {
        RDB_errmsg(envp, "%s", Tcl_GetStringResult(interp));
        return RDB_INTERNAL;
    }

    /*
     * Set arguments
     */

    opargv = (Tcl_Obj **) Tcl_Alloc(sizeof (Tcl_Obj *) * (argc + 2));

    opargv[0] = nametop;

    for (i = 0; i < argc; i++) {
        RDB_table *tbp = RDB_obj_table(argv[i]);

        if (tbp != NULL && updv[i]) {
            if (RDB_table_name(tbp) == NULL) {
                /* Only possible when it's a setter */

                ret = RDB_set_table_name(tbp, "duro_t", txp);                
                if (ret != RDB_OK) {
                    Tcl_Free((char *) opargv);
                    return ret;
                }
                ret = Duro_add_table(interp, (TclState *)cmdinfo.objClientData,
                    tbp, "duro_t", envp);
                if (ret != TCL_OK) {
                    Tcl_Free((char *) opargv);
                    return RDB_INTERNAL;
                }
            }
            opargv[i + 1] = Tcl_NewStringObj(RDB_table_name(tbp),
                    strlen(RDB_table_name(tbp)));
        } else {
            Tcl_Obj *argtop = issetter && i == 0 ? Duro_irep_to_tcl(
                    interp, argv[i], txp) : Duro_to_tcl(interp, argv[i], txp);

            if (updv[i]) {
                char buf[11];
                Tcl_Obj *varnamep;

                snprintf(buf, 11, "duro_%05d", i);
                varnamep = Tcl_NewStringObj(buf, 10);
                if (Tcl_ObjSetVar2(interp, varnamep, NULL, argtop, 0) == NULL) {
                    Tcl_Free((char *) opargv);
                    return RDB_INTERNAL;
                }
                opargv[i + 1] = varnamep;
            } else {
                opargv[i + 1] = argtop;
            }
        }
    }

    opargv[argc + 1] = txtop;

    /* Execute operator by invoking the Tcl procedure just created */
    ret = Tcl_EvalObjv(interp, argc + 2, opargv, 0);

    Tcl_Free((char *) opargv);
    
    switch (ret) {
        case TCL_ERROR:
            RDB_errmsg(envp, "%s", Tcl_GetStringResult(interp));
            return RDB_INVALID_ARGUMENT;
        case TCL_BREAK:
            RDB_errmsg(envp, "invoked \"break\" outside of a loop");
            return RDB_INVALID_ARGUMENT;
        case TCL_CONTINUE:
            RDB_errmsg(envp, "invoked \"continue\" outside of a loop");
            return RDB_INVALID_ARGUMENT;
    }

    /* Store updated values */
    for (i = 0; i < argc; i++) {
        if (updv[i] && RDB_obj_table(argv[i]) == NULL) {
            Tcl_Obj *valobjp;
            RDB_type *convtyp;
            RDB_type *argtyp = RDB_obj_type(argv[i]);
            char buf[11];
            Tcl_Obj *varnamep;

            snprintf(buf, 11, "duro_%05d", i);
            varnamep = Tcl_NewStringObj(buf, 10);

            valobjp = Tcl_ObjGetVar2(interp, varnamep, NULL, 0);

            if (argtyp->kind == RDB_TP_SCALAR) {
                if (issetter && i == 0) {
                    /* It´s the argument of setter, so use internal rep */
                    convtyp = argtyp->var.scalar.arep;
                } else {
                    convtyp = argtyp;
                }
            } else {
                convtyp = argtyp;
            }

            ret = Duro_tcl_to_duro(interp, valobjp, convtyp, argv[i], txp);
            argv[i]->typ = argtyp;

            Tcl_UnsetVar(interp, Tcl_GetString(varnamep), 0);

            if (ret != TCL_OK) {
                return RDB_INTERNAL;
            }
        }
    }

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
    Tcl_Obj *nametop;
    Tcl_Obj *namelistp;
    Tcl_Obj *bodytop;
    Tcl_Obj *procargv[4];
    Tcl_Obj **opargv;
    RDB_type *convtyp;
    RDB_type *rtyp = RDB_obj_type(retvalp);
    int isgetter = argc == 1 && strstr(name, "_get_") != NULL;
    RDB_environment *envp = RDB_db_env(RDB_tx_db(txp));
    Tcl_Interp *interp = txp->user_data;

    nametop = Tcl_NewStringObj(name, strlen(name));

    /* Get operator data (arg names + body) */
    opdatap = Tcl_NewStringObj((CONST char *) iargp, iarglen);

    /* Get argument name list */
    ret = Tcl_ListObjIndex(interp, opdatap, 0, &namelistp);
    if (ret != TCL_OK) {
        return RDB_INTERNAL;
    }

    /* Get body */
    ret = Tcl_ListObjIndex(interp, opdatap, 1, &bodytop);
    if (ret != TCL_OK) {
        return RDB_INTERNAL;
    }

    /*
     * Create a command by executing the 'proc' comand
     */
    procargv[0] = Tcl_NewStringObj("proc", 4);
    procargv[1] = nametop;
    procargv[2] = namelistp;
    procargv[3] = bodytop;
    ret = Tcl_EvalObjv(interp, 4, procargv, 0);
    if (ret != TCL_OK) {
        RDB_errmsg(envp, "%s", Tcl_GetStringResult(interp));
        return RDB_INTERNAL;
    }

    /*
     * Set arguments
     */

    opargv = (Tcl_Obj **) Tcl_Alloc(sizeof (Tcl_Obj *) * argc + 1);

    opargv[0] = nametop;

    for (i = 0; i < argc; i++) {
        /* If the operator is a getter, pass actual rep */
        opargv[i + 1] = isgetter && i == 0 ?
                Duro_irep_to_tcl(interp, argv[i], txp)
                : Duro_to_tcl(interp, argv[i], txp);
    }

    /* Execute operator by invoking the Tcl procedure just created */
    ret = Tcl_EvalObjv(interp, argc + 1, opargv, 0);

    Tcl_Free((char *) opargv);
    
    switch (ret) {
        case TCL_ERROR:
            RDB_errmsg(envp, "%s", Tcl_GetStringResult(interp));
            return RDB_INVALID_ARGUMENT;
        case TCL_BREAK:
            RDB_errmsg(envp, "invoked \"break\" outside of a loop");
            return RDB_INVALID_ARGUMENT;
        case TCL_CONTINUE:
            RDB_errmsg(envp, "invoked \"continue\" outside of a loop");
            return RDB_INVALID_ARGUMENT;
    }

    /*
     * Convert result
     */
    if (rtyp->kind == RDB_TP_SCALAR) {
        if(_RDB_get_possrep(retvalp->typ, name) != NULL) {
            /* It's a selector, so use internal rep */
            convtyp = rtyp->var.scalar.arep;
        } else {
            convtyp = rtyp;
        }
    } else {
        convtyp = rtyp;
    }
    ret = Duro_tcl_to_duro(interp, Tcl_GetObjResult(interp),
            convtyp, retvalp, txp);
    if (rtyp->kind != RDB_TP_RELATION)
        retvalp->typ = rtyp;
    return ret == TCL_OK ? RDB_OK : RDB_INVALID_ARGUMENT;
}
