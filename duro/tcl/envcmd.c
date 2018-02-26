/*
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"
#include <string.h>
#include <rec/env.h>

int
Duro_tcl_close_env(TclState *statep, RDB_environment *envp, Tcl_HashEntry *entryp)
{
    int ret;
    Tcl_HashSearch search;
    table_entry *tbep;
    FILE *errfp;
    RDB_transaction *txp;

    Tcl_DeleteHashEntry(entryp);

    /*
     * Delete local tables which belong to the environment
     */
    entryp = Tcl_FirstHashEntry(&statep->ltables, &search);
    while (entryp != NULL) {
        tbep = Tcl_GetHashValue(entryp);
        if (tbep->envp == envp) {
            /* Drop table, delete entry and start from the beginning */
            Duro_tcl_drop_ltable(tbep, entryp, statep->current_ecp);
            entryp = Tcl_FirstHashEntry(&statep->ltables, &search);
        } else {
            entryp = Tcl_NextHashEntry(&search);
        }
    }

    /*
     * Abort pending transactions
     */
    entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    while (entryp != NULL) {
        txp = Tcl_GetHashValue(entryp);

        /* Abort transaction if the environment of its database is *envp */
        if (RDB_db_env(RDB_tx_db(txp)) == envp) {
            Duro_tcl_rollback(entryp, statep->current_ecp, txp);
            entryp = Tcl_FirstHashEntry(&statep->txs, &search);
        } else {
            entryp = Tcl_NextHashEntry(&search);
        }
    }

    errfp = RDB_env_get_errfile(envp);
    ret = RDB_close_env(envp, &statep->ec);
    if (errfp != NULL) {
        fclose(errfp);
    }
    return ret;
}

int
Duro_env_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    int ret;
    RDB_environment *envp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;
    int new;
    char handle[20];

    if (argc < 2) {
        Tcl_SetResult(interp, "wrong # args: should be \"env option ?arg ...?\"",
                TCL_STATIC);
        return TCL_ERROR;
    }

    RDB_clear_err(statep->current_ecp);

    if (strcmp(argv[1], "open") == 0) {
        int flags;
        const char *path;
    
        if (argc < 3 || argc > 4) {
            Tcl_SetResult(interp, "wrong # args: should be \"env open [-recover] path\"",
                    TCL_STATIC);
            return TCL_ERROR;
        }

        if (strcmp(argv[2], "-recover") == 0) {
            flags = RDB_RECOVER;
            path = argv[3];
        } else {
            flags = 0;
            path = argv[2];
        }

        envp = RDB_open_env(path, flags, &statep->ec);
        if (envp == NULL) {
            Duro_dberror(interp, RDB_get_err(&statep->ec), NULL);
            return TCL_ERROR;
        }

        statep->env_uid++;
        sprintf(handle, "env%d", statep->env_uid);
        entryp = Tcl_CreateHashEntry(&statep->envs, handle, &new);
        Tcl_SetHashValue(entryp, (ClientData)envp);

        Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);
        return TCL_OK;
    }
    if (strcmp(argv[1], "create") == 0) {
        if (argc != 3) {
            Tcl_SetResult(interp, "wrong # args: should be \"env create path\"",
                    TCL_STATIC);
            return TCL_ERROR;
        }

        envp = RDB_create_env(argv[2], &statep->ec);
        if (envp == NULL) {
            Duro_dberror(interp, RDB_get_err(&statep->ec), NULL);
            return TCL_ERROR;
        }

        statep->env_uid++;
        sprintf(handle, "env%d", statep->env_uid);
        entryp = Tcl_CreateHashEntry(&statep->envs, handle, &new);
        Tcl_SetHashValue(entryp, (ClientData)envp);

        Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);
        return TCL_OK;
    }
    if (strcmp(argv[1], "close") == 0) {
        if (argc != 3) {
            Tcl_SetResult(interp, "wrong # args: should be \"env close envId\"",
                    TCL_STATIC);
            return TCL_ERROR;
        }

        entryp = Tcl_FindHashEntry(&statep->envs, argv[2]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown environment: ", argv[2], NULL);
            return TCL_ERROR;
        }
        envp = Tcl_GetHashValue(entryp);
        ret = Duro_tcl_close_env(statep, envp, entryp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, RDB_get_err(statep->current_ecp), NULL);
            return TCL_ERROR;
        }      
        return TCL_OK;
    }
    if (strcmp(argv[1], "dbs") == 0) {
        int i;
        Tcl_Obj *dblistp;
        RDB_object arr;
        RDB_object *dbnamep;
        RDB_object *errp;

        if (argc != 3) {
            Tcl_SetResult(interp, "wrong # args: should be \"env dbs envId\"",
                    TCL_STATIC);
            return TCL_ERROR;
        }

        entryp = Tcl_FindHashEntry(&statep->envs, argv[2]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown environment: ", argv[2], NULL);
            return TCL_ERROR;
        }
        envp = Tcl_GetHashValue(entryp);

        RDB_init_obj(&arr);
        ret = RDB_get_dbs(envp, &arr, statep->current_ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&arr, statep->current_ecp);
            Duro_dberror(interp, RDB_get_err(statep->current_ecp), NULL);
            return TCL_ERROR;
        }      

        dblistp = Tcl_NewListObj(0, NULL);
        if (dblistp == NULL)
            return TCL_ERROR;

        i = 0;
        while ((dbnamep = RDB_array_get(&arr, (RDB_int) i++,
                statep->current_ecp)) != NULL) {
            char *dbname = RDB_obj_string(dbnamep);

            ret = Tcl_ListObjAppendElement(interp, dblistp,
                    Tcl_NewStringObj(dbname, -1));
            if (ret != TCL_OK)
                return ret;
        }
        errp = RDB_get_err(statep->current_ecp);
        if (RDB_obj_type(errp) != &RDB_NOT_FOUND_ERROR) {
            Duro_dberror(interp, errp, NULL);
            return TCL_ERROR;
        }
        RDB_clear_err(statep->current_ecp);

        Tcl_SetObjResult(interp, dblistp);

        return TCL_OK;
    }
    if (strcmp(argv[1], "seterrfile") == 0) {
        FILE *fp;

        if (argc != 4) {
            Tcl_SetResult(interp,
                    "wrong # args: should be \"env errfile envId errfile\"",
                    TCL_STATIC);
            return TCL_ERROR;
        }

        entryp = Tcl_FindHashEntry(&statep->envs, argv[2]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown environment: ", argv[2], NULL);
            return TCL_ERROR;
        }
        envp = Tcl_GetHashValue(entryp);

        fp = fopen(argv[3], "a");
        if (fp == NULL) {
            char *errstr = (char *) Tcl_ErrnoMsg(Tcl_GetErrno());
            if (errstr != NULL) {
                Tcl_SetResult(interp, errstr, TCL_STATIC);
                Tcl_PosixError(interp);
            }
            return TCL_ERROR;
        }
        RDB_env_set_errfile(envp, fp);
        return TCL_OK;
    }
    Tcl_AppendResult(interp, "Bad option: ", argv[1], NULL);
    return TCL_ERROR;
}
