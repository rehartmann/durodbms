/*
 * Copyright (C) 2003 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <string.h>

int
Duro_tcl_close_env(TclState *statep, RDB_environment *envp, Tcl_HashEntry *entryp)
{
    Tcl_HashSearch search;
    table_entry *tbep;

    Tcl_DeleteHashEntry(entryp);

    /* Delete local tables which belong to the environment */

    entryp = Tcl_FirstHashEntry(&statep->ltables, &search);
    while (entryp != NULL) {
        tbep = Tcl_GetHashValue(entryp);
        if (tbep->envp == envp) {
            /* Drop table, delete entry and start from the beginning */
            Duro_tcl_drop_ltable(tbep, entryp);
            entryp = Tcl_FirstHashEntry(&statep->ltables, &search);
        } else {
            entryp = Tcl_NextHashEntry(&search);
        }
    }

    return RDB_close_env(envp);
}

int
Duro_env_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    int ret;
    RDB_environment *envp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (argc < 2) {
        Tcl_SetResult(interp, "wrong # args: should be \"env option ?arg ...?\"",
                TCL_STATIC);
        return TCL_ERROR;
    }

    if (strcmp(argv[1], "open") == 0) {
        int new;
        char handle[20];
    
        if (argc != 3) {
            Tcl_SetResult(interp, "wrong # args: should be \"env open path\"",
                    TCL_STATIC);
            return TCL_ERROR;
        }

        ret = RDB_open_env(argv[2], &envp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }

        /* Store a pointer to the Tcl interpreter in the user_data field */
        /* envp->user_data = interp; */

        statep->env_uid++;
        sprintf(handle, "env%d", statep->env_uid);
        entryp = Tcl_CreateHashEntry(&statep->envs, handle, &new);
        Tcl_SetHashValue(entryp, (ClientData)envp);

        RDB_set_errfile(envp, stderr);
        
        Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);
        return RDB_OK;
    } else if (strcmp(argv[1], "create") == 0) {
        int new;
        char handle[20];
    
        if (argc != 3) {
            Tcl_SetResult(interp, "wrong # args: should be \"env create path\"",
                    TCL_STATIC);
            return TCL_ERROR;
        }

        ret = RDB_create_env(argv[2], &envp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }

        /* Store a pointer to the Tcl interpreter in the user_data field */
        /* envp->user_data = interp; */

        statep->env_uid++;
        sprintf(handle, "env%d", statep->env_uid);
        entryp = Tcl_CreateHashEntry(&statep->envs, handle, &new);
        Tcl_SetHashValue(entryp, (ClientData)envp);

        RDB_set_errfile(envp, stderr);
        
        Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);
        return TCL_OK;
    } else if (strcmp(argv[1], "close") == 0) {
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
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }      
        return TCL_OK;
    } else if (strcmp(argv[1], "dbs") == 0) {
        int i;
        Tcl_Obj *dblistp;
        RDB_object arr;
        RDB_object *dbnamep;

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
        ret = RDB_get_dbs(envp, &arr);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&arr);
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }      

        dblistp = Tcl_NewListObj(0, NULL);
        if (dblistp == NULL)
            return TCL_ERROR;

        i = 0;
        while ((ret = RDB_array_get(&arr, (RDB_int) i++, &dbnamep)) == RDB_OK) {
            char *dbname = RDB_obj_string(dbnamep);

            ret = Tcl_ListObjAppendElement(interp, dblistp,
                    Tcl_NewStringObj(dbname, strlen(dbname)));
            if (ret != TCL_OK)
                return ret;
        }
        if (ret != RDB_NOT_FOUND) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }

        Tcl_SetObjResult(interp, dblistp);

        return TCL_OK;
    } else {
        Tcl_AppendResult(interp, "Bad option: ", argv[1], NULL);
        return TCL_ERROR;
    }
}
