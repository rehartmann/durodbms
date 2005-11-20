/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

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
            Duro_tcl_drop_ltable(tbep, entryp, statep->current_ecp);
            entryp = Tcl_FirstHashEntry(&statep->ltables, &search);
        } else {
            entryp = Tcl_NextHashEntry(&search);
        }
    }

    return RDB_close_env(envp);
}

/*
 * Invoke command "dberror" with the error message as argument.
 * If this fails, write the message to standard error.
 */
static void
dberror(const char *msg, void *arg)
{
    int ret;
    Tcl_Obj *objv[2];
    Tcl_Interp *interp = (Tcl_Interp *)arg;

    objv[0] = Tcl_NewStringObj("dberror", strlen("dberror"));
    objv[1] = Tcl_NewStringObj(msg, strlen(msg));

    /* 
     * Required, otherwise the program crashes when the "dberror" command
     * does not exist
     */
    Tcl_IncrRefCount(objv[0]);
    Tcl_IncrRefCount(objv[1]);

    ret = Tcl_EvalObjv(interp, 2, objv, 0);
    if (ret == TCL_ERROR)
        fprintf(stderr, "%s\n", msg);

    Tcl_DecrRefCount(objv[0]);
    Tcl_DecrRefCount(objv[1]);

    /* Ignore result of Tcl command evaluation */
    Tcl_ResetResult(interp);
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

    RDB_clear_err(statep->current_ecp);

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
            Tcl_AppendResult(interp, "database error: ", db_strerror(ret),
                    (char *) NULL);
            return TCL_ERROR;
        }

        statep->env_uid++;
        sprintf(handle, "env%d", statep->env_uid);
        entryp = Tcl_CreateHashEntry(&statep->envs, handle, &new);
        Tcl_SetHashValue(entryp, (ClientData)envp);

        RDB_set_errfn(envp, &dberror, interp);

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
            Tcl_AppendResult(interp, "database error: ", db_strerror(ret),
                    (char *) NULL);
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
                    Tcl_NewStringObj(dbname, strlen(dbname)));
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
    Tcl_AppendResult(interp, "Bad option: ", argv[1], NULL);
    return TCL_ERROR;
}
