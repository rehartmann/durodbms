/* $Id$ */

#include "duro.h"
#include <string.h>

int
Duro_tcl_close_env(RDB_environment *envp, Tcl_HashEntry *entryp)
{
    Tcl_DeleteHashEntry(entryp);
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
        statep->env_uid++;
        sprintf(handle, "env%d", statep->env_uid);
        entryp = Tcl_CreateHashEntry(&statep->envs, handle, &new);
        Tcl_SetHashValue(entryp, (ClientData)envp);

        RDB_set_errfile(envp, stderr);
        
        Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);
        return RDB_OK;
    } else if (strcmp(argv[1], "close") == 0) {
        if (argc != 3) {
            Tcl_SetResult(interp, "wrong # args: should be \"env close handle\"",
                    TCL_STATIC);
            return TCL_ERROR;
        }

        entryp = Tcl_FindHashEntry(&statep->envs, argv[2]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown environment: ", argv[2], NULL);
            return TCL_ERROR;
        }
        envp = Tcl_GetHashValue(entryp);
        ret = Duro_tcl_close_env(envp, entryp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }      
        return RDB_OK;
    } else {
        Tcl_AppendResult(interp, "Bad option: ", argv[1], NULL);
        return TCL_ERROR;
    }
}
