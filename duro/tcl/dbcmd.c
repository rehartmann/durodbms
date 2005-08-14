/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"
#include <gen/strfns.h>
#include <string.h>

static int
db_create_cmd(TclState *statep, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    int ret;
    Tcl_HashEntry *entryp;
    RDB_database *dbp;
    RDB_environment *envp;

    if (argc != 4) {
        Tcl_SetResult(interp, "wrong # args, should be: duro::db create dbname env",
                TCL_STATIC);
        return TCL_ERROR;
    }

    entryp = Tcl_FindHashEntry(&statep->envs, argv[2]);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown environment: ", argv[3], NULL);
        return TCL_ERROR;
    }
    envp = Tcl_GetHashValue(entryp);

    ret = RDB_create_db_from_env(argv[3], envp, &dbp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, NULL, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

static int
db_drop_cmd(TclState *statep, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    int ret;
    Tcl_HashEntry *entryp;
    RDB_database *dbp;
    RDB_environment *envp;

    if (argc != 4) {
        Tcl_SetResult(interp, "wrong # args, should be: duro::db drop dbname env",
                TCL_STATIC);
        return TCL_ERROR;
    }

    entryp = Tcl_FindHashEntry(&statep->envs, argv[2]);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown environment: ", argv[3], NULL);
        return TCL_ERROR;
    }
    envp = Tcl_GetHashValue(entryp);

    ret = RDB_get_db_from_env(argv[3], envp, &dbp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, NULL, ret);
        return TCL_ERROR;
    }

    ret = RDB_drop_db(dbp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, NULL, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

int
Duro_db_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    TclState *statep = (TclState *) data;

    if (argc < 2) {
        Tcl_SetResult(interp, "wrong # args: should be duro::db option ?arg ...?",
               TCL_STATIC);
        return TCL_ERROR;
    }

    if (strcmp (argv[1], "create") == 0) {
        return db_create_cmd(statep, interp, argc, argv);
    }
    if (strcmp (argv[1], "drop") == 0) {
        return db_drop_cmd(statep, interp, argc, argv);
    }
    return TCL_ERROR;
}
