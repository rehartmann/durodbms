/* $Id$ */

#include "duro.h"
#include <string.h>
#include <stdio.h>

extern int yydebug;

static void
duro_cleanup(ClientData data)
{
    RDB_environment *envp;
    RDB_transaction *txp;
    RDB_array *arrayp;
    Tcl_HashEntry *entryp;
    Tcl_HashSearch search;
    TclState *statep = (TclState *) data;

    /* Destroy existing arrays */
    entryp = Tcl_FirstHashEntry(&statep->arrays, &search);
    while (entryp != NULL) {
        arrayp = Tcl_GetHashValue(entryp);
        Duro_tcl_drop_array(arrayp, entryp);
        entryp = Tcl_FirstHashEntry(&statep->arrays, &search);
    }

    /* Abort pending transactions */
    entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    while (entryp != NULL) {
        txp = Tcl_GetHashValue(entryp);
        Duro_tcl_rollback(txp, entryp);
        entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    }

    /* Close open DB environments */
    entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    while (entryp != NULL) {
        envp = Tcl_GetHashValue(entryp);
        Duro_tcl_close_env(statep, envp, entryp);
        entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    }

    Tcl_Free((char *) statep);
}

void
Duro_dberror(Tcl_Interp *interp, int err)
{
    Tcl_AppendResult(interp, "Database error: ", (char *) RDB_strerror(err),
            TCL_STATIC);
}    

int
Duro_Init(Tcl_Interp *interp)
{
    TclState *statep;

    if (Tcl_InitStubs(interp, "8.1", 0) == NULL) {
        return TCL_ERROR;
    }

    statep = (TclState *) Tcl_Alloc(sizeof (TclState));
    Tcl_InitHashTable(&statep->envs, TCL_STRING_KEYS);
    statep->env_uid = 0;
    Tcl_InitHashTable(&statep->txs, TCL_STRING_KEYS);
    statep->tx_uid = 0;
    Tcl_InitHashTable(&statep->arrays, TCL_STRING_KEYS);
    statep->array_uid = 0;
    Tcl_InitHashTable(&statep->ltables, TCL_STRING_KEYS);
    statep->ltable_uid = 0;

    Tcl_CreateCommand(interp, "duro::env", Duro_env_cmd, (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::begin", Duro_begin_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::commit", Duro_commit_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::rollback", Duro_rollback_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::db", Duro_db_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::table", Duro_table_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::array", Duro_array_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::operator", Duro_operator_cmd,
            (ClientData)statep, NULL);

    Tcl_CreateExitHandler(duro_cleanup, (ClientData)statep);

    /* yydebug = 1; */
    
    Tcl_PkgProvide(interp, "duro", "0.7");
    return TCL_OK;
}
