/* $Id$ */

#include "duro.h"
#include <string.h>
#include <stdio.h>

static void
duro_cleanup(ClientData data)
{
    RDB_environment *envp;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    Tcl_HashSearch search;
    TclState *statep = (TclState *) data;

    /* Abort pending transactions */
    entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    while (entryp != NULL) {
        txp = Tcl_GetHashValue(entryp);
        RDB_tcl_rollback(txp, entryp);
        free(txp);
        entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    }

    /* Close open DB environments */
    entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    while (entryp != NULL) {
        envp = Tcl_GetHashValue(entryp);
        RDB_tcl_close_env(envp, entryp);
        entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    }

    Tcl_Free((char *)statep);
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

    Tcl_CreateCommand(interp, "duro::env", RDB_env_cmd, (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::begin", RDB_begin_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::commit", RDB_commit_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::rollback", RDB_rollback_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::table", RDB_table_cmd,
            (ClientData)statep, NULL);

    Tcl_CreateExitHandler(duro_cleanup, (ClientData)statep);
    
    Tcl_PkgProvide(interp, "duro", "0.7");
    return TCL_OK;
}
