#include "duro.h"
#include <string.h>
#include <stdio.h>

void
duro_cleanup(ClientData data)
{
    RDB_environment *envp;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    Tcl_HashSearch search;
    TclState *statep = (TclState *) data;

    /* Abort transactions */
    entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    while (entryp != NULL) {
        txp = Tcl_GetHashValue(entryp);
        rollback_tx(txp, entryp);
        free(txp);
        entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    }

    /* Close DB environments */
    entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    while (entryp != NULL) {
        envp = Tcl_GetHashValue(entryp);
        close_env(envp, entryp);
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

    Tcl_CreateCommand(interp, "duro::env", env_cmd, (ClientData)statep,
            duro_cleanup);
    Tcl_CreateCommand(interp, "duro::tx", tx_cmd, (ClientData)statep,
            duro_cleanup);
    Tcl_CreateObjCommand(interp, "duro::table", table_cmd, (ClientData)statep,
            duro_cleanup);
    
    Tcl_PkgProvide(interp, "duro", "0.7");
    return TCL_OK;
}
