/* $Id$ */

#include "duro.h"
#include <string.h>

int
RDB_tcl_rollback(RDB_transaction *txp, Tcl_HashEntry *entryp)
{
    Tcl_DeleteHashEntry(entryp);
    return RDB_rollback(txp);
}

int
RDB_begin_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[])
{
    int ret;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    int new;
    char handle[20];
    RDB_environment *envp;
    RDB_database *dbp;
    TclState *statep = (TclState *) data;

    if (argc != 3) {
        interp->result = "wrong # args: should be \"begin env db\"";
        return TCL_ERROR;
    }

    entryp = Tcl_FindHashEntry(&statep->envs, argv[1]);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown environment: ", argv[1], NULL);
        return TCL_ERROR;
    }
    envp = Tcl_GetHashValue(entryp);

    /* Get database */
    ret = RDB_get_db_from_env(argv[2], envp, &dbp);
    if (ret != RDB_OK) { 
        interp->result = (char *) RDB_strerror(ret);
        return TCL_ERROR;
    }

    /* Start transaction */
    txp = (RDB_transaction *)Tcl_Alloc(sizeof (RDB_transaction));
    ret = RDB_begin_tx(txp, dbp, NULL);
    if (ret != RDB_OK) { 
        interp->result = (char *) RDB_strerror(ret);
        return TCL_ERROR;
    }        

    statep->tx_uid++;
    sprintf(handle, "tx%d", statep->tx_uid);
    entryp = Tcl_CreateHashEntry(&statep->txs, handle, &new);
    Tcl_SetHashValue(entryp, (ClientData)txp);
        
    Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);
    return RDB_OK;
}

int
RDB_commit_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[])
{
    int ret;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (argc != 2) {
        interp->result = "wrong # args: should be \"commit tx\"";
        return TCL_ERROR;
    }

    entryp = Tcl_FindHashEntry(&statep->txs, argv[1]);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", argv[1], NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);
    Tcl_DeleteHashEntry(entryp);
    ret = RDB_commit(txp);
    Tcl_Free((char *) txp);
    if (ret != RDB_OK) { 
        interp->result = (char *) RDB_strerror(ret);
        return TCL_ERROR;
    }
    return RDB_OK;
}

int
RDB_rollback_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[])
{
    int ret;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (argc != 2) {
        interp->result = "wrong # args: should be \"rollback tx\"";
        return TCL_ERROR;
    }

    entryp = Tcl_FindHashEntry(&statep->txs, argv[1]);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", argv[1], NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);
    ret = RDB_tcl_rollback(txp, entryp);
    Tcl_Free((char *) txp);
    if (ret != RDB_OK) { 
        interp->result = (char *) RDB_strerror(ret);
        return TCL_ERROR;
    }
    return RDB_OK;
}
