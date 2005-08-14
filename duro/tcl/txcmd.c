/*
 * $Id$
 *
 * Copyright (C) 2004-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"
#include <string.h>

int
Duro_tcl_rollback(RDB_transaction *txp, Tcl_HashEntry *entryp)
{
    int ret;

    Tcl_DeleteHashEntry(entryp);
    ret = RDB_rollback(txp);
    Tcl_Free((char *) txp);
    return ret;
}

int
Duro_begin_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    int ret;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    int new;
    char handle[20];
    RDB_environment *envp;
    RDB_database *dbp;
    RDB_transaction *parentp;
    TclState *statep = (TclState *) data;

    if (argc == 3) {
        /*
         * Transaction is not nested
         */
        entryp = Tcl_FindHashEntry(&statep->envs, argv[1]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown environment: ", argv[1], NULL);
            return TCL_ERROR;
        }
        envp = Tcl_GetHashValue(entryp);

        /* Get database */
        ret = RDB_get_db_from_env(argv[2], envp, &dbp);
        if (ret != RDB_OK) { 
            Duro_dberror(interp, txp, ret);
            return TCL_ERROR;
        }

        parentp = NULL;
    } else if (argc == 2) {
        /*
         * Transaction is nested
         */
        entryp = Tcl_FindHashEntry(&statep->txs, argv[1]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown transaction: ", argv[1], NULL);
            return TCL_ERROR;
        }
        parentp = Tcl_GetHashValue(entryp);
        dbp = RDB_tx_db(parentp);
    } else {
        Tcl_SetResult(interp, "wrong # args: should be \"begin env db\" or \"begin tx\"",
                TCL_STATIC);
        return TCL_ERROR;
    }

    /* Start transaction */
    txp = (RDB_transaction *)Tcl_Alloc(sizeof (RDB_transaction));
    ret = RDB_begin_tx(txp, dbp, parentp);
    if (ret != RDB_OK) { 
        Duro_dberror(interp, txp, ret);
        return TCL_ERROR;
    }        

    statep->tx_uid++;
    sprintf(handle, "tx%d", statep->tx_uid);
    entryp = Tcl_CreateHashEntry(&statep->txs, handle, &new);
    Tcl_SetHashValue(entryp, (ClientData)txp);
        
    Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);

    /*
     * Attach interpreter to the transaction to make it the interpreter
     * available to operators
     */
    txp->user_data = interp;
    return RDB_OK;
}

int
Duro_commit_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    int ret;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (argc != 2) {
        Tcl_SetResult(interp, "wrong # args: should be \"commit tx\"",
                TCL_STATIC);
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
        Duro_dberror(interp, txp, ret);
        return TCL_ERROR;
    }
    return RDB_OK;
}

int
Duro_rollback_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    int ret;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (argc != 2) {
        Tcl_SetResult(interp, "wrong # args: should be \"rollback tx\"",
                TCL_STATIC);
        return TCL_ERROR;
    }

    entryp = Tcl_FindHashEntry(&statep->txs, argv[1]);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", argv[1], NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);
    ret = Duro_tcl_rollback(txp, entryp);
    if (ret != RDB_OK) { 
        Duro_dberror(interp, txp, ret);
        return TCL_ERROR;
    }
    return RDB_OK;
}

int
Duro_txdb_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[])
{
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (argc != 2) {
        Tcl_SetResult(interp, "wrong # args: should be \"txdb tx\"",
                TCL_STATIC);
        return TCL_ERROR;
    }

    entryp = Tcl_FindHashEntry(&statep->txs, argv[1]);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", argv[1], NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);
    Tcl_SetResult(interp, RDB_db_name(RDB_tx_db(txp)), TCL_VOLATILE);
    return RDB_OK;
}
