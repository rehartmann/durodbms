#include "duro.h"
#include <string.h>

int
rollback_tx(RDB_transaction *txp, Tcl_HashEntry *entryp)
{
    Tcl_DeleteHashEntry(entryp);
    return RDB_rollback(txp);
}

int
tx_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[])
{
    int ret;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    if (argc < 2) {
        interp->result = "wrong # args: should be \"tx option ?arg ...?\"";
        return TCL_ERROR;
    }

    if (strcmp(argv[1], "begin") == 0) {
        int new;
        char handle[20];
        RDB_environment *envp;
        RDB_database *dbp;
    
        if (argc != 4) {
            interp->result = "wrong # args: should be \"tx begin env db\"";
            return TCL_ERROR;
        }

        entryp = Tcl_FindHashEntry(&statep->envs, argv[2]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown environment: ", argv[2], NULL);
            return TCL_ERROR;
        }
        envp = Tcl_GetHashValue(entryp);

        /* Get database */
        ret = RDB_get_db_from_env(argv[3], envp, &dbp);
        if (ret != RDB_OK) { 
            interp->result = (char *) RDB_strerror(ret);
            return TCL_ERROR;
        }

        /* Start transaction */
        txp = malloc(sizeof (RDB_transaction));
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
    } else if (strcmp(argv[1], "commit") == 0) {
        if (argc != 3) {
            interp->result = "wrong # args: should be \"tx commit txhandle\"";
            return TCL_ERROR;
        }

        entryp = Tcl_FindHashEntry(&statep->txs, argv[2]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown transaction: ", argv[2], NULL);
            return TCL_ERROR;
        }
        txp = Tcl_GetHashValue(entryp);
        Tcl_DeleteHashEntry(entryp);
        ret = RDB_rollback(txp);
        if (ret != RDB_OK) { 
            interp->result = (char *) RDB_strerror(ret);
            return TCL_ERROR;
        }
        return RDB_OK;
    } else if (strcmp(argv[1], "rollback") == 0) {
        if (argc != 3) {
            interp->result = "wrong # args: should be \"tx commit txhandle\"";
            return TCL_ERROR;
        }

        entryp = Tcl_FindHashEntry(&statep->txs, argv[2]);
        if (entryp == NULL) {
            Tcl_AppendResult(interp, "Unknown transaction: ", argv[2], NULL);
            return TCL_ERROR;
        }
        txp = Tcl_GetHashValue(entryp);
        ret = rollback_tx(txp, entryp);
        if (ret != RDB_OK) { 
            interp->result = (char *) RDB_strerror(ret);
            return TCL_ERROR;
        }
        return RDB_OK;
    } else {
        Tcl_AppendResult(interp, "Bad option: ", argv[1], NULL);
        return TCL_ERROR;
    }
}
