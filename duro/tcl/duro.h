#ifndef TCL_DURO_H
#define TCL_DURO_H

#include <tcl.h>
#include <rel/rdb.h>

typedef struct {
    /* DB Environments */
    Tcl_HashTable envs;
    int env_uid;
    
    /* Transactions */
    Tcl_HashTable txs;
    int tx_uid;
} TclState;

int
env_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
tx_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
table_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
close_env(RDB_environment *, Tcl_HashEntry *entryp);

int
rollback_tx(RDB_transaction *, Tcl_HashEntry *entryp);

#endif
