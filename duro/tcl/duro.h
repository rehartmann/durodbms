#ifndef RDB_TCL_DURO_H
#define RDB_TCL_DURO_H

/* $Id$ */

#include <tcl.h>
#include <rel/rdb.h>

typedef struct {
    /* DB Environments */
    Tcl_HashTable envs;
    int env_uid;
    
    /* Transactions */
    Tcl_HashTable txs;
    int tx_uid;

    /* Arrays */
    Tcl_HashTable arrays;
    int array_uid;
} TclState;

int
Duro_env_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
Duro_begin_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
Duro_commit_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
Duro_rollback_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
Duro_table_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_array_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_tcl_close_env(RDB_environment *, Tcl_HashEntry *entryp);

int
Duro_tcl_rollback(RDB_transaction *, Tcl_HashEntry *entryp);

int
Duro_tcl_drop_array(RDB_array *arrayp, Tcl_HashEntry *entryp);

#endif
