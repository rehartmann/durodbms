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
} TclState;

int
RDB_env_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
RDB_begin_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
RDB_commit_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
RDB_rollback_cmd(ClientData data, Tcl_Interp *interp, int argc, const char *argv[]);

int
RDB_table_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
RDB_tcl_close_env(RDB_environment *, Tcl_HashEntry *entryp);

int
RDB_tcl_rollback(RDB_transaction *, Tcl_HashEntry *entryp);

#endif
