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

    /* Local tables */
    Tcl_HashTable ltables;
    int ltable_uid;

    /* Execution context */
    RDB_exec_context ec;
    RDB_exec_context *current_ecp;
} TclState;

/*
 * Stores a table pointer together with the environment
 * so the tables belonging to an environment can be identified
 */ 
typedef struct {
    RDB_table *tablep;
    RDB_environment *envp;
} table_entry;

int
Duro_env_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[]);

int
Duro_begin_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[]);

int
Duro_commit_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[]);

int
Duro_txdb_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[]);

int
Duro_rollback_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[]);

int
Duro_table_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_insert_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_delete_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_update_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_db_cmd(ClientData data, Tcl_Interp *interp, int argc, CONST char *argv[]);

int
Duro_array_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_operator_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_call_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_expr_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

int
Duro_type_cmd(ClientData data, Tcl_Interp *interp,
        int objc, Tcl_Obj *CONST objv[]);

int
Duro_index_cmd(ClientData data, Tcl_Interp *interp,
        int objc, Tcl_Obj *CONST objv[]);

int
Duro_constraint_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[]);

int
Duro_massign_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[]);

int
Duro_tcl_close_env(TclState *statep, RDB_environment *, Tcl_HashEntry *entryp);

int
Duro_tcl_rollback(Tcl_HashEntry *entryp, RDB_exec_context *, RDB_transaction *);

int
Duro_tcl_drop_ltable(table_entry *, Tcl_HashEntry *entryp, RDB_exec_context *);

int
Duro_tcl_drop_array(RDB_object *arrayp, Tcl_HashEntry *entryp,
        RDB_exec_context *);

void
Duro_dberror(Tcl_Interp *interp, const RDB_object *errp, RDB_transaction *);

RDB_table *
Duro_get_table(TclState *, Tcl_Interp *, const char *name, RDB_transaction *);

Tcl_Obj *
Duro_tuple_to_list(Tcl_Interp *, const RDB_object *, RDB_transaction *);

RDB_table *
Duro_get_ltable(const char *name, void *arg);

Tcl_Obj *
Duro_irep_to_tcl(Tcl_Interp *interp, const RDB_object *objp,
        RDB_exec_context *, RDB_transaction *);

Tcl_Obj *
Duro_to_tcl(Tcl_Interp *interp, const RDB_object *objp, RDB_exec_context *,
        RDB_transaction *);

int
Duro_tcl_to_duro(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
        RDB_object *objp, RDB_exec_context *, RDB_transaction *);

RDB_type *
Duro_get_type(Tcl_Obj *objp, Tcl_Interp *, RDB_exec_context *,
         RDB_transaction *);

RDB_seq_item *
Duro_tobj_to_seq_items(Tcl_Interp *interp, Tcl_Obj *tobjp, int *seqitcp,
        RDB_bool, RDB_bool *);

int
Duro_init_tcl(Tcl_Interp *, TclState **);

int
Duro_add_table(Tcl_Interp *interp, TclState *statep, RDB_table *tbp,
        const char *name, RDB_environment *envp);

RDB_expression *
Duro_parse_expr_utf(Tcl_Interp *, const char *, void *,
        RDB_exec_context *, RDB_transaction *);

RDB_table *
Duro_parse_table_utf(Tcl_Interp *interp, const char *s, void *arg,
        RDB_exec_context *, RDB_transaction *);

#endif
