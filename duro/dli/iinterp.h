#ifndef IINTERP_H
#define IINTERP_H

#include <rel/rdb.h>
#include <obj/opmap.h>
#include "parse.h"

#include <signal.h>
#include <stdio.h>

typedef struct yy_buffer_state *YY_BUFFER_STATE;

typedef struct Duro_tx_node {
    RDB_transaction tx;
    struct Duro_tx_node *parentp;
} tx_node;

/* Contains operators and global transient variables */
typedef struct Duro_module {
    /* Global transient variables */
    RDB_hashmap varmap;

    /* System-defined update operators (not stored), only used in sys_module */
    RDB_op_map upd_op_map;
} Duro_module;

typedef struct Duro_varmap_node {
    RDB_hashmap map;
    struct Duro_varmap_node *parentp;
} varmap_node;

typedef struct foreach_iter {
    RDB_qresult *qrp;
    RDB_object* tbp;
    struct foreach_iter *prevp; /* for chaining nested FOREACHs */
} foreach_iter;

typedef struct {
    sig_atomic_t interrupted;

    RDB_environment *envp;

    tx_node *txnp;

    /*
     * Points to the local variables in the current scope.
     * Linked list from inner to outer scope.
     */
    varmap_node *current_varmapp;

    foreach_iter *current_foreachp;

    RDB_operator *inner_op;

    /* System Duro_module, contains STDIN, STDOUT etc. */
    Duro_module sys_module;

    /* Top-level Duro_module */
    Duro_module root_module;

    int err_line;

    RDB_object prompt;

    const char *leave_targetname;

    const char *impl_typename;

    RDB_object current_db_obj;
    RDB_object implicit_tx_obj;
} Duro_interp;

int
Duro_init_interp(Duro_interp *, RDB_exec_context *, const char *);

void
Duro_destroy_interp(Duro_interp *);

void
Duro_print_error(const RDB_object *);

void
Duro_print_error_f(const RDB_object *, FILE *);

void
Duro_dt_interrupt(Duro_interp *);

int
Duro_dt_execute(RDB_environment *, FILE *, Duro_interp *, RDB_exec_context *);

int
Duro_dt_execute_path(RDB_environment *, const char *,
        Duro_interp *, RDB_exec_context *);

int
Duro_dt_execute_str(RDB_environment *, const char *, Duro_interp *,
        RDB_exec_context *);

int
Duro_dt_evaluate_str(RDB_environment *, const char *, RDB_object *,
        Duro_interp *interp, RDB_exec_context *);

const char*
Duro_dt_prompt(Duro_interp *);

#endif /*IINTERP_H*/
