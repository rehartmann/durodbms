#ifndef IINTERP_H
#define IINTERP_H

#include "interp_eval.h"

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

typedef struct {
    char *libname;
    char *ro_op_symname;
    char *update_op_symname;
} Duro_uop_info;

typedef struct Duro_varmap_node {
    RDB_hashmap map;
    struct Duro_varmap_node *parentp;
} varmap_node;

typedef struct foreach_iter {
    RDB_qresult *qrp;
    RDB_object* tbp;
    struct foreach_iter *prevp; /* for chaining nested FOREACHs */
} foreach_iter;

typedef struct Duro_interp {
    volatile sig_atomic_t interrupted;

    RDB_environment *envp;

    tx_node *txnp;

    /*
     * Points to the local variables in the current scope.
     * Linked list from inner to outer scope.
     */
    varmap_node *current_varmapp;

    foreach_iter *current_foreachp;

    RDB_operator *inner_op;

    /* System variables, contains STDIN, STDOUT etc. */
    RDB_hashmap sys_varmap;

    /* System update operators */
    RDB_op_map sys_upd_op_map;

    /* Top-level variables */
    RDB_hashmap root_varmap;

    /* Name of current module */
    RDB_object module_name;

    int err_line;
    char *err_opname;

    RDB_object prompt;

    const char *leave_targetname;

    const char *impl_typename;

    RDB_object current_db_obj;
    RDB_object implicit_tx_obj;

    /* Data needed for user-defined operators */
    RDB_hashmap uop_info_map;

    void *user_data;
} Duro_interp;

int
Duro_init_interp(Duro_interp *, RDB_exec_context *, RDB_environment *, const char *);

void
Duro_destroy_interp(Duro_interp *);

void
Duro_print_error(const RDB_object *);

void
Duro_print_error_f(const RDB_object *, FILE *);

void
Duro_dt_interrupt(Duro_interp *);

int
Duro_dt_execute(FILE *, Duro_interp *, RDB_exec_context *);

int
Duro_dt_execute_path(const char *,
        Duro_interp *, RDB_exec_context *);

int
Duro_dt_execute_str(const char *, Duro_interp *,
        RDB_exec_context *);

RDB_expression *
Duro_dt_parse_expr_str(const char *,
        Duro_interp *, RDB_exec_context *);

int
Duro_dt_get_type_str(RDB_environment *, const char *, RDB_object *,
        Duro_interp *, RDB_exec_context *);

RDB_object *
Duro_lookup_var(const char *, Duro_interp *, RDB_exec_context *);

const char*
Duro_dt_prompt(Duro_interp *);

RDB_environment *
Duro_dt_env(Duro_interp *);

RDB_transaction *
Duro_dt_tx(Duro_interp *);

int
Duro_dt_put_creop_info(Duro_interp *, const char *, Duro_uop_info *,
        RDB_exec_context *);

Duro_uop_info *
Duro_dt_get_creop_info(const Duro_interp *, const char *);

#endif /*IINTERP_H*/
