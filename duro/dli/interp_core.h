/*
 * interp_core.h
 *
 *  Created on: 24.08.2012
 *      Author: Rene Hartmann
 */

#ifndef INTERP_CORE_H_
#define INTERP_CORE_H_

#include <gen/hashmap.h>
#include <rel/opmap.h>
#include <rel/rdb.h>
#include "parse.h"

#include <signal.h>

#define DURO_RETURN (-1000)
#define DURO_LEAVE (-1001)

enum {
    DURO_MAX_LLEN = 64,
    DEFAULT_VARMAP_SIZE = 128
};

typedef struct Duro_tx_node {
    RDB_transaction tx;
    struct Duro_tx_node *parentp;
} tx_node;

typedef struct Duro_return_info {
    RDB_type *typ;  /* Return type */
    RDB_object *objp;   /* Return value */
} return_info;

/* Contains operators and global transient variables */
typedef struct Duro_module {
    /* Global transient variables */
    RDB_hashmap varmap;

    /* System-defined update operators (not stored), only used in Duro_sys_module */
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

extern foreach_iter *current_foreachp;

struct Duro_op_data {
    RDB_parse_node *stmtlistp;
    int argnamec;
    char **argnamev;
};

extern sig_atomic_t Duro_interrupted;

extern RDB_environment *Duro_envp;

extern tx_node *Duro_txnp;

extern Duro_module Duro_sys_module;

extern RDB_operator *Duro_inner_op;

int
Duro_add_varmap(RDB_exec_context *);

void
Duro_destroy_varmap(RDB_hashmap *);

void
Duro_remove_varmap(void);

void
Duro_init_vars(void);

void
Duro_destroy_vars(void);

varmap_node *
Duro_set_current_varmap(varmap_node *);

RDB_object *
Duro_lookup_transient_var(const char *);

RDB_object *
Duro_lookup_var(const char *, RDB_exec_context *);

int
Duro_put_var(const char *, RDB_object *, RDB_exec_context *);

RDB_type *
Duro_get_var_type(const char *, void *);

int
Duro_evaluate_retry(RDB_expression *, RDB_exec_context *, RDB_object *);

RDB_type *
Duro_expr_type_retry(RDB_expression *, RDB_exec_context *);

int
Duro_exec_vardef(RDB_parse_node *, RDB_exec_context *);

int
Duro_exec_vardef_private(RDB_parse_node *, RDB_exec_context *);

int
Duro_exec_vardef_real(RDB_parse_node *, RDB_exec_context *);

int
Duro_exec_vardef_virtual(RDB_parse_node *, RDB_exec_context *);

int
Duro_exec_vardrop(const RDB_parse_node *, RDB_exec_context *);

const char *
Duro_type_in_use(RDB_type *);

RDB_database *
Duro_get_db(RDB_exec_context *);

RDB_type *
Duro_parse_node_to_type_retry(RDB_parse_node *, RDB_exec_context *);

int
Duro_init_obj(RDB_object *, RDB_type *, RDB_exec_context *,
        RDB_transaction *);

#endif /* INTERP_CORE_H_ */
