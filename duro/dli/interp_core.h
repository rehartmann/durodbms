/*
 * Interpreter core functionality definitions
 *
 * Copyright (C) 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef INTERP_CORE_H_
#define INTERP_CORE_H_

#include "iinterp.h"
#include <gen/hashmap.h>
#include <rel/rdb.h>
#include "parse.h"

#define DURO_RETURN (-1000)
#define DURO_LEAVE (-1001)

enum {
    DURO_MAX_LLEN = 64,
    DEFAULT_VARMAP_SIZE = 128
};

typedef struct Duro_return_info {
    RDB_type *typ;  /* Return type */
    RDB_object *objp;   /* Return value */
} Duro_return_info;

typedef struct {
    RDB_parse_node *rootp;
    RDB_parse_node *stmtlistp;
    int argnamec;
    char **argnamev;
} Duro_op_data;

int
Duro_add_varmap(Duro_interp *, RDB_exec_context *);

void
Duro_destroy_varmap(RDB_hashmap *);

void
Duro_remove_varmap(Duro_interp *);

void
Duro_init_vars(Duro_interp *);

void
Duro_destroy_vars(Duro_interp *);

varmap_node *
Duro_set_current_varmap(Duro_interp *, varmap_node *);

RDB_object *
Duro_lookup_transient_var(Duro_interp *, const char *);

int
Duro_put_var(const char *, RDB_object *, Duro_interp *, RDB_exec_context *);

RDB_type *
Duro_get_var_type(const char *, void *);

int
Duro_exec_vardrop(const RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_rename(const RDB_parse_node *, Duro_interp *, RDB_exec_context *);

const char *
Duro_type_in_use(Duro_interp *, RDB_type *);

RDB_database *
Duro_get_db(Duro_interp *, RDB_exec_context *);

RDB_type *
Duro_parse_node_to_type_retry(RDB_parse_node *, Duro_interp *, RDB_exec_context *);

RDB_object *
Duro_get_var(const char *, void *);

int
Duro_package_q_id(RDB_object *, const char *, Duro_interp *, RDB_exec_context *);

int
Duro_nodes_to_seqitv(RDB_seq_item *, RDB_parse_node *,
        Duro_interp *, RDB_exec_context *);

#endif /* INTERP_CORE_H_ */
