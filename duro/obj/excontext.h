/*
 * Copyright (C) 2013, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef EXCONTEXT_H_
#define EXCONTEXT_H_

#include "object.h"
#include <gen/types.h>
#include <gen/hashmap.h>

enum {
    RDB_ERROR = -1,
};

typedef struct RDB_exec_context {
    /* Internal */

    /* RDB_TRUE if error if the ec contains an error */
    RDB_bool error_active;

    /* The error value, if error_active is RDB_TRUE*/
    RDB_object error;

    /* Property map */
    RDB_hashmap pmap;

    /* RDB_TRUE if the operation that failed should be retried */
    RDB_bool error_retryable;

    /* RDB_TRUE if the transaction should be rolled back */
    RDB_bool rollback;
} RDB_exec_context;

void
RDB_init_exec_context(RDB_exec_context *);

void
RDB_destroy_exec_context(RDB_exec_context *);

RDB_object *
RDB_raise_err(RDB_exec_context *);

RDB_object *
RDB_get_err(RDB_exec_context *);

void
RDB_clear_err(RDB_exec_context *);

RDB_bool
RDB_err_retryable(RDB_exec_context *);

RDB_object *
RDB_raise_no_memory(RDB_exec_context *);

RDB_object *
RDB_raise_invalid_argument(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_no_running_tx(RDB_exec_context *);

RDB_object *
RDB_raise_not_found(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_type_mismatch(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_operator_not_found(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_type_not_found(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_type_constraint_violation(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_element_exists(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_key_violation(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_not_supported(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_name(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_predicate_violation(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_in_use(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_system(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_resource_not_found(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_internal(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_concurrency(RDB_exec_context *);

RDB_object *
RDB_raise_aggregate_undefined(RDB_exec_context *);

RDB_object *
RDB_raise_version_mismatch(RDB_exec_context *);

RDB_object *
RDB_raise_deadlock(RDB_exec_context *);

RDB_object *
RDB_raise_data_corrupted(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_run_recovery(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_syntax(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_connection(const char *, RDB_exec_context *);

int
RDB_ec_set_property(RDB_exec_context *, const char *, void *);

void *
RDB_ec_property(RDB_exec_context *, const char *);

void
RDB_errno_to_error(int, RDB_exec_context *);

#endif /* EXCONTEXT_H_ */
