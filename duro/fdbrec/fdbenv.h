/*
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef FDBREC_FDBENV_H_
#define FDBREC_FDBENV_H_

#define FDB_API_VERSION 600 
#include <foundationdb/fdb_c.h>

typedef struct RDB_environment RDB_environment;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_object RDB_object;

RDB_environment *
RDB_fdb_open_env(const char *, RDB_exec_context *);

int
RDB_fdb_close_env(RDB_environment *, RDB_exec_context *);

void
RDB_handle_fdb_errcode(fdb_error_t, RDB_exec_context *, FDBTransaction *);

int
RDB_fdb_string_literal(RDB_environment *, RDB_object *, const char *,
        RDB_exec_context *);

int
RDB_fdb_binary_literal(RDB_environment *, RDB_object *, const void *,
        size_t, RDB_exec_context *);

#endif /* FDBREC_FDBENV_H_ */
