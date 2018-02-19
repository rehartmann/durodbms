/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef PGREC_PGENV_H_
#define PGREC_PGENV_H_

#include <gen/types.h>
#include <libpq-fe.h>

typedef struct RDB_environment RDB_environment;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_object RDB_object;

RDB_environment *
RDB_pg_open_env(const char *, RDB_exec_context *);

int
RDB_pg_close_env(RDB_environment *, RDB_exec_context *);

RDB_int
RDB_update_pg_sql(RDB_environment *, const char *,
        RDB_rec_transaction *, RDB_exec_context *);

void
RDB_pgresult_to_error(const RDB_environment *, const PGresult *, RDB_exec_context *);

int
RDB_pg_literal(RDB_environment *, RDB_object *, const RDB_object *, RDB_exec_context *);

#endif /* BDBREC_BDBENV_H_ */
