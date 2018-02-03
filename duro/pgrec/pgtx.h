/*
 * Record-layer transaction functions implemented using PostgreSQL
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef PGREC_PGTX_H_
#define PGREC_PGTX_H_

typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_environment RDB_environment;
typedef struct RDB_exec_context RDB_exec_context;

RDB_rec_transaction * RDB_pg_begin_tx(RDB_environment *,
        RDB_rec_transaction *, RDB_exec_context *);

int RDB_pg_commit(RDB_rec_transaction *, RDB_exec_context *);

int RDB_pg_abort(RDB_rec_transaction *, RDB_exec_context *);

int RDB_pg_tx_id(RDB_rec_transaction *);

#endif
