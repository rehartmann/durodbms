/*
 * Record-layer transaction functions implemented using PostgreSQL
 *
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef FDBREC_FDBTX_H_
#define FDBREC_FDBTX_H_

typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_environment RDB_environment;
typedef struct RDB_exec_context RDB_exec_context;

RDB_rec_transaction * RDB_fdb_begin_tx(RDB_environment *,
        RDB_rec_transaction *, RDB_exec_context *);

int RDB_fdb_commit(RDB_rec_transaction *, RDB_exec_context *);

int RDB_fdb_abort(RDB_rec_transaction *, RDB_exec_context *);

int RDB_fdb_tx_id(RDB_rec_transaction *);

#endif
