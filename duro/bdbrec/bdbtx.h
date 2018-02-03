/*
 * Record-layer transaction functions
 *
 * Copyright (C) 2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef BDBREC_BDBTX_H_
#define BDBREC_BDBTX_H_

typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_environment RDB_environment;
typedef struct RDB_exec_context RDB_exec_context;

RDB_rec_transaction * RDB_bdb_begin_tx(RDB_environment *,
        RDB_rec_transaction *, RDB_exec_context *);

int RDB_bdb_commit(RDB_rec_transaction *, RDB_exec_context *);

int RDB_bdb_abort(RDB_rec_transaction *, RDB_exec_context *);

int RDB_bdb_tx_id(RDB_rec_transaction *);

#endif
