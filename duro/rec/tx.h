/*
 * Record-layer transaction functions
 *
 * Copyright (C) 2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_TX_H_
#define REC_TX_H_

/* Placeholder type for the actual (e.g. BDB) transaction */
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_environment RDB_environment;
typedef struct RDB_exec_context RDB_exec_context;

RDB_rec_transaction *RDB_begin_rec_tx(RDB_environment *,
        RDB_rec_transaction *, RDB_exec_context *);

int RDB_commit_rec_tx(RDB_rec_transaction *, RDB_environment *,
        RDB_exec_context *);

int RDB_abort_rec_tx(RDB_rec_transaction *, RDB_environment *,
        RDB_exec_context *);

int RDB_rec_tx_id(RDB_rec_transaction *, RDB_environment *);

#endif /* REC_TX_H_ */
