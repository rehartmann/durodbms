/*
 * Record-layer transaction functions
 *
 * Copyright (C) 2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_TX_H_
#define REC_TX_H_

/* Placeholder type for the actual (e.g. BDB) transaction */
typedef struct RDB_rec_transaction { } RDB_rec_transaction;

typedef struct RDB_environment RDB_environment;

int RDB_begin_rec_tx(RDB_rec_transaction **, RDB_environment *,
        RDB_rec_transaction *);

int RDB_commit_rec_tx(RDB_rec_transaction *);

int RDB_abort_rec_tx(RDB_rec_transaction *);

int RDB_rec_tx_id(RDB_rec_transaction *);

#endif /* REC_TX_H_ */
