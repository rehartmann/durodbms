/*
 * Sequence functions implemented using FoundationDB
 *
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef FDBREC_FDBSEQUENCE_H_
#define FDBREC_FDBSEQUENCE_H_

#include <gen/types.h>

typedef struct RDB_sequence RDB_sequence;
typedef struct RDB_environment RDB_environment;
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_exec_context RDB_exec_context;

RDB_sequence *
RDB_open_fdb_sequence(const char *,
        RDB_environment *, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_close_fdb_sequence(RDB_sequence *, RDB_exec_context *);

int
RDB_delete_fdb_sequence(RDB_sequence *, RDB_environment *, RDB_rec_transaction *,
        RDB_exec_context *);

int
RDB_rename_fdb_sequence(const char *, const char *, RDB_environment *,
        RDB_rec_transaction *, RDB_exec_context *);

int
RDB_fdb_sequence_next(RDB_sequence *, RDB_rec_transaction *, RDB_int *,
        RDB_exec_context *);

#endif /* FDBREC_FDBSEQUENCE_H_ */
