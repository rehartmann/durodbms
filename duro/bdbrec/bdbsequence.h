/*
 * Sequence functions implemented using Berkeley DB
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef BDBREC_BDBSEQUENCE_H_
#define BDBREC_BDBSEQUENCE_H_

#include <gen/types.h>

typedef struct RDB_sequence RDB_sequence;
typedef struct RDB_environment RDB_environment;
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_exec_context RDB_exec_context;

RDB_sequence *
RDB_open_bdb_sequence(const char *,
        RDB_environment *, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_close_bdb_sequence(RDB_sequence *, RDB_exec_context *);

int
RDB_delete_bdb_sequence(RDB_sequence *, RDB_environment *, RDB_rec_transaction *,
        RDB_exec_context *);

int
RDB_rename_bdb_sequence(const char *, const char *, RDB_environment *,
        RDB_rec_transaction *, RDB_exec_context *);

int
RDB_bdb_sequence_next(RDB_sequence *, RDB_rec_transaction *, RDB_int *,
        RDB_exec_context *);

#endif /* BDBREC_BDBSEQUENCE_H_ */
