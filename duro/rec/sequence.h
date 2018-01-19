/*
 * Sequence functions
 *
 * Copyright (C) 2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef SEQUENCE_H_
#define SEQUENCE_H_

#include "tx.h"
#include <gen/types.h>

typedef struct RDB_sequence RDB_sequence;

int
RDB_open_sequence(const char *, const char *, RDB_environment *, RDB_rec_transaction *,
        RDB_sequence **);

int
RDB_close_sequence(RDB_sequence *);

int
RDB_delete_sequence(RDB_sequence *, RDB_environment *, RDB_rec_transaction *);

int
RDB_rename_sequence(const char *, const char *, const char *, RDB_environment *,
        RDB_rec_transaction *);

int
RDB_sequence_next(RDB_sequence *, RDB_rec_transaction *, RDB_int *);

#endif /* SEQUENCE_H_ */
