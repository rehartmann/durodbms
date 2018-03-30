/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_SEQUENCEIMPL_H_
#define REC_SEQUENCEIMPL_H_

#include "sequence.h"

#ifdef BERKELEYDB
#include <db.h>
#endif

typedef struct RDB_sequence {
#ifdef BERKELEYDB
    DB_SEQUENCE *seq;
#endif
    char *filenamp;
    char *cnamp;

    int (*close_fn)(RDB_sequence *, RDB_exec_context *);
    int (*next_fn)(RDB_sequence *, RDB_rec_transaction *, RDB_int *, RDB_exec_context *);
    int (*delete_sequence_fn)(RDB_sequence *, RDB_environment *, RDB_rec_transaction *,
            RDB_exec_context *);
} RDB_sequence;

#endif /* REC_SEQUENCEIMPL_H_ */
