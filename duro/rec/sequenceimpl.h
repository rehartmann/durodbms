/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_SEQUENCEIMPL_H_
#define REC_SEQUENCEIMPL_H_

#include "sequence.h"

#include <db.h>

typedef struct RDB_sequence {
    DB_SEQUENCE *seq;
    char *filenamp;
    char *cnamp;

    int (*close_fn)(RDB_sequence *);
    int (*next_fn)(RDB_sequence *, RDB_rec_transaction *, RDB_int *);
    int (*delete_sequence_fn)(RDB_sequence *, RDB_environment *, RDB_rec_transaction *);
} RDB_sequence;

#endif /* REC_SEQUENCEIMPL_H_ */
