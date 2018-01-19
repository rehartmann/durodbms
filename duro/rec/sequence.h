/*
 * sequence.h
 *
 *  Created on: 15.11.2013
 *      Author: Rene Hartmann
 */

#ifndef SEQUENCE_H_
#define SEQUENCE_H_

#include <gen/types.h>

#include <db.h>

typedef struct RDB_sequence {
    DB_SEQUENCE *seq;
    char *filenamp;
    char *cnamp;
} RDB_sequence;

int
RDB_open_sequence(const char *, const char *, RDB_environment *, DB_TXN *,
        RDB_sequence **);

int
RDB_close_sequence(RDB_sequence *);

int
RDB_delete_sequence(RDB_sequence *, RDB_environment *, DB_TXN *);

int
RDB_rename_sequence(const char *, const char *, const char *, RDB_environment *,
        DB_TXN *);

int
RDB_sequence_next(RDB_sequence *, DB_TXN *, RDB_int *);

#endif /* SEQUENCE_H_ */
