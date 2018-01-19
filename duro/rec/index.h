#ifndef RDB_INDEX_H
#define RDB_INDEX_H

/*
 * Copyright (C) 2003-2004 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "recmap.h"

typedef struct RDB_recmap RDB_recmap;

/*
 * Secondary index.
 */

typedef struct RDB_index RDB_index;

typedef struct RDB_rec_transaction RDB_rec_transaction;

int
RDB_create_index(RDB_recmap *, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *,
        RDB_index **);

int
RDB_open_index(RDB_recmap *, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *, RDB_index **);

/* Close an index. */
int
RDB_close_index(RDB_index *);

RDB_bool
RDB_index_is_ordered(RDB_index *);

/* Delete an index. */
int
RDB_delete_index(RDB_index *, RDB_environment *, RDB_rec_transaction *);

int
RDB_index_get_fields(RDB_index *, RDB_field[], int, RDB_rec_transaction *,
           RDB_field[]);

int
RDB_index_delete_rec(RDB_index *, RDB_field[], RDB_rec_transaction *);

#endif
