#ifndef RDB_INDEX_H
#define RDB_INDEX_H

/*
 * Copyright (C) 2003-2004 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "recmap.h"

/*
 * Secondary index.
 */

typedef struct RDB_index {
    RDB_recmap *rmp;
    DB *dbp;
    char *namp;
    char *filenamp;
    int fieldc;
    int *fieldv;
    /* For ordered indexes */
    RDB_compare_field *cmpv;
} RDB_index;

int
RDB_create_index(RDB_recmap *, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, DB_TXN *, RDB_index **);

int
RDB_open_index(RDB_recmap *, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, DB_TXN *, RDB_index **);

/* Close an index. */
int
RDB_close_index(RDB_index *);

RDB_bool
RDB_index_is_ordered(RDB_index *);

/* Delete an index. */
int
RDB_delete_index(RDB_index *, RDB_environment *, DB_TXN *txid);

int
RDB_index_get_fields(RDB_index *ixp, RDB_field keyv[], int fieldc, DB_TXN *txid,
           RDB_field retfieldv[]);

int
RDB_index_delete_rec(RDB_index *ixp, RDB_field keyv[], DB_TXN *txid);

#endif
