#ifndef FDBREC_FDBINDEX_H
#define FDBREC_FDBINDEX_H

/*
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rec/recmap.h>

typedef struct RDB_recmap RDB_recmap;
typedef struct RDB_exec_context RDB_exec_context;

/*
 * Secondary index.
 */

typedef struct RDB_index RDB_index;
typedef struct RDB_rec_transaction RDB_rec_transaction;

RDB_index *
RDB_create_fdb_index(RDB_recmap *, const char *, const char *,
        RDB_environment *, int, const RDB_field_descriptor[],
        const RDB_compare_field[], int, RDB_rec_transaction *,
        RDB_exec_context *);

RDB_index *
RDB_open_fdb_index(RDB_recmap *, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *,
        RDB_exec_context *);

/* Close an index. */
int
RDB_close_fdb_index(RDB_index *, RDB_exec_context *);

/* Delete an index. */
int
RDB_delete_fdb_index(RDB_index *, RDB_rec_transaction *,
        RDB_exec_context *);

int
RDB_fdb_index_get_fields(RDB_index *, RDB_field[], int, RDB_rec_transaction *,
           RDB_field[], RDB_exec_context *);

int
RDB_fdb_index_delete_rec(RDB_index *, RDB_field[], RDB_rec_transaction *,
        RDB_exec_context *);

#endif
