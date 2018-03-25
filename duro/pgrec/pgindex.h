#ifndef RDB_PGINDEX_H
#define RDB_PGINDEX_H

/*
 * Copyright (C) 2018 Rene Hartmann.
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
RDB_create_pg_index(RDB_recmap *, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const RDB_field_descriptor fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *,
        RDB_exec_context *);

/* Close an index. */
int
RDB_close_pg_index(RDB_index *, RDB_exec_context *);

/* Delete an index. */
int
RDB_delete_pg_index(RDB_index *, RDB_rec_transaction *,
        RDB_exec_context *);

#endif
