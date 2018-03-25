#ifndef RDB_INDEX_H
#define RDB_INDEX_H

/*
 * Copyright (C) 2003-2004 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Secondary index.
 */

#include "recmap.h"

typedef struct RDB_recmap RDB_recmap;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_index RDB_index;
typedef struct RDB_rec_transaction RDB_rec_transaction;

RDB_index *
RDB_create_index(RDB_recmap *, const char *, const char *,
        RDB_environment *, int, const RDB_field_descriptor[],
        const RDB_compare_field[], int, RDB_rec_transaction *,
        RDB_exec_context *);

RDB_index *
RDB_open_index(RDB_recmap *, const char *, const char *,
        RDB_environment *, int, const int[],
        const RDB_compare_field[], int, RDB_rec_transaction *, RDB_exec_context *);

/* Close an index. */
int
RDB_close_index(RDB_index *, RDB_exec_context *);

RDB_bool
RDB_index_is_ordered(const RDB_index *);

/* Delete an index. */
int
RDB_delete_index(RDB_index *, RDB_rec_transaction *,
        RDB_exec_context *);

int
RDB_index_get_fields(RDB_index *, RDB_field[], int, RDB_rec_transaction *,
           RDB_field[], RDB_exec_context *);

int
RDB_index_delete_rec(RDB_index *, RDB_field[], RDB_rec_transaction *,
        RDB_exec_context *);

#endif
