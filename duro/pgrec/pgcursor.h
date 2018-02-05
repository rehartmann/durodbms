/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef PGREC_BDBCURSOR_H_
#define PGREC_BDBCURSOR_H_

#include <gen/types.h>
#include <rec/recmap.h>

typedef struct RDB_cursor RDB_cursor;
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_index RDB_index;
typedef struct RDB_exec_context RDB_exec_context;

RDB_cursor *
RDB_pg_recmap_cursor(RDB_recmap *, RDB_bool wr, RDB_rec_transaction *, RDB_exec_context *);

RDB_cursor *
RDB_pg_query_cursor(RDB_environment*, const char *, RDB_bool wr, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_pg_cursor_get(RDB_cursor *, int fno, void **datapp, size_t *, RDB_exec_context *);

int
RDB_pg_cursor_get_by_name(RDB_cursor *, const char *, void **,
        size_t *, int, RDB_exec_context *);

int
RDB_pg_cursor_first(RDB_cursor *, RDB_exec_context *);

int
RDB_pg_cursor_next(RDB_cursor *, int flags, RDB_exec_context *);

int
RDB_pg_cursor_prev(RDB_cursor *, RDB_exec_context *);

int
RDB_destroy_pg_cursor(RDB_cursor *, RDB_exec_context *);

int
RDB_pg_cursor_set(RDB_cursor *, int, RDB_field[], RDB_exec_context *);

int
RDB_pg_cursor_delete(RDB_cursor *, RDB_exec_context *);

#endif
