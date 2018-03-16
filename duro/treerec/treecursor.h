/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef TREEREC_TREECURSOR_H_
#define TREEREC_TREECURSOR_H_

#include <gen/types.h>
#include <rec/recmap.h>
#include <stddef.h>

typedef struct RDB_cursor RDB_cursor;
typedef struct RDB_recmap RDB_recmap;
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_index RDB_index;
typedef struct RDB_exec_context RDB_exec_context;

RDB_cursor *
RDB_tree_recmap_cursor(RDB_recmap *, RDB_bool wr, RDB_rec_transaction *, RDB_exec_context *);

RDB_cursor *
RDB_tree_index_cursor(RDB_index *, RDB_bool wr, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_tree_cursor_get(RDB_cursor *, int fno, void **datapp, size_t *, RDB_exec_context *);

int
RDB_tree_cursor_set(RDB_cursor *, int fieldc, RDB_field[], RDB_exec_context *);

int
RDB_tree_cursor_delete(RDB_cursor *, RDB_exec_context *);

int
RDB_tree_cursor_first(RDB_cursor *, RDB_exec_context *);

int
RDB_tree_cursor_next(RDB_cursor *, int flags, RDB_exec_context *);

int
RDB_tree_cursor_prev(RDB_cursor *, RDB_exec_context *);

int
RDB_tree_cursor_seek(RDB_cursor *, int fieldc, RDB_field[], int, RDB_exec_context *);

int
RDB_destroy_tree_cursor(RDB_cursor *, RDB_exec_context *);

#endif /* TREEREC_TREECURSOR_H_ */
