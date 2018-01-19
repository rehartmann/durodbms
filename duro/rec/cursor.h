#ifndef RDB_CURSOR_H
#define RDB_CURSOR_H

/*
 * Copyright (C) 2003-2004, 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <gen/types.h>
#include "recmap.h"

#include <stdlib.h>

typedef struct RDB_index RDB_index;
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_cursor RDB_cursor;

enum {
    RDB_REC_DUP = 1,
    RDB_REC_RANGE = 2
};

int
RDB_recmap_cursor(RDB_cursor **, RDB_recmap *, RDB_bool wr, RDB_rec_transaction *);

int
RDB_index_cursor(RDB_cursor **, RDB_index *, RDB_bool wr, RDB_rec_transaction *);

int
RDB_cursor_get(RDB_cursor *, int fno, void **datapp, size_t *);

int
RDB_cursor_set(RDB_cursor *, int fieldc, RDB_field[]);

int
RDB_cursor_delete(RDB_cursor *);

int
RDB_cursor_update(RDB_cursor *, int fieldc, const RDB_field[]);

int
RDB_cursor_first(RDB_cursor *);

int
RDB_cursor_next(RDB_cursor *, int flags);

int
RDB_cursor_prev(RDB_cursor *);

int
RDB_cursor_seek(RDB_cursor *, int fieldc, RDB_field[], int);

int
RDB_destroy_cursor(RDB_cursor *);

#endif
