/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_CURSORIMPL_H_
#define REC_CURSORIMPL_H_

#include "cursor.h"

#include <db.h>

typedef struct RDB_cursor {
    /* internal */
    DBC *cursorp;
    DBT current_key;
    DBT current_data;
    RDB_recmap *recmapp;
    RDB_index *idxp;
    RDB_rec_transaction *tx;

    int (*destroy_fn)(struct RDB_cursor *);
    int (*get_fn)(struct RDB_cursor *, int, void**, size_t *);
    int (*set_fn)(struct RDB_cursor *, int, RDB_field[]);
    int (*delete_fn)(struct RDB_cursor *);
    int (*first_fn)(struct RDB_cursor *);
    int (*next_fn)(struct RDB_cursor *, int);
    int (*prev_fn)(struct RDB_cursor *);
    int (*seek_fn)(struct RDB_cursor *, int, RDB_field[], int);
} RDB_cursor;

#endif /* REC_CURSORIMPL_H_ */
