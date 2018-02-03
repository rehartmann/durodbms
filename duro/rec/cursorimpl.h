/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_CURSORIMPL_H_
#define REC_CURSORIMPL_H_

#include "cursor.h"

#include <db.h>

#ifdef POSTGRESQL
#include <libpq-fe.h>
#endif

typedef struct RDB_exec_context RDB_exec_context;

typedef struct RDB_cursor {
    /* internal */
    union {
        struct {
            DBC *cursorp;
            DBT current_key;
            DBT current_data;
        } bdb;
        struct {
            int id;
#ifdef POSTGRESQL
            PGresult *current_row;
#endif
        } pg;
    } cur;
    RDB_recmap *recmapp;
    RDB_index *idxp;
    RDB_rec_transaction *tx;
    RDB_bool secondary;

    int (*destroy_fn)(struct RDB_cursor *, RDB_exec_context *);
    int (*get_fn)(struct RDB_cursor *, int, void**, size_t *, RDB_exec_context *);
    int (*set_fn)(struct RDB_cursor *, int, RDB_field[], RDB_exec_context *);
    int (*delete_fn)(struct RDB_cursor *, RDB_exec_context *);
    int (*first_fn)(struct RDB_cursor *, RDB_exec_context *);
    int (*next_fn)(struct RDB_cursor *, int, RDB_exec_context *);
    int (*prev_fn)(struct RDB_cursor *, RDB_exec_context *);
} RDB_cursor;

#endif /* REC_CURSORIMPL_H_ */
