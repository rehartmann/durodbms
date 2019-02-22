/*
 * Index internals
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_INDEXIMPL_H_
#define REC_INDEXIMPL_H_

#include "index.h"
#include "recmap.h"
#include <treerec/tree.h>

#ifdef BERKELEYDB
#include <db.h>
#endif

typedef struct RDB_cursor RDB_cursor;

typedef struct RDB_index {
    RDB_recmap *rmp;
    union {
#ifdef BERKELEYDB
        DB *dbp;
#endif
        struct {
            RDB_binary_tree *treep;
        } tree;
    } impl;
    struct RDB_index *nextp;
    char *namp;
    char *filenamp;
    int fieldc;
    int *fieldv;
    /* For ordered indexes */
    RDB_compare_field *cmpv;

    int (*close_index_fn)(RDB_index *, RDB_exec_context *);
    int (*delete_index_fn)(RDB_index *, RDB_rec_transaction *,
            RDB_exec_context *);
    int (*index_get_fields_fn)(RDB_index *, RDB_field[], int, RDB_rec_transaction *,
               RDB_field[], RDB_exec_context *);
    int (*index_delete_rec_fn)(RDB_index *, RDB_field[], RDB_rec_transaction *,
            RDB_exec_context *);
    RDB_cursor * (*index_cursor_fn)(RDB_index *, RDB_bool, RDB_rec_transaction *,
            RDB_exec_context *);
} RDB_index;

#endif /* REC_INDEXIMPL_H_ */
