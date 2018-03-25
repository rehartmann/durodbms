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
#include <db.h>

typedef struct RDB_index {
    RDB_recmap *rmp;
    union {
        DB *dbp;
        struct {
            RDB_binary_tree *treep;
            struct RDB_index *nextp;
        } tree;
    } impl;
    char *namp;
    char *filenamp;
    int fieldc;
    int *fieldv;
    /* For ordered indexes */
    RDB_compare_field *cmpv;

    int (*close_index_fn)(RDB_index *, RDB_exec_context *);
    int (*delete_index_fn)(RDB_index *, RDB_rec_transaction *,
            RDB_exec_context *);
    RDB_bool (*index_is_ordered_fn)(const RDB_index *);
    int (*index_get_fields_fn)(RDB_index *, RDB_field[], int, RDB_rec_transaction *,
               RDB_field[], RDB_exec_context *);
    int (*index_delete_rec_fn)(RDB_index *, RDB_field[], RDB_rec_transaction *,
            RDB_exec_context *);
} RDB_index;

#endif /* REC_INDEXIMPL_H_ */
