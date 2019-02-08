/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_CURSORIMPL_H_
#define REC_CURSORIMPL_H_

#include "cursor.h"

#ifdef BERKELEYDB
#include <db.h>
#endif

#include <treerec/tree.h>

#ifdef POSTGRESQL
#include <libpq-fe.h>
#endif

typedef struct RDB_exec_context RDB_exec_context;

typedef struct RDB_cursor {
    /* internal */
    union {
#ifdef BERKELEYDB
        struct {
            DBC *cursorp;
            DBT current_key;
            DBT current_data;
        } bdb;
#endif
#ifdef POSTGRESQL
        struct {
            unsigned id;
            PGresult *current_row;
        } pg;
#endif
#ifdef FOUNDATIONDB
        struct {
			uint8_t *key;
			int key_length;
			uint8_t *value;
			int value_length;
		} fdb;
#endif
		struct {
            RDB_binary_tree *treep;
            RDB_tree_node *nodep;
        } tree;
    } cur;
    RDB_recmap *recmapp;
    RDB_environment *envp;
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
    int (*seek_fn)(struct RDB_cursor *, int, RDB_field[], int, RDB_exec_context *);
} RDB_cursor;

#endif /* REC_CURSORIMPL_H_ */
