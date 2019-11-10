#ifndef RDB_RECMAPIMPL_H
#define RDB_RECMAPIMPL_H

/*
 * Record map internals
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "recmap.h"
#include "env.h"

#ifdef BERKELEYDB
#include "db.h"
#endif

#include <treerec/tree.h>

typedef struct RDB_cursor RDB_cursor;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_index RDB_index;

typedef struct RDB_recmap {
    /* internal */
    RDB_environment *envp;
    union {
#ifdef BERKELEYDB
        DB *dbp;
#endif
        struct {
            RDB_binary_tree *treep;
        } tree;
    } impl;
    RDB_index *indexes;
    char *namp;
    char *filenamp;
    int fieldcount; /* # of fields total */
    int keyfieldcount;  /* # of primary index fields */

    /*
     * Information about the fields.
     * len contains the length of each field, if it's fixed-length field,
     * RDB_VARIABLE_LEN if it's a variable-length field
     */
    RDB_field_info *fieldinfos;

    int varkeyfieldcount; /* # of variable-length key fields */
    int vardatafieldcount; /* # of variable-length nonkey fields */

    /* For sorted recmaps */
    int cmpc;
    RDB_compare_field *cmpv;

    /* RDB_TRUE if duplicate keys are allowed */
    RDB_bool dup_keys;

    /* RDB_TRUE if deletion is only possible after commit */
    RDB_bool delayed_deletion;

    int (*close_recmap_fn)(RDB_recmap *, RDB_exec_context *);
    int (*delete_recmap_fn)(RDB_recmap *, RDB_rec_transaction *, RDB_exec_context *);
    int (*insert_rec_fn)(RDB_recmap *, RDB_field[], RDB_rec_transaction *,
            RDB_exec_context *);
    int (*update_rec_fn)(RDB_recmap *, RDB_field[],
                   int, const RDB_field[], RDB_rec_transaction *, RDB_exec_context *);
    int (*delete_rec_fn)(RDB_recmap *, int, RDB_field[], RDB_rec_transaction *,
            RDB_exec_context *);
    int (*get_fields_fn)(RDB_recmap *, RDB_field[],
               int, RDB_rec_transaction *, RDB_field[], RDB_exec_context *);
    int (*contains_rec_fn)(RDB_recmap *, RDB_field[], RDB_rec_transaction *,
            RDB_exec_context *);
    int (*recmap_est_size_fn)(RDB_recmap *, RDB_rec_transaction *, unsigned *,
            RDB_exec_context *);
    RDB_cursor * (*cursor_fn)(RDB_recmap *, RDB_bool, RDB_rec_transaction *,
            RDB_exec_context *);
    RDB_index *(*create_index_fn)(RDB_recmap *, const char *,
            RDB_environment *, int, const RDB_field_descriptor[],
            const RDB_compare_field[], int, RDB_rec_transaction *,
            RDB_exec_context *);
    RDB_index *(*open_index_fn)(RDB_recmap *, const char *,
            RDB_environment *, int, const int[],
            const RDB_compare_field[], int, RDB_rec_transaction *,
            RDB_exec_context *);
} RDB_recmap;

RDB_recmap *
RDB_new_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[],
        int, int, RDB_exec_context *);

#endif
