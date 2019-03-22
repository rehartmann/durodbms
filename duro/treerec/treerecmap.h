/*
 * Record map functions implemented using Berkeley DB
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef TREEREC_AVLRECMAP_H_
#define TREEREC_AVLRECMAP_H_

#include <rec/recmap.h>

typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_tree_node RDB_tree_node;
typedef struct RDB_index RDB_index;

RDB_recmap *
RDB_create_tree_recmap(int, const RDB_field_info[], int,
        int, const RDB_compare_field[], int,
        int, const RDB_string_vec *,
        RDB_rec_transaction *, RDB_exec_context *);

int
RDB_close_tree_recmap(RDB_recmap *, RDB_exec_context *);

int
RDB_delete_tree_recmap(RDB_recmap *, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_insert_tree_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_update_tree_rec(RDB_recmap *, RDB_field[],
               int, const RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_delete_tree_rec(RDB_recmap *, int, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_get_tree_fields(RDB_recmap *, RDB_field[],
           int, RDB_rec_transaction *, RDB_field[], RDB_exec_context *);

int
RDB_contains_tree_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_tree_recmap_est_size(RDB_recmap *, RDB_rec_transaction *, unsigned *, RDB_exec_context *);

int
RDB_delete_from_tree_indexes(RDB_recmap *, RDB_tree_node *, RDB_exec_context *);

RDB_bool
RDB_recmap_is_key_update(RDB_recmap *, int, const RDB_field[]);

int
RDB_make_skey(RDB_index *, void *, size_t, void *, size_t, void **, size_t *);

#endif /* TREEREC_AVLRECMAP_H_ */
