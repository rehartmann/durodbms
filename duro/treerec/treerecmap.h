/*
 * Record map functions implemented using Berkeley DB
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef TREEREC_AVLRECMAP_H_
#define TREEREC_AVLRECMAP_H_

#include <rec/recmap.h>

#include <db.h>

typedef RDB_exec_context RDB_exec_context;

RDB_recmap *
RDB_create_tree_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[], int,
        int, const RDB_compare_field[], int,
        int, const RDB_string_vec *,
        RDB_rec_transaction *, RDB_exec_context *);

RDB_recmap *
RDB_open_tree_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[], int,
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

#endif /* TREEREC_AVLRECMAP_H_ */
