/*
 * Record map functions implemented using Berkeley DB
 *
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef FDBREC_FDBRECMAP_H_
#define FDBREC_FDBRECMAP_H_

#include <rec/recmap.h>

typedef RDB_exec_context RDB_exec_context;

RDB_recmap *
RDB_create_fdb_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[], int,
        int, const RDB_compare_field[], int,
        int, const RDB_string_vec *,
        RDB_rec_transaction *, RDB_exec_context *);

RDB_recmap *
RDB_open_fdb_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[], int,
        RDB_rec_transaction *, RDB_exec_context *);

int
RDB_close_fdb_recmap(RDB_recmap *, RDB_exec_context *);

int
RDB_delete_fdb_recmap(RDB_recmap *, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_insert_fdb_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_update_fdb_rec(RDB_recmap *, RDB_field[],
               int, const RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_delete_fdb_rec(RDB_recmap *, int, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_get_fdb_fields(RDB_recmap *, RDB_field[],
           int, RDB_rec_transaction *, RDB_field[], RDB_exec_context *);

int
RDB_contains_fdb_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_fdb_recmap_est_size(RDB_recmap *, RDB_rec_transaction *, unsigned *, RDB_exec_context *);

int
RDB_fdb_key_prefix_length(RDB_recmap *);

#endif /* FDBREC_FDBRECMAP_H_ */
