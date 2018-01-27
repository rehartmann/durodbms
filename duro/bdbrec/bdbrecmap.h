/*
 * Record map functions implemented using Berkeley DB
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef BDBREC_BDBRECMAP_H_
#define BDBREC_BDBRECMAP_H_

#include <rec/recmap.h>

typedef RDB_exec_context RDB_exec_context;

RDB_recmap *
RDB_create_bdb_recmap(const char *, const char *,
        RDB_environment *, int, const int[], int,
        const RDB_compare_field[], int, RDB_rec_transaction *, RDB_exec_context *);

RDB_recmap *
RDB_open_bdb_recmap(const char *, const char *,
        RDB_environment *, int, const int[], int,
        RDB_rec_transaction *, RDB_exec_context *);

int
RDB_close_bdb_recmap(RDB_recmap *, RDB_exec_context *);

int
RDB_delete_bdb_recmap(RDB_recmap *, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_insert_bdb_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_update_bdb_rec(RDB_recmap *, RDB_field[],
               int, const RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_delete_bdb_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_get_bdb_fields(RDB_recmap *, RDB_field[],
           int, RDB_rec_transaction *, RDB_field[], RDB_exec_context *);

int
RDB_contains_bdb_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_bdb_recmap_est_size(RDB_recmap *, RDB_rec_transaction *, unsigned *, RDB_exec_context *);

#endif /* BDBREC_BDBRECMAP_H_ */
