/*
 * Record map functions implemented using Berkeley DB
 *
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef FDBREC_FDBRECMAP_H_
#define FDBREC_FDBRECMAP_H_

#include <rec/recmap.h>
#include <rec/index.h>
#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

typedef RDB_exec_context RDB_exec_context;

extern FDBFuture *RDB_fdb_resultf;
extern uint8_t *RDB_fdb_key_name;

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

int
RDB_fdb_key_index_prefix_length(RDB_index *);

uint8_t *
RDB_fdb_prepend_key_prefix(RDB_recmap *, const void *, size_t, RDB_exec_context *);

uint8_t *
RDB_fdb_prepend_key_index_prefix(RDB_index *, const void *, size_t, RDB_exec_context *);

int
RDB_delete_from_fdb_indexes(RDB_recmap *, uint8_t *, int,
    uint8_t *, int, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_update_fdb_kv(RDB_recmap *, uint8_t *, int,
        void **, size_t *, void **, size_t *,
        int, const RDB_field[],
        RDB_rec_transaction *, RDB_exec_context *);

#endif /* FDBREC_FDBRECMAP_H_ */
