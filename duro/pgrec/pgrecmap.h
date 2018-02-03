/*
 * Record map functions implemented using PostgreSQL
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef PGREC_PGRECMAP_H_
#define PGREC_PGRECMAP_H_

#include <rec/recmap.h>

typedef RDB_exec_context RDB_exec_context;

RDB_recmap *
RDB_create_pg_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[], int,
        const RDB_compare_field[], int, RDB_rec_transaction *, RDB_exec_context *);

RDB_recmap *
RDB_open_pg_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[], int,
        RDB_rec_transaction *, RDB_exec_context *);

int
RDB_close_pg_recmap(RDB_recmap *, RDB_exec_context *);

int
RDB_delete_pg_recmap(RDB_recmap *, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_insert_pg_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_update_pg_rec(RDB_recmap *, RDB_field[],
               int, const RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_delete_pg_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_get_pg_fields(RDB_recmap *, RDB_field[],
           int, RDB_rec_transaction *, RDB_field[], RDB_exec_context *);

int
RDB_contains_pg_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_pg_recmap_est_size(RDB_recmap *, RDB_rec_transaction *, unsigned *, RDB_exec_context *);

void *
RDB_field_to_pg(RDB_field *, RDB_field_info *, int *, RDB_exec_context *);

#endif /* PGREC_PGRECMAP_H_ */
