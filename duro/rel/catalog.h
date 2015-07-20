#ifndef RDB_CATALOG_H
#define RDB_CATALOG_H

/*
 * Copyright (C) 2003-2008, 2012-2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"

int
RDB_cat_insert(RDB_object *, RDB_exec_context *, RDB_transaction *);

int
RDB_cat_insert_ptable(const char *,
        int, const RDB_attr[],
        int, const RDB_string_vec[],
        RDB_exec_context *, RDB_transaction *);

int
RDB_cat_delete(RDB_object *, RDB_exec_context *, RDB_transaction *);

int
RDB_cat_delete_ptable(const char *, RDB_exec_context *, RDB_transaction *);

int
RDB_open_systables(RDB_dbroot *, RDB_exec_context *, RDB_transaction *);

int
RDB_cat_create_db(RDB_exec_context *, RDB_transaction *);

int
RDB_possreps_query(const char *name, RDB_exec_context *, RDB_transaction *,
        RDB_object **tbpp);

int
RDB_cat_get_table(RDB_object *, const char *, RDB_exec_context *, RDB_transaction *);

RDB_object *
RDB_cat_get_ptable_vt(const char *, RDB_exec_context *, RDB_transaction *);

int
RDB_cat_map_ptable(const char *, RDB_expression *,
        RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_cat_get_table_type(const char *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_cat_get_keys(const char *, RDB_exec_context *, RDB_transaction *,
         int *, RDB_string_vec **);

int
RDB_cat_rename_table(RDB_object *, const char *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_cat_dbtables_insert(RDB_object *, RDB_database *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_cat_index_tablename(const char *, RDB_object *tbnamep, RDB_exec_context *,
        RDB_transaction *);

int
RDB_cat_delete_index(const char *, RDB_exec_context *, RDB_transaction *);

int
RDB_cat_create_constraint(const char *name, RDB_expression *exp,
                      RDB_exec_context *, RDB_transaction *);

int
RDB_cat_db_exists(const char *, RDB_dbroot *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_cat_major_version(void);

int
RDB_cat_minor_version(void);

#endif
