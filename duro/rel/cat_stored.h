/*
 * Declarations of catalog functions dealing with physical tables and indexes.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef CAT_STORED_H_
#define CAT_STORED_H_

#include "rdb.h"

typedef struct RDB_dbroot RDB_dbroot;

struct RDB_tbindex;

int
RDB_cat_insert_index(const char *, int, const RDB_seq_item[],
        RDB_bool, RDB_bool, const char *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_cat_get_indexes(const char *, RDB_dbroot *,
        RDB_exec_context *, RDB_transaction *, struct RDB_tbindex **);

int
RDB_cat_insert_table_recmap(RDB_object *, const char *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_cat_recmap_name(RDB_object *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_string_to_id(RDB_object *, const char *, RDB_exec_context *);

RDB_expression *
RDB_tablename_id_eq_expr(const char *, RDB_exec_context *);

#endif /* CAT_STORED_H_ */
