/*
 * Declarations of catalog functions dealing with physical tables and indexes.
 *
 * Copyright (C) 2005, 2012-2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef CAT_STORED_H_
#define CAT_STORED_H_

#include "rdb.h"
#include "internal.h"

int
RDB_cat_insert_index(const char *name, int attrc, const RDB_seq_item attrv[],
        RDB_bool unique, RDB_bool ordered, const char *tbname,
        RDB_exec_context *, RDB_transaction *);

int
RDB_cat_get_indexes(const char *tablename, RDB_dbroot *dbrootp,
        RDB_exec_context *, RDB_transaction *, struct RDB_tbindex **);

int
RDB_cat_insert_table_recmap(RDB_object *tbp, const char *rmname,
        RDB_exec_context *, RDB_transaction *txp);

int
RDB_cat_recmap_name(RDB_object *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_string_to_id(RDB_object *id, const char *str, RDB_exec_context *ecp);

RDB_expression *
RDB_tablename_id_eq_expr(const char *name, RDB_exec_context *ecp);

#endif /* CAT_STORED_H_ */
