#ifndef RDB_CATALOG_H
#define RDB_CATALOG_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"

int
_RDB_cat_insert(RDB_object *, RDB_exec_context *, RDB_transaction *);

int
_RDB_cat_delete(RDB_object *, RDB_exec_context *, RDB_transaction *);

int
_RDB_open_systables(RDB_dbroot *, RDB_exec_context *, RDB_transaction *);

int
_RDB_cat_create_db(RDB_exec_context *, RDB_transaction *);

int
_RDB_possreps_query(const char *name, RDB_exec_context *, RDB_transaction *,
        RDB_object **tbpp);

int
_RDB_possrepcomps_query(const char *name, const char *possrepname,
        RDB_exec_context *, RDB_transaction *txp, RDB_object **tbpp);

RDB_object *
_RDB_cat_get_rtable(const char *, RDB_exec_context *, RDB_transaction *);

RDB_object *
_RDB_cat_get_vtable(const char *, RDB_exec_context *, RDB_transaction *);

int
_RDB_cat_rename_table(RDB_object *, const char *, RDB_exec_context *,
        RDB_transaction *);

int
_RDB_cat_get_type(const char *name, RDB_exec_context *, RDB_transaction *,
        RDB_type **typp);

int
_RDB_make_typesobj(int argc, RDB_type *argtv[], RDB_exec_context *,
        RDB_object *objp);

int
_RDB_cat_get_ro_op(const char *name, int argc, RDB_type *argtv[],
        RDB_exec_context *, RDB_transaction *txp, RDB_ro_op_desc **opp);

RDB_upd_op_data *
_RDB_cat_get_upd_op(const char *name, int argc, RDB_type *argtv[],
        RDB_exec_context *, RDB_transaction *);

int
_RDB_cat_insert_table_recmap(RDB_object *tbp, const char *rmname,
        RDB_exec_context *, RDB_transaction *txp);

int
_RDB_cat_insert_index(const char *name, int attrc, const RDB_seq_item attrv[],
        RDB_bool unique, RDB_bool ordered, const char *tbname,
        RDB_exec_context *, RDB_transaction *);

int
_RDB_cat_index_tablename(const char *, char **tbnamep, RDB_exec_context *,
        RDB_transaction *);

int
_RDB_cat_delete_index(const char *, RDB_exec_context *, RDB_transaction *);

int
_RDB_cat_get_indexes(const char *tablename, RDB_dbroot *dbrootp,
        RDB_exec_context *, RDB_transaction *, struct _RDB_tbindex **);

int
_RDB_cat_create_constraint(const char *name, RDB_expression *exp,
                      RDB_exec_context *, RDB_transaction *);

#endif
