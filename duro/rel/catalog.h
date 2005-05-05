#ifndef RDB_CATALOG_H
#define RDB_CATALOG_H

/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"

int
_RDB_cat_insert(RDB_table *, RDB_transaction *);

int
_RDB_cat_delete(RDB_table *, RDB_transaction *);

int
_RDB_open_systables(RDB_dbroot *, RDB_transaction *);

int
_RDB_cat_create_db(RDB_transaction *);

int
_RDB_possreps_query(const char *name, RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_possrepcomps_query(const char *name, const char *possrepname,
        RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_cat_get_rtable(const char *, RDB_transaction *, RDB_table **tbpp);

int
_RDB_cat_get_vtable(const char *, RDB_transaction *, RDB_table **tbpp);

int
_RDB_cat_rename_table(RDB_table *, const char *, RDB_transaction *);

int
_RDB_cat_get_type(const char *name, RDB_transaction *, RDB_type **typp);

int
_RDB_make_typesobj(int argc, RDB_type *argtv[], RDB_object *objp);

int
_RDB_cat_get_ro_op(const char *name, int argc, RDB_type *argtv[],
        RDB_transaction *txp, RDB_ro_op_desc **opp);

int
_RDB_cat_get_upd_op(const char *name, int argc, RDB_type *argtv[],
        RDB_transaction *txp, RDB_upd_op **opp);

int
_RDB_cat_insert_table_recmap(RDB_table *tbp, const char *rmname,
        RDB_transaction *txp);

int
_RDB_cat_insert_index(_RDB_tbindex *, const char *tbname, RDB_transaction *);

int
_RDB_cat_index_tablename(const char *, char **tbnamep, RDB_transaction *);

int
_RDB_cat_delete_index(const char *, RDB_transaction *);

int
_RDB_cat_get_indexes(const char *tablename, RDB_dbroot *dbrootp,
        RDB_transaction *, _RDB_tbindex **);

int
_RDB_cat_create_constraint(const char *name, RDB_expression *exp,
                      RDB_transaction *);

#endif
