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
_RDB_create_db_in_cat(RDB_transaction *);

int
_RDB_possreps_query(const char *name, RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_possrepcomps_query(const char *name, const char *possrepname,
        RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_get_cat_rtable(const char *, RDB_transaction *, RDB_table **tbpp);

int
_RDB_get_cat_vtable(const char *, RDB_transaction *, RDB_table **tbpp);

int
_RDB_cat_rename_table(RDB_table *, const char *, RDB_transaction *);

int
_RDB_get_cat_type(const char *name, RDB_transaction *, RDB_type **typp);

int
_RDB_get_cat_rtype(const char *opname, RDB_transaction *, RDB_type **typp);

int
_RDB_make_typesobj(int argc, RDB_type *argtv[], RDB_object *objp);

int
_RDB_get_cat_ro_op(const char *name, int argc, RDB_type *argtv[],
        RDB_transaction *txp, RDB_ro_op **opp);

int
_RDB_get_cat_upd_op(const char *name, int argc, RDB_type *argtv[],
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
_RDB_get_indexes(RDB_table *tbp, RDB_dbroot *dbrootp, RDB_transaction *);

#endif
