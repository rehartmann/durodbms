#ifndef RDB_CATALOG_H
#define RDB_CATALOG_H

/* $Id$ */

#include "rdb.h"
#include "internal.h"

int
_RDB_dbtables_insert(RDB_table *, RDB_transaction *);

int
_RDB_catalog_insert(RDB_table *, RDB_transaction *);

int
_RDB_open_systables(RDB_transaction *);

int
_RDB_create_db_in_cat(RDB_transaction *txp);

int
_RDB_possreps_query(const char *name, RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_possrepcomps_query(const char *name, const char *possrepname,
        RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_get_cat_rtable(const char *name, RDB_transaction *, RDB_table **tbpp);

int
_RDB_get_cat_vtable(const char *name, RDB_transaction *, RDB_table **tbpp);

int
_RDB_get_cat_type(const char *name, RDB_transaction *, RDB_type **typp);

char *
_RDB_make_typestr(int argc, RDB_type *argtv[]);

int
_RDB_get_cat_ro_op(const char *name, int argc, RDB_object *argv[],
        RDB_transaction *txp, RDB_ro_op **opp);

int
_RDB_get_cat_upd_op(const char *name, int argc, RDB_object *argv[],
        RDB_transaction *txp, RDB_upd_op **opp);

#endif
