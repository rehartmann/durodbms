#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/* $Id$ */

#include "rdb.h"

int
_RDB_table_to_obj(RDB_table *, RDB_object *);

int
_RDB_expr_to_obj(const RDB_expression *, RDB_object *);

int
_RDB_deserialize_table(RDB_object *valp, RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_deserialize_expr(RDB_object *valp, RDB_transaction *txp,
                      RDB_expression **expp);

#endif
