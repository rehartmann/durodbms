#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/* $Id$ */

#include "rdb.h"

int
_RDB_table_to_obj(RDB_object *, RDB_table *);

int
_RDB_expr_to_obj(RDB_object *, const RDB_expression *);

int
_RDB_type_to_obj(RDB_object *, const RDB_type *);

int
_RDB_deserialize_table(RDB_object *valp, RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_deserialize_expr(RDB_object *valp, RDB_transaction *txp,
                      RDB_expression **expp);

int
_RDB_deserialize_type(RDB_object *valp, RDB_transaction *txp,
                 RDB_type **typp);

#endif
