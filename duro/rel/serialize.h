#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/* $Id$ */

#include "rdb.h"

int
_RDB_vtable_to_value(RDB_table *, RDB_value *);

int
_RDB_expr_to_value(const RDB_expression *, RDB_value *);

int
_RDB_deserialize_vtable(RDB_value *valp, RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_deserialize_expr(RDB_value *valp, RDB_transaction *txp,
                      RDB_expression **expp);

#endif
