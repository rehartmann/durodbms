#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/* $Id$ */

#include "rdb.h"

int
_RDB_vtable_to_value(RDB_table *, RDB_value *);

int
_RDB_expr_to_value(const RDB_expression *, RDB_value *);

int
_RDB_deserialize_table(RDB_value *valp, int *posp, RDB_transaction *txp,
                       RDB_table **tbpp);

#endif
