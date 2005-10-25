#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/* $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"

int
_RDB_table_to_obj(RDB_object *, RDB_table *, RDB_exec_context *);

int
_RDB_expr_to_obj(RDB_object *, const RDB_expression *, RDB_exec_context *);

int
_RDB_type_to_obj(RDB_object *, const RDB_type *, RDB_exec_context *);

int
_RDB_deserialize_table(RDB_object *valp, RDB_exec_context *, RDB_transaction *,
        RDB_table **tbpp);

int
_RDB_deserialize_expr(RDB_object *valp, RDB_exec_context *, RDB_transaction *,
                      RDB_expression **expp);

int
_RDB_deserialize_type(RDB_object *valp, RDB_exec_context *, RDB_transaction *,
                 RDB_type **typp);

#endif
