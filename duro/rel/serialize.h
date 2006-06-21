#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/* $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"

int
_RDB_expr_to_binobj(RDB_object *, const RDB_expression *, RDB_exec_context *);

int
_RDB_type_to_binobj(RDB_object *, const RDB_type *, RDB_exec_context *);

RDB_expression *
_RDB_binobj_to_expr(RDB_object *valp, RDB_exec_context *, RDB_transaction *);

RDB_object *
_RDB_binobj_to_vtable(RDB_object *valp, RDB_exec_context *, RDB_transaction *);

RDB_type *
_RDB_binobj_to_type(RDB_object *valp, RDB_exec_context *, RDB_transaction *);

#endif
