#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/* $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"

int
_RDB_expr_to_binobj(RDB_object *, RDB_expression *, RDB_exec_context *);

int
_RDB_type_to_binobj(RDB_object *, const RDB_type *, RDB_exec_context *);

RDB_expression *
_RDB_binobj_to_expr(RDB_object *, RDB_exec_context *, RDB_transaction *);

RDB_object *
_RDB_binobj_to_vtable(RDB_object *, RDB_exec_context *, RDB_transaction *);

RDB_type *
_RDB_binobj_to_type(RDB_object *, RDB_exec_context *, RDB_transaction *);

int
_RDB_serialize_byte(RDB_object *, int *, RDB_byte,
        RDB_exec_context *);

int
_RDB_serialize_str(RDB_object *, int *, const char *, RDB_exec_context *);

int
_RDB_serialize_int(RDB_object *, int *, RDB_int, RDB_exec_context *);

int
_RDB_serialize_expr(RDB_object *, int *, RDB_expression *,
        RDB_exec_context *);

int
_RDB_serialize_type(RDB_object *, int *, const RDB_type *,
        RDB_exec_context *);

int
_RDB_deserialize_byte(RDB_object *, int *, RDB_exec_context *);

int
_RDB_deserialize_str(RDB_object *, int *, RDB_exec_context *, char **);

int
_RDB_deserialize_strobj(RDB_object *, int *, RDB_exec_context *, RDB_object *);

int
_RDB_deserialize_int(RDB_object *, int *, RDB_exec_context *, RDB_int *);

int
_RDB_deserialize_expr(RDB_object *, int *, RDB_exec_context *,
        RDB_transaction *, RDB_expression **);

RDB_type *
_RDB_deserialize_type(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp);

#endif
