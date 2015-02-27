#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/*
 * Copyright (C) 2003-2007, 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"

int
RDB_expr_to_binobj(RDB_object *, RDB_expression *, RDB_exec_context *);

int
RDB_type_to_binobj(RDB_object *, const RDB_type *, RDB_exec_context *);

RDB_expression *
RDB_binobj_to_expr(RDB_object *, RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_binobj_to_type(RDB_object *, RDB_exec_context *, RDB_transaction *);

int
RDB_serialize_byte(RDB_object *, int *, RDB_byte,
        RDB_exec_context *);

int
RDB_serialize_str(RDB_object *, int *, const char *, RDB_exec_context *);

int
RDB_serialize_int(RDB_object *, int *, RDB_int, RDB_exec_context *);

int
RDB_serialize_expr(RDB_object *, int *, RDB_expression *,
        RDB_exec_context *);

int
RDB_serialize_scalar_type(RDB_object *valp, int *posp, const char *,
        RDB_exec_context *);

int
RDB_serialize_type(RDB_object *, int *, const RDB_type *,
        RDB_exec_context *);

int
RDB_deserialize_byte(RDB_object *, int *, RDB_exec_context *);

int
RDB_deserialize_str(RDB_object *, int *, RDB_exec_context *, char **);

int
RDB_deserialize_strobj(RDB_object *, int *, RDB_exec_context *, RDB_object *);

int
RDB_deserialize_int(RDB_object *, int *, RDB_exec_context *, RDB_int *);

int
RDB_deserialize_expr(RDB_object *, int *, RDB_exec_context *,
        RDB_transaction *, RDB_expression **);

RDB_type *
RDB_deserialize_type(RDB_object *valp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp);

#endif
