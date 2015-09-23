#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/*
 * Copyright (C) 2003-2007, 2012, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"

int
RDB_expr_to_bin(RDB_object *, RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_bin_to_expr(const RDB_object *, RDB_exec_context *, RDB_transaction *);

int
RDB_type_to_bin(RDB_object *, const RDB_type *, RDB_exec_context *);

RDB_type *
RDB_bin_to_type(const RDB_object *, RDB_exec_context *, RDB_transaction *);

int
RDB_serialize_expr(RDB_object *, int *, RDB_expression *,
        RDB_exec_context *);

int
RDB_serialize_scalar_type(RDB_object *valp, int *posp, const char *,
        RDB_exec_context *);

int
RDB_deserialize_expr(const RDB_object *, int *, RDB_exec_context *,
        RDB_transaction *, RDB_expression **);

#endif
