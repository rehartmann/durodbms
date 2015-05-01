/*
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef BUILTINSCOPS_H_
#define BUILTINSCOPS_H_

#include "opmap.h"
#include "excontext.h"
#include "object.h"

typedef struct RDB_transaction RDB_transaction;

int
RDB_eq_bool(int, RDB_object *[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_eq_binary(int, RDB_object *[], RDB_operator *, RDB_exec_context *,
        RDB_transaction *, RDB_object *);

int
RDB_add_builtin_scalar_ro_ops(RDB_op_map *, RDB_exec_context *);

#endif /* BUILTINSCOPS_H_ */
