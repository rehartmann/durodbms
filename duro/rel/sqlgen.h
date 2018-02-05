/*
 * SQL generation functions
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REL_SQLGEN_H_
#define REL_SQLGEN_H_

#include <gen/types.h>

typedef struct RDB_expression RDB_expression;
typedef struct RDB_object RDB_object;
typedef struct RDB_exec_context RDB_exec_context;

RDB_bool
RDB_sql_convertible(RDB_expression *);

int
RDB_expr_to_sql(RDB_object *, RDB_expression *, RDB_exec_context *);

#endif /* REL_SQLGEN_H_ */
