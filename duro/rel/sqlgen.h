/*
 * SQL generation functions
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REL_SQLGEN_H_
#define REL_SQLGEN_H_

#include "rdb.h"

RDB_bool
RDB_sql_convertible(RDB_expression *);

RDB_bool
RDB_nontable_sql_convertible(RDB_expression *, RDB_gettypefn *getfnp, void *getarg);

int
RDB_expr_to_sql(RDB_object *, RDB_expression *, RDB_environment *, RDB_exec_context *);

int
RDB_expr_to_sql_select(RDB_object *, RDB_expression *, int, const RDB_seq_item[],
        RDB_environment *, RDB_exec_context *);

#endif /* REL_SQLGEN_H_ */
