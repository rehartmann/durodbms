/*
 * Interpreter evaluation functions.
 *
 * Copyright (C) 2014-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef INTERP_EVAL_H_
#define INTERP_EVAL_H_

#include <rel/rdb.h>

typedef struct Duro_interp Duro_interp;

int
Duro_evaluate(RDB_expression *, Duro_interp *, RDB_exec_context *,
        RDB_object *);

int
Duro_evaluate_retry(RDB_expression *, Duro_interp *, RDB_exec_context *,
        RDB_object *);

RDB_type *
Duro_expr_type_retry(RDB_expression *, Duro_interp *, RDB_exec_context *);

#endif /* INTERP_EVAL_H_ */
