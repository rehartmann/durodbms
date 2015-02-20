/*
 * Interpreter evaluation functions.
 *
 *  Created on: 15.03.2014
 *      Author: Rene Hartmann
 */

#ifndef INTERP_EVAL_H_
#define INTERP_EVAL_H_

#include <rel/rdb.h>

typedef struct Duro_interp Duro_interp;

int
Duro_evaluate_retry(RDB_expression *, Duro_interp *, RDB_exec_context *,
        RDB_object *);

RDB_type *
Duro_expr_type_retry(RDB_expression *, Duro_interp *, RDB_exec_context *);

#endif /* INTERP_EVAL_H_ */
