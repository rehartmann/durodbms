/*
 * interp_assign.h
 *
 *  Created on: 07.12.2012
 *      Author: Rene Hartmann
 */

#ifndef INTERP_ASSIGN_H_
#define INTERP_ASSIGN_H_

#include <rel/rdb.h>
#include "iinterp.h"
#include "parse.h"

int
Duro_exec_assign(const RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_explain_assign(const RDB_parse_node *, Duro_interp *, RDB_exec_context *);

#endif /* INTERP_ASSIGN_H_ */
