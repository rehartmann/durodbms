/*
 * Definitions for variable assignment and similar functionalities
 *
 * Copyright (C) 2012, 2014-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef INTERP_ASSIGN_H_
#define INTERP_ASSIGN_H_

#include <rel/rdb.h>
#include "iinterp.h"
#include "parse.h"

int
Duro_exec_assign(const RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_load(RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_explain_assign(const RDB_parse_node *, Duro_interp *, RDB_exec_context *);

#endif /* INTERP_ASSIGN_H_ */
