/*
 * Built-in operators for type datetime.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef DATETIMEOPS_H_
#define DATETIMEOPS_H_

#include "opmap.h"
#include "excontext.h"

int
RDB_add_datetime_ro_ops(RDB_op_map *, RDB_exec_context *);

int
RDB_add_datetime_upd_ops(RDB_op_map *, RDB_exec_context *);

#endif /* DATETIMEOPS_H_ */
