#ifndef IOOP_H_
#define IOOP_H_

/*
 * Definitions for I/O operators.
 *
 * Copyright (C) 2009 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>
#include <obj/opmap.h>

extern RDB_object *Duro_stdin_objp;
extern RDB_object *Duro_stdout_objp;
extern RDB_object *Duro_stderr_objp;

int
RDB_add_io_ops(RDB_op_map *, RDB_exec_context *);

#endif /*IOOP_H_*/
