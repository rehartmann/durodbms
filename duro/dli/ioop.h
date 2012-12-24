#ifndef IOOP_H_
#define IOOP_H_

/*
 * $Id$
 *
 * Copyright (C) 2009 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>
#include <rel/opmap.h>

RDB_EXTERN_VAR RDB_object DURO_STDIN_OBJ;
RDB_EXTERN_VAR RDB_object DURO_STDOUT_OBJ;
RDB_EXTERN_VAR RDB_object DURO_STDERR_OBJ;

int
RDB_add_io_ops(RDB_op_map *, RDB_exec_context *);

#endif /*IOOP_H_*/
