#ifndef IOOP_H_
#define IOOP_H_

/*
 * $Id$
 *
 * Copyright (C) 2009 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>
#include <rel/opmap.h>

_RDB_EXTERN_VAR RDB_object DURO_STDIN_OBJ;
_RDB_EXTERN_VAR RDB_object DURO_STDOUT_OBJ;
_RDB_EXTERN_VAR RDB_object DURO_STDERR_OBJ;

int
_RDB_add_io_ops(RDB_op_map *, RDB_exec_context *);

#endif /*IOOP_H_*/
