#ifndef TRANSFORM_H
#define TRANSFORM_H

/*
 * $Id$
 *
 * Copyright (C) 2006 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"

int
RDB_transform(RDB_expression *, RDB_gettypefn *, void *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_remove_to_project(RDB_expression *, RDB_gettypefn *, void *,
        RDB_exec_context *, RDB_transaction *);

#endif /*TRANSFORM_H*/
