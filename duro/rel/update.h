#ifndef UPDATE_H
#define UPDATE_H

/*
 * Copyright (C) 2006, 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Declares internal functions for updating tables.
 */

#include "rdb.h"

RDB_int
RDB_update_nonvirtual(RDB_object *, RDB_expression *,
        int, const RDB_attr_update[],
        RDB_getobjfn *, void *,
        RDB_exec_context *, RDB_transaction *);

RDB_int
RDB_update_where_index(RDB_expression *, RDB_expression *,
        int, const RDB_attr_update [],
        RDB_getobjfn *, void *,
        RDB_exec_context *, RDB_transaction *);

#endif /*UPDATE_H*/
