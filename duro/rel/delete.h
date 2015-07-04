#ifndef DELETE_H
#define DELETE_H

/*
 * Copyright (C) 2006, 2012, 2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Declares internal functions for deleting tuples from tables.
 */

#include "rdb.h"

RDB_int
RDB_delete_real(RDB_object *tbp, RDB_expression *condp,
        RDB_getobjfn *, void *,
        RDB_exec_context *, RDB_transaction *);

RDB_int
RDB_delete_where_index(RDB_expression *texp, RDB_expression *condp,
        RDB_getobjfn *, void *,
        RDB_exec_context *, RDB_transaction *);

RDB_int
RDB_delete_real_tuple(RDB_object *, RDB_object *, int, RDB_exec_context *,
        RDB_transaction *);

#endif /*DELETE_H*/
