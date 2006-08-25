#ifndef DELETE_H
#define DELETE_H

/*
 * $Id$
 *
 * Copyright (C) 2006 René Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Declares internal functions for deleting tuples from tables.
 */

#include "rdb.h"

RDB_int
_RDB_delete_real(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *,
        RDB_transaction *);

RDB_int
_RDB_delete_where_index(RDB_expression *texp, RDB_expression *condp,
        RDB_exec_context *, RDB_transaction *);

#endif /*DELETE_H*/
