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
RDB_update_real(RDB_object *tbp, RDB_expression *condp, int updc,
        const RDB_attr_update updv[], RDB_exec_context *, RDB_transaction *);

RDB_int
RDB_update_where_index(RDB_expression *, RDB_expression *,
        int updc, const RDB_attr_update updv[], RDB_exec_context *,
        RDB_transaction *);

#endif /*UPDATE_H*/
