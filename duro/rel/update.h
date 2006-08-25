#ifndef UPDATE_H
#define UPDATE_H

/*
 * $Id$
 *
 * Copyright (C) 2006 René Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Declares internal functions for updating tables.
 */

#include "rdb.h"

RDB_int
_RDB_update_real(RDB_object *tbp, RDB_expression *condp, int updc,
        const RDB_attr_update updv[], RDB_exec_context *, RDB_transaction *);

RDB_int
_RDB_update_where_index(RDB_expression *, RDB_expression *,
        int updc, const RDB_attr_update updv[], RDB_exec_context *,
        RDB_transaction *);

#endif /*UPDATE_H*/
