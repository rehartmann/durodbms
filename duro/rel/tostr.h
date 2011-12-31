#ifndef RDB_TOSTR_H
#define RDB_TOSTR_H

/*
 * $Id$
 *
 * Copyright (C) 2004-2011 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>
#include <stdio.h>

enum {
    RDB_SHOW_INDEX = 1
};

int
_RDB_obj_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *, RDB_transaction *);

int
_RDB_table_def_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *, RDB_transaction *, int options);

int
_RDB_expr_to_str(RDB_object *dstp, const RDB_expression *exp,
        RDB_exec_context *, RDB_transaction *, int options);

int
_RDB_print_expr(const RDB_expression *, FILE *, RDB_exec_context *);

#endif
