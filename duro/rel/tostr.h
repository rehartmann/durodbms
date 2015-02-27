#ifndef RDB_TOSTR_H
#define RDB_TOSTR_H

/*
 * Copyright (C) 2011-2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>

enum {
    RDB_SHOW_INDEX = 1
};

int
RDB_obj_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *, RDB_transaction *);

int
RDB_table_def_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *, RDB_transaction *, int options);

int
RDB_expr_to_str(RDB_object *dstp, const RDB_expression *exp,
        RDB_exec_context *, RDB_transaction *, int options);

#endif
