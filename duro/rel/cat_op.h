/*
 * Copyright (C) 2012, 2015, 2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef CAT_OP_H_
#define CAT_OP_H_

#include <gen/types.h>

typedef struct RDB_transaction RDB_transaction;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_object RDB_object;

int
RDB_cat_load_ro_op(const char *, RDB_exec_context *, RDB_transaction *);

int
RDB_cat_load_upd_op(const char *, RDB_exec_context *, RDB_transaction *);

RDB_int
RDB_cat_del_ops(const char *, RDB_object *, RDB_object* ,
        RDB_exec_context *, RDB_transaction *);

RDB_int
RDB_cat_del_op_version(const char *, const char *, RDB_object *,
        RDB_exec_context *, RDB_transaction *);

#endif /* CAT_OP_H_ */
