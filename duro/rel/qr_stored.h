/*
 * Copyright (C) 2014, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef QR_STORED_H_
#define QR_STORED_H_

#include "rdb.h"

typedef struct RDB_cursor RDB_cursor;

int
RDB_init_cursor_qresult(RDB_qresult *, RDB_cursor *, RDB_object *,
        RDB_expression *, RDB_exec_context *, RDB_transaction *);

int
RDB_init_stored_qresult(RDB_qresult *, RDB_object *, RDB_expression *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_next_stored_tuple(RDB_qresult *, RDB_object *, RDB_object *,
        RDB_bool, RDB_bool, RDB_type *,
        RDB_exec_context *, RDB_transaction *);

#endif /* QR_STORED_H_ */
