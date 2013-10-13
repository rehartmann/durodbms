#ifndef RDB_OPMAP_H
#define RDB_OPMAP_H

/*
 * $Id$
 *
 * Copyright (C) 2006-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Functions for storing operator in a hashmap.
 */

#include "object.h"
#include "operator.h"
#include <gen/hashmap.h>

typedef struct RDB_transaction RDB_transaction;

typedef struct {
    RDB_hashmap map;
} RDB_op_map;

void
RDB_init_op_map(RDB_op_map *);

void
RDB_destroy_op_map(RDB_op_map *);

int
RDB_put_op(RDB_op_map *, RDB_operator *, RDB_exec_context *);

RDB_operator *
RDB_get_op(const RDB_op_map *, const char *name, int argc, RDB_type *argtv[],
        RDB_exec_context *);

int
RDB_del_ops(RDB_op_map *, const char *name, RDB_exec_context *);

int
RDB_put_upd_op(RDB_op_map *, const char *, int, RDB_parameter *,
        RDB_upd_op_func *, RDB_exec_context *);

#endif /*RDB_OPMAP_H*/
