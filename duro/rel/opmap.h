#ifndef RDB_OPMAP_H
#define RDB_OPMAP_H

/*
 * $Id$
 *
 * Copyright (C) 2006-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Functions for mapping operator signatures to arbitrary data.
 */

#include <rel/rdb.h>
#include <gen/hashmap.h>

typedef struct {
    RDB_hashmap map;
} RDB_op_map;

void
RDB_init_op_map(RDB_op_map *);

void
RDB_destroy_op_map(RDB_op_map *);

int
RDB_put_op(RDB_op_map *, const char *name, int argc, RDB_type **argtv, void *datap,
        RDB_exec_context *);

void *
RDB_get_op(const RDB_op_map *, const char *name, int argc, RDB_type *argtv[]);

int
RDB_del_ops(RDB_op_map *, const char *name, /* !! del_fn, */ RDB_exec_context *);

#endif /*RDB_OPMAP_H*/
