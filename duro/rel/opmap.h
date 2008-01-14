#ifndef RDB_OPMAP_H
#define RDB_OPMAP_H

/*
 * $Id$
 *
 * Copyright (C) 2006-2008 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Functions for mapping operator signatures to arbitrary data.
 */

#include <rel/rdb.h>
#include <gen/hashmap.h>

#include <ltdl.h>

typedef int RDB_upd_op_func(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *, RDB_transaction *);

typedef struct {
    RDB_hashmap map;
} RDB_op_map;

typedef struct {
    RDB_object iarg;
    lt_dlhandle modhdl;
    RDB_bool *updv;
    RDB_type *rtyp;
    union {
        RDB_upd_op_func *upd_fp;
        RDB_ro_op_func *ro_fp;
    } opfn;
} RDB_op_data;

void
RDB_init_op_map(RDB_op_map *);

void
RDB_destroy_op_map(RDB_op_map *);

int
RDB_put_op(RDB_op_map *, const char *name, int argc, RDB_type **argtv,
        RDB_op_data *, RDB_exec_context *);

RDB_op_data *
RDB_get_op(const RDB_op_map *, const char *name, int argc, RDB_type *argtv[],
        RDB_exec_context *);

int
RDB_del_ops(RDB_op_map *, const char *name, RDB_exec_context *);

RDB_op_data *
_RDB_new_ro_op_data(RDB_type *rtyp, RDB_ro_op_func *,
        RDB_exec_context *);

int
RDB_free_op_data(RDB_op_data *, RDB_exec_context *);

#endif /*RDB_OPMAP_H*/
