/*
 * Tuple functions and definitions
 *
 * Copyright (C) 2013-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef TUPLE_H_
#define TUPLE_H_

#include "type.h"

typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_object RDB_object;

int
RDB_tuple_set(RDB_object *, const char *, const RDB_object *,
        RDB_exec_context *);

int
RDB_tuple_set_bool(RDB_object *, const char *, RDB_bool val,
        RDB_exec_context *);

int
RDB_tuple_set_int(RDB_object *, const char *, RDB_int val,
        RDB_exec_context *);

int
RDB_tuple_set_float(RDB_object *, const char *, RDB_float val,
        RDB_exec_context *);

int
RDB_tuple_set_string(RDB_object *, const char *, const char *,
        RDB_exec_context *);

RDB_object *
RDB_tuple_get(const RDB_object *, const char *);

RDB_bool
RDB_tuple_get_bool(const RDB_object *, const char *);

RDB_int
RDB_tuple_get_int(const RDB_object *, const char *);

RDB_float
RDB_tuple_get_float(const RDB_object *, const char *);

RDB_int
RDB_tuple_size(const RDB_object *);

void
RDB_tuple_attr_names(const RDB_object *, char **);

char *
RDB_tuple_get_string(const RDB_object *, const char *);

int
RDB_project_tuple(const RDB_object *, int, const char *[],
                 RDB_exec_context *, RDB_object *);

int
RDB_remove_tuple(const RDB_object *, int, const char *[],
                 RDB_exec_context *, RDB_object *);

int
RDB_rename_tuple(const RDB_object *, int renc, const RDB_renaming renv[],
                 RDB_exec_context *, RDB_object *restplp);

RDB_bool
RDB_is_tuple(const RDB_object *);

#endif /* TUPLE_H_ */
