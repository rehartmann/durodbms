/*
 * Array functions
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef ARRAY_H_
#define ARRAY_H_

#include "object.h"

typedef struct RDB_exec_context RDB_exec_context;

RDB_object *
RDB_array_get(const RDB_object *, RDB_int idx, RDB_exec_context *);

int
RDB_array_set(RDB_object *, RDB_int idx, const RDB_object *tplp,
        RDB_exec_context *);

RDB_int
RDB_array_length(const RDB_object *, RDB_exec_context *);

int
RDB_set_array_length(RDB_object *arrp, RDB_int len, RDB_exec_context *);

RDB_bool
RDB_is_array(const RDB_object *);

#endif /* ARRAY_H_ */
