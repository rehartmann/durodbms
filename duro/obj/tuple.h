/*
 * tuple.h
 *
 *  Created on: 29.09.2013
 *      Author: rene
 */

#ifndef TUPLE_H_
#define TUPLE_H_

#include "object.h"
#include "excontext.h"

int
RDB_tuple_set(RDB_object *, const char *name, const RDB_object *,
        RDB_exec_context *);

int
RDB_tuple_set_bool(RDB_object *, const char *name, RDB_bool val,
        RDB_exec_context *);

int
RDB_tuple_set_int(RDB_object *, const char *name, RDB_int val,
        RDB_exec_context *);

int
RDB_tuple_set_float(RDB_object *, const char *name, RDB_float val,
        RDB_exec_context *);

int
RDB_tuple_set_string(RDB_object *, const char *name, const char *,
        RDB_exec_context *);

RDB_object *
RDB_tuple_get(const RDB_object *, const char *name);

RDB_bool
RDB_tuple_get_bool(const RDB_object *, const char *name);

RDB_int
RDB_tuple_get_int(const RDB_object *, const char *name);

RDB_float
RDB_tuple_get_float(const RDB_object *, const char *name);

RDB_int
RDB_tuple_size(const RDB_object *);

void
RDB_tuple_attr_names(const RDB_object *, char **namev);

char *
RDB_tuple_get_string(const RDB_object *, const char *name);

int
RDB_project_tuple(const RDB_object *, int attrc, char *attrv[],
                 RDB_exec_context *, RDB_object *restplp);

int
RDB_remove_tuple(const RDB_object *, int, char *[],
                 RDB_exec_context *, RDB_object *);

#endif /* TUPLE_H_ */
