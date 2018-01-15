/*
 * Function to convert a DuroDBMS tuple to JSON.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef RDB_JSON_H_
#define RDB_JSON_H_

typedef struct RDB_object RDB_object;

typedef struct RDB_exec_context RDB_exec_context;

int
RDB_obj_to_json(RDB_object *, RDB_object *, RDB_exec_context *);

#endif /* RDB_JSON_H_ */
