/*
 * Function to convert a DuroDBMS tuple to JSON.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef JSON_H_
#define JSON_H_

#include <rel/rdb.h>

int
Dr_obj_to_json(RDB_object *, RDB_object *, RDB_exec_context *);

#endif /* JSON_H_ */
