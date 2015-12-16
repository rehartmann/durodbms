/*
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef OBJMATCH_H_
#define OBJMATCH_H_

#include <gen/types.h>

typedef struct RDB_type RDB_type;
typedef struct RDB_object RDB_object;

RDB_bool
RDB_obj_matches_type(const RDB_object *, RDB_type *);

#endif /* OBJMATCH_H_ */
