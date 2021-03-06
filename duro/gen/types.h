#ifndef RDB_TYPES_H
#define RDB_TYPES_H

/*
 * Definitions for built-in basic data types
 */

/*
 * Copyright (C) 2003, 2005, 2007, 2013, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <limits.h>
#include <float.h>
#include <stdint.h>

typedef int32_t RDB_int;
typedef double RDB_float;
typedef int8_t RDB_bool;

#define RDB_INT_MAX INT32_MAX
#define RDB_INT_MIN INT32_MIN
#define RDB_FLOAT_MIN DBL_MIN
#define RDB_FLOAT_MAX DBL_MAX

#define RDB_TRUE ((RDB_bool) 1)
#define RDB_FALSE ((RDB_bool) 0)

#define RDB_VARIABLE_LEN (-1) /* indicates a variable field length */

enum {
    RDB_OK = 0
};

#endif
