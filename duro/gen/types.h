#ifndef RDB_TYPES_H
#define RDB_TYPES_H

/* $Id$ */

#include <limits.h>
#include <float.h>

typedef int RDB_int;
typedef double RDB_rational;
typedef unsigned char RDB_byte;
typedef char RDB_bool;

#define RDB_INT_MAX INT_MAX
#define RDB_INT_MIN INT_MIN
#define RDB_RATIONAL_MAX DBL_MAX
#define RDB_RATIONAL_MIN DBL_MIN

#define RDB_TRUE ((RDB_bool) 1)
#define RDB_FALSE ((RDB_bool) 0)

enum {
    RDB_OK = 0,
};

#endif
