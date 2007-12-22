#ifndef TESTS_POINT_H
#define TESTS_POINT_H
/* $Id$ */

#include <rel/rdb.h>

/* Internal/actual representation of type POINT */
typedef struct {
    RDB_float x;
    RDB_float y;
} i_point;

#endif
