#ifndef RDB_TYPEIMPL_H
#define RDB_TYPEIMPL_H

/* $Id$ */

#include <rel/rdb.h>

enum {
    RDB_IREP_BOOLEAN = 1,
    RDB_IREP_INTEGER = 2,
    RDB_IREP_RATIONAL = 3,
    RDB_IREP_BINARY = 4, /* binary, variable-length */
    RDB_IREP_TUPLE = 5,
    RDB_IREP_TABLE = 6
};

int
RDB_implement_type(const char *name, const char *modname, int options,
        RDB_transaction *);

/*
 * Return a pointer to the internal representaion of the value
 * pointed to by valp and store the length of the internal
 * representation in the location pointed to by lenp.
 */
void *
RDB_value_irep(RDB_value *valp, size_t *lenp);

/*
 * Initialize the value pointed to by valp with the internal
 * representation given by datap and len.
 * Return RDB_OK on success, RDB_NO_MEMORY if allocating memory failed.
 */
int
RDB_irep_to_value(RDB_value *valp, RDB_type *, void *datap, size_t len);

#endif
