#ifndef RDB_TYPEIMPL_H
#define RDB_TYPEIMPL_H

#include <rel/rdb.h>

/* $Id$ */

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
