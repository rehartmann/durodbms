#ifndef RDB_HASHTABLE_H
#define RDB_HASHTABLE_H

/*
 * $Id$
 *
 * Copyright (C) 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <stdlib.h>
#include "types.h"

typedef int RDB_hashfn(const void *, void *);
typedef RDB_bool RDB_equalsfn(const void *, const void *, void *);

typedef struct {
    void **entries;
    int capacity;	/* # of table entries */
    int entry_count;	/* # of actual entries */
    RDB_hashfn *hfnp;
    RDB_equalsfn *efnp;
} RDB_hashtable;

/*
 * Initialize the hashtable pointed to by hp.
 *
 * Returns:
 *     RDB_OK        success
 *     RDB_NO_MEMORY insufficient memory
 */
void
RDB_init_hashtable(RDB_hashtable *, int capacity, RDB_hashfn *, RDB_equalsfn *);

/*
 * Free the resources associated with the hashtable pointed to by hp.
 * Calling RDB_destroy_hashtable() again has no effect.
 */
void
RDB_destroy_hashtable(RDB_hashtable *);

/*
 * Insert the entry given by entryp into the hashtable pointed to by hp.
 *
 * Returns:
 *     RDB_OK        success
 *     RDB_NO_MEMORY insufficient memory
 */
int
RDB_hashtable_put(RDB_hashtable *, void *, void *);

/*
 * Return the entry which is equal to the entry given by entryp.
 */
void *
RDB_hashtable_get(const RDB_hashtable *, void *, void *);

/*
 * Return the number of entries the hashtable contains.
 */
int
RDB_hashtable_size(const RDB_hashtable *);

#endif
