#ifndef RDB_HASHTABLE_H
#define RDB_HASHTABLE_H

/*
 * Hashtable
 *
 * Copyright (C) 2005, 2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <stdlib.h>
#include "types.h"

typedef unsigned RDB_hashfn(const void *, void *);
typedef RDB_bool RDB_equalsfn(const void *, const void *, void *);

typedef struct {
    void **entries;
    int capacity;	/* # of table entries */
    int entry_count;	/* # of actual entries */
    RDB_hashfn *hfnp;
    RDB_equalsfn *efnp;
} RDB_hashtable;

void
RDB_init_hashtable(RDB_hashtable *, int capacity, RDB_hashfn *, RDB_equalsfn *);

void
RDB_destroy_hashtable(RDB_hashtable *);

int
RDB_hashtable_put(RDB_hashtable *, void *, void *);

void *
RDB_hashtable_get(const RDB_hashtable *, void *, void *);

int
RDB_hashtable_size(const RDB_hashtable *);

void
RDB_clear_hashtable(RDB_hashtable *);

#endif
