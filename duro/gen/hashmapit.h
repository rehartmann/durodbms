#ifndef RDB_HASHMAP_IT_H
#define RDB_HASHMAP_IT_H

/* $Id$ */

#include "hashmap.h"
#include "hashtabit.h"

typedef struct {
    RDB_hashtable_iter it;
} RDB_hashmap_iter;

void
RDB_init_hashmap_iter(RDB_hashmap_iter *, RDB_hashmap *);

#define RDB_destroy_hashmap_iter(iter)

const char *
RDB_hashmap_next(RDB_hashmap_iter *, void **);

#endif
