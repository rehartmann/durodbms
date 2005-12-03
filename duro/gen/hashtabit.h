#ifndef RDB_HASHTABLE_IT_H
#define RDB_HASHTABLE_IT_H

/* $Id$ */

#include "hashtable.h"

typedef struct {
    RDB_hashtable *hp;
    int pos;
} RDB_hashtable_iter;

void
RDB_init_hashtable_iter(RDB_hashtable_iter *hip, RDB_hashtable *hp);

#define RDB_destroy_hashtable_iter(hp)

void *
RDB_hashtable_next(RDB_hashtable_iter *hp);

#endif
