/*
 * $Id$
 *
 * Copyright (C) 2003-2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
*/

#include "hashmapit.h"

void
RDB_init_hashmap_iter(RDB_hashmap_iter *hip, RDB_hashmap *hp)
{
    RDB_init_hashtable_iter(&hip->it, &hp->tab);
}

/*
 * Return a pointer to the next key and store the value in **valuepp
 * Return NULL if the end is reached.
 */
const char *
RDB_hashmap_next(RDB_hashmap_iter *hip, void **valuepp)
{
    RDB_kv_pair *entryp = RDB_hashtable_next(&hip->it);

    if (entryp == NULL) {
        return NULL;
    }

    *valuepp = entryp->valuep;
    return entryp->key;
}
