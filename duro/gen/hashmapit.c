/* $Id$ */

#include "hashmapit.h"

void
RDB_init_hashmap_iter(RDB_hashmap_iter *hip, RDB_hashmap *hp)
{
    RDB_init_hashtable_iter(&hip->it, &hp->tab);
}

void *
RDB_hashmap_next(RDB_hashmap_iter *hip, char **keyp)
{
    RDB_kv_pair *entryp = RDB_hashtable_next(&hip->it);

    if (entryp == NULL)
        return NULL;

    *keyp = entryp->key;
    return entryp->valuep;
}
