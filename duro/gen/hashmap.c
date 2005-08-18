/* $Id$ */

#include "hashmap.h"
#include "hashtabit.h"
#include "errors.h"
#include "strfns.h"
#include <string.h>

static unsigned
hash_str(const void *entryp, void *arg)
{
    return RDB_hash_str(((RDB_kv_pair *) entryp)->key);
}

static RDB_bool
str_equals(const void *e1p, const void *e2p, void *arg)
{
    return (RDB_bool) strcmp(((RDB_kv_pair *) e1p)->key,
            ((RDB_kv_pair *) e2p)->key) == 0;
}

void
RDB_init_hashmap(RDB_hashmap *hp, int capacity)
{
    RDB_init_hashtable(&hp->tab, capacity, &hash_str, &str_equals);
}

void
RDB_destroy_hashmap(RDB_hashmap *hp)
{
    RDB_destroy_hashtable(&hp->tab);
}

int
RDB_hashmap_put(RDB_hashmap *hp, const char *key, void *valp)
{
    int ret;
    RDB_kv_pair entry;
    RDB_kv_pair *entryp;
    
    entry.key = (char *) key;
    entryp = RDB_hashtable_get(&hp->tab, &entry, NULL);
    if (entryp != NULL) {
        entryp->valuep = valp;
    } else {
        entryp = malloc(sizeof (RDB_kv_pair));
        if (entryp == NULL)
            return RDB_NO_MEMORY;
        entryp->key = RDB_dup_str(key);
        if (entryp->key == NULL) {
            free(entryp);
            return RDB_NO_MEMORY;
        }
        entryp->valuep = valp;
        ret = RDB_hashtable_put(&hp->tab, entryp, NULL);
        if (ret != RDB_OK) {
            free(entryp->key);
            free(entryp);
            return ret;
        }
    }
    return RDB_OK;
}

void *
RDB_hashmap_get(const RDB_hashmap *hp, const char *key)
{
    RDB_kv_pair entry;
    RDB_kv_pair *entryp;

    entry.key = (char *) key;
    entryp = RDB_hashtable_get(&hp->tab, &entry, NULL);
    if (entryp == NULL)
        return NULL;

    return entryp->valuep;
}

int
RDB_hashmap_size(const RDB_hashmap *hp)
{
    return RDB_hashtable_size(&hp->tab);
}

void
RDB_hashmap_keys(const RDB_hashmap *hp, char *keyv[])
{
    int i = 0;
    RDB_kv_pair *entryp;
    RDB_hashtable_iter hiter;

    RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &hp->tab);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        keyv[i++] = entryp->key;
    }
    RDB_destroy_hashtable_iter(&hiter);
}
