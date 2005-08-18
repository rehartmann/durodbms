/*
 * $Id$
 *
 * Copyright (C) 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <stdlib.h>
#include "hashtable.h"
#include <gen/errors.h>

static void
alloc_map(RDB_hashtable *hp) {
    int i;

    hp->entries = malloc(hp->capacity * sizeof(void *));
    if (hp->entries == NULL) {
        return;
    }

    for (i = 0; i < hp->capacity; i++) {
        hp->entries[i] = NULL;
    }
}

void
RDB_init_hashtable(RDB_hashtable *hp, int capacity, RDB_hashfn *hfp,
        RDB_equalsfn *efp)
{
    hp->capacity = capacity;
    hp->entry_count = 0;
    hp->entries = NULL;
    hp->hfnp = hfp;
    hp->efnp = efp;
}

void
RDB_destroy_hashtable(RDB_hashtable *hp)
{
    free(hp->entries);
}

/*
 * Insert the entry given by entryp into the hashtable pointed to by hp.
 *
 * Returns:
 *     RDB_OK        success
 *     RDB_NO_MEMORY insufficient memory
 */
int
RDB_hashtable_put(RDB_hashtable *hp, void *entryp, void *arg)
{
    int idx;

    /* Allocate map, if necessary */
    if (hp->entries == NULL) {
        alloc_map(hp);
        if (hp->entries == NULL)
            return RDB_NO_MEMORY;
    }

    /* If fill ratio is at 80%, rehash */
    if (hp->entry_count * 10 / hp->capacity >= 8) {
        void **oldtab = hp->entries;
        int oldcapacity = hp->capacity;
        int i;
    
        /* Build new empty table with twice the size */
        hp->capacity *= 2;
        hp->entries = malloc(sizeof(void *) * hp->capacity);
        if (hp->entries == NULL)
            return RDB_NO_MEMORY;

        for (i = 0; i < hp->capacity; i++) {
            hp->entries[i] = NULL;
        }
        
        /* Copy old table to new */
        for (i = 0; i < oldcapacity; i++) {
            if (oldtab[i] != NULL) {
                idx = (*hp->hfnp)(oldtab[i], arg) % hp->capacity;
                while (hp->entries[idx] != NULL) {
                    if (++idx >= hp->capacity)
                        idx = 0;
                }
                hp->entries[idx] = oldtab[i];
            }
        }
        
        free(oldtab);
    }

    idx = (*hp->hfnp)(entryp, arg) % hp->capacity;
    while (hp->entries[idx] != NULL 
            && !(*hp->efnp)(hp->entries[idx], entryp, arg)) {
        if (++idx >= hp->capacity)
           idx = 0;
    }
    if (hp->entries[idx] == NULL)
        hp->entry_count++;
    hp->entries[idx] = entryp;
    return RDB_OK;
}

/*
 * Return the entry which is equal to the entry given by entryp.
 */
void *
RDB_hashtable_get(const RDB_hashtable *hp, void *entryp, void *arg)
{
    int idx;
    int cnt = 0;

    if (hp->entries == NULL)
        return NULL;

    idx = (*hp->hfnp)(entryp, arg) % hp->capacity;
    while (hp->entries[idx] != NULL 
            && !(*hp->efnp)(hp->entries[idx], entryp, arg)) {
        if (++idx >= hp->capacity)
            idx = 0;
        if (++cnt >= hp->capacity)
            return NULL;
    }
    return hp->entries[idx];
}

/*
 * Return the number of entries the hashtable contains.
 */
int
RDB_hashtable_size(const RDB_hashtable *hp) {
    return hp->entry_count;
}
