/* $Id$ */

#include "hashmap.h"
#include "errors.h"
#include <string.h>
#include <stdlib.h>

static void
alloc_map(RDB_hashmap *hp) {
    int i;

    hp->kv_tab = malloc(hp->capacity * sizeof(struct RDB_kv_pair));
    if (hp->kv_tab == NULL) {
        return;
    }
    
    for (i = 0; i < hp->capacity; i++) {
        hp->kv_tab[i].keyp = NULL;
    }
}

void
RDB_init_hashmap(RDB_hashmap *hp, int capacity)
{
    hp->capacity = capacity;
    hp->key_count = 0;
    hp->kv_tab = NULL;
}

void
RDB_destroy_hashmap(RDB_hashmap *hp)
{
    int i;

    if (hp->kv_tab == NULL)
        return;
    for (i = 0; i < hp->capacity; i++) {
        if (hp->kv_tab[i].keyp != NULL) {
            struct RDB_kv_pair *attrp = &hp->kv_tab[i];
            free(attrp->keyp);
            free(attrp->valuep);
        }
    }
    free(hp->kv_tab);
    hp->kv_tab = NULL;
}

/*
 * Compute a hash value for the string str.
 */
static unsigned
hash_str(const char *str)
{
    int len = (int) strlen(str);
    int i;
    unsigned res = 0;
    
    for (i = 0; i < len; i++)
        res += str[i];

    return res;
}

int
RDB_hashmap_put(RDB_hashmap *hp, const char *key, const void *valp, size_t len)
{
    int idx;
    struct RDB_kv_pair *attrp;

    /* allocate map, if necessary */
    if (hp->kv_tab == NULL) {
        alloc_map(hp);
        if (hp->kv_tab == NULL)
            return RDB_NO_MEMORY;
    }

    /* if fill ratio is at 80%, rehash */
    if (hp->key_count * 10 / hp->capacity >= 8) {
        struct RDB_kv_pair *oldtab = hp->kv_tab;
        int oldcapacity = hp->capacity;
        int i;
    
        /* Build new empty table with twice the size */
        hp->capacity *= 2;
        hp->kv_tab = malloc(sizeof(struct RDB_kv_pair) * hp->capacity);
        if (hp->kv_tab == NULL)
            return RDB_NO_MEMORY;

        for (i = 0; i < hp->capacity; i++) {
            hp->kv_tab[i].keyp = NULL;
        }
        
        /* Copy old table to new */
        for (i = 0; i < oldcapacity; i++) {
            if (oldtab[i].keyp != NULL) {
                idx = (int)(hash_str(oldtab[i].keyp) % hp->capacity);
                while (hp->kv_tab[idx].keyp != NULL) {
                    if (++idx >= hp->capacity)
                        idx = 0;
                }
                attrp = &hp->kv_tab[idx];
                attrp->keyp = oldtab[i].keyp;
                attrp->valuep = oldtab[i].valuep;
                attrp->len = oldtab[i].len;
            }
        }
        
        free(oldtab);
    }

    idx = (int)(hash_str(key) % hp->capacity);
    while (hp->kv_tab[idx].keyp != NULL 
            && strcmp(hp->kv_tab[idx].keyp, key) != 0) {
        if (++idx >= hp->capacity)
           idx = 0;
    }
    attrp = &hp->kv_tab[idx];
    if (attrp->keyp == NULL) {
        attrp->keyp = malloc(strlen(key) + 1);
        if (attrp->keyp == NULL)
            return RDB_NO_MEMORY;

        strcpy(attrp->keyp, key);
        attrp->valuep = malloc(len);
        if (attrp->valuep == NULL) {
            free(attrp->keyp);
            attrp->keyp = NULL;
            return RDB_NO_MEMORY;
        }

        memcpy(attrp->valuep, valp, len);
        hp->key_count++;
    } else {
        if (attrp->len < len) {
            void *newp = realloc(attrp->valuep, len);

            if (newp == NULL)
                return RDB_NO_MEMORY;
            attrp->valuep = newp;
        }
        memcpy(attrp->valuep, valp, len);
    }
    attrp->len = len;
    return RDB_OK;
}

void *
RDB_hashmap_get(const RDB_hashmap *hp, const char *key, size_t *lenp)
{
    int idx = (int)(hash_str(key) % hp->capacity);
    struct RDB_kv_pair *attrp;

    if (hp->kv_tab == NULL)
        return NULL;

    while (hp->kv_tab[idx].keyp != NULL 
            && strcmp(hp->kv_tab[idx].keyp, key) != 0) {
        if (++idx >= hp->capacity)
           idx = 0;
    }
    attrp = &hp->kv_tab[idx];
    if (attrp->keyp == NULL)
        return NULL;
    if (lenp != NULL)
        *lenp = attrp->len;
    return attrp->valuep;
}

int
RDB_hashmap_size(const RDB_hashmap *hp)
{
    return hp->key_count;
}

void
RDB_hashmap_keys(const RDB_hashmap *hp, char **keys)
{
    int i;
    int ki = 0;
    
    if (hp->key_count == 0)
        return;

    for (i = 0; i < hp->capacity; i++) {
        if (hp->kv_tab[i].keyp != NULL)
            keys[ki++] = hp->kv_tab[i].keyp;
    }
}
