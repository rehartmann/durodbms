#ifndef RDB_HASHMAP_H
#define RDB_HASHMAP_H

/* $Id$ */

#include "hashtable.h"
#include <stdlib.h>

/**
 * A hashmap which maps key strings to arbitary values. It does not support
 * removing keys, so a static hash table can be used without problems.
 */

typedef struct {
    RDB_hashtable tab;
} RDB_hashmap;

typedef struct {
    char *key;
    void *valuep;
} RDB_kv_pair;

/*
 * Initialize the hashmap pointed to by hp.
 *
 * Returns:
 *     RDB_OK        success
 *     RDB_NO_MEMORY insufficient memory
 */
void
RDB_init_hashmap(RDB_hashmap *, int capacity);

/*
 * Free the resources associated with the hashmap pointed to by hp.
 * Calling RDB_destroy_hashmap() again has no effect.
 * Calling RDB_destroy_hashmap after an unsuccessful call to RDB_init_hashmap()
 * also has no effect.
 */
void
RDB_destroy_hashmap(RDB_hashmap *);

/*
 * Insert the key/value pair (key, valp) into the hashmap pointed to by hp.
 * If a value for the key already exists in the hashmap, the old
 * key/value pair is replaced.
 *
 * Returns:
 *     RDB_OK        success
 *     RDB_NO_MEMORY insufficient memory
 */
int
RDB_hashmap_put(RDB_hashmap *, const char *keyp, void *valp);

/**
 * Return a pointer to the value key maps to.
 * If the key does not exist, NULL is retuned.
 *
 * Returns:
 *     A pointer to the value if a value exists for the specified key,
 *     NULL otherwise.
 */
void *
RDB_hashmap_get(const RDB_hashmap *, const char *key);

/*
 * Return the number of key/data pairs the hashmap contains.
 */
int
RDB_hashmap_size(const RDB_hashmap *);

/*
 * Store pointers to the keys in the string vector keyv.
 * The strings are not copied. If the hashmap is modified or
 * deintialized, some or all of the pointers will become invalid.
 */
void
RDB_hashmap_keys(const RDB_hashmap *, char **keyv);

#endif
