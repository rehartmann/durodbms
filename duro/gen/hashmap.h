#ifndef RDB_HASHMAP_H
#define RDB_HASHMAP_H

/* $Id$ */

#include <stdlib.h>

#include "types.h"

/**
 * A hashmap which maps key strings to arbitary values. It does not support
 * removing keys, so a static hash table can be used without problems.
 */

struct RDB_kv_pair {
    char *keyp;
    void *valuep;
    size_t len;
};

typedef struct {
    struct RDB_kv_pair *kv_tab;
    int capacity;	/* # of table entries */
    int key_count;	/* # of key/value pairs */
} RDB_hashmap;

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
 * The length of the value is specified by len.
 * If a value for the key already exists in the hashmap, the old
 * key/value pair is replaced.
 *
 * Returns:
 *     RDB_OK        success
 *     RDB_NO_MEMORY insufficient memory
 */
int
RDB_hashmap_put(RDB_hashmap *, const char *keyp, const void *valp, size_t len);

/**
 * Return a pointer to the value key maps to. If the value exists for the key
 * and lenp is not NULL, the length will be stored in the location pointed to
 * by lengthp.
 * The pointer returned will become invalid if the hashmap is destroyed
 * using RDB_destroy_hashmap() or if the value is overwritten
 * by calling RDB_hashmap_put() with the same keyp argument.
 * If the key does not exist, NULL is retuned.
 *
 * Returns:
 *     A pointer to the value if a value exists for the specified key,
 *     NULL otherwise.
 */
void *
RDB_hashmap_get(const RDB_hashmap *, const char *key, size_t *lenp);

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
