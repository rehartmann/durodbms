/* $Id$ */

#include "hashmapit.h"

void
RDB_init_hashmap_iter(RDB_hashmap_iter *hip, RDB_hashmap *hp)
{
    hip->hp = hp;
    hip->pos = 0;
}

void *
RDB_hashmap_next(RDB_hashmap_iter *hip, char **keyp, size_t *lenp)
{
    void *valp;

    if (hip->hp->key_count == 0)
        return NULL;

    while (hip->pos < hip->hp->capacity
            && hip->hp->kv_tab[hip->pos].keyp == NULL)
        hip->pos++;

    if (hip->pos >= hip->hp->capacity) {
        /* End of hashtable reached */
        return NULL;
    }

    *keyp = hip->hp->kv_tab[hip->pos].keyp;
    if (lenp != NULL)
        *lenp = hip->hp->kv_tab[hip->pos].len;
    valp = hip->hp->kv_tab[hip->pos].valuep;
    hip->pos++;

    return valp;
}
