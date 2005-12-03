/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "hashtabit.h"

void
RDB_init_hashtable_iter(RDB_hashtable_iter *hip, RDB_hashtable *hp)
{
    hip->hp = hp;
    hip->pos = 0;
}

void *
RDB_hashtable_next(RDB_hashtable_iter *hip)
{
    if (hip->hp->entry_count == 0)
        return NULL;

    while (hip->pos < hip->hp->capacity
            && hip->hp->entries[hip->pos] == NULL)
        hip->pos++;

    if (hip->pos >= hip->hp->capacity) {
        /* End of hashtable reached */
        return NULL;
    }

    return hip->hp->entries[hip->pos++];
}
