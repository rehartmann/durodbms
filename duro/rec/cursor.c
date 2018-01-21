/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "cursorimpl.h"
#include "recmapimpl.h"
#include <bdbrec/bdbcursor.h>

/*
 * Create a cursor for a recmap. The initial position of the cursor is
 * undefined.
 */
int
RDB_recmap_cursor(RDB_cursor **curpp, RDB_recmap *rmp, RDB_bool wr,
        RDB_rec_transaction *rtxp)
{
    return (*rmp->cursor_fn)(curpp, rmp, wr, rtxp);
}

/*
 * Create a cursor over an index. The initial position of the cursor is
 * undefined.
 */
int
RDB_index_cursor(RDB_cursor **curpp, RDB_index *idxp, RDB_bool wr,
                  RDB_rec_transaction *rtxp)
{
    return RDB_bdb_index_cursor(curpp, idxp, wr, rtxp);
}

/*
 * Destroy the cursor, releasing the resources associated with it
 * and freeing its memory.
 */
int
RDB_destroy_cursor(RDB_cursor *curp)
{
    return (*curp->destroy_fn)(curp);
}

/* Read the value of field fno from the current record.  */
int
RDB_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp)
{
    return (*curp->get_fn)(curp, fno, datapp, lenp);
}

/* Set the values of the fields specified by fieldc/fieldv.  */
int
RDB_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[])
{
    return (*curp->set_fn)(curp, fieldc, fields);
}

/* Delete the record at the current position. */
int
RDB_cursor_delete(RDB_cursor *curp)
{
    return (*curp->delete_fn)(curp);
}

int
RDB_cursor_update(RDB_cursor *curp, int fieldc, const RDB_field fieldv[])
{
    return RDB_bdb_cursor_update(curp, fieldc, fieldv);
}

/*
 * Move the cursor to the first record.
 * If there is no first record, DB_NOTFOUND is returned.
 */
int
RDB_cursor_first(RDB_cursor *curp)
{
    return (*curp->first_fn)(curp);
}

/*
 * Move the cursor to the next record.
 * If the cursor is at the end, DB_NOTFOUND is returned.
 * If flags is RDB_REC_DUP, return DB_NOTFOUND if the next
 * record has a different key.
 */
int
RDB_cursor_next(RDB_cursor *curp, int flags)
{
    return (*curp->next_fn)(curp, flags);
}

int
RDB_cursor_prev(RDB_cursor *curp)
{
    return (*curp->prev_fn)(curp);
}

/*
 * Move the cursor to the position specified by keyv.
 */
int
RDB_cursor_seek(RDB_cursor *curp, int fieldc, RDB_field keyv[], int flags)
{
    return (*curp->seek_fn)(curp, fieldc, keyv, flags);
}
