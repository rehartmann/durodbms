/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "cursorimpl.h"
#include "recmapimpl.h"
#include "indeximpl.h"
#include "envimpl.h"
#include <bdbrec/bdbcursor.h>
#include <obj/excontext.h>

/*
 * Create a cursor for a recmap. The initial position of the cursor is
 * undefined.
 */
RDB_cursor *
RDB_recmap_cursor(RDB_recmap *rmp, RDB_bool wr,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    return (*rmp->cursor_fn)(rmp, wr, rtxp, ecp);
}

/*
 * Create a cursor over an index. The initial position of the cursor is
 * undefined.
 */
RDB_cursor *
RDB_index_cursor(RDB_index *idxp, RDB_bool wr,
                  RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    if (idxp->rmp->envp != NULL && idxp->rmp->envp->queries) {
        RDB_raise_not_supported("RDB_index_cursor", ecp);
        return NULL;
    }
    return RDB_bdb_index_cursor(idxp, wr, rtxp, ecp);
}

/*
 * Destroy the cursor, releasing the resources associated with it
 * and freeing its memory.
 */
int
RDB_destroy_cursor(RDB_cursor *curp, RDB_exec_context *ecp)
{
    return (*curp->destroy_fn)(curp, ecp);
}

/* Read the value of field fno from the current record.  */
int
RDB_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp,
        RDB_exec_context *ecp)
{
    return (*curp->get_fn)(curp, fno, datapp, lenp, ecp);
}

/* Set the values of the fields specified by fieldc/fieldv.  */
int
RDB_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[],
        RDB_exec_context *ecp)
{
    return (*curp->set_fn)(curp, fieldc, fields, ecp);
}

/* Delete the record at the current position. */
int
RDB_cursor_delete(RDB_cursor *curp, RDB_exec_context *ecp)
{
    return (*curp->delete_fn)(curp, ecp);
}

/*
 * Move the cursor to the first record.
 * If there is no first record, RDB_NOT_FOUND_ERROR is returned in *ecp.
 */
int
RDB_cursor_first(RDB_cursor *curp, RDB_exec_context *ecp)
{
    return (*curp->first_fn)(curp, ecp);
}

/*
 * Move the cursor to the next record.
 * If the cursor is at the end, DB_NOTFOUND is returned.
 * If flags is RDB_REC_DUP, return DB_NOTFOUND if the next
 * record has a different key.
 */
int
RDB_cursor_next(RDB_cursor *curp, int flags, RDB_exec_context *ecp)
{
    return (*curp->next_fn)(curp, flags, ecp);
}

int
RDB_cursor_prev(RDB_cursor *curp, RDB_exec_context *ecp)
{
    return (*curp->prev_fn)(curp, ecp);
}

/*
 * Move the cursor to the position specified by keyv.
 */
int
RDB_cursor_seek(RDB_cursor *curp, int fieldc, RDB_field keyv[], int flags, RDB_exec_context *ecp)
{
    if (curp->recmapp->envp != NULL && curp->recmapp->envp->queries) {
        RDB_raise_not_supported("RDB_cursor_seek", ecp);
        return RDB_ERROR;
    }
    return RDB_bdb_cursor_seek(curp, fieldc, keyv, flags, ecp);
}
