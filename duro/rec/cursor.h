#ifndef RDB_CURSOR_H
#define RDB_CURSOR_H

/* $Id$ */

#include "recmap.h"
#include <db.h>
#include <stdlib.h>

typedef struct {
    /* internal */
    DBC *cursorp;
    DBT current_key;
    DBT current_data;
    RDB_recmap *recmapp;
    DB_TXN *txid;
} RDB_cursor;

/* Create a cursor for a recmap. The initial position of the cursor is
 * undefined.
 */
int
RDB_recmap_cursor(RDB_cursor **, RDB_recmap *, RDB_bool wr, DB_TXN *txid);

/* Read the value of field fno from the current record.  */
int
RDB_cursor_get(RDB_cursor *, int fno, void **datapp, size_t *sizep);

/* Set the value of field fno of the current record.  */
int
RDB_cursor_set(RDB_cursor *, int fieldc, RDB_field[]);

/* Delete the record at the current position. */
int
RDB_cursor_delete(RDB_cursor *);

/* Move the cursor to the first record.
 * If there is no first record, RDB_NOT_FOUND is returned.
 */
int
RDB_cursor_first(RDB_cursor *);

/* Move the cursor to the next record.
 * If the cursor is at the end, RDB_NOT_FOUND is returned.
 */
int
RDB_cursor_next(RDB_cursor *);

/* Detroy the cursor, releasing the resources associated with it
 * and freeing its memory.
 */
int
RDB_destroy_cursor(RDB_cursor *);

#endif
