/* $Id$ */

#include "cursor.h"
#include <gen/errors.h>
#include <string.h>

int
RDB_recmap_cursor(RDB_cursor **curpp, RDB_recmap *rfp, RDB_bool wr,
                  DB_TXN *txid)
{
    int res;
    RDB_cursor *curp = malloc(sizeof(RDB_cursor));
    u_int32_t flags = 0;
    
    if (curp == NULL)
        return RDB_NO_MEMORY;
    *curpp = curp;
    if (wr)
        flags |= DB_WRITECURSOR;
    curp->recmapp = rfp;
    curp->txid = txid;
    res = rfp->dbp->cursor(rfp->dbp, txid, &curp->cursorp, flags);
    if (res != 0) {
        free(curp);
        return RDB_convert_err(res);
    }
    memset(&curp->current_key, 0, sizeof(DBT));
    curp->current_key.flags = DB_DBT_REALLOC;
    memset(&curp->current_data, 0, sizeof(DBT));
    curp->current_data.flags = DB_DBT_REALLOC;

    return RDB_OK;
}

int
RDB_destroy_cursor(RDB_cursor *curp)
{
    int res;

    free(curp->current_key.data);
    free(curp->current_data.data);
    return RDB_convert_err(curp->cursorp->c_close(curp->cursorp));
}

int
RDB_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp)
{
    RDB_byte *databp;
    int offs;

    if (fno < curp->recmapp->keyfieldcount) {
        databp = ((RDB_byte *)curp->current_key.data);
        offs = _RDB_get_field(curp->recmapp, fno,
                curp->current_key.data, curp->current_key.size, lenp, NULL);
    } else {
        databp = ((RDB_byte *)curp->current_data.data);
        offs = _RDB_get_field(curp->recmapp, fno,
                curp->current_data.data, curp->current_data.size, lenp, NULL);
    }
    *datapp = databp + offs;
    return RDB_OK;
}

int
RDB_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[])
{
    int res;
    int i;
    RDB_bool keymodfd = RDB_FALSE;

    for (i = 0; i < fieldc; i++) {
        if (fields[i].no < curp->recmapp->keyfieldcount) {
            _RDB_set_field(curp->recmapp, &curp->current_key, &fields[i],
                      curp->recmapp->varkeyfieldcount);
            keymodfd = RDB_TRUE;
        } else {
            _RDB_set_field(curp->recmapp, &curp->current_data, &fields[i],
                      curp->recmapp->vardatafieldcount);
        }
    }
    
    /* Write record back */

    if (keymodfd) {
        /* Key was modified, so delete record first */
        res = curp->cursorp->c_del(curp->cursorp, 0);
        if (res != 0) {
            return RDB_convert_err(res);
        }
        /* Write both key and data */
        res = curp->recmapp->dbp->put(curp->recmapp->dbp, curp->txid,
                &curp->current_key, &curp->current_data, 0);
    } else {
        /* Key not modified, so write data only */
        res = curp->cursorp->c_put(curp->cursorp,
                &curp->current_key, &curp->current_data, DB_CURRENT);
    }
    return RDB_convert_err(res);
}

int
RDB_cursor_delete(RDB_cursor *curp)
{
    return RDB_convert_err(curp->cursorp->c_del(curp->cursorp, 0));
}

int
RDB_cursor_first(RDB_cursor *curp)
{
    return RDB_convert_err(curp->cursorp->c_get(curp->cursorp, &curp->current_key,
                &curp->current_data, DB_FIRST));
}

int
RDB_cursor_next(RDB_cursor *curp)
{
    return RDB_convert_err(curp->cursorp->c_get(curp->cursorp,
            &curp->current_key, &curp->current_data, DB_NEXT));
}    

int
RDB_cursor_prev(RDB_cursor *curp)
{
    return RDB_convert_err(curp->cursorp->c_get(curp->cursorp, &curp->current_key,
            &curp->current_data, DB_PREV));
}    
