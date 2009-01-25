/*
 * $Id$ 
 *
 * Copyright (C) 2003-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "cursor.h"
#include <string.h>
#include <errno.h>

/*
 * Allocate and initialize a RDB_cursor structure.
 */
static RDB_cursor *
new_cursor(RDB_recmap *rmp, DB_TXN *txid, RDB_index *idxp)
{
    RDB_cursor *curp = malloc(sizeof(RDB_cursor));
    if (curp == NULL)
        return NULL;

    curp->recmapp = rmp;
    curp->idxp = idxp;
    curp->txid = txid;
    memset(&curp->current_key, 0, sizeof(DBT));
    curp->current_key.flags = DB_DBT_REALLOC;
    memset(&curp->current_data, 0, sizeof(DBT));
    curp->current_data.flags = DB_DBT_REALLOC;

    return curp;
}

int
RDB_recmap_cursor(RDB_cursor **curpp, RDB_recmap *rmp, RDB_bool wr,
                  DB_TXN *txid)
{
    /*
     * The wr agument is ignored, because setting DB_WRITECURSOR only
     * works for multiple reader/single writer access.
     */

    int ret;
    RDB_cursor *curp = new_cursor(rmp, txid, NULL);
    
    if (curp == NULL)
        return ENOMEM;
    ret = rmp->dbp->cursor(rmp->dbp, txid, &curp->cursorp, 0);
    if (ret != 0) {
        free(curp);
        return ret;
    }
    *curpp = curp;
    return RDB_OK;
}

int
RDB_index_cursor(RDB_cursor **curpp, RDB_index *idxp, RDB_bool wr,
                  DB_TXN *txid)
{
    int ret;
    RDB_cursor *curp = new_cursor(idxp->rmp, txid, idxp);
    
    if (curp == NULL)
        return ENOMEM;
    ret = idxp->dbp->cursor(idxp->dbp, txid, &curp->cursorp, 0);
    if (ret != 0) {
        free(curp);        
        return ret;
    }
    *curpp = curp;
    return RDB_OK;
}

int
RDB_destroy_cursor(RDB_cursor *curp)
{
    int ret;

    free(curp->current_key.data);
    free(curp->current_data.data);

    ret = curp->cursorp->close(curp->cursorp);
    free(curp);
    return ret;
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
    if (offs < 0)
       return offs;
    *datapp = databp + offs;
    return RDB_OK;
}

int
RDB_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[])
{
    int i;
    int ret;
    RDB_bool keymodfd = RDB_FALSE;

    for (i = 0; i < fieldc; i++) {
        if (fields[i].no < curp->recmapp->keyfieldcount) {
            ret = _RDB_set_field(curp->recmapp, &curp->current_key, &fields[i],
                      curp->recmapp->varkeyfieldcount);
            if (ret != RDB_OK)
                return ret;
            keymodfd = RDB_TRUE;
        } else {
            ret = _RDB_set_field(curp->recmapp, &curp->current_data, &fields[i],
                      curp->recmapp->vardatafieldcount);
            if (ret != RDB_OK)
                return ret;
        }
    }
    
    /* Write record back */

    if (keymodfd) {
        /* Key modification is not supported */
        return EINVAL;
    }

    /* Key not modified, so write data only */
    return curp->cursorp->put(curp->cursorp,
                &curp->current_key, &curp->current_data, DB_CURRENT);
}

int
RDB_cursor_delete(RDB_cursor *curp)
{
    return curp->cursorp->del(curp->cursorp, 0);
}

int
RDB_cursor_update(RDB_cursor *curp, int fieldc, const RDB_field fieldv[])
{
    DBT key, pkey, data;
    int ret;

    memset(&key, 0, sizeof (key));
    memset(&pkey, 0, sizeof (pkey));
    memset(&data, 0, sizeof (data));
    data.flags = DB_DBT_REALLOC;

    ret = curp->cursorp->pget(curp->cursorp, &key, &pkey, &data, DB_CURRENT);
    if (ret != 0) {
        goto cleanup;
    }

    ret = _RDB_update_rec(curp->recmapp, &pkey, &data, fieldc, fieldv,
            curp->txid);

cleanup:
    free(data.data);
    return ret;
}

int
RDB_cursor_first(RDB_cursor *curp)
{
    if (curp->idxp == NULL) {
        return curp->cursorp->get(curp->cursorp,
                &curp->current_key, &curp->current_data, DB_FIRST);
    } else {
        DBT key;

        memset(&key, 0, sizeof key);
        return curp->cursorp->pget(curp->cursorp,
                &key, &curp->current_key, &curp->current_data, DB_FIRST);
    }
}

int
RDB_cursor_next(RDB_cursor *curp, int flags)
{
    DBT key;

    if (curp->idxp == NULL) {
        return curp->cursorp->get(curp->cursorp,
                &curp->current_key, &curp->current_data,
                flags == RDB_REC_DUP ? DB_NEXT_DUP : DB_NEXT);
    } else {
        memset(&key, 0, sizeof key);
        return curp->cursorp->pget(curp->cursorp,
                &key, &curp->current_key, &curp->current_data,
                flags == RDB_REC_DUP ? DB_NEXT_DUP : DB_NEXT);
    }
}

int
RDB_cursor_prev(RDB_cursor *curp)
{
    DBT key;

    if (curp->idxp == NULL) {
        return curp->cursorp->get(curp->cursorp,
                &curp->current_key, &curp->current_data, DB_PREV);
    } else {
        memset(&key, 0, sizeof key);
        return curp->cursorp->pget(curp->cursorp,
                &key, &curp->current_key, &curp->current_data, DB_PREV);
    }
}

int
RDB_cursor_seek(RDB_cursor *curp, int fieldc, RDB_field keyv[], int flags)
{
    int ret;
    int i;
    DBT key;

    if (curp->idxp == NULL) {
        for (i = 0; i < curp->recmapp->keyfieldcount; i++)
            keyv[i].no = i;
    } else {
        for (i = 0; i < fieldc; i++)
            keyv[i].no = curp->idxp->fieldv[i];
    }

    if (curp->idxp == NULL) {
        ret = _RDB_fields_to_DBT(curp->recmapp, curp->recmapp->keyfieldcount,
                keyv, &curp->current_key);
    } else {
        ret = _RDB_fields_to_DBT(curp->recmapp, fieldc, keyv, &key);
        key.flags = DB_DBT_REALLOC;
    }
    if (ret != RDB_OK)
        return ret;

    if (curp->idxp == NULL) {
        ret = curp->cursorp->get(curp->cursorp,
                &curp->current_key, &curp->current_data,
                flags == RDB_REC_RANGE ? DB_SET_RANGE : DB_SET);
    } else {
        ret = curp->cursorp->pget(curp->cursorp,
                &key, &curp->current_key, &curp->current_data,
                flags == RDB_REC_RANGE ? DB_SET_RANGE : DB_SET);
    }
    if (curp->idxp != NULL)
        free(key.data);
    return ret;
}
