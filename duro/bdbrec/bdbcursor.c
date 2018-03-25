/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "bdbcursor.h"
#include "bdbrecmap.h"
#include <rec/cursorimpl.h>
#include <rec/recmapimpl.h>
#include <rec/indeximpl.h>
#include <obj/excontext.h>
#include <string.h>

/*
 * Allocate and initialize a RDB_cursor structure.
 */
static RDB_cursor *
new_bdb_cursor(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_index *idxp,
        RDB_exec_context *ecp)
{
    RDB_cursor *curp = malloc(sizeof(RDB_cursor));
    if (curp == NULL)
        return NULL;

    curp->recmapp = rmp;
    curp->idxp = idxp;
    curp->tx = rtxp;
    memset(&curp->cur.bdb.current_key, 0, sizeof(DBT));
    curp->cur.bdb.current_key.flags = DB_DBT_REALLOC;
    memset(&curp->cur.bdb.current_data, 0, sizeof(DBT));
    curp->cur.bdb.current_data.flags = DB_DBT_REALLOC;

    curp->destroy_fn = &RDB_destroy_bdb_cursor;
    curp->get_fn = &RDB_bdb_cursor_get;
    curp->set_fn = &RDB_bdb_cursor_set;
    curp->delete_fn = &RDB_bdb_cursor_delete;
    curp->first_fn = &RDB_bdb_cursor_first;
    curp->next_fn = &RDB_bdb_cursor_next;
    curp->prev_fn = &RDB_bdb_cursor_prev;

    return curp;
}

RDB_cursor *
RDB_bdb_recmap_cursor(RDB_recmap *rmp, RDB_bool wr,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    /*
     * The wr agument is ignored, because setting DB_WRITECURSOR only
     * works for multiple reader/single writer access.
     */

    int ret;
    RDB_cursor *curp = new_bdb_cursor(rmp, rtxp, NULL, ecp);
    
    if (curp == NULL) {
        return NULL;
    }
    ret = rmp->impl.dbp->cursor(rmp->impl.dbp, (DB_TXN *) rtxp, &curp->cur.bdb.cursorp, 0);
    if (ret != 0) {
        free(curp);
        RDB_errcode_to_error(ret, ecp);
        return NULL;
    }
    curp->secondary = RDB_FALSE;
    return curp;
}

RDB_cursor *
RDB_bdb_index_cursor(RDB_index *idxp, RDB_bool wr,
                  RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    int ret;
    RDB_cursor *curp = new_bdb_cursor(idxp->rmp, rtxp, idxp, ecp);

    if (curp == NULL) {
        return NULL;
    }
    ret = idxp->impl.dbp->cursor(idxp->impl.dbp, (DB_TXN *) rtxp, &curp->cur.bdb.cursorp, 0);
    if (ret != 0) {
        free(curp);
        RDB_errcode_to_error(ret, ecp);
        return NULL;
    }
    curp->secondary = RDB_TRUE;
    return curp;
}

int
RDB_destroy_bdb_cursor(RDB_cursor *curp, RDB_exec_context *ecp)
{
    int ret;

    free(curp->cur.bdb.current_key.data);
    free(curp->cur.bdb.current_data.data);

    ret = curp->cur.bdb.cursorp->close(curp->cur.bdb.cursorp);
    RDB_free(curp);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp,
        RDB_exec_context *ecp)
{
    RDB_byte *databp;
    int offs;

    if (fno < curp->recmapp->keyfieldcount) {
        databp = (RDB_byte *)curp->cur.bdb.current_key.data;
        offs = RDB_get_field(curp->recmapp, fno,
                databp, curp->cur.bdb.current_key.size, lenp, NULL);
    } else {
        databp = (RDB_byte *)curp->cur.bdb.current_data.data;
        offs = RDB_get_field(curp->recmapp, fno,
                databp, curp->cur.bdb.current_data.size, lenp, NULL);
    }
    if (offs < 0) {
        RDB_errcode_to_error(offs, ecp);
        return RDB_ERROR;
    }
    *datapp = databp + offs;
    return RDB_OK;
}

/*
 * Update the current record.
 * This works only with cursors over secondary indexes.
 */
static int
RDB_bdb_cursor_update(RDB_cursor *curp, int fieldc, const RDB_field fieldv[],
        RDB_exec_context *ecp)
{
    DBT key, pkey, data;
    int ret;

    memset(&key, 0, sizeof (key));
    memset(&pkey, 0, sizeof (pkey));
    memset(&data, 0, sizeof (data));
    data.flags = DB_DBT_REALLOC;

    ret = curp->cur.bdb.cursorp->pget(curp->cur.bdb.cursorp, &key, &pkey,
            &data, DB_CURRENT);
    if (ret != 0) {
        goto cleanup;
    }

    ret = RDB_update_DBT_rec(curp->recmapp, &pkey, &data, fieldc, fieldv,
            (DB_TXN *) curp->tx);

cleanup:
    free(data.data);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[],
        RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (curp->secondary)
        return RDB_bdb_cursor_update(curp, fieldc, fields, ecp);

    for (i = 0; i < fieldc; i++) {
        if (fields[i].no < curp->recmapp->keyfieldcount) {
            RDB_raise_invalid_argument("Modifiying the key is not supported", ecp);
            return RDB_ERROR;
        }
        ret = RDB_set_field(curp->recmapp, &curp->cur.bdb.current_data, &fields[i],
                curp->recmapp->vardatafieldcount);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }
    }
    
    /* Write record back */

    /* Key not modified, so write data only */
    ret = curp->cur.bdb.cursorp->put(curp->cur.bdb.cursorp,
                &curp->cur.bdb.current_key, &curp->cur.bdb.current_data, DB_CURRENT);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_cursor_delete(RDB_cursor *curp, RDB_exec_context *ecp)
{
    int ret = curp->cur.bdb.cursorp->del(curp->cur.bdb.cursorp, 0);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/*
 * Move the cursor to the first record.
 * If there is no first record, RDB_NOT_FOUND is raised.
 */
int
RDB_bdb_cursor_first(RDB_cursor *curp, RDB_exec_context *ecp)
{
    int ret;
    if (curp->idxp == NULL) {
        ret = curp->cur.bdb.cursorp->get(curp->cur.bdb.cursorp,
                &curp->cur.bdb.current_key, &curp->cur.bdb.current_data, DB_FIRST);
    } else {
        DBT key;

        memset(&key, 0, sizeof key);
        ret = curp->cur.bdb.cursorp->pget(curp->cur.bdb.cursorp,
                &key, &curp->cur.bdb.current_key, &curp->cur.bdb.current_data, DB_FIRST);
    }
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_cursor_next(RDB_cursor *curp, int flags, RDB_exec_context *ecp)
{
    DBT key;
    int ret;

    if (curp->idxp == NULL) {
        ret = curp->cur.bdb.cursorp->get(curp->cur.bdb.cursorp,
                &curp->cur.bdb.current_key, &curp->cur.bdb.current_data,
                flags == RDB_REC_DUP ? DB_NEXT_DUP : DB_NEXT);
    } else {
        memset(&key, 0, sizeof key);
        ret = curp->cur.bdb.cursorp->pget(curp->cur.bdb.cursorp,
                &key, &curp->cur.bdb.current_key, &curp->cur.bdb.current_data,
                flags == RDB_REC_DUP ? DB_NEXT_DUP : DB_NEXT);
    }
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_cursor_prev(RDB_cursor *curp, RDB_exec_context *ecp)
{
    DBT key;
    int ret;

    if (curp->idxp == NULL) {
        ret = curp->cur.bdb.cursorp->get(curp->cur.bdb.cursorp,
                &curp->cur.bdb.current_key, &curp->cur.bdb.current_data, DB_PREV);
    } else {
        memset(&key, 0, sizeof key);
        ret = curp->cur.bdb.cursorp->pget(curp->cur.bdb.cursorp,
                &key, &curp->cur.bdb.current_key, &curp->cur.bdb.current_data, DB_PREV);
    }
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/*
 * Move the cursor to the position specified by keyv.
 */
int
RDB_bdb_cursor_seek(RDB_cursor *curp, int fieldc, RDB_field keyv[], int flags,
        RDB_exec_context *ecp)
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
        ret = RDB_fields_to_DBT(curp->recmapp, curp->recmapp->keyfieldcount,
                keyv, &curp->cur.bdb.current_key);
    } else {
        ret = RDB_fields_to_DBT(curp->recmapp, fieldc, keyv, &key);
        key.flags = DB_DBT_REALLOC;
    }
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    if (curp->idxp == NULL) {
        ret = curp->cur.bdb.cursorp->get(curp->cur.bdb.cursorp,
                &curp->cur.bdb.current_key, &curp->cur.bdb.current_data,
                flags == RDB_REC_RANGE ? DB_SET_RANGE : DB_SET);
    } else {
        ret = curp->cur.bdb.cursorp->pget(curp->cur.bdb.cursorp,
                &key, &curp->cur.bdb.current_key, &curp->cur.bdb.current_data,
                flags == RDB_REC_RANGE ? DB_SET_RANGE : DB_SET);
    }
    if (curp->idxp != NULL)
        free(key.data);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}
