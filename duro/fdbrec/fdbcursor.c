/*
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "fdbcursor.h"
#include "fdbenv.h"
#include "fdbrecmap.h"
#include "fdbsequence.h"
#include "fdbtx.h"
#include <rec/dbdefs.h>
#include <rec/cursorimpl.h>
#include <rec/recmapimpl.h>
#include <rec/indeximpl.h>
#include <treerec/field.h>
#include <treerec/treerecmap.h>
#include <obj/excontext.h>
#include "fdbrecmap.h"

#include <string.h>

#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

/*
 * Allocate and initialize a RDB_cursor structure.
 */
static RDB_cursor *
new_fdb_cursor(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_index *idxp,
        RDB_exec_context *ecp)
{
    RDB_cursor *curp = RDB_alloc(sizeof(RDB_cursor), ecp);
    if (curp == NULL)
        return NULL;

    curp->recmapp = rmp;
    curp->idxp = idxp;
    curp->tx = rtxp;
	curp->cur.fdb.key = NULL;
	curp->cur.fdb.value = NULL;
    curp->secondary = idxp != NULL ? RDB_TRUE : RDB_FALSE;

    curp->destroy_fn = &RDB_destroy_fdb_cursor;
    curp->get_fn = &RDB_fdb_cursor_get;
    curp->set_fn = &RDB_fdb_cursor_set;
    curp->delete_fn = &RDB_fdb_cursor_delete;
    curp->first_fn = &RDB_fdb_cursor_first;
    curp->next_fn = &RDB_fdb_cursor_next;
    curp->prev_fn = &RDB_fdb_cursor_prev;
    curp->seek_fn = &RDB_fdb_cursor_seek;

    return curp;
}

RDB_cursor *
RDB_fdb_recmap_cursor(RDB_recmap *rmp, RDB_bool wr,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    return new_fdb_cursor(rmp, rtxp, NULL, ecp);
}

RDB_cursor *
RDB_fdb_index_cursor(RDB_index *idxp, RDB_bool wr,
                  RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    if (RDB_UNIQUE & idxp->flags) {
        RDB_raise_not_supported("cursor over unique index not supported", ecp);
        return NULL;
    }
    return new_fdb_cursor(idxp->rmp, rtxp, idxp, ecp);
}

int
RDB_destroy_fdb_cursor(RDB_cursor *curp, RDB_exec_context *ecp)
{
	RDB_free(curp->cur.fdb.key);
	RDB_free(curp->cur.fdb.value);
    RDB_free(curp);
	return RDB_OK;
}

int
RDB_fdb_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp,
        RDB_exec_context *ecp)
{
	uint8_t *databp;
	int offs;

    if (fno < curp->recmapp->keyfieldcount) {
        int prefixlen;
        if (curp->idxp != NULL) {
            int skeylen;

            prefixlen = RDB_fdb_key_index_prefix_length(curp->idxp);
            if (curp->cur.fdb.key_length < sizeof(int)) {
                RDB_raise_internal("invalid size of secondary index record", ecp);
                return RDB_ERROR;
            }
            memcpy(&skeylen,
                ((uint8_t *)curp->cur.fdb.key) + curp->cur.fdb.key_length - sizeof(int),
                sizeof(int));
            if (prefixlen + skeylen > curp->cur.fdb.key_length) {
                RDB_raise_internal("invalid secondary key length", ecp);
                return RDB_ERROR;
            }
            databp = ((uint8_t *)curp->cur.fdb.key) + prefixlen + skeylen;
            offs = RDB_get_field(curp->recmapp, fno, databp,
                curp->cur.fdb.key_length - prefixlen - skeylen - sizeof(int),
                lenp, NULL);
        } else {
            prefixlen = RDB_fdb_key_prefix_length(curp->recmapp);
            databp = ((uint8_t *)curp->cur.fdb.key) + prefixlen;
            offs = RDB_get_field(curp->recmapp, fno, databp, curp->cur.fdb.key_length - prefixlen,
                lenp, NULL);
        }
	} else {
        databp = curp->cur.fdb.value;
		offs = RDB_get_field(curp->recmapp, fno,
			    databp,	curp->cur.fdb.value_length, lenp, NULL);
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
 */
static int
RDB_fdb_cursor_update(RDB_cursor *curp, int fieldc, const RDB_field fieldv[],
        RDB_exec_context *ecp)
{
    int ret;
    int skeylen;
    void *key;
    size_t key_length;
    uint8_t *pkey;
    void *value = curp->cur.fdb.value;
    size_t value_length = curp->cur.fdb.value_length;
    int prefix_length = RDB_fdb_key_index_prefix_length(curp->idxp);

    memcpy(&skeylen, (uint8_t *)curp->cur.fdb.key + curp->cur.fdb.key_length - sizeof(int),
            sizeof(int));
    if (skeylen > curp->cur.fdb.key_length - sizeof(int)) {
        RDB_raise_internal("invalid size of secondary index record", ecp);
        return RDB_ERROR;
    }
    key_length = curp->cur.fdb.key_length - prefix_length - skeylen - sizeof(int);
    pkey = RDB_fdb_prepend_key_prefix(curp->recmapp,
            curp->cur.fdb.key + prefix_length + skeylen, key_length, ecp);
    if (pkey == NULL) {
        return RDB_ERROR;
    }

    key = malloc(key_length);
    if (key == NULL) {
        RDB_free(pkey);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    memcpy(key, pkey + prefix_length, key_length);

    ret = RDB_update_fdb_kv(curp->recmapp, pkey, key_length + RDB_fdb_key_prefix_length(curp->recmapp),
            &key, &key_length, &value, &value_length, fieldc, fieldv, curp->tx, ecp);

    free(key);
    RDB_free(pkey);

    return ret;
}

int
RDB_fdb_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[],
        RDB_exec_context *ecp)
{
    int i;
    int ret;
    void *data = curp->cur.fdb.value;
    size_t data_length = (size_t)curp->cur.fdb.value_length;

    if (RDB_recmap_is_key_update(curp->recmapp, fieldc, fields)) {
        RDB_raise_invalid_argument("Modifying the key is not supported", ecp);
        return RDB_ERROR;
    }

    if (curp->idxp != NULL)
        return RDB_fdb_cursor_update(curp, fieldc, fields, ecp);

    for (i = 0; i < fieldc; i++) {
        ret = RDB_set_field_mem(curp->recmapp, &data, &data_length, &fields[i],
                curp->recmapp->vardatafieldcount);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }
    }
    curp->cur.fdb.value = (uint8_t *)data;
    curp->cur.fdb.value_length = data_length;

    /* Write record back */

    /* Key not modified, so write data only */
    fdb_transaction_set((FDBTransaction*)curp->tx,
            curp->cur.fdb.key, curp->cur.fdb.key_length,
            curp->cur.fdb.value, curp->cur.fdb.value_length);
    return RDB_OK;
}

int
RDB_fdb_cursor_delete(RDB_cursor *curp, RDB_exec_context *ecp)
{
    if (curp->idxp == NULL) {
        fdb_transaction_clear((FDBTransaction*)curp->tx, curp->cur.fdb.key, curp->cur.fdb.key_length);
        return RDB_delete_from_fdb_indexes(curp->recmapp, curp->cur.fdb.key, curp->cur.fdb.key_length,
            curp->cur.fdb.value, curp->cur.fdb.value_length, (RDB_rec_transaction *)curp->tx, ecp);
    } else {
        int pkeylen;
        int skeylen;
        uint8_t *pkey;
        int res;
        int iprefixlen = RDB_fdb_key_index_prefix_length(curp->idxp);

        if (curp->cur.fdb.key_length < iprefixlen + sizeof(int)) {
            RDB_raise_internal("invalid key record size", ecp);
            return RDB_ERROR;
        }

        memcpy(&skeylen, (uint8_t *)curp->cur.fdb.key + curp->cur.fdb.key_length - sizeof(int),
                sizeof(int));
        if (skeylen > curp->cur.fdb.key_length - sizeof(int)) {
            RDB_raise_internal("invalid size of secondary index record", ecp);
            return RDB_ERROR;
        }
        pkeylen = curp->cur.fdb.key_length - iprefixlen - skeylen - sizeof(int);

        pkey = RDB_fdb_prepend_key_prefix(curp->recmapp,
                ((uint8_t*)curp->cur.fdb.key) + iprefixlen + skeylen, pkeylen, ecp);
        if (pkey == NULL) {
            return RDB_ERROR;
        }

        res = RDB_delete_fdb_kv(curp->recmapp, pkey, pkeylen + RDB_fdb_key_prefix_length(curp->recmapp),
                (RDB_rec_transaction *)curp->tx, ecp);
        RDB_free(pkey);
        return res;
    }
}

static int
fdbkv_to_cursor(RDB_cursor *curp, const FDBKeyValue *fdbkv, RDB_exec_context *ecp)
{
    if (curp->idxp != NULL && (RDB_UNIQUE & curp->idxp->flags)
            && fdbkv->key_length < sizeof(int)) {
        RDB_raise_internal("invalid size of secondary index record", ecp);
        return RDB_ERROR;
    }

	RDB_free(curp->cur.fdb.key);
	free(curp->cur.fdb.value);
	curp->cur.fdb.key = curp->cur.fdb.value = NULL;
    if (fdbkv->key_length > 0) {
        curp->cur.fdb.key = RDB_alloc(fdbkv->key_length, ecp);
        if (curp->cur.fdb.key == NULL) {
            return RDB_ERROR;
        }
        memcpy(curp->cur.fdb.key, fdbkv->key, fdbkv->key_length);
        curp->cur.fdb.key_length = fdbkv->key_length;
    }

    if (curp->idxp == NULL) {
        curp->cur.fdb.value_length = fdbkv->value_length;
        if (fdbkv->value_length > 0) {
            curp->cur.fdb.value = malloc(fdbkv->value_length);
            if (curp->cur.fdb.value == NULL) {
                RDB_free(curp->cur.fdb.key);
                curp->cur.fdb.key = NULL;
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            memcpy(curp->cur.fdb.value, fdbkv->value, fdbkv->value_length);
        }
    } else {
        int pkeylen;
        int skeylen;
        int value_length;
        const uint8_t *value;
        uint8_t *pkey;
        FDBFuture *f;
        fdb_error_t err;
        fdb_bool_t present;
        int iprefixlen = RDB_fdb_key_index_prefix_length(curp->idxp);

        if (fdbkv->key_length < iprefixlen + sizeof(int)) {
            RDB_raise_internal("invalid key record size", ecp);
            return RDB_ERROR;
        }

        memcpy(&skeylen,
                (uint8_t *)fdbkv->key + fdbkv->key_length - sizeof(int),
                sizeof(int));
        if (skeylen > fdbkv->key_length - sizeof(int)) {
            RDB_raise_internal("invalid size of secondary index record", ecp);
            return RDB_ERROR;
        }
        pkeylen = fdbkv->key_length - iprefixlen - skeylen - sizeof(int);

        pkey = RDB_fdb_prepend_key_prefix(curp->recmapp,
                ((uint8_t*)fdbkv->key) + iprefixlen + skeylen, pkeylen, ecp);
        if (pkey == NULL) {
            return RDB_ERROR;
        }

        f = fdb_transaction_get((FDBTransaction*)curp->tx,
                pkey, pkeylen + RDB_fdb_key_prefix_length(curp->recmapp), 0);
        RDB_free(pkey);
        err = fdb_future_block_until_ready(f);
        if (err != 0) {
            RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)curp->tx);
            fdb_future_destroy(f);
            return RDB_ERROR;
        }
        err = fdb_future_get_value(f, &present, &value, &value_length);
        if (err != 0) {
            RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)curp->tx);
            fdb_future_destroy(f);
            return RDB_ERROR;
        }
        if (!present) {
            fdb_future_destroy(f);
            RDB_raise_not_found("", ecp);
            return RDB_ERROR;
        }

        curp->cur.fdb.value = malloc(value_length);
        if (curp->cur.fdb.value == NULL) {
            fdb_future_destroy(f);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        memcpy(curp->cur.fdb.value, value, value_length);
        curp->cur.fdb.value_length = value_length;
        fdb_future_destroy(f);
    }
	return RDB_OK;
}

/*
 * Move the cursor to the first record.
 * If there is no first record, RDB_NOT_FOUND is raised.
 */
int
RDB_fdb_cursor_first(RDB_cursor *curp, RDB_exec_context *ecp)
{
	fdb_error_t err;
	FDBFuture* f;
	const FDBKeyValue *out_kv;
	int out_count;
	fdb_bool_t out_more;
	uint8_t *keybuf;
	uint8_t *endkeybuf;
	int keylen;

    if (curp->idxp == NULL) {
        keylen = RDB_fdb_key_prefix_length(curp->recmapp);
    } else {
        keylen = RDB_fdb_key_index_prefix_length(curp->idxp);
    }

	/* Allocate and fill buffer for start key and end key */
	keybuf = RDB_alloc(keylen, ecp);
	if (keybuf == NULL)
		return RDB_ERROR;
	endkeybuf = RDB_alloc(keylen, ecp);
	if (endkeybuf == NULL) {
		RDB_free(keybuf);
		return RDB_ERROR;
	}
    if (curp->idxp == NULL) {
        strcpy((char *)keybuf, "t/");
        strcat((char *)keybuf, curp->recmapp->namp);
    } else {
        strcpy((char *)keybuf, "i/");
        strcat((char *)keybuf, curp->idxp->namp);
    }
	strcpy((char *) endkeybuf, (char *) keybuf);
	keybuf[keylen - 1] = (uint8_t) '/';
	endkeybuf[keylen - 1] = (uint8_t) '/' + 1;

	/* Read first key/value pair of recmap */
	f = fdb_transaction_get_range((FDBTransaction*) curp->tx,
			FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(keybuf, keylen),
			FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(endkeybuf, keylen), 1, 0,
			FDB_STREAMING_MODE_SMALL, 0, 0, 0);
	err = fdb_future_block_until_ready(f);
	RDB_free(keybuf);
	RDB_free(endkeybuf);
	if (err != 0) {
		fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)curp->tx);
        return RDB_ERROR;
	}
	err = fdb_future_get_keyvalue_array(f, &out_kv, &out_count, &out_more);
	if (err != 0) {
		fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)curp->tx);
        return RDB_ERROR;
	}
	if (out_count == 0) {
		fdb_future_destroy(f);
		RDB_raise_not_found("no first record", ecp);
		return RDB_ERROR;
	}
	if (fdbkv_to_cursor(curp, &out_kv[0], ecp) != RDB_OK) {
		fdb_future_destroy(f);
		return RDB_ERROR;
	}
	fdb_future_destroy(f);
	return RDB_OK;
}

int
RDB_fdb_cursor_next(RDB_cursor *curp, int flags, RDB_exec_context *ecp)
{
	fdb_error_t err;
	FDBFuture* f;
	const FDBKeyValue *out_kv;
	int out_count;
	fdb_bool_t out_more;
	uint8_t *endkeybuf;
	int keylen;

    if (curp->idxp == NULL) {
        keylen = RDB_fdb_key_prefix_length(curp->recmapp);
        /* Allocate and fill buffer for end key */
        endkeybuf = RDB_alloc(keylen, ecp);
        if (endkeybuf == NULL) {
            return RDB_ERROR;
        }
        strcpy((char *)endkeybuf, "t/");
        strcat((char *)endkeybuf, curp->recmapp->namp);
    } else {
        keylen = RDB_fdb_key_index_prefix_length(curp->idxp);
        endkeybuf = RDB_alloc(keylen, ecp);
        if (endkeybuf == NULL) {
            return RDB_ERROR;
        }
        strcpy((char *)endkeybuf, "i/");
        strcat((char *)endkeybuf, curp->idxp->namp);
    }
	endkeybuf[keylen - 1] = (uint8_t) '/' + 1;

	/* Read next key/value pair */
	f = fdb_transaction_get_range((FDBTransaction*) curp->tx,
			FDB_KEYSEL_FIRST_GREATER_THAN(curp->cur.fdb.key, curp->cur.fdb.key_length),
			FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(endkeybuf, keylen), 1, 0,
			FDB_STREAMING_MODE_SMALL, 0, 0, 0);
	err = fdb_future_block_until_ready(f);
	RDB_free(endkeybuf);
	if (err != 0) {
		fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)curp->tx);
        return RDB_ERROR;
	}
	err = fdb_future_get_keyvalue_array(f, &out_kv, &out_count, &out_more);
	if (err != 0) {
		fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)curp->tx);
        return RDB_ERROR;
	}

	if (out_count == 0) {
		fdb_future_destroy(f);
		RDB_raise_not_found("no next record", ecp);
		return RDB_ERROR;
	}

    if (RDB_REC_DUP & flags) {
        /* If the secondary key has changed, return not_found */
        int skeylen;
        memcpy(&skeylen,
            (uint8_t *)out_kv[0].key + out_kv[0].key_length - sizeof(int),
            sizeof(int));
        if (out_kv[0].key_length < skeylen + keylen) {
            RDB_raise_internal("invalid record length", ecp);
            fdb_future_destroy(f);
            return RDB_ERROR;
        }
        if (curp->cur.fdb.key_length < skeylen + keylen
                || memcmp(out_kv[0].key, curp->cur.fdb.key,
                        skeylen + keylen) != 0) {
            fdb_future_destroy(f);
            RDB_raise_not_found("no record for key", ecp);
            return RDB_ERROR;
        }
    }

    if (fdbkv_to_cursor(curp, &out_kv[0], ecp) != RDB_OK) {
		fdb_future_destroy(f);
		return RDB_ERROR;
	}

	fdb_future_destroy(f);
    return RDB_OK;
}

int
RDB_fdb_cursor_prev(RDB_cursor *curp, RDB_exec_context *ecp)
{
    RDB_raise_not_supported("scrolling backward not supported", ecp);
	return RDB_ERROR;
}

static uint8_t *
fdb_prepend_index_key_prefix(RDB_index *ixp, const void *key, size_t keylen, RDB_exec_context *ecp)
{
    size_t namelen = strlen(ixp->namp);
    uint8_t *key_name = RDB_alloc(RDB_fdb_key_index_prefix_length(ixp) + keylen, ecp);
    if (key_name == NULL)
        return NULL;

    memcpy(key_name, "i/", 2);
    memcpy(key_name + 2, ixp->namp, namelen);
    key_name[2 + namelen] = '/';
    memcpy(key_name + 3 + namelen, key, keylen);
    return key_name;
}

/*
 * Move the cursor to the position specified by keyv.
 */
int
RDB_fdb_cursor_seek(RDB_cursor *curp, int fieldc, RDB_field keyv[], int flags,
        RDB_exec_context *ecp)
{
	RDB_field *trkeyv;
    int ret;
    int i;
    size_t keylen;
    void *key;
    int prefixlen;
    uint8_t *endkeybuf;
    uint8_t *key_name;
    FDBFuture* f;
    const FDBKeyValue *out_kv;
    int out_count;
    fdb_bool_t out_more;
    fdb_error_t err;

    if (curp->idxp == NULL) {
        for (i = 0; i < fieldc; i++)
            keyv[i].no = i;
    } else {
        for (i = 0; i < fieldc; i++)
            keyv[i].no = curp->idxp->fieldv[i];
    }

    if (curp->idxp != NULL && (RDB_ORDERED & curp->idxp->flags)) {
    	trkeyv = RDB_alloc(curp->idxp->fieldc * sizeof(RDB_field), ecp);
    	if (trkeyv == NULL) {
    		return RDB_ERROR;
    	}
    	if (RDB_fdb_transform_fields(curp->idxp->fieldc, trkeyv, keyv,
    			curp->idxp->rmp->fieldinfos, ecp) != RDB_OK) {
    		RDB_free(trkeyv);
    		return RDB_ERROR;
    	}
    	ret = RDB_fields_to_mem(curp->idxp->rmp, curp->idxp->fieldc, trkeyv, &key, &keylen);
    	for (i = 0; i < curp->idxp->fieldc; i++) {
    		free(trkeyv[i].datap);
    	}
    	RDB_free(trkeyv);
    } else {
    	ret = RDB_fields_to_mem(curp->recmapp, fieldc,
    			keyv, &key, &keylen);
    }
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    if (curp->idxp != NULL) {
        key_name = fdb_prepend_index_key_prefix(curp->idxp, key, keylen, ecp);
    } else {
        key_name = RDB_fdb_prepend_key_prefix(curp->recmapp, key, keylen, ecp);
    }
    RDB_free(key);
    if (key_name == NULL) {
        return RDB_ERROR;
    }

    /* Allocate and fill buffer for end key */
    prefixlen = curp->idxp != NULL ? RDB_fdb_key_index_prefix_length(curp->idxp)
            : RDB_fdb_key_prefix_length(curp->recmapp);
    endkeybuf = RDB_alloc(prefixlen, ecp);
    if (endkeybuf == NULL) {
        return RDB_ERROR;
    }
    if (curp->idxp == NULL) {
        strcpy((char *)endkeybuf, "t/");
        strcat((char *)endkeybuf, curp->recmapp->namp);
    }
    else {
        strcpy((char *)endkeybuf, "i/");
        strcat((char *)endkeybuf, curp->idxp->namp);
    }
    endkeybuf[prefixlen - 1] = (uint8_t) '/' + 1;

    /* Read this or next key/value pair */
    f = fdb_transaction_get_range((FDBTransaction*)curp->tx,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(key_name, prefixlen + keylen),
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(endkeybuf, prefixlen), 1, 0,
        FDB_STREAMING_MODE_SMALL, 0, 0, 0);
    RDB_free(endkeybuf);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)curp->tx);
        return RDB_ERROR;
    }
    err = fdb_future_get_keyvalue_array(f, &out_kv, &out_count, &out_more);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)curp->tx);
        return RDB_ERROR;
    }

    if (out_count == 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_raise_not_found("no record", ecp);
        return RDB_ERROR;
    }

    if (!(RDB_REC_RANGE & flags)) {
        /* If the key has not been read, return not_found */
        if (out_kv[0].key_length < prefixlen + keylen
                || memcmp(out_kv[0].key, key_name, prefixlen + keylen) != 0) {
            RDB_free(key_name);
            fdb_future_destroy(f);
            RDB_raise_not_found("no record for key", ecp);
            return RDB_ERROR;
        }
    }

    RDB_free(key_name);
    if (fdbkv_to_cursor(curp, &out_kv[0], ecp) != RDB_OK) {
        fdb_future_destroy(f);
        return RDB_ERROR;
    }

    fdb_future_destroy(f);
    return RDB_OK;
}
