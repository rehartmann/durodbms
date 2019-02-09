/*
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "fdbcursor.h"
#include "fdbrecmap.h"
#include <rec/cursorimpl.h>
#include <rec/recmapimpl.h>
#include <obj/excontext.h>
#include <fdbrec/fdbsequence.h>
#include <fdbrec/fdbtx.h>

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
    RDB_raise_not_supported("indexes not available", ecp);
	return NULL;
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
		int prefixlen = RDB_fdb_key_prefix_length(curp->recmapp);
		databp = ((uint8_t *)curp->cur.fdb.key) + prefixlen;
		offs = RDB_get_field(curp->recmapp, fno,
			databp,	curp->cur.fdb.key_length - prefixlen, lenp, NULL);
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
 * This works only with cursors over secondary indexes.
 */
static int
RDB_fdb_cursor_update(RDB_cursor *curp, int fieldc, const RDB_field fieldv[],
        RDB_exec_context *ecp)
{
    RDB_raise_not_supported("update by cursor not available", ecp);
	return RDB_ERROR;
}

int
RDB_fdb_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[],
        RDB_exec_context *ecp)
{
    RDB_raise_not_supported("RDB_fdb_cursor_set not implemented", ecp);
	return RDB_ERROR;
}

int
RDB_fdb_cursor_delete(RDB_cursor *curp, RDB_exec_context *ecp)
{
    fdb_transaction_clear((FDBTransaction*)curp->tx, curp->cur.fdb.key, curp->cur.fdb.key_length);
	return RDB_OK;
}

static int
fdbkv_to_cursor(RDB_cursor *curp, const FDBKeyValue *fdbkv, RDB_exec_context *ecp)
{
	curp->cur.fdb.key_length = fdbkv->key_length;
	curp->cur.fdb.value_length = fdbkv->value_length;
	RDB_free(curp->cur.fdb.key);
	RDB_free(curp->cur.fdb.value);
	curp->cur.fdb.key = curp->cur.fdb.value = NULL;
	if (fdbkv->key_length > 0) {
		curp->cur.fdb.key = RDB_alloc(fdbkv->key_length, ecp);
		if (curp->cur.fdb.key == NULL) {
			return RDB_ERROR;
		}
		memcpy(curp->cur.fdb.key, fdbkv->key, fdbkv->key_length);
	}
	if (fdbkv->value_length > 0) {
		curp->cur.fdb.value = RDB_alloc(fdbkv->value_length, ecp);
		if (curp->cur.fdb.value == NULL) {
			RDB_free(curp->cur.fdb.key);
			curp->cur.fdb.key = NULL;
			return RDB_ERROR;
		}
		memcpy(curp->cur.fdb.value, fdbkv->value, fdbkv->value_length);
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
	fdb_bool_t out_present;
	uint8_t const* out_value;
	int out_value_length;
	FDBFuture* f;
	const FDBKeyValue *out_kv;
	int out_count;
	fdb_bool_t out_more;
	uint8_t *keybuf;
	uint8_t *endkeybuf;
	int keylen = RDB_fdb_key_prefix_length(curp->recmapp);

	/* Allocate and fill buffer for start key and end key */
	keybuf = RDB_alloc(keylen, ecp);
	if (keybuf == NULL)
		return RDB_ERROR;
	endkeybuf = RDB_alloc(keylen, ecp);
	if (endkeybuf == NULL) {
		RDB_free(keybuf);
		return RDB_ERROR;
	}
	strcpy((char *) keybuf, "t/");
	strcat((char *) keybuf, curp->recmapp->namp);
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
		RDB_fdb_errcode_to_error(err, ecp);
		return RDB_ERROR;
	}
	err = fdb_future_get_keyvalue_array(f, &out_kv, &out_count, &out_more);
	if (err != 0) {
		fdb_future_destroy(f);
		RDB_fdb_errcode_to_error(err, ecp);
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
	fdb_bool_t out_present;
	uint8_t const* out_value;
	int out_value_length;
	FDBFuture* f;
	const FDBKeyValue *out_kv;
	int out_count;
	fdb_bool_t out_more;
	uint8_t *endkeybuf;
	int keylen = RDB_fdb_key_prefix_length(curp->recmapp);

	/* Allocate and fill buffer for end key */
	endkeybuf = RDB_alloc(keylen, ecp);
	if (endkeybuf == NULL) {
		return RDB_ERROR;
	}
	strcpy((char *)endkeybuf, "t/");
	strcat((char *)endkeybuf, curp->recmapp->namp);
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
		RDB_fdb_errcode_to_error(err, ecp);
		return RDB_ERROR;
	}
	err = fdb_future_get_keyvalue_array(f, &out_kv, &out_count, &out_more);
	if (err != 0) {
		fdb_future_destroy(f);
		RDB_fdb_errcode_to_error(err, ecp);
		return RDB_ERROR;
	}

	if (out_count == 0) {
		fdb_future_destroy(f);
		RDB_raise_not_found("no next record", ecp);
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
RDB_fdb_cursor_prev(RDB_cursor *curp, RDB_exec_context *ecp)
{
    RDB_raise_not_supported("cursors not available", ecp);
	return RDB_ERROR;
}

/*
 * Move the cursor to the position specified by keyv.
 */
int
RDB_fdb_cursor_seek(RDB_cursor *curp, int fieldc, RDB_field keyv[], int flags,
        RDB_exec_context *ecp)
{
    RDB_raise_not_supported("cursors not available", ecp);
	return RDB_ERROR;
}
