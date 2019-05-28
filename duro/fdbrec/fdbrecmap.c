/*
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "fdbrecmap.h"
#include "fdbenv.h"
#include "fdbcursor.h"
#include "fdbindex.h"
#include <rec/dbdefs.h>
#include <obj/excontext.h>
#include <rec/recmapimpl.h>
#include <rec/indeximpl.h>
#include <treerec/field.h>
#include <treerec/treerecmap.h>

#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

FDBFuture *RDB_fdb_resultf = NULL;
uint8_t *RDB_fdb_key_name;

/*
 * Allocate a RDB_recmap structure and initialize its fields for FDB.
 */
static RDB_recmap *
new_recmap(const char *namp, const char *filenamp,
	RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
	int keyfieldc, int flags, RDB_exec_context *ecp)
{
	int ret;
	RDB_recmap *rmp = RDB_new_recmap(namp, filenamp, envp, fieldc, fieldinfov,
		keyfieldc, flags, ecp);
	if (rmp == NULL)
		return NULL;

    rmp->indexes = NULL;
	rmp->close_recmap_fn = RDB_close_fdb_recmap;
	rmp->delete_recmap_fn = &RDB_delete_fdb_recmap;
	rmp->insert_rec_fn = &RDB_insert_fdb_rec;
	rmp->update_rec_fn = &RDB_update_fdb_rec;
	rmp->delete_rec_fn = &RDB_delete_fdb_rec;
	rmp->get_fields_fn = &RDB_get_fdb_fields;
	rmp->contains_rec_fn = &RDB_contains_fdb_rec;
	rmp->recmap_est_size_fn = &RDB_fdb_recmap_est_size;
	rmp->cursor_fn = &RDB_fdb_recmap_cursor;
	rmp->create_index_fn = &RDB_create_fdb_index;
	rmp->open_index_fn = &RDB_open_fdb_index;

	return rmp;
}

RDB_recmap *
RDB_create_fdb_recmap(const char *name, const char *filename,
	RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
	int keyfieldc, int cmpc, const RDB_compare_field cmpv[], int flags,
	int keyc, const RDB_string_vec *keyv,
	RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
	if (cmpv != NULL) {
		RDB_raise_not_supported("comparison function not supported", ecp);
		return NULL;
	}
	return new_recmap(name, filename, envp, fieldc, fieldinfov,
		keyfieldc, flags, ecp);
}

RDB_recmap *
RDB_open_fdb_recmap(const char *name, const char *filename,
	RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
	int keyfieldc, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    return new_recmap(name, filename, envp, fieldc, fieldinfov,
		keyfieldc, RDB_UNIQUE, ecp);
}

int
RDB_close_fdb_recmap(RDB_recmap *rmp, RDB_exec_context *ecp)
{
    RDB_free(rmp->namp);
    RDB_free(rmp->filenamp);
    RDB_free(rmp->fieldinfos);
    RDB_free(rmp->cmpv);
    RDB_free(rmp);
	return RDB_OK;
}

int
RDB_delete_fdb_recmap(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    uint8_t *keybuf;
    uint8_t *endkeybuf;
    int keylen = RDB_fdb_key_prefix_length(rmp);

    /* Allocate and fill buffer for start key and end key */
    keybuf = RDB_alloc(keylen, ecp);
    if (keybuf == NULL)
        return RDB_ERROR;
    endkeybuf = RDB_alloc(keylen, ecp);
    if (endkeybuf == NULL) {
        RDB_free(keybuf);
        return RDB_ERROR;
    }
    strcpy((char *)keybuf, "t/");
    strcat((char *)keybuf, rmp->namp);
    strcpy((char *)endkeybuf, (char *)keybuf);
    keybuf[keylen - 1] = (uint8_t) '/';
    endkeybuf[keylen - 1] = (uint8_t) '/' + 1;

    /* Delete all key/value pairs of recmap */
    fdb_transaction_clear_range((FDBTransaction*)rtxp,
            keybuf, keylen, endkeybuf, keylen);
    RDB_free(keybuf);
    RDB_free(endkeybuf);

    RDB_free(rmp->namp);
    RDB_free(rmp->filenamp);
    RDB_free(rmp->fieldinfos);
    RDB_free(rmp->cmpv);
    RDB_free(rmp);
    return RDB_OK;
}

static int
key_to_mem(RDB_recmap *rmp, RDB_field fldv[], void **keyp, size_t *keylen)
{
	int i;

	for (i = 0; i < rmp->keyfieldcount; i++) {
		fldv[i].no = i;
	}

	return RDB_fields_to_mem(rmp, rmp->keyfieldcount, fldv, keyp, keylen);
}

static int
value_to_mem(RDB_recmap *rmp, RDB_field fldv[], void **valuep, size_t *valuelen)
{
	int i;

	for (i = rmp->keyfieldcount; i < rmp->fieldcount; i++) {
		fldv[i].no = i;
	}

	return RDB_fields_to_mem(rmp, rmp->fieldcount - rmp->keyfieldcount,
		fldv + rmp->keyfieldcount, valuep, valuelen);
}

int
RDB_fdb_key_prefix_length(RDB_recmap *rmp)
{
	return strlen(rmp->namp) + 3;
}

int
RDB_fdb_key_index_prefix_length(RDB_index *ixp)
{
    return strlen(ixp->namp) + 3;
}

uint8_t *
RDB_fdb_prepend_key_prefix(RDB_recmap *rmp, const void *key, size_t keylen, RDB_exec_context *ecp)
{
	size_t namelen = strlen(rmp->namp);
    uint8_t *key_name = RDB_alloc(RDB_fdb_key_prefix_length(rmp) + keylen, ecp);
    if (key_name == NULL)
        return NULL;

	memcpy(key_name, "t/", 2);
	memcpy(key_name + 2, rmp->namp, namelen);
	key_name[2 + namelen] = '/';
	memcpy(key_name + 3 + namelen, key, keylen);
    return key_name;
}

uint8_t *
RDB_fdb_prepend_key_index_prefix(RDB_index *ixp, const void *key, size_t keylen, RDB_exec_context *ecp)
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

static int
fields_to_fdb_key(RDB_recmap *rmp, RDB_field fieldv[],
    uint8_t **key_name, int *key_name_length, RDB_exec_context *ecp)
{
    void *key;
    size_t keylen;
    int ret = key_to_mem(rmp, fieldv, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    *key_name_length = RDB_fdb_key_prefix_length(rmp) + keylen;
    *key_name = RDB_fdb_prepend_key_prefix(rmp, key, keylen, ecp);
    if (*key_name == NULL) {
        free(key);
        return RDB_ERROR;
    }
    free(key);
    return RDB_OK;
}

static int
insert_into_fdb_indexes(RDB_recmap *rmp, uint8_t *key, int key_length, void *value, int valuelen,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_index *ixp;
    int ret;
    uint8_t **key_name;
    int *key_name_length;
    int i;
    int idxcount = 0;
    for (ixp = rmp->indexes; ixp != NULL; ixp = ixp->nextp) {
        idxcount++;
    }
    key_name = RDB_alloc(sizeof(uint8_t *) * idxcount, ecp);
    if (key_name == NULL)
        return RDB_ERROR;
    key_name_length = RDB_alloc(sizeof(int) * idxcount, ecp);
    if (key_name_length == NULL) {
        RDB_free(key_name);
        return RDB_ERROR;
    }
    for (i = 0; i < idxcount; i++) {
        key_name[i] = NULL;
    }

    for (i = 0, ixp = rmp->indexes; ixp != NULL; ixp = ixp->nextp, i++) {
        void *skey = NULL;
        size_t skeylen;

        ret = RDB_make_skey(ixp, key, (size_t)key_length, value, (size_t)valuelen, &skey, &skeylen);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            goto error;
        }

        key_name_length[i] = skeylen + RDB_fdb_key_index_prefix_length(ixp);
        key_name[i] = RDB_fdb_prepend_key_index_prefix(ixp, skey, skeylen, ecp);
        RDB_free(skey);
        if (key_name[i] == NULL) {
            goto error;
        }

        if (RDB_UNIQUE & ixp->flags) {
            RDB_bool exists;
            ret = RDB_fdb_key_exists(key_name[i], key_name_length[i], (FDBTransaction*)rtxp,
                ecp, &exists);
            if (ret != RDB_OK) {
                goto error;
            }
            if (exists) {
                RDB_raise_key_violation("", ecp);
                goto error;
            }
        }
    }

    for (i = 0, ixp = rmp->indexes; ixp != NULL; ixp = ixp->nextp, i++) {
        fdb_transaction_set((FDBTransaction*)rtxp, key_name[i], key_name_length[i],
                key, key_length);
        RDB_free(key_name[i]);
    }
    RDB_free(key_name);
    RDB_free(key_name_length);
    return RDB_OK;

error:
    for (i = 0; i < idxcount; i++) {
        RDB_free(key_name[i]);
    }
    RDB_free(key_name);
    RDB_free(key_name_length);
    return RDB_ERROR;
}

static int
RDB_fdb_key_exists(uint8_t *key_name, int key_name_length, FDBTransaction *tx,
        RDB_exec_context *ecp, RDB_bool *resultp)
{
    fdb_bool_t present;
    uint8_t* out_value;
    int out_value_length;
    FDBFuture *f = fdb_transaction_get(tx, key_name, key_name_length, 0);
    fdb_error_t err = fdb_future_block_until_ready(f);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, tx);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(f, &present, &out_value, &out_value_length);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, tx);
        return RDB_ERROR;
    }
    *resultp = present ? RDB_TRUE : RDB_FALSE;
    fdb_future_destroy(f);
    return RDB_OK;
}

int
RDB_insert_fdb_rec(RDB_recmap *rmp, RDB_field fieldv[], RDB_rec_transaction *rtxp,
	RDB_exec_context *ecp)
{
	size_t valuelen;
	void *value = NULL;
	uint8_t *key_name;
	int key_name_length;
	int ret;
    uint8_t* out_value;
    int out_value_length;
    fdb_bool_t present;
    fdb_error_t err;
    int prefix_length;
    RDB_bool exists;

    if (fields_to_fdb_key(rmp, fieldv, &key_name, &key_name_length, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    /* Check if the key already exists */
    ret = RDB_fdb_key_exists(key_name, key_name_length, (FDBTransaction*)rtxp,
            ecp, &exists);
    if (ret != RDB_OK) {
        RDB_free(key_name);
        return RDB_ERROR;
    }
    if (exists) {
        RDB_free(key_name);
        RDB_raise_key_violation("", ecp);
        return RDB_ERROR;
    }

    ret = value_to_mem(rmp, fieldv, &value, &valuelen);
	if (ret != RDB_OK) {
        RDB_free(key_name);
        RDB_errcode_to_error(ret, ecp);
		return RDB_ERROR;
	}

    prefix_length = RDB_fdb_key_prefix_length(rmp);
    if (insert_into_fdb_indexes(rmp, key_name + prefix_length, key_name_length - prefix_length, value, valuelen, rtxp, ecp) != RDB_OK) {
        RDB_free(key_name);
        if (valuelen > 0)
            free(value);
        return RDB_ERROR;
    }
    fdb_transaction_set((FDBTransaction*)rtxp, key_name, key_name_length,
        (uint8_t *)value, valuelen);
    RDB_free(key_name);
    if (valuelen > 0)
        free(value);
    return RDB_OK;
}

static int
RDB_update_fdb_kv(RDB_recmap *rmp, uint8_t *key_name, int key_name_length,
        void **key, size_t *key_length, void **data, size_t *data_length,
        int fieldc, const RDB_field fieldv[],
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    fdb_error_t err;
    RDB_bool exists;

    if (RDB_recmap_is_key_update(rmp, fieldc, fieldv)) {
        uint8_t *new_key;
        int prefixlen = RDB_fdb_key_prefix_length(rmp);
        /*
         * Delete entry and insert modified entry
         */
        if (RDB_delete_fdb_kv(rmp, key_name, key_name_length, rtxp, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        for (i = 0; i < fieldc; i++) {
            if (fieldv[i].no < rmp->keyfieldcount) {
                ret = RDB_set_field_mem(rmp, key, key_length, &fieldv[i],
                    rmp->varkeyfieldcount);
            } else {
                ret = RDB_set_field_mem(rmp, data, data_length, &fieldv[i],
                    rmp->vardatafieldcount);
            }
            if (ret != RDB_OK) {
                return RDB_ERROR;
            }
        }
        new_key = RDB_fdb_prepend_key_prefix(rmp, *key, *key_length, ecp);
        if (new_key == NULL) {
            return RDB_ERROR;
        }
        
        /* Check if key exists */
        if (RDB_fdb_key_exists(new_key, (int)*key_length + prefixlen, (FDBTransaction *)rtxp,
                ecp, &exists) != RDB_OK) {
            RDB_free(new_key);
            return RDB_ERROR;
        }
        if (exists) {
            RDB_raise_key_violation("", ecp);
            return RDB_ERROR;
        }
        
        if (insert_into_fdb_indexes(rmp, new_key + prefixlen, *key_length,
                *data, (int)*data_length, rtxp, ecp) != RDB_OK) {
            RDB_free(new_key);
            return RDB_ERROR;
        }
        fdb_transaction_set((FDBTransaction *)rtxp, new_key, (int)*key_length + prefixlen,
            (uint8_t *)*data, (int)*data_length);
        RDB_free(new_key);
    } else {
        for (i = 0; i < fieldc; i++) {
            if (fieldv[i].no < rmp->keyfieldcount) {
                ret = RDB_set_field_mem(rmp, key, key_length, &fieldv[i],
                    rmp->varkeyfieldcount);
            }
            else {
                ret = RDB_set_field_mem(rmp, data, data_length, &fieldv[i],
                    rmp->vardatafieldcount);
            }
            if (ret != RDB_OK)
                return ret;
        }
        /* Write record back */
        fdb_transaction_set((FDBTransaction *)rtxp, key_name, key_name_length,
            (uint8_t *)*data, (int)*data_length);
    }

    return RDB_OK;
}

int
RDB_update_fdb_rec(RDB_recmap *rmp, RDB_field keyv[],
	int fieldc, const RDB_field fieldv[], RDB_rec_transaction *rtxp,
	RDB_exec_context *ecp)
{
    int ret;
    uint8_t *key_name;
    int key_name_length;
    uint8_t *value;
    int value_length;
    size_t key_length;
    void *key;
    size_t data_length;
    void *data;
    fdb_bool_t present;
    fdb_error_t err;
    FDBFuture* f;
    int prefix_length = RDB_fdb_key_prefix_length(rmp);

    if (fields_to_fdb_key(rmp, keyv, &key_name, &key_name_length, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    f = fdb_transaction_get((FDBTransaction*)rtxp, key_name, key_name_length, 0);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(f, &present, &value, &value_length);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        return RDB_ERROR;
    }
    if (!present) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    key_length = key_name_length - prefix_length;
    if (key_length > 0) {
        key = malloc(key_length);
        if (key == NULL) {
            RDB_free(key_name);
            fdb_future_destroy(f);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        memcpy(key, key_name + prefix_length, key_length);
    } else {
        key = NULL;
    }
    data_length = value_length;
    if (data_length > 0) {
        data = malloc(data_length);
        if (data == NULL) {
            RDB_free(key_name);
            fdb_future_destroy(f);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        memcpy(data, value, data_length);
    } else {
        data = NULL;
    }
    fdb_future_destroy(f);

    ret = RDB_update_fdb_kv(rmp, key_name, key_name_length, &key, &key_length,
            &data, &data_length, fieldc, fieldv, rtxp, ecp);
    RDB_free(key_name);
    free(key);
    free(data);
    return ret;
}

int
RDB_delete_from_fdb_indexes(RDB_recmap *rmp, uint8_t *key, int key_length,
    uint8_t *value, int value_length, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    void *skey = NULL;
    size_t skeylen;
    RDB_index *ixp;
    int key_name_length;
    uint8_t *key_name;
    int prefixlen = RDB_fdb_key_prefix_length(rmp);
    int ret;

    for (ixp = rmp->indexes; ixp != NULL; ixp = ixp->nextp) {
        ret = RDB_make_skey(ixp, key + prefixlen, (size_t)(key_length - prefixlen),
                value, (size_t)value_length, &skey, &skeylen);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }
        key_name_length = skeylen + RDB_fdb_key_index_prefix_length(ixp);
        key_name = RDB_fdb_prepend_key_index_prefix(ixp, skey, skeylen, ecp);
        RDB_free(skey);
        if (key_name == NULL) {
            return RDB_ERROR;
        }
        fdb_transaction_clear((FDBTransaction*)rtxp, key_name, key_name_length);
    }
    return RDB_OK;
}

int
RDB_delete_fdb_kv(RDB_recmap *rmp, uint8_t *key_name, int key_name_length,
    RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    if (rmp->indexes != NULL) {
        uint8_t *value;
        int value_length;
        fdb_bool_t present;
        fdb_error_t err;
        FDBFuture* f = fdb_transaction_get((FDBTransaction*)rtxp, key_name, key_name_length, 0);
        err = fdb_future_block_until_ready(f);
        if (err != 0) {
            /* Rollback .. */
            RDB_free(key_name);
            fdb_future_destroy(f);
            RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
            return RDB_ERROR;
        }
        err = fdb_future_get_value(f, &present, &value, &value_length);
        if (err != 0) {
            RDB_free(key_name);
            fdb_future_destroy(f);
            RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
            return RDB_ERROR;
        }
        if (present) {
            if (RDB_delete_from_fdb_indexes(rmp, key_name, key_name_length,
                value, value_length, rtxp, ecp) != RDB_OK) {
                /* Rollback */
                return RDB_ERROR;
            }
        }
        fdb_future_destroy(f);
    }
    fdb_transaction_clear((FDBTransaction*)rtxp, key_name, key_name_length);
    return RDB_OK;
}

int
RDB_delete_fdb_rec(RDB_recmap *rmp, int fieldc, RDB_field keyv[],
	RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    uint8_t *key_name;
    int key_name_length;
    int ret;

    if (fields_to_fdb_key(rmp, keyv, &key_name, &key_name_length, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    ret = RDB_delete_fdb_kv(rmp, key_name, key_name_length, rtxp, ecp);
    RDB_free(key_name);
	return RDB_OK;
}

int
RDB_get_fdb_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc,
	RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    int key_name_length;
    int ret;
    uint8_t *value;
    int value_length;
    fdb_bool_t present;
    fdb_error_t err;

    if (RDB_fdb_key_name == NULL) {
        RDB_free(RDB_fdb_key_name);
        RDB_fdb_key_name = NULL;
    }
    if (fields_to_fdb_key(rmp, keyv, &RDB_fdb_key_name, &key_name_length, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

	if (RDB_fdb_resultf != NULL) {
        fdb_future_destroy(RDB_fdb_resultf);
    }
    RDB_fdb_resultf = fdb_transaction_get((FDBTransaction*)rtxp, RDB_fdb_key_name, key_name_length, 0);
    err = fdb_future_block_until_ready(RDB_fdb_resultf);
    if (err != 0) {
        RDB_free(RDB_fdb_key_name);
        RDB_fdb_key_name = NULL;
        fdb_future_destroy(RDB_fdb_resultf);
		RDB_fdb_resultf = NULL;
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(RDB_fdb_resultf, &present, &value, &value_length);
    if (err != 0) {
        RDB_free(RDB_fdb_key_name);
        RDB_fdb_key_name = NULL;
        fdb_future_destroy(RDB_fdb_resultf);
		RDB_fdb_resultf = NULL;
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        return RDB_ERROR;
    }
    if (!present) {
        RDB_free(RDB_fdb_key_name);
        RDB_fdb_key_name = NULL;
        fdb_future_destroy(RDB_fdb_resultf);
		RDB_fdb_resultf = NULL;
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    int prefix_len = RDB_fdb_key_prefix_length(rmp);
    ret = RDB_get_mem_fields(rmp, RDB_fdb_key_name + prefix_len, (size_t)(key_name_length - prefix_len),
            value, (size_t)value_length, fieldc, retfieldv);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_contains_fdb_rec(RDB_recmap *rmp, RDB_field fieldv[], RDB_rec_transaction *rtxp,
	RDB_exec_context *ecp)
{
    size_t valuelen;
    void *value;
    uint8_t *key_name;
    int key_name_length;
    int ret;
    uint8_t const* out_value;
    int out_value_length;
    fdb_bool_t present;
    fdb_error_t err;
    FDBFuture* f;

    if (fields_to_fdb_key(rmp, fieldv, &key_name, &key_name_length, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    f = fdb_transaction_get((FDBTransaction*)rtxp, key_name, key_name_length, 0);
    err = fdb_future_block_until_ready(f);
    RDB_free(key_name);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(f, &present, &out_value, &out_value_length);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        return RDB_ERROR;
    }
    if (!present) {
        fdb_future_destroy(f);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    ret = value_to_mem(rmp, fieldv, &value, &valuelen);
    if (ret != RDB_OK) {
        fdb_future_destroy(f);
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    if (out_value_length != valuelen
        || memcmp(out_value, value, valuelen) != 0) {
        fdb_future_destroy(f);
        free(value);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }
    fdb_future_destroy(f);
    free(value);
    return RDB_OK;
}

int
RDB_fdb_recmap_est_size(RDB_recmap *rmp, RDB_rec_transaction *rtxp, unsigned *sz,
	RDB_exec_context *ecp)
{
	*sz = 0;
	return RDB_OK;
}
