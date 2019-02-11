#include "fdbrecmap.h"
#include <rec/dbdefs.h>
#include <obj/excontext.h>
#include <rec/recmapimpl.h>
#include <fdbrec/fdbcursor.h>
#include <fdbrec/fdbindex.h>
#include <treerec/field.h>

#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

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
	free(rmp->namp);
	free(rmp->filenamp);
	free(rmp->fieldinfos);
	free(rmp->cmpv);
	free(rmp);
	return RDB_OK;
}

int
RDB_delete_fdb_recmap(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
	RDB_raise_not_supported("delete recmap", ecp);
	return RDB_ERROR;
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

static uint8_t *
prepend_key_prefix(RDB_recmap *rmp, const void *key, size_t keylen, RDB_exec_context *ecp)
{
	size_t namelen = strlen(rmp->namp);
    uint8_t *key_name = RDB_alloc(namelen + RDB_fdb_key_prefix_length(rmp), ecp);
    if (key_name == NULL)
        return NULL;

	memcpy(key_name, "t/", 2);
	memcpy(key_name + 2, rmp->namp, namelen);
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
    *key_name = prepend_key_prefix(rmp, key, keylen, ecp);
    if (*key_name == NULL) {
        free(key);
        return RDB_ERROR;
    }
    free(key);
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
    uint8_t const* out_value;
    int out_value_length;
    fdb_bool_t present;
    fdb_error_t err;
    FDBFuture* f;

    if (fields_to_fdb_key(rmp, fieldv, &key_name, &key_name_length, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    /* Check if the key already exists */
    f = fdb_transaction_get((FDBTransaction*) rtxp, key_name, key_name_length, 0);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(f, &present, &out_value, &out_value_length);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        return RDB_ERROR;
    }
    if (present) {
        RDB_free(key_name);
        RDB_raise_key_violation("", ecp);
        fdb_future_destroy(f);
        return RDB_ERROR;
    }
    fdb_future_destroy(f);

	ret = value_to_mem(rmp, fieldv, &value, &valuelen);
	if (ret != RDB_OK) {
		RDB_errcode_to_error(ret, ecp);
		return RDB_ERROR;
	}

	fdb_transaction_set((FDBTransaction*) rtxp, key_name, key_name_length,
			(uint8_t *)value, valuelen);
    RDB_free(key_name);
    free(value);
	return RDB_OK;
}

static int
RDB_update_fdb_kv(RDB_recmap *rmp, uint8_t *key_name, int key_name_length,
        void **key, size_t *key_length, void **data, size_t *data_length,
        int fieldc, const RDB_field fieldv[],
        FDBTransaction *txp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    fdb_error_t err;
    RDB_bool modifykey = RDB_FALSE;

    /* Check if the key is to be modified */
    for (i = 0; i < fieldc; i++) {
        if (fieldv[i].no < rmp->keyfieldcount) {
            modifykey = RDB_TRUE;
            break;
        }
    }

    if (modifykey) {
        /* Delete original key */
        fdb_transaction_clear(txp, key_name, key_name_length);
    }

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

    /* If the key changes, check if new key already exists */
    if (modifykey) {
        FDBFuture *f;
        uint8_t const* out_value;
        int out_value_length;
        fdb_bool_t present;

        key_name_length = *key_length + RDB_fdb_key_prefix_length(rmp);
        key_name = prepend_key_prefix(rmp, key, *key_length, ecp);
        if (key_name == NULL)
            return RDB_ERROR;

        f = fdb_transaction_get(txp, key_name, key_name_length, 0);
        err = fdb_future_block_until_ready(f);
        if (err != 0) {
            RDB_free(key_name);
            fdb_future_destroy(f);
            RDB_fdb_errcode_to_error(err, ecp);
            return RDB_ERROR;
        }
        err = fdb_future_get_value(f, &present, &out_value, &out_value_length);
        fdb_future_destroy(f);
        if (err != 0) {
            RDB_free(key_name);
            RDB_fdb_errcode_to_error(err, ecp);
            return RDB_ERROR;
        }
        if (present) {
            RDB_free(key_name);
            RDB_raise_key_violation("", ecp);
            return RDB_ERROR;
        }
    }

    /* Write record back */
    fdb_transaction_set(txp, key_name, key_name_length, (uint8_t *)*data, *data_length);
    if (modifykey)
        RDB_free(key_name);
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
        RDB_fdb_errcode_to_error(err, ecp);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(f, &present, &value, &value_length);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        return RDB_ERROR;
    }
    if (!present) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    key_length = key_name_length - prefix_length;
    key = malloc(key_length - prefix_length);
    if (key == NULL) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    memcpy(key, key_name + prefix_length, key_length);
    data_length = value_length;
    data = malloc(data_length);
    if (data == NULL) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    memcpy(data, value, data_length);
    fdb_future_destroy(f);

    ret = RDB_update_fdb_kv(rmp, key_name, key_name_length, &key, &key_length,
            &data, &data_length,
            fieldc, fieldv, (FDBTransaction*)rtxp, ecp);
    RDB_free(key_name);
    free(key);
    free(data);
    return ret;
}

int
RDB_delete_fdb_rec(RDB_recmap *rmp, int fieldc, RDB_field keyv[],
	RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    uint8_t *key_name;
    int key_name_length;

    if (fields_to_fdb_key(rmp, keyv, &key_name, &key_name_length, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    fdb_transaction_clear((FDBTransaction*) rtxp, key_name, key_name_length);
    RDB_free(key_name);
	return RDB_OK;
}

int
RDB_get_fdb_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc,
	RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    uint8_t *key_name;
    int key_name_length;
    int ret;
    uint8_t *value;
    int value_length;
    fdb_bool_t present;
    fdb_error_t err;
    FDBFuture* f;

    if (fields_to_fdb_key(rmp, keyv, &key_name, &key_name_length, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    f = fdb_transaction_get((FDBTransaction*)rtxp, key_name, key_name_length, 0);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(f, &present, &value, &value_length);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        return RDB_ERROR;
    }
    if (!present) {
        RDB_free(key_name);
        fdb_future_destroy(f);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    int prefix_len = RDB_fdb_key_prefix_length(rmp);
    ret = RDB_get_mem_fields(rmp, key_name + prefix_len, key_name_length - prefix_len,
            value, value_length, fieldc, retfieldv);
    RDB_free(key_name);
    fdb_future_destroy(f);
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
        RDB_fdb_errcode_to_error(err, ecp);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(f, &present, &out_value, &out_value_length);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
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
