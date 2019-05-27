/*
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "fdbindex.h"
#include "fdbenv.h"
#include "fdbrecmap.h"
#include <rec/dbdefs.h>
#include <rec/indeximpl.h>
#include <rec/recmapimpl.h>
#include <gen/strfns.h>
#include <obj/excontext.h>

#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

static RDB_index *
new_fdb_index(RDB_recmap *rmp, const char *name,
    int fieldc, const int fieldv[],
    int flags, RDB_exec_context *ecp)
{
    int i;
    RDB_index *ixp;
    RDB_bool all_cmpfn = RDB_TRUE;

    if (!(RDB_UNIQUE & flags)) {
        RDB_raise_invalid_argument("non-unique index not supported", ecp);
        return NULL;
    }

    ixp = RDB_alloc(sizeof(RDB_index), ecp);
    if (ixp == NULL) {
        return NULL;
    }
    ixp->fieldv = NULL;
    ixp->rmp = rmp;
    ixp->flags = flags;

    ixp->namp = RDB_dup_str(name);
    if (ixp->namp == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    ixp->filenamp = NULL;

    ixp->fieldc = fieldc;
    ixp->fieldv = malloc(fieldc * sizeof(int));
    if (ixp->fieldv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    for (i = 0; i < fieldc; i++)
        ixp->fieldv[i] = fieldv[i];

    ixp->close_index_fn = &RDB_close_fdb_index;
    ixp->delete_index_fn = &RDB_delete_fdb_index;
    ixp->index_delete_rec_fn = &RDB_fdb_index_delete_rec;
    ixp->index_get_fields_fn = &RDB_fdb_index_get_fields;
    ixp->index_cursor_fn = NULL; /* Not needed because there are only unique indexes */

    return ixp;

error:
    free(ixp->fieldv);
    free(ixp);
    return NULL;
}

/*
 * Create index.
 */
RDB_index *
RDB_create_fdb_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const RDB_field_descriptor fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_index *ixp = NULL;
    int i;

    if (cmpv != NULL) {
        RDB_raise_not_supported("comparison function not supported", ecp);
        return NULL;
    }
    int *fnov = RDB_alloc(fieldc * sizeof(int), ecp);
    if (fnov == NULL)
        goto error;
    for (i = 0; i < fieldc; i++) {
        fnov[i] = fieldv[i].no;
    }
    ixp = new_fdb_index(rmp, namp, fieldc, fnov, flags, ecp);
    RDB_free(fnov);
    if (ixp == NULL)
        return NULL;

    /* Associate index with recmap */
    ixp->nextp = rmp->indexes;
    rmp->indexes = ixp;

    return ixp;

error:
    if (ixp != NULL)
        RDB_close_fdb_index(ixp, ecp);
    return NULL;
}

RDB_index *
RDB_open_fdb_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_index *ixp = new_fdb_index(rmp, namp, fieldc, fieldv, flags, ecp);
    if (ixp == NULL)
        return NULL;

    /* Associate index with recmap */
    ixp->nextp = rmp->indexes;
    rmp->indexes = ixp;

    return ixp;

error:
    RDB_close_fdb_index(ixp, ecp);
    return NULL;
}

int
RDB_close_fdb_index(RDB_index *ixp, RDB_exec_context *ecp)
{
    free(ixp->namp);
    free(ixp->fieldv);
    free(ixp);

    return RDB_OK;
}

/* Delete an index. */
int
RDB_delete_fdb_index(RDB_index *ixp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    uint8_t *keybuf;
    uint8_t *endkeybuf;
    int keylen = RDB_fdb_key_index_prefix_length(ixp);

    /* Allocate and fill buffer for start key and end key */
    keybuf = RDB_alloc(keylen, ecp);
    if (keybuf == NULL)
        return RDB_ERROR;
    endkeybuf = RDB_alloc(keylen, ecp);
    if (endkeybuf == NULL) {
        RDB_free(keybuf);
        return RDB_ERROR;
    }
    strcpy((char *)keybuf, "i/");
    strcat((char *)keybuf, ixp->namp);
    strcpy((char *)endkeybuf, (char *)keybuf);
    keybuf[keylen - 1] = (uint8_t) '/';
    endkeybuf[keylen - 1] = (uint8_t) '/' + 1;

    /* Delete all key/value pairs of recmap */
    fdb_transaction_clear_range((FDBTransaction*)rtxp, keybuf, keylen,
            endkeybuf, keylen);
    RDB_free(keybuf);
    RDB_free(endkeybuf);

    return RDB_close_fdb_index(ixp, ecp);
}

int
RDB_fdb_index_get_fields(RDB_index *ixp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    size_t keylen;
    void *key;
    uint8_t *key_name;
    int key_name_length;
    uint8_t *pkey;
    int pkey_length;
    int pkey_name_length;
    uint8_t *value;
    int value_length;
    int i;
    fdb_error_t err;
    int ret;
    fdb_bool_t present;
    FDBFuture *f1;
    int recmap_prefix_length = RDB_fdb_key_prefix_length(ixp->rmp);

    for (i = 0; i < ixp->fieldc; i++) {
        keyv[i].no = ixp->fieldv[i];
    }

    ret = RDB_fields_to_mem(ixp->rmp, ixp->fieldc, keyv, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    key_name_length = RDB_fdb_key_index_prefix_length(ixp) + keylen;
    key_name = RDB_fdb_prepend_key_index_prefix(ixp, key, keylen, ecp);
    if (key_name == NULL) {
        free(key);
        return RDB_ERROR;
    }
    free(key);

    f1 = fdb_transaction_get((FDBTransaction*)rtxp, key_name, key_name_length, 0);
    err = fdb_future_block_until_ready(f1);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f1);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        return RDB_ERROR;
    }
    err = fdb_future_get_value(f1, &present, &pkey, &pkey_length);
    if (err != 0) {
        RDB_free(key_name);
        fdb_future_destroy(f1);
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        return RDB_ERROR;
    }
    RDB_free(key_name);
    if (!present) {
        fdb_future_destroy(f1);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (RDB_fdb_key_name != NULL) {
        RDB_free(RDB_fdb_key_name);
    }
    RDB_fdb_key_name = RDB_fdb_prepend_key_prefix(ixp->rmp, pkey, pkey_length, ecp);
    if (RDB_fdb_key_name == NULL) {
        fdb_future_destroy(f1);
        return RDB_ERROR;
    }
    pkey_name_length = pkey_length + recmap_prefix_length;
    if (RDB_fdb_resultf != NULL) {
        fdb_future_destroy(RDB_fdb_resultf);
    }
    RDB_fdb_resultf = fdb_transaction_get((FDBTransaction*)rtxp, RDB_fdb_key_name, pkey_name_length, 0);
    err = fdb_future_block_until_ready(RDB_fdb_resultf);
    fdb_future_destroy(f1);
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

    ret = RDB_get_mem_fields(ixp->rmp, RDB_fdb_key_name + recmap_prefix_length, (size_t) pkey_length,
            value, (size_t) value_length, fieldc, retfieldv);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_fdb_index_delete_rec(RDB_index *ixp, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
	RDB_raise_not_supported("deleting by index not supported", ecp);
    return RDB_ERROR;
}
