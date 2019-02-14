/*
 * Sequence functions
 *
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 */

#include "fdbsequence.h"
#include <rec/sequenceimpl.h>
#include <obj/excontext.h>
#include <gen/strfns.h>

#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

RDB_sequence *
RDB_open_fdb_sequence(const char *cname, const char *filename,
        RDB_environment *envp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    uint8_t *key;
    int key_length;
    uint8_t const* out_value;
    int out_value_length;
    fdb_bool_t present;
    fdb_error_t err;
    FDBFuture *f;
    RDB_sequence *seqp = malloc(sizeof(RDB_sequence));
    if (seqp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    seqp->cnamp = RDB_dup_str(cname);
    if (seqp->cnamp == NULL) {
        free(seqp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    seqp->close_fn = &RDB_close_fdb_sequence;
    seqp->next_fn = &RDB_fdb_sequence_next;
    seqp->delete_sequence_fn = &RDB_delete_fdb_sequence;

    /* Check if sequence exists */
    key_length = 2 + strlen(cname);
    key = RDB_alloc(key_length, ecp);
    if (key == NULL)
        goto error;
    key[0] = 's';
    key[1] = '/';
    memcpy(key + 2, cname, strlen(cname));
    f = fdb_transaction_get((FDBTransaction*)rtxp, key, key_length, 0);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        goto error;
    }
    err = fdb_future_get_value(f, &present, &out_value, &out_value_length);
    fdb_future_destroy(f);
    if (err != 0) {
        RDB_fdb_errcode_to_error(err, ecp);
        goto error;
    }
    if (!present) {
        /* Insert start value */
        RDB_int value = (RDB_int)0;

        fdb_transaction_set((FDBTransaction*)rtxp, key, key_length,
            (uint8_t *)&value, sizeof(RDB_int));
    }

    RDB_free(key);
    return seqp;

error:
    RDB_free(key);
    free(seqp->cnamp);
    free(seqp);
    return NULL;
}

int
RDB_close_fdb_sequence(RDB_sequence *seqp, RDB_exec_context *ecp)
{
    free(seqp->cnamp);
    free(seqp);
    return RDB_OK;
}

/*
 * Delete the sequence.
 */
int
RDB_delete_fdb_sequence(RDB_sequence *seqp, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    int key_length = 2 + strlen(seqp->cnamp);
    uint8_t *key = RDB_alloc(key_length, ecp);
    if (key == NULL)
        return RDB_ERROR;
    key[0] = 's';
    key[1] = '/';
    memcpy(key + 2, seqp->cnamp, strlen(seqp->cnamp));
    fdb_transaction_clear((FDBTransaction *)rtxp, key, key_length);
    free(seqp->cnamp);
    free(seqp);
    return RDB_OK;
}

int
RDB_fdb_sequence_next(RDB_sequence *seqp, RDB_rec_transaction *rtxp, RDB_int *valp,
    RDB_exec_context *ecp)
{
    uint8_t *key;
    RDB_int val;
    uint8_t const* out_value;
    int out_value_length;
    fdb_bool_t present;
    fdb_error_t err;
    FDBFuture *f;
    RDB_int *value;
    int key_length = 2 + strlen(seqp->cnamp);
    
    key = RDB_alloc(key_length, ecp);
    if (key == NULL)
        goto error;
    key[0] = 's';
    key[1] = '/';
    memcpy(key + 2, seqp->cnamp, strlen(seqp->cnamp));
    f = fdb_transaction_get((FDBTransaction*)rtxp, key, key_length, 0);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        goto error;
    }
    err = fdb_future_get_value(f, &present, &out_value, &out_value_length);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        goto error;
    }
    if (!present || out_value_length != sizeof(RDB_int)) {
        fdb_future_destroy(f);
        RDB_raise_internal("sequence not found or invalid", ecp);
        goto error;
    }
    memcpy(&val, out_value, sizeof(RDB_int));
    *valp = val + 1;
    fdb_transaction_set((FDBTransaction*)rtxp, key, key_length,
            (uint8_t *)valp, sizeof(RDB_int));
    RDB_free(key);
    return RDB_OK;

error:
    RDB_free(key);
    return RDB_ERROR;
}

int
RDB_rename_fdb_sequence(const char *oldname, const char *newname,
        const char *filename, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    uint8_t const* out_value;
    int out_value_length;
    fdb_bool_t present;
    fdb_error_t err;
    FDBFuture *f;
    int to_key_length = 2 + strlen(newname);
    uint8_t *to_key = NULL;
    int from_key_length = 2 + strlen(oldname);
    uint8_t *from_key = RDB_alloc(from_key_length, ecp);
    if (from_key == NULL)
        return RDB_ERROR;
    from_key[0] = 's';
    from_key[1] = '/';
    memcpy(from_key + 2, oldname, strlen(oldname));

    to_key = RDB_alloc(to_key_length, ecp);
    if (to_key == NULL)
        return RDB_ERROR;
    to_key[0] = 's';
    to_key[1] = '/';
    memcpy(to_key + 2, newname, strlen(newname));

    f = fdb_transaction_get((FDBTransaction*)rtxp, from_key, from_key_length, 0);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        goto error;
    }
    err = fdb_future_get_value(f, &present, &out_value, &out_value_length);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_fdb_errcode_to_error(err, ecp);
        goto error;
    }
    if (!present || out_value_length != sizeof(RDB_int)) {
        fdb_future_destroy(f);
        RDB_raise_resource_not_found("sequence not found or invalid", ecp);
        goto error;
    }
    fdb_transaction_clear((FDBTransaction *)rtxp, from_key, from_key_length);
    fdb_transaction_set((FDBTransaction*)rtxp, to_key, to_key_length,
            out_value, sizeof(RDB_int));
    fdb_future_destroy(f);

    free(from_key);
    free(to_key);
    return RDB_OK;

error:
    free(from_key);
    free(to_key);
    return RDB_ERROR;
}
