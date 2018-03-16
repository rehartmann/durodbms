/*
 * Record map functions implemented using a binary search tree.
 * 
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "treerecmap.h"
#include "tree.h"
#include <rec/dbdefs.h>
#include <rec/recmapimpl.h>
#include <bdbrec/bdbrecmap.h>
#include <obj/excontext.h>
#include <gen/strfns.h>
#include <treerec/treerecmap.h>
#include <treerec/treecursor.h>

#include <errno.h>
#include <string.h>

/*
 * Allocate a RDB_recmap structure and initialize its fields.
 * The underlying BDB database is created using db_create(), but not opened.
 */
static RDB_recmap *
new_tree_recmap(int fieldc, const RDB_field_info fieldinfov[],
        int keyfieldc, int flags, RDB_exec_context *ecp)
{
    RDB_recmap *rmp = RDB_new_recmap(NULL, NULL, NULL, fieldc, fieldinfov,
            keyfieldc, flags, ecp);
    if (rmp == NULL)
        return NULL;

    rmp->close_recmap_fn = RDB_close_tree_recmap;
    rmp->delete_recmap_fn = &RDB_delete_tree_recmap;
    rmp->insert_rec_fn = &RDB_insert_tree_rec;
    rmp->update_rec_fn = &RDB_update_tree_rec;
    rmp->delete_rec_fn = &RDB_delete_tree_rec;
    rmp->get_fields_fn = &RDB_get_tree_fields;
    rmp->contains_rec_fn = &RDB_contains_tree_rec;
    rmp->recmap_est_size_fn = &RDB_tree_recmap_est_size;
    rmp->cursor_fn = &RDB_tree_recmap_cursor;

    return rmp;
}

/*
 * Comparison function for binary search trees.
 * Compares records by comparing the fields.
 */
static int
compare_key(const void *d1, size_t size1,
        const void *d2, size_t size2, void *arg)
{
    int i;
    int res;
    RDB_recmap *rmp = (RDB_recmap *) arg;

    for (i = 0; i < rmp->cmpc; i++) {
        int offs1, offs2;
        size_t len1, len2;
        void *data1p, *data2p;

        offs1 = RDB_get_field(rmp, rmp->cmpv[i].fno, d1, size1, &len1, NULL);
        if (offs1 < 0)
            return offs1;
        offs2 = RDB_get_field(rmp, rmp->cmpv[i].fno, d2, size2, &len2, NULL);
        if (offs2 < 0)
            return offs2;
        data1p = ((RDB_byte *) d1) + offs1;
        data2p = ((RDB_byte *) d2) + offs2;

        /* Compare field */
        res = (*rmp->cmpv[i].comparep)(data1p, len1, data2p, len2,
                rmp->envp, rmp->cmpv[i].arg);

        /* If the fields are different, we're done */
        if (res != 0) {
            /* If order is descending, revert result */
            if (!rmp->cmpv[i].asc)
                res = -res;
            return res;
        }
    }
    if (rmp->cmpc == rmp->fieldcount)
        return 0;

    res = memcmp(d1, d2, size1 <= size2 ? size1 : size2);
    if (res != 0)
        return res;

    return abs(size1 - size2);
}

RDB_recmap *
RDB_create_tree_recmap(const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
        int keyfieldc, int cmpc, const RDB_compare_field cmpv[], int flags,
        int keyc, const RDB_string_vec *keyv,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_recmap *rmp;
    int i;

    /* Allocate and initialize RDB_recmap structure */

    rmp = new_tree_recmap(fieldc, fieldinfov, keyfieldc, flags, ecp);
    if (rmp == NULL) {
        return NULL;
    }

    if (cmpc > 0) {
        rmp->cmpc = cmpc;
        rmp->cmpv = malloc(sizeof (RDB_compare_field) * cmpc);
        if (rmp->cmpv == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        for (i = 0; i < cmpc; i++) {
            rmp->cmpv[i].fno = cmpv[i].fno;
            rmp->cmpv[i].comparep = cmpv[i].comparep;
            rmp->cmpv[i].arg = cmpv[i].arg;
            rmp->cmpv[i].asc = cmpv[i].asc;
        }
    }

    if (!(RDB_UNIQUE & flags)) {
        RDB_raise_not_supported("RDB_UNIQUE is required", ecp);
        goto error;
    }

    /* Create tree */
    rmp->impl.treep = RDB_tree_create(compare_key, rmp, ecp);
    if (rmp->impl.treep == NULL)
        goto error;

    return rmp;

error:
    if (rmp != NULL) {
        RDB_free(rmp->fieldinfos);
        RDB_free(rmp->cmpv);
        RDB_free(rmp);
    }
    return NULL;
}

int
RDB_close_tree_recmap(RDB_recmap *rmp, RDB_exec_context *ecp)
{
    return RDB_delete_tree_recmap(rmp, NULL, ecp);
}

int
RDB_delete_tree_recmap(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_tree_delete(rmp->impl.treep);

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
RDB_insert_tree_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    size_t keylen;
    void *key = NULL;
    size_t valuelen;
    void *value = NULL;
    int ret;

    ret = key_to_mem(rmp, flds, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    ret = value_to_mem(rmp, flds, &value, &valuelen);
    if (ret != RDB_OK) {
        free(key);
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    ret = RDB_tree_insert(rmp->impl.treep, key, keylen, value, valuelen, ecp);
    free(key);
    free(value);
    return ret;
}

int
RDB_update_tree_rec(RDB_recmap *rmp, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], RDB_rec_transaction *rtxp,
               RDB_exec_context *ecp)
{
    RDB_raise_not_supported("", ecp);
    return RDB_ERROR;
}

int
RDB_delete_tree_rec(RDB_recmap *rmp, int fieldc, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_raise_not_supported("", ecp);
    return RDB_ERROR;
}

int
RDB_get_tree_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    size_t keylen;
    void *key;
    size_t valuelen;
    void *value;
    int ret;

    ret = key_to_mem(rmp, keyv, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    value = RDB_tree_get(rmp->impl.treep, key, keylen, &valuelen);
    if (value == NULL) {
        free(key);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    ret = RDB_get_mem_fields(rmp, key, keylen, value, valuelen, fieldc, retfieldv);
    free(key);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_contains_tree_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_raise_not_supported("", ecp);
    return RDB_ERROR;
}

int
RDB_tree_recmap_est_size(RDB_recmap *rmp, RDB_rec_transaction *rtxp, unsigned *sz,
        RDB_exec_context *ecp)
{
    *sz = 1;
    return RDB_OK;
}
