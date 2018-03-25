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
#include <rec/indeximpl.h>
#include <bdbrec/bdbrecmap.h>
#include <obj/excontext.h>
#include <gen/strfns.h>
#include <treerec/treerecmap.h>
#include <treerec/treecursor.h>
#include <treerec/treeindex.h>

#include <errno.h>
#include <string.h>

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

    rmp->impl.tree.treep = RDB_create_tree(compare_key, rmp, ecp);
    if (rmp->impl.tree.treep == NULL) {
        RDB_free(rmp->fieldinfos);
        RDB_free(rmp);
        return NULL;
    }
    rmp->impl.tree.indexes = NULL;

    rmp->close_recmap_fn = RDB_close_tree_recmap;
    rmp->delete_recmap_fn = &RDB_delete_tree_recmap;
    rmp->insert_rec_fn = &RDB_insert_tree_rec;
    rmp->update_rec_fn = &RDB_update_tree_rec;
    rmp->delete_rec_fn = &RDB_delete_tree_rec;
    rmp->get_fields_fn = &RDB_get_tree_fields;
    rmp->contains_rec_fn = &RDB_contains_tree_rec;
    rmp->recmap_est_size_fn = &RDB_tree_recmap_est_size;
    rmp->cursor_fn = &RDB_tree_recmap_cursor;
    rmp->create_index_fn = &RDB_create_tree_index;

    return rmp;
}

RDB_recmap *
RDB_create_tree_recmap(int fieldc, const RDB_field_info fieldinfov[],
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

    rmp->cmpc = cmpc;
    if (cmpc > 0) {
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
    RDB_drop_tree(rmp->impl.tree.treep);

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

static int
make_skey(RDB_index *ixp, void *key, size_t keylen, void *value, size_t valuelen,
        void **skeyp, size_t *skeylenp)
{
    RDB_field *fieldv;
    int ret;
    int i;

    fieldv = malloc (sizeof(RDB_field) * ixp->fieldc);
    if (fieldv == NULL)
        return ENOMEM;

    for (i = 0; i < ixp->fieldc; i++) {
        fieldv[i].no = ixp->fieldv[i];
        fieldv[i].copyfp = &memcpy;
    }
    ret = RDB_get_mem_fields(ixp->rmp, key, keylen, value, valuelen, ixp->fieldc, fieldv);
    if (ret != RDB_OK) {
        free(fieldv);
        return ret;
    }

    ret = RDB_fields_to_mem(ixp->rmp, ixp->fieldc, fieldv, skeyp, skeylenp);
    free(fieldv);
    return ret;
}

static int
insert_into_indexes(RDB_recmap *rmp, void *key, size_t keylen, void *value, size_t valuelen,
        RDB_tree_node *nodep, RDB_exec_context *ecp)
{
    void *skey = NULL;
    size_t skeylen;
    RDB_index *ixp;
    int ret;
    RDB_tree_node **nodepp;

    for (ixp = rmp->impl.tree.indexes; ixp != NULL; ixp = ixp->impl.tree.nextp) {
        ret = make_skey(ixp, key, keylen, value, valuelen, &skey, &skeylen);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }

        nodepp = RDB_alloc(sizeof(RDB_tree_node *), ecp);
        if (nodepp == NULL)
            return RDB_ERROR;

        *nodepp = nodep;
        if (RDB_tree_insert(ixp->impl.tree.treep, skey, skeylen,
                nodepp, sizeof(RDB_tree_node *), ecp) == NULL) {
            free(skey);
            RDB_free(nodepp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
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
    RDB_tree_node *nodep;

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

    nodep = RDB_tree_insert(rmp->impl.tree.treep, key, keylen, value, valuelen, ecp);
    if (nodep == NULL) {
        free(key);
        free(value);
        return RDB_ERROR;
    }

    return insert_into_indexes(rmp, key, keylen, value, valuelen, nodep, ecp);
}

int
RDB_update_tree_node(RDB_recmap *rmp, RDB_tree_node *nodep, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_bool keymod = RDB_FALSE;

    /* Check if the key is to be modified */
    for (i = 0; i < fieldc; i++) {
        if (fieldv[i].no < rmp->keyfieldcount) {
            keymod = RDB_TRUE;
            break;
        }
    }

    if (keymod) {
        void *key = nodep->key;
        size_t keylen = nodep->keylen;
        void *value = nodep->value;
        size_t valuelen = nodep->valuelen;

        /* Delete node, but keep the data */
        nodep->keylen = 0;
        nodep->key = NULL;
        nodep->valuelen = 0;
        nodep->value = NULL;
        if (RDB_tree_delete_node(rmp->impl.tree.treep, nodep, ecp) != RDB_OK)
            return RDB_ERROR;

        for (i = 0; i < fieldc; i++) {
            if (fieldv[i].no < rmp->keyfieldcount) {
                ret = RDB_set_field_mem(rmp, &key, &keylen, &fieldv[i],
                               rmp->varkeyfieldcount);
            } else {
                ret = RDB_set_field_mem(rmp, &value, &valuelen, &fieldv[i],
                               rmp->vardatafieldcount);
            }
            if (ret != RDB_OK)
                return ret;
        }

        /* Insert node */
        if (RDB_tree_insert(rmp->impl.tree.treep, key, keylen, value, valuelen, ecp) != RDB_OK) {
            if (keylen > 0)
                free(key);
            if (valuelen > 0)
                free(value);
            return RDB_ERROR;
        }
        return RDB_OK;
    } else {
        for (i = 0; i < fieldc; i++) {
            ret = RDB_set_field_mem(rmp, &nodep->value, &nodep->valuelen,
                    &fieldv[i], rmp->vardatafieldcount);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp);
                return RDB_ERROR;
            }
        }
    }
    return RDB_OK;
}

int
RDB_update_tree_rec(RDB_recmap *rmp, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], RDB_rec_transaction *rtxp,
               RDB_exec_context *ecp)
{
    size_t keylen;
    void *key;
    int ret;
    RDB_tree_node *nodep;

    ret = key_to_mem(rmp, keyv, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    nodep = RDB_tree_find(rmp->impl.tree.treep, key, keylen);
    if (keylen > 0)
        free(key);
    if (nodep == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    return RDB_update_tree_node(rmp, nodep, keyv, fieldc, fieldv, ecp);
}

int
RDB_delete_tree_rec(RDB_recmap *rmp, int fieldc, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    size_t keylen;
    void *key;
    int ret;

    ret = key_to_mem(rmp, keyv, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    ret = RDB_tree_delete(rmp->impl.tree.treep, key, keylen, ecp);
    free(key);
    return ret;
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

    value = RDB_tree_get(rmp->impl.tree.treep, key, keylen, &valuelen);
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
    size_t keylen;
    void *key;
    size_t valuelen;
    void *value;
    size_t value2len;
    void *value2;
    int ret;

    ret = key_to_mem(rmp, flds, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    value = RDB_tree_get(rmp->impl.tree.treep, key, keylen, &valuelen);
    free(key);
    if (value == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    ret = value_to_mem(rmp, flds, &value2, &value2len);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    if (valuelen != value2len) {
        free(value2);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (memcmp(value, value2, valuelen) != 0) {
        free(value2);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    free(value2);
    return RDB_OK;
}

int
RDB_tree_recmap_est_size(RDB_recmap *rmp, RDB_rec_transaction *rtxp, unsigned *sz,
        RDB_exec_context *ecp)
{
    *sz = 1;
    return RDB_OK;
}
