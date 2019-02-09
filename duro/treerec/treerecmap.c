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
#include <obj/excontext.h>
#include <obj/builtintypes.h>
#include <gen/strfns.h>
#include <treerec/field.h>
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
        data1p = ((uint8_t *) d1) + offs1;
        data2p = ((uint8_t *) d2) + offs2;

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
    rmp->open_index_fn = NULL;

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
check_in_indexes(RDB_recmap *rmp, void *key, size_t keylen,
        void *value, size_t valuelen, RDB_exec_context *ecp)
{
    void *skey = NULL;
    size_t skeylen;
    RDB_index *ixp;
    int ret;

    for (ixp = rmp->impl.tree.indexes; ixp != NULL; ixp = ixp->impl.tree.nextp) {
        ret = make_skey(ixp, key, keylen, value, valuelen, &skey, &skeylen);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }
        if (RDB_tree_find(ixp->impl.tree.treep, skey, skeylen) != NULL) {
            free(skey);
            RDB_raise_key_violation("", ecp);
            return RDB_ERROR;
        }
        free(skey);
    }
    return RDB_OK;
}

static int
insert_into_indexes(RDB_recmap *rmp, RDB_tree_node *nodep, RDB_exec_context *ecp)
{
    void *skey = NULL;
    size_t skeylen;
    RDB_index *ixp;
    int ret;

    for (ixp = rmp->impl.tree.indexes; ixp != NULL; ixp = ixp->impl.tree.nextp) {
        void *key;

        ret = make_skey(ixp, nodep->key, nodep->keylen, nodep->value, nodep->valuelen, &skey, &skeylen);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }

        key = RDB_alloc(nodep->keylen, ecp);
        if (key == NULL)
            return RDB_ERROR;
        memcpy (key, nodep->key, nodep->keylen);

        if (RDB_tree_insert(ixp->impl.tree.treep, skey, skeylen,
                key, nodep->keylen, ecp) == NULL) {
            free(skey);
            RDB_free(key);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
delete_tree_rec_by_key(RDB_recmap *rmp, void *key, size_t keylen, RDB_exec_context *ecp)
{
    if (rmp->impl.tree.indexes != NULL) {
        /* Delete entry from indexes */
        RDB_tree_node *nodep = RDB_tree_find(rmp->impl.tree.treep, key, keylen);
        if (nodep != NULL) {
            if (RDB_delete_from_tree_indexes(rmp, nodep, ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }
    return RDB_tree_delete_node(rmp->impl.tree.treep, key, keylen, ecp);
}

static int
insert_tree_rec_kv(RDB_recmap *rmp, void *key, size_t keylen,
        void *value, size_t valuelen, RDB_exec_context *ecp)
{
    RDB_tree_node *nodep;

    if (check_in_indexes(rmp, key, keylen, value, valuelen, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    nodep = RDB_tree_insert(rmp->impl.tree.treep, key, keylen, value, valuelen, ecp);
    if (nodep == NULL) {
        return RDB_ERROR;
    }

    if (insert_into_indexes(rmp, nodep, ecp) != RDB_OK) {
        delete_tree_rec_by_key(rmp, key, keylen, ecp);
        return RDB_ERROR;
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

    ret = insert_tree_rec_kv(rmp, key, keylen, value, valuelen, ecp);
    if (ret != RDB_OK) {
        free(key);
        free(value);
        return RDB_ERROR;
    }
    return RDB_OK;
}

RDB_bool
RDB_recmap_is_key_update(RDB_recmap *rmp, int fieldc, const RDB_field fieldv[])
{
    int i;
    for (i = 0; i < fieldc; i++) {
        RDB_index *ixp;

        if (fieldv[i].no < rmp->keyfieldcount)
            return RDB_TRUE;
        for (ixp = rmp->impl.tree.indexes; ixp != NULL; ixp = ixp->impl.tree.nextp) {
            int j;
            for (j = 0; j < ixp->fieldc; j++) {
                if (fieldv[i].no == ixp->fieldv[j])
                    return RDB_TRUE;
            }
        }
    }
    return RDB_FALSE;
}

static int
delete_update_reinsert_tree_node(RDB_recmap *rmp, RDB_tree_node *nodep,
               int fieldc, const RDB_field fieldv[], RDB_exec_context *ecp)
{
    int ret;
    int i;
    void *key = NULL;
    void *value = NULL;
    size_t keylen, valuelen;
    void *key2 = NULL;
    void *value2 = NULL;
    size_t keylen2, valuelen2;

    keylen = nodep->keylen;
    valuelen = nodep->valuelen;
    if (keylen > 0) {
        key = RDB_alloc(keylen, ecp);
        if (key == NULL)
            goto error;
        memcpy(key, nodep->key, keylen);
    }
    if (valuelen > 0) {
        value = RDB_alloc(valuelen, ecp);
        if (value == NULL)
            goto error;
        memcpy(value, nodep->value, valuelen);
    }

    if (delete_tree_rec_by_key(rmp, key, keylen, ecp) != RDB_OK) {
        goto error;
    }

    keylen2 = keylen;
    valuelen2 = valuelen;
    if (keylen2 > 0) {
        key2 = RDB_alloc(keylen2, ecp);
        if (key2 == NULL)
            goto error;
        memcpy(key2, key, keylen2);
    }
    if (valuelen > 0) {
        value2 = RDB_alloc(valuelen2, ecp);
        if (value2 == NULL)
            goto error;
        memcpy(value2, value, valuelen2);
    }

    for (i = 0; i < fieldc; i++) {
        if (fieldv[i].no < rmp->keyfieldcount) {
            ret = RDB_set_field_mem(rmp, &key, &keylen, &fieldv[i],
                           rmp->varkeyfieldcount);
        } else {
            ret = RDB_set_field_mem(rmp, &value, &valuelen, &fieldv[i],
                           rmp->vardatafieldcount);
        }
        if (ret != RDB_OK) {
            free(key);
            return ret;
        }
    }
    ret = insert_tree_rec_kv(rmp, key, keylen, value, valuelen, ecp);
    if (ret != RDB_OK) {
        /* Insert unmodified record */
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR) {
            if (insert_tree_rec_kv(rmp, key2, keylen2, value2, valuelen2, ecp) == RDB_OK) {
                key2 = NULL;
                value2 = NULL;
            }
        }
        goto error;
    }
    RDB_free(key2);
    RDB_free(value2);

    return RDB_OK;

error:
    if (key != NULL) {
        RDB_free(key);
    }
    if (value != NULL) {
        RDB_free(value);
    }
    if (key2 != NULL) {
        RDB_free(key2);
    }
    if (value2 != NULL) {
        RDB_free(value2);
    }

    return RDB_ERROR;
}

static int
RDB_update_tree_node(RDB_recmap *rmp, RDB_tree_node *nodep,
               int fieldc, const RDB_field fieldv[], RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (RDB_recmap_is_key_update(rmp, fieldc, fieldv)) {
        return delete_update_reinsert_tree_node(rmp, nodep, fieldc, fieldv, ecp);
    }

    for (i = 0; i < fieldc; i++) {
        ret = RDB_set_field_mem(rmp, &nodep->value, &nodep->valuelen,
                &fieldv[i], rmp->vardatafieldcount);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
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

    return RDB_update_tree_node(rmp, nodep, fieldc, fieldv, ecp);
}

int
RDB_delete_from_tree_indexes(RDB_recmap *rmp, RDB_tree_node *nodep, RDB_exec_context *ecp)
{
    void *skey = NULL;
    size_t skeylen;
    RDB_index *ixp;
    int ret;

    for (ixp = rmp->impl.tree.indexes; ixp != NULL; ixp = ixp->impl.tree.nextp) {
        ret = make_skey(ixp, nodep->key, nodep->keylen, nodep->value, nodep->valuelen, &skey, &skeylen);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }

        ret = RDB_tree_delete_node(ixp->impl.tree.treep, skey, skeylen, ecp);
        free(skey);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
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

    ret = delete_tree_rec_by_key(rmp, key, keylen, ecp);
    free(key);
    return ret;
}

int
RDB_get_tree_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    size_t keylen;
    void *key;
    RDB_tree_node *nodep;
    int ret;

    ret = key_to_mem(rmp, keyv, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    nodep = RDB_tree_find(rmp->impl.tree.treep, key, keylen);
    if (nodep == NULL) {
        free(key);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    ret = RDB_get_mem_fields(rmp, key, keylen, nodep->value, nodep->valuelen, fieldc, retfieldv);
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
    RDB_tree_node *nodep;
    size_t value2len;
    void *value2;
    int ret;

    ret = key_to_mem(rmp, flds, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    nodep = RDB_tree_find(rmp->impl.tree.treep, key, keylen);
    free(key);
    if (nodep == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    ret = value_to_mem(rmp, flds, &value2, &value2len);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    if (nodep->valuelen != value2len) {
        free(value2);
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (memcmp(nodep->value, value2, nodep->valuelen) != 0) {
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
