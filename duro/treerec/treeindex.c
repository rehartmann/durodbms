/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "treeindex.h"
#include "treerecmap.h"
#include "tree.h"
#include "field.h"
#include <rec/dbdefs.h>
#include <rec/recmapimpl.h>
#include <rec/indeximpl.h>
#include <obj/excontext.h>

#include <string.h>
#include <errno.h>

/*
 * Read field from an index DBT
 */
static int
get_field(RDB_index *ixp, int fi, const void *datap, size_t len, size_t *lenp,
              int *vposp)
{
    int i, vpos;
    int offs = 0;
    RDB_byte *databp = (RDB_byte *) datap;
    int fno = ixp->fieldv[fi];

    /*
     * Compute offset and length for key
     */
    if (ixp->rmp->fieldinfos[fno].len != RDB_VARIABLE_LEN) {
        /* Offset is sum of lengths of previous fields */
        for (i = 0; i < fi; i++) {
            if (ixp->rmp->fieldinfos[ixp->fieldv[i]].len != RDB_VARIABLE_LEN) {
                offs += ixp->rmp->fieldinfos[ixp->fieldv[i]].len;
            }
        }

        *lenp = (size_t) ixp->rmp->fieldinfos[fno].len;
    } else {
        /*
         * Offset is sum of lengths of fixed-length fields
         * plus lengths of previous variable-length fields
         */
        int vfcnt = 0;
        for (i = 0; i < ixp->fieldc; i++) {
            if (ixp->rmp->fieldinfos[ixp->fieldv[i]].len == RDB_VARIABLE_LEN)
                vfcnt++;
        }

        vpos = 0;
        for (i = 0; i < fi; i++) {
            if (ixp->rmp->fieldinfos[ixp->fieldv[i]].len != RDB_VARIABLE_LEN) {
                offs += ixp->rmp->fieldinfos[i].len;
            } else {
                offs += RDB_get_vflen(databp, len, vfcnt, vpos++);
            }
        }
        *lenp = RDB_get_vflen(databp, len, vfcnt, vpos);
    }
    if (vposp != NULL)
        *vposp = vpos;
    return offs;
}

static int
compare_key(const void *d1, size_t size1,
        const void *d2, size_t size2, void *comparison_arg)
{
    int i;
    RDB_index *ixp = comparison_arg;

    if (ixp->cmpv == NULL) {
        int res = memcmp(d1, d2, size1 <= size2 ? size1 : size2);
        if (res != 0)
            return res;

        return abs(size1 - size2);
    }

    for (i = 0; i < ixp->fieldc; i++) {
        int offs1, offs2;
        size_t len1, len2;
        void *data1p, *data2p;
        int res;

        offs1 = get_field(ixp, i, d1, size1, &len1, NULL);
        offs2 = get_field(ixp, i, d2, size2, &len2, NULL);
        data1p = ((RDB_byte *) d1) + offs1;
        data2p = ((RDB_byte *) d2) + offs2;

        /* Compare fields */
        res = (*ixp->cmpv[i].comparep)(data1p, len1, data2p, len2,
                    ixp->rmp->envp, ixp->cmpv[i].arg);

        /* If the fields are different, we're done */
        if (res != 0) {
            /* If order is descending, revert result */
            if (!ixp->cmpv[i].asc)
                res = -res;
            return res;
        }
    }
    /* All fields equal */
    return 0;
}

static RDB_index *
new_tree_index(RDB_recmap *rmp,
        int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_exec_context *ecp)
{
    int i;
    RDB_index *ixp;
    RDB_bool all_cmpfn = RDB_TRUE;

    if (!(RDB_UNIQUE & flags)) {
        RDB_raise_invalid_argument("non-unique index not supported", ecp);
        return NULL;
    }

    ixp = RDB_alloc(sizeof (RDB_index), ecp);
    if (ixp == NULL) {
        return NULL;
    }
    ixp->fieldv = NULL;
    ixp->rmp = rmp;

    ixp->namp = ixp->filenamp = NULL;

    ixp->fieldc = fieldc;
    ixp->fieldv = malloc(fieldc * sizeof(int));
    if (ixp->fieldv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    for (i = 0; i < fieldc; i++)
        ixp->fieldv[i] = fieldv[i];
    ixp->cmpv = 0;

    if (cmpv != NULL) {
        all_cmpfn = RDB_TRUE;

        ixp->cmpv = RDB_alloc(sizeof (RDB_compare_field) * fieldc, ecp);
        if (ixp->cmpv == NULL)
            goto error;
        for (i = 0; i < fieldc; i++) {
            ixp->cmpv[i].comparep = cmpv[i].comparep;
            ixp->cmpv[i].arg = cmpv[i].arg;
            ixp->cmpv[i].asc = cmpv[i].asc;

            if (cmpv[i].comparep == NULL)
                all_cmpfn = RDB_FALSE;
        }
    }

    ixp->impl.tree.treep = RDB_create_tree(all_cmpfn ? compare_key : NULL, ixp, ecp);
    if (ixp->impl.tree.treep == NULL)
        goto error;

    ixp->close_index_fn = &RDB_close_tree_index;
    ixp->delete_index_fn = &RDB_delete_tree_index;
    ixp->index_delete_rec_fn = &RDB_tree_index_delete_rec;
    ixp->index_get_fields_fn = &RDB_tree_index_get_fields;
    ixp->index_cursor_fn = NULL;

    return ixp;

error:
    free(ixp->fieldv);
    free(ixp);
    return NULL;
}

/*
 * Create index. If cmpv is not NULL and RDB_ORDERED is in flags,
 * it must be array of size fieldc specifying the order. cmpv[].fno is ignored.
 */
RDB_index *
RDB_create_tree_index(RDB_recmap *rmp, const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const RDB_field_descriptor fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_index *ixp = NULL;
    int i;

    int *fnov = RDB_alloc(fieldc * sizeof(int), ecp);
    if (fnov == NULL)
        goto error;
    for (i = 0; i < fieldc; i++) {
        fnov[i] = fieldv[i].no;
    }
    ixp = new_tree_index(rmp, fieldc, fnov, cmpv, flags, ecp);
    RDB_free(fnov);
    if (ixp == NULL)
        return NULL;

    /* Associate index with recmap */
    ixp->impl.tree.nextp = rmp->impl.tree.indexes;
    rmp->impl.tree.indexes = ixp;

    return ixp;

error:
    if (ixp != NULL)
        RDB_delete_tree_index(ixp, rtxp, ecp);
    return NULL;
}

/* Delete an index. */
int
RDB_delete_tree_index(RDB_index *ixp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_drop_tree(ixp->impl.tree.treep);
    /* Remove from list */

    free(ixp->fieldv);
    free(ixp->cmpv);
    free(ixp);

    return RDB_OK;
}

int
RDB_close_tree_index(RDB_index *ixp, RDB_exec_context *ecp)
{
    return RDB_delete_tree_index(ixp, NULL, ecp);
}

int
RDB_tree_index_get_fields(RDB_index *ixp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    size_t keylen;
    void *key;
    RDB_tree_node *nodep, *snodep;
    int ret;
    int i;

    for (i = 0; i < ixp->fieldc; i++) {
        keyv[i].no = ixp->fieldv[i];
    }

    for (i = 0; i < ixp->rmp->keyfieldcount; i++) {
        keyv[i].no = i;
    }
    ret = RDB_fields_to_mem(ixp->rmp, ixp->rmp->keyfieldcount, keyv, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    nodep = RDB_tree_find(ixp->impl.tree.treep, key, keylen);
    if (nodep == NULL) {
        RDB_raise_not_found("", ecp);
        free(key);
        return RDB_ERROR;
    }

    snodep = *(RDB_tree_node**)nodep->value;

    /* Get field values */
    for (i = 0; i < fieldc; i++) {
        int offs;
        int fno = retfieldv[i].no;

        if (fno < ixp->rmp->keyfieldcount) {
            offs = RDB_get_field(ixp->rmp, fno, snodep->key, snodep->keylen,
                    &retfieldv[i].len, NULL);
            if (offs < 0)
                return offs;
            retfieldv[i].datap = ((RDB_byte *)snodep->key) + offs;
        } else {
            offs = RDB_get_field(ixp->rmp, fno,
                    snodep->value, snodep->valuelen, &retfieldv[i].len, NULL);
            if (offs < 0)
                return offs;
            retfieldv[i].datap = ((RDB_byte *)snodep->value) + offs;
        }
    }
    free(key);
    return RDB_OK;
}

int
RDB_tree_index_delete_rec(RDB_index *ixp, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    size_t keylen;
    void *key;
    RDB_tree_node *pnodep, *snodep;
    int ret;
    int i;

    for (i = 0; i < ixp->fieldc; i++) {
        keyv[i].no = ixp->fieldv[i];
    }

    for (i = 0; i < ixp->rmp->keyfieldcount; i++) {
        keyv[i].no = i;
    }
    ret = RDB_fields_to_mem(ixp->rmp, ixp->rmp->keyfieldcount, keyv, &key, &keylen);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    pnodep = RDB_tree_find(ixp->impl.tree.treep, key, keylen);
    free(key);
    if (pnodep == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    snodep = (RDB_tree_node *) pnodep->value;
    if (RDB_delete_from_tree_indexes(ixp->rmp, snodep, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_tree_delete_node(ixp->impl.tree.treep, snodep, ecp);
}
