/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "treecursor.h"
#include "tree.h"
#include <rec/cursorimpl.h>
#include <rec/recmapimpl.h>
#include <rec/indeximpl.h>
#include <bdbrec/bdbrecmap.h>
#include <obj/excontext.h>
#include <string.h>

/*
 * Allocate and initialize a RDB_cursor structure.
 */
static RDB_cursor *
new_tree_cursor(RDB_recmap *rmp, RDB_binary_tree *treep, RDB_exec_context *ecp)
{
    RDB_cursor *curp = RDB_alloc(sizeof(RDB_cursor), ecp);
    if (curp == NULL)
        return NULL;

    curp->recmapp = rmp;
    curp->cur.tree.treep = treep;
    curp->cur.tree.nodep = NULL;

    curp->destroy_fn = &RDB_destroy_tree_cursor;
    curp->get_fn = &RDB_tree_cursor_get;
    curp->set_fn = &RDB_tree_cursor_set;
    curp->delete_fn = &RDB_tree_cursor_delete;
    curp->first_fn = &RDB_tree_cursor_first;
    curp->next_fn = &RDB_tree_cursor_next;
    curp->prev_fn = &RDB_tree_cursor_prev;

    return curp;
}

RDB_cursor *
RDB_tree_recmap_cursor(RDB_recmap *rmp, RDB_bool wr,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_cursor *curp = new_tree_cursor(rmp, rmp->impl.treep, ecp);
    
    if (curp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    curp->secondary = RDB_FALSE;
    return curp;
}

int
RDB_destroy_tree_cursor(RDB_cursor *curp, RDB_exec_context *ecp)
{
    RDB_free(curp);
    return RDB_OK;
}

int
RDB_tree_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp,
        RDB_exec_context *ecp)
{
    RDB_byte *databp;
    int offs;

    if (curp->cur.tree.nodep == NULL) {
        RDB_raise_not_found("invalid cursor", ecp);
        return RDB_ERROR;
    }

    if (fno < curp->recmapp->keyfieldcount) {
        databp = curp->cur.tree.nodep->key;
        offs = RDB_get_field(curp->recmapp, fno,
                databp, curp->cur.tree.nodep->keylen, lenp, NULL);
    } else {
        databp = curp->cur.tree.nodep->value;
        offs = RDB_get_field(curp->recmapp, fno,
                databp, curp->cur.tree.nodep->valuelen, lenp, NULL);
    }
    if (offs < 0) {
        RDB_errcode_to_error(offs, ecp);
        return RDB_ERROR;
    }
    *datapp = databp + offs;
    return RDB_OK;
}

int
RDB_tree_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[],
        RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (curp->cur.tree.nodep == NULL) {
        RDB_raise_not_found("invalid cursor", ecp);
        return RDB_ERROR;
    }

    /*
    if (curp->secondary)
    */

    for (i = 0; i < fieldc; i++) {
        if (fields[i].no < curp->recmapp->keyfieldcount) {
            RDB_raise_invalid_argument("Modifiying the key is not supported", ecp);
            return RDB_ERROR;
        }
        ret = RDB_set_field_mem(curp->recmapp, &curp->cur.tree.nodep->value,
                &curp->cur.tree.nodep->valuelen, &fields[i],
                curp->recmapp->vardatafieldcount);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }
    }

    return RDB_OK;
}

int
RDB_tree_cursor_delete(RDB_cursor *curp, RDB_exec_context *ecp)
{
    RDB_tree_node *nodep = curp->cur.tree.nodep;
    int ret = RDB_tree_cursor_next(curp, 0, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    return RDB_tree_delete_node(curp->cur.tree.treep, nodep, ecp);
}

/*
 * Move the cursor to the first record.
 * If there is no first record, RDB_NOT_FOUND is raised.
 */
int
RDB_tree_cursor_first(RDB_cursor *curp, RDB_exec_context *ecp)
{
    RDB_tree_node *nodep = curp->cur.tree.treep->root;
    if (nodep == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }
    while (nodep->left != NULL)
        nodep = nodep->left;
    curp->cur.tree.nodep = nodep;
    return RDB_OK;
}

int
RDB_tree_cursor_next(RDB_cursor *curp, int flags, RDB_exec_context *ecp)
{
    RDB_tree_node *nodep = curp->cur.tree.nodep;
    if (nodep == NULL) {
        RDB_raise_invalid_argument("cursor position undefined", ecp);
        return RDB_ERROR;
    }
    if (nodep->right != NULL) {
        curp->cur.tree.nodep = nodep->right;
        return RDB_OK;
    }
    while (nodep->parent != NULL && nodep != nodep->parent->left) {
        nodep = nodep->parent;
    }
    curp->cur.tree.nodep = nodep->parent;
    if (curp->cur.tree.nodep != NULL)
        return RDB_OK;
    RDB_raise_not_found("", ecp);
    return RDB_ERROR;
}

int
RDB_tree_cursor_prev(RDB_cursor *curp, RDB_exec_context *ecp)
{
    RDB_tree_node *nodep = curp->cur.tree.nodep;
    if (nodep == NULL) {
        RDB_raise_invalid_argument("cursor position undefined", ecp);
        return RDB_ERROR;
    }
    if (nodep->right != NULL) {
        curp->cur.tree.nodep = nodep->left;
        return RDB_OK;
    }
    while (nodep->parent != NULL && nodep != nodep->parent->right) {
        nodep = nodep->parent;
    }
    curp->cur.tree.nodep = nodep->parent;
    if (curp->cur.tree.nodep != NULL)
        return RDB_OK;
    RDB_raise_not_found("", ecp);
    return RDB_ERROR;
}

/*
 * Move the cursor to the position specified by keyv.
 */
int
RDB_tree_cursor_seek(RDB_cursor *curp, int fieldc, RDB_field keyv[], int flags,
        RDB_exec_context *ecp)
{
    int ret;
    int i;
    void *key;
    size_t keylen;

    if (curp->idxp == NULL) {
        for (i = 0; i < curp->recmapp->keyfieldcount; i++)
            keyv[i].no = i;
    } else {
        for (i = 0; i < fieldc; i++)
            keyv[i].no = curp->idxp->fieldv[i];
    }

    if (curp->idxp == NULL) {
        ret = RDB_fields_to_mem(curp->recmapp, curp->recmapp->keyfieldcount,
                keyv, &key, &keylen);
    } else {
        ret = RDB_fields_to_mem(curp->recmapp, fieldc, keyv, &key, &keylen);
    }
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    curp->cur.tree.nodep = RDB_tree_find(curp->cur.tree.treep,
                key, keylen);
    free(key);
    if (curp->cur.tree.nodep == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}
