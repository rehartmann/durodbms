/*
 * Binary search tree
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <gen/types.h>
#include <obj/object.h>
#include <obj/excontext.h>
#include <treerec/tree.h>

#include <string.h>

RDB_binary_tree *
RDB_tree_create(RDB_comparison_func *cmpfp, void *arg, RDB_exec_context *ecp)
{
    RDB_binary_tree *treep = RDB_alloc(sizeof(RDB_binary_tree), ecp);
    if (treep == NULL)
        return NULL;

    treep->root = NULL;
    treep->comparison_fp = cmpfp;
    treep->arg = arg;
    return treep;
}

static void
delete_subtree(struct tree_node *nodep)
{
    if (nodep->header.left != NULL)
        delete_subtree(nodep->header.left);
    if (nodep->header.right != NULL)
        delete_subtree(nodep->header.right);
    RDB_free(nodep);
}

void
RDB_tree_delete(RDB_binary_tree *treep)
{
    if (treep->root != NULL) {
        delete_subtree(treep->root);
    }
    RDB_free(treep);
}

static int
compare_key(const RDB_binary_tree *treep, const struct tree_node *nodep,
        const void *key, size_t keylen)
{
    int res;

    if (treep->comparison_fp != NULL) {
        return (*treep->comparison_fp)(nodep->data, nodep->header.keylen,
                key, keylen, treep->arg);
    }
    res = memcmp(nodep->data, key, nodep->header.keylen <= keylen ?
            nodep->header.keylen : keylen);
    if (res != 0)
        return res;

    return abs(nodep->header.keylen - keylen);
}

RDB_tree_node *
RDB_tree_find(const RDB_binary_tree *treep, void *key, size_t keylen)
{
    struct tree_node *nodep = treep->root;
    while (nodep != NULL) {
        int cmpres = compare_key(treep, nodep, key, keylen);
        if (cmpres == 0)
            return nodep;
        if (cmpres < 0)
            nodep = nodep->header.left;
        else
            nodep = nodep->header.right;
    }
    return NULL;
}

void *
RDB_tree_get(const RDB_binary_tree *treep, void *key, size_t keylen,
        size_t *valuelenp)
{
    struct tree_node *nodep = RDB_tree_find(treep, key, keylen);
    if (nodep == NULL)
        return NULL;
    *valuelenp = nodep->header.valuelen;
    return &nodep->data[keylen];
}

static struct tree_node *
create_node(struct tree_node *parentp, void *key, size_t keylen,
        void *val, size_t vallen, RDB_exec_context *ecp)
{
    struct tree_node *nodep = RDB_alloc(sizeof(struct tree_node_header)
            + keylen + vallen, ecp);
    if (nodep == NULL) {
        return NULL;
    }
    nodep->header.left = NULL;
    nodep->header.right = NULL;
    nodep->header.parent = parentp;
    nodep->header.balance = 0;
    nodep->header.keylen = keylen;
    nodep->header.valuelen = vallen;
    memcpy(&nodep->data, key, keylen);
    memcpy(&nodep->data[keylen], val, vallen);
    return nodep;
}

int
RDB_tree_insert(RDB_binary_tree *treep, void *key, size_t keylen,
        void *val, size_t vallen, RDB_exec_context *ecp)
{
    struct tree_node *nodep;

    if (treep->root == NULL) {
        treep->root = create_node(NULL, key, keylen, val, vallen, ecp);
        return treep->root != NULL ? RDB_OK : RDB_ERROR;
    }
    nodep = treep->root;
    for(;;) {
        int cmpres = compare_key(treep, nodep, key, keylen);
        if (cmpres == 0) {
            RDB_raise_key_violation("", ecp);
            return RDB_ERROR;
        }
        if (cmpres < 0) {
            if (nodep->header.left == NULL) {
                nodep->header.left = create_node(nodep, key, keylen, val, vallen, ecp);
                return nodep->header.left != NULL ? RDB_OK : RDB_ERROR;
            }
            nodep = nodep->header.left;
        } else {
            if (nodep->header.right == NULL) {
                nodep->header.right = create_node(nodep, key, keylen, val, vallen, ecp);
                return nodep->header.right != NULL ? RDB_OK : RDB_ERROR;
            }
            nodep = nodep->header.right;
        }
    }
}
