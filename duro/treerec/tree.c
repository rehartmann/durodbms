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

static void
del_node(RDB_tree_node *nodep) {
    if (nodep->keylen > 0)
        free(nodep->key);
    if (nodep->valuelen > 0)
        free(nodep->value);
    RDB_free(nodep);
}

RDB_binary_tree *
RDB_create_tree(RDB_comparison_func *cmpfp, void *arg, RDB_exec_context *ecp)
{
    RDB_binary_tree *treep = RDB_alloc(sizeof(RDB_binary_tree), ecp);
    if (treep == NULL)
        return NULL;

    treep->root = NULL;
    treep->comparison_fp = cmpfp;
    treep->comparison_arg = arg;
    return treep;
}

static void
delete_subtree(RDB_tree_node *nodep)
{
    if (nodep->left != NULL)
        delete_subtree(nodep->left);
    if (nodep->right != NULL)
        delete_subtree(nodep->right);
    del_node(nodep);
}

void
RDB_drop_tree(RDB_binary_tree *treep)
{
    if (treep->root != NULL) {
        delete_subtree(treep->root);
    }
    RDB_free(treep);
}

static int
compare_key(const RDB_binary_tree *treep, const void *key, size_t keylen,
        const RDB_tree_node *nodep)
{
    int res;

    if (treep->comparison_fp != NULL) {
        return (*treep->comparison_fp)(key, keylen,
                nodep->key, nodep->keylen, treep->comparison_arg);
    }
    res = memcmp(key, nodep->key, nodep->keylen <= keylen ?
            nodep->keylen : keylen);
    if (res != 0)
        return res;

    return abs(keylen - nodep->keylen);
}

RDB_tree_node *
RDB_tree_find(const RDB_binary_tree *treep, const void *key, size_t keylen)
{
    RDB_tree_node *nodep = treep->root;
    while (nodep != NULL) {
        int cmpres = compare_key(treep, key, keylen, nodep);
        if (cmpres == 0)
            return nodep;
        if (cmpres < 0)
            nodep = nodep->left;
        else
            nodep = nodep->right;
    }
    return NULL;
}

int
RDB_tree_delete_node(RDB_binary_tree *treep, RDB_tree_node *nodep,
        RDB_exec_context *ecp)
{
    RDB_tree_node *xnodep;

    if (nodep->right == NULL) {
        if (nodep->left == NULL) {
            /* Node to delete has no children */
            if (nodep->parent == NULL) {
                treep->root = NULL;
            } else if (nodep->parent->left == nodep) {
                nodep->parent->left = NULL;
            } else {
                nodep->parent->right = NULL;
            }
        } else {
            /* Node has only left child */
            if (nodep->parent == NULL) {
                treep->root = nodep->left;
                nodep->left->parent = NULL;
            } else {
                if (nodep->parent->right == nodep) {
                    nodep->parent->right = nodep->left;
                } else {
                    nodep->parent->left = nodep->left;
                }
                nodep->left->parent = nodep->parent;
            }
        }
        del_node(nodep);
        return RDB_OK;
    }
    if (nodep->left == NULL) {
        /* Node has only right child */
        if (nodep->parent == NULL) {
            treep->root = nodep->right;
            nodep->right->parent = NULL;
        } else {
            if (nodep->parent->right == nodep) {
                nodep->parent->right = nodep->right;
            } else {
                nodep->parent->left = nodep->right;
            }
            nodep->right->parent = nodep->parent;
        }
        del_node(nodep);
        return RDB_OK;
    }
    /* Node to delete has two children */

    for (xnodep = nodep->right; xnodep->left != NULL; xnodep = xnodep->left);

    /* Remove xnodep */
    if (xnodep->parent->left == xnodep) {
        xnodep->parent->left = xnodep->right;
    } else {
        xnodep->parent->right = xnodep->right;
    }

    /* Replace data of nodep with xnodep and delete xnodep */
    if (nodep->keylen > 0)
        free(nodep->key);
    nodep->keylen = xnodep->keylen;
    nodep->key = xnodep->key;
    if (nodep->valuelen > 0)
        free(nodep->value);
    nodep->valuelen = xnodep->valuelen;
    nodep->value = xnodep->value;
    RDB_free(xnodep);
    return RDB_OK;
}

static RDB_tree_node *
create_node(RDB_tree_node *parentp, void *key, size_t keylen,
        void *val, size_t vallen, RDB_exec_context *ecp)
{
    RDB_tree_node *nodep = RDB_alloc(sizeof(RDB_tree_node), ecp);
    if (nodep == NULL) {
        return NULL;
    }
    nodep->left = NULL;
    nodep->right = NULL;
    nodep->parent = parentp;
    nodep->balance = 0;
    nodep->keylen = keylen;
    if (keylen > 0)
        nodep->key = key;
    nodep->valuelen = vallen;
    if (vallen > 0)
        nodep->value = val;
    return nodep;
}

RDB_tree_node *
RDB_tree_insert(RDB_binary_tree *treep, void *key, size_t keylen,
        void *val, size_t vallen, RDB_exec_context *ecp)
{
    RDB_tree_node *nodep;

    if (treep->root == NULL) {
        treep->root = create_node(NULL, key, keylen, val, vallen, ecp);
        return treep->root;
    }
    nodep = treep->root;
    for(;;) {
        int cmpres = compare_key(treep, key, keylen, nodep);
        if (cmpres == 0) {
            RDB_raise_key_violation("", ecp);
            return NULL;
        }
        if (cmpres < 0) {
            if (nodep->left == NULL) {
                nodep->left = create_node(nodep, key, keylen, val, vallen, ecp);
                return nodep->left;
            }
            nodep = nodep->left;
        } else {
            if (nodep->right == NULL) {
                nodep->right = create_node(nodep, key, keylen, val, vallen, ecp);
                return nodep->right;
            }
            nodep = nodep->right;
        }
    }
}
