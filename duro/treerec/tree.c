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

static RDB_tree_node *
avl_tree_insert(RDB_binary_tree *treep, RDB_tree_node **nodepp, RDB_tree_node *parentp,
        void *key, size_t keylen, void *val, size_t vallen,
        RDB_bool *hp, RDB_exec_context *ecp)
{
    int cmpres;
    RDB_tree_node *newnodep;
    RDB_tree_node *nodep1, *nodep2;

    if (*nodepp == NULL) {
        *nodepp = create_node(parentp, key, keylen, val, vallen, ecp);
        *hp = RDB_TRUE;
        return *nodepp;
    }
    cmpres = compare_key(treep, key, keylen, *nodepp);
    if (cmpres == 0) {
        RDB_raise_key_violation("", ecp);
        return NULL;
    }
    if (cmpres < 0) {
        newnodep = avl_tree_insert(treep, &(*nodepp)->left, *nodepp, key, keylen, val, vallen,
                hp, ecp);
        if (newnodep == NULL)
            return NULL;
        if (*hp) {
            switch((*nodepp)->balance) {
            case 1:
                (*nodepp)->balance = 0;
                *hp = RDB_FALSE;
                break;
            case 0:
                (*nodepp)->balance = -1;
                break;
            case -1:
                nodep1 = (*nodepp)->left;
                if (nodep1->balance == -1) {
                    (*nodepp)->left = nodep1->right;
                    if (nodep1->right != NULL)
                        nodep1->right->parent = *nodepp;
                    nodep1->right = *nodepp;
                    (*nodepp)->parent = nodep1;
                    (*nodepp)->balance = 0;
                    nodep1->parent = parentp;
                    *nodepp = nodep1;
                } else {
                    nodep2 = nodep1->right;
                    nodep1->right = nodep2->left;
                    if (nodep2->left != NULL)
                        nodep2->left->parent = nodep1;
                    nodep2->left = nodep1;
                    nodep1->parent = nodep2;
                    (*nodepp)->left = nodep2->right;
                    if (nodep2->right != NULL)
                        nodep2->right->parent = *nodepp;
                    nodep2->right = *nodepp;
                    (*nodepp)->parent = nodep2;
                    if (nodep2->balance == -1) {
                        (*nodepp)->balance = 1;
                    } else {
                        (*nodepp)->balance = 0;
                    }
                    if (nodep2->balance == 1) {
                        nodep1->balance = -1;
                    } else {
                        nodep1->balance = 0;
                    }
                    nodep2->parent = parentp;
                    *nodepp = nodep2;
                }
                (*nodepp)->balance = 0;
                *hp = RDB_FALSE;
                break;
            }
        }
    } else {
        newnodep = avl_tree_insert(treep, &(*nodepp)->right, *nodepp, key, keylen, val, vallen,
                hp, ecp);
        if (newnodep == NULL)
            return NULL;
        if (*hp) {
            switch((*nodepp)->balance) {
            case -1:
                (*nodepp)->balance = 0;
                *hp = RDB_FALSE;
                break;
            case 0:
                (*nodepp)->balance = 1;
                break;
            case 1:
                nodep1 = (*nodepp)->right;
                if (nodep1->balance == 1) {
                    (*nodepp)->right = nodep1->left;
                    if (nodep1->left != NULL)
                        nodep1->left->parent = *nodepp;
                    nodep1->left = *nodepp;
                    (*nodepp)->parent = nodep1;
                    (*nodepp)->balance = 0;
                    nodep1->parent = parentp;
                    *nodepp = nodep1;
                } else {
                    nodep2 = nodep1->left;
                    nodep1->left = nodep2->right;
                    if (nodep2->right != NULL)
                        nodep2->right->parent = nodep1;
                    nodep2->right = nodep1;
                    nodep1->parent = nodep2;
                    (*nodepp)->right = nodep2->left;
                    if (nodep2->left != NULL)
                        nodep2->left->parent = *nodepp;
                    nodep2->left = *nodepp;
                    (*nodepp)->parent = nodep2;
                    if (nodep2->balance == 1) {
                        (*nodepp)->balance = -1;
                    } else {
                        (*nodepp)->balance = 0;
                    }
                    if (nodep2->balance == -1) {
                        nodep1->balance = 1;
                    } else {
                        nodep1->balance = 0;
                    }
                    nodep2->parent = parentp;
                    *nodepp = nodep2;
                }
                (*nodepp)->balance = 0;
                *hp = RDB_FALSE;
                break;
            }
        }
    }
    return newnodep;
}

RDB_tree_node *
RDB_tree_insert(RDB_binary_tree *treep, void *key, size_t keylen,
        void *val, size_t vallen, RDB_exec_context *ecp)
{
    RDB_bool h = RDB_FALSE;
    return avl_tree_insert(treep, &treep->root, NULL, key, keylen, val, vallen, &h, ecp);
}

static void
avl_balance_l(RDB_tree_node **nodepp, RDB_bool *hp)
{
    RDB_tree_node *parentp;
    RDB_tree_node *nodep1, *nodep2;
    int bal;

    switch((*nodepp)->balance) {
    case -1:
        (*nodepp)->balance = 0;
        break;
    case 0:
        (*nodepp)->balance = 1;
        *hp = RDB_FALSE;
        break;
    case 1:
        parentp = (*nodepp)->parent;
        nodep1 = (*nodepp)->right;
        bal = nodep1->balance;
        if (bal >= 0) {
            (*nodepp)->right = nodep1->left;
            if (nodep1->left != NULL)
                nodep1->left->parent = *nodepp;
            nodep1->left = *nodepp;
            (*nodepp)->parent = nodep1;
            if (bal == 0) {
                (*nodepp)->balance = 1;
                nodep1->balance = -1;
                *hp = RDB_FALSE;
            } else {
                (*nodepp)->balance = 0;
                nodep1->balance = 0;
            }
            *nodepp = nodep1;
        } else {
            nodep2 = nodep1->left;
            bal = nodep2->balance;
            nodep1->left = nodep2->right;
            if (nodep2->right != NULL)
                nodep2->right->parent = nodep1;
            nodep2->right = nodep1;
            nodep1->parent = nodep2;
            (*nodepp)->right = nodep2->left;
            if (nodep2->left != NULL)
                nodep2->left->parent = *nodepp;
            nodep2->left = *nodepp;
            (*nodepp)->parent = nodep2;
            (*nodepp)->balance = (bal == 1) ? -1 : 0;
            nodep1->balance = (bal == -1) ? 1 : 0;
            *nodepp = nodep2;
            nodep2->balance = 0;
        }
        (*nodepp)->parent = parentp;
    }
}

static void
avl_balance_r(RDB_tree_node **nodepp, RDB_bool *hp)
{
    RDB_tree_node *parentp;
    RDB_tree_node *nodep1, *nodep2;
    int bal;

    switch((*nodepp)->balance) {
    case 1:
        (*nodepp)->balance = 0;
        break;
    case 0:
        (*nodepp)->balance = -1;
        *hp = RDB_FALSE;
        break;
    case -1:
        parentp = (*nodepp)->parent;
        nodep1 = (*nodepp)->left;
        bal = nodep1->balance;
        if (bal <= 0) {
            (*nodepp)->left = nodep1->right;
            if (nodep1->right != NULL)
                nodep1->right->parent = *nodepp;
            nodep1->right = *nodepp;
            (*nodepp)->parent = nodep1;
            if (bal == 0) {
                (*nodepp)->balance = -1;
                nodep1->balance = 1;
                *hp = RDB_FALSE;
            } else {
                (*nodepp)->balance = 0;
                nodep1->balance = 0;
            }
            *nodepp = nodep1;
        } else {
            nodep2 = nodep1->right;
            bal = nodep2->balance;
            nodep1->right = nodep2->left;
            if (nodep2->left != NULL)
                nodep2->left->parent = nodep1;
            nodep2->left = nodep1;
            nodep1->parent = nodep2;
            (*nodepp)->left = nodep2->right;
            if (nodep2->right != NULL)
                nodep2->right->parent = *nodepp;
            nodep2->right = *nodepp;
            (*nodepp)->parent = nodep2;
            (*nodepp)->balance = (bal == -1) ? 1 : 0;
            nodep1->balance = (bal == 1) ? -1 : 0;
            *nodepp = nodep2;
            nodep2->balance = 0;
        }
        (*nodepp)->parent = parentp;
    }
}

static void
del_rightmost(RDB_tree_node **rnodepp,
        RDB_tree_node **delnodepp, RDB_bool *hp)
{
    if ((*rnodepp)->right != NULL) {
        del_rightmost(&(*rnodepp)->right, delnodepp, hp);
        if (*hp) {
            avl_balance_r(rnodepp, hp);
        }
    } else {
        if ((*delnodepp)->keylen > 0)
            free ((*delnodepp)->key);
        if ((*delnodepp)->valuelen > 0)
            free ((*delnodepp)->value);
        (*delnodepp)->keylen = (*rnodepp)->keylen;
        (*delnodepp)->key = (*rnodepp)->key;
        (*delnodepp)->valuelen = (*rnodepp)->valuelen;
        (*delnodepp)->value = (*rnodepp)->value;
        (*rnodepp)->keylen = 0;
        (*rnodepp)->key = NULL;
        (*rnodepp)->valuelen = 0;
        (*rnodepp)->value = NULL;
        *delnodepp = *rnodepp;
        if ((*rnodepp)->left != NULL)
            (*rnodepp)->left->parent = (*rnodepp)->parent;
        *rnodepp = (*rnodepp)->left;
        *hp = RDB_TRUE;
    }
}

static int
avl_delete_node(RDB_binary_tree *treep, RDB_tree_node **nodepp,
        void *key, size_t keylen, RDB_bool *hp, RDB_exec_context *ecp)
{
    int cmpres;
    int ret;

    if (*nodepp == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    cmpres = compare_key(treep, key, keylen, *nodepp);
    if (cmpres < 0) {
        ret = avl_delete_node(treep, &(*nodepp)->left, key, keylen, hp, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        if (*hp) {
            avl_balance_l(nodepp, hp);
        }
    } else if (cmpres > 0) {
        ret = avl_delete_node(treep, &(*nodepp)->right, key, keylen, hp, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        if (*hp) {
            avl_balance_r(nodepp, hp);
        }
    } else {
        RDB_tree_node *delnodep = *nodepp;
        if (delnodep->right == NULL) {
            *nodepp = delnodep->left;
            *hp = RDB_TRUE;
        } else if (delnodep->left == NULL) {
            *nodepp = delnodep->right;
            *hp = RDB_TRUE;
        } else {
            del_rightmost(&delnodep->left, &delnodep, hp);
            if (*hp) {
                avl_balance_l(nodepp, hp);
            }
        }
        if (delnodep->left != NULL)
            delnodep->left->parent = delnodep->parent;
        if (delnodep->right != NULL)
            delnodep->right->parent = delnodep->parent;
        del_node(delnodep);
    }
    return RDB_OK;
}

int
RDB_tree_delete_node(RDB_binary_tree *treep, void *key, size_t keylen,
        RDB_exec_context *ecp)
{
    RDB_bool h = RDB_FALSE;
    return avl_delete_node(treep, &treep->root, key, keylen, &h, ecp);
}
