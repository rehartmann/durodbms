/*
 * AVL Tree
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef TREEREC_TREE_H_
#define TREEREC_TREE_H_

#include <stdlib.h>

typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_index RDB_index;

typedef int RDB_comparison_func (const void *d1, size_t size1,
        const void *d2, size_t size2, void *comparison_arg);

typedef struct RDB_tree_node {
    struct RDB_tree_node *left;
    struct RDB_tree_node *right;
    struct RDB_tree_node *parent;
    int balance;
    size_t keylen;
    size_t valuelen;
    void *key;
    void *value;
} RDB_tree_node;

typedef struct {
    RDB_tree_node *root;
    RDB_comparison_func *comparison_fp;
    void *comparison_arg;
} RDB_binary_tree;

RDB_binary_tree *
RDB_create_tree(RDB_comparison_func *, void *, RDB_exec_context *);

void
RDB_drop_tree(RDB_binary_tree *);

RDB_tree_node *
RDB_tree_find(const RDB_binary_tree *, const void *, size_t);

RDB_tree_node *
RDB_tree_insert(RDB_binary_tree *, void *, size_t,
        void *, size_t, RDB_exec_context *);

int
RDB_tree_delete_node(RDB_binary_tree *, void *key, size_t keylen,
        RDB_exec_context *);

#endif /* TREEREC_TREE_H_ */
