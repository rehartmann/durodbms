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

typedef int RDB_comparison_func (const void *d1, size_t size1,
        const void *d2, size_t size2, void *arg);

typedef struct RDB_binary_tree {
    struct tree_node *root;
    RDB_comparison_func *comparison_fp;
    void *arg;
} RDB_binary_tree;

struct tree_node_header {
    struct tree_node *left;
    struct tree_node *right;
    struct tree_node *parent;
    int balance;
    size_t keylen;
    size_t valuelen;
};

typedef struct tree_node {
    struct tree_node_header header;
    RDB_byte data[1];
} RDB_tree_node;

RDB_binary_tree *
RDB_tree_create(RDB_comparison_func *, void *, RDB_exec_context *);

void
RDB_tree_delete(RDB_binary_tree *);

void *
RDB_tree_get(const RDB_binary_tree *, void *, size_t, size_t *);

RDB_tree_node *
RDB_tree_find(const RDB_binary_tree *, void *, size_t);

int
RDB_tree_insert(RDB_binary_tree *, void *, size_t,
        void *, size_t, RDB_exec_context *);

#endif /* TREEREC_TREE_H_ */
