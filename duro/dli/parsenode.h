#ifndef RDB_PARSENODE_H
#define RDB_PARSENODE_H

#include <rel/rdb.h>

/*
 * $Id$
 *
 * Copyright (C) 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

enum _RDB_node_kind {
    RDB_NODE_TOK, /* Token */
    RDB_NODE_EXPR, /* Expression */
    RDB_NODE_INNER /* List of nodes */
};

typedef struct RDB_parse_node {
    enum _RDB_node_kind kind;
    union {
        int token;
        struct {
            struct RDB_parse_node *firstp;
            struct RDB_parse_node *lastp;
        } children;
    } val;

    /*
     * Not in the union because a list of nodes can represent a read-only
     * operator invocation which can be converted to an expression
     */
    RDB_expression *exp;

    /* Pointer to the following node, NULL if there is no node following */
    struct RDB_parse_node *nextp;

    /* Line number in the source file */
    int lineno;

    /* White space or comment before the node */
    RDB_object *whitecommp;
} RDB_parse_node;

RDB_parse_node *
RDB_new_parse_token(int tok, RDB_object *, RDB_exec_context *);

RDB_parse_node *
RDB_new_parse_inner(RDB_exec_context *);

RDB_parse_node *
RDB_new_parse_expr(RDB_expression *, RDB_object *, RDB_exec_context *);

const char *
RDB_parse_node_ID(const RDB_parse_node *);

void
RDB_parse_add_child(RDB_parse_node *, RDB_parse_node *);

int
RDB_parse_del_node(RDB_parse_node *, RDB_exec_context *);

int
RDB_parse_del_nodelist(RDB_parse_node *, RDB_exec_context *);

RDB_int
RDB_parse_nodelist_length(const RDB_parse_node *);

RDB_parse_node *
RDB_parse_node_child(const RDB_parse_node *, RDB_int idx);

int
RDB_parse_node_var_name_idx(const RDB_parse_node *, const char *);

int
Duro_parse_node_to_obj_string(RDB_object *, RDB_parse_node *,
        RDB_exec_context *, RDB_transaction *);

void
RDB_print_parse_node(FILE *, RDB_parse_node *,
        RDB_exec_context *);

const char *
_RDB_token_name(int);

#endif
