#ifndef RDB_PARSE_H
#define RDB_PARSE_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2011 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <stdio.h>
#include <rel/rdb.h>

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
     * a operator invocation which can be converted to an expression
     */
    RDB_expression *exp;

    /* Pointer to the following node, NULL if there is no node following */
    struct RDB_parse_node *nextp;

    /* Line number in the source file */
    int lineno;
} RDB_parse_node;

RDB_parse_node *
RDB_new_parse_token(int tok, RDB_exec_context *);

RDB_parse_node *
RDB_new_parse_inner(RDB_exec_context *);

RDB_parse_node *
RDB_new_parse_expr(RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_parse_node_expr(RDB_parse_node *, RDB_exec_context *);

const char *
RDB_parse_node_ID(const RDB_parse_node *);

void
RDB_parse_add_child(RDB_parse_node *, RDB_parse_node *);

#if defined (_WIN32) && !defined (NO_DLL_IMPORT)
#define _RDB_EXTERN_VAR __declspec(dllimport)
#else
#define _RDB_EXTERN_VAR extern
#endif

_RDB_EXTERN_VAR int _RDB_parse_interactive;
_RDB_EXTERN_VAR int _RDB_parse_case_insensitive;
_RDB_EXTERN_VAR char *_RDB_parse_prompt;

void
_RDB_parse_init_buf(FILE *);

RDB_parse_node *
RDB_parse_expr(const char *, RDB_exec_context *);

RDB_parse_node *
RDB_parse_stmt(RDB_exec_context *);

RDB_parse_node *
RDB_parse_stmt_string(const char *, RDB_exec_context *);

int
RDB_parse_del_node(RDB_parse_node *, RDB_exec_context *);

int
RDB_parse_del_nodelist(RDB_parse_node *, RDB_exec_context *);

RDB_int
RDB_parse_nodelist_length(const RDB_parse_node *);

RDB_parse_node *
RDB_parse_node_child(const RDB_parse_node *, RDB_int idx);

int
Duro_parse_node_to_obj_string(RDB_object *, RDB_parse_node *, RDB_exec_context *, RDB_transaction *);

int
RDB_parse_node_var_name_idx(const RDB_parse_node *, const char *);

void
RDB_print_parse_node(FILE *, RDB_parse_node *,
        RDB_exec_context *);

#endif
