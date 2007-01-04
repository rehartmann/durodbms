#ifndef RDB_PARSE_H
#define RDB_PARSE_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>

enum {
    DURO_MAX_LLEN = 64
};

typedef enum {
    RDB_STMT_NOOP,
    RDB_STMT_CALL,
    RDB_STMT_VAR_DEF,
    RDB_STMT_IF,
    RDB_STMT_ASSIGN,
    RDB_STMT_FOR,
    RDB_STMT_WHILE
} RDB_parse_stmt_kind;

typedef struct RDB_parse_statement {
    RDB_parse_stmt_kind kind;
    union {
        struct {
            RDB_object opname;
            int argc;
            RDB_expression *argv[DURO_MAX_LLEN];
        } call;
        struct {
            RDB_object varname;
            RDB_type *typ;
            RDB_expression *initexp;
        } vardef;
        struct {
            RDB_expression *condp;
            struct RDB_parse_statement *ifp;
            struct RDB_parse_statement *elsep;
        } ifthen;
        struct {
            int ac;
            struct {
                RDB_expression *dstp;
                RDB_expression *srcp;
            } av[DURO_MAX_LLEN];
        } assignment;
        struct {
            RDB_expression *varexp;
            RDB_expression *fromp;
            RDB_expression *top;
            struct RDB_parse_statement *bodyp;
        } forloop;
        struct {
            RDB_expression *condp;
            struct RDB_parse_statement *bodyp;
        } whileloop;
    } var;
    struct RDB_parse_statement *nextp;
} RDB_parse_statement;

typedef RDB_object *RDB_ltablefn(const char *, void *);

extern int _RDB_parse_interactive;

void
_RDB_parse_init_buf(void);

RDB_expression *
RDB_parse_expr(const char *, RDB_ltablefn *, void *, RDB_exec_context *,
        RDB_transaction *);

RDB_parse_statement *
RDB_parse_stmt(RDB_exec_context *);

int
RDB_parse_del_stmt(RDB_parse_statement *, RDB_exec_context *);

int
RDB_parse_del_stmtlist(RDB_parse_statement *, RDB_exec_context *);

#endif
