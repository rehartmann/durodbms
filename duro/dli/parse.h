#ifndef RDB_PARSE_H
#define RDB_PARSE_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2008 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>

typedef enum {
    RDB_STMT_NOOP,
    RDB_STMT_CALL,
    RDB_STMT_VAR_DEF,
    RDB_STMT_VAR_DEF_REAL,
    RDB_STMT_VAR_DEF_VIRTUAL,
    RDB_STMT_VAR_DEF_PRIVATE,
    RDB_STMT_VAR_DROP,
    RDB_STMT_IF,
    RDB_STMT_FOR,
    RDB_STMT_WHILE,
    RDB_STMT_ASSIGN,
    RDB_STMT_BEGIN_TX,
    RDB_STMT_COMMIT,
    RDB_STMT_ROLLBACK,
    RDB_STMT_TYPE_DEF,
    RDB_STMT_TYPE_DROP,
    RDB_STMT_RO_OP_DEF,
    RDB_STMT_UPD_OP_DEF,
    RDB_STMT_OP_DROP,
    RDB_STMT_RETURN,
    RDB_STMT_CONSTRAINT_DEF,
    RDB_STMT_CONSTRAINT_DROP
} RDB_parse_stmt_kind;

typedef struct RDB_parse_assign {
    enum {
        RDB_STMT_COPY,
        RDB_STMT_INSERT,
        RDB_STMT_UPDATE,
        RDB_STMT_DELETE
    } kind;
    union {
        struct {
            RDB_expression *dstp;
            RDB_expression *srcp;
        } copy;
        struct {
            RDB_expression *dstp;
            RDB_expression *srcp;
        } ins;
        struct {
            RDB_expression *dstp;
            RDB_expression *condp;
            struct RDB_parse_assign *assignlp;
        } upd;
        struct {
            RDB_expression *dstp;
            RDB_expression *condp;
        } del;
    } var;
    struct RDB_parse_assign *nextp;
} RDB_parse_assign;

typedef struct RDB_parse_keydef {
    RDB_expr_list attrlist;
    struct RDB_parse_keydef *nextp;
} RDB_parse_keydef;

typedef struct RDB_parse_type {
    RDB_expression *exp;
    RDB_type *typ;
} RDB_parse_type;

typedef struct RDB_parse_arg {
    RDB_object name;
    RDB_parse_type type;
    RDB_bool upd;
} RDB_parse_arg;

typedef struct RDB_parse_possrep {
    RDB_expression *namexp;
    RDB_expr_list attrlist;
    struct RDB_parse_possrep *nextp;
} RDB_parse_possrep;

typedef struct RDB_parse_statement {
    RDB_parse_stmt_kind kind;
    union {
        struct {
            RDB_object opname;
            RDB_expr_list arglist;
        } call;
        struct {
            RDB_object varname;
            RDB_parse_type type;
            RDB_expression *exp;
            RDB_parse_keydef *firstkeyp;
        } vardef;
        struct {
            RDB_object varname;
        } vardrop;
        struct {
            RDB_parse_assign *assignp;
        } assignment;
        struct {
            RDB_expression *condp;
            struct RDB_parse_statement *ifp;
            struct RDB_parse_statement *elsep;
        } ifthen;
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
        struct {
            RDB_object typename;
            RDB_parse_possrep *replistp;
            RDB_expression *constraintp;
        } deftype;
        struct {
            RDB_object typename;
        } typedrop;
        struct {
            RDB_object opname;
            int argc;
            RDB_parse_arg *argv;
            RDB_parse_type rtype;
            struct RDB_parse_statement *bodyp;
        } opdef;
        struct {
            RDB_object opname;
        } opdrop;
        struct {
            RDB_object constrname;
            RDB_expression *constraintp;
        } constrdef;
        struct {
            RDB_object constrname;
        } constrdrop;
        RDB_expression *retexp;
    } var;
    RDB_int lineno;
    struct RDB_parse_statement *nextp;
} RDB_parse_statement;

#if defined (_WIN32) && !defined (NO_DLL_IMPORT)
#define _RDB_EXTERN_VAR __declspec(dllimport)
#else
#define _RDB_EXTERN_VAR extern
#endif

_RDB_EXTERN_VAR int _RDB_parse_interactive;
_RDB_EXTERN_VAR int _RDB_parse_case_insensitive;
_RDB_EXTERN_VAR char *_RDB_parse_prompt;

void
_RDB_parse_init_buf(void);

int
RDB_parse_destroy_assign(RDB_parse_assign *, RDB_exec_context *);

RDB_expression *
RDB_parse_expr(const char *, RDB_exec_context *);

RDB_parse_statement *
RDB_parse_stmt(RDB_exec_context *);

int
RDB_parse_del_keydef_list(RDB_parse_keydef *firstkeyp, RDB_exec_context *);

RDB_int
RDB_parse_assignlist_length(const RDB_parse_assign *);

int
RDB_parse_del_assignlist(RDB_parse_assign *, RDB_exec_context *);

int
RDB_parse_del_stmt(RDB_parse_statement *, RDB_exec_context *);

int
RDB_parse_del_stmtlist(RDB_parse_statement *, RDB_exec_context *);

RDB_parse_statement *
RDB_parse_new_call(char *name, RDB_expr_list *);

#endif
