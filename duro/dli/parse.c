/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "parse.h"
#include <rel/rdb.h>
#include <rel/internal.h>

#include <stdio.h>
#include <string.h>

int yyparse(void);

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *txt);
void yy_delete_buffer(YY_BUFFER_STATE);

RDB_transaction *_RDB_parse_txp;
RDB_expression *_RDB_parse_resultp;
RDB_parse_statement *_RDB_parse_stmtp;
RDB_ltablefn *_RDB_parse_ltfp;
void *_RDB_parse_arg;
RDB_exec_context *_RDB_parse_ecp;
int _RDB_parse_interactive = 0;

RDB_expression *
_RDB_parse_lookup_table(RDB_expression *);

void
_RDB_parse_start_exp(void);

void
_RDB_parse_start_stmt(void);

void
yy_switch_to_buffer(YY_BUFFER_STATE);

typedef struct YYLTYPE
{
    int first_line;
    int first_column;
    int last_line;
    int last_column;
} YYLTYPE;

extern YYLTYPE yylloc;

void yyerror(char *errtxt)
{
    if (RDB_get_err(_RDB_parse_ecp) == NULL) {
        char *bufp = malloc(strlen(errtxt) + 32);
        if (bufp == NULL) {
            RDB_raise_no_memory(_RDB_parse_ecp);
            return;
        }
        sprintf(bufp, ": %s at line %d", errtxt, yylloc.first_line);
        RDB_raise_syntax(bufp, _RDB_parse_ecp);
        free(bufp);
    }
}

int
RDB_parse_del_stmt(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int ret = RDB_OK;

    switch(stmtp->kind) {
        case RDB_STMT_CALL:
            for (i = 0; i < stmtp->var.call.argc; i++)
                RDB_drop_expr(stmtp->var.call.argv[i], ecp);
            ret = RDB_destroy_obj(&stmtp->var.call.opname, ecp);
            break;
        case RDB_STMT_VAR_DEF:
            ret = RDB_destroy_obj(&stmtp->var.vardef.varname, ecp);
            if (stmtp->var.vardef.typ != NULL
                    && !RDB_type_is_scalar(stmtp->var.vardef.typ))
                ret = RDB_drop_type(stmtp->var.vardef.typ, ecp, NULL);
            if (stmtp->var.vardef.initexp != NULL)
                ret = RDB_drop_expr(stmtp->var.vardef.initexp, ecp);
            break;
        case RDB_STMT_IF:
            RDB_drop_expr(stmtp->var.ifthen.condp, ecp);
            RDB_parse_del_stmtlist(stmtp->var.ifthen.ifp, ecp);
            if (stmtp->var.ifthen.elsep != NULL)
                ret = RDB_parse_del_stmtlist(stmtp->var.ifthen.elsep, ecp);
            break;
        case RDB_STMT_ASSIGN:
            for (i = 0; i < stmtp->var.assignment.ac; i++) {
                 RDB_drop_expr(stmtp->var.assignment.av[i].dstp, ecp);
                 RDB_drop_expr(stmtp->var.assignment.av[i].srcp, ecp);
            }
            break;
        case RDB_STMT_FOR:
            RDB_drop_expr(stmtp->var.forloop.varexp, ecp);
            RDB_drop_expr(stmtp->var.forloop.fromp, ecp);
            RDB_drop_expr(stmtp->var.forloop.top, ecp);
            ret = RDB_parse_del_stmtlist(stmtp->var.forloop.bodyp, ecp);
            break;
        case RDB_STMT_WHILE:
            RDB_drop_expr(stmtp->var.whileloop.condp, ecp);
            ret = RDB_parse_del_stmtlist(stmtp->var.whileloop.bodyp, ecp);
            break;
        case RDB_STMT_NOOP:
            break;
    }
    free(stmtp);
    return ret;
}

int
RDB_parse_del_stmtlist(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int r;
    RDB_parse_statement *nextp;
    int ret = RDB_OK;
    
    do {
        nextp = stmtp->nextp;
        r = RDB_parse_del_stmt(stmtp, ecp);
        if (r != RDB_OK)
            ret = RDB_ERROR;
        stmtp = nextp;
    } while (stmtp != NULL);
    return ret;
}

RDB_expression *
RDB_parse_expr(const char *txt, RDB_ltablefn *lt_fp, void *lt_arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int pret;
    RDB_expression *exp;
    YY_BUFFER_STATE buf;

    _RDB_parse_txp = txp;
    _RDB_parse_ltfp = lt_fp;
    _RDB_parse_arg = lt_arg;
    _RDB_parse_ecp = ecp;

    buf = yy_scan_string(txt);
    _RDB_parse_start_exp();
    pret = yyparse();
    yy_delete_buffer(buf);
    if (pret != 0) {
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("parser error", ecp);
        }
        return NULL;
    }

    /* If the expression represents an attribute, try to get table */
    if (_RDB_parse_resultp->kind == RDB_EX_VAR) {
        exp = _RDB_parse_lookup_table(_RDB_parse_resultp);
        if (exp == NULL) {
            RDB_drop_expr(_RDB_parse_resultp, ecp);
            RDB_raise_no_memory(ecp);
            return NULL;
        }
    } else {
        exp = _RDB_parse_resultp;
    }
    return exp;
}

RDB_parse_statement *
RDB_parse_stmt(RDB_exec_context *ecp)
{
    int pret;

    _RDB_parse_ecp = ecp;

    _RDB_parse_start_stmt();
    pret = yyparse();
    if (pret != 0) {
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("parser error", ecp);
        }
        return NULL;
    }
    return _RDB_parse_stmtp;
}
