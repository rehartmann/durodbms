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
int _RDB_parse_case_insensitive = 1;

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
        sprintf(bufp, "%s at line %d", errtxt, yylloc.first_line);
        RDB_raise_syntax(bufp, _RDB_parse_ecp);
        free(bufp);
    }
}

int
RDB_parse_del_keydef_list(RDB_parse_keydef *firstkeyp, RDB_exec_context *ecp)
{
    int ret = RDB_OK;
    while (firstkeyp != NULL) {
        RDB_parse_keydef *nextp = firstkeyp->nextp;
        if (RDB_destroy_expr_list(&firstkeyp->attrlist, _RDB_parse_ecp) != RDB_OK)
            ret = RDB_ERROR;
        firstkeyp = nextp;
    }
    return ret;
}

int
RDB_parse_destroy_assign(RDB_parse_assign *ap, RDB_exec_context *ecp)
{
    switch(ap->kind) {
        case RDB_STMT_COPY:
            RDB_drop_expr(ap->var.copy.dstp, ecp);
            RDB_drop_expr(ap->var.copy.srcp, ecp);
            break;
        case RDB_STMT_INSERT:
            RDB_drop_expr(ap->var.ins.dstp, ecp);
            RDB_drop_expr(ap->var.ins.srcp, ecp);
            break;
        case RDB_STMT_UPDATE:
            RDB_drop_expr(
                    ap->var.upd.dstp, ecp);
            if (ap->var.upd.condp != NULL)
                RDB_drop_expr(ap->var.upd.condp, ecp);
            RDB_parse_del_assignlist(ap->var.upd.assignlp, ecp);
            break;
        case RDB_STMT_DELETE:
            RDB_drop_expr(
                    ap->var.del.dstp, ecp);
            if (ap->var.del.condp != NULL)
                RDB_drop_expr(ap->var.del.condp, ecp);
            break;
    }
    return RDB_OK;
}

int
RDB_parse_del_assignlist(RDB_parse_attr_assign *ap, RDB_exec_context *ecp)
{
    RDB_parse_attr_assign *hap;
    do {
        RDB_drop_expr(ap->dstp, ecp);
        RDB_drop_expr(ap->srcp, ecp);
        hap = ap->nextp;
        free(ap);
        ap = hap;
    } while (ap != NULL);
    return RDB_OK;
}

int
RDB_parse_del_stmt(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int ret = RDB_OK;

    switch(stmtp->kind) {
        case RDB_STMT_CALL:
            ret = RDB_destroy_expr_list(&stmtp->var.call.arglist, ecp);
            break;
        case RDB_STMT_VAR_DEF:
            ret = RDB_destroy_obj(&stmtp->var.vardef.varname, ecp);
            if (stmtp->var.vardef.typ != NULL
                    && !RDB_type_is_scalar(stmtp->var.vardef.typ))
                ret = RDB_drop_type(stmtp->var.vardef.typ, ecp, NULL);
            if (stmtp->var.vardef.initexp != NULL)
                ret = RDB_drop_expr(stmtp->var.vardef.initexp, ecp);
            break;
        case RDB_STMT_VAR_DEF_REAL:
            ret = RDB_destroy_obj(&stmtp->var.vardef_real.varname, ecp);
            if (stmtp->var.vardef_real.typ != NULL
                    && !RDB_type_is_scalar(stmtp->var.vardef_real.typ))
                ret = RDB_drop_type(stmtp->var.vardef_real.typ, ecp, NULL);
            if (stmtp->var.vardef_real.initexp != NULL)
                ret = RDB_drop_expr(stmtp->var.vardef_real.initexp, ecp);
            if (stmtp->var.vardef_real.firstkeyp != NULL)
                RDB_parse_del_keydef_list(stmtp->var.vardef_real.firstkeyp,
                    ecp);
            break;
        case RDB_STMT_VAR_DEF_VIRTUAL:
            RDB_destroy_obj(&stmtp->var.vardef_virtual.varname, ecp);
            ret = RDB_drop_expr(stmtp->var.vardef_virtual.exp, ecp);
            break;
        case RDB_STMT_VAR_DROP:
            ret = RDB_destroy_obj(&stmtp->var.vardrop.varname, ecp);
            break;
        case RDB_STMT_IF:
            RDB_drop_expr(stmtp->var.ifthen.condp, ecp);
            RDB_parse_del_stmtlist(stmtp->var.ifthen.ifp, ecp);
            if (stmtp->var.ifthen.elsep != NULL)
                ret = RDB_parse_del_stmtlist(stmtp->var.ifthen.elsep, ecp);
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
        case RDB_STMT_ASSIGN:            
            for (i = 0; i < stmtp->var.assignment.ac; i++) {
                ret = RDB_parse_destroy_assign(&stmtp->var.assignment.av[i],
                        ecp);
            }
            break;
        case RDB_STMT_BEGIN_TX:
        case RDB_STMT_COMMIT:
        case RDB_STMT_ROLLBACK:
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

/**@defgroup parse Parsing functions
 * \#include <dli/parse.h>
 * @{
 */

/**
 * Parse the <a href="../expressions.html">expression</a>
specified by <var>txt</var>. If <var>lt_fp</var> is not NULL,
it must point to a function which is used to look up local tables.
The function is invoked with the table name and <var>lt_arg</var> as
arguments. It must return a pointer to the table or NULL if the table was
not found.

@returns The parsed expression, or NULL if the parsing failed.

@par Errors:

<dl>
<dt>RDB_SYNTAX_ERROR
<dd>A syntax error occurred during parsing.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.

@warning The parser is not reentrant.
 */
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

/*@}*/

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
