/*
 * $Id$
 *
 * Copyright (C) 2003-2008 René Hartmann.
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

RDB_expression *_RDB_parse_resultp;
RDB_parse_statement *_RDB_parse_stmtp;
RDB_exec_context *_RDB_parse_ecp;
int _RDB_parse_interactive = 0;
int _RDB_parse_case_insensitive = 1;

void
_RDB_parse_start_exp(void);

void
_RDB_parse_start_stmt(void);

void
yyerror(char *errtxt)
{
    if (RDB_get_err(_RDB_parse_ecp) == NULL) {
        char *bufp = RDB_alloc(strlen(errtxt) + 32, _RDB_parse_ecp);
        if (bufp == NULL) {
            return;
        }
        sprintf(bufp, "%s", errtxt);
        RDB_raise_syntax(bufp, _RDB_parse_ecp);
        RDB_free(bufp);
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
    switch (ap->kind) {
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
RDB_parse_del_assignlist(RDB_parse_assign *ap, RDB_exec_context *ecp)
{
    RDB_parse_assign *hap;
    do {
        hap = ap->nextp;
        RDB_parse_destroy_assign(ap, ecp);
        RDB_free(ap);
        ap = hap;
    } while (ap != NULL);
    return RDB_OK;
}

static int
destroy_parse_type(RDB_parse_type *ptyp, RDB_exec_context *ecp)
{
    int ret = RDB_OK;
    
    if (ptyp->exp != NULL)
        ret = RDB_drop_expr(ptyp->exp, ecp);
    if (ptyp->typ != NULL && !RDB_type_is_scalar(ptyp->typ))
        ret = RDB_drop_type(ptyp->typ, ecp, NULL);
    return ret;    
}

int
RDB_parse_del_stmt(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret = RDB_OK;

    switch(stmtp->kind) {
        case RDB_STMT_CALL:
            ret = RDB_destroy_expr_list(&stmtp->var.call.arglist, ecp);
            break;
        case RDB_STMT_VAR_DEF:
            ret = RDB_destroy_obj(&stmtp->var.vardef.varname, ecp);
            ret = destroy_parse_type(&stmtp->var.vardef.type, ecp);
            if (stmtp->var.vardef.exp != NULL)
                ret = RDB_drop_expr(stmtp->var.vardef.exp, ecp);
            break;
        case RDB_STMT_VAR_DEF_REAL:
        case RDB_STMT_VAR_DEF_PRIVATE:
            ret = RDB_destroy_obj(&stmtp->var.vardef.varname, ecp);
            ret = destroy_parse_type(&stmtp->var.vardef.type, ecp);
            if (stmtp->var.vardef.exp != NULL)
                ret = RDB_drop_expr(stmtp->var.vardef.exp, ecp);
            if (stmtp->var.vardef.firstkeyp != NULL)
                RDB_parse_del_keydef_list(stmtp->var.vardef.firstkeyp,
                    ecp);
            break;
        case RDB_STMT_VAR_DEF_VIRTUAL:
            RDB_destroy_obj(&stmtp->var.vardef.varname, ecp);
            ret = RDB_drop_expr(stmtp->var.vardef.exp, ecp);
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
            ret = RDB_parse_del_assignlist(stmtp->var.assignment.assignp, ecp);
            break;
        case RDB_STMT_TYPE_DEF:
            {
                RDB_parse_possrep *rep = stmtp->var.deftype.replistp;
                while (rep != NULL) {
                    RDB_parse_possrep *nextrep = rep->nextp;

                    if (rep->namexp != NULL)
                        RDB_drop_expr(rep->namexp, ecp);
                    RDB_destroy_expr_list(&rep->attrlist, ecp);
                    RDB_free(rep);
                    rep = nextrep;
                }
                if (stmtp->var.deftype.constraintp != NULL)
                    RDB_drop_expr(stmtp->var.deftype.constraintp, ecp);
                ret = RDB_destroy_obj(&stmtp->var.deftype.typename, ecp);
            }
            break;
        case RDB_STMT_TYPE_DROP:
            ret = RDB_destroy_obj(&stmtp->var.typedrop.typename, ecp);
            break;
        case RDB_STMT_RO_OP_DEF:
            destroy_parse_type(&stmtp->var.opdef.rtype, ecp);
            ret = RDB_destroy_obj(&stmtp->var.opdef.opname, ecp);
            break;
        case RDB_STMT_UPD_OP_DEF:
            ret = RDB_destroy_obj(&stmtp->var.opdef.opname, ecp);
            break;
        case RDB_STMT_OP_DROP:
            ret = RDB_destroy_obj(&stmtp->var.opdrop.opname, ecp);
            break;
        case RDB_STMT_RETURN:
            if (stmtp->var.retexp != NULL)
                ret = RDB_drop_expr(stmtp->var.retexp, ecp);
            break;
        case RDB_STMT_CONSTRAINT_DEF:
            RDB_destroy_obj(&stmtp->var.constrdef.constrname, ecp);
            ret = RDB_drop_expr(stmtp->var.constrdef.constraintp, ecp);
            break;
        case RDB_STMT_CONSTRAINT_DROP:
            ret = RDB_destroy_obj(&stmtp->var.constrdrop.constrname, ecp);
            break;
        case RDB_STMT_BEGIN_TX:
        case RDB_STMT_COMMIT:
        case RDB_STMT_ROLLBACK:
        case RDB_STMT_NOOP:
            break;
    }
    RDB_free(stmtp);
    return ret;
}

RDB_int
RDB_parse_assignlist_length(const RDB_parse_assign *assignp)
{
    RDB_int cnt = 0;
    while (assignp != NULL) {
        cnt++;
        assignp = assignp->nextp;
    }
    return cnt;
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

RDB_parse_statement *
RDB_parse_new_call(char *name, RDB_expr_list *explistp)
{
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL)
        return NULL;

    stmtp->kind = RDB_STMT_CALL;
    RDB_init_obj(&stmtp->var.call.opname);
    if (RDB_string_to_obj(&stmtp->var.call.opname, name, _RDB_parse_ecp)
            != RDB_OK) {
        RDB_destroy_obj(&stmtp->var.call.opname, NULL);
        free(stmtp);
        return NULL;
    }
    stmtp->var.call.arglist = *explistp;
    return stmtp;
}

/**@defgroup parse Parsing functions
 * \#include <dli/parse.h>
 * @{
 */

/**
 * Parse the <a href="../../expressions.html">expression</a>
specified by <var>txt</var>.

@returns The parsed expression, or NULL if the parsing failed.

@par Errors:
<dl>
<dt>RDB_SYNTAX_ERROR
<dd>A syntax error occurred during parsing.
</dl>

The call may also fail for a @ref system-errors "system error".

@warning The parser is not reentrant.
 */
RDB_expression *
RDB_parse_expr(const char *txt, RDB_exec_context *ecp)
{
    int pret;
    YY_BUFFER_STATE buf;

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

    return _RDB_parse_resultp;
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
