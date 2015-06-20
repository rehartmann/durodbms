/*
 * Parse tree functions.
 *
 * Copyright (C) 2012-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "parsenode.h"
#include "exparse.h"
#include <rel/tostr.h>
#include <string.h>

RDB_parse_node *
RDB_new_parse_token(int tok, RDB_object *wcp, RDB_exec_context *ecp)
{
    RDB_parse_node *pnodep = RDB_alloc(sizeof (RDB_parse_node), ecp);
    if (pnodep == NULL)
        return NULL;
    pnodep->kind = RDB_NODE_TOK;
    pnodep->exp = NULL;
    pnodep->val.token = tok;
    pnodep->whitecommp = wcp;
    return pnodep;
}

RDB_parse_node *
RDB_new_parse_inner(RDB_exec_context *ecp)
{
    RDB_parse_node *pnodep = RDB_alloc(sizeof (RDB_parse_node), ecp);
    if (pnodep == NULL)
        return NULL;
    pnodep->kind = RDB_NODE_INNER;
    pnodep->exp = NULL;
    pnodep->val.children.firstp = pnodep->val.children.lastp = NULL;
    pnodep->whitecommp = NULL;
    return pnodep;
}

RDB_parse_node *
RDB_new_parse_expr(RDB_expression *exp, RDB_object *wcp, RDB_exec_context *ecp)
{
    RDB_parse_node *pnodep = RDB_alloc(sizeof (RDB_parse_node), ecp);
    if (pnodep == NULL)
        return NULL;
    pnodep->kind = RDB_NODE_EXPR;
    pnodep->exp = exp;
    pnodep->whitecommp = wcp;
    return pnodep;
}

const char *
RDB_parse_node_ID(const RDB_parse_node *nodep)
{
    if (nodep->kind == RDB_NODE_EXPR) {
        return RDB_expr_var_name(nodep->exp);
    }
    return NULL;
}

void
RDB_parse_add_child(RDB_parse_node *pnodep, RDB_parse_node *childp) {
    childp->nextp = NULL;
    if (pnodep->val.children.firstp == NULL) {
        pnodep->val.children.firstp = pnodep->val.children.lastp = childp;
    } else {
        pnodep->val.children.lastp->nextp = childp;
        pnodep->val.children.lastp = childp;
    }
}

RDB_int
RDB_parse_nodelist_length(const RDB_parse_node *pnodep)
{
    RDB_parse_node *nodep = pnodep->val.children.firstp;
    RDB_int cnt = 0;
    while (nodep != NULL) {
        cnt++;
        nodep = nodep->nextp;
    }
    return cnt;
}

RDB_parse_node *
RDB_parse_node_child(const RDB_parse_node *nodep, RDB_int idx)
{
    RDB_parse_node *chnodep = nodep->val.children.firstp;
    if (nodep->kind != RDB_NODE_INNER)
        return NULL;
    while (idx-- > 0 && chnodep != NULL) {
        chnodep = chnodep->nextp;
    }
    return chnodep;
}

/**@addtogroup parse
 * \#include <dli/parse.h>
 * @{
 */

/**
 * Destroy the parse node *nodep and free its memory, including all children.
 * The expression returned from RDB_parse_node_expr() will also be destroyed.
 */
int
RDB_parse_del_node(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret = RDB_OK;
    if (nodep->kind == RDB_NODE_INNER
            && nodep->val.children.firstp != NULL) {
        ret = RDB_parse_del_nodelist(nodep->val.children.firstp, ecp);
    }
    if (nodep->exp != NULL) {
        int ret2 = RDB_del_expr(nodep->exp, ecp);
        if (ret == RDB_OK)
            ret = ret2;
    }
    if (nodep->whitecommp != NULL) {
        RDB_free_obj(nodep->whitecommp, ecp);
    }
    RDB_free(nodep);
    return ret;
}

/* @} */

int
RDB_parse_del_nodelist(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    do {
        RDB_parse_node *np = nodep->nextp;
        if (RDB_parse_del_node(nodep, ecp) != RDB_OK)
            return RDB_ERROR;
        nodep = np;
    } while (nodep != NULL);
    return RDB_OK;
}

/*
 * Search var node in a comma-separated list
 */
int
RDB_parse_node_var_name_idx(const RDB_parse_node *nodep, const char *namep)
{
    int idx = 0;

    if (nodep == NULL)
        return -1;
    for(;;) {
        if (strcmp(RDB_expr_var_name(nodep->exp), namep) == 0)
            break;
        idx++;
        nodep = nodep->nextp;
        if (nodep == NULL)
            return -1;
        nodep = nodep->nextp;
    }
    return idx;
}

/*
 * Store string representation of node in *dstp.
 */
int
Duro_parse_node_to_obj_string(RDB_object *dstp, RDB_parse_node *nodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_parse_node *np;
    RDB_object strobj;
    int ret;

    /*
     * Get comments and whitespace, if present
     */
    if (nodep->whitecommp != NULL) {
        ret = RDB_copy_obj(dstp, nodep->whitecommp, ecp);
    } else {
        ret = RDB_string_to_obj(dstp, "", ecp);
    }
    if (ret != RDB_OK)
        return ret;

    switch(nodep->kind) {
    case RDB_NODE_TOK:
        return RDB_append_string(dstp, RDB_token_name(nodep->val.token), ecp);
    case RDB_NODE_INNER:
        RDB_init_obj(&strobj);
        np = nodep->val.children.firstp;
        while (np != NULL) {
            /*
             * Convert child to string and append it
             */
            ret = Duro_parse_node_to_obj_string(&strobj, np, ecp, txp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&strobj, ecp);
                return ret;
            }
            ret = RDB_append_string(dstp, RDB_obj_string(&strobj), ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&strobj, ecp);
                return ret;
            }
            np = np->nextp;
        }
        return RDB_destroy_obj(&strobj, ecp);
    case RDB_NODE_EXPR:
        RDB_init_obj(&strobj);
        ret = RDB_expr_to_str(&strobj, nodep->exp, ecp, txp, 0);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&strobj, ecp);
            return ret;
        }
        ret = RDB_append_string(dstp, RDB_obj_string(&strobj), ecp);
        if (ret != RDB_OK) {
            return ret;
        }
        return RDB_destroy_obj(&strobj, ecp);
    }
    RDB_raise_internal("invalid parse node", ecp);
    return RDB_ERROR;
}

void
RDB_print_parse_node(FILE *fp, RDB_parse_node *nodep,
        RDB_exec_context *ecp)
{
    RDB_object strobj;

    RDB_init_obj(&strobj);
    Duro_parse_node_to_obj_string(&strobj, nodep, ecp, NULL);
    fputs(RDB_obj_string(&strobj), fp);
    RDB_destroy_obj(&strobj, ecp);
}

const char *
RDB_token_name(int tok)
{
    static char chtok[2];

    switch (tok) {
    case TOK_WHERE:
        return "WHERE";
    case TOK_UNION:
        return "UNION";
    case TOK_D_UNION:
        return "D_UNION";
    case TOK_INTERSECT:
        return "INTERSECT";
    case TOK_MINUS:
        return "MINUS";
    case TOK_SEMIMINUS:
        return "SEMIMINUS";
    case TOK_SEMIJOIN:
        return "SEMIJOIN";
    case TOK_MATCHING:
        return "MATCHING";
    case TOK_JOIN:
        return "JOIN";
    case TOK_RENAME:
        return "RENAME";
    case TOK_EXTEND:
        return "EXTEND";
    case TOK_EXTERN:
        return "EXTERN";
    case TOK_SUMMARIZE:
        return "SUMMARIZE";
    case TOK_DIVIDEBY:
        return "DIVIDEBY";
    case TOK_WRAP:
        return "WRAP";
    case TOK_UNWRAP:
        return "UNWRAP";
    case TOK_GROUP:
        return "GROUP";
    case TOK_UNGROUP:
        return "UNGROUP";
    case TOK_CALL:
        return "CALL";
    case TOK_FROM:
        return "FROM";
    case TOK_TUPLE:
        return "TUPLE";
    case TOK_RELATION:
        return "RELATION";
    case TOK_ARRAY:
        return "ARRAY";
    case TOK_BUT:
        return "BUT";
    case TOK_AS:
        return "AS";
    case TOK_PER:
        return "PER";
    case TOK_VAR:
        return "VAR";
    case TOK_CONST:
        return "CONST";
    case TOK_DROP:
        return "DROP";
    case TOK_INIT:
        return "INIT";
    case TOK_INDEX:
        return "INDEX";
    case TOK_BEGIN:
        return "BEGIN";
    case TOK_TX:
        return "TRANSACTION";
    case TOK_REAL:
        return "REAL";
    case TOK_VIRTUAL:
        return "VIRTUAL";
    case TOK_PRIVATE:
        return "PRIVATE";
    case TOK_KEY:
        return "KEY";
    case TOK_COMMIT:
        return "COMMIT";
    case TOK_ROLLBACK:
        return "ROLLBACK";
    case TOK_IN:
        return "IN";
    case TOK_SUBSET_OF:
        return "SUBSET_OF";
    case TOK_OR:
        return "OR";
    case TOK_AND:
        return "AND";
    case TOK_NOT:
        return "NOT";
    case TOK_CONCAT:
        return "||";
    case TOK_NE:
        return "<>";
    case TOK_LE:
        return "<=";
    case TOK_GE:
        return ">=";
    case TOK_LIKE:
        return "LIKE";
    case TOK_REGEX_LIKE:
        return "REGEX_LIKE";
    case TOK_COUNT:
        return "COUNT";
    case TOK_SUM:
        return "SUM";
    case TOK_AVG:
        return "AVG";
    case TOK_MAX:
        return "MAX";
    case TOK_MIN:
        return "MIN";
    case TOK_ALL:
        return "ALL";
    case TOK_ANY:
        return "ANY";
    case TOK_SAME_TYPE_AS:
        return "SAME_TYPE_AS";
    case TOK_SAME_HEADING_AS:
        return "SAME_HEADING_AS";
    case TOK_IF:
        return "IF";
    case TOK_THEN:
        return "THEN";
    case TOK_ELSE:
        return "ELSE";
    case TOK_CASE:
        return "CASE";
    case TOK_WHEN:
        return "WHEN";
    case TOK_END:
        return "END";
    case TOK_FOR:
        return "FOR";
    case TOK_TO:
        return "TO";
    case TOK_WHILE:
        return "WHILE";
    case TOK_LEAVE:
        return "LEAVE";
    case TOK_TABLE_DEE:
        return "TABLE_DEE";
    case TOK_TABLE_DUM:
        return "TABLE_DUM";
    case TOK_ASSIGN:
        return ":=";
    case TOK_INSERT:
        return "INSERT";
    case TOK_D_INSERT:
        return "D_INSERT";
    case TOK_DELETE:
        return "DELETE";
    case TOK_I_DELETE:
        return "I_DELETE";
    case TOK_UPDATE:
        return "UPDATE";
    case TOK_TYPE:
        return "TYPE";
    case TOK_POSSREP:
        return "POSSREP";
    case TOK_CONSTRAINT:
        return "CONSTRAINT";
    case TOK_OPERATOR:
        return "OPERATOR";
    case TOK_RETURNS:
        return "RETURNS";
    case TOK_UPDATES:
        return "UPDATES";
    case TOK_RETURN:
        return "RETURN";
    case TOK_DEFAULT:
        return "DEFAULT";
    case TOK_LOAD:
        return "LOAD";
    case TOK_ORDER:
        return "ORDER";
    case TOK_ASC:
        return "ASC";
    case TOK_DESC:
        return "DESC";
    case TOK_WITH:
        return "WITH";
    case TOK_RAISE:
        return "RAISE";
    case TOK_TRY:
        return "TRY";
    case TOK_CATCH:
        return "CATCH";
    case TOK_IMPLEMENT:
        return "IMPLEMENT";
    case TOK_PACKAGE:
        return "PACKAGE";
    case TOK_EXPLAIN:
        return "EXPLAIN";
    }
    chtok[0] = (char) tok;
    chtok[1] = '\0';
    return chtok;
}
