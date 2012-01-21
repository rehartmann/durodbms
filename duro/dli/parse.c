/*
 * $Id$
 *
 * Copyright (C) 2003-2011 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "parse.h"
#include "exparse.h"
#include "rel/tostr.h"
#include <rel/transform.h>
#include <rel/rdb.h>
#include <rel/internal.h>

#include <stdio.h>
#include <string.h>
#include <assert.h>

int yyparse(void);

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *txt);
void yy_delete_buffer(YY_BUFFER_STATE);
void yy_switch_to_buffer(YY_BUFFER_STATE);

extern YY_BUFFER_STATE _RDB_parse_buffer;

RDB_parse_node *_RDB_parse_resultp;
RDB_exec_context *_RDB_parse_ecp;
int _RDB_parse_interactive = 0;
int _RDB_parse_case_insensitive = 1;

void
_RDB_parse_start_exp(void);

void
_RDB_parse_start_stmt(void);

const char *
_RDB_token_name(int tok);

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

static RDB_expression *
binop_node_expr(RDB_parse_node *nodep, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_parse_node *firstp = nodep->val.children.firstp;
    char opbuf[] = " ";
    char *opnamep;

    /* Binary operator */
    switch (firstp->nextp->val.token) {
        case '+':
        case '-':
        case '*':
        case '/':
        case '>':
        case '<':
        case '=':
            opbuf[0] = firstp->nextp->val.token;
            opnamep = opbuf;
            break;
        case TOK_AND:
            opnamep = "AND";
            break;
        case TOK_OR:
            opnamep = "OR";
            break;
        case TOK_XOR:
            opnamep = "XOR";
            break;
        case TOK_CONCAT:
            opnamep = "||";
            break;
        case TOK_MATCHES:
            opnamep = "MATCHES";
            break;
        case TOK_IN:
            opnamep = "IN";
            break;
        case TOK_SUBSET_OF:
            opnamep = "SUBSET_OF";
            break;
        case TOK_NE:
            opnamep = "<>";
            break;
        case TOK_LE:
            opnamep = "<=";
            break;
        case TOK_GE:
            opnamep = ">=";
            break;
        case TOK_WHERE:
            opnamep = "WHERE";
            break;
        case TOK_UNION:
            opnamep = "UNION";
            break;
        case TOK_INTERSECT:
            opnamep = "INTERSECT";
            break;
        case TOK_MINUS:
            opnamep = "MINUS";
            break;
        case TOK_SEMIMINUS:
            opnamep = "SEMIMINUS";
            break;
        case TOK_SEMIJOIN:
            opnamep = "SEMIJOIN";
            break;
        case TOK_JOIN:
            opnamep = "JOIN";
            break;
        case TOK_UNGROUP:
            nodep->exp = RDB_ro_op("UNGROUP", ecp);
            if (nodep->exp == NULL)
                goto error;
            argp = RDB_parse_node_expr(firstp, ecp, txp);
            if (argp == NULL)
                goto error;
            argp = RDB_dup_expr(argp, ecp);
            if (argp == NULL)
                goto error;
            RDB_add_arg(nodep->exp, argp);

            argp = RDB_string_to_expr(
                    RDB_expr_var_name(RDB_parse_node_expr(
                            firstp->nextp->nextp, ecp, txp)),
                    ecp);
            RDB_add_arg(nodep->exp, argp);
            return nodep->exp;
        case TOK_FROM:
            argp = RDB_parse_node_expr(firstp->nextp->nextp, ecp, txp);
            if (argp == NULL)
                goto error;
            argp = RDB_dup_expr(argp, ecp);
            if (argp == NULL)
                goto error;
            nodep->exp = RDB_tuple_attr(argp,
                    RDB_expr_var_name(RDB_parse_node_expr(firstp, ecp, txp)), ecp);
            return nodep->exp;
        case '.':
            argp = RDB_parse_node_expr(firstp, ecp, txp);
            if (argp == NULL)
                goto error;
            argp = RDB_dup_expr(argp, ecp);
            if (argp == NULL)
                goto error;
            nodep->exp = RDB_tuple_attr(argp,
                    RDB_expr_var_name(RDB_parse_node_expr(
                            firstp->nextp->nextp, ecp, txp)), ecp);
            return nodep->exp;
        default:
            RDB_raise_internal("invalid binary operator", ecp);
            return NULL;
    }
    nodep->exp = RDB_ro_op(opnamep, ecp);
    if (nodep->exp == NULL)
        goto error;
    argp = RDB_parse_node_expr(firstp, ecp, txp);
    if (argp == NULL)
        goto error;
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(nodep->exp, argp);

    argp = RDB_parse_node_expr(firstp->nextp->nextp, ecp, txp);
    if (argp == NULL)
        goto error;
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(nodep->exp, argp);
    return nodep->exp;

error:
    if (nodep->exp != NULL) {
        RDB_drop_expr(nodep->exp, ecp);
        nodep->exp = NULL;
    }
    return NULL;
}

static int
add_args(RDB_expression *exp, RDB_parse_node *pnodep, RDB_exec_context *ecp,
        RDB_transaction *txp) {
    RDB_parse_node *nodep = pnodep->val.children.firstp;
    RDB_expression *chexp;

    if (nodep != NULL) {
        for(;;) {
            chexp = RDB_parse_node_expr(nodep, ecp, txp);
            if (chexp == NULL)
                return RDB_ERROR;
            chexp = RDB_dup_expr(chexp, ecp);
            if (chexp == NULL)
                return RDB_ERROR;        
            RDB_add_arg(exp, chexp);
            
            if (nodep->nextp == NULL)
               break;
               
            // Skip comma
            nodep = nodep->nextp->nextp;
        }
    }
    return RDB_OK;
}

static RDB_expression *
aggr_node_expr(int token, RDB_parse_node *nodep, RDB_exec_context *ecp,
        RDB_transaction *txp) {
    RDB_expression *exp;
    const char *opname;
    
    switch (token) {
        case TOK_SUM:
            opname = "SUM";
            break;
        case TOK_AVG:
            opname = "AVG";
            break;
        case TOK_MAX:
            opname = "MAX";
            break;
        case TOK_MIN:
            opname = "MIN";
            break;
        case TOK_AND:
        case TOK_ALL:
            opname = "ALL";
            break;
        case TOK_OR:
        case TOK_ANY:
            opname = "ANY";
            break;
    }
    exp = RDB_ro_op(opname, ecp);
    if (exp == NULL)
        return NULL;

    if (add_args(exp, nodep, ecp, txp) != RDB_OK)
        return NULL;    
    
    return exp;
}

static RDB_expression *
unop_expr(const char *opname, RDB_parse_node *nodep,
        RDB_parse_node *argnodep, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;

    nodep->exp = RDB_ro_op(opname, ecp);
    if (nodep->exp == NULL)
        return NULL;

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(nodep->exp, argp);
    return nodep->exp;
}

static RDB_expression *
if_expr(RDB_parse_node *nodep,
        RDB_parse_node *argnodep, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;

    nodep->exp = RDB_ro_op("IF", ecp);
    if (nodep->exp == NULL)
        return NULL;

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(nodep->exp, argp);

    argp = RDB_parse_node_expr(argnodep->nextp->nextp, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(nodep->exp, argp);

    argp = RDB_parse_node_expr(argnodep->nextp->nextp->nextp->nextp, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(nodep->exp, argp);

    return nodep->exp;
}

static RDB_expression *
extend_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_parse_node *nodep;

    RDB_expression *rexp = RDB_ro_op("EXTEND", ecp);
    if (rexp == NULL)
        return NULL;

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    nodep = argnodep->nextp->nextp->nextp->val.children.firstp;
    if (nodep != NULL) {
        for (;;) {
            argp = RDB_parse_node_expr(nodep->val.children.firstp, ecp, txp);
            if (argp == NULL)
                return NULL;
            argp = RDB_dup_expr(argp, ecp);
            if (argp == NULL)
                return NULL;
            RDB_add_arg(rexp, argp);
    
            argp = RDB_string_to_expr(
                    RDB_expr_var_name(RDB_parse_node_expr(
                            nodep->val.children.firstp->nextp->nextp, ecp, txp)),
                    ecp);
            RDB_add_arg(rexp, argp);
                
            nodep = nodep->nextp;
            if (nodep == NULL)
                break;
            
            /* Skip comma */
            nodep = nodep->nextp;            
        }
    }              

    return rexp;
}

static RDB_expression *
summarize_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_parse_node *nodep;
    RDB_expression *rexp = RDB_ro_op("SUMMARIZE", ecp);
    if (rexp == NULL)
        return NULL;

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    argp = RDB_parse_node_expr(argnodep->nextp->nextp, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    nodep = argnodep->nextp->nextp->nextp->nextp->nextp->val.children.firstp;
    if (nodep != NULL) {
        for (;;) {
            int aggrtok = nodep->val.children.firstp->val.token;
            if (aggrtok == TOK_AND)
                aggrtok = TOK_ALL;
            if (aggrtok == TOK_OR)
                aggrtok = TOK_ANY;
            RDB_expression *aggrexp = RDB_ro_op(_RDB_token_name(aggrtok), ecp);
            if (aggrexp == NULL)
                return NULL;

            if (aggrtok != TOK_COUNT) {
                argp = RDB_dup_expr(RDB_parse_node_expr(
                        RDB_parse_node_child(nodep, 2), ecp, txp), ecp);
                if (argp == NULL)
                    return NULL;
                RDB_add_arg(aggrexp, argp);
            }
            RDB_add_arg(rexp, aggrexp);

            argp = RDB_string_to_expr(
                    RDB_expr_var_name(RDB_parse_node_expr(
                            RDB_parse_node_child(nodep,
                                aggrtok == TOK_COUNT ? 4 : 5), ecp, txp)),
                    ecp);
            if (argp == NULL)
                return NULL;
            RDB_add_arg(rexp, argp);
                
            nodep = nodep->nextp;
            if (nodep == NULL)
                break;
            
            /* Skip comma */
            nodep = nodep->nextp;            
        }
    }              

    return rexp;
}

static RDB_expression *
update_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_parse_node *nodep;
    RDB_expression *rexp = RDB_ro_op("UPDATE", ecp);
    if (rexp == NULL)
        return NULL;

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    nodep = argnodep->nextp->nextp->val.children.firstp;
    if (nodep != NULL) {
        for (;;) {
            argp = RDB_string_to_expr(
                    RDB_expr_var_name(RDB_parse_node_expr(
                            nodep->val.children.firstp, ecp, txp)),
                    ecp);
            RDB_add_arg(rexp, argp);

            argp = RDB_parse_node_expr(nodep->val.children.firstp->nextp->nextp,
                    ecp, txp);
            if (argp == NULL)
                return NULL;
            argp = RDB_dup_expr(argp, ecp);
            if (argp == NULL)
                return NULL;
            RDB_add_arg(rexp, argp);
                    
            nodep = nodep->nextp;
            if (nodep == NULL)
                break;
            
            /* Skip comma */
            nodep = nodep->nextp;            
        }
    }

    return rexp;
}

static RDB_expression *
var_name_to_string_expr(RDB_expression *exp, RDB_exec_context *ecp)
{
    const char *attrname = RDB_expr_var_name(exp);

    if (attrname == NULL) {
        RDB_raise_invalid_argument("missing attribute name", ecp);
        return NULL;
    }
    return RDB_string_to_expr(attrname, ecp);
}

static RDB_expression *
rename_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_parse_node *nodep;

    RDB_expression *rexp = RDB_ro_op("RENAME", ecp);

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    nodep = argnodep->nextp->nextp->nextp->val.children.firstp;
    if (nodep != NULL) {
        for (;;) {
            argp = var_name_to_string_expr(RDB_parse_node_expr(
                            nodep->val.children.firstp, ecp, txp), ecp);
            RDB_add_arg(rexp, argp);
 
            argp = var_name_to_string_expr(RDB_parse_node_expr(
                            nodep->val.children.firstp->nextp->nextp, ecp, txp), ecp);
            RDB_add_arg(rexp, argp);
                
            nodep = nodep->nextp;
            if (nodep == NULL)
                break;
            
            /* Skip comma */
            nodep = nodep->nextp;            
        }
    }              

    return rexp;
}

static int
add_id_list(RDB_expression *exp, RDB_parse_node *nodep, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *argp;

    for(;;) {
        argp = RDB_parse_node_expr(nodep, ecp, txp);
        if (argp == NULL)
            return RDB_ERROR;
        argp = var_name_to_string_expr(argp, ecp);
        if (argp == NULL)
            return RDB_ERROR;
        RDB_add_arg(exp, argp);
        
        nodep = nodep->nextp;
        if (nodep == NULL)
            break;
        nodep = nodep->nextp;
    }
    return RDB_OK;
}

static RDB_expression *
wrap_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_expression *rexp = RDB_ro_op("WRAP", ecp);
    RDB_parse_node *wnodep, *nodep;

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    wnodep = argnodep->nextp->nextp->nextp->val.children.firstp;
    if (wnodep != NULL) {
        for (;;) {
            RDB_expression *attrexp;
            RDB_type *arrtyp;
            RDB_object *arrp;
            const char *attrname;
            int i;

            /* Create array arg */
            argp = RDB_obj_to_expr(NULL, _RDB_parse_ecp);
            if (argp == NULL)
                return NULL;
            arrp = RDB_expr_obj(argp);
            if (RDB_set_array_length(arrp,
                    (RDB_parse_nodelist_length(wnodep->val.children.firstp->nextp) + 1) / 2,
                    _RDB_parse_ecp) != RDB_OK)
                return NULL;
            arrtyp = RDB_create_array_type(&RDB_STRING, _RDB_parse_ecp);
            if (arrtyp == NULL)
                return NULL;
            RDB_obj_set_typeinfo(arrp, arrtyp);

            i = 0;
            nodep = wnodep->val.children.firstp->nextp->val.children.firstp;
            if (nodep != NULL) {
                for (;;) {                    
                    attrexp = RDB_parse_node_expr(nodep, ecp, txp);
                    if (attrexp == NULL)
                        return NULL;

                    attrname = RDB_expr_var_name(attrexp);
                    if (attrname == NULL) {
                        RDB_raise_invalid_argument("missing attribute name", ecp);
                        return NULL;
                    }
                    if (RDB_string_to_obj(RDB_array_get(arrp, i++, _RDB_parse_ecp),
                            attrname, _RDB_parse_ecp) != RDB_OK)
                        return NULL;
                    if (nodep->nextp == NULL)
                        break;
                    nodep = nodep->nextp->nextp;
                }
            }
            RDB_add_arg(rexp, argp);

            argp = RDB_parse_node_expr(
                    wnodep->val.children.firstp->nextp->nextp->nextp->nextp, ecp, txp);
            if (argp == NULL)
                return NULL;
            argp = var_name_to_string_expr(argp, ecp);
            if (argp == NULL)
                return NULL;
            RDB_add_arg(rexp, argp);

            if (wnodep->nextp == NULL)
                break;
            wnodep = wnodep->nextp->nextp;
        }
    }
    return rexp;
}                  

static RDB_expression *
unwrap_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_expression *rexp = RDB_ro_op("UNWRAP", ecp);

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    if (argnodep->nextp->nextp->nextp->val.children.firstp != NULL) {
        if (add_id_list(rexp,
                argnodep->nextp->nextp->nextp->val.children.firstp, ecp, txp) != RDB_OK)
            return NULL;
    }

    return rexp;
}

static RDB_expression *
group_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_expression *rexp = RDB_ro_op("GROUP", ecp);

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    if (argnodep->nextp->nextp->nextp->val.children.firstp != NULL) {
        if (add_id_list(rexp,
                argnodep->nextp->nextp->nextp->val.children.firstp, ecp, txp) != RDB_OK)
            return NULL;
    }

    argp = RDB_string_to_expr(
            RDB_expr_var_name(RDB_parse_node_expr(
                    argnodep->nextp->nextp->nextp->nextp->nextp->nextp, ecp, txp)),
            ecp);
    RDB_add_arg(rexp, argp);
    return rexp;
}

static int
resolve_exprname(RDB_expression **expp, RDB_parse_node *nameintrop,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;

    switch ((*expp)->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return resolve_exprname(&(*expp)->var.op.args.firstp,
                    nameintrop, ecp, txp);
        case RDB_EX_RO_OP:
            argp = (*expp)->var.op.args.firstp;
            (*expp)->var.op.args.firstp = NULL;
            while (argp != NULL) {
                RDB_expression *nextp = argp->nextp;
                if (resolve_exprname(&argp, nameintrop, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
                RDB_add_arg(*expp, argp);
                argp = nextp;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
			if (strcmp(RDB_expr_var_name(
			        nameintrop->val.children.firstp->nextp->nextp->exp),
			        RDB_expr_var_name(*expp)) == 0) {
                RDB_expression *exp = RDB_parse_node_expr(
                		nameintrop->val.children.firstp, ecp, txp);
                if (exp == NULL)
                    return RDB_ERROR;
                exp = RDB_dup_expr(exp, ecp);
                if (exp == NULL) {
                    return RDB_ERROR;
                }

                exp->nextp = (*expp)->nextp;
                RDB_drop_expr(*expp, ecp);
                *expp = exp;
            }
            return RDB_OK;
    }
    abort();
}

static int
resolve_exprnames(RDB_expression **expp, RDB_parse_node *nameintrop,
        RDB_exec_context *ecp, RDB_transaction *txp) {
	if (nameintrop->nextp != NULL) {
	    /* Skip comma, process rest of list */
		if (resolve_exprnames(expp, nameintrop->nextp->nextp, ecp, txp) != RDB_OK)
			return RDB_ERROR;
	}
	return resolve_exprname(expp, nameintrop, ecp, txp);
}

static RDB_expression *
with_node_expr(RDB_parse_node *nodep, RDB_exec_context *ecp, RDB_transaction *txp)
{
	RDB_expression *exp;

	/* Convert node to expression */
	exp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txp);
	if (exp == NULL)
		return NULL;

	exp = RDB_dup_expr(exp, ecp);
	if (exp == NULL)
		return NULL;

	/* Resolve */
	if (nodep->val.children.firstp != NULL) {
        if (resolve_exprnames(&exp, nodep->val.children.firstp, ecp, txp) != RDB_OK) {
            return NULL;
        }
	}

	return exp;
}

static RDB_expression *
divide_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_expression *rexp = RDB_ro_op("DIVIDE", ecp);

    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    argp = RDB_parse_node_expr(argnodep->nextp->nextp, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    argp = RDB_parse_node_expr(argnodep->nextp->nextp->nextp->nextp, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    return rexp;
}

static RDB_expression *
ro_op_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *rexp;
    const char *opnamep = RDB_expr_var_name(argnodep->exp);
    
    // Readonly operator
    if (strncmp(opnamep, "THE_", 4) == 0) {
        RDB_expression *argp;

        if (argnodep->nextp->nextp == NULL) {
            RDB_raise_syntax("THE_ operator requires argument", ecp);
            return NULL;
        }
        argp = RDB_dup_expr(RDB_parse_node_expr(RDB_parse_node_child(
                argnodep->nextp->nextp, 0), ecp, txp), ecp);
        if (argp == NULL)
            return NULL;
        rexp = RDB_expr_comp(argp, opnamep + 4, ecp);
    } else {
        rexp = RDB_ro_op(opnamep, ecp);
        if (rexp == NULL)
            return NULL;
        if (add_args(rexp, argnodep->nextp->nextp, ecp, txp) != RDB_OK) {
            return NULL;
        }
    }
    return rexp;
}

static RDB_expression *
subscript_node_expr(RDB_parse_node *argnodep, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *rexp;
    RDB_expression *argp;

    rexp = RDB_ro_op("[]", ecp);
    if (rexp == NULL)
        return NULL;
    argp = RDB_parse_node_expr(argnodep, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);

    argp = RDB_parse_node_expr(argnodep->nextp->nextp, ecp, txp);
    if (argp == NULL)
        return NULL;        
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(rexp, argp);
    
    return rexp;
}

static int
add_arg_list(RDB_expression *exp, RDB_parse_node *nodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
	RDB_expression *argp;
    RDB_parse_node *itemp = nodep->val.children.firstp;

    if (itemp == NULL)
    	return RDB_OK;
	for(;;) {
		argp = RDB_parse_node_expr(itemp, ecp, txp);
		if (argp == NULL)
			return RDB_ERROR;
		argp = RDB_dup_expr(argp, ecp);
		if (argp == NULL)
			return RDB_ERROR;
		RDB_add_arg(exp, argp);

		itemp = itemp->nextp;
		if (itemp == NULL)
			return RDB_OK;
		/* Skip comma */
		itemp = itemp->nextp;
	}
	return RDB_OK;
}

/**
 * Parse heading and convert it to a tuple or relation type,
 * depending on <var>rel</var>.
 * *<var>nodep</var> must be a list of name/type pair nodes
 */
static RDB_type *
parse_heading(RDB_parse_node *nodep, RDB_bool rel, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int attrc;
    RDB_attr *attrv = NULL;
    RDB_type *typ = NULL;

    if (nodep->kind == RDB_NODE_INNER) {
        attrc = (RDB_parse_nodelist_length(nodep) + 1) / 3;
    } else {
        attrc = 0;
    }
    if (attrc > 0) {
        RDB_expression *exp;

        attrv = RDB_alloc(sizeof (RDB_attr) * attrc, ecp);
        if (attrv == NULL)
            return NULL;

        RDB_parse_node *np = nodep->val.children.firstp;
        for (i = 0; i < attrc; i++) {
            exp = RDB_parse_node_expr(np, ecp, txp);
            if (exp == NULL)
                goto cleanup;
            attrv[i].name = (char *) RDB_expr_var_name(exp);

            np = np->nextp;
            attrv[i].typ = RDB_parse_node_to_type(np, NULL, NULL, ecp, txp);
            if (attrv[i].typ == NULL)
                goto cleanup;
            if ((i + 1) < attrc)
                np = np->nextp->nextp;
        }
    }

    if (rel) {
        typ = RDB_create_relation_type(attrc, attrv, ecp);
    } else {
        typ = RDB_create_tuple_type(attrc, attrv, ecp);
    }

cleanup:
    for (i = 0; i < attrc; i++) {
        if (attrv[i].typ != NULL && !RDB_type_is_scalar(attrv[i].typ))
            RDB_drop_type(attrv[i].typ, ecp, NULL);
    }
    RDB_free(attrv);
    return typ;
}

static RDB_type *
tup_rel_node_to_type(RDB_parse_node *nodep, RDB_gettypefn *getfnp, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp;

    if (nodep->nextp->kind == RDB_NODE_TOK
            && nodep->nextp->val.token == TOK_SAME_HEADING_AS) {
        RDB_type *typ;

        exp = RDB_parse_node_expr(nodep->nextp->nextp->nextp, ecp, txp);
        if (exp == NULL)
            return NULL;
        typ = RDB_expr_type(exp, getfnp, getarg, ecp, txp);
        if (typ == NULL)
            return NULL;
        if (nodep->val.token == TOK_TUPLE) {
            if (RDB_type_is_tuple(typ))
                return RDB_dup_nonscalar_type(typ, ecp);
            if (RDB_type_is_relation(typ))
                return RDB_dup_nonscalar_type(RDB_base_type(typ), ecp);
            RDB_raise_type_mismatch("tuple or relation type required", ecp);
            return NULL;
        }
        if (RDB_type_is_tuple(typ)) {
            typ = RDB_dup_nonscalar_type(typ, ecp);
            if (typ == NULL)
                return NULL;
            return RDB_create_relation_type_from_base(typ, ecp);
        }
        if (RDB_type_is_relation(typ))
            return RDB_dup_nonscalar_type(typ, ecp);
        RDB_raise_type_mismatch("tuple or relation type required", ecp);
        return NULL;
    }

    return parse_heading(nodep->nextp->nextp,
            (RDB_bool) (nodep->val.token == TOK_RELATION), ecp, txp);
}

RDB_type *
RDB_parse_node_to_type(RDB_parse_node *nodep, RDB_gettypefn *getfnp, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (nodep->kind == RDB_NODE_EXPR) {
        const char *name = RDB_expr_var_name(nodep->exp);
        if (strcmp(name, "CHAR") == 0)
            return &RDB_STRING;
        return RDB_get_type(name, ecp, txp);
    }
    if (nodep->kind == RDB_NODE_INNER
            && nodep->val.children.firstp->kind == RDB_NODE_TOK) {
        switch(nodep->val.children.firstp->val.token) {
            case TOK_TUPLE:
            case TOK_RELATION:
                return tup_rel_node_to_type(nodep->val.children.firstp,
                        getfnp, getarg, ecp, txp);
            case TOK_ARRAY:
            {
                RDB_type *typ = RDB_parse_node_to_type(
                        nodep->val.children.firstp->nextp, getfnp, getarg, ecp, txp);
                if (typ == NULL)
                    return NULL;
                return RDB_create_array_type(typ, ecp);
            }
            case TOK_SAME_TYPE_AS:
            {
                RDB_type *typ;
                RDB_expression *exp = RDB_parse_node_expr(
                        nodep->val.children.firstp->nextp->nextp, ecp, txp);
                if (exp == NULL)
                    return NULL;
                typ = RDB_expr_type(exp, getfnp, getarg, ecp, txp);
                if (typ == NULL)
                    return NULL;
                return RDB_dup_nonscalar_type(typ, ecp);
            }
        }
    }

    RDB_raise_not_supported("unsupported type", ecp);
    return NULL;
}

static int
add_tuple_item_list(RDB_expression *exp, RDB_parse_node *nodep,
RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_parse_node *itemp = nodep->val.children.firstp;
    if (itemp == NULL)
        return RDB_OK;
    for(;;) {
        RDB_expression *argp = RDB_string_to_expr(RDB_expr_var_name(itemp->exp), ecp);
        if (argp == NULL)
            return RDB_ERROR;
        RDB_add_arg(exp, argp);

        argp = RDB_parse_node_expr(itemp->nextp, ecp, txp);
        if (argp == NULL)
            return RDB_ERROR;
        argp = RDB_dup_expr(argp, ecp);
        if (argp == NULL)
            return RDB_ERROR;
        RDB_add_arg(exp, argp);

        if (itemp->nextp->nextp == NULL)
            break;

        itemp = itemp->nextp->nextp->nextp;
    }
    return RDB_OK;
}


static RDB_expression *
inner_node_expr(RDB_parse_node *nodep, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_parse_node *firstp = nodep->val.children.firstp;
    RDB_parse_node *id_list_nodep;
    RDB_expression *argp;

    if (firstp->kind == RDB_NODE_TOK) {
        switch (firstp->val.token) {
            case TOK_TUPLE:
                if (firstp->nextp->kind == RDB_NODE_TOK
                        && firstp->nextp->val.token == TOK_FROM) {
                    nodep->exp = RDB_ro_op("TO_TUPLE", ecp);
                    argp = RDB_parse_node_expr(firstp->nextp->nextp, ecp, txp);
                    if (argp == NULL)
                        return NULL;
                    argp = RDB_dup_expr(argp, ecp);
                    if (argp == NULL)
                        return NULL;
                    RDB_add_arg(nodep->exp, argp);
                    return nodep->exp;
                }
                nodep->exp = RDB_ro_op("TUPLE", ecp);
                if (nodep->exp == NULL)
                    return NULL;
    
                if (firstp->nextp->nextp->kind == RDB_NODE_INNER) {
                    if (add_tuple_item_list(nodep->exp,
                            firstp->nextp->nextp, ecp, txp)
                                    != RDB_OK) {
                        RDB_drop_expr(nodep->exp, ecp);
                        return nodep->exp = NULL;
                    }
                }
                return nodep->exp;
            case TOK_RELATION:
                nodep->exp = RDB_ro_op("RELATION", ecp);
                if (nodep->exp == NULL)
                    return NULL;

                if (firstp->nextp->nextp->nextp->nextp != NULL) {
                    /*
                     * Parse heading
                     */
                    RDB_type *reltyp = parse_heading(firstp->nextp->nextp,
                            RDB_TRUE, ecp, txp);
                    if (reltyp == NULL) {
                        RDB_drop_expr(nodep->exp, ecp);
                        return nodep->exp = NULL;
                    }
                    RDB_set_expr_type(nodep->exp, reltyp);
                    if (add_arg_list(nodep->exp, firstp->nextp->nextp->nextp->nextp->nextp, ecp, txp)
                            != RDB_OK) {
                        RDB_drop_expr(nodep->exp, ecp);
                        return nodep->exp = NULL;
                    }
                    return nodep->exp;
                } else {
                    if (add_arg_list(nodep->exp, firstp->nextp->nextp, ecp, txp) != RDB_OK) {
                        RDB_drop_expr(nodep->exp, ecp);
                        return nodep->exp = NULL;
                    }
                }
                return nodep->exp;
            case TOK_ARRAY:
                nodep->exp = RDB_ro_op("ARRAY", ecp);
                if (nodep->exp == NULL)
                    return NULL;

                assert(firstp->nextp->nextp->kind == RDB_NODE_INNER);
				if (add_arg_list(nodep->exp, firstp->nextp->nextp, ecp, txp) != RDB_OK)
					return NULL;
                return nodep->exp;
            case TOK_TABLE_DEE:
            case TOK_TABLE_DUM:
                return nodep->exp;
            case '-':
                return unop_expr("-", nodep, firstp->nextp, ecp, txp);
            case '+':
                argp = RDB_parse_node_expr(firstp->nextp, ecp, txp);
                if (argp == NULL)
                    return NULL;        
                nodep->exp = RDB_dup_expr(argp, ecp);
                return nodep->exp;
            case TOK_NOT:
                return unop_expr("NOT", nodep, firstp->nextp, ecp, txp);
            case TOK_IF:
                return if_expr(nodep, firstp->nextp, ecp, txp);
            case TOK_COUNT:
                nodep->exp = RDB_ro_op("COUNT", ecp);
                if (nodep->exp == NULL)
                    return NULL;

                argp = RDB_parse_node_expr(firstp->nextp->nextp, ecp, txp);
                if (argp == NULL)
                    return NULL;        
                argp = RDB_dup_expr(argp, ecp);
                if (argp == NULL)
                    return NULL;
                RDB_add_arg(nodep->exp, argp);
                return nodep->exp;
            case TOK_SUM:
            case TOK_AVG:
            case TOK_MAX:
            case TOK_MIN:
            case TOK_AND:
            case TOK_ALL:
            case TOK_OR:
            case TOK_ANY:
                return nodep->exp = aggr_node_expr(firstp->val.token,
                        firstp->nextp->nextp, ecp, txp);
            case TOK_EXTEND:
                return nodep->exp = extend_node_expr(firstp->nextp, ecp, txp);
            case TOK_SUMMARIZE:
                return nodep->exp = summarize_node_expr(firstp->nextp, ecp, txp);
            case TOK_UPDATE:
                return nodep->exp = update_node_expr(firstp->nextp, ecp, txp);
            case '(':
                argp = RDB_parse_node_expr(firstp->nextp, ecp, txp);
                if (argp == NULL)
                    return NULL;
                return nodep->exp = RDB_dup_expr(argp, ecp);
            case TOK_WITH:
            	return nodep->exp = with_node_expr(firstp->nextp, ecp, txp);
            default:
                RDB_raise_syntax("invalid token", ecp);
                return NULL;
        }
    }

    if (firstp->nextp != NULL && firstp->nextp->kind == RDB_NODE_TOK
            && firstp->nextp->val.token == '{') {
        if (firstp->nextp->nextp != NULL
                && firstp->nextp->nextp->kind == RDB_NODE_TOK
                && firstp->nextp->nextp->val.token == TOK_ALL
                && firstp->nextp->nextp->nextp != NULL
                && firstp->nextp->nextp->nextp->kind == RDB_NODE_TOK
                && firstp->nextp->nextp->nextp->val.token == TOK_BUT) {
            nodep->exp = RDB_ro_op("REMOVE", ecp);
            if (nodep->exp == NULL)
                return NULL;
            id_list_nodep = firstp->nextp->nextp->nextp->nextp->val.children.firstp;
        } else {
            nodep->exp = RDB_ro_op("PROJECT", ecp);
            if (nodep->exp == NULL)
                return NULL;
            id_list_nodep = firstp->nextp->nextp->val.children.firstp;
        }
        argp = RDB_parse_node_expr(firstp, ecp, txp);
        if (argp == NULL)
            return NULL;        
        argp = RDB_dup_expr(argp, ecp);
        if (argp == NULL)
            return NULL;
        RDB_add_arg(nodep->exp, argp);

        if (id_list_nodep != NULL) {
            if (add_id_list(nodep->exp, id_list_nodep, ecp, txp) != RDB_OK)
                return NULL;
        }
        return nodep->exp;
    }

    if (firstp->nextp != NULL) {
        if (firstp->nextp->kind == RDB_NODE_TOK) {
            switch (firstp->nextp->val.token) {
                case '(':
                    return nodep->exp = ro_op_node_expr(firstp, ecp, txp);
                 case TOK_RENAME:
                    return nodep->exp = rename_node_expr(firstp, ecp, txp);
                 case TOK_WRAP:
                    return nodep->exp = wrap_node_expr(firstp, ecp, txp);
                 case TOK_UNWRAP:
                    return nodep->exp = unwrap_node_expr(firstp, ecp, txp);
                 case TOK_GROUP:
                    return nodep->exp = group_node_expr(firstp, ecp, txp);
                 case TOK_DIVIDEBY:
                    return nodep->exp = divide_node_expr(firstp, ecp, txp);
                 case '[':
                    return nodep->exp = subscript_node_expr(firstp, ecp, txp);
            }
        }
        if (firstp->nextp->nextp != NULL
                && firstp->nextp->nextp->nextp == NULL
                && firstp->nextp->kind == RDB_NODE_TOK) {
            return binop_node_expr(nodep, ecp, txp);
        }
    }

    RDB_raise_internal("cannot convert parse tree to expression", ecp);
    return NULL;
}

RDB_expression *
RDB_parse_node_expr(RDB_parse_node *nodep, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (nodep->exp != NULL)
        return nodep->exp;

    assert(nodep->kind != RDB_NODE_TOK);

    if (nodep->kind == RDB_NODE_INNER) {
        if (inner_node_expr(nodep, ecp, txp) == NULL)
            return NULL;
    }

    return nodep->exp;
}

const char *
RDB_parse_node_ID(const RDB_parse_node *nodep)
{
    if (nodep->kind == RDB_NODE_EXPR && nodep->exp->kind == RDB_EX_VAR) {
        return nodep->exp->var.varname;
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
    if (nodep->kind != RDB_NODE_INNER)
        return NULL;
    RDB_parse_node *chnodep = nodep->val.children.firstp;
    while (idx-- > 0 && chnodep != NULL) {
        chnodep = chnodep->nextp;
    }
    return chnodep;
}

int
RDB_parse_del_node(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    if (nodep->kind == RDB_NODE_INNER
            && nodep->val.children.firstp != NULL) {
        if (RDB_parse_del_nodelist(nodep->val.children.firstp, ecp) != RDB_OK)
            return RDB_ERROR;
    }
    if (nodep->exp != NULL)
        return RDB_drop_expr(nodep->exp, ecp);
    if (nodep->whitecommp != NULL) {
        RDB_destroy_obj(nodep->whitecommp, ecp);
        RDB_free(nodep->whitecommp);
    }
    return RDB_OK;
}

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

int
RDB_parse_node_var_name_idx(const RDB_parse_node *nodep, const char *namep)
{
	int idx = 0;

	if (nodep == NULL)
		return -1;
	for(;;) {
		if (strcmp(RDB_expr_var_name(nodep->exp), namep) == 0)
			return idx;
		idx++;
		nodep = nodep->nextp;
		if (nodep == NULL)
			return -1;
		nodep = nodep->nextp;
	}
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
<dt>SYNTAX_ERROR
<dd>A syntax error occurred during parsing.
</dl>

The call may also fail for a @ref system-errors "system error".

@warning The parser is not reentrant.
 */
RDB_parse_node *
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

RDB_parse_node *
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
    return _RDB_parse_resultp;
}

RDB_parse_node *
RDB_parse_stmt_string(const char *txt, RDB_exec_context *ecp)
{
    int pret;
    YY_BUFFER_STATE oldbuf = _RDB_parse_buffer;

    _RDB_parse_ecp = ecp;

    _RDB_parse_buffer = yy_scan_string(txt);

    _RDB_parse_start_stmt();
    pret = yyparse();
    yy_delete_buffer(_RDB_parse_buffer);
    if (_RDB_parse_interactive) {
        _RDB_parse_buffer = yy_scan_string("");
    } else {
        yy_switch_to_buffer(oldbuf);
        _RDB_parse_buffer = oldbuf;
    }

    if (pret != 0) {
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("parser error", ecp);
        }
        return NULL;
    }

    return _RDB_parse_resultp;
}

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
            return RDB_append_string(dstp, _RDB_token_name(nodep->val.token), ecp);
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
            ret = _RDB_expr_to_str(&strobj, nodep->exp, ecp, txp, 0);
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
