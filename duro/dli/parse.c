/*
 * Functions to convert parse subtrees to expressions.
 *
 * Copyright (C) 2003-2009, 2011-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "parse.h"
#include "parsenode.h"
#include "exparse.h"
#include "rel/tostr.h"
#include <rel/transform.h>
#include <rel/rdb.h>

#include <stdio.h>
#include <string.h>
#include <locale.h>

int yyparse(void);

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *txt);
void yy_delete_buffer(YY_BUFFER_STATE);
void yy_switch_to_buffer(YY_BUFFER_STATE);

extern YY_BUFFER_STATE RDB_parse_buffer;
extern int RDB_parse_buffer_valid;

extern int yylineno;

RDB_parse_node *RDB_parse_resultp;
RDB_exec_context *RDB_parse_ecp;

/* Array of tokens in alphabetical order for keyword completion */
int RDB_parse_tokens[] = {
    TOK_AND, TOK_ALL, TOK_ANY, TOK_AVG, TOK_ARRAY, TOK_AS, TOK_ASC,
    TOK_BEGIN, TOK_BUT, TOK_CALL, TOK_CASE, TOK_CATCH, TOK_COUNT, TOK_COMMIT,
    TOK_CONST, TOK_CONSTRAINT, TOK_DEFAULT, TOK_DELETE, TOK_DESC,
    TOK_DIVIDEBY, TOK_DROP, TOK_D_INSERT, TOK_D_UNION,
    TOK_ELSE, TOK_END, TOK_EXPLAIN, TOK_EXTEND, TOK_EXTERN,
    TOK_FOR, TOK_FROM, TOK_GROUP,
    TOK_IF, TOK_IMPLEMENT, TOK_IN, TOK_INDEX, TOK_INIT, TOK_INSERT,
    TOK_INTERSECT, TOK_IS, TOK_I_DELETE, TOK_JOIN, TOK_KEY, TOK_LEAVE,
    TOK_LIKE, TOK_LIMIT, TOK_LOAD, TOK_MAX, TOK_MATCHING,
    TOK_MIN, TOK_MINUS, TOK_NOT, TOK_OPERATOR, TOK_OR,
    TOK_ORDER, TOK_ORDERED, TOK_PACKAGE, TOK_PER,
    TOK_POSSREP, TOK_PRIVATE, TOK_RAISE, TOK_REAL, TOK_REGEX_LIKE,
    TOK_RELATION, TOK_RENAME, TOK_RETURN, TOK_RETURNS,
    TOK_ROLLBACK, TOK_SAME_HEADING_AS, TOK_SAME_TYPE_AS,
    TOK_SEMIJOIN, TOK_SEMIMINUS, TOK_SUBSET_OF, TOK_SUM,
    TOK_SUMMARIZE, TOK_TABLE_DEE, TOK_TABLE_DUM, TOK_THEN, TOK_TO,
    TOK_TX /* TRANSACTION */, TOK_TRY, TOK_TUPLE, TOK_TYPE, TOK_UNION,
    TOK_UNGROUP, TOK_UNWRAP, TOK_UPDATE, TOK_UPDATES, TOK_VAR, TOK_VERSION,
    TOK_VIRTUAL, TOK_WHEN, TOK_WHERE, TOK_WHILE, TOK_WITH, TOK_WRAP, TOK_XOR, -1
};

void
RDB_parse_start_exp(void);

void
RDB_parse_start_stmt(void);

void
yyerror(char *errtxt)
{
    if (RDB_get_err(RDB_parse_ecp) == NULL) {
        char *bufp = RDB_alloc(strlen(errtxt) + 32, RDB_parse_ecp);
        if (bufp == NULL) {
            return;
        }
        sprintf(bufp, "%s", errtxt);
        RDB_raise_syntax(bufp, RDB_parse_ecp);
        RDB_free(bufp);
    }
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
               
            /* Skip comma */
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
        opname = "sum";
        break;
    case TOK_AVG:
        opname = "avg";
        break;
    case TOK_MAX:
        opname = "max";
        break;
    case TOK_MIN:
        opname = "min";
        break;
    case TOK_AND:
    case TOK_ALL:
        opname = "all";
        break;
    case TOK_OR:
    case TOK_ANY:
        opname = "any";
        break;
    case TOK_COUNT:
        opname = "count";
        break;
    }
    exp = RDB_ro_op(opname, ecp);
    if (exp == NULL)
        return NULL;

    if (nodep != NULL) {
        if (add_args(exp, nodep, ecp, txp) != RDB_OK)
            return NULL;
    }

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
not_matching_expr(RDB_parse_node *nodep, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *argp;

    nodep->exp = RDB_ro_op("semiminus", ecp);
    if (nodep->exp == NULL)
        return NULL;

    argp = RDB_parse_node_expr(nodep->val.children.firstp, ecp, txp);
    if (argp == NULL)
        return NULL;
    argp = RDB_dup_expr(argp, ecp);
    if (argp == NULL)
        return NULL;
    RDB_add_arg(nodep->exp, argp);
    argp = RDB_parse_node_expr(nodep->val.children.firstp->nextp->nextp->nextp,
            ecp, txp);
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

    nodep->exp = RDB_ro_op("if", ecp);
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

    RDB_expression *rexp = RDB_ro_op("extend", ecp);
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
            argp = RDB_parse_node_expr(nodep->val.children.firstp->nextp->nextp, ecp, txp);
            if (argp == NULL)
                return NULL;
            argp = RDB_dup_expr(argp, ecp);
            if (argp == NULL)
                return NULL;
            RDB_add_arg(rexp, argp);

            argp = RDB_string_to_expr(
                    RDB_expr_var_name(RDB_parse_node_expr(
                            nodep->val.children.firstp, ecp, txp)),
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
    RDB_expression *rexp = RDB_ro_op("summarize", ecp);
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
            RDB_expression *aggrexp = RDB_parse_node_expr(
                    nodep->val.children.firstp->nextp->nextp, ecp, txp);
            if (aggrexp == NULL)
                return NULL;
            aggrexp = RDB_dup_expr(aggrexp, ecp);
            if (aggrexp == NULL)
                return NULL;

            RDB_add_arg(rexp, aggrexp);

            argp = RDB_string_to_expr(
                    RDB_expr_var_name(RDB_parse_node_expr(nodep->val.children.firstp, ecp, txp)), ecp);
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
    RDB_expression *rexp = RDB_ro_op("update", ecp);
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

    RDB_expression *rexp = RDB_ro_op("rename", ecp);

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
    RDB_expression *rexp = RDB_ro_op("wrap", ecp);
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
            argp = RDB_obj_to_expr(NULL, RDB_parse_ecp);
            if (argp == NULL)
                return NULL;
            arrp = RDB_expr_obj(argp);
            if (RDB_set_array_length(arrp,
                    (RDB_parse_nodelist_length(wnodep->val.children.firstp->nextp) + 1) / 2,
                    RDB_parse_ecp) != RDB_OK)
                return NULL;
            arrtyp = RDB_new_array_type(&RDB_STRING, RDB_parse_ecp);
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
                    if (RDB_string_to_obj(RDB_array_get(arrp, i++, RDB_parse_ecp),
                            attrname, RDB_parse_ecp) != RDB_OK)
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
    RDB_expression *rexp = RDB_ro_op("unwrap", ecp);

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
    RDB_expression *rexp = RDB_ro_op("group", ecp);

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

static RDB_expression *
with_node_expr(RDB_parse_node *nodep, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp;
	RDB_parse_node *inodep, *jnodep;
	int ret;

	/* Convert node to expression */
	exp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp, txp);
	if (exp == NULL) {
	    return NULL;
	}

	exp = RDB_dup_expr(exp, ecp);
	if (exp == NULL)
		return NULL;

	/* Resolve */
	inodep = nodep->nextp->val.children.firstp;
	if (inodep != NULL) {
	    for(;;) {
            const char *varname = RDB_expr_var_name(inodep->val.children.firstp->exp);
            RDB_expression *dstexp = RDB_parse_node_expr(
                    inodep->val.children.firstp->nextp->nextp, ecp, txp);
            if (dstexp == NULL)
                return NULL;

            /* Replace name by expression in the following name intros */
            if (inodep->nextp != NULL) {
                jnodep = inodep->nextp->nextp;
                for(;;) {
                    if (RDB_parse_node_expr(jnodep->val.children.firstp->nextp->nextp,
                            ecp, txp) == NULL) {
                        return NULL;
                    }
                    ret = RDB_expr_resolve_varname_expr(
                            &jnodep->val.children.firstp->nextp->nextp->exp,
                            varname, dstexp, ecp);
                    if (ret != RDB_OK)
                        return NULL;
                    if (jnodep->nextp == NULL)
                        break;
                    jnodep = jnodep->nextp->nextp;
                }
            }

            /* Replace name by expression in exp */
            ret = RDB_expr_resolve_varname_expr(&exp,
                    varname, dstexp, ecp);
            if (ret != RDB_OK)
                return NULL;

            if (inodep->nextp == NULL)
                break;
            inodep = inodep->nextp->nextp;
	    }
	}

	return exp;
}

static RDB_expression *
divide_node_expr(RDB_parse_node *argnodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;
    RDB_expression *rexp = RDB_ro_op("divide", ecp);

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

int
RDB_parse_node_pkgname(RDB_parse_node *nodep, RDB_object *nameobjp, RDB_exec_context *ecp)
{
    const char *name;

    switch (nodep->kind) {
    case RDB_NODE_EXPR:
        name = RDB_expr_var_name(nodep->exp);
        /* Must check because the grammar allows general expressions */
        if (name == NULL) {
            RDB_raise_syntax("identifier expected", ecp);
            return RDB_ERROR;
        }
        return RDB_string_to_obj(nameobjp, name, ecp);
    case RDB_NODE_INNER:
        if (RDB_parse_nodelist_length(nodep) != 3) {
            RDB_raise_syntax("invalid expression", ecp);
            return RDB_ERROR;
        }
        if (nodep->val.children.firstp->nextp->kind != RDB_NODE_TOK
                || nodep->val.children.firstp->nextp->val.token != '.') {
            RDB_raise_syntax("invalid expression", ecp);
            return RDB_ERROR;
        }
        if (nodep->val.children.firstp->nextp->nextp->kind != RDB_NODE_EXPR) {
            RDB_raise_syntax("identifier expected", ecp);
            return RDB_ERROR;
        }
        if (RDB_parse_node_pkgname(nodep->val.children.firstp, nameobjp, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        name = RDB_expr_var_name(nodep->val.children.firstp->nextp->nextp->exp);
        if (name == NULL) {
            RDB_raise_syntax("identifier expected", ecp);
            return RDB_ERROR;
        }
        if (RDB_append_char(nameobjp, '.', ecp) != RDB_OK)
            return RDB_ERROR;
        return RDB_append_string(nameobjp, name, ecp);
    default: ;
    }
    RDB_raise_syntax("invalid expression", ecp);
    return RDB_ERROR;
}

static RDB_expression *
ro_op_node_expr(RDB_parse_node *argnodep, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *rexp;
    const char *opnamep;
    RDB_object qop_nameobj;

    RDB_init_obj(&qop_nameobj);

    if (argnodep->kind == RDB_NODE_INNER) {
        if (RDB_parse_node_pkgname(argnodep->val.children.firstp, &qop_nameobj, ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&qop_nameobj, ".", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&qop_nameobj,
                RDB_expr_var_name(argnodep->val.children.firstp->nextp->nextp->exp),
                ecp) != RDB_OK)
            goto error;
        opnamep = RDB_obj_string(&qop_nameobj);
    } else {
        opnamep = RDB_expr_var_name(argnodep->exp);
    }

    /* Readonly operator */
    if (strncmp(opnamep, RDB_THE_PREFIX, sizeof(RDB_THE_PREFIX) - 1) == 0) {
        RDB_expression *argp;

        if (argnodep->nextp->nextp == NULL) {
            RDB_raise_syntax("the_ operator requires argument", ecp);
            goto error;
        }
        argp = RDB_dup_expr(RDB_parse_node_expr(RDB_parse_node_child(
                argnodep->nextp->nextp, 0), ecp, txp), ecp);
        if (argp == NULL)
            goto error;
        rexp = RDB_expr_property(argp, opnamep + sizeof(RDB_THE_PREFIX) - 1,
                ecp);
    } else {
        rexp = RDB_ro_op(opnamep, ecp);
        if (rexp == NULL)
            goto error;
        if (add_args(rexp, argnodep->nextp->nextp, ecp, txp) != RDB_OK) {
            goto error;
        }
    }
    RDB_destroy_obj(&qop_nameobj, ecp);
    return rexp;

error:
    RDB_destroy_obj(&qop_nameobj, ecp);
    return NULL;
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

    if (itemp == NULL) {
    	return RDB_OK;
    }
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

    attrc = (RDB_parse_nodelist_length(nodep) + /* 1 */ 2) / 3;
    if (attrc > 0) {
        RDB_expression *exp;
        RDB_parse_node *np = nodep->val.children.firstp;

        attrv = RDB_alloc(sizeof (RDB_attr) * attrc, ecp);
        if (attrv == NULL)
            return NULL;

        for (i = 0; i < attrc; i++) {
            attrv[i].typ = NULL;
        }

        for (i = 0; i < attrc; i++) {
            if (np->kind == RDB_NODE_TOK) {
                attrv[i].name = NULL;
                break;
            }
            exp = RDB_parse_node_expr(np, ecp, txp);
            if (exp == NULL)
                goto cleanup;
            attrv[i].name = (char *) RDB_expr_var_name(exp);

            np = np->nextp;
            attrv[i].typ = RDB_parse_node_to_type(np, NULL, NULL, ecp, txp);
            if (attrv[i].typ == NULL)
                goto cleanup;
            if (RDB_type_is_generic(attrv[i].typ)) {
                RDB_raise_syntax("generic type not permitted", ecp);
                goto cleanup;
            }
            if (i + 1 < attrc)
                np = np->nextp->nextp;
        }
    }

    if (rel) {
        typ = RDB_new_relation_type(attrc, attrv, ecp);
    } else {
        typ = RDB_new_tuple_type(attrc, attrv, ecp);
    }

cleanup:
    for (i = 0; i < attrc; i++) {
        if (attrv[i].typ != NULL && !RDB_type_is_scalar(attrv[i].typ))
            RDB_del_nonscalar_type(attrv[i].typ, ecp);
    }
    RDB_free(attrv);
    return typ;
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
    case '%':
    case '>':
    case '<':
    case '=':
        opbuf[0] = firstp->nextp->val.token;
        opnamep = opbuf;
        break;
    case TOK_AND:
        opnamep = "and";
        break;
    case TOK_OR:
        opnamep = "or";
        break;
    case TOK_XOR:
        opnamep = "xor";
        break;
    case TOK_CONCAT:
        opnamep = "||";
        break;
    case TOK_LIKE:
        opnamep = "like";
        break;
    case TOK_REGEX_LIKE:
        opnamep = "regex_like";
        break;
    case TOK_IN:
        opnamep = "in";
        break;
    case TOK_SUBSET_OF:
        opnamep = "subset_of";
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
        opnamep = "where";
        break;
    case TOK_UNION:
        opnamep = "union";
        break;
    case TOK_D_UNION:
        opnamep = "d_union";
        break;
    case TOK_INTERSECT:
        opnamep = "intersect";
        break;
    case TOK_MINUS:
        opnamep = "minus";
        break;
    case TOK_SEMIMINUS:
        opnamep = "semiminus";
        break;
    case TOK_SEMIJOIN:
    case TOK_MATCHING:
        opnamep = "semijoin";
        break;
    case TOK_JOIN:
        opnamep = "join";
        break;
    case TOK_UNGROUP:
        nodep->exp = RDB_ro_op("ungroup", ecp);
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
        RDB_del_expr(nodep->exp, ecp);
        nodep->exp = NULL;
    }
    return NULL;
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
                nodep->exp = RDB_ro_op("to_tuple", ecp);
                argp = RDB_parse_node_expr(firstp->nextp->nextp, ecp, txp);
                if (argp == NULL)
                    return NULL;
                argp = RDB_dup_expr(argp, ecp);
                if (argp == NULL)
                    return NULL;
                RDB_add_arg(nodep->exp, argp);
                return nodep->exp;
            }
            nodep->exp = RDB_ro_op("tuple", ecp);
            if (nodep->exp == NULL)
                return NULL;

            if (firstp->nextp->nextp->kind == RDB_NODE_INNER) {
                if (add_tuple_item_list(nodep->exp,
                        firstp->nextp->nextp, ecp, txp)
                        != RDB_OK) {
                    RDB_del_expr(nodep->exp, ecp);
                    return nodep->exp = NULL;
                }
            }
            return nodep->exp;
        case TOK_RELATION:
            nodep->exp = RDB_ro_op("relation", ecp);
            if (nodep->exp == NULL)
                return NULL;

            if (firstp->nextp->nextp->nextp->nextp != NULL) {
                /*
                 * Parse heading
                 */
                RDB_type *reltyp = parse_heading(firstp->nextp->nextp,
                        RDB_TRUE, ecp, txp);
                if (reltyp == NULL) {
                    RDB_del_expr(nodep->exp, ecp);
                    return nodep->exp = NULL;
                }
                RDB_set_expr_type(nodep->exp, reltyp);
                if (add_arg_list(nodep->exp, firstp->nextp->nextp->nextp->nextp->nextp, ecp, txp)
                        != RDB_OK) {
                    RDB_del_expr(nodep->exp, ecp);
                    return nodep->exp = NULL;
                }
                return nodep->exp;
            } else {
                if (add_arg_list(nodep->exp, firstp->nextp->nextp, ecp, txp) != RDB_OK) {
                    RDB_del_expr(nodep->exp, ecp);
                    return nodep->exp = NULL;
                }
            }
            return nodep->exp;
        case TOK_ARRAY:
            nodep->exp = RDB_ro_op("array", ecp);
            if (nodep->exp == NULL)
                return NULL;

            if (firstp->nextp->kind != RDB_NODE_TOK) {
                /* Set expression type */
                RDB_type *arrtyp;
                RDB_type *typ = RDB_parse_node_to_type(firstp->nextp,
                        NULL, NULL, ecp, txp);
                if (typ == NULL) {
                    RDB_del_expr(nodep->exp, ecp);
                    return nodep->exp = NULL;
                }
                if (RDB_type_is_generic(typ)) {
                    RDB_raise_syntax("generic type not permitted", ecp);
                    if (!RDB_type_is_scalar(typ))
                        RDB_del_nonscalar_type(typ, ecp);
                    return NULL;
                }
                arrtyp = RDB_new_array_type(typ, ecp);
                if (arrtyp == NULL) {
                    RDB_del_expr(nodep->exp, ecp);
                    return nodep->exp = NULL;
                }

                RDB_set_expr_type(nodep->exp, arrtyp);

                if (add_arg_list(nodep->exp, firstp->nextp->nextp->nextp, ecp, txp) != RDB_OK)
                    return NULL;
                return nodep->exp;
            }

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
            return unop_expr("not", nodep, firstp->nextp, ecp, txp);
        case TOK_IF:
            return if_expr(nodep, firstp->nextp, ecp, txp);
        case TOK_COUNT:
            nodep->exp = RDB_ro_op("count", ecp);
            if (nodep->exp == NULL)
                return NULL;

            if (firstp->nextp->nextp->nextp != NULL) {
                argp = RDB_parse_node_expr(firstp->nextp->nextp, ecp, txp);
                if (argp == NULL)
                    return NULL;
                argp = RDB_dup_expr(argp, ecp);
                if (argp == NULL)
                    return NULL;
                RDB_add_arg(nodep->exp, argp);
            }
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
            nodep->exp = RDB_ro_op("remove", ecp);
            if (nodep->exp == NULL)
                return NULL;
            id_list_nodep = firstp->nextp->nextp->nextp->nextp->val.children.firstp;
        } else {
            nodep->exp = RDB_ro_op("project", ecp);
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
                && firstp->nextp->kind == RDB_NODE_TOK) {
            if (firstp->nextp->nextp->nextp == NULL)
                return binop_node_expr(nodep, ecp, txp);
            if (firstp->nextp->nextp->nextp != NULL
                    && firstp->nextp->val.token == TOK_NOT
                    && firstp->nextp->nextp->kind == RDB_NODE_TOK
                    && firstp->nextp->nextp->val.token == TOK_MATCHING) {
                return not_matching_expr(nodep, ecp, txp);
            }
        }
    }

    RDB_raise_internal("cannot convert parse tree to expression", ecp);
    return NULL;
}

/**@defgroup parse Parsing functions
 * \#include <dli/parse.h>
 * @{
 */

/**
 * Convert a parse tree to an expression.
 * The expression is managed by the parse node. Calling RDB_parse_del_node()
 * will destroy the expression.
 *
 * @returns the expression, or NULL if the parse tree could not be converted.
 * (But see the warning below)
 *
 * @warning Passing a parse node that is not a syntactically valid expression
 * may lead to undefined behavior.
 */
RDB_expression *
RDB_parse_node_expr(RDB_parse_node *nodep, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (nodep->exp != NULL)
        return nodep->exp;

    if (nodep->kind == RDB_NODE_TOK) {
        RDB_raise_syntax("unexpected token", ecp);
        return NULL;
    }

    if (nodep->kind == RDB_NODE_INNER) {
        return inner_node_expr(nodep, ecp, txp);
    }

    RDB_raise_syntax("no expression", ecp);
    return NULL;
}

/**
 * Parse the <a href="../../expressions.html">expression</a>
specified by <var>txt</var>.

@returns A pointer to the RDB_parse_node representing the expression,
or NULL if the parsing failed.

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
    RDB_object oldlocaleobj;
    char *oldlocale;

    RDB_init_obj(&oldlocaleobj);

    RDB_parse_ecp = ecp;

    /* Store old LC_NUMERIC locale */
    oldlocale = setlocale(LC_NUMERIC, NULL);
    if (oldlocale == NULL) {
        RDB_raise_system("cannot get LC_NUMERIC locale", ecp);
        goto error;
    }
    if (RDB_string_to_obj(&oldlocaleobj, oldlocale, ecp) != RDB_OK)
        goto error;

    /* Set LC_NUMERIC locale to C so atof() uses '.' as separator */
    if (setlocale(LC_NUMERIC, "C") == NULL) {
        RDB_raise_system("cannot set LC_NUMERIC locale", ecp);
        goto error;
    }

    buf = yy_scan_string(txt);
    RDB_parse_start_exp();
    pret = yyparse();
    yy_delete_buffer(buf);
    if (pret != 0) {
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("parser error", ecp);
        }
        goto error;
    }

    if (setlocale(LC_NUMERIC, RDB_obj_string(&oldlocaleobj)) == NULL) {
        RDB_raise_system("cannot restore LC_NUMERIC locale", ecp);
        goto error;
    }

    RDB_destroy_obj(&oldlocaleobj, ecp);
    return RDB_parse_resultp;

error:
    RDB_destroy_obj(&oldlocaleobj, ecp);
    return NULL;
}

/*@}*/

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
        typ = RDB_expr_type(exp, getfnp, getarg, NULL, ecp, txp);
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
            return RDB_new_relation_type_from_base(typ, ecp);
        }
        if (RDB_type_is_relation(typ))
            return RDB_dup_nonscalar_type(typ, ecp);
        RDB_raise_type_mismatch("tuple or relation type required", ecp);
        return NULL;
    }

    return parse_heading(nodep->nextp->nextp,
            (RDB_bool) (nodep->val.token == TOK_RELATION), ecp, txp);
}

int
RDB_parse_node_qid(RDB_parse_node *parentp, RDB_object *idobjp, RDB_exec_context *ecp)
{
    RDB_parse_node *nodep = parentp->val.children.firstp;
    if (RDB_string_to_obj(idobjp, RDB_expr_var_name(nodep->exp), ecp) != RDB_OK)
        return RDB_ERROR;
    while (nodep->nextp != NULL) {
        if (RDB_append_string(idobjp, ".", ecp) != RDB_OK)
            return RDB_ERROR;
        nodep = nodep->nextp->nextp;
        if (RDB_append_string(idobjp, RDB_expr_var_name(nodep->exp), ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

RDB_type *
RDB_parse_node_to_type(RDB_parse_node *nodep, RDB_gettypefn *getfnp, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (nodep->val.children.firstp->kind == RDB_NODE_TOK) {
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
            if (RDB_type_is_generic(typ)) {
                RDB_raise_syntax("generic type not permitted", ecp);
                if (!RDB_type_is_scalar(typ))
                    RDB_del_nonscalar_type(typ, ecp);
                return NULL;
            }
            return RDB_new_array_type(typ, ecp);
        }
        case TOK_SAME_TYPE_AS:
        {
            RDB_type *typ;
            RDB_expression *exp = RDB_parse_node_expr(
                    nodep->val.children.firstp->nextp->nextp, ecp, txp);
            if (exp == NULL)
                return NULL;
            typ = RDB_expr_type(exp, getfnp, getarg, NULL, ecp, txp);
            if (typ == NULL)
                return NULL;
            return RDB_dup_nonscalar_type(typ, ecp);
        }
        }
    } else {
        RDB_object idobj;
        const char *name;
        RDB_type *typ;

        RDB_init_obj(&idobj);
        if (RDB_parse_node_qid(nodep, &idobj, ecp) != RDB_OK) {
            RDB_destroy_obj(&idobj, ecp);
            return NULL;
        }

        name = RDB_obj_string(&idobj);
        if (strcmp(name, "char") == 0) {
            typ = &RDB_STRING;
        } else if (strcmp(name, "int") == 0) {
            typ = &RDB_INTEGER;
        } else if (strcmp(name, "rational") == 0) {
            typ = &RDB_FLOAT;
        } else if (strcmp(name, "rat") == 0) {
            typ = &RDB_FLOAT;
        } else if (strcmp(name, "bool") == 0) {
            typ = &RDB_BOOLEAN;
        } else {
            typ = RDB_get_type(name, ecp, txp);
        }
        RDB_destroy_obj(&idobj, ecp);
        return typ;
    }

    RDB_raise_not_supported("unsupported type", ecp);
    return NULL;
}

RDB_parse_node *
RDB_parse_stmt(RDB_exec_context *ecp)
{
    int pret;
    RDB_object oldlocaleobj;
    char *oldlocale;

    RDB_init_obj(&oldlocaleobj);

    RDB_parse_ecp = ecp;

    /* Store old LC_NUMERIC locale */
    oldlocale = setlocale(LC_NUMERIC, NULL);
    if (oldlocale == NULL) {
        RDB_raise_system("cannot get LC_NUMERIC locale", ecp);
        goto error;
    }
    if (RDB_string_to_obj(&oldlocaleobj, oldlocale, ecp) != RDB_OK)
        goto error;

    /* Set LC_NUMERIC locale to C so atof() uses '.' as separator */
    if (setlocale(LC_NUMERIC, "C") == NULL) {
        RDB_raise_system("cannot set LC_NUMERIC locale", ecp);
        goto error;
    }

    RDB_clear_err(RDB_parse_ecp);
    RDB_parse_start_stmt();
    pret = yyparse();
    if (pret != 0) {
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("parser error", ecp);
        }
        goto error;
    }

    if (setlocale(LC_NUMERIC, RDB_obj_string(&oldlocaleobj)) == NULL) {
        RDB_raise_system("cannot restore LC_NUMERIC locale", ecp);
        goto error;
    }

    RDB_destroy_obj(&oldlocaleobj, ecp);

    /* No error occured, so clear any previously raised error */
    RDB_clear_err(ecp);

    return RDB_parse_resultp;

error:
    RDB_destroy_obj(&oldlocaleobj, ecp);
    return NULL;
}

RDB_parse_node *
RDB_parse_stmt_string(const char *txt, RDB_exec_context *ecp)
{
    int pret;
    RDB_object oldlocaleobj;
    char *oldlocale;
    int lineno = yylineno;
    YY_BUFFER_STATE oldbuf;
    int pbuf_was_valid = RDB_parse_buffer_valid;

    /* If the parse buffer is valid, save it */
    if (RDB_parse_buffer_valid) {
        oldbuf = RDB_parse_buffer;
    }

    yylineno = 1;

    RDB_init_obj(&oldlocaleobj);

    RDB_parse_ecp = ecp;

    RDB_parse_buffer = yy_scan_string(txt);
    RDB_parse_buffer_valid = 1;

    /* Store old LC_NUMERIC locale */
    oldlocale = setlocale(LC_NUMERIC, NULL);
    if (oldlocale == NULL) {
        RDB_raise_system("cannot get LC_NUMERIC locale", ecp);
        goto error;
    }
    if (RDB_string_to_obj(&oldlocaleobj, oldlocale, ecp) != RDB_OK)
        goto error;

    /* Set LC_NUMERIC locale to C so atof() uses '.' as separator */
    if (setlocale(LC_NUMERIC, "C") == NULL) {
        RDB_raise_system("cannot set LC_NUMERIC locale", ecp);
        goto error;
    }

    RDB_parse_start_stmt();
    pret = yyparse();
    yy_delete_buffer(RDB_parse_buffer);

    /* If the parse buffer was valid, restore it */
    if (pbuf_was_valid) {
        yy_switch_to_buffer(oldbuf);
        RDB_parse_buffer = oldbuf;
    }
    RDB_parse_buffer_valid = pbuf_was_valid;

    if (pret != 0) {
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("parser error", ecp);
        }
        goto error;
    }

    if (setlocale(LC_NUMERIC, RDB_obj_string(&oldlocaleobj)) == NULL) {
        RDB_raise_system("cannot restore LC_NUMERIC locale", ecp);
        goto error;
    }

    RDB_destroy_obj(&oldlocaleobj, ecp);
    yylineno = lineno;
    return RDB_parse_resultp;

error:
    RDB_destroy_obj(&oldlocaleobj, ecp);
    yylineno = lineno;
    return NULL;
}
