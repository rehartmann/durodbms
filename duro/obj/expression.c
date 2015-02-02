/*
 * $Id$
 *
 * Copyright (C) 2004, 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "object.h"
#include "type.h"
#include "objinternal.h"
#include "builtintypes.h"
#include "tuple.h"
#include <gen/strfns.h>

#include <string.h>

/**@defgroup expr Expression functions
 * @{
 */

/**
 * RDB_expr_is_const returns if the expression is a constant expression.

@returns

RDB_TRUE if the expression is a constant expression, RDB_FALSE otherwise.
 */
RDB_bool
RDB_expr_is_const(const RDB_expression *exp)
{
    RDB_expression *argp;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            return RDB_TRUE;
        case RDB_EX_RO_OP:
            argp = exp->def.op.args.firstp;
            while (argp != NULL) {
                if (!RDB_expr_is_const(argp))
                    return RDB_FALSE;
                argp = argp->nextp;
            }
            return RDB_TRUE;
        case RDB_EX_GET_COMP:
            return RDB_expr_is_const(exp->def.op.args.firstp);
        default: ;
    }
    return RDB_FALSE;
}

/**
 * Return the kind of which expresion *exp is.
 */
enum RDB_expr_kind
RDB_expr_kind(const RDB_expression *exp)
{
    return exp->kind;
}

/**
 * Replace all occurrences of variable name varname by a copy of expression *texp.
 * Replacement is performed in-place if possible.
 */
int
RDB_expr_resolve_varname_expr(RDB_expression **expp, const char *varname,
        RDB_expression *texp, RDB_exec_context *ecp)
{
    RDB_expression *argp;

    switch ((*expp)->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return RDB_expr_resolve_varname_expr(&(*expp)->def.op.args.firstp,
                    varname, texp, ecp);
        case RDB_EX_RO_OP:
            argp = (*expp)->def.op.args.firstp;
            (*expp)->def.op.args.firstp = NULL;
            while (argp != NULL) {
                RDB_expression *nextp = argp->nextp;
                if (RDB_expr_resolve_varname_expr(&argp, varname, texp, ecp) != RDB_OK)
                    return RDB_ERROR;
                RDB_add_arg(*expp, argp);
                argp = nextp;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
            if (strcmp(varname, RDB_expr_var_name(*expp)) == 0) {
                RDB_expression *exp = RDB_dup_expr(texp, ecp);
                if (exp == NULL) {
                    return RDB_ERROR;
                }

                exp->nextp = (*expp)->nextp;
                RDB_del_expr(*expp, ecp);
                *expp = exp;
            }
            return RDB_OK;
    }
    /* Never reached */
    abort();
}

/**
 * Return operator name for read-only operator expressions.
 * 
 * @returns The operator name if the expression is a read-only operator
 * invocation, or NULL if it is not.
 */
const char *
RDB_expr_op_name(const RDB_expression *exp)
{
    return exp->kind == RDB_EX_RO_OP || exp->kind == RDB_EX_GET_COMP
            || exp->kind == RDB_EX_TUPLE_ATTR ?
            exp->def.op.name : NULL;
}

/**
 * Return a pointer to the argument list of a read-only operator expression.
 */
RDB_expr_list *
RDB_expr_op_args(RDB_expression *exp)
{
    return exp->kind == RDB_EX_RO_OP || exp->kind == RDB_EX_GET_COMP
            || exp->kind == RDB_EX_TUPLE_ATTR ?
            &exp->def.op.args : NULL;
}

/**
 * Return variable name for expressions referring to variables.
 * 
 * @returns The variable name if the expression is a read-only variable
 * reference, or NULL if it is not.
 */
const char *
RDB_expr_var_name(const RDB_expression *exp)
{
    return exp->kind == RDB_EX_VAR ? exp->def.varname : NULL;
}

static RDB_expression *
new_expr(RDB_exec_context *ecp)
{
    RDB_expression *exp = RDB_alloc(sizeof (RDB_expression), ecp);
    if (exp == NULL) {
        return NULL;
    }
    exp->typ = NULL;
    exp->transformed = RDB_FALSE;
    exp->optimized = RDB_FALSE;
	return exp;
}

/**
 * RDB_bool_to_expr creates a constant expression of type BOOLEAN.

@returns    A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_bool_to_expr(RDB_bool v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->def.obj.cleanup_fp = NULL;
    exp->def.obj.typ = &RDB_BOOLEAN;
    exp->def.obj.kind = RDB_OB_BOOL;
    exp->def.obj.val.bool_val = v;

    return exp;
}

/**
 * RDB_int_to_expr creates a constant expression of type INTEGER.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_int_to_expr(RDB_int v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->def.obj.cleanup_fp = NULL;
    exp->def.obj.typ = &RDB_INTEGER;
    exp->def.obj.kind = RDB_OB_INT;
    exp->def.obj.val.int_val = v;

    return exp;
}

/**
 * RDB_float_to_expr creates a constant expression of type float.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_float_to_expr(RDB_float v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->def.obj.cleanup_fp = NULL;
    exp->def.obj.typ = &RDB_FLOAT;
    exp->def.obj.kind = RDB_OB_FLOAT;
    exp->def.obj.val.float_val = v;

    return exp;
}

/**
 * RDB_string_to_expr creates a constant expression of type string.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_string_to_expr(const char *v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);    
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->def.obj.cleanup_fp = NULL;
    exp->def.obj.typ = &RDB_STRING;
    exp->def.obj.kind = RDB_OB_BIN;
    exp->def.obj.val.bin.datap = RDB_dup_str(v);
    if (exp->def.obj.val.bin.datap == NULL) {
        RDB_free(exp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    exp->def.obj.val.bin.len = strlen(v) + 1;

    return exp;
}

/**
 * RDB_obj_to_expr creates a constant expression from a RDB_object.

Passing an <var>objp</var> of NULL is equivalent to passing
a pointer to a RDB_object which has been newly initialized
using RDB_init_obj().

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_obj_to_expr(const RDB_object *objp, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL)
        return NULL;

    exp->kind = RDB_EX_OBJ;
    RDB_init_obj(&exp->def.obj);
    if (objp != NULL) {        
        if (RDB_copy_obj(&exp->def.obj, objp, ecp) != RDB_OK) {
            RDB_free(exp);
            return NULL;
        }
        if (objp->typ != NULL && exp->def.obj.typ == NULL) {
            exp->def.obj.typ = RDB_dup_nonscalar_type(objp->typ, ecp);
            if (exp->def.obj.typ == NULL)
                return NULL;
        }
    }
    return exp;
}

/**
 * RDB_table_ref creates an expression which refers to a table.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_table_ref(RDB_object *tbp, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->kind = RDB_EX_TBP;
    exp->def.tbref.tbp = tbp;
    exp->def.tbref.indexp = NULL;
    return exp;
}

/**
 * RDB_var_ref creates an expression that refers to a variable.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_var_ref(const char *attrname, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->kind = RDB_EX_VAR;
    exp->def.varname = RDB_dup_str(attrname);
    if (exp->def.varname == NULL) {
        RDB_raise_no_memory(ecp);
        RDB_free(exp);
        return NULL;
    }
    
    return exp;
}

/**
 * RDB_ro_op creates an expression which represents the invocation
of a readonly operator.

Use RDB_add_arg() to add arguments.

@returns

On success, a pointer to the newly created expression is returned.
If the expression could not be created due to insufficient memory,
NULL is returned and an error is left in *<var>ecp</var>.
 */
RDB_expression *
RDB_ro_op(const char *opname, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->kind = RDB_EX_RO_OP;

    exp->def.op.name = RDB_dup_str(opname);
    if (exp->def.op.name == NULL) {
        RDB_free(exp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    RDB_init_expr_list(&exp->def.op.args);

    exp->def.op.optinfo.objc = 0;
    /* exp->def.op.optinfo.objv = NULL; */
    exp->def.op.optinfo.stopexp = NULL;

    return exp;
}

/**
 * RDB_add_arg adds the child expression *<var>argp</var>
to the expression *<var>exp</var>.
*<var>exp</var> must represent a read-only operator invocation.
RDB_ro_op() should be used to create
such an expression.

To obtain a valid expression representing a read-only
operator invocation, RDB_add_arg must be called once
for each argument of the operator.
 */
void
RDB_add_arg(RDB_expression *exp, RDB_expression *argp)
{
    RDB_expr_list_append(&exp->def.op.args, argp);
    exp->transformed = RDB_FALSE;
}

/**
 * RDB_eq creates an expression that represents an "is equal" operator.
If one of the arguments is NULL, NULL is returned.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_eq(RDB_expression *arg1, RDB_expression *arg2, RDB_exec_context *ecp)
{
    RDB_expression *exp = RDB_ro_op("=", ecp);
    if (exp == NULL)
        return NULL;

    RDB_add_arg(exp, arg1);
    RDB_add_arg(exp, arg2);
    return exp;
}

/**
 * RDB_tuple_attr creates an expression that represents a tuple attribute
extraction.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_tuple_attr(RDB_expression *arg, const char *attrname,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;

    exp = RDB_create_unexpr(arg, RDB_EX_TUPLE_ATTR, ecp);
    if (exp == NULL)
        return NULL;

    exp->def.op.name = RDB_dup_str(attrname);
    if (exp->def.op.name == NULL) {
        RDB_raise_no_memory(ecp);
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    return exp;
}

/**
 * Extract qualified id from attribute expression
 */
int
RDB_expr_attr_qid(const RDB_expression *exp, RDB_object *idobjp, RDB_exec_context *ecp)
{
    RDB_expr_list *arglistp = RDB_expr_op_args((RDB_expression *) exp);
    RDB_expression *arg1p = RDB_expr_list_get(arglistp, 0);

    switch (RDB_expr_kind(arg1p)) {
        case RDB_EX_VAR:
            if (RDB_string_to_obj(idobjp, RDB_expr_var_name(arg1p), ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        case RDB_EX_TUPLE_ATTR:
            if (RDB_expr_attr_qid(arg1p, idobjp, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        default:
            RDB_raise_invalid_argument("invalid usage of \'.\'", ecp);
            return RDB_ERROR;
    }

    if (RDB_append_string(idobjp, ".", ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(idobjp, RDB_expr_op_name(exp), ecp);
}

/**
 * RDB_expr_comp creates an expression which evaluates to a
possible representation component.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_expr_comp(RDB_expression *arg, const char *compname,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;

    exp = RDB_create_unexpr(arg, RDB_EX_GET_COMP, ecp);
    if (exp == NULL)
        return NULL;

    exp->def.op.name = RDB_dup_str(compname);
    if (exp->def.op.name == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    return exp;
}

/**
 * Destroy the expression specified to by <var>exp</var>
(including all subexpressions) and frees all resources associated with it.

@returns

RDB_OK on success, RDB_ERROR on failure.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_del_expr(RDB_expression *exp, RDB_exec_context *ecp)
{
    int ret;

    if (RDB_drop_expr_children(exp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    ret = RDB_destroy_expr(exp, ecp);
    RDB_free(exp);
    return ret;
}

static RDB_expression *
dup_ro_op(const RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_expression *argp;
    RDB_expression *argdp;
    RDB_expression *newexp = RDB_ro_op(exp->def.op.name, ecp);
    if (newexp == NULL)
        return NULL;

    /*
     * Duplicate child expressions and append them to new expression
     */
    argp = exp->def.op.args.firstp;
    while (argp != NULL) {
        argdp = RDB_dup_expr(argp, ecp);
        if (argdp == NULL)
            goto error;
        RDB_add_arg(newexp, argdp);
        argp = argp->nextp;
    }

    /*
     * If the expression is e.g. RELATION {}, duplicate the type if available
     * because otherwise it would be impossible to determine the type
     * of the copy.
     */
    if (RDB_copy_expr_typeinfo_if_needed(newexp, exp, ecp) != RDB_OK) {
        goto error;
    }

    if (exp->def.op.optinfo.objc > 0) {
        int i;

        /*
         * Copy the values exp->def.op.optinfo.objpv points to
         * to nexp->def.op.optinfo.objv and initialize
         * nexp->def.op.optinfo.objpv
         */
        newexp->def.op.optinfo.objv = RDB_alloc(exp->def.op.optinfo.objc
                * sizeof(RDB_object), ecp);
        if (newexp->def.op.optinfo.objv == NULL) {
            goto error;
        }
        newexp->def.op.optinfo.objpv = RDB_alloc(exp->def.op.optinfo.objc
                * sizeof(RDB_object *), ecp);
        if (newexp->def.op.optinfo.objpv == NULL) {
            goto error;
        }
        newexp->def.op.optinfo.objc = exp->def.op.optinfo.objc;
        for (i = 0; i < exp->def.op.optinfo.objc; i++) {
            RDB_init_obj(&newexp->def.op.optinfo.objv[i]);
        }
        for (i = 0; i < exp->def.op.optinfo.objc; i++) {
            if (RDB_copy_obj(&newexp->def.op.optinfo.objv[i],
                    exp->def.op.optinfo.objpv[i], ecp) != RDB_OK) {
                goto error;
            }
            newexp->def.op.optinfo.objv[i].store_typ =
                    exp->def.op.optinfo.objpv[i]->store_typ;

            newexp->def.op.optinfo.objpv[i] = &newexp->def.op.optinfo.objv[i];
        }
    }

    return newexp;

error:
    RDB_del_expr(newexp, ecp);
    return NULL;
}

/**
 * Return a copy of an expression, copying child expressions.
 */
RDB_expression *
RDB_dup_expr(const RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_expression *newexp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = RDB_dup_expr(exp->def.op.args.firstp, ecp);
            if (newexp == NULL)
                return NULL;
            return RDB_tuple_attr(newexp, exp->def.op.name, ecp);
        case RDB_EX_GET_COMP:
            newexp = RDB_dup_expr(exp->def.op.args.firstp, ecp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_comp(newexp, exp->def.op.name, ecp);
        case RDB_EX_RO_OP:
            return dup_ro_op(exp, ecp);
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->def.obj, ecp);
        case RDB_EX_TBP:
            newexp = RDB_table_ref(exp->def.tbref.tbp, ecp);
            if (newexp == NULL)
                return NULL;
            newexp->def.tbref.indexp = exp->def.tbref.indexp;
            return newexp;
        case RDB_EX_VAR:
            return RDB_var_ref(exp->def.varname, ecp);
    }
    abort();
}

/**
 * RDB_expr_obj returns a pointer to RDB_object embedded in an expression.

@returns

A pointer to the embedded RDB_object or NULL if the expression does
not represent a RDB_object.
 */
RDB_object *
RDB_expr_obj(RDB_expression *exp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            return &exp->def.obj;
        case RDB_EX_TBP:
            return exp->def.tbref.tbp;
        default: ;
    }
    return NULL;
}

/**
 * Set the type of expression *<var>exp</var> to *<var>typ</var>.
 * Only used for special purposes, such as specifying the type of relation selectors.
 */
void
RDB_set_expr_type(RDB_expression *exp, RDB_type *typ)
{
    exp->typ = typ;
}

/**
 * Return RDB_TRUE if *<var>exp</var> is of type string,
 * otherwise RDB_FALSE.
 */
RDB_bool
RDB_expr_is_string(const RDB_expression *exp)
{
    return exp->kind == RDB_EX_OBJ && exp->def.obj.typ == &RDB_STRING;
}

/**
 * Return RDB_TRUE if *<var>exp</var> is an invocation of the operator
 * given by name <var>name</var>, otherwise RDB_FALSE.
 */
RDB_bool
RDB_expr_is_op(const RDB_expression *exp, const char *name)
{
    return (exp->kind == RDB_EX_RO_OP) && (strcmp(exp->def.op.name, name) == 0);
}

RDB_bool
RDB_expr_op_is_noarg(const RDB_expression *exp)
{
    return (RDB_bool) (exp->def.op.args.firstp == NULL);
}

/**
 * If *tbp is a virtual table, return the defining expression,
 * otherwise NULL.
 */
RDB_expression *
RDB_vtable_expr(const RDB_object *tbp) {
    if (tbp->kind != RDB_OB_TABLE)
        return NULL;
    return tbp->val.tb.exp;
}

/**
 * Return RDB_TRUE if *<var>op</var> is a table reference,
 * otherwise RDB_FALSE
 */
RDB_bool
RDB_expr_is_table_ref(const RDB_expression *exp)
{
    return (RDB_bool) exp->kind == RDB_EX_TBP;
}

/*@}*/

int
RDB_drop_expr_children(RDB_expression *exp, RDB_exec_context *ecp)
{
    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            if (RDB_del_expr(exp->def.op.args.firstp, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        case RDB_EX_RO_OP:
            if (RDB_destroy_expr_list(&exp->def.op.args, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        default: ;
    }
    return RDB_OK;
}

/*
 * Copy type information from *srcexp to *dstexp
 * if the type info would otherwise get lost
 * (as with RELATION())
 */
int
RDB_copy_expr_typeinfo_if_needed(RDB_expression *dstexp, const RDB_expression *srcexp,
        RDB_exec_context *ecp)
{
    /*
     * RELATION() and ARRAY() need the type information, because othewise
     * the result type will be unknown
     */
    if (srcexp->typ != NULL && dstexp->typ == NULL
            && srcexp->def.op.args.firstp == NULL
            && (strcmp(srcexp->def.op.name, "relation") == 0
             || strcmp(srcexp->def.op.name, "array") == 0)) {
        RDB_type *typ = RDB_dup_nonscalar_type(srcexp->typ, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        dstexp->typ = typ;
    }
    return RDB_OK;
}

/**
 * Initialize empty expression list.
 */
void
RDB_init_expr_list(RDB_expr_list *explistp)
{
    explistp->firstp = explistp->lastp = NULL;
}


/**
 * Drop all expressions in the list.
 */
int
RDB_destroy_expr_list(RDB_expr_list *explistp, RDB_exec_context *ecp)
{
    int ret = RDB_OK;
    RDB_expression *nexp;
    RDB_expression *exp = explistp->firstp;
    while (exp != NULL) {
        nexp = exp->nextp;
        if (RDB_del_expr(exp, ecp) != RDB_OK)
            ret = RDB_ERROR;
        exp = nexp;
    }
    return ret;
}


RDB_int
RDB_expr_list_length(const RDB_expr_list *explistp)
{
    int len = 0;
    RDB_expression *exp = explistp->firstp;
    while (exp != NULL) {
        ++len;
        exp = exp->nextp;
    }
    return (RDB_int) len;
}

void
RDB_expr_list_append(RDB_expr_list *explistp, RDB_expression *exp)
{
    exp->nextp = NULL;
    if (explistp->firstp == NULL) {
        /* It's the first argument */
        explistp->firstp = explistp->lastp = exp;
    } else {
        explistp->lastp->nextp = exp;
        explistp->lastp = exp;
    }
}

void
RDB_expr_list_set_lastp(RDB_expr_list *explistp)
{
    RDB_expression *exp = explistp->firstp;
    if (exp == NULL) {
        explistp->lastp = NULL;
        return;
    }
    while (exp->nextp != NULL)
        exp = exp->nextp;
    explistp->lastp = exp;
}

void
RDB_join_expr_lists(RDB_expr_list *explist1p, RDB_expr_list *explist2p)
{
    if (explist1p->firstp == NULL) {
        explist1p->firstp = explist2p->firstp;
    } else {
        explist1p->lastp->nextp = explist2p->firstp;
    }
    explist1p->lastp = explist2p->lastp;
    explist2p->firstp = explist2p->lastp = NULL;
}

/**
 * Destroy the expression but not the children and don't
 * free the memory.
 */
int
RDB_destroy_expr(RDB_expression *exp, RDB_exec_context *ecp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            /* The expression takes responsibility for non-scalar types */
            if (exp->def.obj.typ != NULL
                    && !RDB_type_is_scalar(exp->def.obj.typ)
                    && exp->def.obj.kind != RDB_OB_TABLE)
                RDB_del_nonscalar_type(exp->def.obj.typ, ecp);
            if (RDB_destroy_obj(&exp->def.obj, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        case RDB_EX_TBP:
            break;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            RDB_free(exp->def.op.name);
            break;
        case RDB_EX_RO_OP:
            RDB_free(exp->def.op.name);
            if (exp->def.op.optinfo.objc > 0) {
                RDB_free(exp->def.op.optinfo.objpv);
                if (exp->def.op.optinfo.objv != NULL) {
                    int i;
                    for (i = 0; i < exp->def.op.optinfo.objc; i++)
                        RDB_destroy_obj(&exp->def.op.optinfo.objv[i], ecp);
                    RDB_free(exp->def.op.optinfo.objv);
                }
            }
            break;
        case RDB_EX_VAR:
            RDB_free(exp->def.varname);
            break;
    }
    if (exp->typ != NULL && !RDB_type_is_scalar(exp->typ))
        return RDB_del_nonscalar_type(exp->typ, ecp);
    return RDB_OK;
}

RDB_bool
RDB_expr_depends_expr(const RDB_expression *ex1p, const RDB_expression *ex2p)
{
    switch (ex1p->kind) {
        case RDB_EX_OBJ:
            return RDB_FALSE;
        case RDB_EX_TBP:
            return RDB_expr_depends_table(ex2p, ex1p->def.tbref.tbp);
        case RDB_EX_VAR:
            return RDB_FALSE;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return RDB_expr_depends_expr(ex1p->def.op.args.firstp, ex2p);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp = ex1p->def.op.args.firstp;
            while (argp != NULL) {
                if (RDB_expr_depends_expr(argp, ex2p))
                    return RDB_TRUE;
                argp = argp->nextp;
            }
            return RDB_FALSE;
        }
    }
    /* Should never be reached */
    abort();
}

RDB_bool
RDB_expr_refers(const RDB_expression *exp, const RDB_object *tbp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            return RDB_FALSE;
        case RDB_EX_TBP:
            return RDB_table_refers(exp->def.tbref.tbp, tbp);
        case RDB_EX_VAR:
            return RDB_FALSE;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return RDB_expr_refers(exp->def.op.args.firstp, tbp);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp = exp->def.op.args.firstp;
            while (argp != NULL) {
                if (RDB_expr_refers(argp, tbp))
                    return RDB_TRUE;
                argp = argp->nextp;
            }
            
            return RDB_FALSE;
        }
    }
    /* Should never be reached */
    abort();
}

RDB_bool
RDB_expr_refers_var(const RDB_expression *exp, const char *attrname)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_FALSE;
        case RDB_EX_VAR:
            return (RDB_bool) (strcmp(exp->def.varname, attrname) == 0);
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return RDB_expr_refers_var(exp->def.op.args.firstp, attrname);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp = exp->def.op.args.firstp;
            while (argp != NULL) {
                if (RDB_expr_refers_var(argp, attrname))
                    return RDB_TRUE;
                argp = argp->nextp;
            }            
            return RDB_FALSE;
        }
    }
    /* Should never be reached */
    abort();
}

/*
 * Check if there is some table which both exp and tbp depend on
 */
RDB_bool
RDB_expr_depends_table(const RDB_expression *exp, const RDB_object *tbp)
{
    if (tbp->val.tb.exp == NULL)
        return RDB_expr_refers(exp, tbp);
    return RDB_expr_depends_expr(tbp->val.tb.exp, exp);
}

RDB_object *
RDB_tpl_get(const char *name, void *arg)
{
    return RDB_tuple_get((RDB_object *) arg, name);
}

RDB_expression *
RDB_create_unexpr(RDB_expression *arg, enum RDB_expr_kind kind,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;

    if (arg == NULL)
        return NULL;

    exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->kind = kind;
    exp->def.op.args.firstp = exp->def.op.args.lastp = arg;
    arg->nextp = NULL;

    return exp;
}

RDB_expression *
RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2,
        enum RDB_expr_kind kind, RDB_exec_context *ecp)
{
    RDB_expression *exp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = kind;
    exp->def.op.args.firstp = arg1;
    exp->def.op.args.lastp = arg2;
    arg1->nextp = arg2;
    arg2->nextp = NULL;

    return exp;
}

int
RDB_invrename_expr(RDB_expression *exp, RDB_expression *texp,
        RDB_exec_context *ecp)
{
    RDB_expression *argp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return RDB_invrename_expr(exp->def.op.args.firstp, texp, ecp);
        case RDB_EX_RO_OP:
            argp = exp->def.op.args.firstp;
            while (argp != NULL) {
                if (RDB_invrename_expr(argp, texp, ecp) != RDB_OK)
                    return RDB_ERROR;
                argp = argp->nextp;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
            /* Search attribute name in dest attrs */
            argp = texp->def.op.args.firstp->nextp;
            while (argp != NULL) {
                RDB_expression *toargp = argp->nextp;
                if (strcmp(RDB_obj_string(&toargp->def.obj),
                        exp->def.varname) == 0)
                    break;
                argp = toargp->nextp;
            }

            /* If found, replace it */
            if (argp != NULL) {
                char *from = RDB_obj_string(&argp->def.obj);
                char *name = RDB_realloc(exp->def.varname, strlen(from) + 1, ecp);
                if (name == NULL) {
                    return RDB_ERROR;
                }

                strcpy(name, from);
                exp->def.varname = name;
            }
            return RDB_OK;
    }
    abort();
}

int
RDB_resolve_exprnames(RDB_expression **expp, RDB_expression *texp,
        RDB_exec_context *ecp)
{
    RDB_expression *argp;

    switch ((*expp)->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return RDB_resolve_exprnames(&(*expp)->def.op.args.firstp,
                    texp, ecp);
        case RDB_EX_RO_OP:
            argp = (*expp)->def.op.args.firstp;
            (*expp)->def.op.args.firstp = NULL;
            while (argp != NULL) {
                RDB_expression *nextp = argp->nextp;
                if (RDB_resolve_exprnames(&argp, texp, ecp) != RDB_OK)
                    return RDB_ERROR;
                RDB_add_arg(*expp, argp);
                argp = nextp;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
            /* Search attribute name in attrs */
            argp = texp;
            while (argp != NULL) {
                if (strcmp(RDB_obj_string(&argp->nextp->def.obj),
                        (*expp)->def.varname) == 0)
                    break;
                argp = argp->nextp->nextp;
            }

            if (argp != NULL) {
                RDB_expression *exp = RDB_dup_expr(argp, ecp);
                if (exp == NULL) {
                    return RDB_ERROR;
                }

                exp->nextp = (*expp)->nextp;
                RDB_del_expr(*expp, ecp);
                *expp = exp;
            }
            return RDB_OK;
    }
    abort();
}

/**
 * Returns if *exp is an invocation of operator *opname whose first
 * argument is a reference to *attrname.
 */
static RDB_bool
expr_var(RDB_expression *exp, const char *attrname, char *opname)
{
    if (exp->kind == RDB_EX_RO_OP && strcmp(exp->def.op.name, opname) == 0) {
        if (exp->def.op.args.firstp->kind == RDB_EX_VAR
                && strcmp(exp->def.op.args.firstp->def.varname, attrname) == 0
                && exp->def.op.args.firstp->nextp->kind == RDB_EX_OBJ)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

/**
 * Find term which is an invocation of operator *opname whose first
 * argument is a reference to *attrname.
 */
RDB_expression *
RDB_attr_node(RDB_expression *exp, const char *attrname, char *opname)
{
    while (exp->kind == RDB_EX_RO_OP
            && strcmp (exp->def.op.name, "and") == 0) {
        if (expr_var(exp->def.op.args.firstp->nextp, attrname, opname))
            return exp->def.op.args.firstp->nextp;
        exp = exp->def.op.args.firstp;
    }
    if (expr_var(exp, attrname, opname))
        return exp;
    return NULL;
}

/**
 * Return TRUE if *srctbp depends on *dsttbp, FALSE otherwise.
 */
RDB_bool
RDB_table_refers(const RDB_object *srctbp, const RDB_object *dsttbp)
{
    RDB_expression *exp;

    if (srctbp == dsttbp)
        return RDB_TRUE;

    exp = RDB_vtable_expr(srctbp);
    if (exp == NULL)
        return RDB_FALSE;
    return RDB_expr_refers(exp, dsttbp);
}

/**
 * Return the n-th expression of an expression list,
 * or NULL if there is no n-th element does not exist.
 */
RDB_expression *
RDB_expr_list_get(RDB_expr_list *explistp, int n)
{
    RDB_expression *exp = explistp->firstp;
    while (n > 0 && exp != NULL) {
        exp = exp->nextp;
        --n;
    }
    return exp;
}
