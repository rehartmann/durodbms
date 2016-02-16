/*
 * Expression functions involving storage.
 *
 * Copyright (C) 2013, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <obj/objinternal.h>

#include <string.h>

/** @addtogroup expr
 * @{
 */

/**
 * Return a new expression in which all variable names for which
 * *<var>getfnp</var>() or RDB_get_table() return an RDB_object
 * have been replaced by this object.
 * Table names are replaced by table refs.
 */
RDB_expression *
RDB_expr_resolve_varnames(RDB_expression *exp, RDB_getobjfn *getfnp,
        void *getdata, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *objp;

    switch (exp->kind) {
    case RDB_EX_RO_OP:
    {
        RDB_expression *argp;
        RDB_expression *newexp;

        newexp = RDB_ro_op(exp->def.op.name, ecp);
        if (newexp == NULL)
            return NULL;

        /* Perform resolve on all arguments */
        argp = exp->def.op.args.firstp;
        while (argp != NULL) {
            RDB_expression *argexp = RDB_expr_resolve_varnames(
                    argp, getfnp, getdata, ecp, txp);
            if (argexp == NULL) {
                RDB_del_expr(newexp, ecp);
                return NULL;
            }
            RDB_add_arg(newexp, argexp);
            argp = argp->nextp;
        }

        /*
         * Duplicate type of RELATION() because otherwise the type information
         * is lost
         */
        if (RDB_copy_expr_typeinfo_if_needed(newexp, exp, ecp) != RDB_OK) {
            RDB_del_expr(newexp, ecp);
            return NULL;
        }

        return newexp;
    }
    case RDB_EX_OBJ:
        return RDB_obj_to_expr(&exp->def.obj, ecp);
    case RDB_EX_TBP:
        return RDB_table_ref(exp->def.tbref.tbp, ecp);
    case RDB_EX_VAR:
        if (getfnp != (RDB_getobjfn *) NULL) {
            objp = (*getfnp)(exp->def.varname, getdata);
            if (objp != NULL) {
                if (objp->kind == RDB_OB_TABLE && objp->val.tbp->exp != NULL) {
                    return RDB_expr_resolve_varnames(objp->val.tbp->exp,
                            getfnp, getdata, ecp, txp);
                }
                if (objp->kind == RDB_OB_TABLE)
                    return RDB_table_ref(objp, ecp);
                return RDB_obj_to_expr(objp, ecp);
            }
        }
        if (txp != NULL) {
            objp = RDB_get_table(exp->def.varname, ecp, txp);
            if (objp != NULL) {
                return RDB_table_ref(objp, ecp);
            }
            RDB_clear_err(ecp);
        }

        return RDB_var_ref(exp->def.varname, ecp);
    }
    abort();
} /* RDB_expr_resolve_varnames */

/**
 * Compare the two expression *<var>ex1p</var> and *<var>ex2p</var>
 * and store RDB_TRUE in *<var>resp</var> if they are equal, otherwise RDB_FALSE.
 */
int
RDB_expr_equals(const RDB_expression *ex1p, const RDB_expression *ex2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_expression *arg1p, *arg2p;

    if (ex1p->kind != ex2p->kind) {
        *resp = RDB_FALSE;
        return RDB_OK;
    }

    switch (ex1p->kind) {
    case RDB_EX_OBJ:
        return RDB_obj_equals(&ex1p->def.obj, &ex2p->def.obj, ecp, txp,
                resp);
    case RDB_EX_TBP:
        if (RDB_table_is_persistent(ex1p->def.tbref.tbp)
                || RDB_table_is_persistent(ex2p->def.tbref.tbp)) {
            *resp = (RDB_bool) (ex1p->def.tbref.tbp == ex2p->def.tbref.tbp);
            return RDB_OK;
        }
        return RDB_obj_equals(ex1p->def.tbref.tbp, ex2p->def.tbref.tbp, ecp, txp,
                resp);
    case RDB_EX_VAR:
        *resp = (RDB_bool)
        (strcmp (ex1p->def.varname, ex2p->def.varname) == 0);
        break;
    case RDB_EX_RO_OP:
        if (RDB_expr_list_length(&ex1p->def.op.args)
                != RDB_expr_list_length(&ex2p->def.op.args)
                || strcmp(ex1p->def.op.name, ex2p->def.op.name) != 0) {
            *resp = RDB_FALSE;
            return RDB_OK;
        }
        arg1p = ex1p->def.op.args.firstp;
        arg2p = ex2p->def.op.args.firstp;
        while (arg1p != NULL) {
            ret = RDB_expr_equals(arg1p, arg2p, ecp, txp, resp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            if (!*resp)
                return RDB_OK;
            arg1p = arg1p->nextp;
            arg2p = arg2p->nextp;
        }
        *resp = RDB_TRUE;
        break;
    }
    return RDB_OK;
}

/**
 * Returns RDB_TRUE if *exp refers to *tbp, RDB_FALSE otherwise.
 */
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

/*@}*/

RDB_expression *
RDB_attr_eq_strval(const char *attrname, const char *str, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_expression *arg2p;
    RDB_expression *argp = RDB_var_ref(attrname, ecp);
    if (argp == NULL) {
        return NULL;
    }

    arg2p = RDB_string_to_expr(str, ecp);
    if (arg2p == NULL) {
        RDB_del_expr(argp, ecp);
        return NULL;
    }
    exp = RDB_eq(argp, arg2p, ecp);
    if (exp == NULL) {
        RDB_del_expr(argp, ecp);
        RDB_del_expr(arg2p, ecp);
        return NULL;
    }

    /* Set transformed flag to avoid infinite recursion */
    exp->transformed = RDB_TRUE;
    return exp;
}

/**
 * If *tbp is a virtual table, return the defining expression,
 * otherwise NULL.
 */
RDB_expression *
RDB_vtable_expr(const RDB_object *tbp) {
    if (tbp->kind != RDB_OB_TABLE)
        return NULL;
    return tbp->val.tbp->exp;
}

/*
 * Check if there is some table which both exp and tbp depend on
 */
RDB_bool
RDB_expr_depends_table(const RDB_expression *exp, const RDB_object *tbp)
{
    if (tbp->val.tbp->exp == NULL)
        return RDB_expr_refers(exp, tbp);
    return RDB_expr_depends_expr(tbp->val.tbp->exp, exp);
}

/*
 * Returns TRUE if *ex1p depends on *ex2p, FALSE otherwise.
 */
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

/*
 * Returns TRUE if *srctbp depends on *dsttbp, FALSE otherwise.
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
