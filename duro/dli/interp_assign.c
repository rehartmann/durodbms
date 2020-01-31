/*
 * Interpreter assignment functions
 *
 * Copyright (C) 2012, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "interp_assign.h"
#include "interp_core.h"
#include "exparse.h"
#include <rel/tostr.h>

#include <string.h>

/*
 * Resolve a qualified intentifier.
 *
 * Given a.b, first check if a is a variable.
 * If it is, a must be a tuple and b must a tuple attribute.
 * It it is not, treat a as a package name.
 */
static RDB_object *
resolve_parse_node_qid(RDB_parse_node *parentp, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_object idobj;
    RDB_object *varp = NULL;
    RDB_parse_node *nodep = parentp->val.children.firstp;

    RDB_init_obj(&idobj);
    if (RDB_string_to_obj(&idobj, "", ecp) != RDB_OK)
        goto error;
    for(;;) {
        if (varp != NULL) {
            if (!RDB_is_tuple(varp)) {
                RDB_raise_invalid_argument("not a tuple", ecp);
                goto error;
            }
            varp = RDB_tuple_get(varp, RDB_expr_var_name(nodep->exp));
            if (varp == NULL) {
                RDB_raise_invalid_argument(RDB_expr_var_name(nodep->exp), ecp);
                goto error;
            }
        } else {
            if (RDB_append_string(&idobj, RDB_expr_var_name(nodep->exp), ecp) != RDB_OK)
                goto error;
            varp = Duro_lookup_var(RDB_obj_string(&idobj), interp, ecp);
        }
        if (nodep->nextp == NULL)
            break;
        if (RDB_append_string(&idobj, ".", ecp) != RDB_OK)
            goto error;
        nodep = nodep->nextp->nextp;
    }
    RDB_destroy_obj(&idobj, ecp);
    return varp;

error:
    RDB_destroy_obj(&idobj, ecp);
    return NULL;
}

static RDB_object *
resolve_target(RDB_expression *exp, Duro_interp *interp, RDB_exec_context *ecp)
{
    const char *varname;
    RDB_object *objp;
    RDB_object *prop;
    RDB_expr_list *arglistp;
    const char *opname = RDB_expr_op_name(exp);

    if (opname != NULL) {
        arglistp = RDB_expr_op_args((RDB_expression *)exp);
        if (strcmp(opname, "[]") == 0
                && RDB_expr_list_length(arglistp) == 2) {
            RDB_object idxobj;
            RDB_object *retvalp;

            /*
             * Resolve array subscription
             */

            /* Get first argument, which must be an array */
            objp = resolve_target(RDB_expr_list_get(arglistp, 0), interp, ecp);
            if (objp == NULL)
                return NULL;

            if (RDB_is_array(objp)) {
            	/* Get second argument, which must be integer */
            	RDB_init_obj(&idxobj);
            	if (RDB_evaluate(RDB_expr_list_get(arglistp, 1), &Duro_get_var, interp, interp->envp, ecp,
            			interp->txnp != NULL ? &interp->txnp->tx : NULL,
            					&idxobj) != RDB_OK) {
            		RDB_destroy_obj(&idxobj, ecp);
            		return NULL;
            	}
            	if (RDB_obj_type(&idxobj) != &RDB_INTEGER) {
            		RDB_raise_type_mismatch("array index must be integer", ecp);
            		RDB_destroy_obj(&idxobj, ecp);
            		return NULL;
            	}
            	retvalp = RDB_array_get(objp, RDB_obj_int(&idxobj), ecp);
            	RDB_destroy_obj(&idxobj, ecp);
            	return retvalp;
            } else if (RDB_is_tuple(objp)) {
            	/* Get second argument, which must be string */
            	RDB_init_obj(&idxobj);
            	if (RDB_evaluate(RDB_expr_list_get(arglistp, 1), &Duro_get_var, interp, interp->envp, ecp,
            			interp->txnp != NULL ? &interp->txnp->tx : NULL,
            					&idxobj) != RDB_OK) {
            		RDB_destroy_obj(&idxobj, ecp);
            		return NULL;
            	}
            	if (RDB_obj_type(&idxobj) != &RDB_STRING) {
            		RDB_raise_type_mismatch("tuple attribute must be string", ecp);
            		RDB_destroy_obj(&idxobj, ecp);
            		return NULL;
            	}
            	retvalp = RDB_tuple_get(objp, RDB_obj_string(&idxobj));
            	if (retvalp == NULL) {
            		RDB_raise_name(RDB_obj_string(&idxobj), ecp);
            	}
            	RDB_destroy_obj(&idxobj, ecp);
            	return retvalp;
            } else {
                RDB_raise_type_mismatch("array or tuple required", ecp);
                return NULL;
            }
        }
        if (strcmp(opname, ".") == 0 && RDB_expr_list_length(arglistp) == 2) {
            RDB_object idobj;
            RDB_object *resp;

            /* Check if it's a tuple attribute */
            objp = resolve_target(RDB_expr_list_get(arglistp, 0), interp, ecp);

            if (objp != NULL) {
                if (RDB_is_tuple(objp)) {
                    resp = RDB_tuple_get(objp, RDB_expr_var_name(RDB_expr_list_get(arglistp, 1)));
                    if (resp != NULL)
                        return resp;
                } else if (RDB_obj_type(objp) != NULL) {
                    const char *propname;

                    /* Type must be system-implemented with tuple as internal rep */
                    if (!objp->typ->def.scalar.sysimpl
                            || objp->typ->def.scalar.repv[0].compc <= 1) {
                        RDB_raise_not_supported("unsupported property assignment target", ecp);
                        return NULL;
                    }

                    propname = RDB_expr_var_name(RDB_expr_list_get(arglistp, 1));
                    prop = RDB_tuple_get(objp, propname);
                    if (prop == NULL) {
                        RDB_raise_operator_not_found(propname, ecp);
                        return NULL;
                    }
                    if (prop->typ == NULL) {
                        int i;

                        for (i = 0;
                             i < objp->typ->def.scalar.repv[0].compc
                                    && strcmp(objp->typ->def.scalar.repv[0].compv[i].name,
                                              propname) != 0;
                             i++);
                        if (i >= objp->typ->def.scalar.repv[0].compc) {
                            RDB_raise_internal("component not found", ecp);
                            return NULL;
                        }
                        RDB_obj_set_typeinfo(prop, objp->typ->def.scalar.repv[0].compv[i].typ);
                    }
                    return prop;
                }
            }

            RDB_init_obj(&idobj);
            if (RDB_expr_attr_qid(exp, &idobj, ecp) != RDB_OK) {
                RDB_destroy_obj(&idobj, ecp);
                return NULL;
            }
            resp = Duro_lookup_var(RDB_obj_string(&idobj), interp, ecp);
            RDB_destroy_obj(&idobj, ecp);
            return resp;
        }
        if (strlen(opname) > RDB_TREAT_PREFIX_LEN
                && strncmp(opname, RDB_TREAT_PREFIX, RDB_TREAT_PREFIX_LEN) == 0
                && RDB_expr_list_length(arglistp) == 1) {
            RDB_object *argobjp;
            RDB_type *typ = RDB_get_type(opname + RDB_TREAT_PREFIX_LEN, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL);
            if (typ == NULL)
                return NULL;
            argobjp = resolve_target(RDB_expr_list_get(arglistp, 0), interp, ecp);
            if (argobjp == NULL)
                return NULL;

            if (!RDB_obj_matches_type(argobjp, typ)) {
                RDB_raise_type_mismatch(opname, ecp);
                return NULL;
            }
            return argobjp;
        }

        RDB_raise_syntax("invalid assignment target", ecp);
        return NULL;
    }

    /* Resolve variable name */
    varname = RDB_expr_var_name(exp);
    if (varname != NULL) {
        return Duro_lookup_var(varname, interp, ecp);
    }
    RDB_raise_syntax("invalid assignment target", ecp);
    return NULL;
}

static int
comp_idx(RDB_possrep *possrep, const char *name)
{
    int i;
    for (i = 0; i < possrep->compc; i++) {
        if (strcmp(possrep->compv[i].name, name) == 0)
            return i;
    }
    return -1;
}

static int
comp_node_to_copy(RDB_ma_copy *copyp, RDB_parse_node *nodep,
        RDB_expression *dstexp, RDB_expression *srcexp,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    int i;
    RDB_possrep *possrep;
    RDB_object **argpv = NULL;
    RDB_object *argv = NULL;
    const char *propname = RDB_expr_var_name(RDB_expr_list_get(RDB_expr_op_args(dstexp), 1));
    RDB_type *typ = RDB_expr_type(RDB_expr_list_get(RDB_expr_op_args(dstexp), 0), &Duro_get_var_type, interp,
                interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (typ == NULL)
        return RDB_ERROR;

    possrep = RDB_comp_possrep(typ, propname);
    if (possrep == NULL) {
        RDB_raise_name(propname, ecp);
        return RDB_ERROR;
    }

    argpv = RDB_alloc(sizeof(RDB_object *) * possrep->compc, ecp);
    if (argpv == NULL)
        goto error;
    argv = RDB_alloc(sizeof(RDB_object) * possrep->compc, ecp);
    if (argv == NULL)
        goto error;
    for (i = 0; i < possrep->compc; i++) {
        argpv[i] = NULL;
        RDB_init_obj(&argv[i]);
    }

    i = comp_idx(possrep, propname);
    if (RDB_evaluate(srcexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL,
            &argv[i]) != RDB_OK)
        goto error;
    argpv[i] = &argv[i];

    /* Search for other the_ assignments of the same destination */
    while (nodep->nextp != NULL) {
        /* Skip comma */
        nodep = nodep->nextp->nextp;
        if (nodep->xdata != NULL) {
            RDB_expression *fdstexp = nodep->xdata;
            const char *opname = RDB_expr_op_name(fdstexp);
            RDB_expr_list *fdstarglist = RDB_expr_op_args(fdstexp);
            if (opname != NULL && strcmp(opname, ".") == 0
                    && RDB_expr_list_length(fdstarglist) == 2) {
                RDB_bool iseq;
                if (RDB_expr_equals(RDB_expr_list_get(RDB_expr_op_args(dstexp), 0),
                        RDB_expr_list_get(fdstarglist, 0), ecp,
                        interp->txnp != NULL ? &interp->txnp->tx : NULL, &iseq)
                        != RDB_OK) {
                    goto error;
                }
                if (iseq) {
                    i = comp_idx(possrep, RDB_expr_var_name(
                            RDB_expr_list_get(fdstarglist, 1)));
                    if (i == -1) {
                        /* Invald component name or different possrep */
                        RDB_raise_syntax("invalid assignment", ecp);
                        goto error;
                    }
                    if (argpv[i] != NULL) {
                        RDB_raise_syntax("identical assignment targets", ecp);
                        goto error;
                    }
                    srcexp = RDB_parse_node_expr(
                            nodep->val.children.firstp->nextp->nextp, ecp,
                            interp->txnp != NULL ? &interp->txnp->tx : NULL);
                    if (srcexp == NULL)
                        goto error;
                    if (RDB_evaluate(srcexp, &Duro_get_var, interp, interp->envp, ecp,
                            interp->txnp != NULL ? &interp->txnp->tx : NULL,
                            &argv[i]) != RDB_OK)
                        goto error;
                    argpv[i] = &argv[i];

                    /* This node has been consumed */
                    nodep->xdata = NULL;
                }
            }
        }
    }

    copyp->dstp = resolve_target(RDB_expr_list_get(RDB_expr_op_args(dstexp), 0), interp, ecp);
    if (copyp->dstp == NULL)
        goto error;

    /* Get remaining selector arguments from destination */

    for (i = 0; i < possrep->compc; i++) {
        if (argpv[i] == NULL) {
            if (RDB_obj_property(copyp->dstp, possrep->compv[i].name, &argv[i],
                    interp->envp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL)
                            != RDB_OK) {
                goto error;
            }
            argpv[i] = &argv[i];
        }
    }

    if (RDB_call_ro_op_by_name_e(possrep->name, possrep->compc, argpv,
            interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL,
            copyp->srcp) != RDB_OK)
        goto error;

    if (argpv != NULL)
        RDB_free(argpv);
    if (argv != NULL) {
        for (i = 0; i < possrep->compc; i++)
            RDB_destroy_obj(&argv[i], ecp);
        RDB_free(argv);
    }
    return RDB_OK;

error:
    if (argpv != NULL)
        RDB_free(argpv);
    if (argv != NULL) {
        for(i = 0; i < possrep->compc; i++)
            RDB_destroy_obj(&argv[i], ecp);
        RDB_free(argv);
    }
    return RDB_ERROR;
}

static int
is_property(RDB_expression *exp, Duro_interp *interp,
        RDB_exec_context *ecp, RDB_bool *resp)
{
    *resp = RDB_FALSE;
    if (RDB_expr_is_op(exp, ".")) {
        RDB_expr_list *args = RDB_expr_op_args(exp);
        if (RDB_expr_list_length(args) == 2) {
            RDB_type *typ = RDB_expr_type(RDB_expr_list_get(args, 0),
                &Duro_get_var_type, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
            if (typ == NULL) {
                if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NAME_ERROR)
                    return RDB_OK;
                return RDB_ERROR;
            }
            if (RDB_type_is_scalar(typ))
                *resp = RDB_TRUE;
        }
    }
    return RDB_OK;
}

static int
node_to_copy(RDB_ma_copy *copyp, RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_bool isprop;
    RDB_expression *dstexp = nodep->xdata;
    RDB_expression *srcexp = RDB_parse_node_expr(
            nodep->val.children.firstp->nextp->nextp,
            ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srcexp == NULL) {
        return RDB_ERROR;
    }

    if (is_property(dstexp, interp, ecp, &isprop) != RDB_OK) {
        return RDB_ERROR;
    }

    if (isprop) {
        return comp_node_to_copy(copyp, nodep, dstexp, srcexp, interp, ecp);
    }

    if (RDB_evaluate(srcexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL,
            copyp->srcp) != RDB_OK) {
        return RDB_ERROR;
    }

    copyp->dstp = resolve_target(dstexp, interp, ecp);
    if (copyp->dstp == NULL) {
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
tuple_update_to_copy(RDB_ma_copy *copyp, RDB_object *dstp, RDB_parse_node *nodep,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *srcexp;
    RDB_expression *srcvarexp;
    RDB_parse_node *ap;

    if (nodep->nextp->val.token == TOK_WHERE) {
        RDB_raise_syntax("WHERE not allowed with tuple UPDATE", ecp);
        goto error;
    }

    copyp->dstp = dstp;

    /*
     * Get source value by creating an UPDATE expression
     * and evaluating it
     */
    srcexp = RDB_ro_op("update", ecp);
    if (srcexp == NULL)
        return RDB_ERROR;
    srcvarexp = RDB_dup_expr(nodep->exp, ecp);
    if (srcvarexp == NULL)
        goto error;
    RDB_add_arg(srcexp, srcvarexp);

    ap = nodep->nextp->nextp->val.children.firstp;
    for(;;) {
        RDB_expression *exp = RDB_string_to_expr(
                RDB_expr_var_name(RDB_parse_node_expr(
                        ap->val.children.firstp, ecp,
                        interp->txnp != NULL ? &interp->txnp->tx : NULL)),
                ecp);
        if (exp == NULL)
            goto error;
        RDB_add_arg(srcexp, exp);

        exp = RDB_parse_node_expr(
                ap->val.children.firstp->nextp->nextp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (exp == NULL)
            goto error;
        exp = RDB_dup_expr(exp, ecp);
        if (exp == NULL)
            goto error;
        RDB_add_arg(srcexp, exp);

        ap = ap->nextp;
        if (ap == NULL)
            break;

        /* Skip comma */
        ap = ap->nextp;
    }

    if (RDB_evaluate(srcexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL,
            copyp->srcp) != RDB_OK) {
        goto error;
    }

    RDB_del_expr(srcexp, ecp);
    return RDB_OK;

error:
    RDB_del_expr(srcexp, ecp);
    return RDB_ERROR;
}

static int
node_to_insert(RDB_ma_insert *insp, RDB_parse_node *nodep, Duro_interp *interp,
        int flags, RDB_exec_context *ecp)
{
    RDB_expression *srcexp;
    RDB_type *srctyp;

    insp->tbp = resolve_parse_node_qid(nodep, interp, ecp);
    if (insp->tbp == NULL) {
        return RDB_ERROR;
    }

    insp->flags = flags;

    /* Only tables are allowed as target */
    if (insp->tbp->typ == NULL
            || !RDB_type_is_relation(insp->tbp->typ)) {
        RDB_raise_type_mismatch("INSERT target must be relation", ecp);
        return RDB_ERROR;
    }

    srcexp = RDB_parse_node_expr(nodep->nextp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srcexp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_evaluate(srcexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL,
            insp->objp) != RDB_OK) {
        return RDB_ERROR;
    }

    if (RDB_is_tuple(insp->objp)) {
        /* Get type of the source tuple */
        srctyp = RDB_expr_type(srcexp, &Duro_get_var_type, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (srctyp == NULL) {
            return RDB_ERROR;
        }
        RDB_obj_set_typeinfo(insp->objp, srctyp);
    }

    return RDB_OK;
}

static int
node_to_update(RDB_ma_update *updp, RDB_object *dstp, RDB_parse_node *nodep,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_parse_node *np;
    RDB_parse_node *aafnp;
    int i;

    updp->tbp = dstp;

    if (nodep->nextp->val.token == TOK_WHERE) {
        updp->condp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (updp->condp == NULL)
            return RDB_ERROR;
        np = nodep->nextp->nextp->nextp->nextp->val.children.firstp;
    } else {
        updp->condp = NULL;
        np = nodep->nextp->nextp->val.children.firstp;
    }

    for (i = 0; i < updp->updc; i++) {
        if (i > 0)
            np = np->nextp;
        aafnp = np->val.children.firstp;
        updp->updv[i].name = RDB_expr_var_name(aafnp->exp);
        updp->updv[i].exp = RDB_parse_node_expr(aafnp->nextp->nextp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (updp->updv[i].exp == NULL)
            return RDB_ERROR;
        np = np->nextp;
    }
    return RDB_OK;
}

static int
node_to_delete(RDB_ma_delete *delp, RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    delp->tbp = resolve_parse_node_qid(nodep, interp, ecp);
    if (delp->tbp == NULL) {
        return RDB_ERROR;
    }

    /* Only tables are allowed as target */
    if (delp->tbp->typ == NULL
            || !RDB_type_is_relation(delp->tbp->typ)) {
        RDB_raise_type_mismatch("DELETE target must be relation", ecp);
        return RDB_ERROR;
    }

    if (nodep->nextp == NULL) {
        delp->condp = NULL;
    } else {
        delp->condp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (delp->condp == NULL)
            return RDB_ERROR;
    }

    return RDB_OK;
}

static int
node_to_vdelete(RDB_ma_vdelete *delp, RDB_parse_node *nodep, Duro_interp *interp,
        int flags, RDB_exec_context *ecp)
{
    RDB_expression *srcexp;

    delp->tbp = resolve_parse_node_qid(nodep, interp, ecp);
    if (delp->tbp == NULL) {
        return RDB_ERROR;
    }

    /* Only tables are allowed as target */
    if (delp->tbp->typ == NULL
            || !RDB_type_is_relation(delp->tbp->typ)) {
        RDB_raise_type_mismatch("INSERT target must be relation", ecp);
        return RDB_ERROR;
    }

    srcexp = RDB_parse_node_expr(nodep->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srcexp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_evaluate(srcexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL,
            delp->objp) != RDB_OK) {
        return RDB_ERROR;
    }

    delp->flags = flags;

    return RDB_OK;
}

/*
 * Check if the first child of nodep is an operator invocation
 * and return it as an expression if it is.
 */
static const RDB_expression *
op_assign(const RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    const char *opname;
    RDB_parse_node *tnodep = nodep->val.children.firstp;
    if (tnodep->kind == RDB_NODE_TOK)
        return NULL;

    exp = RDB_parse_node_expr(tnodep, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    opname = RDB_expr_op_name(exp);
    if (opname == NULL)
        return NULL;
    return exp;
}

static int
resize_array_by_exp(RDB_object *arrp, RDB_expression *srcexp,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_object lenobj;
    RDB_int len;
    RDB_int olen;

    RDB_type *arrtyp = RDB_obj_type(arrp);
    if (arrtyp == NULL || !RDB_type_is_array(arrtyp)) {
        RDB_raise_type_mismatch("not an array", ecp);
        return RDB_ERROR;
    }
    olen = RDB_array_length(arrp, ecp);
    if (olen < 0)
        return RDB_ERROR;

    RDB_init_obj(&lenobj);
    if (RDB_evaluate(srcexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL,
                    &lenobj) != RDB_OK) {
        goto error;
    }
    if (RDB_obj_type(&lenobj) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("array length must be INTEGER", ecp);
        goto error;
    }
    len = RDB_obj_int(&lenobj);
    if (RDB_set_array_length(arrp, len, ecp) != RDB_OK) {
        goto error;
    }

    /* Initialize new elements */
    if (len > olen) {
        int i;
        RDB_type *basetyp = RDB_base_type(arrtyp);
        for (i = olen; i < len; i++) {
            RDB_object *elemp = RDB_array_get(arrp, i, ecp);
            if (elemp == NULL)
                goto error;

            if (RDB_set_init_value(elemp, basetyp, interp->envp, ecp) != RDB_OK)
                goto error;
        }
    }
    RDB_destroy_obj(&lenobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&lenobj, ecp);
    return RDB_ERROR;
}

static int
exec_length_assign(const RDB_parse_node *nodep, RDB_expression *argexp,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *srcexp;
    RDB_object *arrp;

    if (nodep->nextp != NULL) {
        RDB_raise_syntax("only single assignment of array length permitted", ecp);
        return RDB_ERROR;
    }
    srcexp = RDB_parse_node_expr(nodep->val.children.firstp->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srcexp == NULL)
        return RDB_ERROR;

    arrp = resolve_target(argexp, interp, ecp);
    if (arrp == NULL) {
        /*
         * length argument cannot be resolved, evaluate and set
         */
        int ret;
        RDB_object arrobj;
        RDB_object *dstobjp;
        RDB_expression *dstexp;

        RDB_init_obj(&arrobj);
        if (RDB_evaluate(argexp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL,
                &arrobj) != RDB_OK) {
            RDB_destroy_obj(&arrobj, ecp);
            return RDB_ERROR;
        }
        if (resize_array_by_exp(&arrobj, srcexp, interp, ecp) != RDB_OK) {
            RDB_destroy_obj(&arrobj, ecp);
            return RDB_ERROR;
        }
        dstexp = RDB_expr_list_get(RDB_expr_op_args(argexp), 0);
        dstobjp = resolve_target(dstexp, interp, ecp);
        if (dstobjp == NULL) {
            RDB_destroy_obj(&arrobj, ecp);
            return RDB_ERROR;
        }
        ret = RDB_obj_set_property(dstobjp,
                RDB_expr_var_name(RDB_expr_list_get(RDB_expr_op_args(argexp), 1)), &arrobj,
                interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        RDB_destroy_obj(&arrobj, ecp);
        return ret;
    }

    return resize_array_by_exp(arrp, srcexp, interp, ecp);
}

/*
 * Execute property assignment using the dot operator
 */
static int
exec_dot_assign_set(RDB_object *dstp, const RDB_type *dsttyp,
        const RDB_parse_node *nodep, const RDB_expression *opexp,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_expression *srcexp;
    RDB_object srcobj;
    RDB_expr_list *arglist = RDB_expr_op_args((RDB_expression *) opexp);
    const char *propname = RDB_expr_var_name(RDB_expr_list_get(arglist, 1));

    if (RDB_type_property(dsttyp, propname) == NULL) {
        RDB_raise_name(propname, ecp);
        return RDB_ERROR;
    }
    srcexp = RDB_parse_node_expr(nodep->val.children.firstp->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srcexp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&srcobj);
    if (RDB_evaluate(srcexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL,
            &srcobj) != RDB_OK) {
        RDB_destroy_obj(&srcobj, ecp);
        return RDB_ERROR;
    }

    ret = RDB_obj_set_property(dstp, propname, &srcobj,
            interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    RDB_destroy_obj(&srcobj, ecp);
    return ret;
}

/*
 * Convert assignments so they can be passed to RDB_multi_assign()
 */
static int
node_to_multi_assign(const RDB_parse_node *listnodep,
        int *copycp, RDB_ma_copy *copyv,
        int *inscp, RDB_ma_insert *insv,
        int *updcp, RDB_ma_update *updv,
        int *delcp, RDB_ma_delete *delv,
        int *vdelcp, RDB_ma_vdelete *vdelv,
        int *srcobjcp, RDB_object *srcobjv,
        int *attrupdcp, RDB_attr_update *attrupdv,
        Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int i;
    RDB_expression *dstexp;
    RDB_parse_node *nodep;

    /*
     * If there are several the_() assignments to the same target
     * they must be combined into a single selector call.
     *
     * First get the target expressions of all := assignments.
     * When several the_() assignments are combined, the target expressions
     * except the first are eliminated.
     */

    nodep = listnodep->val.children.firstp;
    for(;;) {
        if (nodep->val.children.firstp->kind != RDB_NODE_TOK) {
            /* First node not a token, so it's a := assignment */
            RDB_expression *dstexp = RDB_parse_node_expr(
                    nodep->val.children.firstp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL);
            if (dstexp == NULL) {
                return RDB_ERROR;
            }
            nodep->xdata = dstexp;
        } else {
            nodep->xdata = NULL;
        }
        if (nodep->nextp == NULL)
            break;

        /* Skip comma */
        nodep = nodep->nextp->nextp;
    }

    nodep = listnodep->val.children.firstp;
    for(;;) {
        RDB_object *dstp;
        RDB_parse_node *firstp = nodep->val.children.firstp;

        if (firstp->kind == RDB_NODE_TOK) {
            switch(firstp->val.token) {
            case TOK_INSERT:
            case TOK_D_INSERT:
                if ((*srcobjcp) >= DURO_MAX_LLEN) {
                    RDB_raise_not_supported("too many assigments", ecp);
                    return RDB_ERROR;
                }

                RDB_init_obj(&srcobjv[(*srcobjcp)++]);
                insv[(*inscp)].objp = &srcobjv[(*srcobjcp) - 1];
                if (node_to_insert(&insv[(*inscp)++], firstp->nextp,
                        interp, firstp->val.token == TOK_INSERT ? 0 : RDB_DISTINCT,
                                ecp) != RDB_OK) {
                    goto error;
                }
                break;
            case TOK_UPDATE:
                if ((*updcp) >= DURO_MAX_LLEN) {
                    RDB_raise_not_supported("too many updates", ecp);
                    return RDB_ERROR;
                }

                /* 3rd node must be a token, either WHERE or { */
                if (firstp->nextp->nextp->val.token == TOK_WHERE) {
                    /* WHERE condition is present */
                    updv[(*updcp)].updc = (RDB_parse_nodelist_length(
                            firstp->nextp->nextp->nextp->nextp->nextp) + 1) / 2;
                } else {
                    updv[(*updcp)].updc = (RDB_parse_nodelist_length(
                            firstp->nextp->nextp->nextp) + 1) / 2;
                }
                if ((*attrupdcp) + updv[(*updcp)].updc > DURO_MAX_LLEN) {
                    RDB_raise_not_supported("too many assigments", ecp);
                    goto error;
                }

                dstexp = RDB_parse_node_expr(firstp->nextp, ecp,
                        interp->txnp != NULL ? &interp->txnp->tx : NULL);
                if (dstexp == NULL) {
                    goto error;
                }

                dstp = resolve_target(dstexp, interp, ecp);
                if (dstp == NULL) {
                    return RDB_ERROR;
                }

                /* Only tables and tuples are allowed as target */
                if (dstp->typ == NULL
                        || !(RDB_type_is_relation(dstp->typ)
                                || RDB_type_is_tuple(dstp->typ))) {
                    RDB_raise_type_mismatch(
                            "UPDATE target must be tuple or relation", ecp);
                    return RDB_ERROR;
                }
                if (dstp->typ != NULL && RDB_type_is_tuple(dstp->typ)) {
                    /*
                     * Tuple update
                     */
                    if ((*srcobjcp) >= DURO_MAX_LLEN) {
                        RDB_raise_not_supported("too many assigments", ecp);
                        return RDB_ERROR;
                    }

                    RDB_init_obj(&srcobjv[(*srcobjcp)++]);
                    copyv[(*copycp)].srcp = &srcobjv[(*srcobjcp) - 1];
                    if (tuple_update_to_copy(&copyv[(*copycp)++], dstp, firstp->nextp,
                            interp, ecp) != RDB_OK) {
                        goto error;
                    }
                } else {
                    if ((*updcp) >= DURO_MAX_LLEN) {
                        RDB_raise_not_supported("too many updates", ecp);
                        return RDB_ERROR;
                    }
                    updv[(*updcp)].updv = &attrupdv[(*attrupdcp)];
                    if (node_to_update(&updv[(*updcp)], dstp, firstp->nextp,
                            interp, ecp) != RDB_OK) {
                        goto error;
                    }
                    (*attrupdcp) += updv[(*updcp)].updc;
                    (*updcp)++;
                }
                break;
            case TOK_DELETE:
            case TOK_I_DELETE:
                if (firstp->nextp->nextp == NULL
                        || (firstp->nextp->nextp->nextp != NULL
                                && firstp->nextp->nextp->nextp->kind
                                != RDB_NODE_TOK)) {
                    if ((*delcp) >= DURO_MAX_LLEN) {
                        RDB_raise_not_supported("too many deletes", ecp);
                        return RDB_ERROR;
                    }
                    if (node_to_delete(&delv[(*delcp)++], firstp->nextp,
                            interp, ecp) != RDB_OK) {
                        goto error;
                    }
                } else {
                    /* DELETE <exp> <exp> */
                    if ((*srcobjcp) >= DURO_MAX_LLEN) {
                        RDB_raise_not_supported("too many assigments", ecp);
                        return RDB_ERROR;
                    }

                    RDB_init_obj(&srcobjv[(*srcobjcp)++]);
                    vdelv[(*vdelcp)].objp = &srcobjv[(*srcobjcp) - 1];
                    if (node_to_vdelete(&vdelv[(*vdelcp)++], firstp->nextp,
                            interp, firstp->val.token == TOK_DELETE ? 0 : RDB_INCLUDED,
                                    ecp) != RDB_OK) {
                        goto error;
                    }
                }
                break;
            }
        } else if (nodep->xdata != NULL) {
            /* xdata is set to NULL when an assignment has been consumed */

            if ((*srcobjcp) >= DURO_MAX_LLEN) {
                RDB_raise_not_supported("too many assigments", ecp);
                return RDB_ERROR;
            }

            RDB_init_obj(&srcobjv[(*srcobjcp)++]);
            copyv[(*copycp)].srcp = &srcobjv[(*srcobjcp) - 1];
            if (node_to_copy(&copyv[(*copycp)++], nodep, interp, ecp) != RDB_OK) {
                goto error;
            }
        }

        if (nodep->nextp == NULL)
            break;

        /* Skip comma */
        nodep = nodep->nextp->nextp;
    }
    return RDB_OK;

    error:
        for (i = 0; i < (*srcobjcp); i++)
            RDB_destroy_obj(&srcobjv[i], ecp);
        return RDB_ERROR;
}

int
Duro_exec_assign(const RDB_parse_node *listnodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int i;
    int cnt;
    const RDB_expression *opexp;
    RDB_ma_copy copyv[DURO_MAX_LLEN];
    RDB_ma_insert insv[DURO_MAX_LLEN];
    RDB_ma_update updv[DURO_MAX_LLEN];
    RDB_ma_delete delv[DURO_MAX_LLEN];
    RDB_ma_vdelete vdelv[DURO_MAX_LLEN];
    RDB_object srcobjv[DURO_MAX_LLEN];
    RDB_attr_update attrupdv[DURO_MAX_LLEN];
    int copyc = 0;
    int insc = 0;
    int updc = 0;
    int delc = 0;
    int vdelc = 0;
    int srcobjc = 0;
    int attrupdc = 0;
    RDB_parse_node *nodep = listnodep->val.children.firstp;

    /*
     * Special handling for setting array length and THE_ operator
     * when there is only one assignment
     */
    if (nodep->nextp == NULL) {
        opexp = op_assign(nodep, interp, ecp);
        if (opexp != NULL) {
            RDB_expr_list *arglist = RDB_expr_op_args((RDB_expression *) opexp);
            int argcount = (int) RDB_expr_list_length(arglist);
            const char *opname = RDB_expr_op_name(opexp);
            if (opname != NULL) {
                if (strcmp(opname, "length") == 0 && argcount == 1) {
                    return exec_length_assign(nodep,
                            RDB_expr_list_get(arglist, 0), interp, ecp);
                }

                if (strcmp(opname, ".") == 0 && argcount == 2) {
                    /* If the first argument is scalar, assign property */
                    RDB_expression *dstexp = RDB_expr_list_get(arglist, 0);
                    RDB_object *dstp = resolve_target(dstexp, interp, ecp);
                    if (dstp != NULL) {
                        RDB_type *dsttyp = RDB_expr_type(dstexp, &Duro_get_var_type, interp,
                                        interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
                        if (dsttyp != NULL && RDB_type_is_scalar(dsttyp))
                            return exec_dot_assign_set(dstp, dsttyp, nodep, opexp, interp, ecp);
                    }
                }
            }
        }
    }

    if (node_to_multi_assign(listnodep,
            &copyc, copyv, &insc, insv, &updc, updv, &delc, delv, &vdelc, vdelv,
            &srcobjc, srcobjv, &attrupdc, attrupdv, interp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    /*
     * Execute assignments
     */
    cnt = RDB_multi_assign(insc, insv, updc, updv, delc, delv, vdelc, vdelv,
            copyc, copyv, &Duro_get_var, interp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;

    if (RDB_parse_get_interactive() && interp->inner_op == NULL) {
        if (cnt == 1) {
            printf("1 element affected.\n");
        } else {
            printf("%d elements affected.\n", (int) cnt);
        }
    }

    for (i = 0; i < srcobjc; i++)
        RDB_destroy_obj(&srcobjv[i], ecp);

    return RDB_OK;

error:
    for (i = 0; i < srcobjc; i++)
        RDB_destroy_obj(&srcobjv[i], ecp);

    return RDB_ERROR;
}

int
Duro_exec_load(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object srctb;
    RDB_object *srctbp;
    RDB_expression *tbexp;
    RDB_object *dstp;
    RDB_type *dsttyp;
    RDB_type *srctyp;
    const char *srcvarname;
    int seqitc;
    RDB_parse_node *seqitnodep;
    RDB_seq_item *seqitv = NULL;

    RDB_init_obj(&srctb);

    dstp = resolve_parse_node_qid(nodep, interp, ecp);
    if (dstp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    tbexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (tbexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Type check
     */

    dsttyp = RDB_obj_type(dstp);
    if (dsttyp == NULL) {
        RDB_raise_invalid_argument("destination type not available", ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (!RDB_type_is_array(dsttyp)) {
        RDB_raise_type_mismatch("destination is not an array", ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    srctyp = RDB_expr_type(tbexp, &Duro_get_var_type, interp,
            interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srctyp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (!RDB_type_is_relation(srctyp)) {
        RDB_raise_type_mismatch("source is not a relation", ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (!RDB_type_equals(RDB_base_type(srctyp), RDB_base_type(dsttyp))) {
        RDB_raise_type_mismatch("source does not match destination", ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    /*
     * If the expression is a variable reference, look up the variable,
     * otherwise evaluate the expression
     */
    srcvarname = RDB_expr_var_name(tbexp);
    if (srcvarname != NULL) {
    	int flags;

        srctbp = Duro_lookup_sym(srcvarname, interp, &flags, ecp);
        if (srctbp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    } else {
        if (RDB_evaluate(tbexp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL,
                &srctb) != RDB_OK) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        srctbp = &srctb;
    }

    seqitnodep = nodep->nextp->nextp->nextp->nextp->nextp;
    seqitc = (RDB_parse_nodelist_length(seqitnodep) + 1) / 2;
    if (seqitc > 0) {
        seqitv = RDB_alloc(sizeof(RDB_seq_item) * seqitc, ecp);
        if (seqitv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    }
    ret = Duro_nodes_to_seqitv(seqitv, seqitnodep->val.children.firstp,
            interp, ecp);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    ret = RDB_table_to_array(dstp, srctbp, seqitc, seqitv, 0, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);

cleanup:
    if (seqitv != NULL)
        RDB_free(seqitv);
    RDB_destroy_obj(&srctb, ecp);
    return ret;
}

static int
put_constraint_expr(RDB_expression *exp, const char *name,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object strobj;

    RDB_init_obj(&strobj);

    /* Convert tree to STRING */
    if (RDB_expr_to_str(&strobj, exp, ecp, txp, RDB_SHOW_INDEX) != RDB_OK) {
        goto error;
    }

    printf("check %s: %s\n", name, RDB_obj_string(&strobj));

    RDB_destroy_obj(&strobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&strobj, ecp);
    return RDB_ERROR;
}

int
Duro_exec_explain_assign(const RDB_parse_node *listnodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int i;
    RDB_ma_copy copyv[DURO_MAX_LLEN];
    RDB_ma_insert insv[DURO_MAX_LLEN];
    RDB_ma_update updv[DURO_MAX_LLEN];
    RDB_ma_delete delv[DURO_MAX_LLEN];
    RDB_ma_vdelete vdelv[DURO_MAX_LLEN];
    RDB_object srcobjv[DURO_MAX_LLEN];
    RDB_attr_update attrupdv[DURO_MAX_LLEN];
    int copyc = 0;
    int insc = 0;
    int updc = 0;
    int delc = 0;
    int vdelc = 0;
    int srcobjc = 0;
    int attrupdc = 0;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (node_to_multi_assign(listnodep,
            &copyc, copyv, &insc, insv, &updc, updv, &delc, delv, &vdelc, vdelv,
            &srcobjc, srcobjv, &attrupdc, attrupdv, interp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    /*
     * Print constraint expressions
     */
    if (RDB_apply_constraints(insc, insv, updc, updv, delc, delv, vdelc, vdelv,
            copyc, copyv, put_constraint_expr,
            &Duro_get_var, interp, ecp, &interp->txnp->tx) != RDB_OK)
        goto error;
    fflush(stdout);

    for (i = 0; i < srcobjc; i++)
        RDB_destroy_obj(&srcobjv[i], ecp);

    return RDB_OK;

error:
    for (i = 0; i < srcobjc; i++)
        RDB_destroy_obj(&srcobjv[i], ecp);

    return RDB_ERROR;
}
