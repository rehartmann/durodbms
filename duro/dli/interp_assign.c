/*
 * Interpreter assignment functions
 *
 * Copyright (C) 2012, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "interp_assign.h"
#include "interp_core.h"
#include "interp_eval.h"
#include "exparse.h"
#include <rel/tostr.h>

#include <string.h>

static RDB_object *
resolve_target(const RDB_expression *exp, Duro_interp *interp, RDB_exec_context *ecp)
{
    const char *varname;
    const char *opname = RDB_expr_op_name(exp);

    if (RDB_expr_kind(exp) == RDB_EX_TUPLE_ATTR) {
        RDB_object idobj;
        RDB_object *resp;

        RDB_init_obj(&idobj);
        if (RDB_expr_attr_qid(exp, &idobj, ecp) != RDB_OK) {
            RDB_destroy_obj(&idobj, ecp);
            return NULL;
        }
        resp = Duro_lookup_var(RDB_obj_string(&idobj), interp, ecp);
        RDB_destroy_obj(&idobj, ecp);
        return resp;
    }

    if (opname != NULL) {
        if (strcmp(opname, "[]") == 0
                && RDB_expr_list_length(RDB_expr_op_args((RDB_expression *) exp)) == 2) {
            RDB_int idx;
            RDB_object idxobj;
            RDB_expr_list *arglistp = RDB_expr_op_args((RDB_expression *)exp);

            /*
             * Resolve array subscription
             */

            /* Get first argument, which must be an array */
            RDB_object *arrp = resolve_target(RDB_expr_list_get(arglistp, 0), interp, ecp);
            if (arrp == NULL)
                return NULL;
            if (RDB_obj_type(arrp) == NULL
                    || !RDB_type_is_array(RDB_obj_type(arrp))) {
                RDB_raise_type_mismatch("not an array", ecp);
                return NULL;
            }

            /* Get second argument, which must be INTEGER */
            RDB_init_obj(&idxobj);
            if (Duro_evaluate_retry(RDB_expr_list_get(arglistp, 1), interp, ecp,
                    &idxobj) != RDB_OK) {
                RDB_destroy_obj(&idxobj, ecp);
                return NULL;
            }
            if (RDB_obj_type(&idxobj) != &RDB_INTEGER) {
                RDB_raise_type_mismatch("array index must be INTEGER", ecp);
                RDB_destroy_obj(&idxobj, ecp);
                return NULL;
            }
            idx = RDB_obj_int(&idxobj);
            RDB_destroy_obj(&idxobj, ecp);
            return RDB_array_get(arrp, idx, ecp);
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
    RDB_type *typ = Duro_expr_type_retry(RDB_expr_list_get(RDB_expr_op_args(dstexp), 0), interp, ecp);
    if (typ == NULL)
        return RDB_ERROR;

    possrep = RDB_comp_possrep(typ, RDB_expr_op_name(dstexp));
    if (possrep == NULL) {
        RDB_raise_name(RDB_expr_op_name(dstexp), ecp);
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

    i = comp_idx(possrep, RDB_expr_op_name(dstexp));
    if (Duro_evaluate_retry(srcexp, interp, ecp, &argv[i]) != RDB_OK)
        goto error;
    argpv[i] = &argv[i];

    /* Search for other the_ assignments of the same destination */
    while (nodep->nextp != NULL) {
        /* Skip comma */
        nodep = nodep->nextp->nextp;
        if (nodep->xdata != NULL) {
            RDB_expression *fdstexp = nodep->xdata;
            if (RDB_expr_kind(fdstexp) == RDB_EX_GET_COMP) {
                RDB_bool iseq;
                if (RDB_expr_equals(RDB_expr_list_get(RDB_expr_op_args(dstexp), 0),
                        RDB_expr_list_get(RDB_expr_op_args(fdstexp), 0), ecp,
                        interp->txnp != NULL ? &interp->txnp->tx : NULL, &iseq)
                        != RDB_OK) {
                    goto error;
                }
                if (iseq) {
                    i = comp_idx(possrep, RDB_expr_op_name(fdstexp));
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
                    if (Duro_evaluate_retry(srcexp, interp, ecp,
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
            RDB_init_obj(&argv[i]);
        RDB_free(argv);
    }
    return RDB_OK;

error:
    if (argpv != NULL)
        RDB_free(argpv);
    if (argv != NULL) {
        for(i = 0; i < possrep->compc; i++)
            RDB_init_obj(&argv[i]);
        RDB_free(argv);
    }
    return RDB_ERROR;
}

static int
node_to_copy(RDB_ma_copy *copyp, RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_expression *dstexp = nodep->xdata;
    RDB_expression *srcexp = RDB_parse_node_expr(
            nodep->val.children.firstp->nextp->nextp,
            ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srcexp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_expr_kind(dstexp) == RDB_EX_GET_COMP) {
        return comp_node_to_copy(copyp, nodep, dstexp, srcexp, interp, ecp);
    }

    if (Duro_evaluate_retry(srcexp, interp, ecp, copyp->srcp) != RDB_OK) {
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

    if (Duro_evaluate_retry(srcexp, interp, ecp, copyp->srcp) != RDB_OK) {
        return RDB_ERROR;
    }

    return RDB_OK;

error:
    RDB_del_expr(srcexp, ecp);
    return RDB_ERROR;
}

static int
node_to_insert(RDB_ma_insert *insp, RDB_parse_node *nodep, Duro_interp *interp,
        int flags, RDB_exec_context *ecp)
{
    RDB_object idobj;
    RDB_expression *srcexp;

    RDB_init_obj(&idobj);

    if (RDB_parse_node_qid(nodep, &idobj, ecp) != RDB_OK) {
        RDB_destroy_obj(&idobj, ecp);
        return RDB_ERROR;
    }

    insp->tbp = Duro_lookup_var(RDB_obj_string(&idobj), interp, ecp);
    RDB_destroy_obj(&idobj, ecp);
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

    if (Duro_evaluate_retry(srcexp, interp, ecp, insp->objp) != RDB_OK) {
        return RDB_ERROR;
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
        if (nodep == NULL)
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
    RDB_object idobj;

    RDB_init_obj(&idobj);

    if (RDB_parse_node_qid(nodep, &idobj, ecp) != RDB_OK) {
        RDB_destroy_obj(&idobj, ecp);
        return RDB_ERROR;
    }

    delp->tbp = Duro_lookup_var(RDB_obj_string(&idobj), interp, ecp);
    RDB_destroy_obj(&idobj, ecp);
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
        RDB_exec_context *ecp)
{
    RDB_object idobj;
    RDB_expression *srcexp;

    RDB_init_obj(&idobj);

    if (RDB_parse_node_qid(nodep, &idobj, ecp) != RDB_OK) {
        RDB_destroy_obj(&idobj, ecp);
        return RDB_ERROR;
    }

    delp->tbp = Duro_lookup_var(RDB_obj_string(&idobj), interp, ecp);
    RDB_destroy_obj(&idobj, ecp);
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

    if (Duro_evaluate_retry(srcexp, interp, ecp, delp->objp) != RDB_OK) {
        return RDB_ERROR;
    }

    return RDB_OK;
}

/*
 * Check if the first child of nodep is an operator invocation with one argument
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
    if (RDB_expr_kind(exp) == RDB_EX_GET_COMP)
        return exp;
    opname = RDB_expr_op_name(exp);
    if (opname == NULL)
        return NULL;
    return RDB_expr_list_length(RDB_expr_op_args(exp)) == 1 ? exp : NULL;
}

static int
exec_length_assign(const RDB_parse_node *nodep, const RDB_expression *argexp,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_type *arrtyp;
    RDB_expression *srcexp;
    RDB_object *arrp;
    RDB_object lenobj;
    RDB_int len;
    RDB_int olen;

    if (nodep->nextp != NULL) {
        RDB_raise_syntax("only single assignment of array length permitted", ecp);
        return RDB_ERROR;
    }
    arrp = resolve_target(argexp, interp, ecp);
    if (arrp == NULL)
        return RDB_ERROR;

    arrtyp = RDB_obj_type(arrp);
    if (arrtyp == NULL || !RDB_type_is_array(arrtyp)) {
        RDB_raise_syntax("unsupported assignment", ecp);
        return RDB_ERROR;
    }
    olen = RDB_array_length(arrp, ecp);
    if (olen < 0)
        return RDB_ERROR;

    RDB_init_obj(&lenobj);
    srcexp = RDB_parse_node_expr(nodep->val.children.firstp->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srcexp == NULL)
        return RDB_ERROR;
    if (Duro_evaluate_retry(srcexp, interp, ecp, &lenobj) != RDB_OK) {
        RDB_destroy_obj(&lenobj, ecp);
        return RDB_ERROR;
    }
    if (RDB_obj_type(&lenobj) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("array length must be INTEGER", ecp);
        RDB_destroy_obj(&lenobj, ecp);
        return RDB_ERROR;
    }
    len = RDB_obj_int(&lenobj);
    RDB_destroy_obj(&lenobj, ecp);
    if (RDB_set_array_length(arrp, len, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    /* Initialize new elements */
    if (len > olen) {
        int i;
        RDB_type *basetyp = RDB_base_type(arrtyp);
        for (i = olen; i < len; i++) {
            RDB_object *elemp = RDB_array_get(arrp, i, ecp);
            if (elemp == NULL)
                return RDB_ERROR;

            if (Duro_init_obj(elemp, basetyp, interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL) != RDB_OK)
                return RDB_ERROR;
        }
    }

    return RDB_OK;
}

/*
 * Execute assignment to THE_XXX(...)
 */
static int
exec_the_assign_set(const RDB_parse_node *nodep, const RDB_expression *opexp,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *argp;
    RDB_expression *srcexp;
    RDB_object srcobj;
    RDB_expression *argexp = RDB_expr_list_get(RDB_expr_op_args((RDB_expression *) opexp), 0);

    argp = resolve_target(argexp, interp, ecp);
    if (argp == NULL)
        return RDB_ERROR;

    srcexp = RDB_parse_node_expr(nodep->val.children.firstp->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (srcexp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&srcobj);
    if (Duro_evaluate_retry(srcexp, interp, ecp, &srcobj) != RDB_OK) {
        RDB_destroy_obj(&srcobj, ecp);
        return RDB_ERROR;
    }

    ret = RDB_obj_set_property(argp, RDB_expr_op_name(opexp), &srcobj,
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
                                interp, ecp) != RDB_OK) {
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
            const char *opname = RDB_expr_op_name(opexp);
            if (opname != NULL) {
                if (strcmp(opname, "length") == 0) {
                    return exec_length_assign(nodep,
                            RDB_expr_list_get(RDB_expr_op_args((RDB_expression *) opexp), 0), interp, ecp);
                }
            }
            if (RDB_expr_kind(opexp) == RDB_EX_GET_COMP)
                return exec_the_assign_set(nodep, opexp, interp, ecp);
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
            copyc, copyv, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;

    if (RDB_parse_get_interactive()) {
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
            copyc, copyv, put_constraint_expr, ecp, &interp->txnp->tx) != RDB_OK)
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
