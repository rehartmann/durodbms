/*
 * Functions for declarative integrity constraints
 *
 * Copyright (C) 2005-2009, 2011-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "transform.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include "optimize.h"
#include <gen/strfns.h>
#include <obj/objinternal.h>

#include <string.h>

static int
optimize_constr_expr(RDB_expression *exp, RDB_exec_context *ecp)
{
    if (RDB_expr_is_op(exp, "subset_of") && exp->def.op.args.firstp != NULL
            && exp->def.op.args.firstp->nextp != NULL
            && exp->def.op.args.firstp->nextp->nextp == NULL) {
        /* Convert T1 subset_of T2 to is_empty(T1 minus T2) */
        RDB_expression *minusexp;
        char *isenamp = RDB_dup_str("is_empty");
        if (isenamp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        minusexp = RDB_ro_op("minus", ecp);
        if (minusexp == NULL) {
            RDB_free(isenamp);
            return RDB_ERROR;
        }

        minusexp->def.op.args.firstp = exp->def.op.args.firstp;
        minusexp->def.op.args.lastp = exp->def.op.args.lastp;
        minusexp->nextp = NULL;
        RDB_free(exp->def.op.name);
        exp->def.op.name = isenamp;
        exp->def.op.args.firstp = exp->def.op.args.lastp = minusexp;
    }
    return RDB_OK;
}

/*
 * Add constraint to the linked list of constraints managed by dbroot
 */
static int
add_constraint(RDB_constraint *constrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_dbroot *dbrootp = RDB_tx_db(txp)->dbrootp;

    if (optimize_constr_expr(constrp->exp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_expr_resolve_tbnames(constrp->exp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    constrp->nextp = dbrootp->first_constrp;
    dbrootp->first_constrp = constrp;

    return RDB_OK;
}

/*
 * Read constraints from catalog
 */
int
RDB_read_constraints(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object constrs;
    RDB_object *tplp;
    RDB_dbroot *dbrootp = RDB_tx_db(txp)->dbrootp;

    RDB_init_obj(&constrs);

    ret = RDB_table_to_array(&constrs, dbrootp->constraints_tbp, 0, NULL,
            0, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    for (i = 0; (tplp = RDB_array_get(&constrs, i, ecp)) != NULL; i++) {
        RDB_constraint *constrp = RDB_alloc(sizeof(RDB_constraint), ecp);

        if (constrp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        constrp->name = RDB_dup_str(RDB_obj_string(RDB_tuple_get(tplp,
                "constraintname")));
        if (constrp->name == NULL) {
            RDB_free(constrp);
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        constrp->exp = RDB_bin_to_expr(RDB_tuple_get(tplp, "i_expr"), ecp,
                txp);
        if (constrp->exp == NULL) {
            RDB_free(constrp->name);
            RDB_free(constrp);
            ret = RDB_ERROR;
            goto cleanup;
        }

        /* Add to list */
        if (add_constraint(constrp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        ret = RDB_OK;
    } else {
        ret = RDB_ERROR;
    }
    if (ret == RDB_OK) {
        RDB_tx_db(txp)->dbrootp->constraints_read = RDB_TRUE;
    }

cleanup:
    RDB_destroy_obj(&constrs, ecp);
    return ret;
}

static void
expr_remove_tick(const RDB_expression *exp)
{
    size_t lpos;

    switch (exp->kind) {
    case RDB_EX_OBJ:
    case RDB_EX_TBP:
        return;
    case RDB_EX_GET_COMP:
        expr_remove_tick(exp->def.op.args.firstp);
        return;
    case RDB_EX_VAR:
        /* If last char is "'", overwrite with null byte */
        lpos = strlen (exp->def.varname) - 1;
        if (exp->def.varname[lpos] == '\'')
            exp->def.varname[lpos] = '\0';
        return;
    case RDB_EX_RO_OP:
    {
        RDB_expression *argp = exp->def.op.args.firstp;
        while (argp != NULL) {
            expr_remove_tick(argp);
            argp = argp->nextp;
        }

        return;
    }
    }
    /* Should never be reached */
    abort();
}

/** @defgroup constr Constraint functions 
 * @{
 */

/**
 * 
RDB_create_constraint creates a constraint with the name <var>name</var>
on the database the transaction specified by <var>txp</var> interacts with.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:
<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>type_mismatch_error
<dd>The *<var>constrp</var> is not of type BOOLEAN.
<dt>predicate_violation_error
<dd>The *<var>constrp</var> does not evaluate to TRUE.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_constraint(const char *name, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_bool res;
    RDB_constraint *constrp;
    RDB_expression *nexp = RDB_dup_expr(exp, ecp);
    if (nexp == NULL)
        return RDB_ERROR;

    /*
     * Remove ' from variable names, then check constraint
     */

    expr_remove_tick(nexp);
    ret = RDB_evaluate_bool(nexp, NULL, NULL, NULL, ecp, txp, &res);
    RDB_del_expr(nexp, ecp);
    if (ret != RDB_OK)
        return ret;
    if (!res) {
        RDB_raise_predicate_violation(name, ecp);
        return RDB_ERROR;
    }

    if (!RDB_tx_db(txp)->dbrootp->constraints_read) {
        if (RDB_read_constraints(ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }

    constrp = RDB_alloc(sizeof (RDB_constraint), ecp);
    if (constrp == NULL) {
        return RDB_ERROR;
    }

    constrp->exp = exp;

    constrp->name = RDB_dup_str(name);
    if (constrp->name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    /* Create constraint in catalog */
    ret = RDB_cat_create_constraint(name, exp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    /* Insert constraint into list */
    if (add_constraint(constrp, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    
    return RDB_OK;

error:
    RDB_free(constrp->name);
    RDB_free(constrp);

    return RDB_ERROR;
}

/**
 * RDB_drop_constraint deletes the constraint with the name <var>name</var>.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:
<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>not_found_error
<dd>A constraint with the name <var>name</var> could not be found.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_drop_constraint(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *condp;
    RDB_constraint *constrp;
    RDB_constraint *prevconstrp = NULL;
    RDB_dbroot *dbrootp = RDB_tx_db(txp)->dbrootp;

    /*
     * Read constraints because otherwise the RDB_delete() call
     * may read the constraint that is meant to be deleted.
     */
    if (!dbrootp->constraints_read) {
        if (RDB_read_constraints(ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    /* Delete constraint from list */
    constrp = dbrootp->first_constrp;
    while (constrp != NULL && strcmp(constrp->name, name) != 0) {
        prevconstrp = constrp;
        constrp = constrp->nextp;
    }
    if (constrp != NULL) {
        if (prevconstrp == NULL) {
            dbrootp->first_constrp = constrp->nextp;
        } else {
            prevconstrp->nextp = constrp->nextp;
        }

        RDB_del_expr(constrp->exp, ecp);
        RDB_free(constrp->name);
        RDB_free(constrp);
    }

    /* Delete constraint from catalog */
    condp = RDB_eq(RDB_var_ref("constraintname", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (condp == NULL) {
        return RDB_ERROR;
    }
    ret = RDB_delete(dbrootp->constraints_tbp, condp, ecp, txp);
    RDB_del_expr(condp, ecp);
    if (ret == 0) {
        RDB_raise_not_found(name, ecp);
        return RDB_ERROR;
    }

    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

/*@}*/

static RDB_bool
expr_refers_target(const RDB_expression *exp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int vdelc, const RDB_ma_vdelete vdelv[],
        int copyc, const RDB_ma_copy copyv[])
{
    int i;

    for (i = 0; i < insc; i++) {
        if (RDB_expr_depends_table(exp, insv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < updc; i++) {
        if (RDB_expr_depends_table(exp, updv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < delc; i++) {
        if (RDB_expr_depends_table(exp, delv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < vdelc; i++) {
        if (RDB_expr_depends_table(exp, vdelv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < copyc; i++) {
        RDB_object *tbp = RDB_expr_obj((RDB_expression *) exp);
        if (tbp != NULL && tbp->kind == RDB_OB_TABLE) {
            if (tbp != NULL && RDB_expr_depends_table(exp, tbp))
                return RDB_TRUE;
        }
    }

    return RDB_FALSE;
}

static RDB_expression *
replace_updattrs(RDB_expression *exp, int updc, RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *uexp, *hexp;

    uexp = RDB_ro_op("update", ecp);
    if (uexp == NULL)
        return NULL;
    RDB_add_arg(uexp, exp);

    for (i = 0; i < updc; i++) {
        hexp = RDB_string_to_expr(updv[i].name, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(uexp, hexp);
        hexp = RDB_dup_expr(updv[i].exp, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(uexp, hexp);
    }
    return uexp;

error:
    RDB_del_expr(uexp, ecp);
    return NULL;
}

static RDB_expression *
replace_targets_real_ins(RDB_object *tbp, const RDB_ma_insert *insp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exp, *argp;
    RDB_type *tbtyp;

    exp = RDB_ro_op("union", ecp);
    if (exp == NULL) {
        return NULL;
    }
    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_obj_to_expr(NULL, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    tbtyp = RDB_dup_nonscalar_type(RDB_obj_type(tbp), ecp);
    if (tbtyp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    if (RDB_init_table_from_type(RDB_expr_obj(argp), NULL,
            tbtyp, 0, NULL, 0, NULL, ecp) != RDB_OK) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    /* Temporarily attach default values */
    if (tbp->val.tb.default_map != NULL) {
        RDB_expr_obj(argp)->val.tb.default_map = tbp->val.tb.default_map;
    }
    ret = RDB_insert(RDB_expr_obj(argp), insp->objp, ecp, NULL);
    RDB_expr_obj(argp)->val.tb.default_map = NULL;
    if (ret != RDB_OK) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    return exp;
}

static RDB_expression *
replace_targets_real_upd(RDB_object *tbp, const RDB_ma_update *updp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
   RDB_expression *exp;
   RDB_expression *wexp, *condp, *ncondp, *refexp;
   RDB_expression *uexp = NULL;

    if (updp->condp == NULL) {
        return replace_updattrs(RDB_table_ref(tbp, ecp),
                updp->updc, updp->updv, ecp, txp);
    }

    exp = RDB_ro_op("union", ecp);
    if (exp == NULL) {
        return NULL;
    }

    uexp = RDB_ro_op("where", ecp);
    if (uexp == NULL) {
        goto error;
    }

    refexp = RDB_table_ref(tbp, ecp);
    if (refexp == NULL) {
        goto error;
    }
    RDB_add_arg(uexp, refexp);

    condp = RDB_dup_expr(updp->condp, ecp);
    if (condp == NULL)
        return NULL;
    RDB_add_arg(uexp, condp);

    wexp = replace_updattrs(uexp, updp->updc, updp->updv, ecp, txp);
    if (wexp == NULL) {
        goto error;
    }
    uexp = NULL;
    RDB_add_arg(exp, wexp);

    wexp = RDB_ro_op("where", ecp);
    if (wexp == NULL) {
        goto error;
    }
    RDB_add_arg(exp, wexp);

    /*
     * *tbp cannot be transient, because a transient table must not
     * appear in both a target and a constraint (it would have to be copied)
     */
    refexp = RDB_table_ref(tbp, ecp);
    if (refexp == NULL) {
        goto error;
    }
    RDB_add_arg(wexp, refexp);

    ncondp = RDB_ro_op("not", ecp);
    if (ncondp == NULL) {
        goto error;
    }
    RDB_add_arg(wexp, ncondp);

    condp = RDB_dup_expr(updp->condp, ecp);
    if (condp == NULL)
        goto error;
    RDB_add_arg(ncondp, condp);

    return exp;

error:
    RDB_del_expr(exp, ecp);
    if (uexp != NULL)
        RDB_del_expr(uexp, ecp);

    return NULL;
}

static RDB_expression *
replace_targets_real_vdel(RDB_object *tbp, const RDB_ma_vdelete *vdelp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exp, *argp;
    RDB_type *tbtyp;

    /* Create <Dest table> MINUS <Source> */

    exp = RDB_ro_op("minus", ecp);
    if (exp == NULL) {
        return NULL;
    }
    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_obj_to_expr(NULL, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    tbtyp = RDB_dup_nonscalar_type(RDB_obj_type(tbp), ecp);
    if (tbtyp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    if (RDB_init_table_from_type(RDB_expr_obj(argp), NULL,
            tbtyp, 0, NULL, 0, NULL, ecp) != RDB_OK) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        RDB_del_expr(exp, ecp);
        return NULL;
    }

    ret = RDB_insert(RDB_expr_obj(argp), vdelp->objp, ecp, NULL);
    RDB_expr_obj(argp)->val.tb.default_map = NULL;
    if (ret != RDB_OK) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    return exp;
}

static RDB_expression *
replace_targets_real(RDB_object *tbp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int vdelc, const RDB_ma_vdelete vdelv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *exp;

    for (i = 0; i < insc; i++) {
        if (insv[i].tbp == tbp) {
            return replace_targets_real_ins(tbp, &insv[i], ecp, txp);
        }
    }

    for (i = 0; i < updc; i++) {
        if (updv[i].tbp == tbp) {
            return replace_targets_real_upd(tbp, &updv[i], ecp, txp);
        }
    }

    for (i = 0; i < delc; i++) {
        if (delv[i].tbp == tbp) {
            if (delv[i].condp != NULL) {
                /* Create <Table> WHERE NOT <condition> */
                RDB_expression *argp, *nexp;
                exp = RDB_ro_op("where", ecp);
                if (exp == NULL)
                    return NULL;
                argp = RDB_table_ref(tbp, ecp);
                if (argp == NULL) {
                    RDB_del_expr(exp, ecp);
                    return NULL;
                }
                RDB_add_arg(exp, argp);

                nexp = RDB_ro_op("not", ecp);
                if (nexp == NULL) {
                    RDB_del_expr(exp, ecp);
                    return NULL;
                }
                argp = RDB_dup_expr(delv[i].condp, ecp);
                if (argp == NULL) {
                    RDB_del_expr(exp, ecp);
                    return NULL;
                }
                RDB_add_arg(nexp, argp);
                RDB_add_arg(exp, nexp);
            } else {
                /* condition is NULL - table will become empty */
                RDB_type *tbtyp = RDB_dup_nonscalar_type(tbp->typ, ecp);
                if (tbtyp == NULL)
                    return NULL;

                exp = RDB_obj_to_expr(NULL, ecp);
                if (exp == NULL) {
                    RDB_del_nonscalar_type(tbtyp, ecp);
                    return NULL;
                }
                if (RDB_init_table_from_type(RDB_expr_obj(exp), NULL, tbtyp,
                        0, NULL, 0, NULL, ecp) != RDB_OK) {
                    RDB_del_nonscalar_type(tbtyp, ecp);
                    return NULL;
                }
            }
            return exp;
        }
    }

    for (i = 0; i < vdelc; i++) {
        if (vdelv[i].tbp == tbp) {
            return replace_targets_real_vdel(tbp, &vdelv[i], ecp, txp);
        }
    }

    for (i = 0; i < copyc; i++) {
        if (copyv[i].dstp->kind == RDB_OB_TABLE
                && copyv[i].dstp == tbp) {
            return RDB_obj_to_expr(copyv[i].srcp, ecp);
        }
    }

    return RDB_table_ref(tbp, ecp);
}

/*
 * Create a copy of exp where child tables are replaced
 * by the assignment sources.
 */
static RDB_expression *
replace_targets(RDB_expression *exp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int vdelc, const RDB_ma_vdelete vdelv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *newexp;
    RDB_object *tbp;
    size_t lpos;

    switch (exp->kind) {
    case RDB_EX_GET_COMP:
        newexp = replace_targets(exp->def.op.args.firstp, insc, insv,
                updc, updv, delc, delv, vdelc, vdelv, copyc, copyv, ecp, txp);
        if (newexp == NULL)
            return NULL;
        return RDB_expr_comp(newexp, exp->def.op.name, ecp);
    case RDB_EX_RO_OP:
    {
        RDB_expression *hexp;
        RDB_expression *argp;

        newexp = RDB_ro_op(exp->def.op.name, ecp);
        argp = exp->def.op.args.firstp;
        while (argp != NULL) {
            hexp = replace_targets(argp,
                    insc, insv, updc, updv, delc, delv, vdelc, vdelv,
                    copyc, copyv, ecp, txp);
            if (hexp == NULL)
                return NULL;
            RDB_add_arg(newexp, hexp);
            argp = argp->nextp;
        }
        return newexp;
    }
    case RDB_EX_OBJ:
        return RDB_obj_to_expr(&exp->def.obj, ecp);
    case RDB_EX_TBP:
        if (exp->def.tbref.tbp->val.tb.exp == NULL) {
            return replace_targets_real(exp->def.tbref.tbp,
                    insc, insv, updc, updv, delc, delv, vdelc, vdelv,
                    copyc, copyv, ecp, txp);
        }
        return replace_targets(exp->def.tbref.tbp->val.tb.exp, insc, insv,
                updc, updv, delc, delv, vdelc, vdelv, copyc, copyv, ecp, txp);
    case RDB_EX_VAR:
        /*
         * Support for transition constraints:
         * If the table name ends with "'", remove that character and read the table
         */
         lpos = strlen(exp->def.varname) - 1;
         if (exp->def.varname[lpos] == '\'') {
             char *tbname = RDB_dup_str(exp->def.varname);
             if (tbname == NULL) {
                 RDB_raise_no_memory(ecp);
                 return NULL;
             }
             tbname[lpos] = '\0';
             tbp = RDB_get_table(tbname, ecp, txp);
             RDB_free(tbname);
             if (tbp != NULL)
                 return RDB_table_ref(tbp, ecp);
         }
         return RDB_var_ref(exp->def.varname, ecp);
    }
    abort();
}

/*
 * If the constraint is of the form is_empty(table), return table
 */
static RDB_expression *
get_empty(RDB_expression *exp)
{
    if (exp->kind == RDB_EX_RO_OP
            && exp->def.op.args.firstp != NULL
            && exp->def.op.args.firstp->nextp == NULL
            && strcmp(exp->def.op.name, "is_empty") == 0) {
        RDB_expression *eexp = exp->def.op.args.firstp;
        if (eexp->kind == RDB_EX_RO_OP
                && strcmp(eexp->def.op.name, "project") == 0) {
            eexp = eexp->def.op.args.firstp;
        }
        return eexp;
    }
    return NULL;
}

int
RDB_apply_constraints_i(int ninsc, const RDB_ma_insert ninsv[],
        int nupdc, const RDB_ma_update nupdv[],
        int ndelc, const RDB_ma_delete ndelv[],
        int nvdelc, const RDB_ma_vdelete nvdelv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_apply_constraint_fn *applyfnp,
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_constraint *constrp;
    RDB_dbroot *dbrootp = RDB_tx_db(txp)->dbrootp;

    /*
     * Read constraints from DB
     */
    if (!dbrootp->constraints_read) {
        if (RDB_read_constraints(ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    constrp = dbrootp->first_constrp;
    while (constrp != NULL) {
        /*
         * Check if the constraint refers to an assignment target
         */
        if (expr_refers_target(constrp->exp, ninsc, ninsv, nupdc, nupdv,
                ndelc, ndelv, nvdelc, nvdelv, copyc, copyv)) {
            RDB_expression *empty_tbexp;
            RDB_expression *opt_check_exp;
            RDB_expression *res_check_exp;

            /*
             * Replace target tables
             */
            RDB_expression *check_exp = replace_targets(constrp->exp,
                    ninsc, ninsv, nupdc, nupdv, ndelc, ndelv, nvdelc, nvdelv,
                    copyc, copyv, ecp, txp);
            if (check_exp == NULL) {
                return RDB_ERROR;
            }

            if (RDB_env_trace(RDB_db_env(RDB_tx_db(txp))) > 0) {
                fprintf(stderr, "Checking constraint %s\n", constrp->name);
            }

            /*
             * If the constraint declares a table to be empty, get that table
             */
            empty_tbexp = get_empty(constrp->exp);

            /* Resolve variables */
            res_check_exp = RDB_expr_resolve_varnames(check_exp, getfn, getarg, ecp, txp);
            RDB_del_expr(check_exp, ecp);
            if (res_check_exp == NULL) {
                return RDB_ERROR;
            }

            opt_check_exp = RDB_optimize_expr(res_check_exp, 0, NULL,
                    empty_tbexp, ecp, txp);
            RDB_del_expr(res_check_exp, ecp);
            if (opt_check_exp == NULL) {
                return RDB_ERROR;
            }
            ret = (*applyfnp) (opt_check_exp, constrp->name, ecp, txp);
            if (ret != RDB_OK) {
                RDB_del_expr(opt_check_exp, ecp);
                return ret;
            }
            if (RDB_del_expr(opt_check_exp, ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
        constrp = constrp->nextp;
    }
    return RDB_OK;
}
