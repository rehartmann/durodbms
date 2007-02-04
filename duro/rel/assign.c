/*
 * $Id$
 *
 * Copyright (C) 2005-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Functions for assignment operations (insert, update, delete, copy),
 * including multiple assignment and DB/table constraint checking
 */

#include "rdb.h"
#include "update.h"
#include "delete.h"
#include "insert.h"
#include "optimize.h"
#include "internal.h"
#include <gen/strfns.h>

#include <string.h>
#include <assert.h>

static RDB_bool
expr_refers_target(const RDB_expression *exp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[])
{
    int i;

    for (i = 0; i < insc; i++) {
        if (_RDB_expr_table_depend(exp, insv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < updc; i++) {
        if (_RDB_expr_table_depend(exp, updv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < delc; i++) {
        if (_RDB_expr_table_depend(exp, delv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < copyc; i++) {
    	RDB_object *tbp = RDB_expr_obj((RDB_expression *) exp);
        if (tbp != NULL && tbp->kind == RDB_OB_TABLE) {
            if (tbp != NULL && _RDB_expr_table_depend(exp, tbp))
                return RDB_TRUE;
        }
    }

    return RDB_FALSE;
}

typedef struct insert_node {
    RDB_ma_insert ins;
    struct insert_node *nextp;
} insert_node;

typedef struct update_node {
    RDB_ma_update upd;
    struct update_node *nextp;
} update_node;

typedef struct delete_node {
    RDB_ma_delete del;
    struct delete_node *nextp;
} delete_node;

static RDB_expression *prefixed_string_expr(char *str, RDB_exec_context *ecp)
{
	RDB_expression *exp;
	char *nstr = malloc(strlen(str) + 2);
	if (nstr == NULL) {
		RDB_raise_no_memory(ecp);
	    return NULL;
	}
    nstr[0] = '$';
	strcpy(nstr + 1, str);
	exp = RDB_string_to_expr(nstr, ecp);
	free(nstr);
	return exp;
}

static RDB_expression *
replace_updattrs(RDB_expression *exp, int updc, RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *rexp, *xexp, *hexp;

    /*
     * Add 'updated' attributes
     */

    rexp = xexp = RDB_ro_op("EXTEND", 1 + updc * 2, ecp);
    if (rexp == NULL)
        return NULL;
    RDB_add_arg(rexp, exp);
    for (i = 0; i < updc; i++) {
        hexp = RDB_dup_expr(updv[i].exp, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(rexp, hexp);
        hexp = prefixed_string_expr(updv[i].name, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(rexp, hexp);
    }

    /*
     * Remove old attributes
     */
    hexp = RDB_ro_op("REMOVE", updc + 1, ecp);
    if (hexp == NULL)
        goto error;
    RDB_add_arg(hexp, rexp);
    rexp = hexp;
    for (i = 0; i < updc; i++) {
        hexp = RDB_string_to_expr(updv[i].name, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(rexp, hexp);
    }

    /*
     * Rename new attributes
     */
    hexp = RDB_ro_op("RENAME", 1 + updc * 2, ecp);
    if (hexp == NULL)
        goto error;
    RDB_add_arg(hexp, rexp);
    rexp = hexp;
    for (i = 0; i < updc; i++) {
        hexp = prefixed_string_expr(updv[i].name, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(rexp, hexp);
        hexp = RDB_string_to_expr(updv[i].name, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(rexp, hexp);
    }
    return rexp;

error:
    /* Prevent exp from being dropped */
    xexp->var.op.argv[0] = NULL;

    RDB_drop_expr(rexp, ecp);
    return NULL;
}

static RDB_expression *
replace_targets_real_ins(RDB_object *tbp, const RDB_ma_insert *insp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
	RDB_expression *exp, *argp;
    RDB_type *tbtyp;

    exp = RDB_ro_op("UNION", 2, ecp);
    if (exp == NULL) {
        return NULL;
    }
    argp =  RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_obj_to_expr(NULL, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return NULL;
    }
    tbtyp = RDB_dup_nonscalar_type(RDB_obj_type(tbp), ecp);
    if (tbtyp == NULL) {
        RDB_drop_expr(exp, ecp);
        return NULL;
    }
    if (RDB_init_table_from_type(RDB_expr_obj(argp), NULL,
            tbtyp, 0, NULL, ecp) != RDB_OK) {
        RDB_drop_type(tbtyp, ecp, NULL);
        RDB_drop_expr(exp, ecp);
        return NULL;
    }
    ret = RDB_insert(RDB_expr_obj(argp), insp->tplp, ecp, NULL);
    if (ret != RDB_OK) {
        RDB_drop_expr(exp, ecp);
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

    exp = RDB_ro_op("UNION", 2, ecp);
    if (exp == NULL) {
        return NULL;
    }

    uexp = RDB_ro_op("WHERE", 2, ecp);
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

    wexp = RDB_ro_op("WHERE", 2, ecp);
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

    ncondp = RDB_ro_op("NOT", 1, ecp);
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
    RDB_drop_expr(exp, ecp);
    if (uexp != NULL)
        RDB_drop_expr(uexp, ecp);

    return NULL;
}

static RDB_expression *
replace_targets_real(RDB_object *tbp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
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
                RDB_expression *argp, *wexp;
                exp = RDB_ro_op("MINUS", 2, ecp);
                if (exp == NULL)
                    return NULL;
                argp = RDB_table_ref(tbp, ecp);
                if (argp == NULL) {
                    RDB_drop_expr(exp, ecp);
                    return NULL;
                }
                RDB_add_arg(exp, argp);

                wexp = RDB_ro_op("WHERE", 2, ecp);
                if (wexp == NULL) {
                    RDB_drop_expr(exp, ecp);
                    return NULL;
                }
                argp = RDB_table_ref(tbp, ecp);
                if (argp == NULL) {
                    RDB_drop_expr(exp, ecp);
                    RDB_drop_expr(wexp, ecp);
                    return NULL;
                }
                RDB_add_arg(wexp, argp);
                argp = RDB_dup_expr(delv[i].condp, ecp);
                if (argp == NULL) {
                    RDB_drop_expr(exp, ecp);
                    RDB_drop_expr(wexp, ecp);
                    return NULL;
                }
                RDB_add_arg(wexp, argp);
                RDB_add_arg(exp, wexp);
            } else {
                /* condition is NULL - table will become empty */
                RDB_type *tbtyp = RDB_dup_nonscalar_type(tbp->typ, ecp);
                if (tbtyp == NULL)
                    return NULL;

                exp = RDB_obj_to_expr(NULL, ecp);
                if (exp == NULL) {
                    RDB_drop_type(tbtyp, ecp, NULL);
                    return NULL;
                }
                if (RDB_init_table_from_type(RDB_expr_obj(exp), NULL, tbtyp,
                        0, NULL, ecp) != RDB_OK) {
                    RDB_drop_type(tbtyp, ecp, NULL);
                    return NULL;
                }
            }
            return exp;
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
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *newexp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = replace_targets(exp->var.op.argv[0], insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (newexp == NULL)
                return NULL;
            return RDB_tuple_attr(newexp, exp->var.op.name, ecp);
        case RDB_EX_GET_COMP:
            newexp = replace_targets(exp->var.op.argv[0], insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_comp(newexp, exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            int i;
            RDB_expression *hexp;

            newexp = RDB_ro_op(exp->var.op.name, exp->var.op.argc, ecp);
            for (i = 0; i < exp->var.op.argc; i++) {
                hexp = replace_targets(exp->var.op.argv[i],
                        insc, insv, updc, updv, delc, delv,
                        copyc, copyv, ecp, txp);
                if (hexp == NULL)
                    return NULL;
                RDB_add_arg(newexp, hexp);
            }
            return newexp;
        }
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj, ecp);
        case RDB_EX_TBP:
            if (exp->var.tbref.tbp->var.tb.exp == NULL) {
                return replace_targets_real(exp->var.tbref.tbp,
                            insc, insv, updc, updv, delc, delv,
                            copyc, copyv, ecp, txp);
            }
          	return replace_targets(exp->var.tbref.tbp->var.tb.exp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
        case RDB_EX_VAR:
            return RDB_var_ref(exp->var.varname, ecp);
    }
    abort();
}

static void
concat_inslists(insert_node **dstpp, insert_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        insert_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static void
concat_updlists(update_node **dstpp, update_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        update_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static void
concat_dellists(delete_node **dstpp, delete_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        delete_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static int
check_extend_tuple(const RDB_object *tplp, const RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;

    /* Check the additional attributes */
    for (i = 1; i < exp->var.op.argc; i += 2) {
        RDB_object val;
        RDB_object *valp;
        RDB_bool iseq;
        
        valp = RDB_tuple_get(tplp, RDB_obj_string(&exp->var.op.argv[i + 1]->var.obj));
        if (valp == NULL) {
            RDB_raise_invalid_argument("invalid EXTEND attribute", ecp);
            return RDB_ERROR;
        }
        RDB_init_obj(&val);
        ret = RDB_evaluate(exp->var.op.argv[i], &_RDB_tpl_get, (void *) tplp,
                ecp, txp, &val);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&val, ecp);
            return ret;
        }
        ret = RDB_obj_equals(&val, valp, ecp, txp, &iseq);
        RDB_destroy_obj(&val, ecp);
        if (ret != RDB_OK)
            return ret;
        if (!iseq) {
            RDB_raise_predicate_violation("EXTEND predicate violation", ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static insert_node *
new_insert_node(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp)
{
    int ret;

    insert_node *insnp = malloc(sizeof (insert_node));
    if (insnp == NULL)
        return NULL;
    insnp->ins.tplp = malloc(sizeof(RDB_object));
    if (insnp->ins.tplp == NULL) {
        free(insnp);
        return NULL;
    }
    RDB_init_obj(insnp->ins.tplp);
    ret = _RDB_copy_tuple(insnp->ins.tplp, tplp, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(insnp->ins.tplp, ecp);
        free(insnp->ins.tplp);
        free(insnp);
        return NULL;
    }
    insnp->ins.tbp = tbp;
    insnp->nextp = NULL;
    return insnp;
}

static void
del_inslist(insert_node *insnp, RDB_exec_context *ecp)
{
    insert_node *hinsnp;

    while (insnp != NULL) {
        hinsnp = insnp->nextp;

        if (insnp->ins.tplp != NULL) {
            RDB_destroy_obj(insnp->ins.tplp, ecp);
            free(insnp->ins.tplp);
        }
        free(insnp);
        insnp = hinsnp;
    }    
}

static void
del_updlist(update_node *updnp, RDB_exec_context *ecp)
{
    update_node *hupdnp;

    while (updnp != NULL) {
        hupdnp = updnp->nextp;

        if (updnp->upd.condp != NULL) {
            RDB_drop_expr(updnp->upd.condp, ecp);
        }
        free(updnp);
        updnp = hupdnp;
    }    
}

static void
del_dellist(delete_node *delnp, RDB_exec_context *ecp)
{
    delete_node *hdelnp;

    while (delnp != NULL) {
        hdelnp = delnp->nextp;

        if (delnp->del.condp != NULL) {
            RDB_drop_expr(delnp->del.condp, ecp);
        }
        free(delnp);
        delnp = hdelnp;
    }    
}

static int
resolve_insert(RDB_object *tbp, const RDB_object *tplp, insert_node **insnpp,
               RDB_exec_context *, RDB_transaction *);

static int
resolve_insert_expr(RDB_expression *exp, const RDB_object *tplp,
    insert_node **insnpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    switch (exp->kind) {
        case RDB_EX_TBP:
            return resolve_insert(exp->var.tbref.tbp, tplp, insnpp, ecp, txp);
        case RDB_EX_OBJ:
            return resolve_insert(&exp->var.obj, tplp, insnpp, ecp, txp);
        case RDB_EX_RO_OP:
            break;
        default:
            RDB_raise_invalid_argument("invalid target table", ecp);
            return RDB_ERROR;
    }

    if (strcmp(exp->var.op.name, "WHERE") == 0) {
        ret = RDB_evaluate_bool(exp->var.op.argv[1], &_RDB_tpl_get, (void *) tplp,
                ecp, txp, &b);
        if (ret != RDB_OK)
            return RDB_ERROR;
        if (!b) {
            RDB_raise_predicate_violation("WHERE predicate violation",
                    ecp);
            return RDB_ERROR;
        }
        return resolve_insert_expr(exp->var.op.argv[0], tplp, insnpp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0
        || strcmp(exp->var.op.name, "REMOVE") == 0) {
        return resolve_insert_expr(exp->var.op.argv[0], tplp, insnpp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "RENAME") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        ret = _RDB_invrename_tuple(tplp, exp, ecp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        ret = resolve_insert_expr(exp->var.op.argv[0], &tpl, insnpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    if (strcmp(exp->var.op.name, "EXTEND") == 0) {
        ret = check_extend_tuple(tplp, exp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        return resolve_insert_expr(exp->var.op.argv[0], tplp, insnpp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "UNWRAP") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        ret = _RDB_invunwrap_tuple(tplp, exp, ecp, txp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        ret = resolve_insert_expr(exp->var.op.argv[0], &tpl, insnpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    if (strcmp(exp->var.op.name, "WRAP") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        ret = _RDB_invwrap_tuple(tplp, exp, ecp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        ret = resolve_insert_expr(exp->var.op.argv[0], &tpl, insnpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    RDB_raise_not_supported("insert is not supported for this kind of table",
            ecp);
    return RDB_ERROR;
}

static int
resolve_insert(RDB_object *tbp, const RDB_object *tplp, insert_node **insnpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
  	if (tbp->var.tb.exp == NULL) {
        *insnpp = new_insert_node(tbp, tplp, ecp);
        if (*insnpp == NULL)
            return RDB_ERROR;
        return RDB_OK;
    }
    
    return resolve_insert_expr(tbp->var.tb.exp, tplp, insnpp, ecp, txp);
}

static int
resolve_update(RDB_object *tbp, RDB_expression *condp,
        int updc, RDB_attr_update *updv, update_node **updnpp,
        RDB_exec_context *ecp, RDB_transaction *txp);

static update_node *
new_update_node(RDB_object *tbp, RDB_expression *condp,
        int updc, RDB_attr_update *updv, RDB_exec_context *ecp)
{
    update_node *nupdnp = malloc(sizeof (update_node));
    if (nupdnp == NULL)
        return NULL;
    if (condp == NULL) {
        nupdnp->upd.condp = NULL;
    } else {
        nupdnp->upd.condp = RDB_dup_expr(condp, ecp);
        if (nupdnp->upd.condp == NULL) {
            free(nupdnp);
            return NULL;
        }
    }
    nupdnp->upd.tbp = tbp;
    nupdnp->upd.updc = updc;
    nupdnp->upd.updv = updv;
    nupdnp->nextp = NULL;
    return nupdnp;
}

static int
resolve_update_expr(RDB_expression *texp, RDB_expression *condp,
    int updc, RDB_attr_update *updv, update_node **updnpp,
    RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (texp->kind == RDB_EX_TBP) {
        return resolve_update(texp->var.tbref.tbp, condp, updc, updv,
                updnpp, ecp, txp);
    }

    if (texp->kind != RDB_EX_RO_OP) {
        RDB_raise_not_supported("tried to update invalid virtual table", ecp);
        return RDB_ERROR;
    }

    if (strcmp(texp->var.op.name, "PROJECT") == 0
            || strcmp(texp->var.op.name, "REMOVE") == 0) {
        return resolve_update_expr(texp->var.op.argv[0],
                condp, updc, updv, updnpp, ecp, txp);
    }

    RDB_raise_not_supported(
            "Update is not supported for this virtual table", ecp);
    return RDB_ERROR;   
}

static int
resolve_update(RDB_object *tbp, RDB_expression *condp,
        int updc, RDB_attr_update *updv, update_node **updnpp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp = tbp->var.tb.exp;

   	if (exp == NULL) {
        *updnpp = new_update_node(tbp, condp, updc, updv, ecp);
        if (*updnpp == NULL)
            return RDB_ERROR;
        return RDB_OK;
    }
    return resolve_update_expr(exp, condp, updc, updv, updnpp, ecp, txp);
}

static delete_node *
new_delete_node(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *ecp)
{
    delete_node *ndelnp = malloc(sizeof (delete_node));
    if (ndelnp == NULL)
        return NULL;
    if (condp == NULL) {
        ndelnp->del.condp = NULL;
    } else {
        ndelnp->del.condp = RDB_dup_expr(condp, ecp);
        if (ndelnp->del.condp == NULL) {
            free(ndelnp);
            return NULL;
        }
    }
    ndelnp->del.tbp = tbp;
    ndelnp->nextp = NULL;
    return ndelnp;
}

static int
resolve_delete(RDB_object *tbp, RDB_expression *condp, delete_node **,
               RDB_exec_context *, RDB_transaction *);

static int
resolve_delete_expr(RDB_expression *exp, RDB_expression *condp,
        delete_node **delnpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    delete_node *delnp;

    switch (exp->kind) {
        case RDB_EX_TBP:
            return resolve_delete(exp->var.tbref.tbp, condp, delnpp, ecp, txp);
        case RDB_EX_OBJ:
            return resolve_delete(&exp->var.obj, condp, delnpp, ecp, txp);
        case RDB_EX_RO_OP:
            break;
        default:
            RDB_raise_invalid_argument("invalid target table", ecp);
            return RDB_ERROR;
    }

    if (strcmp(exp->var.op.name, "PROJECT") == 0
            || strcmp(exp->var.op.name, "REMOVE") == 0) {
        return resolve_delete_expr(exp->var.op.argv[0],
                condp, delnpp, ecp, txp);
    }

    /*
     * Add WHERE condition to all deletes
     */
    if (strcmp(exp->var.op.name, "WHERE") == 0) {
        if (resolve_delete_expr(exp->var.op.argv[0], condp, &delnp,
                ecp, txp) != RDB_OK)
            return RDB_ERROR;

        *delnpp = delnp;
        while (delnp != NULL) {
            if (delnp->del.condp == NULL) {
                delnp->del.condp = RDB_dup_expr(exp->var.op.argv[1], ecp);
                if (delnp->del.condp == NULL) {
                    del_dellist(*delnpp, ecp);
                    return RDB_ERROR;
                }
            } else {
                RDB_expression *ncondp;
                RDB_expression *hcondp = RDB_dup_expr(exp->var.op.argv[1],
                        ecp);
                if (hcondp == NULL) {
                    del_dellist(delnp, ecp);
                    return RDB_ERROR;
                }
                ncondp = RDB_ro_op("AND", 2, ecp);
                if (ncondp == NULL) {
                    RDB_drop_expr(hcondp, ecp);
                    del_dellist(delnp, ecp);
                    return RDB_ERROR;
                }
                RDB_add_arg(ncondp, hcondp);
                RDB_add_arg(ncondp, delnp->del.condp);
                delnp->del.condp = ncondp;
            }
            delnp = delnp->nextp;
        }
        return RDB_OK;
    }

    if (strcmp(exp->var.op.name, "RENAME") == 0) {
        if (resolve_delete_expr(exp->var.op.argv[0], condp, &delnp,
                ecp, txp) != RDB_OK)
            return RDB_ERROR;

        *delnpp = delnp;
        while (delnp != NULL) {
            if (delnp->del.condp != NULL) {
                if (_RDB_invrename_expr(delnp->del.condp, exp, ecp) != RDB_OK) {
                    del_dellist(*delnpp, ecp);
                    return RDB_ERROR;
                }
            }
            delnp = delnp->nextp;
        }
        return RDB_OK;
    }

    if (strcmp(exp->var.op.name, "EXTEND") == 0) {
        if (resolve_delete_expr(exp->var.op.argv[0], condp, &delnp,
                ecp, txp) != RDB_OK)
            return RDB_ERROR;

        *delnpp = delnp;
        while (delnp != NULL) {
            if (delnp->del.condp != NULL) {
                if (_RDB_resolve_extend_expr(&delnp->del.condp,
                        exp, ecp) != RDB_OK) {
                    del_dellist(*delnpp, ecp);
                    return RDB_ERROR;
                }
            }
            delnp = delnp->nextp;
        }
        return RDB_OK;
    }

    RDB_raise_not_supported("delete is not supported for this kind of table",
            ecp);
    return RDB_ERROR;   
}

static int
resolve_delete(RDB_object *tbp, RDB_expression *condp, delete_node **delnpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
	if (tbp->var.tb.exp == NULL) {
        *delnpp = new_delete_node(tbp, condp, ecp);
        if (*delnpp == NULL)
            return RDB_ERROR;
        return RDB_OK;
	}

    return resolve_delete_expr(tbp->var.tb.exp, condp, delnpp, ecp, txp);
}

/*
 * Perform update. *updp->tbp must be a real table.
 */
static RDB_int
do_update(const RDB_ma_update *updp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *tbexp, *nexp;
    RDB_expression *exp = NULL;

    if (updp->condp == NULL) {
        return _RDB_update_real(updp->tbp, NULL, updp->updc, updp->updv,
                ecp, txp);
    }

    tbexp = RDB_table_ref(updp->tbp, ecp);
    if (tbexp == NULL)
        return RDB_ERROR;

    /*
     * Build WHERE expression
     */
    exp = RDB_ro_op("WHERE", 2, ecp);
    if (exp == NULL)
        return RDB_ERROR;
    RDB_add_arg(exp, tbexp);
    RDB_add_arg(exp, updp->condp);

    nexp = _RDB_optimize_expr(exp, 0, NULL, ecp, txp);
    exp->var.op.argv[0] = NULL;
    exp->var.op.argv[1] = NULL;
    RDB_drop_expr(exp, ecp);
    RDB_drop_expr(tbexp, ecp);
    if (nexp == NULL)
        return RDB_ERROR;

    if (nexp->kind == RDB_EX_TBP) {
        ret = _RDB_update_real(nexp->var.tbref.tbp, NULL, updp->updc, updp->updv,
                ecp, txp);
        RDB_drop_expr(nexp, ecp);
        return ret;
    }
    if (nexp->kind == RDB_EX_RO_OP
            && strcmp (nexp->var.op.name, "WHERE") == 0) {
        if (nexp->var.op.optinfo.objpc > 0) {
            ret = _RDB_update_where_index(nexp, NULL, updp->updc, updp->updv,
                    ecp, txp);
            RDB_drop_expr(nexp, ecp);
            return ret;
        }

        if (nexp->var.op.argv[0]->kind == RDB_EX_RO_OP
                && strcmp(nexp->var.op.argv[0]->var.op.name, "WHERE") == 0
                && nexp->var.op.argv[0]->var.op.optinfo.objpc > 0) {
            ret = _RDB_update_where_index(nexp->var.op.argv[0],
                    nexp->var.op.argv[1], updp->updc, updp->updv,
                    ecp, txp);
            RDB_drop_expr(nexp, ecp);
            return ret;
        }

        if (nexp->var.op.argv[0]->kind == RDB_EX_TBP) {
            ret = _RDB_update_real(nexp->var.op.argv[0]->var.tbref.tbp,
                    nexp->var.op.argv[1], updp->updc, updp->updv, ecp, txp);
            RDB_drop_expr(nexp, ecp);
            return ret;
        }
    }
    RDB_drop_expr(nexp, ecp);
    RDB_raise_not_supported("Unsupported update", ecp);
    return RDB_ERROR;
}

/*
 * Perform a delete. updp->tbp must be a real table.
 */
static RDB_int
do_delete(const RDB_ma_delete *delp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *tbexp, *exp, *nexp;

    if (delp->condp == NULL) {
        return _RDB_delete_real(delp->tbp, NULL, ecp, txp);
    }

    tbexp = RDB_table_ref(delp->tbp, ecp);
    if (tbexp == NULL)
        return RDB_ERROR;

    exp = RDB_ro_op("WHERE", 2, ecp);
    if (exp == NULL)
        return RDB_ERROR;
    RDB_add_arg(exp, tbexp);
    RDB_add_arg(exp, delp->condp);

    nexp = _RDB_optimize_expr(exp, 0, NULL, ecp, txp);
    if (nexp == NULL)
        return RDB_ERROR;

    exp->var.op.argv[0] = NULL;
    exp->var.op.argv[1] = NULL;
    RDB_drop_expr(exp, ecp);
    RDB_drop_expr(tbexp, ecp);

    if (nexp->kind == RDB_EX_TBP) {
        ret = _RDB_delete_real(nexp->var.tbref.tbp, NULL, ecp, txp);
        RDB_drop_expr(nexp, ecp);
        return ret;
    }
    if (nexp->kind == RDB_EX_RO_OP && strcmp (nexp->var.op.name, "WHERE") == 0) {
        if (nexp->var.op.optinfo.objpc > 0) {
            ret = _RDB_delete_where_index(nexp, NULL, ecp, txp);
            RDB_drop_expr(nexp, ecp);
            return ret;
        }

        if (nexp->var.op.argv[0]->kind == RDB_EX_RO_OP
                && strcmp(nexp->var.op.argv[0]->var.op.name, "WHERE") == 0
                && nexp->var.op.argv[0]->var.op.optinfo.objpc > 0) {
            ret = _RDB_delete_where_index(nexp->var.op.argv[0],
                    nexp->var.op.argv[1], ecp, txp);
            RDB_drop_expr(nexp, ecp);
            return ret;
        }

        if (nexp->var.op.argv[0]->kind == RDB_EX_TBP) {
            ret = _RDB_delete_real(nexp->var.op.argv[0]->var.tbref.tbp,
                    nexp->var.op.argv[1], ecp, txp);
            RDB_drop_expr(nexp, ecp);
            return ret;
        }
    }
    RDB_drop_expr(nexp, ecp);
    RDB_raise_not_supported("Unsupported delete", ecp);
    return RDB_ERROR;
}

static int
copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *srctyp = RDB_obj_type(srcvalp);

    if (_RDB_copy_obj(dstvalp, srcvalp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (srctyp != NULL && RDB_type_is_scalar(srctyp))
        dstvalp->typ = srctyp;
    return RDB_OK;
} 

static int
resolve_inserts(int insc, const RDB_ma_insert *insv, RDB_ma_insert **ninsvp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int llen;

    /* list of generated inserts */
    insert_node *geninsnp = NULL;

    insert_node *insnp;

    for (i = 0; i < insc; i++) {
        if (insv[i].tbp->var.tb.exp != NULL) {
            ret = resolve_insert(insv[i].tbp, insv[i].tplp, &insnp, ecp, txp);
            if (ret != RDB_OK)
                goto cleanup;

            /* Add inserts to list */
            concat_inslists(&geninsnp, insnp);
        }
    }

    /*
     * If inserts have been generated, allocate new insert list
     * which consists of old and new inserts
     */

    llen = 0;
    insnp = geninsnp;
    while (insnp != NULL) {
        llen++;
        insnp = insnp->nextp;
    }

    if (llen > 0) {
        (*ninsvp) = malloc(sizeof (RDB_ma_insert) * (insc + llen));
        if (*ninsvp == NULL)
            goto cleanup;

        for (i = 0; i < insc; i++) {
            (*ninsvp)[i].tbp = insv[i].tbp;
            (*ninsvp)[i].tplp = insv[i].tplp;
        }

        insnp = geninsnp;
        while (insnp != NULL) {
            (*ninsvp)[i].tbp = insnp->ins.tbp;
            (*ninsvp)[i].tplp = insnp->ins.tplp;

            /* mark as copied */
            insnp->ins.tplp = NULL;

            insnp = insnp->nextp;
            i++;
        }
    } else {
        *ninsvp = (RDB_ma_insert *) insv;
    }
    ret = insc + llen;
    
cleanup:
    if (geninsnp != NULL)
        del_inslist(geninsnp, ecp);

    return ret;
}

static int
resolve_updates(int updc, const RDB_ma_update *updv, RDB_ma_update **nupdvp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int llen;

    /* list of generated updates */
    update_node *genupdnp = NULL;

    update_node *updnp;

    for (i = 0; i < updc; i++) {
        if (updv[i].tbp->var.tb.exp != NULL) {
            /* Convert virtual table updates to real table updates */
            ret = resolve_update(updv[i].tbp, updv[i].condp,
                    updv[i].updc, updv[i].updv, &updnp, ecp, txp);
            if (ret != RDB_OK)
                goto cleanup;

            /* Add updates to list */
            concat_updlists(&genupdnp, updnp);
        }
    }

    /*
     * If inserts have been generated, allocate new list
     * which consists of old and new updates
     */

    llen = 0;
    updnp = genupdnp;
    while (updnp != NULL) {
        llen++;
        updnp = updnp->nextp;
    }

    if (llen > 0) {
        (*nupdvp) = malloc(sizeof (RDB_ma_update) * (updc + llen));
        if (*nupdvp == NULL)
            goto cleanup;

        for (i = 0; i < updc; i++) {
            (*nupdvp)[i].tbp = updv[i].tbp;
            (*nupdvp)[i].condp = updv[i].condp;
            (*nupdvp)[i].updc = updv[i].updc;
            (*nupdvp)[i].updv = updv[i].updv;
        }

        updnp = genupdnp;
        while (updnp != NULL) {
            (*nupdvp)[i].tbp = updnp->upd.tbp;
            (*nupdvp)[i].condp = updnp->upd.condp;
            (*nupdvp)[i].updc = updnp->upd.updc;
            (*nupdvp)[i].updv = updnp->upd.updv;

            /* to prevent the expression from being dropped twice */
            updnp->upd.condp = NULL;

            updnp = updnp->nextp;
            i++;
        }
    } else {
        *nupdvp = (RDB_ma_update *) updv;
    }
    ret = updc + llen;
    
cleanup:
    if (genupdnp != NULL)
        del_updlist(genupdnp, ecp);

    return ret;
}

static int
resolve_deletes(int delc, const RDB_ma_delete *delv, RDB_ma_delete **ndelvp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int llen;

    /* list of generated deletes */
    delete_node *gendelnp = NULL;

    delete_node *delnp;

    for (i = 0; i < delc; i++) {
        if (delv[i].tbp->var.tb.exp != NULL) {
            /* Convert virtual table deletes to real table deletes */
            ret = resolve_delete(delv[i].tbp, delv[i].condp, &delnp, ecp, txp);
            if (ret != RDB_OK)
                goto cleanup;

            /* Add deletes to list */
            concat_dellists(&gendelnp, delnp);
        }
    }

    /*
     * If inserts have been generated, allocate new list
     * which consists of old and new deletes
     */

    llen = 0;
    delnp = gendelnp;
    while (delnp != NULL) {
        llen++;
        delnp = delnp->nextp;
    }

    if (llen > 0) {
        (*ndelvp) = malloc(sizeof (RDB_ma_delete) * (delc + llen));
        if (*ndelvp == NULL)
            goto cleanup;

        for (i = 0; i < delc; i++) {
            (*ndelvp)[i].tbp = delv[i].tbp;
            (*ndelvp)[i].condp = delv[i].condp;
        }

        delnp = gendelnp;
        while (delnp != NULL) {
            (*ndelvp)[i].tbp = delnp->del.tbp;
            (*ndelvp)[i].condp = delnp->del.condp;

            /* to prevent the expression from being dropped twice */
            delnp->del.condp = NULL;

            delnp = delnp->nextp;
            i++;
        }
    } else {
        *ndelvp = (RDB_ma_delete *) delv;
    }
    ret = delc + llen;
    
cleanup:
    if (gendelnp != NULL)
        del_dellist(gendelnp, ecp);

    return ret;
}

static RDB_bool copy_needs_tx(const RDB_object *dstp, const RDB_object *srcp)
{
    if (dstp->kind != RDB_OB_TABLE)
        return RDB_FALSE;
    return (RDB_bool) (dstp->var.tb.is_persistent
            || (srcp->kind != RDB_OB_TABLE
                    && srcp->var.tb.is_persistent));
}

/** @addtogroup table
 * @{
 */

/** @struct RDB_ma_insert rdb.h <rel/rdb.h>
 * Represents an insert.
 */

/** @struct RDB_ma_update rdb.h <rel/rdb.h>
 * Represents an update.
 */

/** @struct RDB_ma_delete rdb.h <rel/rdb.h>
 * Represents a delete.
 */

/**
 * Perform a number of insert, update, delete,
and copy operations in a single call.

For each of the RDB_ma_insert elements given by <var>insc</var> and <var>insv</var>,
the tuple *<var>insv</var>[i]->tplp is inserted into *<var>insv</var>[i]->tbp.

For each of the RDB_ma_update elements given by <var>updc</var> and <var>updv</var>,
the attributes given by <var>updv</var>[i]->updc and <var>updv</var>[i]->updv
of the tuples of *<var>updv</var>[i]->tbp for which *<var>updv</var>[i]->condp
evaluates to RDB_TRUE are updated.

For each of the RDB_ma_delete elements given by <var>delc</var> and <var>delv</var>,
the tuples for which *<var>delv</var>[i]->condp evaluates to RDB_TRUE
are deleted from *<var>delv</var>[i]->tbp.

For each of the RDB_ma_copy elements given by <var>copyc</var> and <var>copyv</var>,
*<var>copyv</var>[i]->scrp is copied to *<var>copyv</var>[i]->dstp.

A <strong>RDB_multi_assign</strong> call is atomic with respect to
constraint checking; a constraint violation error can only occur
if the result of <em>all</em> operations violates a constraint.

A table may not appear twice as a target in the arguments to
<strong>RDB_multi_assign</strong>, and it may not appear
as a source if it appears as a target in a previous assignment.

This means that an assignment like the following:</p>

<code>
UPDATE S WHERE S# = S#('S2') STATUS := 15,
UPDATE S WHERE S# = S#('S3') STATUS := 25;
</code>

(taken from <em>TTM</em>, chapter 6)
cannot be performed directly. It can, however, be converted to a
form like the following:</p>

<code>
UPDATE S WHERE (S# = S#('S2')) OR (S# = S#('S3'))
        STATUS := IF (S# = S#('S2')) THEN 15 ELSE 25;
</code>

The restrictions of RDB_insert(), RDB_update(), RDB_delete(),
and RDB_copy_obj()
regarding virtual target tables apply to RDB_multi_assign,
too.

<var>txp</var> must point to a running transaction execpt
if (1) no update or delete is specified, (2) only
inserts into local tables are specified, and (3)
there is no copy operation with an existing table as a target
which involves a global table as source or target.

@returns

On success, the number of tuples inserted, deleted, and updated due to
<var>insc</var>, <var>insv</var>, <var>updc</var>, <var>updv</var>,
<var>delc</var> and <var>delv</var> arguments,
plus <var>objc</var>. If an error occurred, (RDB_int) RDB_ERROR is returned.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> must point to a running transaction (see above)
but does not.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd>A table appears twice as a target.
<dt>RDB_NOT_SUPPORTED_ERROR
<dd>A table is both source and target.
<dd>A virtual table appears as a target in <var>copyv</var>.
<dt>RDB_PREDICATE_VIOLATION_ERROR
<dd>A constraint has been violated.
</dl>

The errors that can be raised by RDB_insert(),
RDB_update(), RDB_delete() and RDB_copy_obj() can also be raised.
 */
RDB_int
RDB_multi_assign(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    RDB_int rcount, cnt;
    RDB_dbroot *dbrootp;
    int ninsc;
    int nupdc;
    int ndelc;
    RDB_bool need_tx;
    RDB_transaction subtx;
    RDB_transaction *atxp = NULL;
    RDB_ma_insert *ninsv = NULL;
    RDB_ma_update *nupdv = NULL;
    RDB_ma_delete *ndelv = NULL;

    /*
     * A running transaction is required for:
     * - updates
     * - deletes
     * - copying persistent tables (except if the target is newly initialized)
     * - inserts into persistent tables
     */
    if (updc > 0 || delc > 0) {
        need_tx = RDB_TRUE;
    } else {
        for (i = 0;
             i < copyc && !copy_needs_tx(copyv[i].dstp, copyv[i].dstp);
             i++);
        need_tx = (RDB_bool) (i < copyc);
        if (!need_tx) {
            for (i = 0;
                 i < insc && !insv[i].tbp->var.tb.is_persistent;
                 i++);
            need_tx = (RDB_bool) (i < insc);
        }
    }

    if (need_tx && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Check if conditions are of type BOOLEAN
     */

    for (i = 0; i < updc; i++) {
        if (updv[i].condp != NULL) {
            if (_RDB_check_expr_type(updv[i].condp,
                    updv[i].tbp->typ->var.basetyp,
                    &RDB_BOOLEAN, ecp, txp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }

    for (i = 0; i < delc; i++) {
        if (delv[i].condp != NULL) {
            if (_RDB_check_expr_type(delv[i].condp,
                    delv[i].tbp->typ->var.basetyp,
                    &RDB_BOOLEAN, ecp, txp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }

    /*
     * Check types of updated attributes
     */
    for (i = 0; i < updc; i++) {
        for (j = 0; j < updv[i].updc; j++) {
            RDB_attr *attrp = _RDB_tuple_type_attr(
                    updv[i].tbp->typ->var.basetyp, updv[i].updv[j].name);
            if (attrp == NULL) {
                RDB_raise_attribute_not_found(updv[i].updv[j].name, ecp);
                return RDB_ERROR;
            }

            if (RDB_type_is_scalar(attrp->typ)) {
                if (_RDB_check_expr_type(updv[i].updv[j].exp,
                        updv[i].tbp->typ->var.basetyp, attrp->typ,
                        ecp, txp) != RDB_OK) {
                    return RDB_ERROR;
                }
            }
        }
    }

    /*
     * Check types of copied values
     */
    for (i = 0; i < copyc; i++) {
        RDB_type *srctyp = RDB_obj_type(copyv[i].srcp);

        if (srctyp != NULL && RDB_type_is_scalar(srctyp)) {
            /* If destination carries a value, types must match */
            if (copyv[i].dstp->kind != RDB_OB_INITIAL
                    && (copyv[i].dstp->typ == NULL
                     || !RDB_type_equals(copyv[i].srcp->typ,
                                    copyv[i].dstp->typ))) {
                RDB_raise_type_mismatch("source does not match destination",
                        ecp);
                return RDB_ERROR;
            }
        }
    }

    /*
     * Resolve virtual table assignment
     */

    ninsc = resolve_inserts(insc, insv, &ninsv, ecp, txp);
    if (ninsc == RDB_ERROR) {
        rcount = RDB_ERROR;
        ninsv = NULL;
        goto cleanup;
    }

    nupdc = resolve_updates(updc, updv, &nupdv, ecp, txp);
    if (nupdc == RDB_ERROR) {
        rcount = RDB_ERROR;
        nupdv = NULL;
        goto cleanup;
    }

    ndelc = resolve_deletes(delc, delv, &ndelv, ecp, txp);
    if (ndelc == RDB_ERROR) {
        rcount = RDB_ERROR;
        ndelv = NULL;
        goto cleanup;
    }

    /*
     * Check if the same target is assigned twice
     * or if there is a later assignment that depends on a target
     */
    for (i = 0; i < ninsc; i++) {
        for (j = i + 1; j < ninsc; j++) {
            if (ninsv[i].tbp == ninsv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        for (j = 0; j < nupdc; j++) {
            if (ninsv[i].tbp == nupdv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (nupdv[j].condp != NULL
                    && _RDB_expr_refers(nupdv[j].condp, ninsv[i].tbp)) {
                RDB_raise_not_supported("update condition depends on target", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        for (j = 0; j < ndelc; j++) {
            if (ninsv[i].tbp == ndelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (ndelv[j].condp != NULL
                    && _RDB_expr_refers(ndelv[j].condp, ninsv[i].tbp)) {
                RDB_raise_not_supported("delete condition depends on target", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && insv[i].tbp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }

            /*
             * Check if a presviously modified table is source of a copy
             */
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && _RDB_table_refers(copyv[j].srcp, insv[i].tbp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
    }
    for (i = 0; i < nupdc; i++) {
        for (j = i + 1; j < updc; j++) {
            if (nupdv[i].tbp == nupdv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (nupdv[j].condp != NULL
                    && _RDB_expr_refers(nupdv[j].condp, nupdv[i].tbp)) {
                RDB_raise_not_supported("update condition depends on target", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        for (j = 0; j < ndelc; j++) {
            if (updv[i].tbp == ndelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (ndelv[j].condp != NULL
                    && _RDB_expr_refers(ndelv[j].condp, nupdv[i].tbp)) {
                RDB_raise_not_supported("delete condition depends on target", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && updv[i].tbp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && _RDB_table_refers(copyv[j].srcp, updv[i].tbp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
    }
    for (i = 0; i < ndelc; i++) {
        for (j = i + 1; j < ndelc; j++) {
            if (ndelv[i].tbp == ndelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (ndelv[j].condp != NULL
                    && _RDB_expr_refers(ndelv[j].condp, ndelv[i].tbp)) {
                RDB_raise_not_supported("delete condition depends on target", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && ndelv[i].tbp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && _RDB_table_refers(copyv[j].srcp, delv[i].tbp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
    }

    for (i = 0; i < copyc; i++) {
        for (j = i + 1; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && copyv[i].dstp->kind == RDB_OB_TABLE
                    && copyv[i].dstp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && copyv[i].dstp->kind == RDB_OB_TABLE
                    && copyv[j].srcp->kind == RDB_OB_TABLE
                    && _RDB_table_refers(copyv[j].srcp, copyv[i].dstp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
    }

    /*
     * Check constraints
     */
    if (txp != NULL) {
        RDB_constraint *constrp;

        dbrootp = RDB_tx_db(txp)->dbrootp;

        if (!dbrootp->constraints_read) {
            if (_RDB_read_constraints(ecp, txp) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            dbrootp->constraints_read = RDB_TRUE;
        }

        constrp = dbrootp->first_constrp;
        while (constrp != NULL) {
            int ret;
            /*
             * Check if constraint refers to assignment target
             */

            /* resolved inserts/updates/... */
            if (expr_refers_target(constrp->exp, ninsc, ninsv, nupdc, nupdv,
                    ndelc, ndelv, copyc, copyv)) {
                RDB_bool b;

                /*
                 * Replace target tables
                 */
                RDB_expression *newexp = replace_targets(constrp->exp,
                        ninsc, ninsv, nupdc, nupdv, ndelc, ndelv,
                        copyc, copyv, ecp, txp);
                if (newexp == NULL) {
                    rcount = RDB_ERROR;
                    goto cleanup;
                }

                /*
                 * Check constraint
                 */
                ret = RDB_evaluate_bool(newexp, NULL, NULL, ecp, txp, &b);
                RDB_drop_expr(newexp, ecp);
                if (ret != RDB_OK) {
                    rcount = RDB_ERROR;
                    goto cleanup;
                }
                if (!b) {
                    RDB_raise_predicate_violation(constrp->name, ecp);
                    rcount = RDB_ERROR;
                    goto cleanup;
                }
            }
            constrp = constrp->nextp;
        }
    }

    /*
     * Execute assignments
     */

    /*
     * Start subtransaction, if there is more than one assignment.
     * A subtransaction is also needed with an insert into a table
     * with secondary indexes, because the insert is not atomic
     * in Berkeyley DB 4.5.
     */
    if (need_tx
            && (ninsc + nupdc + ndelc + copyc > 1
            || (ninsc == 1 && ninsv[0].tbp->var.tb.stp != NULL
                    && ninsv[0].tbp->var.tb.stp->indexc > 1))) {
        if (RDB_begin_tx(ecp, &subtx, RDB_tx_db(txp), txp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
        atxp = &subtx;
    } else {
        atxp = txp;
    }

    rcount = 0;
    for (i = 0; i < ninsc; i++) {
        if (ninsv[i].tbp->var.tb.exp == NULL) {
            if (_RDB_insert_real(ninsv[i].tbp, ninsv[i].tplp, ecp, atxp)
                    != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
    }
    for (i = 0; i < nupdc; i++) {
        if (nupdv[i].tbp->var.tb.exp == NULL) {
            cnt = do_update(&nupdv[i], ecp, atxp);
            if (cnt == RDB_ERROR) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount += cnt;
        }
    }
    for (i = 0; i < ndelc; i++) {
        if (ndelv[i].tbp->var.tb.exp == NULL) {
            cnt = do_delete(&ndelv[i], ecp, atxp);
            if (cnt == RDB_ERROR) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount += cnt;
        }
    }
    for (i = 0; i < copyc; i++) {
        if (copyv[i].dstp->kind == RDB_OB_TABLE
                && copyv[i].dstp->var.tb.exp != NULL) {
            RDB_raise_not_supported(
                    "Virtual table is copy destination", ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        if (copy_obj(copyv[i].dstp, copyv[i].srcp, ecp, atxp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
        rcount++;
    }

    /* Commit subtx, if it has been started */
    if (need_tx && (atxp == &subtx)) {
        if (RDB_commit(ecp, &subtx) != RDB_OK)
            rcount = RDB_ERROR;
        atxp = NULL;
    }

cleanup:
    /*
     * Free generated inserts, updates, deletes
     */
    if (ninsv != insv) {
        for (i = insc; i < ninsc; i++) {
            RDB_destroy_obj(ninsv[i].tplp, ecp);
            free(ninsv[i].tplp);
        }
        free(ninsv);
    }

    if (nupdv != updv) {
        for (i = updc; i < nupdc; i++) {
            if (nupdv[i].condp != NULL) {
                RDB_drop_expr(nupdv[i].condp, ecp);
            }
        }
        free(nupdv);
    }

    if (ndelv != delv) {
        for (i = delc; i < ndelc; i++) {
            if (ndelv[i].condp != NULL) {
                RDB_drop_expr(ndelv[i].condp, ecp);
            }
        }
        free(ndelv);
    }

    /* Abort subtx, if necessary */
    if (rcount == RDB_ERROR) {
        if (atxp == &subtx) {
            RDB_rollback(ecp, &subtx);
        }
    }

    return rcount;
}

/**
 * RDB_insert inserts the tuple specified by <var>tplp</var>
into the table specified by <var>tbp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

Currently, RDB_insert is not supported for virtual tables which are the result
of a UNION, MINUS, SEMIMINUS, INTERSECT, JOIN, SEMIJOIN, SUMMARIZE, DIVIDE,
GROUP, or UNGROUP.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd>The table given by <var>tbp</var> is global and <var>txp</var>
does not point to a running transaction.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd>A table attribute is missing in the tuple and no default value
was specified for that attribute.
<dt>RDB_ELEMENT_EXIST_ERROR
<dd>The tuple was already an element of the table.
<dt>RDB_KEY_VIOLATION_ERROR
<dd>Inserting the tuple would result in a table which contains
a key value twice.
<dt>RDB_PREDICATE_VIOLATION_ERROR
<dd>Inserting the tuple would result in a table which violates its
predicate.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of a tuple attribute does not match the type of the
corresponding table attribute.
<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
<dt>RDB_NOT_SUPPORTED_ERROR
<dd>RDB_insert is not supported for this type of table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_insert(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
           RDB_transaction *txp)
{
    RDB_ma_insert ins;
    RDB_int count;

    ins.tbp = tbp;
    ins.tplp = (RDB_object *) tplp;
    count = RDB_multi_assign(1, &ins, 0, NULL, 0, NULL, 0, NULL, ecp, txp);
    if (count == RDB_ERROR)
        return RDB_ERROR;
    return RDB_OK;
}

/**
 * RDB_update updates all tuples from the table specified by <var>tbp</var>
for which the expression specified by <var>exp</var> evaluates to true.
If <var>exp</var> is NULL, all tuples are updated.

If an error occurs, an error value is left in *<var>ecp</var>.

The attributes to be updated are specified by the <var>updv</var> array.
The attribute specified by the field name is set to the value
obtained by evaluating the expression specified by the field exp.

Currently, RDB_update is not supported for virtual tables except PROJECT.

@returns

The number of updated tuples in real tables if the call was successful.
A call which did not modify any tuple because no tuple matched the condition
is considered a successful call and returns zero.
If an error occurred, (RDB_int)RDB_ERROR is returned.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.

<dt>RDB_ATTRIBUTE_NOT_FOUND_ERROR
<dd>One of the attributes in <var>updv</var> does not exist in the table.
<dd>One of the expressions specified in <var>updv</var> refers to an attribute
which does not exist in the table.

<dt>RDB_ELEMENT_EXIST_ERROR
<dd>The update operation would update a tuple so that it would be equal
to a tuple which is already an element of the table.

<dt>RDB_KEY_VIOLATION_ERROR
<dd>The update operation would result in a table which contains
a key value twice.

<dt>RDB_PREDICATE_VIOLATION_ERROR
<dd>The update operation would result in a table which violates its
predicate.

<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of one of the expressions in <var>updv</var> is not the same
as the type of the corresponding table attribute.

<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
<dd>The expression specified by <var>exp</var>
refers to a non-existing operator.
<dd>One of the expressions specified in <var>updv</var>
refers to a non-existing operator.

<dt>RDB_NOT_SUPPORTED_ERROR
<dd>RDB_update is not supported for this type of table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
RDB_int
RDB_update(RDB_object *tbp, RDB_expression *condp, int updc,
           const RDB_attr_update updv[], RDB_exec_context *ecp,
           RDB_transaction *txp)
{
    RDB_ma_update upd;

    upd.tbp = tbp;
    upd.condp = condp;
    upd.updc = updc;
    upd.updv = (RDB_attr_update *) updv;
    return RDB_multi_assign(0, NULL, 1, &upd, 0, NULL, 0, NULL, ecp, txp);
}

/**
 * RDB_delete deletes all tuples from the table specified by <var>tbp</var>
for which the expression specified by <var>exp</var> evaluates to true.
If <var>exp</var> is NULL, all tuples are deleted.

If an error occurs, an error value is left in *<var>ecp</var>.

Currently, RDB_delete is not supported for virtual tables except
PROJECT, WHERE, RENAME, and EXTEND.

@returns

The number of deleted tuples, if no error occurred.
A call which did not delete any tuple because no tuple matched the condition
is considered a successful call and returns zero.
If an error occurred, (RDB_int)RDB_ERROR is returned.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd><var>exp</var> refers to an attribute which does not exist in the table.
<dt>RDB_PREDICATE_VIOLATION_ERROR
<dd>Deleting the tuples would result in a table which violates its
predicate.
<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>The definition of the table specified by <var>tbp</var> refers to a non-existing operator.
<dd>The expression specified by <var>exp</var> refers to a non-existing operator.
<dt>RDB_NOT_SUPPORTED_ERROR
<dd>RDB_delete is not supported for this type of table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
RDB_int
RDB_delete(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_ma_delete del;

    del.tbp = tbp;
    del.condp = condp;
    return RDB_multi_assign(0, NULL, 0, NULL, 1, &del, 0, NULL, ecp, txp);
}

/*@}*/
