/*
 * $Id$
 *
 * Copyright (C) 2005-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
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
    RDB_expression *rexp;
    RDB_expression **expv;

    /*
     * Add 'updated' attributes
     */

    expv = malloc(sizeof(RDB_expression *) * (updc * 2 + 1));
    if (expv == NULL)
        return NULL;
    expv[0] = exp;
    for (i = 0; i < updc; i++) {
    	expv[1 + i * 2] = NULL;
        expv[2 + i * 2] = NULL;
    }
    for (i = 0; i < updc; i++) {
    	expv[1 + i * 2] = RDB_dup_expr(updv[i].exp, ecp);
        if (expv[1 + i * 2] == NULL)
            goto cleanup;
        expv[2 + i * 2] = prefixed_string_expr(updv[i].name, ecp);
        if (expv[2 + i * 2] == NULL)
            goto cleanup;
    }
    rexp = RDB_ro_op("EXTEND", 1 + updc * 2, expv, ecp);
    if (rexp == NULL)
        goto cleanup;

    /*
     * Remove old attributes
     */
    expv[0] = rexp;
    for (i = 0; i < updc; i++) {
        expv[1 + i] = RDB_string_to_expr(updv[i].name, ecp);
        if (expv[1 + i] == NULL)
            goto cleanup;
    }
    rexp = RDB_ro_op("REMOVE", updc + 1, expv, ecp);
    if (rexp == NULL)
        goto cleanup;

    /*
     * Rename new attributes
     */
    expv[0] = rexp;
    for (i = 0; i < updc; i++) {
    	expv[1 + i * 2] = prefixed_string_expr(updv[i].name, ecp);
        expv[2 + i * 2] = RDB_string_to_expr(updv[i].name, ecp);
    }
    rexp = RDB_ro_op("RENAME", 1 + updc * 2, expv, ecp);

cleanup:
    free(expv);

    return rexp;
}

static RDB_expression *
replace_targets_real_ins(RDB_object *tbp, const RDB_ma_insert *insp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (insp->tbp == tbp) {
    	RDB_expression *exp;
        RDB_object *tb1p;

        tb1p = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
                0, NULL, ecp, NULL);
        if (tb1p == NULL)
            return NULL;
        ret = RDB_insert(tb1p, insp->tplp, ecp, NULL);
        if (ret != RDB_OK)
            return NULL;
        exp = RDB_ro_op_va("UNION", ecp, RDB_table_ref_to_expr(tbp, ecp),
                RDB_table_ref_to_expr(tb1p, ecp),
                (RDB_expression *) NULL);
        if (exp == NULL) {
            RDB_drop_table(tb1p, ecp, NULL);
            return NULL;
        }
        return exp;
    }
    return RDB_table_ref_to_expr(tbp, ecp);
}

static RDB_expression *
replace_targets_real_upd(RDB_object *tbp, const RDB_ma_update *updp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
	RDB_expression *exp;
    RDB_expression *cond2p = NULL;
    RDB_object *utbp = updp->tbp;

    if (utbp == tbp) {
        if (updp->condp == NULL && cond2p == NULL) {
            return replace_updattrs(RDB_table_ref_to_expr(tbp, ecp),
                    updp->updc, updp->updv, ecp, txp);
        } else {
            RDB_expression *ucondp, *nucondp, *tcondp;
            RDB_expression *uexp, *nuexp;

            /*
             * Create condition for the updated part
             */
            if (updp->condp != NULL) {
                ucondp = RDB_dup_expr(updp->condp, ecp);
                if (ucondp == NULL)
                    return NULL;
                if (cond2p != NULL) {
                    tcondp = RDB_dup_expr(cond2p, ecp);
                    if (tcondp == NULL) {
                        RDB_drop_expr(cond2p, ecp);
                        return NULL;
                    }
                    cond2p = tcondp;
                    tcondp = RDB_ro_op_va("AND", ecp, ucondp, cond2p,
                            (RDB_expression *) NULL);
                    if (tcondp == NULL) {
                        RDB_drop_expr(ucondp, ecp);
                        RDB_drop_expr(cond2p, ecp);
                        return NULL;
                    }
                    ucondp = tcondp;
                }
            } else {
                ucondp = RDB_dup_expr(cond2p, ecp);
                if (ucondp == NULL)
                    return NULL;
            }

            /*
             * Create condition for not updated part
             */
            tcondp = RDB_dup_expr(ucondp, ecp);
            if (tcondp == NULL) {
                RDB_drop_expr(ucondp, ecp);
                return NULL;
            }
            nucondp = RDB_ro_op_va("NOT", ecp, tcondp, (RDB_expression *) NULL);
            if (nucondp == NULL) {
                RDB_drop_expr(tcondp, ecp);
                return NULL;
            }

            /*
             * Create selections and merge them by 'union'
             */

            uexp = RDB_ro_op_va("WHERE", ecp,
                    RDB_table_ref_to_expr(tbp, ecp), ucondp, (RDB_expression *) NULL);
            if (uexp == NULL) {
                return NULL;
            }

            nuexp = RDB_ro_op_va("WHERE", ecp,
                    RDB_table_ref_to_expr(tbp, ecp), nucondp, (RDB_expression *) NULL);
            if (nuexp == NULL) {
                return NULL;
            }

            exp = replace_updattrs(uexp, updp->updc, updp->updv, ecp, txp);
            if (tbp == NULL) {
                return NULL;
            }
            uexp = exp;
                
            exp = RDB_ro_op_va("UNION", ecp, uexp, nuexp, (RDB_expression *) NULL);
            if (exp == NULL) {
                return NULL;
            }
        }
    }
    return exp;
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
    RDB_object *tb1p;

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
	        	return RDB_ro_op_va("MINUS", ecp,
	        	        RDB_table_ref_to_expr(tbp, ecp),
	        	        RDB_ro_op_va("WHERE", ecp,
	        	                RDB_table_ref_to_expr(tbp, ecp),
	        	                RDB_dup_expr(delv[i].condp, ecp),
	        	                (RDB_expression *) NULL),
	        	        (RDB_expression *) NULL);
            }                
            /* Expression is NULL - table will become empty */
            tb1p = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
                    0, NULL, ecp, NULL);
            if (tb1p == NULL)
                return NULL;
            return RDB_table_ref_to_expr(tb1p, ecp);
        }
    }

    for (i = 0; i < copyc; i++) {
        if (copyv[i].dstp->kind == RDB_OB_TABLE
                && copyv[i].dstp == tbp) {
            return RDB_obj_to_expr(copyv[i].srcp, ecp);
        }
    }

    return RDB_table_ref_to_expr(tbp, ecp);
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
            RDB_expression **argexpv = (RDB_expression **)
                    malloc(sizeof (RDB_expression *) * exp->var.op.argc);

            if (argexpv == NULL)
                return NULL;

            for (i = 0; i < exp->var.op.argc; i++) {
                argexpv[i] = replace_targets(exp->var.op.argv[i],
                        insc, insv, updc, updv, delc, delv,
                        copyc, copyv, ecp, txp);
                if (argexpv[i] == NULL)
                    return NULL;
            }
            newexp = RDB_ro_op(exp->var.op.name, exp->var.op.argc, argexpv,
                    ecp);
            free(argexpv);
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
            return RDB_expr_var(exp->var.varname, ecp);
    }
    abort();
}

static void
concat_inslists (insert_node **dstpp, insert_node *srcp)
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
concat_updlists (update_node **dstpp, update_node *srcp)
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
        ret = RDB_evaluate(exp->var.op.argv[i], tplp, ecp, txp, &val);
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
        ret = RDB_evaluate_bool(exp->var.op.argv[1], tplp, ecp, txp, &b);
        if (ret != RDB_OK)
            return RDB_ERROR;
        if (!b) {
            RDB_raise_predicate_violation("SELECT predicate violation",
                    ecp);
            return RDB_ERROR;
        }
        return resolve_insert_expr(exp->var.op.argv[0], tplp, insnpp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        return resolve_insert_expr(exp->var.op.argv[0], tplp, insnpp, ecp, txp);
    }
#ifdef REMOVED
    if (strcmp(exp->var.op.name, "JOIN") == 0) {
        if (RDB_table_contains(insp->tbp->var.join.tb1p, insp->tplp,
                ecp, txp, &b) != RDB_OK) {
            return RDB_ERROR;
        }
        if (RDB_table_contains(insp->tbp->var.join.tb2p, insp->tplp,
                ecp, txp, &b2) != RDB_OK) {
            return RDB_ERROR;
        }

        /*
         * If both 'subtables' contain the tuple, the insert fails
         */
        if (b && b2) {
            RDB_raise_element_exists("tuple is already in table", ecp);
            return RDB_ERROR;
        }

        /*
         * Insert the tuple into the table(s) which do not contain it
         */
        *inslpp = NULL;
        if (!b) {
            ins.tbp = insp->tbp->var.join.tb1p;
            ins.tplp = insp->tplp;
            ret = resolve_insert(tbp, tplp, inslpp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        }
        if (!b2) {
            insert_node *hinsnp;

            ins.tbp = insp->tbp->var.join.tb2p;
            ins.tplp = insp->tplp;
            ret = resolve_insert(&ins, &hinsnp, ecp, txp);
            if (ret != RDB_OK) {
                if (*inslpp != NULL)
                    del_inslist(*inslpp, ecp);
                return RDB_ERROR;
            }
            if (*inslpp == NULL) {
                *inslpp = hinsnp;
            } else {
                concat_inslists(inslpp, hinsnp);
            }
        }
        return RDB_OK;
    }
#endif
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

static update_node *
new_update_node(const RDB_ma_update *updp, RDB_exec_context *ecp)
{
    update_node *nupdnp = malloc(sizeof (update_node));
    if (nupdnp == NULL)
        return NULL;
    if (updp->condp == NULL) {
        nupdnp->upd.condp = NULL;
    } else {
        nupdnp->upd.condp = RDB_dup_expr(updp->condp, ecp);
        if (nupdnp->upd.condp == NULL) {
            free(nupdnp);
            return NULL;
        }
    }
    nupdnp->upd.tbp = updp->tbp;
    nupdnp->upd.updc = updp->updc;
    nupdnp->upd.updv = updp->updv;
    nupdnp->nextp = NULL;
    return nupdnp;
}

static int
resolve_update(const RDB_ma_update *updp, update_node **updnpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
/*
    RDB_ma_update upd;
    update_node *updnp;
    int ret;
*/
   	if (updp->tbp->var.tb.exp == NULL) {
       *updnpp = new_update_node(updp, ecp);
       if (*updnpp == NULL)
            return RDB_ERROR;
       return RDB_OK;
    }
    RDB_raise_not_supported("resolve_update mit virtueller Tabelle", ecp);
    return RDB_ERROR;

#ifdef NIX
    switch (updp->tbp->kind) {
        case RDB_TB_REAL:
            *updnpp = new_update_node(updp, ecp);
            if (*updnpp == NULL)
                return RDB_ERROR;
            return RDB_OK;
        case RDB_TB_SELECT:
            upd.tbp = updp->tbp->var.select.tbp;
            upd.condp = updp->condp;
            upd.updc = updp->updc;
            upd.updv = updp->updv;
            ret = resolve_update(&upd, updnpp, ecp, txp);
            if (ret != RDB_OK)
                return ret;

            updnp = *updnpp;
            while (updnp != NULL) {
                if (updnp->upd.condp == NULL) {
                    updnp->upd.condp = RDB_dup_expr(updp->tbp->var.select.exp,
                            ecp);
                    if (updnp->upd.condp == NULL) {
                        del_updlist(*updnpp, ecp);
                        return RDB_ERROR;
                    }
                } else {
                    RDB_expression *ncondp;
                    RDB_expression *hcondp = RDB_dup_expr(
                            updp->tbp->var.select.exp, ecp);
                    if (hcondp == NULL) {
                        del_updlist(*updnpp, ecp);
                        return RDB_ERROR;
                    }
                    ncondp = RDB_ro_op_va("AND", ecp, hcondp, updnp->upd.condp,
                            (RDB_expression *) NULL);
                    if (ncondp == NULL) {
                        RDB_drop_expr(hcondp, ecp);
                        del_updlist(*updnpp, ecp);
                        return RDB_ERROR;
                    }                        
                    updnp->upd.condp = ncondp;
                }
                updnp = updnp->nextp;
            }
            return RDB_OK;
        case RDB_TB_PROJECT:
            upd.tbp = updp->tbp->var.project.tbp;
            upd.condp = updp->condp;
            upd.updc = updp->updc;
            upd.updv = updp->updv;
            return resolve_update(&upd, updnpp, ecp, txp);
        default: ;            
    }
    RDB_raise_not_supported(
            "Update is not supported for this virtual table", ecp);
    return RDB_ERROR;
#endif
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
                ncondp = RDB_ro_op_va("AND", ecp, hcondp, delnp->del.condp,
                        (RDB_expression *) NULL);
                if (ncondp == NULL) {
                    RDB_drop_expr(hcondp, ecp);
                    del_dellist(delnp, ecp);
                    return RDB_ERROR;
                }                        
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

        delnp = *delnpp;
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

#ifdef NIX
    switch (delp->tbp->kind) {
        case RDB_TB_REAL:
            *delnpp = new_delete_node(delp, ecp);
            if (*delnpp == NULL)
                return RDB_ERROR;
            return RDB_OK;
        case RDB_TB_SELECT:
            del.tbp = delp->tbp->var.select.tbp;
            del.condp = delp->condp;
            ret = resolve_delete(&del, delnpp, ecp, txp);
            if (ret != RDB_OK)
                return ret;

            delnp = *delnpp;
            while (delnp != NULL) {
                if (delnp->del.condp == NULL) {
                    delnp->del.condp = RDB_dup_expr(delp->tbp->var.select.exp,
                            ecp);
                    if (delnp->del.condp == NULL) {
                        del_dellist(*delnpp, ecp);
                        return RDB_ERROR;
                    }
                } else {
                    RDB_expression *ncondp;
                    RDB_expression *hcondp = RDB_dup_expr(
                            delp->tbp->var.select.exp, ecp);
                    if (hcondp == NULL) {
                        del_dellist(*delnpp, ecp);
                        return RDB_ERROR;
                    }
                    ncondp = RDB_ro_op_va("AND", ecp, hcondp, delnp->del.condp,
                            (RDB_expression *) NULL);
                    if (ncondp == NULL) {
                        RDB_drop_expr(hcondp, ecp);
                        del_dellist(*delnpp, ecp);
                        return RDB_ERROR;
                    }                        
                    delnp->del.condp = ncondp;
                }
                delnp = delnp->nextp;
            }
            return RDB_OK;
        case RDB_TB_PROJECT:
            del.tbp = delp->tbp->var.project.tbp;
            del.condp = delp->condp;
            return resolve_delete(&del, delnpp, ecp, txp);
        case RDB_TB_RENAME:
            del.tbp = delp->tbp->var.rename.tbp;
            del.condp = delp->condp;
            ret = resolve_delete(&del, delnpp, ecp, txp);
            if (ret != RDB_OK)
                return ret;
            delnp = *delnpp;
            while (delnp != NULL) {
                if (delnp->del.condp != NULL) {
                    ret = _RDB_invrename_expr(delnp->del.condp,
                            delp->tbp->var.rename.renc,
                            delp->tbp->var.rename.renv, ecp);
                    if (ret != RDB_OK) {
                        del_dellist(*delnpp, ecp);
                        return RDB_ERROR;
                    }
                }
                delnp = delnp->nextp;
            }
            return RDB_OK;
        case RDB_TB_EXTEND:
            del.tbp = delp->tbp->var.extend.tbp;
            del.condp = delp->condp;
            ret = resolve_delete(&del, delnpp, ecp, txp);
            if (ret != RDB_OK)
                return ret;
            delnp = *delnpp;
            while (delnp != NULL) {
                if (delnp->del.condp != NULL) {
                    ret = _RDB_resolve_extend_expr(&delnp->del.condp,
                            delp->tbp->var.extend.attrc,
                            delp->tbp->var.extend.attrv, ecp);
                    if (ret != RDB_OK) {
                        del_dellist(*delnpp, ecp);
                        return ret;
                    }
                }
                delnp = delnp->nextp;
            }
            return RDB_OK;
        case RDB_TB_SEMIMINUS:
        case RDB_TB_SEMIJOIN:
        case RDB_TB_UNION:
        case RDB_TB_JOIN:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_WRAP:
        case RDB_TB_UNWRAP:
        case RDB_TB_GROUP:
        case RDB_TB_UNGROUP:
        case RDB_TB_SDIVIDE:
            RDB_raise_not_supported(
                    "Delete is not supported for this virtual table", ecp);
            return RDB_ERROR;
    }
    abort();
#endif
}

static RDB_int
do_update(const RDB_ma_update *updp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *tbexp, *exp, *nexp;

    if (updp->tbp->var.tb.exp == NULL && updp->condp == NULL) {
        return _RDB_update_real(updp->tbp, NULL, updp->updc, updp->updv,
                ecp, txp);
    }
    tbexp = updp->tbp->var.tb.exp;
    if (tbexp == NULL) {
        tbexp = RDB_table_ref_to_expr(updp->tbp, ecp);
        if (tbexp == NULL)
            return RDB_ERROR;
    }
    
    if (updp->condp != NULL) {
        /*
         * Build WHERE expression
         */
        exp = RDB_ro_op("WHERE", 2, NULL, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        RDB_add_arg(exp, tbexp);
        RDB_add_arg(exp, updp->condp);
    } else {
        exp = tbexp;
    }

    nexp = _RDB_optimize_expr(exp, 0, NULL, ecp, txp);
    if (nexp == NULL)
        return RDB_ERROR;

    if (nexp->kind == RDB_EX_TBP) {
        return _RDB_update_real(nexp->var.tbref.tbp, NULL, updp->updc, updp->updv,
                ecp, txp);
    }
    if (nexp->kind == RDB_EX_RO_OP && strcmp (nexp->var.op.name, "WHERE") == 0
            && nexp->var.op.argv[0]->kind == RDB_EX_TBP) {
        return _RDB_update_real(nexp->var.op.argv[0]->var.tbref.tbp,
                nexp->var.op.argv[1], updp->updc, updp->updv, ecp, txp);
    }
    RDB_raise_not_supported("Unsupported update", ecp);
    return RDB_ERROR;

#ifdef NIX
    /* !! drop select */
    _RDB_free_table(tbp, ecp);

    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    if (ntbp->var.select.tbp->kind == RDB_TB_SELECT) {
        condp = ntbp->var.select.exp;
        dtbp = ntbp->var.select.tbp;
    } else {
        condp = NULL;
        dtbp = ntbp;
    }

    if (dtbp->var.select.tbp->var.project.indexp != NULL) {
        if (dtbp->var.select.tbp->var.project.indexp->idxp == NULL) {
            rcount = _RDB_update_select_pindex(dtbp, condp,
                    updp->updc, updp->updv, ecp, txp);
        } else {
            rcount = _RDB_update_select_index(dtbp, condp,
                    updp->updc, updp->updv, ecp, txp);
        }
    } else {
        rcount = _RDB_update_real(ntbp->var.select.tbp->var.project.tbp,
                ntbp->var.select.exp, updp->updc, updp->updv, ecp, txp);
    }
    RDB_drop_table(ntbp, ecp, NULL);
    return rcount;
#endif
}

static RDB_int
do_delete(const RDB_ma_delete *delp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *tbexp, *exp, *nexp;

    if (delp->tbp->var.tb.exp == NULL && delp->condp == NULL) {
        return _RDB_delete_real(delp->tbp, NULL, ecp, txp);
    }

    tbexp = delp->tbp->var.tb.exp;
    if (tbexp == NULL) {
        tbexp = RDB_table_ref_to_expr(delp->tbp, ecp);
        if (tbexp == NULL)
            return RDB_ERROR;
    }

    if (delp->condp != NULL) {
        exp = RDB_ro_op("WHERE", 2, NULL, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        RDB_add_arg(exp, tbexp);
        RDB_add_arg(exp, delp->condp);
    } else {
        exp = tbexp;
    }

    nexp = _RDB_optimize_expr(exp, 0, NULL, ecp, txp);
    if (nexp == NULL)
        return RDB_ERROR;

    /* !! drop select */
    if (nexp->kind == RDB_EX_TBP) {
        return _RDB_delete_real(nexp->var.tbref.tbp, NULL, ecp, txp);
    }
    if (nexp->kind == RDB_EX_RO_OP && strcmp (nexp->var.op.name, "WHERE") == 0
            && nexp->var.op.argv[0]->kind == RDB_EX_TBP) {
        return _RDB_delete_real(nexp->var.op.argv[0]->var.tbref.tbp,
                nexp->var.op.argv[1], ecp, txp);
    }
    RDB_raise_not_supported("Unsupported delete", ecp);
    return RDB_ERROR;

#ifdef REMOVED
    if (delp->condp != NULL) {
        tbp = _RDB_select(delp->tbp, delp->condp, ecp);
        if (tbp == NULL)
            return RDB_ERROR;
    } else {
        tbp = delp->tbp;
    }

    ret = _RDB_optimize(tbp, 0, NULL, ecp, txp, &ntbp);

    /* drop select */
    _RDB_free_table(tbp, ecp);

    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    if (ntbp->var.select.tbp->kind == RDB_TB_SELECT) {
        condp = ntbp->var.select.exp;
        dtbp = ntbp->var.select.tbp;
    } else {
        condp = NULL;
        dtbp = ntbp;
    }

    if (dtbp->var.select.tbp->var.project.indexp != NULL) {
        if (dtbp->var.select.tbp->var.project.indexp->unique) {
            rcount = _RDB_delete_select_uindex(dtbp, condp, ecp, txp);
        } else {
            rcount = _RDB_delete_select_index(dtbp, condp, ecp, txp);
        }
    } else {
        rcount = _RDB_delete_real(ntbp->var.select.tbp->var.project.tbp,
                ntbp->var.select.exp, ecp, txp);
    }
    RDB_drop_table(ntbp, ecp, NULL);
    return rcount;
#endif
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
    if (geninsnp == NULL)
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
            ret = resolve_update(&updv[i], &updnp, ecp, txp);
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
    if (genupdnp == NULL)
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
    if (gendelnp == NULL)
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
                ret = RDB_evaluate_bool(newexp, NULL, ecp, txp, &b);
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

    /* Start subtransaction, if there is more than one assignment */
    if (ninsc + nupdc + ndelc + copyc > 1) {
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
    if (atxp == &subtx) {
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
