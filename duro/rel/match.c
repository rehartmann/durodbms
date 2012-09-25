/*
 * Copyright (C) 2005-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 *
 * $Id$
 */

#include "rdb.h"
#include "qresult.h"
#include "optimize.h"
#include "internal.h"
#include "stable.h"

#include <string.h>
#include <assert.h>

static int
qr_matching_tuple(RDB_qresult *qrp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_object tpl;

    *resultp = RDB_FALSE;
    RDB_init_obj(&tpl);

    while ((ret = RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        if (RDB_tuple_matches(tplp, &tpl, ecp, txp, resultp) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        if (*resultp) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_OK;
        }
    }
    if (RDB_obj_type(&ecp->error) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&tpl, ecp);
}

static int
project_matching(RDB_expression *texp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_expression *argp;
    RDB_object tpl;

    /*
     * Pick attributes which are attributes of the table
     */
    RDB_init_obj(&tpl);
    argp = texp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        char *attrname = RDB_obj_string(&argp->def.obj);
        RDB_object *attrp = RDB_tuple_get(tplp, attrname);
        if (attrp != NULL) {
            if (RDB_tuple_set(&tpl, attrname, attrp, ecp) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
        }
        argp = argp->nextp;
    }
    if (RDB_expr_matching_tuple(texp->def.op.args.firstp, &tpl, ecp, txp,
            resultp) != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    return RDB_destroy_obj(&tpl, ecp);
}

/*
 * Check if one of the tuples in *exp matches *tplp
 * (The expression must be relation-valued)
 */
int
RDB_expr_matching_tuple(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp;

    if (exp->kind == RDB_EX_OBJ) {
        return RDB_matching_tuple(&exp->def.obj, tplp, ecp, txp, resultp);
    }
    if (exp->kind == RDB_EX_TBP) {
        return RDB_matching_tuple(exp->def.tbref.tbp, tplp, ecp, txp, resultp);
    }
    if (exp->kind == RDB_EX_RO_OP && strcmp (exp->def.op.name, "project") == 0) {
        return project_matching(exp, tplp, ecp, txp, resultp);
    }
    
    qrp = RDB_expr_qresult(exp, ecp, txp);
    if (qrp == NULL)
        goto error;

    ret = qr_matching_tuple(qrp, tplp, ecp, txp, resultp);
    if (ret != RDB_OK) {
        goto error;
    }
    return RDB_del_qresult(qrp, ecp, txp);

error:
    if (qrp != NULL)
        RDB_del_qresult(qrp, ecp, txp);
    return RDB_ERROR;
}

/*
 * Check if one of the tuples in *tbp matches *tplp
 */
int
matching_ts(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp = RDB_table_qresult(tbp, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    ret = qr_matching_tuple(qrp, tplp, ecp, txp, resultp);
    if (ret != RDB_OK) {
        RDB_del_qresult(qrp, ecp, txp);
        return ret;
    }
    return RDB_del_qresult(qrp, ecp, txp);
}

static RDB_bool
index_covers_tuple(RDB_tbindex *indexp, const RDB_object *tplp)
{
    int i;

    for (i = 0; i < indexp->attrc; i++) {
        if (RDB_tuple_get(tplp, indexp->attrv[i].attrname) == NULL)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

static int
stored_matching(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int i;
    RDB_tbindex *indexp;
    int ret;
    RDB_object tpl;
    RDB_object **objpv;

    if (tbp->val.tb.stp == NULL) {
        /* Physical table representation has not been created, so table is empty */
        *resultp = RDB_FALSE;
        return RDB_OK;
    }

    /*
     * Search for a unique index that covers the tuple
     */
    for (i = 0;
            i < tbp->val.tb.stp->indexc
                    && !(tbp->val.tb.stp->indexv[i].unique
                        && index_covers_tuple(&tbp->val.tb.stp->indexv[i], tplp));
            i++);
    if (i >= tbp->val.tb.stp->indexc) {
        /* Not found - scan *tbp for a matching tuple */
        return matching_ts(tbp, tplp, ecp, txp, resultp);
    }
    indexp = &tbp->val.tb.stp->indexv[i];
    objpv = RDB_alloc(sizeof(RDB_object *) * indexp->attrc, ecp);
    if (objpv == NULL) {
        return RDB_ERROR;
    }
    for (i = 0; i < indexp->attrc; i++) {
        objpv[i] = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
        objpv[i]->store_typ = objpv[i]->typ;
    }
    RDB_init_obj(&tpl);
    ret = RDB_get_by_uindex(tbp, objpv, indexp, tbp->typ->def.basetyp, ecp,
            txp, &tpl);
    if (ret == RDB_ERROR) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            *resultp = RDB_FALSE;
            ret = RDB_OK;
        }
        goto cleanup;
    }
    if (ret != RDB_OK) {
        goto cleanup;
    }
    if (indexp->attrc < tbp->typ->def.basetyp->def.tuple.attrc) {
        ret = RDB_tuple_matches(tplp, &tpl, ecp, txp, resultp);
    } else {
        *resultp = RDB_TRUE;
    }

cleanup:
    RDB_free(objpv);
    RDB_destroy_obj(&tpl, ecp);
    return ret;   
}

int
RDB_matching_tuple(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    assert(tbp->kind == RDB_OB_TABLE);
	if (tbp->val.tb.exp == NULL) {
		return stored_matching(tbp, tplp, ecp, txp, resultp);
	}
	return RDB_expr_matching_tuple(tbp->val.tb.exp, tplp, ecp, txp, resultp);
}
