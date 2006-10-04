/*
 * Copyright (C) 2005-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 *
 * $Id$
 */

#include "rdb.h"
#include "internal.h"
#include "qresult.h"
#include <string.h>
#include <assert.h>
#include <dli/tabletostr.h>

static int
qr_matching_tuple(RDB_qresult *qrp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_object tpl;

    *resultp = RDB_FALSE;
    RDB_init_obj(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        if (_RDB_tuple_matches(tplp, &tpl, ecp, txp, resultp) != RDB_OK) {
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
    int i;
    RDB_object tpl;

    /*
     * Pick attributes which are attributes of the table
     */
    RDB_init_obj(&tpl);
    for (i = 0; i < texp->var.op.argc - 1; i++) {
        char *attrname = RDB_obj_string(&texp->var.op.argv[i + 1]->var.obj);
        RDB_object *attrp = RDB_tuple_get(tplp, attrname);
        if (attrp != NULL) {
            if (RDB_tuple_set(&tpl, attrname, attrp, ecp) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
        }
    }
    if (_RDB_expr_matching_tuple(texp->var.op.argv[0], &tpl, ecp, txp, resultp)
            != RDB_OK) {
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
_RDB_expr_matching_tuple(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_expression *texp;

    if (exp->kind == RDB_EX_OBJ) {
        return _RDB_matching_tuple(&exp->var.obj, tplp, ecp, txp, resultp);
    }
    if (exp->kind == RDB_EX_TBP) {
        return _RDB_matching_tuple(exp->var.tbref.tbp, tplp, ecp, txp, resultp);
    }
    if (exp->kind == RDB_EX_RO_OP && strcmp (exp->var.op.name, "PROJECT") == 0) {
        return project_matching(exp, tplp, ecp, txp, resultp);
    }

    texp = _RDB_optimize_expr(exp, 0, NULL, ecp, txp);
    if (texp == NULL)
        return RDB_ERROR;
    
    qrp = _RDB_expr_qresult(texp, ecp, txp);
    if (qrp == NULL)
        goto error;

    ret = qr_matching_tuple(qrp, tplp, ecp, txp, resultp);
    if (ret != RDB_OK) {
        goto error;
    }
    _RDB_drop_qresult(qrp, ecp, txp);
    return RDB_drop_expr(texp, ecp);

error:
    if (qrp != NULL)
        _RDB_drop_qresult(qrp, ecp, txp);
    RDB_drop_expr(texp, ecp);
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
    RDB_qresult *qrp = _RDB_table_qresult(tbp, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    ret = qr_matching_tuple(qrp, tplp, ecp, txp, resultp);
    if (ret != RDB_OK) {
        _RDB_drop_qresult(qrp, ecp, txp);
        return ret;
    }
    return _RDB_drop_qresult(qrp, ecp, txp);
}

static RDB_bool
index_covers_tuple(_RDB_tbindex *indexp, const RDB_object *tplp)
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
    _RDB_tbindex *indexp;
    int ret;
    RDB_object tpl;
    RDB_object **objpv;

    if (tbp->var.tb.stp == NULL) {
        /* Physical table representation has not been created, so table is empty */
        *resultp = RDB_FALSE;
        return RDB_OK;
    }
     
    /*
     * Search for a unique index that covers the tuple
     */
    for (i = 0;
            i < tbp->var.tb.stp->indexc
                    && !(tbp->var.tb.stp->indexv[i].unique
                        && index_covers_tuple(&tbp->var.tb.stp->indexv[i], tplp));
            i++);
    if (i >= tbp->var.tb.stp->indexc) {
        return matching_ts(tbp, tplp, ecp, txp, resultp);
    }
    indexp = &tbp->var.tb.stp->indexv[i];
    objpv = malloc(sizeof(RDB_object *) * indexp->attrc);
    if (objpv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    for (i = 0; i < indexp->attrc; i++) {
        objpv[i] = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
    }
    RDB_init_obj(&tpl);
    ret = _RDB_get_by_uindex(tbp, objpv, indexp, tbp->typ->var.basetyp, ecp,
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
    if (indexp->attrc < tbp->typ->var.basetyp->var.tuple.attrc) {
        ret = _RDB_tuple_matches(tplp, &tpl, ecp, txp, resultp);
    } else {
        *resultp = RDB_TRUE;
    }

cleanup:
    free(objpv);
    RDB_destroy_obj(&tpl, ecp);
    return ret;   
}

int
_RDB_matching_tuple(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    assert(tbp->kind == RDB_OB_TABLE);
	if (tbp->var.tb.exp == NULL) {
		return stored_matching(tbp, tplp, ecp, txp, resultp);
	}
	return _RDB_expr_matching_tuple(tbp->var.tb.exp, tplp, ecp, txp, resultp);
}
