/*
 * qr_tclose.c
 *
 *  Created on: 30.09.2012
 *      Author: Rene Hartmann
 *
 *  RDB_qresult functions for TCLOSE operator.
 */

#include "qr_tclose.h"
#include "internal.h"
#include "qresult.h"
#include "obj/objinternal.h"

int
RDB_tclose_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    qrp->exp = exp;
    qrp->nested = RDB_TRUE;
    qrp->val.children.qr2p = NULL;
    qrp->val.children.tpl_valid = RDB_FALSE;

    /* Create qresult for child */
    qrp->val.children.qrp = RDB_expr_qresult(qrp->exp->def.op.args.firstp,
            ecp, txp);
    if (qrp->val.children.qrp == NULL)
        return RDB_ERROR;
    return RDB_OK;
}

/*
 * Generate expression *tbp WHERE attr1name = (to be set later)
 */
static RDB_expression *
tclose_step_tuples_expr(RDB_object *tbp, const char *attr1name,
        RDB_exec_context *ecp)
{
    RDB_expression *cmpvarp, *cmpvalp, *condp, *wherep, *tbrefp;

    cmpvarp = RDB_var_ref(attr1name, ecp);
    if (cmpvarp == NULL)
        return NULL;

    cmpvalp = RDB_obj_to_expr(NULL, ecp);
    if (cmpvalp == NULL) {
        RDB_del_expr(cmpvarp, ecp);
        return NULL;
    }

    condp = RDB_ro_op("=", ecp);
    if (condp == NULL) {
        RDB_del_expr(cmpvarp, ecp);
        RDB_del_expr(cmpvalp, ecp);
        return NULL;
    }

    RDB_add_arg(condp, cmpvarp);
    RDB_add_arg(condp, cmpvalp);

    wherep = RDB_ro_op("where", ecp);
    if (wherep == NULL) {
        RDB_del_expr(condp, ecp);
        return NULL;
    }

    tbrefp = RDB_table_ref(tbp, ecp);
    if (tbrefp == NULL) {
        RDB_del_expr(condp, ecp);
        RDB_del_expr(wherep, ecp);
        return NULL;
    }

    RDB_add_arg(wherep, tbrefp);
    RDB_add_arg(wherep, condp);

    return wherep;
}

static int
tclose_insert(RDB_object *chtbp, RDB_object *tplp,
        RDB_object *dsttbp, RDB_bool *insertedp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_bool contains;
    int ret;

    *insertedp = RDB_FALSE;

    /* Check if the arg table contains the tuple */
    RDB_table_contains(chtbp, tplp, ecp, txp, &contains);
    if (!contains) {
        ret = RDB_insert(dsttbp, tplp, ecp, NULL);
        if (ret == RDB_OK) {
            *insertedp = RDB_TRUE;
        } else {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_ELEMENT_EXISTS_ERROR) {
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        }
    }
    return RDB_OK;
}

static int
tclose_step(RDB_qresult *qrp, RDB_object *tplp,
        RDB_object *chtbp,
        RDB_expression *wherechp, RDB_expression *wherematp,
        RDB_bool *insertedp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *wqrp;
    RDB_object tpl;
    const char *attr1name = qrp->matp->typ->def.basetyp
            ->def.tuple.attrv[0].name;
    const char *attr2name = qrp->matp->typ->def.basetyp
            ->def.tuple.attrv[1].name;

    /*
     * For a tuple {attr1 a, attr2 b}:
     * Find tuples {attr1 b, attr2 x} and insert tuple {attr1 a, attr2 x}
     */

    *insertedp = RDB_FALSE;

    /* Set attribute value where condition */
    if (RDB_copy_obj(RDB_expr_obj(wherechp->def.op.args.firstp
            ->nextp->def.op.args.firstp->nextp),
            RDB_tuple_get(tplp, attr2name), ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    wqrp = RDB_expr_qresult(wherechp, ecp, txp);
    if (wqrp == NULL)
        goto error;
    while (RDB_next_tuple(wqrp, &tpl, ecp, txp) == RDB_OK) {
        RDB_bool inserted;

        if (RDB_tuple_set(&tpl, attr1name,
                RDB_tuple_get(tplp, attr1name), ecp) != RDB_OK)
            goto error;
        if (tclose_insert(chtbp, &tpl, qrp->matp, &inserted,
                ecp, txp) != RDB_OK)
            goto error;
        if (inserted)
            *insertedp = RDB_TRUE;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        goto error;
    RDB_clear_err(ecp);

    RDB_del_qresult(wqrp, ecp, txp);
    wqrp = NULL;

    /* Set where condition */
    if (RDB_copy_obj(RDB_expr_obj(wherematp->def.op.args.firstp
            ->nextp->def.op.args.firstp->nextp),
            RDB_tuple_get(tplp, attr2name), ecp) != RDB_OK) {
        goto error;
    }

    wqrp = RDB_expr_qresult(wherematp, ecp, txp);
    if (wqrp == NULL)
        goto error;
    while (RDB_next_tuple(wqrp, &tpl, ecp, txp) == RDB_OK) {
        RDB_bool inserted;

        /* Set new tuple to be inserted */
        if (RDB_tuple_set(&tpl, attr1name,
                RDB_tuple_get(tplp, attr1name), ecp) != RDB_OK) {
            goto error;
        }

        if (tclose_insert(chtbp, &tpl, qrp->matp, &inserted,
                ecp, txp) != RDB_OK)
            goto error;
        if (inserted)
            *insertedp = RDB_TRUE;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        goto error;
    RDB_clear_err(ecp);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_del_qresult(wqrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    if (wqrp != NULL)
        RDB_del_qresult(wqrp, ecp, txp);
    return RDB_ERROR;
}

int
RDB_next_tclose_tuple(RDB_qresult *qrp, RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_type *reltyp;
    RDB_object tpl;
    RDB_bool inserted;
    RDB_qresult *lqrp;
    RDB_object *chtbp;
    RDB_expression *chexp;
    RDB_expression *wherechp = NULL;
    RDB_expression *wherematp = NULL;

    /*
     * First return the tuples from the child table.
     * When all tuples from the child table have been returned,
     * generate the additional tuples, store them in qrp->matp
     * and return them from there.
     */

    if (qrp->matp != NULL) {
        return RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
    }

    ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
    if (ret == RDB_OK) {
        return RDB_OK;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);

    /* Create temporary table */

    qrp->matp = RDB_new_obj(ecp);
    if (qrp->matp == NULL)
        return RDB_ERROR;

    reltyp = RDB_expr_type(qrp->exp->def.op.args.firstp, NULL, NULL, NULL, ecp, txp);
    if (reltyp == NULL) {
        return RDB_ERROR;
    }
    reltyp = RDB_dup_nonscalar_type(reltyp, ecp);
    if (reltyp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);

    if (RDB_init_table_i(qrp->matp, NULL, RDB_FALSE, reltyp, 0, NULL,
            0, NULL, RDB_TRUE, NULL, ecp) != RDB_OK) {
        RDB_del_nonscalar_type(reltyp, ecp);
        return RDB_ERROR;
    }

    /*
     * Perform the transitive closure,
     * storing the result tuples in qrp->matp
     */

    chexp = RDB_dup_expr(qrp->exp->def.op.args.firstp, ecp);
    if (chexp == NULL)
        goto error;
    chtbp = RDB_expr_to_vtable(chexp, ecp, txp);
    if (chtbp == NULL) {
        RDB_del_expr(chexp, ecp);
        goto error;
    }

    wherechp = tclose_step_tuples_expr(chtbp, qrp->matp->typ->def.basetyp
            ->def.tuple.attrv[0].name, ecp);
    wherematp = tclose_step_tuples_expr(qrp->matp, qrp->matp->typ->def.basetyp
            ->def.tuple.attrv[0].name, ecp);

    /* Perform rounds until no additional tuple has been inserted */
    do {
        RDB_bool inserted2;

        inserted = RDB_FALSE;

        lqrp = RDB_expr_qresult(qrp->exp->def.op.args.firstp, ecp, txp);
        if (lqrp == NULL)
            goto error;

        /* Iterate over child table */
        for(;;) {
            ret = RDB_next_tuple(lqrp, &tpl, ecp, txp);
            if (ret != RDB_OK)
                break;
            ret = tclose_step(qrp, &tpl, chtbp, wherechp, wherematp,
                    &inserted2, ecp, txp);
            if (ret != RDB_OK)
                goto error;
            if (inserted2)
                inserted = RDB_TRUE;
        }
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
            goto error;
        RDB_clear_err(ecp);

        if (RDB_del_qresult(lqrp, ecp, txp) != RDB_OK) {
            qrp = NULL;
            goto error;
        }

        /* Iterate over temporary table */
        lqrp = RDB_table_qresult(qrp->matp, ecp, txp);
        if (lqrp == NULL)
            goto error;

        for(;;) {
            ret = RDB_next_tuple(lqrp, &tpl, ecp, txp);
            if (ret != RDB_OK)
                break;
            ret = tclose_step(qrp, &tpl, chtbp, wherechp, wherematp,
                    &inserted2, ecp, txp);
            if (ret != RDB_OK)
                goto error;
            if (inserted2)
                inserted = RDB_TRUE;
        }

        RDB_del_qresult(lqrp, ecp, txp);
        lqrp = NULL;
    } while (inserted);

    /*
     * Drop original child qresult and continue with qresult over
     * temporary table
     */

    if (RDB_del_qresult(qrp->val.children.qrp, ecp, txp) != RDB_OK) {
        qrp->val.children.qrp = NULL;
        goto error;
    }

    qrp->val.children.qrp = RDB_table_qresult(qrp->matp, ecp, txp);

    RDB_destroy_obj(&tpl, ecp);
    RDB_drop_table(chtbp, ecp, txp);
    RDB_del_expr(wherechp, ecp);
    RDB_del_expr(wherematp, ecp);
    return RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);

error:
    if (lqrp != NULL)
        RDB_del_qresult(lqrp, ecp, txp);
    if (chtbp != NULL)
        RDB_drop_table(chtbp, ecp, txp);
    if (wherechp != NULL)
        RDB_del_expr(wherechp, ecp);
    if (wherematp != NULL)
        RDB_del_expr(wherematp, ecp);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_ERROR;
}
