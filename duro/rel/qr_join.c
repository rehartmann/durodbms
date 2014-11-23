/*
 * qr_join.c
 *
 *  Created on: 21.11.2014
 *      Author: Rene Hartmann
 */

#include "qr_join.h"
#include "qresult.h"
#include "qr_stored.h"
#include "stable.h"
#include "internal.h"
#include "obj/objinternal.h"

int
RDB_join_qresult(RDB_qresult *qrp, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *arg2p;

    /* Create qresult for the first table */
    qrp->exp = exp;
    qrp->nested = RDB_TRUE;
    qrp->val.children.qrp = RDB_expr_qresult(qrp->exp->def.op.args.firstp,
            ecp, txp);
    if (qrp->val.children.qrp == NULL)
        return RDB_ERROR;

    qrp->val.children.tpl_valid = RDB_FALSE;

    /* Create qresult for 2nd table, except if the primary index is used */
    arg2p = qrp->exp->def.op.args.firstp->nextp;
    if (arg2p->kind != RDB_EX_TBP || arg2p->def.tbref.indexp == NULL
            || !arg2p->def.tbref.indexp->unique) {
        qrp->val.children.qr2p = RDB_expr_qresult(arg2p, ecp, txp);
        if (qrp->val.children.qr2p == NULL) {
            RDB_del_qresult(qrp->val.children.qrp, ecp, txp);
            return RDB_ERROR;
        }
    } else {
        qrp->val.children.qr2p = NULL;
    }
    return RDB_OK;
}

static int
next_join_rename_uix(RDB_qresult *qrp, RDB_object *tplp, RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl;
    RDB_object rtpl;
    RDB_object **objpv;
    RDB_object *tbp = qrp->val.children.qr2p->exp->def.op.args.firstp
            ->def.tbref.tbp;
    RDB_bool match = RDB_FALSE;

    RDB_expression *tstexp = qrp->exp;

    puts((char *)tstexp);

    tstexp = NULL;

    objpv = RDB_alloc(sizeof(RDB_object *) * indexp->attrc, ecp);
    if (objpv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(tplp, ecp);
    RDB_init_obj(tplp);

    RDB_init_obj(&tpl);
    RDB_init_obj(&rtpl);

    do {
        ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;

        /* Read tuple from table #2 using the index */
        for (i = 0; i < indexp->attrc; i++) {
            objpv[i] = RDB_tuple_get(tplp, RDB_rename_attr(indexp->attrv[i].attrname,
                    qrp->val.children.qr2p->exp));
            objpv[i]->store_typ = objpv[i]->typ;
        }
        ret = RDB_get_by_uindex(tbp, objpv, indexp,
                tbp->typ->def.basetyp,
                ecp, txp, &tpl);
        if (ret == RDB_ERROR) {
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                RDB_clear_err(ecp);
                continue;
            }
            goto cleanup;
        }

        /* Rename attributes */
        ret = RDB_rename_tuple_ex(&rtpl, &tpl, qrp->val.children.qr2p->exp, ecp);
        if (ret != RDB_OK)
            goto cleanup;

        /* Check if the tuples match */
        ret = RDB_tuple_matches(tplp, &rtpl, ecp, txp, &match);
        if (ret != RDB_OK)
            goto cleanup;
    } while (!match);

    /* Join the tuples */
    ret = RDB_add_tuple(tplp, &rtpl, ecp, txp);

cleanup:
    RDB_free(objpv);
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&rtpl, ecp);

    return ret;
}

static int
next_join_rename_nuix(RDB_qresult *qrp, RDB_object *tplp, RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object itpl, oxtpl;
    RDB_init_obj(&itpl);
    RDB_init_obj(&oxtpl);

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->val.children.tpl_valid) {
        RDB_init_obj(&qrp->val.children.tpl);
        if (RDB_next_tuple(qrp->val.children.qrp, &qrp->val.children.tpl,
                ecp, txp) != RDB_OK)
            goto error;
        qrp->val.children.tpl_valid = RDB_TRUE;

        if (RDB_invrename_tuple_ex(&qrp->val.children.tpl,
                qrp->val.children.qr2p->exp, ecp, &oxtpl) != RDB_OK)
            goto error;

        /* Set cursor position */
        if (RDB_seek_index_qresult(qrp->val.children.qr2p->val.children.qrp, indexp,
                &oxtpl, ecp, txp) != RDB_OK)
            goto error;
    }

    RDB_destroy_obj(tplp, ecp);
    RDB_init_obj(tplp);

    for (;;) {
        /* read next 'inner' tuple */
        RDB_qresult *sqrp = qrp->val.children.qr2p->val.children.qrp;

        ret = RDB_next_stored_tuple(sqrp,
                sqrp->val.stored.tbp, &itpl, RDB_TRUE, RDB_TRUE,
                sqrp->val.stored.tbp->typ->def.basetyp,
                ecp, txp);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
                goto error;
            }
            RDB_clear_err(ecp);
        } else {
            RDB_bool match;

            if (RDB_rename_tuple_ex(tplp, &itpl, qrp->val.children.qr2p->exp, ecp) != RDB_OK)
                goto error;

            if (RDB_tuple_matches(tplp, &qrp->val.children.tpl, ecp, txp,
                    &match) != RDB_OK) {
                goto error;
            }

            /*
             * If common attributes are equal, leave the loop,
             * otherwise read next tuple
             */
            if (match)
                break;
            continue;
        }

        /* read next 'outer' tuple */
        if (RDB_next_tuple(qrp->val.children.qrp, &qrp->val.children.tpl,
                ecp, txp) != RDB_OK) {
            goto error;
        }

        if (RDB_invrename_tuple_ex(&qrp->val.children.tpl,
                qrp->val.children.qr2p->exp, ecp, &oxtpl) != RDB_OK)
            goto error;

        /* reset cursor */
        if (RDB_seek_index_qresult(qrp->val.children.qr2p->val.children.qrp,
                indexp, &oxtpl, ecp, txp) != RDB_OK)
            goto error;
    }

    RDB_destroy_obj(&itpl, ecp);
    RDB_destroy_obj(&oxtpl, ecp);

    /* join the two tuples into tplp */
    return RDB_add_tuple(tplp, &qrp->val.children.tpl, ecp, txp);

error:
    RDB_destroy_obj(&itpl, ecp);
    RDB_destroy_obj(&oxtpl, ecp);
    return RDB_ERROR;
}

static int
next_join_uix(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl;
    RDB_object **objpv;
    RDB_bool match = RDB_FALSE;
    RDB_tbindex *indexp = qrp->exp->def.op.args.firstp->nextp->def.tbref.indexp;

    objpv = RDB_alloc(sizeof(RDB_object *) * indexp->attrc, ecp);
    if (objpv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(tplp, ecp);
    RDB_init_obj(tplp);

    RDB_init_obj(&tpl);

    do {
        ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;

        for (i = 0; i < indexp->attrc; i++) {
            objpv[i] = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
            objpv[i]->store_typ = objpv[i]->typ;
        }
        ret = RDB_get_by_uindex(qrp->exp->def.op.args.firstp->nextp->def.tbref.tbp,
                objpv, indexp,
                qrp->exp->def.op.args.firstp->nextp->def.tbref.tbp->typ->def.basetyp,
                ecp, txp, &tpl);
        if (ret == RDB_ERROR) {
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                RDB_clear_err(ecp);
                continue;
            }
            goto cleanup;
        }

        ret = RDB_tuple_matches(tplp, &tpl, ecp, txp, &match);
        if (ret != RDB_OK)
            goto cleanup;
    } while (!match);

    ret = RDB_add_tuple(tplp, &tpl, ecp, txp);

cleanup:
    RDB_free(objpv);
    RDB_destroy_obj(&tpl, ecp);

    return ret;
}

static int
next_join_nuix(RDB_qresult *qrp, RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_tbindex *indexp = qrp->exp->def.op.args.firstp->nextp->def.tbref.indexp;

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->val.children.tpl_valid) {
        RDB_init_obj(&qrp->val.children.tpl);
        ret = RDB_next_tuple(qrp->val.children.qrp, &qrp->val.children.tpl,
                ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        qrp->val.children.tpl_valid = RDB_TRUE;

        /* Set cursor position */
        ret = RDB_seek_index_qresult(qrp->val.children.qr2p, indexp,
                &qrp->val.children.tpl, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    RDB_destroy_obj(tplp, ecp);
    RDB_init_obj(tplp);

    for (;;) {
        /* read next 'inner' tuple */
        ret = RDB_next_stored_tuple(qrp->val.children.qr2p,
                qrp->val.children.qr2p->val.stored.tbp, tplp, RDB_TRUE, RDB_TRUE,
                qrp->val.children.qr2p->val.stored.tbp->typ->def.basetyp,
                ecp, txp);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        } else {
            RDB_bool match;

            if (RDB_tuple_matches(tplp, &qrp->val.children.tpl, ecp, txp,
                    &match) != RDB_OK) {
                return RDB_ERROR;
            }

            /*
             * If common attributes are equal, leave the loop,
             * otherwise read next tuple
             */
            if (match)
                break;
            continue;
        }

        /* read next 'outer' tuple */
        ret = RDB_next_tuple(qrp->val.children.qrp, &qrp->val.children.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* reset cursor */
        ret = RDB_seek_index_qresult(qrp->val.children.qr2p, indexp,
                &qrp->val.children.tpl, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    /* join the two tuples into tplp */
    return RDB_add_tuple(tplp, &qrp->val.children.tpl, ecp, txp);
}

int
RDB_next_join(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (qrp->exp->def.op.args.firstp->nextp->kind == RDB_EX_TBP
            && qrp->exp->def.op.args.firstp->nextp->def.tbref.indexp != NULL) {
        RDB_tbindex *indexp = qrp->exp->def.op.args.firstp->nextp->def.tbref.indexp;
        return indexp->unique ? next_join_uix(qrp, tplp, ecp, txp)
                : next_join_nuix(qrp, tplp, ecp, txp);
    }

    /* Check if the 2nd arg is a RENAME over a stored table */
    if (qrp->val.children.qr2p->exp != NULL
            && RDB_expr_is_op(qrp->val.children.qr2p->exp, "rename")
            && qrp->val.children.qr2p->exp->def.op.args.firstp != NULL
            && qrp->val.children.qr2p->exp->def.op.args.firstp->kind == RDB_EX_TBP) {
        RDB_tbindex *indexp = qrp->val.children.qr2p->exp->def.op.args.firstp->def.tbref.indexp;

        if (indexp != NULL) {
            if (indexp->unique) {
                return next_join_rename_uix(qrp, tplp, indexp, ecp, txp);
            } else {
                return next_join_rename_nuix(qrp, tplp, indexp, ecp, txp);
            }
        }
    }

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->val.children.tpl_valid) {
        RDB_init_obj(&qrp->val.children.tpl);
        qrp->val.children.tpl_valid = RDB_TRUE;
        if (RDB_next_tuple(qrp->val.children.qrp, &qrp->val.children.tpl,
                ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    RDB_destroy_obj(tplp, ecp);
    RDB_init_obj(tplp);
    for (;;) {
        /* Read next 'inner' tuple */
        ret = RDB_next_tuple(qrp->val.children.qr2p, tplp, ecp, txp);
        if (ret != RDB_OK && RDB_obj_type(RDB_get_err(ecp))
                != &RDB_NOT_FOUND_ERROR) {
            return RDB_ERROR;
        }

        if (ret == RDB_OK) {
            RDB_bool iseq;

            /* Compare common attributes */
            ret = RDB_tuple_matches(tplp, &qrp->val.children.tpl, ecp, txp, &iseq);
            if (ret != RDB_OK)
                return RDB_ERROR;

            /*
             * If common attributes are equal, leave the loop,
             * otherwise read next tuple
             */
            if (iseq)
                break;
            continue;
        }
        RDB_clear_err(ecp);

        /* reset nested qresult */
        ret = RDB_reset_qresult(qrp->val.children.qr2p, ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* read next 'outer' tuple */
        ret = RDB_next_tuple(qrp->val.children.qrp, &qrp->val.children.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    }

    /* join the two tuples into tplp */
    return RDB_add_tuple(tplp, &qrp->val.children.tpl, ecp, txp);
}
