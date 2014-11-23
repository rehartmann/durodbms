/*
 * qr_stored.c
 *
 *  Created on: 23.11.2014
 *      Author: Rene Hartmann
 */

#include "qr_stored.h"
#include "stable.h"
#include "qresult.h"
#include "typeimpl.h"

int
RDB_init_stored_qresult(RDB_qresult *qrp, RDB_object *tbp, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    qrp->exp = exp;
    qrp->nested = RDB_FALSE;
    qrp->val.stored.tbp = tbp;
    if (tbp->val.tb.stp == NULL) {
        /*
         * Table has no physical representation, which means it is empty
         */
        qrp->endreached = RDB_TRUE;
        qrp->val.stored.curp = NULL;
        return RDB_OK;
    }

    ret = RDB_recmap_cursor(&qrp->val.stored.curp, tbp->val.tb.stp->recmapp,
                    RDB_FALSE, RDB_table_is_persistent(tbp) ? txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    ret = RDB_cursor_first(qrp->val.stored.curp);
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_next_stored_tuple(RDB_qresult *qrp, RDB_object *tbp, RDB_object *tplp,
        RDB_bool asc, RDB_bool dup, RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (qrp->endreached) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (tplp != NULL) {
        ret = RDB_get_by_cursor(tbp, qrp->val.stored.curp, tpltyp, tplp, ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (asc) {
        ret = RDB_cursor_next(qrp->val.stored.curp, dup ? RDB_REC_DUP : 0);
    } else {
        if (dup) {
            RDB_raise_invalid_argument("", ecp);
            return RDB_ERROR;
        }
        ret = RDB_cursor_prev(qrp->val.stored.curp);
    }
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/*
 * Get the next tuple using cursor *curp and store the result tuple in *tplp.
 */
int
RDB_get_by_cursor(RDB_object *tbp, RDB_cursor *curp, RDB_type *tpltyp,
        RDB_object *tplp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_object val;
    RDB_int fno;
    RDB_attr *attrp;
    void *datap;
    size_t len;

    for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
        attrp = &tpltyp->def.tuple.attrv[i];

        fno = *RDB_field_no(tbp->val.tb.stp, attrp->name);
        ret = RDB_cursor_get(curp, fno, &datap, &len);
        if (ret != RDB_OK) {
            RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }
        RDB_init_obj(&val);
        ret = RDB_tuple_set(tplp, attrp->name, &val, ecp);
        RDB_destroy_obj(&val, ecp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
        ret = RDB_irep_to_obj(RDB_tuple_get(tplp, attrp->name),
                attrp->typ, datap, len, ecp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}
