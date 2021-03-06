/*
 * Functions for RDB_qresult structures that iterate over stored
 * or local tables.
 *
 * Copyright (C) 2014-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "qr_stored.h"
#include "stable.h"
#include "qresult.h"
#include "typeimpl.h"
#include "internal.h"

#ifdef POSTGRESQL
#include <pgrec/pgcursor.h>
#endif

int
RDB_init_cursor_qresult(RDB_qresult *qrp, RDB_cursor *curp, RDB_object *tbp,
        RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    qrp->exp = exp;
    qrp->nested = RDB_FALSE;
    qrp->val.stored.tbp = tbp;
    qrp->endreached = RDB_FALSE;
    qrp->matp = NULL;

    qrp->val.stored.curp = curp;
    ret = RDB_cursor_first(qrp->val.stored.curp, ecp);
    if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        RDB_handle_err(ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_init_stored_qresult(RDB_qresult *qrp, RDB_object *tbp, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_cursor *curp;

    if (tbp->val.tbp->stp == NULL) {
        /*
         * The stored table may have been created by another process,
         * so try to open it
         */
        if (RDB_provide_stored_table(tbp,
                RDB_FALSE, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }

        if (tbp->val.tbp->stp == NULL) {
            /*
             * Table has no physical representation which means it is empty
             */
            qrp->exp = exp;
            qrp->nested = RDB_FALSE;
            qrp->val.stored.tbp = tbp;
            qrp->matp = NULL;
            qrp->endreached = RDB_TRUE;
            qrp->val.stored.curp = NULL;
            return RDB_OK;
        }
    }
    curp = RDB_recmap_cursor(tbp->val.tbp->stp->recmapp,
                    RDB_FALSE, RDB_table_is_persistent(tbp) ? txp->tx : NULL, ecp);
    if (curp == NULL) {
        RDB_handle_err(ecp, txp);
        return RDB_ERROR;
    }
    if (RDB_init_cursor_qresult(qrp, curp, tbp, exp, ecp, txp) != RDB_OK) {
        RDB_destroy_cursor(curp, ecp);
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
        ret = RDB_cursor_next(qrp->val.stored.curp, dup ? RDB_REC_DUP : 0, ecp);
    } else {
        if (dup) {
            RDB_raise_invalid_argument("", ecp);
            return RDB_ERROR;
        }
        ret = RDB_cursor_prev(qrp->val.stored.curp, ecp);
    }
    if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        RDB_handle_err(ecp, txp);
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

        if (tbp != NULL) {
            fno = *RDB_field_no(tbp->val.tbp->stp, attrp->name);
            ret = RDB_cursor_get(curp, fno, &datap, &len, ecp);
        } else {
#ifdef POSTGRESQL
            ret = RDB_pg_cursor_get_by_name(curp, attrp->name, &datap, &len,
                    RDB_type_field_flags(attrp->typ), ecp);
#else
            RDB_raise_not_supported("SQL queries not supported", ecp);
            ret = RDB_ERROR;
#endif
        }
        if (ret != RDB_OK) {
            RDB_handle_err(ecp, txp);
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
