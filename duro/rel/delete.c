/*
 * $Id$
 *
 * Copyright (C) 2004-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "delete.h"
#include "typeimpl.h"
#include "internal.h"
#include "stable.h"
#include "qresult.h"
#include <obj/objinternal.h>
#include <gen/strfns.h>

#include <string.h>

static RDB_int
delete_by_uindex(RDB_object *tbp, RDB_object *objpv[], RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_field *fv;
    int i;
    int ret;
    int keylen = indexp->attrc;

    fv = RDB_alloc(sizeof (RDB_field) * keylen, ecp);
    if (fv == NULL) {
        RDB_raise_no_memory(ecp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < keylen; i++) {
        ret = RDB_obj_to_field(&fv[i], objpv[i], ecp);
        if (ret != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    RDB_cmp_ecp = ecp;
    if (indexp->idxp == NULL) {
        ret = RDB_delete_rec(tbp->val.tb.stp->recmapp, fv,
                RDB_table_is_persistent(tbp) ? txp->txid : NULL);
    } else {
        ret = RDB_index_delete_rec(indexp->idxp, fv,
                RDB_table_is_persistent(tbp) ? txp->txid : NULL);
    }
    switch (ret) {
        case RDB_OK:
            rcount = 1;
            break;
        case DB_NOTFOUND:
            rcount = 0;
            break;
        default:
            RDB_handle_errcode(ret, ecp, txp);
            rcount = RDB_ERROR;
    }

cleanup:
    RDB_free(fv);
    return rcount;
}

RDB_int
RDB_delete_real(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_int rcount;
    int ret;
    int i;
    RDB_cursor *curp;
    RDB_object tpl;
    void *datap;
    size_t len;
    RDB_bool b;
    RDB_type *tpltyp = tbp->typ->def.basetyp;

    if (tbp->val.tb.stp == NULL) {
        /* Physical table representation has not been created, so table is empty */
        return 0;
    }

    ret = RDB_recmap_cursor(&curp, tbp->val.tb.stp->recmapp, RDB_TRUE,
            RDB_table_is_persistent(tbp) ? txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    ret = RDB_cursor_first(curp);
    
    RDB_cmp_ecp = ecp;
    rcount = 0;
    while (ret == RDB_OK) {
        RDB_init_obj(&tpl);
        if (condp != NULL) {
            for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
                RDB_object val;

                ret = RDB_cursor_get(curp,
                        *RDB_field_no(tbp->val.tb.stp, tpltyp->def.tuple.attrv[i].name),
                        &datap, &len);
                if (ret != RDB_OK) {
                   RDB_destroy_obj(&tpl, ecp);
                   RDB_handle_errcode(ret, ecp, txp);
                   goto error;
                }
                RDB_init_obj(&val);
                ret = RDB_irep_to_obj(&val, tpltyp->def.tuple.attrv[i].typ,
                                 datap, len, ecp);
                if (ret != RDB_OK) {
                   RDB_destroy_obj(&val, ecp);
                   RDB_destroy_obj(&tpl, ecp);
                   goto error;
                }
                ret = RDB_tuple_set(&tpl, tpltyp->def.tuple.attrv[i].name,
                        &val, ecp);
                RDB_destroy_obj(&val, ecp);
                if (ret != RDB_OK) {
                   RDB_destroy_obj(&tpl, ecp);
                   goto error;
                }
            }

            if (RDB_evaluate_bool(condp, &RDB_tpl_get, &tpl, NULL, ecp, txp, &b)
                    != RDB_OK)
                 goto error;
        } else {
            b = RDB_TRUE;
        }
        if (b) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                RDB_handle_errcode(ret, ecp, txp);
                RDB_destroy_obj(&tpl, ecp);
                goto error;
            }
            rcount++;
        }
        RDB_destroy_obj(&tpl, ecp);
        ret = RDB_cursor_next(curp, 0);
    };
    if (ret != DB_NOTFOUND) {
        RDB_handle_errcode(ret, ecp, txp);
        goto error;
    }

    if (RDB_destroy_cursor(curp) != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        curp = NULL;
        goto error;
    }
    return rcount;

error:
    if (curp != NULL)
        RDB_destroy_cursor(curp);
    return RDB_ERROR;
}

static RDB_int
delete_where_uindex(RDB_expression *texp, RDB_expression *condp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    int ret;
    RDB_expression *refexp;

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }

    if (condp != NULL) {
        RDB_object tpl;
        RDB_bool b;

        RDB_init_obj(&tpl);

        /*
         * Read tuple and check condition
         */
        if (RDB_get_by_uindex(refexp->def.tbref.tbp,
                texp->def.op.optinfo.objpv, refexp->def.tbref.indexp,
                refexp->def.tbref.tbp->typ->def.basetyp, ecp, txp, &tpl)
                    != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        ret = RDB_evaluate_bool(condp, &RDB_tpl_get, &tpl, NULL, ecp, txp, &b);
        RDB_destroy_obj(&tpl, ecp);
        if (ret != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }

        if (!b)
            return 0;
    }

    rcount = delete_by_uindex(refexp->def.tbref.tbp,
            texp->def.op.optinfo.objpv, refexp->def.tbref.indexp,
            ecp, txp);

cleanup:
    return rcount;
}

static RDB_int
delete_where_nuindex(RDB_expression *texp, RDB_expression *condp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    int ret;
    int i;
    int flags;
    RDB_cursor *curp = NULL;
    RDB_field *fv = NULL;
    RDB_expression *refexp;
    RDB_tbindex *indexp;
    int keylen;

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }

    indexp = refexp->def.tbref.indexp;
    keylen = indexp->attrc;

    ret = RDB_index_cursor(&curp, indexp->idxp, RDB_TRUE,
            RDB_table_is_persistent(refexp->def.tbref.tbp) ?
            txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }

    fv = RDB_alloc(sizeof (RDB_field) * keylen, ecp);
    if (fv == NULL) {
        RDB_raise_no_memory(ecp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < keylen; i++) {
        if (RDB_obj_to_field(&fv[i], texp->def.op.optinfo.objpv[i], ecp)
                != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    if (texp->def.op.optinfo.objc != indexp->attrc
            || !texp->def.op.optinfo.all_eq)
        flags = RDB_REC_RANGE;
    else
        flags = 0;

    ret = RDB_cursor_seek(curp, texp->def.op.optinfo.objc, fv, flags);
    if (ret == DB_NOTFOUND) {
        rcount = 0;
        goto cleanup;
    }

    RDB_cmp_ecp = ecp;
    rcount = 0;
    do {
        RDB_bool del = RDB_TRUE;
        RDB_bool b;

        RDB_object tpl;

        RDB_init_obj(&tpl);

        /*
         * Read tuple and check condition
         */
        ret = RDB_get_by_cursor(refexp->def.tbref.tbp,
                curp, refexp->def.tbref.tbp->typ->def.basetyp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        if (texp->def.op.optinfo.stopexp != NULL) {
            ret = RDB_evaluate_bool(texp->def.op.optinfo.stopexp,
                    &RDB_tpl_get, &tpl, NULL, ecp, txp, &b);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (!b) {
                RDB_destroy_obj(&tpl, ecp);
                goto cleanup;
            }
        }
        if (condp != NULL) {
            ret = RDB_evaluate_bool(condp, &RDB_tpl_get, &tpl, NULL, ecp, txp,
                    &del);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }
        ret = RDB_evaluate_bool(texp->def.op.args.firstp->nextp, &RDB_tpl_get,
                &tpl, NULL, ecp, txp, &b);
        RDB_destroy_obj(&tpl, ecp);
        if (ret != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
        del = (RDB_bool) (del && b);

        if (del) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                RDB_handle_errcode(ret, ecp, txp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }

        if (texp->def.op.optinfo.objc == indexp->attrc
                && texp->def.op.optinfo.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;
        ret = RDB_cursor_next(curp, flags);
    } while (ret == RDB_OK);

    if (ret != DB_NOTFOUND) {
        RDB_handle_errcode(ret, ecp, txp);
        rcount = RDB_ERROR;
    }

cleanup:
    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK && rcount != RDB_ERROR) {
            RDB_handle_errcode(ret, ecp, txp);
            rcount = RDB_ERROR;
        }
    }
    RDB_free(fv);
    return rcount;
}

RDB_int
RDB_delete_where_index(RDB_expression *texp, RDB_expression *condp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *refexp;

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }

    if (refexp->def.tbref.indexp->unique) {
        return delete_where_uindex(texp, condp, ecp, txp);
    }
    return delete_where_nuindex(texp, condp, ecp, txp);
}
