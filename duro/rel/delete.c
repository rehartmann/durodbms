/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <string.h>

static int
delete_by_uindex(RDB_table *tbp, RDB_object *objpv[], _RDB_tbindex *indexp,
        RDB_transaction *txp)
{
    RDB_field *fv;
    int i;
    int ret;
    int keylen = indexp->attrc;

    fv = malloc(sizeof (RDB_field) * keylen);
    if (fv == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    
    for (i = 0; i < keylen; i++) {
        ret = _RDB_obj_to_field(&fv[i], objpv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }

    if (indexp->idxp == NULL) {
        ret = RDB_delete_rec(tbp->var.real.recmapp, fv,
                tbp->is_persistent ? txp->txid : NULL);
    } else {
        ret = RDB_index_delete_rec(indexp->idxp, fv,
                tbp->is_persistent ? txp->txid : NULL);
    }
    if (ret != RDB_OK) {
        goto cleanup;
    }

    ret = RDB_OK;

cleanup:
    free(fv);
    return ret;
}

static int
delete_stored(RDB_table *tbp, RDB_expression *condp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_cursor *curp;
    RDB_object tpl;
    void *datap;
    size_t len;
    RDB_bool b;
    RDB_type *tpltyp = tbp->typ->var.basetyp;

    ret = RDB_recmap_cursor(&curp, tbp->var.real.recmapp, RDB_TRUE,
            tbp->is_persistent ? txp->txid : NULL);
    if (ret != RDB_OK)
        return ret;
    ret = RDB_cursor_first(curp);
    if (ret == RDB_NOT_FOUND) {
        RDB_destroy_cursor(curp);
        return RDB_OK;
    }
    
    do {
        RDB_init_obj(&tpl);
        if (condp != NULL) {
            for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
                RDB_object val;

                ret = RDB_cursor_get(curp, i, &datap, &len);
                if (ret != 0) {
                   RDB_destroy_obj(&tpl);
                   goto error;
                }
                RDB_init_obj(&val);
                ret = RDB_irep_to_obj(&val, tpltyp->var.tuple.attrv[i].typ,
                                 datap, len);
                if (ret != RDB_OK) {
                   RDB_destroy_obj(&val);
                   RDB_destroy_obj(&tpl);
                   goto error;
                }
                ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val);
                RDB_destroy_obj(&val);
                if (ret != RDB_OK) {
                   RDB_destroy_obj(&tpl);
                   goto error;
                }
            }

            ret = RDB_evaluate_bool(condp, &tpl, txp, &b);
            if (ret != RDB_OK)
                 goto error;
        } else {
            b = RDB_TRUE;
        }
        if (b) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                RDB_errmsg(txp->dbp->dbrootp->envp, "cannot delete record: %s",
                        RDB_strerror(ret));
                RDB_destroy_obj(&tpl);
                goto error;
            }
        }
        RDB_destroy_obj(&tpl);
        ret = RDB_cursor_next(curp, 0);
    } while (ret == RDB_OK);
    if (ret != RDB_NOT_FOUND)
        goto error;
    return RDB_destroy_cursor(curp);

error:
    RDB_destroy_cursor(curp);
    return ret;
}  

static int
delete_select_uindex(RDB_table *tbp, RDB_expression *condp,
        RDB_transaction *txp)
{
    int ret;

    if (condp != NULL) {
        RDB_object tpl;
        RDB_bool b;

        RDB_init_obj(&tpl);

        /*
         * Read tuple and check condition
         */
        ret = _RDB_get_by_uindex(tbp->var.select.tbp->var.project.tbp,
                tbp->var.select.objpv, tbp->var.select.tbp->var.project.indexp,
                tbp->typ->var.basetyp, txp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            goto cleanup;
        }
        ret = RDB_evaluate_bool(condp, &tpl, txp, &b);
        RDB_destroy_obj(&tpl);
        if (ret != RDB_OK)
            goto cleanup;

        if (!b)
            return RDB_OK;
    }

    ret = delete_by_uindex(tbp->var.select.tbp->var.project.tbp,
            tbp->var.select.objpv, tbp->var.select.tbp->var.project.indexp,
            txp);
    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;

cleanup:
    return ret;
}

static int
delete_select_index(RDB_table *tbp, RDB_expression *condp,
        RDB_transaction *txp)
{
    int ret, ret2;
    int i;
    int flags;
    RDB_cursor *curp = NULL;
    RDB_field *fv = NULL;
    _RDB_tbindex *indexp = tbp->var.select.tbp->var.project.indexp;
    int keylen = indexp->attrc;

    ret = RDB_index_cursor(&curp, indexp->idxp, RDB_TRUE,
            tbp->var.select.tbp->var.project.tbp->is_persistent ?
            txp->txid : NULL);
    if (ret != RDB_OK) {
        if (txp != NULL) {
            RDB_errmsg(txp->dbp->dbrootp->envp, "cannot create cursor: %s",
                    RDB_strerror(ret));
        }
        return ret;
    }

    fv = malloc(sizeof (RDB_field) * keylen);
    if (fv == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    for (i = 0; i < keylen; i++) {
        ret =_RDB_obj_to_field(&fv[i], tbp->var.select.objpv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }

    if (tbp->var.select.objpc != indexp->attrc
            || !tbp->var.select.all_eq)
        flags = RDB_REC_RANGE;
    else
        flags = 0;

    ret = RDB_cursor_seek(curp, tbp->var.select.objpc, fv, flags);
    if (ret == RDB_NOT_FOUND) {
        ret = RDB_OK;
        goto cleanup;
    }

    do {
        RDB_bool del = RDB_TRUE;
        RDB_bool b;

        RDB_object tpl;

        RDB_init_obj(&tpl);

        /*
         * Read tuple and check condition
         */
        ret = _RDB_get_by_cursor(tbp->var.select.tbp->var.project.tbp,
                curp, tbp->typ->var.basetyp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            goto cleanup;
        }
        if (tbp->var.select.stopexp != NULL) {
            ret = RDB_evaluate_bool(tbp->var.select.stopexp, &tpl,
                    txp, &b);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                goto cleanup;
            }
            if (!b) {
                ret = RDB_OK;
                RDB_destroy_obj(&tpl);
                goto cleanup;
            }
        }
        if (condp != NULL) {
            ret = RDB_evaluate_bool(condp, &tpl, txp, &del);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                goto cleanup;
            }
        }
        ret = RDB_evaluate_bool(tbp->var.select.exp, &tpl, txp, &b);
        RDB_destroy_obj(&tpl);
        if (ret != RDB_OK)
            goto cleanup;
        del = (RDB_bool) (del && b);

        if (del) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK)
                goto cleanup;
        }

        if (tbp->var.select.objpc == indexp->attrc
                && tbp->var.select.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;
        ret = RDB_cursor_next(curp, flags);
    } while (ret == RDB_OK);

    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;

cleanup:
    if (curp != NULL) {
        ret2 = RDB_destroy_cursor(curp);
        if (ret2 != RDB_OK && ret == RDB_OK)
            ret = ret2;
    }
    free(fv);
    return ret;
}

static int
delete(RDB_table *, RDB_expression *, RDB_transaction *);

static int
delete_extend(RDB_table *tbp, RDB_expression *condp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *newexp = NULL;

    if (condp != NULL) {    
        newexp = RDB_dup_expr(condp);
        if (newexp == NULL)
            return RDB_NO_MEMORY;

        ret = _RDB_resolve_extend_expr(&newexp, tbp->var.extend.attrc,
                tbp->var.extend.attrv);
        if (ret != RDB_OK) {
            RDB_drop_expr(newexp);
            return ret;
        }
    }
    ret = delete(tbp->var.extend.tbp, newexp, txp);
    if (newexp != NULL)
        RDB_drop_expr(newexp);
    return ret;
}

static int
delete(RDB_table *tbp, RDB_expression *condp, RDB_transaction *txp)
{
    int ret;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            return delete_stored(tbp, condp, txp);
        case RDB_TB_MINUS:
            return delete(tbp->var.minus.tb1p, condp, txp);
        case RDB_TB_UNION:
            ret = delete(tbp->var._union.tb1p, condp, txp);
            if (ret != RDB_OK)
                return ret;
            return delete(tbp->var._union.tb2p, condp, txp);
        case RDB_TB_INTERSECT:
            ret = delete(tbp->var.intersect.tb1p, condp, txp);
            if (ret != RDB_OK)
                return ret;
            return delete(tbp->var.intersect.tb2p, condp, txp);
        case RDB_TB_SELECT:
            if (tbp->var.select.tbp->kind != RDB_TB_PROJECT
                    || tbp->var.select.tbp->var.project.indexp == NULL
                    || tbp->var.select.objpc == 0)
            {
                RDB_expression *ncondp = NULL;

                if (condp != NULL) {
                    /* Untested due to optimization */
                    ncondp = RDB_ro_op_va("AND", tbp->var.select.exp, condp,
                            (RDB_expression *) NULL);
                    if (ncondp == NULL)
                        return RDB_NO_MEMORY;
                }
                ret = delete(tbp->var.select.tbp,
                        ncondp != NULL ? ncondp : tbp->var.select.exp, txp);
                free(ncondp);
                return ret;
            } else {
                if (tbp->var.select.tbp->var.project.indexp->unique)
                    return delete_select_uindex(tbp, condp, txp);
                return delete_select_index(tbp, condp, txp);
            }
        case RDB_TB_JOIN:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_EXTEND:
            return delete_extend(tbp, condp, txp);
        case RDB_TB_PROJECT:
            /* !! check if condp refers to attributes "projected away" */
            return delete(tbp->var.project.tbp, condp, txp);
        case RDB_TB_SUMMARIZE:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_RENAME:
            if (condp != NULL) {
                ret = _RDB_invrename_expr(condp, tbp->var.rename.renc,
                        tbp->var.rename.renv);
                if (ret != RDB_OK)
                    return ret;
            }
            return delete(tbp->var.rename.tbp, condp, txp);
        case RDB_TB_WRAP:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_UNWRAP:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_GROUP:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_UNGROUP:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_SDIVIDE:
            return RDB_NOT_SUPPORTED;
    }
    /* should never be reached */
    abort();
}

int
RDB_delete(RDB_table *tbp, RDB_expression *condp, RDB_transaction *txp)
{
    int ret;
    RDB_table *ntbp;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /*
     * If there is a condition, create new table, so the optimization
     * uses the contition.
     */
    if (condp != NULL) {
        RDB_table *ctbp;

        ret = RDB_select(tbp, condp, txp, &ctbp);
        if (ret != RDB_OK)
            return ret;
        tbp = ctbp;
    }

    ret = _RDB_optimize(tbp, 0, NULL, txp, &ntbp);
    if (ret != RDB_OK) {
        if (condp != NULL)
            _RDB_free_table(tbp);
        return ret;
    }

    ret = delete(ntbp, NULL, txp);
    if (ret == RDB_OK) {
        if (ntbp->kind != RDB_TB_REAL)
            ret = RDB_drop_table(ntbp, txp);
        ntbp = NULL;
    }

    if (condp != NULL)
        _RDB_free_table(tbp);
    if (ntbp != NULL && ntbp->kind != RDB_TB_REAL)
        RDB_drop_table(ntbp, txp);
    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}
