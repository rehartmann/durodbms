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
delete_by_uindex(RDB_table *tbp, RDB_object valv[], _RDB_tbindex *indexp,
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
        _RDB_obj_to_field(&fv[i], &valv[i]);
    }

    if (indexp->idxp == NULL) {
        ret = RDB_delete_rec(tbp->var.stored.recmapp, fv,
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
delete_stored(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_cursor *curp;

    ret = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp, 0, txp->txid);
    if (ret != RDB_OK)
        return ret;
    ret = RDB_cursor_first(curp);
    if (ret == RDB_NOT_FOUND) {
        RDB_destroy_cursor(curp);
        return RDB_OK;
    }
    
    do {
        ret = RDB_cursor_delete(curp);
        if (ret != RDB_OK) {
            goto error;
        }
        ret = RDB_cursor_next(curp);
    } while (ret == RDB_OK);
    if (ret != RDB_NOT_FOUND)
        goto error;
    return RDB_destroy_cursor(curp);

error:
    RDB_destroy_cursor(curp);
    return ret;
}  

static int
delete_select(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_cursor *curp;
    RDB_object tpl;
    void *datap;
    size_t len;
    RDB_bool b;
    RDB_type *tpltyp = tbp->typ->var.basetyp;

    if (tbp->var.select.tbp->kind != RDB_TB_STORED)
        return RDB_NOT_SUPPORTED;

    ret = RDB_recmap_cursor(&curp, tbp->var.select.tbp->var.stored.recmapp,
            0, txp->txid);
    if (ret != RDB_OK)
        return ret;
    ret = RDB_cursor_first(curp);
    if (ret == RDB_NOT_FOUND) {
        RDB_destroy_cursor(curp);
        return RDB_OK;
    }
    
    do {
        RDB_init_obj(&tpl);
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
               RDB_destroy_obj(&tpl);
               goto error;
            }
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val);
            if (ret != RDB_OK) {
               RDB_destroy_obj(&tpl);
               goto error;
            }
        }

        ret = RDB_evaluate_bool(tbp->var.select.exprp, &tpl, txp, &b);
        if (ret != RDB_OK)
             goto error;

        if (b) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                goto error;
            }
        }
        RDB_destroy_obj(&tpl);
        ret = RDB_cursor_next(curp);
    } while (ret == RDB_OK);
    if (ret != RDB_NOT_FOUND)
        goto error;
    RDB_destroy_cursor(curp);
    return RDB_OK;

error:
    RDB_destroy_cursor(curp);
    return ret;
}  

static int
delete_select_index(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_object val;

    RDB_init_obj(&val);
    /* !! */
    ret = RDB_evaluate(tbp->var.select.exprp->var.op.arg2, NULL, txp, &val);
    if (ret != RDB_OK)
        return ret;

    ret = delete_by_uindex(tbp->var.select.tbp, &val, tbp->var.select.indexp,
            txp);

    RDB_destroy_obj(&val);
    return ret;
}

static int
delete(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            ret = delete_stored(tbp, txp);
            if (RDB_is_syserr(ret)) {
                RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                RDB_rollback_all(txp);
            }
            return ret;
        case RDB_TB_MINUS:
            return delete(tbp->var.minus.tb1p, txp);
        case RDB_TB_UNION:
            ret = delete(tbp->var._union.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            return delete(tbp->var._union.tb2p, txp);
        case RDB_TB_INTERSECT:
            ret = delete(tbp->var.intersect.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            return delete(tbp->var.intersect.tb2p, txp);
        case RDB_TB_SELECT:
            return delete_select(tbp, txp);
        case RDB_TB_SELECT_INDEX:
            return delete_select_index(tbp, txp);
        case RDB_TB_JOIN:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_EXTEND:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_PROJECT:
            return delete(tbp->var.project.tbp, txp);
        case RDB_TB_SUMMARIZE:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_RENAME:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_WRAP:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_UNWRAP:
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

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    if (condp != NULL) {
        ret = RDB_select(tbp, condp, &tbp);
        if (ret != RDB_OK)
            return ret;
    }

    if (!tbp->optimized) {
        ret = _RDB_optimize(tbp, txp);
        if (ret != RDB_OK) {
            _RDB_free_table(tbp, NULL);
            return ret;
        }
    }

    ret = delete(tbp, txp);
    if (condp != NULL)
        _RDB_free_table(tbp, NULL);
    return ret;
}
