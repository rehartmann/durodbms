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

    ret = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp, 0, txp->txid);
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
delete_select_index(RDB_table *tbp, RDB_expression *condp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    int objc = tbp->var.select.indexp->attrc;
    RDB_object *objv = malloc(sizeof(RDB_object) * objc);
    if (objv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < objc; i++)
        RDB_init_obj(&objv[i]);

    ret = _RDB_index_expr_to_objv(tbp->var.select.indexp,
          tbp->var.select.exp, tbp->typ, objv);
    if (ret != RDB_OK)
        goto cleanup;

    if (condp != NULL) {
        RDB_object tpl;
        RDB_bool b;

        RDB_init_obj(&tpl);

        /*
         * Read tuple and check condition
         */
        ret = _RDB_get_by_uindex(tbp->var.select.tbp, objv,
                tbp->var.select.indexp, txp, &tpl);
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

    ret = delete_by_uindex(tbp->var.select.tbp, objv, tbp->var.select.indexp,
            txp);
    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;

cleanup:
    for (i = 0; i < objc; i++) {
        int ret2 = RDB_destroy_obj(&objv[i]);
        if (ret2 != RDB_OK && ret == RDB_OK)
            return ret2;
    }
    free(objv);
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
        case RDB_TB_STORED:
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
        {
            RDB_expression *ncondp = NULL;

            if (condp != NULL) {
                ncondp = RDB_and(tbp->var.select.exp, condp);
                if (ncondp == NULL)
                    return RDB_NO_MEMORY;
            }
            ret = delete(tbp->var.select.tbp,
                    ncondp != NULL ? ncondp : tbp->var.select.exp, txp);
            free(ncondp);
            return ret;
        }
        case RDB_TB_SELECT_INDEX:
            return delete_select_index(tbp, condp, txp);
        case RDB_TB_JOIN:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_EXTEND:
            return delete_extend(tbp, condp, txp);
        case RDB_TB_PROJECT:
            return delete(tbp->var.project.tbp, condp, txp); /* !! */
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
            _RDB_free_table(tbp);
            return ret;
        }
    }

    ret = delete(tbp, NULL, txp);
    if (condp != NULL)
        _RDB_free_table(tbp);
    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}
