/*
 * $Id$
 *
 * Copyright (C) 2004-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
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
        ret = RDB_delete_rec(tbp->stp->recmapp, fv,
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

    ret = RDB_recmap_cursor(&curp, tbp->stp->recmapp, RDB_TRUE,
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

                ret = RDB_cursor_get(curp,
                        *(int*) RDB_hashmap_get(
                                &tbp->stp->attrmap,
                                tpltyp->var.tuple.attrv[i].name, NULL),
                        &datap, &len);
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

static int
check_delete_empty_real(RDB_table *chtbp, RDB_table *tbp, RDB_transaction *txp)
{
    switch (tbp->kind) {
        case RDB_TB_REAL:
            return chtbp == tbp ? RDB_MAYBE : RDB_OK;
        case RDB_TB_SELECT:
        case RDB_TB_RENAME:
        case RDB_TB_EXTEND:
        case RDB_TB_MINUS:
        case RDB_TB_WRAP:
        case RDB_TB_UNWRAP:
        case RDB_TB_GROUP:
        case RDB_TB_UNGROUP:
        case RDB_TB_UNION:
        case RDB_TB_INTERSECT:
        case RDB_TB_JOIN:
        case RDB_TB_PROJECT:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_SDIVIDE:
            return _RDB_table_refers(tbp, chtbp) ? RDB_MAYBE : RDB_OK;
    }
    abort();
}

static int
check_delete_empty(RDB_table *chtbp, RDB_table *tbp, RDB_transaction *txp)
{
    switch (chtbp->kind) {
        case RDB_TB_REAL:
            return check_delete_empty_real(chtbp, tbp, txp);
        case RDB_TB_SELECT:
            return check_delete_empty(chtbp->var.rename.tbp, tbp, txp);
        case RDB_TB_RENAME:
            return check_delete_empty(chtbp->var.rename.tbp, tbp, txp);
        case RDB_TB_EXTEND:
            return check_delete_empty(chtbp->var.extend.tbp, tbp, txp);
        case RDB_TB_MINUS:
            return RDB_MAYBE;
        case RDB_TB_WRAP:
            return check_delete_empty(chtbp->var.wrap.tbp, tbp, txp);
        case RDB_TB_UNWRAP:
            return check_delete_empty(chtbp->var.unwrap.tbp, tbp, txp);
        case RDB_TB_GROUP:
            return check_delete_empty(chtbp->var.group.tbp, tbp, txp);
        case RDB_TB_UNGROUP:
            return check_delete_empty(chtbp->var.ungroup.tbp, tbp, txp);
        case RDB_TB_UNION:
        case RDB_TB_INTERSECT:
        case RDB_TB_JOIN:
        case RDB_TB_PROJECT:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_SDIVIDE:
            return RDB_MAYBE;
    }
    abort();
}

int
RDB_delete(RDB_table *tbp, RDB_expression *condp, RDB_transaction *txp)
{
    int ret;
    RDB_table *ntbp;
    RDB_dbroot *dbrootp;
    RDB_constraint *constrp;
    RDB_transaction tx;
    RDB_constraint *checklistp = NULL;
    RDB_bool need_subtx = RDB_FALSE;

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

    dbrootp = RDB_tx_db(txp)->dbrootp;

    if (!dbrootp->constraints_read) {
        ret = _RDB_read_constraints(txp);
        if (ret != RDB_OK) {
            return ret;
        }
        dbrootp->constraints_read = RDB_TRUE;
    }

    /*
     * Identify the constraints which must be checked
     * and insert them into a list
     */
    constrp = dbrootp->first_constrp;
    while (constrp != NULL) {
        RDB_bool check;
        RDB_constraint *chconstrp;

        if (constrp->empty_tbp != NULL) {
            ret = check_delete_empty(constrp->empty_tbp, tbp, txp);
            if (ret != RDB_OK && ret != RDB_MAYBE) {
                goto cleanup;
            }
            check = (RDB_bool) (ret == RDB_MAYBE);
        } else {
            /* Check if constrp->exp and tbp depend on the same table(s) */
            check = _RDB_expr_table_depend(constrp->exp, tbp);
        }
        if (check) {
            need_subtx = RDB_TRUE;
            chconstrp = malloc(sizeof(RDB_constraint));
            if (chconstrp == NULL) {
                ret = RDB_NO_MEMORY;
                goto cleanup;
            }
            chconstrp->name = RDB_dup_str(constrp->name);
            if (chconstrp->name == NULL) {
                ret = RDB_NO_MEMORY;
                goto cleanup;
            }
            chconstrp->exp = constrp->exp;
            chconstrp->empty_tbp = constrp->empty_tbp;
            chconstrp->nextp = checklistp;
            checklistp = chconstrp;
        }                
        constrp = constrp->nextp;
    }

    if (need_subtx) {
        ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
        txp = &tx;
    }
    ret = delete(ntbp, NULL, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /*
     * Check constraints in list
     */
    if (txp != NULL) {
        ret = _RDB_check_constraints(checklistp, txp);
        if (ret != RDB_OK) {
            if (need_subtx)
                RDB_rollback(&tx);
            goto cleanup;
        }
    }
    ret = need_subtx ? RDB_commit(&tx) : RDB_OK;

cleanup:
    while (checklistp != NULL) {
        RDB_constraint *nextp = checklistp->nextp;

        free(checklistp->name);
        free(checklistp);
        checklistp = nextp;
    }

    if (condp != NULL)
        _RDB_free_table(tbp);
    if (ntbp != NULL && ntbp->kind != RDB_TB_REAL)
        RDB_drop_table(ntbp, txp);
    _RDB_handle_syserr(txp, ret);
    return ret;
}
