/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

static RDB_bool
is_keyattr(const char *attrname, RDB_table *tbp)
{
    int i, j;
    int keyc;
    RDB_string_vec *keyv;
    
    keyc = RDB_table_keys(tbp, &keyv);
    for (i = 0; i < keyc; i++)
        for (j = 0; j < keyv[i].strc; j++)
            if (strcmp(attrname, keyv[i].strv[j]) == 0)
                return RDB_TRUE;
    return RDB_FALSE;
}

static int
upd_to_vals(int updc, const RDB_attr_update updv[],
               RDB_object *tplp, RDB_object *valv, RDB_transaction *txp)
{
    int i, ret;

    for (i = 0; i < updc; i++) {
        ret = RDB_evaluate(updv[i].exp, tplp, txp, &valv[i]);
        if (ret != RDB_OK) {
            int j;
            
            for (j = 0; j < i; j++)
                RDB_destroy_obj(&valv[i]);
            return ret;
        }
    }                
    return RDB_OK;
}

static int
update_stored_complex(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_transaction *txp)
{
    RDB_table *tmptbp = NULL;
    RDB_object tpl;
    int ret, ret2;
    int i;
    void *datap;
    size_t len;
    RDB_bool b;
    RDB_transaction tx;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    RDB_cursor *curp = NULL;
    RDB_object *valv = malloc(sizeof(RDB_object) * updc);

    if (valv == NULL) {
        return RDB_NO_MEMORY;
    }

    /* Start subtransaction */
    ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);
    RDB_init_obj(&tpl);

    /*
     * Iterate over the records and insert the updated records into
     * a temporary table. For the temporary table, only one key is needed.
     */
    ret = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
            1, tbp->keyv, &tx, &tmptbp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_recmap_cursor(&curp, tbp->stp->recmapp, RDB_TRUE,
            tbp->is_persistent ? tx.txid : NULL);
    if (ret != RDB_OK)        
        goto cleanup;
    ret = RDB_cursor_first(curp);
    
    while (ret == RDB_OK) {
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp,
                    *(int*) RDB_hashmap_get(&tbp->stp->attrmap,
                            tpltyp->var.tuple.attrv[i].name, NULL),
                    &datap, &len);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            RDB_init_obj(&val);
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val);
            RDB_destroy_obj(&val);
            if (ret != RDB_OK) {
                goto cleanup;
            }            
            ret = RDB_irep_to_obj(
                    RDB_tuple_get(&tpl, tpltyp->var.tuple.attrv[i].name),
                    tpltyp->var.tuple.attrv[i].typ, datap, len);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        }

        /* Evaluate condition */
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            ret = RDB_evaluate_bool(condp, &tpl, &tx, &b);
            if (ret != RDB_OK) {
                return ret;
            }
        }
        if (b) {
            ret = upd_to_vals(updc, updv, &tpl, valv, &tx);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Update tuple */
                ret = RDB_tuple_set(&tpl, updv[i].name, &valv[i]);
                if (ret != RDB_OK)
                    goto cleanup;
            }
            
            /* Insert tuple into temporary table */
            ret = RDB_insert(tmptbp, &tpl, &tx);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        }
        ret = RDB_cursor_next(curp, 0);
    };

    if (ret != RDB_NOT_FOUND)
        goto cleanup;

    /*
     * Delete the updated records from the original table.
     */

    /* Reset cursor */
    ret = RDB_cursor_first(curp);
    
    while (ret == RDB_OK) {
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp,
                    *(int*) RDB_hashmap_get(&tbp->stp->attrmap,
                            tpltyp->var.tuple.attrv[i].name, NULL),
                    &datap, &len);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            RDB_init_obj(&val);
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val);
            RDB_destroy_obj(&val);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            ret = RDB_irep_to_obj(
                    RDB_tuple_get(&tpl, tpltyp->var.tuple.attrv[i].name),
                    tpltyp->var.tuple.attrv[i].typ, datap, len);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        }

        /* Evaluate condition */
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            ret = RDB_evaluate_bool(condp, &tpl, &tx, &b);
            if (ret != RDB_OK) {
                return ret;
            }
        }
        if (b) {
            /* Delete tuple */
            RDB_cursor_delete(curp);
        }
        ret = RDB_cursor_next(curp, 0);
    }
    ret = RDB_destroy_cursor(curp);
    curp = NULL;
    if (ret != RDB_OK)
        goto cleanup;

    /*
     * Insert the records from the temporary table into the original table.
     */
     ret = _RDB_move_tuples(tbp, tmptbp, &tx);

cleanup:
    free(valv);

    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, &tx);
    if (curp != NULL) {
        ret2 = RDB_destroy_cursor(curp);
        if (ret == RDB_OK)
            ret = ret2;
    }
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i]);
    ret2 = RDB_destroy_obj(&tpl);
    if (ret == RDB_OK)
        ret = ret2;

    if (ret == RDB_OK) {
        return RDB_commit(&tx);
    }
    RDB_rollback(&tx);
    return ret;
}

static int
update_stored_simple(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[], RDB_transaction *txp)
{
    RDB_object tpl;
    int ret, ret2;
    int i;
    void *datap;
    size_t len;
    RDB_bool b;
    RDB_transaction tx;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    RDB_cursor *curp = NULL;
    RDB_object *valv = malloc(sizeof(RDB_object) * updc);
    RDB_field *fieldv = malloc(sizeof(RDB_field) * updc);

    if (valv == NULL || fieldv == NULL) {
        free(valv);
        free(fieldv);
        return RDB_NO_MEMORY;
    }

    /* Start subtransaction */
    ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);
    RDB_init_obj(&tpl);

    /*
     * Iterator over the records and update them if the select expression
     * evaluates to true.
     */
    ret = RDB_recmap_cursor(&curp, tbp->stp->recmapp, RDB_TRUE,
            tbp->is_persistent ? tx.txid : NULL);
    if (ret != RDB_OK)        
        return ret;

    ret = RDB_cursor_first(curp);
    while (ret == RDB_OK) {
        /* Read tuple */
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp,
                    *(int*) RDB_hashmap_get(
                            &tbp->stp->attrmap,
                            tpltyp->var.tuple.attrv[i].name, NULL),
                    &datap, &len);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            RDB_init_obj(&val);
            ret = RDB_irep_to_obj(&val, tpltyp->var.tuple.attrv[i].typ,
                              datap, len);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                goto cleanup;
            }
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val);
            RDB_destroy_obj(&val);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        }
        
        /* Evaluate condition */
        if (condp != NULL) {
            ret = RDB_evaluate_bool(condp, &tpl, &tx, &b);
            if (ret != RDB_OK)
                return ret;
        } else {
            b = RDB_TRUE;
        }

        if (b) {
            /* Perform update */
            ret = upd_to_vals(updc, updv, &tpl, valv, &tx);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Get field number from map */
                fieldv[i].no = *(int*) RDB_hashmap_get(
                        &tbp->stp->attrmap,
                        updv[i].name, NULL);

                /* Set type - needed for tuple and array attributes */
                if (valv[i].typ == NULL
                        && (valv[i].kind == RDB_OB_TUPLE
                         || valv[i].kind == RDB_OB_ARRAY)) {
                    valv[i].typ = RDB_type_attr_type(RDB_table_type(tbp),
                            updv[i].name);
                }

                /* Get data */
                ret = _RDB_obj_to_field(&fieldv[i], &valv[i]);
                if (ret != RDB_OK)
                    goto cleanup;
            }
            ret = RDB_cursor_set(curp, updc, fieldv);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        }
        ret = RDB_cursor_next(curp, 0);
    };

    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;
cleanup:
    free(valv);
    free(fieldv);

    if (curp != NULL) {
        ret2 = RDB_destroy_cursor(curp);
        if (ret == RDB_OK)
            ret = ret2;
    }
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i]);
    ret2 = RDB_destroy_obj(&tpl);
    if (ret == RDB_OK)
        ret = ret2;

    if (ret == RDB_OK) {
        return RDB_commit(&tx);
    }
    RDB_rollback(&tx);
    return ret;
}

int
_RDB_update_select_pindex(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[], RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;
    int i;
    RDB_bool b;
    _RDB_tbindex *indexp = tbp->var.select.tbp->var.project.indexp;
    int objc = indexp->attrc;
    RDB_field *fvv = malloc(sizeof(RDB_field) * objc);
    RDB_object *valv = malloc(sizeof(RDB_object) * updc);
    RDB_field *fieldv = malloc(sizeof(RDB_field) * updc);

    if (fvv == NULL || valv == NULL || fieldv == NULL) {
        free(fvv);
        free(valv);
        free(fieldv);
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        ret = _RDB_obj_to_field(&fvv[i], tbp->var.select.objpv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }

    /* Read tuple */
    RDB_init_obj(&tpl);
    ret = _RDB_get_by_uindex(tbp->var.select.tbp->var.project.tbp,
            tbp->var.select.objpv, indexp,
            tbp->typ->var.basetyp, txp, &tpl);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        if (ret == RDB_NOT_FOUND)
            ret = RDB_OK;
        goto cleanup;
    }

    if (condp != NULL) {
        /*
         * Check condition
         */
        ret = RDB_evaluate_bool(condp, &tpl, txp, &b);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            goto cleanup;
        }

        if (!b) {
            RDB_destroy_obj(&tpl);
            goto cleanup;
        }
    }

    ret = upd_to_vals(updc, updv, &tpl, valv, txp);
    RDB_destroy_obj(&tpl);
    if (ret != RDB_OK)
        goto cleanup;

    for (i = 0; i < updc; i++) {
        fieldv[i].no = *(int*) RDB_hashmap_get(
                 &tbp->var.select.tbp->var.project.tbp->stp->attrmap,
                 updv[i].name, NULL);
         
        /* Set type - needed for tuple and array attributes */
        if (valv[i].typ == NULL
                && (valv[i].kind == RDB_OB_TUPLE
                 || valv[i].kind == RDB_OB_ARRAY)) {
            valv[i].typ = RDB_type_attr_type(
                    RDB_table_type(tbp->var.select.tbp), updv[i].name);
        }
        ret = _RDB_obj_to_field(&fieldv[i], &valv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }
        
    ret = RDB_update_rec(tbp->var.select.tbp->var.project.tbp->stp->recmapp,
            fvv, updc, fieldv, tbp->var.select.tbp->var.project.tbp->is_persistent ?
            txp->txid : NULL);

cleanup:
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i]);
    free(valv);
    free(fieldv);
    free(fvv);

    return ret;
}

static int
update_select_index_simple(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[], RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_transaction tx;
    int ret, ret2;
    int i;
    int flags;
    _RDB_tbindex *indexp = tbp->var.select.tbp->var.project.indexp;
    int objc = indexp->attrc;
    RDB_cursor *curp = NULL;
    RDB_field *fv = malloc(sizeof(RDB_field) * objc);
    RDB_object *valv = malloc(sizeof(RDB_object) * updc);
    RDB_field *fieldv = malloc(sizeof(RDB_field) * updc);

    if (fv == NULL || valv == NULL || fieldv == NULL) {
        free(fv);
        free(valv);
        free(fieldv);
        return RDB_NO_MEMORY;
    }

    /* Start subtransaction */
    ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK) {
        free(fv);
        free(valv);
        free(fieldv);
        return ret;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    ret = RDB_index_cursor(&curp, indexp->idxp, RDB_TRUE,
            tbp->var.select.tbp->var.project.tbp->is_persistent ? tx.txid : NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        ret = _RDB_obj_to_field(&fv[i], tbp->var.select.objpv[i]);
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
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        RDB_init_obj(&tpl);
        ret = _RDB_get_by_cursor(tbp->var.select.tbp->var.project.tbp, curp,
                tbp->typ->var.basetyp, &tpl);
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
            /*
             * Check condition
             */
            ret = RDB_evaluate_bool(condp, &tpl, &tx, &upd);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                goto cleanup;
            }
        }

        ret = RDB_evaluate_bool(tbp->var.select.exp, &tpl, &tx, &b);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            goto cleanup;
        }
        upd = (RDB_bool) (upd && b);

        if (upd) {
            ret = upd_to_vals(updc, updv, &tpl, valv, &tx);
            RDB_destroy_obj(&tpl);
            if (ret != RDB_OK)
                goto cleanup;

            for (i = 0; i < updc; i++) {
                fieldv[i].no = *(int*) RDB_hashmap_get(
                         &tbp->var.select.tbp->var.project.tbp->stp->attrmap,
                         updv[i].name, NULL);
                 
                /* Set type - needed for tuple and array attributes */
                if (valv[i].typ == NULL
                        && (valv[i].kind == RDB_OB_TUPLE
                         || valv[i].kind == RDB_OB_ARRAY)) {
                    valv[i].typ = RDB_type_attr_type(
                            RDB_table_type(tbp->var.select.tbp), updv[i].name);
                }
                ret = _RDB_obj_to_field(&fieldv[i], &valv[i]);
                if (ret != RDB_OK)
                    goto cleanup;
            }
                
            ret = RDB_cursor_update(curp, updc, fieldv);
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

    for (i = 0; i < updc; i++) {
        ret2 = RDB_destroy_obj(&valv[i]);
        if (ret2 != RDB_OK && ret == RDB_OK)
            ret = ret2;
    }
    free(valv);
    free(fieldv);
    free(fv);

    if (ret == RDB_OK) {
        return RDB_commit(&tx);
    }
    RDB_rollback(&tx);
    return ret;
}

static RDB_bool
upd_complex(RDB_table *tbp, int updc, const RDB_attr_update updv[])
{
    int i;

    /*
     * If a key is updated, the simple update method cannot be used
     */

    /* Check if a key attribute is updated */
    for (i = 0; i < updc; i++) {
        if (is_keyattr(updv[i].name, tbp)) {
            return RDB_TRUE;
        }
    }

    /* Check if one of the expressions refers to the table itself */

    for (i = 0; i < updc; i++) {
        if (_RDB_expr_refers(updv[i].exp, tbp))
            return RDB_TRUE;
    }

    return RDB_FALSE;
}

int
_RDB_update_real(RDB_table *tbp, RDB_expression *condp, int updc,
        const RDB_attr_update updv[], RDB_transaction *txp)
{
    if (upd_complex(tbp, updc, updv)
            || (condp != NULL && _RDB_expr_refers(condp, tbp)))
        return update_stored_complex(tbp, condp, updc, updv, txp);
    return update_stored_simple(tbp, condp, updc, updv, txp);
}

static int
update_select(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[], RDB_transaction *txp);

int
_RDB_update_select_index(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[], RDB_transaction *txp)
{
    if (upd_complex(tbp, updc, updv)
        || _RDB_expr_refers(tbp->var.select.exp, tbp->var.select.tbp)
        || (condp != NULL && _RDB_expr_refers(condp, tbp->var.select.tbp))) {

        /* !! Should do a complex update by index, but this is not implemented */
        return update_select(tbp, condp, updc, updv, txp);
    }
    return update_select_index_simple(tbp, condp, updc, updv, txp);
}

static int
update(RDB_table *tbp, RDB_expression *condp, int updc,
        const RDB_attr_update updv[], RDB_transaction *);

static int
update_select(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[], RDB_transaction *txp)
{
    int ret;
    RDB_expression *ncondp = NULL;

    if (condp != NULL) {
        ncondp = RDB_ro_op_va("AND", tbp->var.select.exp, condp,
                (RDB_expression *) NULL);
        if (ncondp == NULL)
            return RDB_NO_MEMORY;
    }
    ret = update(tbp->var.select.tbp,
            ncondp != NULL ? ncondp : tbp->var.select.exp, updc, updv, txp);
    free(ncondp);
    return ret;
}

static int
update(RDB_table *tbp, RDB_expression *condp, int updc,
        const RDB_attr_update updv[], RDB_transaction *txp)
{
    switch (tbp->kind) {
        case RDB_TB_REAL:
            return _RDB_update_real(tbp, condp, updc, updv, txp);
        case RDB_TB_UNION:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_MINUS:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_INTERSECT:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_SELECT:
            if (tbp->var.select.tbp->kind != RDB_TB_PROJECT
                    || tbp->var.select.tbp->var.project.indexp == NULL
                    || tbp->var.select.objpc == 0)
                return update_select(tbp, condp, updc, updv, txp);
            if (tbp->var.select.tbp->var.project.indexp->idxp == NULL)
                return _RDB_update_select_pindex(tbp, condp, updc, updv, txp);
            return _RDB_update_select_index(tbp, condp, updc, updv, txp);
        case RDB_TB_JOIN:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_EXTEND:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_PROJECT:
            return update(tbp->var.project.tbp, condp, updc, updv, txp);
        case RDB_TB_SUMMARIZE:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_RENAME:
            return RDB_NOT_SUPPORTED;
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
RDB_update(RDB_table *tbp, RDB_expression *condp, int updc,
                const RDB_attr_update updv[], RDB_transaction *txp)
{
    RDB_ma_update upd;

    upd.tbp = tbp;
    upd.condp = condp;
    upd.updc = updc;
    upd.updv = (RDB_attr_update *) updv;
    return RDB_multi_assign(0, NULL, 1, &upd, 0, NULL, 0, NULL, txp);
}
