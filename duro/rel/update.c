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
is_keyattr(const char *attrname, RDB_table *tbp, RDB_exec_context *ecp)
{
    int i, j;
    int keyc;
    RDB_string_vec *keyv;
    
    keyc = RDB_table_keys(tbp, ecp, &keyv);
    for (i = 0; i < keyc; i++)
        for (j = 0; j < keyv[i].strc; j++)
            if (strcmp(attrname, keyv[i].strv[j]) == 0)
                return RDB_TRUE;
    return RDB_FALSE;
}

static int
upd_to_vals(int updc, const RDB_attr_update updv[],
            RDB_object *tplp, RDB_object *valv,
            RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, ret;

    for (i = 0; i < updc; i++) {
        ret = RDB_evaluate(updv[i].exp, tplp, ecp, txp, &valv[i]);
        if (ret != RDB_OK) {
            int j;
            
            for (j = 0; j < i; j++)
                RDB_destroy_obj(&valv[i], ecp);
            return ret;
        }
    }                
    return RDB_OK;
}

static RDB_int
update_stored_complex(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    int ret;
    int i;
    void *datap;
    size_t len;
    RDB_bool b;
    RDB_transaction tx;
    RDB_table *tmptbp = NULL;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    RDB_cursor *curp = NULL;
    RDB_object *valv = malloc(sizeof(RDB_object) * updc);

    if (valv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /* Start subtransaction */
    if (RDB_begin_tx(ecp, &tx, RDB_tx_db(txp), txp) != RDB_OK) {
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);
    RDB_init_obj(&tpl);

    /*
     * Iterate over the records and insert the updated records into
     * a temporary table. For the temporary table, only one key is needed.
     */

    tmptbp = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
            1, tbp->keyv, ecp, &tx);
    if (tmptbp == NULL) {
        rcount = RDB_ERROR;
        goto cleanup;
    }

    if (RDB_recmap_cursor(&curp, tbp->stp->recmapp, RDB_TRUE,
            tbp->is_persistent ? tx.txid : NULL) != RDB_OK) {
        rcount = RDB_ERROR;
        goto cleanup;
    }
    ret = RDB_cursor_first(curp);
    rcount = 0;
    while (ret == RDB_OK) {
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;
            RDB_object *attrobjp;

            ret = RDB_cursor_get(curp,
                    *_RDB_field_no(tbp->stp, tpltyp->var.tuple.attrv[i].name),
                    &datap, &len);
            if (ret != RDB_OK) {
                _RDB_handle_errcode(ret, ecp, &tx);
                rcount = RDB_ERROR;
                goto cleanup;
            }

            /*
             * First set tuple attribute to 'empty' value,
             * then update tuple attribute
             */
            RDB_init_obj(&val);
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val, ecp);
            RDB_destroy_obj(&val, ecp);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            attrobjp = RDB_tuple_get(&tpl, tpltyp->var.tuple.attrv[i].name);
            if (attrobjp == NULL) {
                RDB_raise_attribute_not_found(tpltyp->var.tuple.attrv[i].name, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            ret = RDB_irep_to_obj(attrobjp, tpltyp->var.tuple.attrv[i].typ,
                    datap, len, ecp);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        /* Evaluate condition */
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            ret = RDB_evaluate_bool(condp, &tpl, ecp, &tx, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                return ret;
            }
        }
        if (b) {
            ret = upd_to_vals(updc, updv, &tpl, valv, ecp, &tx);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Update tuple */
                ret = RDB_tuple_set(&tpl, updv[i].name, &valv[i], ecp);
                if (ret != RDB_OK)
                    goto cleanup;
            }
            
            /* Insert tuple into temporary table */
            ret = _RDB_insert_real(tmptbp, &tpl, ecp, &tx);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            rcount++;
        }
        ret = RDB_cursor_next(curp, 0);
    };

    if (ret != DB_NOTFOUND) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Delete the updated records from the original table.
     */

    /* Reset cursor */
    ret = RDB_cursor_first(curp);
    
    _RDB_cmp_ecp = ecp;
    while (ret == RDB_OK) {
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp,
                    *_RDB_field_no(tbp->stp, tpltyp->var.tuple.attrv[i].name),
                    &datap, &len);
            if (ret != RDB_OK) {
                _RDB_handle_errcode(ret, ecp, &tx);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            RDB_init_obj(&val);
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val,
                    ecp);
            RDB_destroy_obj(&val, ecp);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            ret = RDB_irep_to_obj(
                    RDB_tuple_get(&tpl, tpltyp->var.tuple.attrv[i].name),
                    tpltyp->var.tuple.attrv[i].typ, datap, len, ecp);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        }

        /* Evaluate condition */
        if (condp == NULL) {
            b = RDB_TRUE;
        } else {
            ret = RDB_evaluate_bool(condp, &tpl, ecp, &tx, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        if (b) {
            /* Delete tuple */
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                _RDB_handle_errcode(ret, ecp, &tx);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        ret = RDB_cursor_next(curp, 0);
    };

    if (ret != DB_NOTFOUND) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }
    
    ret = RDB_destroy_cursor(curp);
    curp = NULL;
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Insert the records from the temporary table into the original table.
     */
    if (_RDB_move_tuples(tbp, tmptbp, ecp, &tx) != RDB_OK) {
        rcount = RDB_ERROR;
    }

cleanup:
    free(valv);

    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, ecp, &tx);
    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, &tx);
            rcount = RDB_ERROR;
        }
    }
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    if (RDB_destroy_obj(&tpl, ecp) != RDB_OK) {
        rcount = RDB_ERROR;
    }

    if (rcount == RDB_ERROR) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    if (RDB_commit(ecp, &tx) != RDB_OK) {
        return RDB_ERROR;
    }
    return rcount;
}

static RDB_int
update_stored_simple(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[], RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    int ret;
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
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /* Start subtransaction */
    ret = RDB_begin_tx(ecp, &tx, RDB_tx_db(txp), txp);
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
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    rcount = 0;
    ret = RDB_cursor_first(curp);
    while (ret == RDB_OK) {
        /* Read tuple */
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp,
                    *_RDB_field_no(tbp->stp, tpltyp->var.tuple.attrv[i].name),
                    &datap, &len);
            if (ret != RDB_OK) {
                _RDB_handle_errcode(ret, ecp, &tx);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            RDB_init_obj(&val);
            if (RDB_irep_to_obj(&val, tpltyp->var.tuple.attrv[i].typ,
                              datap, len, ecp) != RDB_OK) {
                RDB_destroy_obj(&val, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val,
                    ecp);
            RDB_destroy_obj(&val, ecp);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        
        /* Evaluate condition */
        if (condp != NULL) {
            ret = RDB_evaluate_bool(condp, &tpl, ecp, &tx, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        } else {
            b = RDB_TRUE;
        }

        if (b) {
            /* Perform update */
            if (upd_to_vals(updc, updv, &tpl, valv, ecp, &tx) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Get field number from map */
                fieldv[i].no = *_RDB_field_no(tbp->stp, updv[i].name);

                /* Set type - needed for tuple and array attributes */
                if (valv[i].typ == NULL
                        && (valv[i].kind == RDB_OB_TUPLE
                         || valv[i].kind == RDB_OB_ARRAY)) {
                    valv[i].typ = RDB_type_attr_type(RDB_table_type(tbp),
                            updv[i].name);
                }

                /* Get data */
                if (_RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
                    ret = RDB_ERROR;
                    goto cleanup;
                }
            }
            ret = RDB_cursor_set(curp, updc, fieldv);
            if (ret != RDB_OK) {
                _RDB_handle_errcode(ret, ecp, txp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        ret = RDB_cursor_next(curp, 0);
    };

    if (ret != DB_NOTFOUND) {
        _RDB_handle_errcode(ret, ecp, txp);
        rcount = RDB_ERROR;
     }

cleanup:
    free(valv);
    free(fieldv);

    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, txp);
            rcount = RDB_ERROR;
        }
    }
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    ret = RDB_destroy_obj(&tpl, ecp);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        rcount = RDB_ERROR;
    }
    if (rcount == RDB_ERROR) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }
    return rcount;
}

RDB_int
_RDB_update_select_pindex(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int rcount;
    RDB_object tpl;
    int ret;
    int i;
    RDB_bool b;
    _RDB_tbindex *indexp = tbp->var.select.tbp->var.project.indexp;
    int objc = indexp->attrc;
    RDB_field *fvv;
    RDB_object *valv;
    RDB_field *fieldv;

    if (tbp->var.select.tbp->var.project.tbp->stp == NULL)
        return 0;

    fvv = malloc(sizeof(RDB_field) * objc);
    valv = malloc(sizeof(RDB_object) * updc);
    fieldv = malloc(sizeof(RDB_field) * updc);

    if (fvv == NULL || valv == NULL || fieldv == NULL) {
        free(fvv);
        free(valv);
        free(fieldv);
        RDB_raise_no_memory(ecp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        if (_RDB_obj_to_field(&fvv[i], tbp->var.select.objpv[i], ecp)
                != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    /* Read tuple */
    RDB_init_obj(&tpl);
    ret = _RDB_get_by_uindex(tbp->var.select.tbp->var.project.tbp,
            tbp->var.select.objpv, indexp,
            tbp->typ->var.basetyp, ecp, txp, &tpl);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        if (ret == DB_NOTFOUND) {
            rcount = RDB_OK;
        } else {
            rcount = RDB_ERROR;
        }
        goto cleanup;
    }

    if (condp != NULL) {
        /*
         * Check condition
         */
        if (RDB_evaluate_bool(condp, &tpl, ecp, txp, &b) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }

        if (!b) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = 0;
            goto cleanup;
        }
    }

    ret = upd_to_vals(updc, updv, &tpl, valv, ecp, txp);
    RDB_destroy_obj(&tpl, ecp);
    if (ret != RDB_OK) {
        rcount = RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < updc; i++) {
        fieldv[i].no = *_RDB_field_no(
                 tbp->var.select.tbp->var.project.tbp->stp, updv[i].name);
         
        /* Set type - needed for tuple and array attributes */
        if (valv[i].typ == NULL
                && (valv[i].kind == RDB_OB_TUPLE
                 || valv[i].kind == RDB_OB_ARRAY)) {
            valv[i].typ = RDB_type_attr_type(
                    RDB_table_type(tbp->var.select.tbp), updv[i].name);
        }
        if (_RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }
        
    _RDB_cmp_ecp = ecp;
    ret = RDB_update_rec(tbp->var.select.tbp->var.project.tbp->stp->recmapp,
            fvv, updc, fieldv,
            tbp->var.select.tbp->var.project.tbp->is_persistent ?
                    txp->txid : NULL);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        rcount = RDB_ERROR;
    } else {
       rcount = 1;
    }

cleanup:
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    free(valv);
    free(fieldv);
    free(fvv);

    return rcount;
}

static RDB_int
update_select_index_simple(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    RDB_transaction tx;
    int ret;
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
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /* Start subtransaction */
    ret = RDB_begin_tx(ecp, &tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK) {
        free(fv);
        free(valv);
        free(fieldv);
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    ret = RDB_index_cursor(&curp, indexp->idxp, RDB_TRUE,
            tbp->var.select.tbp->var.project.tbp->is_persistent ? tx.txid : NULL);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, &tx);
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        if (_RDB_obj_to_field(&fv[i], tbp->var.select.objpv[i], ecp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    if (tbp->var.select.objpc != indexp->attrc
            || !tbp->var.select.all_eq)
        flags = RDB_REC_RANGE;
    else
        flags = 0;

    ret = RDB_cursor_seek(curp, tbp->var.select.objpc, fv, flags);
    if (ret == DB_NOTFOUND) {
        rcount = 0;
        goto cleanup;
    }

    rcount = 0;
    _RDB_cmp_ecp = ecp;
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        RDB_init_obj(&tpl);
        ret = _RDB_get_by_cursor(tbp->var.select.tbp->var.project.tbp, curp,
                tbp->typ->var.basetyp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }

        if (tbp->var.select.stopexp != NULL) {
            ret = RDB_evaluate_bool(tbp->var.select.stopexp, &tpl,
                    ecp, txp, &b);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (!b) {
                rcount = RDB_OK;
                RDB_destroy_obj(&tpl, ecp);
                goto cleanup;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &tpl, ecp, &tx, &upd) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(tbp->var.select.exp, &tpl, ecp, &tx, &b)
                != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        upd = (RDB_bool) (upd && b);

        if (upd) {
            ret = upd_to_vals(updc, updv, &tpl, valv, ecp, &tx);
            RDB_destroy_obj(&tpl, ecp);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }

            for (i = 0; i < updc; i++) {
                fieldv[i].no = *_RDB_field_no(
                        tbp->var.select.tbp->var.project.tbp->stp, updv[i].name);
                 
                /* Set type - needed for tuple and array attributes */
                if (valv[i].typ == NULL
                        && (valv[i].kind == RDB_OB_TUPLE
                         || valv[i].kind == RDB_OB_ARRAY)) {
                    valv[i].typ = RDB_type_attr_type(
                            RDB_table_type(tbp->var.select.tbp), updv[i].name);
                }
                if (_RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
                    rcount = RDB_ERROR;
                    goto cleanup;
                }
            }
                
            _RDB_cmp_ecp = ecp;
            ret = RDB_cursor_update(curp, updc, fieldv);
            if (ret != RDB_OK) {
                _RDB_handle_errcode(ret, ecp, txp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        } else {
            RDB_destroy_obj(&tpl, ecp);
        }
        if (tbp->var.select.objpc == indexp->attrc
                && tbp->var.select.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;

        ret = RDB_cursor_next(curp, flags);
    } while (ret == RDB_OK);

    if (ret != DB_NOTFOUND) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
    }

cleanup:
    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                _RDB_handle_errcode(ret, ecp, &tx);
                rcount = RDB_ERROR;
            }
        }
    }

    for (i = 0; i < updc; i++) {
        ret = RDB_destroy_obj(&valv[i], ecp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                _RDB_handle_errcode(ret, ecp, &tx);
                rcount = RDB_ERROR;
            }
        }
    }
    free(valv);
    free(fieldv);
    free(fv);

    if (rcount == RDB_ERROR) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }
    return rcount;
}

static RDB_int
update_select_index_complex(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    RDB_transaction tx;
    int ret;
    int i;
    int flags;
    RDB_table *tmptbp = NULL;
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
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /* Start subtransaction */
    ret = RDB_begin_tx(ecp, &tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK) {
        free(fv);
        free(valv);
        free(fieldv);
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    /*
     * Iterate over the records and insert the updated records into
     * a temporary table. For the temporary table, only one key is needed.
     */

    tmptbp = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
            1, tbp->keyv, ecp, &tx);
    if (tmptbp == NULL) {
        rcount = RDB_ERROR;
        goto cleanup;
    }

    ret = RDB_index_cursor(&curp, indexp->idxp, RDB_TRUE,
            tbp->var.select.tbp->var.project.tbp->is_persistent ? tx.txid : NULL);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        if (_RDB_obj_to_field(&fv[i], tbp->var.select.objpv[i], ecp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    if (tbp->var.select.objpc != indexp->attrc
            || !tbp->var.select.all_eq)
        flags = RDB_REC_RANGE;
    else
        flags = 0;

    /* Set cursor position */
    ret = RDB_cursor_seek(curp, tbp->var.select.objpc, fv, flags);
    if (ret == DB_NOTFOUND) {
        rcount = RDB_OK;
        goto cleanup;
    }
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    rcount = 0;
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        RDB_init_obj(&tpl);
        if (_RDB_get_by_cursor(tbp->var.select.tbp->var.project.tbp, curp,
                tbp->typ->var.basetyp, &tpl, ecp, txp) != RDB_OK) {
            rcount = RDB_ERROR;
            RDB_destroy_obj(&tpl, ecp);
            goto cleanup;
        }

        if (tbp->var.select.stopexp != NULL) {
            if (RDB_evaluate_bool(tbp->var.select.stopexp, &tpl,
                    ecp, txp, &b) != RDB_OK) {
                rcount = RDB_ERROR;
                RDB_destroy_obj(&tpl, ecp);
                goto cleanup;
            }
            if (!b) {
                ret = RDB_OK;
                RDB_destroy_obj(&tpl, ecp);
                goto cleanup;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &tpl, ecp, &tx, &upd) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(tbp->var.select.exp, &tpl, ecp, &tx, &b) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        upd = (RDB_bool) (upd && b);

        if (upd) {
            if (upd_to_vals(updc, updv, &tpl, valv, ecp, &tx) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Update tuple */
                if (RDB_tuple_set(&tpl, updv[i].name, &valv[i], ecp) != RDB_OK) {
                    rcount = RDB_ERROR;
                    goto cleanup;
                }
            }
            
            /* Insert tuple into temporary table */
            if (RDB_insert(tmptbp, &tpl, ecp, &tx) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        if (tbp->var.select.objpc == indexp->attrc
                && tbp->var.select.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;

        ret = RDB_cursor_next(curp, flags);
    } while (ret == RDB_OK);

    if (ret != DB_NOTFOUND) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Delete the updated records from the original table.
     */

    /* Reset cursor */
    ret = RDB_cursor_seek(curp, tbp->var.select.objpc, fv, flags);
    if (ret == DB_NOTFOUND) {
        ret = RDB_OK;
        goto cleanup;
    }
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    _RDB_cmp_ecp = ecp;
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        RDB_init_obj(&tpl);
        ret = _RDB_get_by_cursor(tbp->var.select.tbp->var.project.tbp, curp,
                tbp->typ->var.basetyp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }

        if (tbp->var.select.stopexp != NULL) {
            if (RDB_evaluate_bool(tbp->var.select.stopexp, &tpl, ecp, txp,
                    &b) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (!b) {
                rcount = RDB_OK;
                RDB_destroy_obj(&tpl, ecp);
                goto cleanup;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &tpl, ecp, &tx, &upd) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(tbp->var.select.exp, &tpl, ecp, &tx, &b)
                != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        upd = (RDB_bool) (upd && b);

        if (upd) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                _RDB_handle_errcode(ret, ecp, &tx);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        if (tbp->var.select.objpc == indexp->attrc
                && tbp->var.select.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;

        ret = RDB_cursor_next(curp, flags);
    } while (ret == RDB_OK);

    if (ret != DB_NOTFOUND) {
        _RDB_handle_errcode(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Insert the records from the temporary table into the original table.
     */
     if (_RDB_move_tuples(tbp->var.select.tbp->var.project.tbp, tmptbp, ecp,
                          &tx) != RDB_OK) {
         rcount = RDB_ERROR;
     }

cleanup:
    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, ecp, &tx);

    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                _RDB_handle_errcode(ret, ecp, &tx);
                rcount = RDB_ERROR;
            }
        }
    }

    for (i = 0; i < updc; i++) {
        RDB_destroy_obj(&valv[i], ecp);
    }
    free(valv);
    free(fieldv);
    free(fv);

    if (rcount == RDB_ERROR) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }
    return rcount;
}

static RDB_bool
upd_complex(RDB_table *tbp, int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp)
{
    int i;

    /*
     * If a key is updated, the simple update method cannot be used
     */

    /* Check if a key attribute is updated */
    for (i = 0; i < updc; i++) {
        if (is_keyattr(updv[i].name, tbp, ecp)) {
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

RDB_int
_RDB_update_real(RDB_table *tbp, RDB_expression *condp, int updc,
        const RDB_attr_update updv[], RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (tbp->stp == NULL)
        return RDB_OK;

    if (upd_complex(tbp, updc, updv, ecp)
            || (condp != NULL && _RDB_expr_refers(condp, tbp)))
        return update_stored_complex(tbp, condp, updc, updv, ecp, txp);
    return update_stored_simple(tbp, condp, updc, updv, ecp, txp);
}

RDB_int
_RDB_update_select_index(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (tbp->var.select.tbp->var.project.tbp->stp == NULL)
        return 0;

    if (upd_complex(tbp, updc, updv, ecp)
        || _RDB_expr_refers(tbp->var.select.exp, tbp->var.select.tbp)
        || (condp != NULL && _RDB_expr_refers(condp, tbp->var.select.tbp))) {
        return update_select_index_complex(tbp, condp, updc, updv, ecp, txp);
    }
    return update_select_index_simple(tbp, condp, updc, updv, ecp, txp);
}

RDB_int
RDB_update(RDB_table *tbp, RDB_expression *condp, int updc,
           const RDB_attr_update updv[], RDB_exec_context *ecp,
           RDB_transaction *txp)
{
    RDB_ma_update upd;

    upd.tbp = tbp;
    upd.condp = condp;
    upd.updc = updc;
    upd.updv = (RDB_attr_update *) updv;
    return RDB_multi_assign(0, NULL, 1, &upd, 0, NULL, 0, NULL, ecp, txp);
}
