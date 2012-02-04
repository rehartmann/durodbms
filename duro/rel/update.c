/*
 * $Id$
 *
 * Copyright (C) 2003-2008 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "update.h"
#include "typeimpl.h"
#include "qresult.h"
#include "insert.h"
#include "internal.h"
#include "stable.h"
#include <gen/strfns.h>
#include <string.h>

static RDB_bool
is_keyattr(const char *attrname, RDB_object *tbp, RDB_exec_context *ecp)
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
        ret = RDB_evaluate(updv[i].exp, &_RDB_tpl_get, tplp, ecp, txp, &valv[i]);
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
update_stored_complex(RDB_object *tbp, RDB_expression *condp,
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
    RDB_object tmptb;
    RDB_type *tmptbtyp;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    RDB_cursor *curp = NULL;
    RDB_object *valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    if (valv == NULL) {
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
    tmptbtyp = RDB_dup_nonscalar_type(tbp->typ, ecp);
    if (tmptbtyp == NULL) {
        rcount = RDB_ERROR;
        goto cleanup;
    }        

    ret = RDB_init_table_from_type(&tmptb, NULL, tmptbtyp, 1, tbp->var.tb.keyv, ecp);
    if (ret != RDB_OK) {
        RDB_drop_type(tmptbtyp, ecp, NULL);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    if (RDB_recmap_cursor(&curp, tbp->var.tb.stp->recmapp, RDB_TRUE,
            tbp->var.tb.is_persistent ? tx.txid : NULL) != RDB_OK) {
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
                    *_RDB_field_no(tbp->var.tb.stp, tpltyp->var.tuple.attrv[i].name),
                    &datap, &len);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp, &tx);
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
                RDB_raise_name(tpltyp->var.tuple.attrv[i].name, ecp);
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
            ret = RDB_evaluate_bool(condp, &_RDB_tpl_get, &tpl, ecp, &tx, &b);
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
            ret = _RDB_insert_real(&tmptb, &tpl, ecp, &tx);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            rcount++;
        }
        ret = RDB_cursor_next(curp, 0);
    };

    if (ret != DB_NOTFOUND) {
        RDB_errcode_to_error(ret, ecp, &tx);
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
                    *_RDB_field_no(tbp->var.tb.stp, tpltyp->var.tuple.attrv[i].name),
                    &datap, &len);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp, &tx);
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
            ret = RDB_evaluate_bool(condp, &_RDB_tpl_get, &tpl, ecp, &tx, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        if (b) {
            /* Delete tuple */
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp, &tx);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        ret = RDB_cursor_next(curp, 0);
    };

    if (ret != DB_NOTFOUND) {
        RDB_errcode_to_error(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }
    
    ret = RDB_destroy_cursor(curp);
    curp = NULL;
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Insert the records from the temporary table into the original table.
     */
    if (RDB_move_tuples(tbp, &tmptb, ecp, &tx) == (RDB_int) RDB_ERROR) {
        rcount = RDB_ERROR;
    }

cleanup:
    RDB_destroy_obj(&tmptb, ecp);
    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp, &tx);
            rcount = RDB_ERROR;
        }
    }
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    RDB_free(valv);

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
update_stored_simple(RDB_object *tbp, RDB_expression *condp,
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
    RDB_object *valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    RDB_field *fieldv = RDB_alloc(sizeof(RDB_field) * updc, ecp);

    if (valv == NULL || fieldv == NULL) {
        RDB_free(valv);
        RDB_free(fieldv);
        return RDB_ERROR;
    }

    if (txp != NULL) {
        /* Start subtransaction */
        if (RDB_begin_tx(ecp, &tx, RDB_tx_db(txp), txp) != RDB_OK)
            return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);
    RDB_init_obj(&tpl);

    /*
     * Iterator over the records and update them if the select expression
     * evaluates to true.
     */
    ret = RDB_recmap_cursor(&curp, tbp->var.tb.stp->recmapp, RDB_TRUE,
            tbp->var.tb.is_persistent ? tx.txid : NULL);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, &tx);
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
                    *_RDB_field_no(tbp->var.tb.stp, tpltyp->var.tuple.attrv[i].name),
                    &datap, &len);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp, txp != NULL ? &tx: NULL);
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
            ret = RDB_evaluate_bool(condp, &_RDB_tpl_get, &tpl, ecp,
                    txp != NULL ? &tx: NULL, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        } else {
            b = RDB_TRUE;
        }

        if (b) {
            /* Perform update */
            if (upd_to_vals(updc, updv, &tpl, valv, ecp, txp != NULL ? &tx: NULL) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Get field number from map */
                fieldv[i].no = *_RDB_field_no(tbp->var.tb.stp, updv[i].name);

                /* Set type - needed for tuple and array attributes */
                valv[i].store_typ = RDB_type_attr_type(RDB_obj_type(tbp),
                            updv[i].name);

                /* Get data */
                if (_RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
                    ret = RDB_ERROR;
                    goto cleanup;
                }
            }
            ret = RDB_cursor_set(curp, updc, fieldv);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp, txp != NULL ? &tx: NULL);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        ret = RDB_cursor_next(curp, 0);
    };

    if (ret != DB_NOTFOUND) {
        RDB_errcode_to_error(ret, ecp, txp != NULL ? &tx: NULL);
        rcount = RDB_ERROR;
     }

cleanup:
    RDB_free(fieldv);

    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp, txp);
            rcount = RDB_ERROR;
        }
    }
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    RDB_free(valv);

    ret = RDB_destroy_obj(&tpl, ecp);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        rcount = RDB_ERROR;
    }
    if (rcount == RDB_ERROR) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    if (txp != NULL) {
        if (RDB_commit(ecp, &tx) != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return rcount;
}

static RDB_int
update_where_pindex(RDB_expression *texp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int rcount;
    RDB_object tpl;
    int ret;
    int i;
    RDB_bool b;
    int objc;
    RDB_field *fvv;
    RDB_object *valv;
    RDB_field *fieldv;
    RDB_expression *refexp;

    if (texp->var.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->var.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->var.op.args.firstp->var.op.args.firstp;
    }
    objc = refexp->var.tbref.indexp->attrc;

    if (refexp->var.tbref.tbp->var.tb.stp == NULL)
        return 0;

    fvv = RDB_alloc(sizeof(RDB_field) * objc, ecp);
    valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    fieldv = RDB_alloc(sizeof(RDB_field) * updc, ecp);

    if (fvv == NULL || valv == NULL || fieldv == NULL) {
        RDB_free(fvv);
        RDB_free(valv);
        RDB_free(fieldv);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        if (_RDB_obj_to_field(&fvv[i], texp->var.op.optinfo.objpv[i], ecp)
                != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    /* Read tuple */
    RDB_init_obj(&tpl);
    ret = _RDB_get_by_uindex(refexp->var.tbref.tbp,
            texp->var.op.optinfo.objpv, refexp->var.tbref.indexp,
            refexp->var.tbref.tbp->typ->var.basetyp, ecp, txp, &tpl);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            rcount = 0;
        } else {
            rcount = RDB_ERROR;
        }
        goto cleanup;
    }

    if (condp != NULL) {
        /*
         * Check condition
         */
        if (RDB_evaluate_bool(condp, &_RDB_tpl_get, &tpl, ecp, txp, &b) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }

        if (!b) {
            rcount = 0;
            goto cleanup;
        }
    }

    ret = upd_to_vals(updc, updv, &tpl, valv, ecp, txp);
    if (ret != RDB_OK) {
        rcount = RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < updc; i++) {
        fieldv[i].no = *_RDB_field_no(
                 refexp->var.tbref.tbp->var.tb.stp, updv[i].name);
         
        /* Set type - needed for tuple and array attributes */
        valv[i].store_typ = RDB_type_attr_type(
                    RDB_obj_type(refexp->var.tbref.tbp), updv[i].name);
        if (_RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }
        
    _RDB_cmp_ecp = ecp;
    ret = RDB_update_rec(refexp->var.tbref.tbp->var.tb.stp->recmapp,
            fvv, updc, fieldv,
            refexp->var.tbref.tbp->var.tb.is_persistent ?
                    txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        rcount = RDB_ERROR;
    } else {
       rcount = 1;
    }

cleanup:
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    RDB_destroy_obj(&tpl, ecp);
    RDB_free(valv);
    RDB_free(fieldv);
    RDB_free(fvv);

    return rcount;
}

static RDB_int
update_where_index_simple(RDB_expression *texp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    RDB_transaction tx;
    int ret;
    int i;
    int flags;
    RDB_expression *refexp;
    int objc;
    RDB_field *fv;
    RDB_object *valv;
    RDB_field *fieldv;
    RDB_cursor *curp = NULL;

    if (texp->var.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->var.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->var.op.args.firstp->var.op.args.firstp;
    }
    objc = refexp->var.tbref.indexp->attrc;

    fv = RDB_alloc(sizeof(RDB_field) * objc, ecp);
    valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    fieldv = RDB_alloc(sizeof(RDB_field) * updc, ecp);

    if (fv == NULL || valv == NULL || fieldv == NULL) {
        RDB_free(fv);
        RDB_free(valv);
        RDB_free(fieldv);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /* Start subtransaction */
    ret = RDB_begin_tx(ecp, &tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK) {
        RDB_free(fv);
        RDB_free(valv);
        RDB_free(fieldv);
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    ret = RDB_index_cursor(&curp, refexp->var.tbref.indexp->idxp, RDB_TRUE,
            refexp->var.tbref.tbp->var.tb.is_persistent ? tx.txid : NULL);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, &tx);
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        if (_RDB_obj_to_field(&fv[i], texp->var.op.optinfo.objpv[i],
                ecp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    if (texp->var.op.optinfo.objpc != refexp->var.tbref.indexp->attrc
            || !texp->var.op.optinfo.all_eq)
        flags = RDB_REC_RANGE;
    else
        flags = 0;

    ret = RDB_cursor_seek(curp, texp->var.op.optinfo.objpc, fv, flags);
    if (ret == DB_NOTFOUND) {
        rcount = 0;
        goto cleanup;
    }

    rcount = 0;
    _RDB_cmp_ecp = ecp;
    RDB_init_obj(&tpl);
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        ret = _RDB_get_by_cursor(refexp->var.tbref.tbp, curp,
                refexp->var.tbref.tbp->typ->var.basetyp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }

        if (texp->var.op.optinfo.stopexp != NULL) {
            ret = RDB_evaluate_bool(texp->var.op.optinfo.stopexp,
                    &_RDB_tpl_get, &tpl, ecp, txp, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (!b) {
                ret = DB_NOTFOUND;
                break;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &_RDB_tpl_get, &tpl, ecp, &tx, &upd)
                    != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(texp->var.op.args.firstp->nextp, &_RDB_tpl_get, &tpl,
                ecp, &tx, &b) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
        upd = (RDB_bool) (upd && b);

        if (upd) {
            ret = upd_to_vals(updc, updv, &tpl, valv, ecp, &tx);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }

            for (i = 0; i < updc; i++) {
                fieldv[i].no = *_RDB_field_no(
                        refexp->var.tbref.tbp->var.tb.stp, updv[i].name);
                 
                /* Set type - needed for tuple and array attributes */
                valv[i].store_typ = RDB_type_attr_type(
                            RDB_obj_type(refexp->var.tbref.tbp), updv[i].name);
                if (_RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
                    rcount = RDB_ERROR;
                    goto cleanup;
                }
            }
                
            _RDB_cmp_ecp = ecp;
            ret = RDB_cursor_update(curp, updc, fieldv);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp, txp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        if (texp->var.op.optinfo.objpc == refexp->var.tbref.indexp->attrc
                && texp->var.op.optinfo.all_eq) {
            flags = RDB_REC_DUP;
        } else {
            flags = 0;
        }

        ret = RDB_cursor_next(curp, flags);
    } while (ret == RDB_OK);

    if (ret != DB_NOTFOUND) {
        RDB_errcode_to_error(ret, ecp, &tx);
        rcount = RDB_ERROR;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);

    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                RDB_errcode_to_error(ret, ecp, &tx);
                rcount = RDB_ERROR;
            }
        }
    }

    for (i = 0; i < updc; i++) {
        ret = RDB_destroy_obj(&valv[i], ecp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                RDB_errcode_to_error(ret, ecp, &tx);
                rcount = RDB_ERROR;
            }
        }
    }
    RDB_free(valv);
    RDB_free(fieldv);
    RDB_free(fv);

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
update_where_index_complex(RDB_expression *texp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    RDB_transaction tx;
    int ret;
    int i;
    int flags;
    RDB_expression *refexp;
    int objc;
    RDB_object tmptb;
    RDB_type *tmptbtyp;
    RDB_cursor *curp = NULL;
    RDB_field *fv;
    RDB_object *valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    RDB_field *fieldv = RDB_alloc(sizeof(RDB_field) * updc, ecp);

    if (valv == NULL || fieldv == NULL) {
        RDB_free(valv);
        RDB_free(fieldv);
        return RDB_ERROR;
    }

    if (texp->var.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->var.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->var.op.args.firstp->var.op.args.firstp;
    }
    objc = refexp->var.tbref.indexp->attrc;

    fv = RDB_alloc(sizeof(RDB_field) * objc, ecp);
    if (fv == NULL) {
        RDB_free(valv);
        RDB_free(fieldv);
        return RDB_ERROR;
    }

    /* Start subtransaction */
    ret = RDB_begin_tx(ecp, &tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK) {
        RDB_free(fv);
        RDB_free(valv);
        RDB_free(fieldv);
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    RDB_init_obj(&tmptb);

    /*
     * Iterate over the records and insert the updated records into
     * a temporary table. For the temporary table, only one key is needed.
     */

    tmptbtyp = RDB_dup_nonscalar_type(refexp->var.tbref.tbp->typ, ecp);
    if (tmptbtyp == NULL) {
        rcount = RDB_ERROR;
        goto cleanup;
    }
    ret = RDB_init_table_from_type(&tmptb, NULL, tmptbtyp, 1,
            refexp->var.tbref.tbp->var.tb.keyv, ecp);
    if (ret != RDB_OK) {
        RDB_drop_type(tmptbtyp, ecp, NULL);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    ret = RDB_index_cursor(&curp, refexp->var.tbref.indexp->idxp, RDB_TRUE,
            refexp->var.tbref.tbp->var.tb.is_persistent ? tx.txid : NULL);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        if (_RDB_obj_to_field(&fv[i], texp->var.op.optinfo.objpv[i], ecp)
                != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    if (texp->var.op.optinfo.objpc != refexp->var.tbref.indexp->attrc
            || !texp->var.op.optinfo.all_eq)
        flags = RDB_REC_RANGE;
    else
        flags = 0;

    /* Set cursor position */
    ret = RDB_cursor_seek(curp, texp->var.op.optinfo.objpc, fv, flags);
    if (ret == DB_NOTFOUND) {
        rcount = RDB_OK;
        goto cleanup;
    }
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    rcount = 0;
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        RDB_init_obj(&tpl);
        if (_RDB_get_by_cursor(refexp->var.tbref.tbp, curp,
                refexp->var.tbref.tbp->typ->var.basetyp, &tpl,
                ecp, txp) != RDB_OK) {
            rcount = RDB_ERROR;
            RDB_destroy_obj(&tpl, ecp);
            goto cleanup;
        }

        if (texp->var.op.optinfo.stopexp != NULL) {
            if (RDB_evaluate_bool(texp->var.op.optinfo.stopexp,
                    &_RDB_tpl_get, &tpl, ecp, txp, &b) != RDB_OK) {
                rcount = RDB_ERROR;
                RDB_destroy_obj(&tpl, ecp);
                goto cleanup;
            }
            if (!b) {
                ret = DB_NOTFOUND;
                RDB_destroy_obj(&tpl, ecp);
                break;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &_RDB_tpl_get, &tpl, ecp, &tx, &upd)
                    != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(texp->var.op.args.firstp->nextp, &_RDB_tpl_get, &tpl, ecp,
                &tx, &b) != RDB_OK) {
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
            if (RDB_insert(&tmptb, &tpl, ecp, &tx) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        if (texp->var.op.optinfo.objpc == refexp->var.tbref.indexp->attrc
                && texp->var.op.optinfo.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;

        ret = RDB_cursor_next(curp, flags);
    } while (ret == RDB_OK);

    if (ret != DB_NOTFOUND) {
        RDB_errcode_to_error(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Delete the updated records from the original table.
     */

    /* Reset cursor */
    ret = RDB_cursor_seek(curp, texp->var.op.optinfo.objpc, fv, flags);
    if (ret == DB_NOTFOUND) {
        ret = RDB_OK;
        goto cleanup;
    }
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    _RDB_cmp_ecp = ecp;
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        RDB_init_obj(&tpl);
        ret = _RDB_get_by_cursor(refexp->var.tbref.tbp, curp,
                RDB_obj_type(refexp->var.tbref.tbp)->var.basetyp, &tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }

        if (texp->var.op.optinfo.stopexp != NULL) {
            if (RDB_evaluate_bool(texp->var.op.optinfo.stopexp,
                    &_RDB_tpl_get, &tpl, ecp, txp, &b) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                ret = DB_NOTFOUND;
                goto cleanup;
            }
            if (!b) {
                ret = DB_NOTFOUND;
                RDB_destroy_obj(&tpl, ecp);
                break;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &_RDB_tpl_get, &tpl, ecp, &tx, &upd)
                    != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(texp->var.op.args.firstp->nextp, &_RDB_tpl_get, &tpl, ecp,
                &tx, &b) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        upd = (RDB_bool) (upd && b);

        if (upd) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp, &tx);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        if (texp->var.op.optinfo.objpc == refexp->var.tbref.indexp->attrc
                && texp->var.op.optinfo.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;

        ret = RDB_cursor_next(curp, flags);
    } while (ret == RDB_OK);

    if (ret != DB_NOTFOUND) {
        RDB_errcode_to_error(ret, ecp, &tx);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Insert the records from the temporary table into the original table.
     */
     if (RDB_move_tuples(refexp->var.tbref.tbp, &tmptb, ecp,
                          &tx) == (RDB_int) RDB_ERROR) {
         rcount = RDB_ERROR;
     }

cleanup:
    RDB_destroy_obj(&tmptb, ecp);

    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                RDB_errcode_to_error(ret, ecp, &tx);
                rcount = RDB_ERROR;
            }
        }
    }

    for (i = 0; i < updc; i++) {
        RDB_destroy_obj(&valv[i], ecp);
    }
    RDB_free(valv);
    RDB_free(fieldv);
    RDB_free(fv);

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
upd_complex(RDB_object *tbp, int updc, const RDB_attr_update updv[],
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
_RDB_update_real(RDB_object *tbp, RDB_expression *condp, int updc,
        const RDB_attr_update updv[], RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (tbp->var.tb.stp == NULL)
        return RDB_OK;

    if (upd_complex(tbp, updc, updv, ecp)
            || (condp != NULL && _RDB_expr_refers(condp, tbp)))
        return update_stored_complex(tbp, condp, updc, updv, ecp, txp);
    return update_stored_simple(tbp, condp, updc, updv, ecp, txp);
}

RDB_int
_RDB_update_where_index(RDB_expression *texp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *refexp;

    if (texp->var.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->var.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->var.op.args.firstp->var.op.args.firstp;
    }
    
    if (refexp->var.tbref.tbp->var.tb.stp == NULL)
        return 0;

    if (refexp->var.tbref.indexp->idxp == NULL) {
        return update_where_pindex(texp, condp, updc, updv, ecp, txp);
    }

    if (upd_complex(refexp->var.tbref.tbp, updc, updv, ecp)
        || _RDB_expr_refers(texp->var.op.args.firstp->nextp, refexp->var.tbref.tbp)
        || (condp != NULL && _RDB_expr_refers(condp, refexp->var.tbref.tbp))) {
        return update_where_index_complex(texp, condp, updc, updv, ecp, txp);
    }
    return update_where_index_simple(texp, condp, updc, updv, ecp, txp);
}
