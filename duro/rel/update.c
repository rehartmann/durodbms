/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

static RDB_bool
is_keyattr(const char *attrname, RDB_table *tbp)
{
    int i, j;

    for (i = 0; i < tbp->keyc; i++)
        for (j = 0; j < tbp->keyv[i].strc; j++)
            if (strcmp(attrname, tbp->keyv[i].strv[j]) == 0)
                return RDB_TRUE;
    return RDB_FALSE;
}

static int
upd_to_vals(RDB_table *tbp, int updc, const RDB_attr_update updv[],
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
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    RDB_cursor *curp = NULL;
    RDB_object *valv = malloc(sizeof(RDB_object) * updc);

    if (valv == NULL) {
        return RDB_NO_MEMORY;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);
    RDB_init_obj(&tpl);

    /*
     * Iterate over the records and insert the updated records into
     * a temporary table. For the temporary table, only one key is needed.
     */
    ret = _RDB_create_table(NULL, RDB_FALSE,
            tpltyp->var.tuple.attrc, tpltyp->var.tuple.attrv,
            1, tbp->keyv, txp, &tmptbp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp, 0, txp->txid);
    if (ret != RDB_OK)        
        goto cleanup;
    ret = RDB_cursor_first(curp);
    
    while (ret == RDB_OK) {
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp,
                    *(int*) RDB_hashmap_get(&tbp->var.stored.attrmap,
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
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            ret = RDB_evaluate_bool(condp, &tpl, txp, &b);
            if (ret != RDB_OK) {
                return ret;
            }
        }
        if (b) {
            ret = upd_to_vals(tbp, updc, updv, &tpl, valv, txp);
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
            ret = RDB_insert(tmptbp, &tpl, txp);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        }
        ret = RDB_cursor_next(curp);
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
                    *(int*) RDB_hashmap_get(&tbp->var.stored.attrmap,
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
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            ret = RDB_evaluate_bool(condp, &tpl, txp, &b);
            if (ret != RDB_OK) {
                return ret;
            }
        }
        if (b) {
            /* Delete tuple */
            RDB_cursor_delete(curp);
        }
        ret = RDB_cursor_next(curp);
    }
    ret = RDB_destroy_cursor(curp);
    curp = NULL;
    if (ret != RDB_OK)
        goto cleanup;

    /*
     * Insert the records from the temporary table into the original table.
     */
     ret = _RDB_move_tuples(tbp, tmptbp, txp);

cleanup:
    free(valv);

    if (tmptbp != NULL)
        _RDB_drop_rtable(tmptbp, txp);
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
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    RDB_cursor *curp = NULL;
    RDB_object *valv = malloc(sizeof(RDB_object) * updc);
    RDB_field *fieldv = malloc(sizeof(RDB_field) * updc);

    if (valv == NULL || fieldv == NULL) {
        free(valv);
        free(fieldv);
        return RDB_NO_MEMORY;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);
    RDB_init_obj(&tpl);

    /*
     * Iterator over the records and update them if the select expression
     * evaluates to true.
     */
    ret = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp,
            0, txp->txid);
    if (ret != RDB_OK)        
        return ret;

    ret = RDB_cursor_first(curp);
    while (ret == RDB_OK) {
        /* Read tuple */
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp,
                    *(int*) RDB_hashmap_get(
                            &tbp->var.stored.attrmap,
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
            ret = RDB_evaluate_bool(condp, &tpl, txp, &b);
            if (ret != RDB_OK)
                return ret;
        } else {
            b = RDB_TRUE;
        }

        if (b) {
            /* Perform update */
            ret = upd_to_vals(tbp, updc, updv, &tpl, valv, txp);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Get field number from map */
                fieldv[i].no = *(int*) RDB_hashmap_get(
                        &tbp->var.stored.attrmap,
                        updv[i].name, NULL);

                /* Set type - needed for tuple and array attributes */
                if (valv[i].typ == NULL
                        && (valv[i].kind == RDB_OB_TUPLE
                         || valv[i].kind == RDB_OB_ARRAY)) {
                    _RDB_set_nonsc_type(&valv[i], RDB_type_attr_type(
                            RDB_table_type(tbp), updv[i].name));
                }

                /* Get data */
                _RDB_obj_to_field(&fieldv[i], &valv[i]);
            }
            ret = RDB_cursor_set(curp, updc, fieldv);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        }
        ret = RDB_cursor_next(curp);
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
    return ret;
}

static int
update_select_index(RDB_table *tbp, int updc, const RDB_attr_update updv[],
        RDB_transaction *txp)
{
    RDB_field fv;
    RDB_object val;
    RDB_object tpl;
    int ret;
    int i;
    RDB_object *valv = malloc(sizeof(RDB_object) * updc);
    RDB_field *fieldv = malloc(sizeof(RDB_field) * updc);

    if (valv == NULL || fieldv == NULL) {
        free(valv);
        free(fieldv);
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    RDB_init_obj(&val);
    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    /* Evaluate key */
    /* !! */
    ret = RDB_evaluate(tbp->var.select.exp->var.op.arg2, NULL, txp, &val);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    /* Convert to a field value */
    _RDB_obj_to_field(&fv, &val);

    /* Read tuple */
    RDB_init_obj(&tpl);
    ret = _RDB_get_by_uindex(tbp->var.select.tbp, &val,
            tbp->var.select.indexp, txp, &tpl);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        goto cleanup;
    }

    ret = upd_to_vals(tbp->var.select.tbp, updc, updv, &tpl, valv, txp);
    RDB_destroy_obj(&tpl);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    for (i = 0; i < updc; i++) {
        fieldv[i].no = *(int*) RDB_hashmap_get(
                 &tbp->var.select.tbp->var.stored.attrmap,
                 updv[i].name, NULL);
         
        /* Set type - needed for tuple and array attributes */
        if (valv[i].typ == NULL
                && (valv[i].kind == RDB_OB_TUPLE
                 || valv[i].kind == RDB_OB_ARRAY)) {
            _RDB_set_nonsc_type(&valv[i], RDB_type_attr_type(
                    RDB_table_type(tbp->var.select.tbp), updv[i].name));
        }
        _RDB_obj_to_field(&fieldv[i], &valv[i]);
    }
        
    ret = RDB_update_rec(tbp->var.select.tbp->var.stored.recmapp, &fv, updc, fieldv,
            txp->txid);
    if (RDB_is_syserr(ret)) {
        RDB_errmsg(txp->dbp->dbrootp->envp, "cannot update record: %s",
               RDB_strerror(ret));
        return ret;
    }
    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&val);
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i]);
    free(valv);
    free(fieldv);

    return ret;
}

static RDB_bool
needs_complex_update(RDB_table *tbp, RDB_expression *exprp,
        int updc, const RDB_attr_update updv[])
{
    int i;

    /*
     * If a key is updated or exprp refers to the table,
     * the simple update method cannot be used
     */

    /* Check if a key attribute is updated */
    for (i = 0; i < updc; i++) {
        if (is_keyattr(updv[i].name, tbp)) {
            return RDB_TRUE;
        }
    }

    /* Check if one of the expressions refers to the table itself */

    if (exprp != NULL) {
        return _RDB_expr_refers(exprp, tbp);
    }

    for (i = 0; i < updc; i++) {
        if (_RDB_expr_refers(updv[i].exp, tbp))
            return RDB_TRUE;
    }

    return RDB_FALSE;
}

static int
update_select(RDB_table *tbp, int updc, const RDB_attr_update updv[],
        RDB_transaction *txp)
{
    /* !! */
    if (tbp->var.select.tbp->kind != RDB_TB_STORED)
        return RDB_NOT_SUPPORTED;

    if (needs_complex_update(tbp->var.select.tbp, tbp->var.select.exp,
            updc, updv))
        return update_stored_complex(tbp->var.select.tbp,
                tbp->var.select.exp, updc, updv, txp);

    return update_stored_simple(tbp->var.select.tbp, tbp->var.select.exp,
            updc, updv, txp);
}

static int
update(RDB_table *tbp, int updc, const RDB_attr_update updv[],
        RDB_transaction *txp)
{
    int ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
        {
            if (needs_complex_update(tbp, NULL, updc, updv)) {
                ret = update_stored_complex(tbp, NULL, updc, updv, txp);
            } else {
                ret = update_stored_simple(tbp, NULL, updc, updv, txp);
            }
            if (RDB_is_syserr(ret))
                RDB_rollback_all(txp);
            return ret;
        }
        case RDB_TB_UNION:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_MINUS:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_INTERSECT:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_SELECT:
            ret = update_select(tbp, updc, updv, txp);
            if (RDB_is_syserr(ret)) {
                RDB_rollback_all(txp);
            }
            return ret;
        case RDB_TB_SELECT_INDEX:
            ret = update_select_index(tbp, updc, updv, txp);
            if (RDB_is_syserr(ret)) {
                RDB_rollback_all(txp);
            }
            return ret;
        case RDB_TB_JOIN:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_EXTEND:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_PROJECT:
            return RDB_NOT_SUPPORTED;
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
    int ret;
    int i;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /* Typecheck */
    for (i = 0; i < updc; i++) {
        RDB_type *typ;
        RDB_attr *attrp = _RDB_tuple_type_attr(tbp->typ->var.basetyp,
                updv[i].name);

        if (attrp == NULL)
            return RDB_INVALID_ARGUMENT;
        typ = RDB_expr_type(updv[i].exp, tbp->typ->var.basetyp);
        if (typ != NULL && !RDB_type_equals(typ, attrp->typ))
            return RDB_TYPE_MISMATCH;
    }

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

    ret = update(tbp, updc, updv, txp);
    if (condp != NULL)
        _RDB_free_table(tbp, NULL);
    return ret;
}
