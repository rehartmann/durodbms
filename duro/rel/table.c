/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

RDB_type *
RDB_table_type(const RDB_table *tbp)
{
    return tbp->typ;
}

static int
insert_intersect(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_transaction tx;
    int ret, ret2;

    /* Try to insert tup into both tables. If one insert fails,
     * the insert into the intersection fails.
     * If only one of the inserts fails because the tuple already exists,
     * the overall insert succeeds. 
     */

    /* Start a subtransaction */
    ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK)
        return ret;
            
    ret = RDB_insert(tbp->var.intersect.tbp1, tup, txp);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret;
    }

    ret2 = RDB_insert(tbp->var.intersect.tbp2, tup, txp);
    if (ret2 != RDB_OK && ret2 != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret2;
    }
    /* If both inserts fail because the tuples exist, the insert fails
     * with an error of RDB_ELEMENT_EXISTS.
     */
    if (ret == RDB_ELEMENT_EXISTS && ret2 == RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return RDB_ELEMENT_EXISTS;
    }       

    return RDB_commit(&tx);
}

static int
insert_join(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_transaction tx;
    int ret, ret2;

    /* Try to insert tup into both tables. If one insert fails,
     * the insert into the join fails.
     * If only one of the inserts fails because the tuple already exists,
     * the overall insert succeeds. 
     */

    /* Start a subtransaction */
    ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK)
        return ret;
            
    ret = RDB_insert(tbp->var.join.tbp1, tup, txp);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret;
    }

    ret2 = RDB_insert(tbp->var.join.tbp2, tup, txp);
    if (ret2 != RDB_OK && ret2 != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret2;
    }
    /* If both inserts fail because the tuples exist, the insert fails
     * with an error of RDB_ELEMENT_EXISTS.
     */
    if (ret == RDB_ELEMENT_EXISTS && ret2 == RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return RDB_ELEMENT_EXISTS;
    }       
    
    return RDB_commit(&tx);
}

int
RDB_insert(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    switch (tbp->kind) {
        case RDB_TB_STORED:
        {
            int i;
            RDB_type *tuptyp = tbp->typ->var.basetyp;
            int attrcount = tuptyp->var.tuple.attrc;
            RDB_field *fvp;

            fvp = malloc(sizeof(RDB_field) * attrcount);
            if (fvp == NULL) {
                RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                return RDB_NO_MEMORY;
            }
            for (i = 0; i < attrcount; i++) {
                int *fnop;
                RDB_object *valp;
                
                fnop = RDB_hashmap_get(&tbp->var.stored.attrmap,
                        tuptyp->var.tuple.attrv[i].name, NULL);
                valp = RDB_tuple_get(tup, tuptyp->var.tuple.attrv[i].name);

                /* If there is no value, check if there is a default */
                if (valp == NULL) {
                    valp = tuptyp->var.tuple.attrv[i].defaultp;
                    if (valp == NULL) {
                        return RDB_INVALID_ARGUMENT;
                    }
                }
                
                /* Typecheck */
                if (!RDB_type_equals(valp->typ,
                                     tuptyp->var.tuple.attrv[i].typ)) {
                     return RDB_TYPE_MISMATCH;
                }
                fvp[*fnop].datap = RDB_obj_irep(valp, &fvp[*fnop].len);
            }
            ret = RDB_insert_rec(tbp->var.stored.recmapp, fvp, txp->txid);
            if (RDB_is_syserr(ret)) {
                RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                RDB_rollback(txp);
            } else if (ret == RDB_KEY_VIOLATION) {
                /* check if the tuple is an element of the table */
                if (RDB_contains_rec(tbp->var.stored.recmapp, fvp, txp->txid)
                        == RDB_OK)
                    ret = RDB_ELEMENT_EXISTS;
            }            
            free(fvp);
            return ret;
        }
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            ret = RDB_evaluate_bool(tbp->var.select.exprp, tup, txp, &b);
            if (ret != RDB_OK)
                return ret;
            if (!b)
                return RDB_PREDICATE_VIOLATION;
            return RDB_insert(tbp->var.select.tbp, tup, txp);
        case RDB_TB_MINUS:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_UNION:
        {
            int ret2;

            /* !! may be very inefficient */        
            if (RDB_table_contains(tbp->var._union.tbp1, tup, txp) == RDB_OK
                    || RDB_table_contains(tbp->var._union.tbp2, tup, txp) == RDB_OK)
                return RDB_ELEMENT_EXISTS;
             
            /* Try to insert tup into both tables. The insertion into the union
             * fails if (1) one of the inserts fails for a reason other than
             * a predicate violation or (2) if both inserts fail because of
             * a predicate violation.
             */
            ret = RDB_insert(tbp->var._union.tbp1, tup, txp);
            if (ret != RDB_OK) {
                if (ret != RDB_KEY_VIOLATION && ret != RDB_PREDICATE_VIOLATION)
                    return ret;
            }
            ret2 = RDB_insert(tbp->var._union.tbp2, tup, txp);
            if (ret2 != RDB_OK) {
                if (ret2 != RDB_KEY_VIOLATION && ret2 != RDB_PREDICATE_VIOLATION)
                    return ret2;
                /* 2nd insert failed because of a predicate violation, so
                 * it depends on the result of the 1st insert.
                 */
                return ret;
            } else {
                return RDB_OK;
            }
        }
        case RDB_TB_INTERSECT:
             return insert_intersect(tbp, tup, txp);
        case RDB_TB_JOIN:
             return insert_join(tbp, tup, txp);
        case RDB_TB_EXTEND:
        {
            int i;
        
            /* Check the additional attributes */
            for (i = 0; i < tbp->var.extend.attrc; i++) {
                RDB_virtual_attr *vattrp = &tbp->var.extend.attrv[i];
                RDB_object val;
                RDB_bool iseq;
                
                ret = RDB_evaluate(vattrp->exp, tup, txp, &val);
                if (ret != RDB_OK)
                    return ret;
                iseq = RDB_obj_equals(&val, (RDB_object *)RDB_tuple_get(
                        tup, vattrp->name));
                RDB_destroy_obj(&val);
                if (!iseq)
                    return RDB_PREDICATE_VIOLATION;
            }
        
            /* Insert the tuple (the additional attribute(s) don't do any harm)
             */
             return RDB_insert(tbp->var.extend.tbp, tup, txp);
        }
        case RDB_TB_PROJECT:
             return RDB_NOT_SUPPORTED;
        case RDB_TB_SUMMARIZE:
             return RDB_NOT_SUPPORTED;
        case RDB_TB_RENAME:
             /* !! ... */
             return RDB_NOT_SUPPORTED;
    }
    /* should never be reached */
    abort();
}

static RDB_bool
attr_is_pindex(RDB_table *tbp, const char *attrname) {
    int *fnop = RDB_hashmap_get(&tbp->var.stored.attrmap,
                        attrname, NULL);
    return (fnop != NULL) && RDB_field_is_pindex(tbp->var.stored.recmapp, *fnop);
}

/*
 * Checks if the expression pointed to by exprp is of the form
 * <primary key attribute>=<constant expression> and returns the
 * constant expression if yes, or NULL if no.
 */
static RDB_expression *
pindex_expr(RDB_table *tbp, RDB_expression *exprp) {
    if (tbp->kind != RDB_TB_STORED
            || _RDB_pkey_len(tbp) != 1)
        return NULL;
    if (exprp->var.op.arg1->kind == RDB_ATTR
            && RDB_expr_is_const(exprp->var.op.arg2)
            && attr_is_pindex(tbp, exprp->var.op.arg1->var.attr.name)) {
        return exprp->var.op.arg2;
    }
    if (exprp->var.op.arg2->kind == RDB_ATTR
            && RDB_expr_is_const(exprp->var.op.arg1)
            && attr_is_pindex(tbp, exprp->var.op.arg2->var.attr.name)) {
        return exprp->var.op.arg1;
    }
    return NULL;
}

static int
upd_to_vals(RDB_table *tbp, int updc, const RDB_attr_update updv[],
               RDB_tuple *tplp, RDB_object *valv, RDB_transaction *txp)
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
move_tuples(RDB_table *dstp, RDB_table *srcp, RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    RDB_tuple tpl;
    int ret;

    /* delete all tuples from destination table */
    ret = RDB_delete(dstp, NULL, txp);
    if (ret != RDB_OK)
        return ret;

    /* copy all tuples from source table to destination table */

    ret = _RDB_table_qresult(srcp, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_tuple(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        ret = RDB_insert(dstp, &tpl, txp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
    }
    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;
cleanup:
    _RDB_drop_qresult(qrp, txp);
    RDB_destroy_tuple(&tpl);
    return ret;
}

static int
update_stored_complex(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_transaction *txp)
{
    RDB_table *tmptbp = NULL;
    RDB_tuple tpl;
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
    RDB_init_tuple(&tpl);

    /*
     * Iterate over the records and insert the updated records into
     * a temporary table.
     */
    ret = _RDB_create_table(NULL, RDB_FALSE,
            tpltyp->var.tuple.attrc, tpltyp->var.tuple.attrv,
            tbp->keyc, tbp->keyv, txp, &tmptbp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp, 0, txp->txid);
    if (ret != RDB_OK)        
        goto cleanup;
    ret = RDB_cursor_first(curp);
    
    while (ret == RDB_OK) {
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp, i, &datap, &len);
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

            ret = RDB_cursor_get(curp, i, &datap, &len);
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
     ret = move_tuples(tbp, tmptbp, txp);

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
    ret2 = RDB_destroy_tuple(&tpl);
    if (ret == RDB_OK)
        ret = ret2;
    return ret;
}

static int
update_stored_simple(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_transaction *txp)
{
    RDB_tuple tpl;
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
    RDB_init_tuple(&tpl);

    /*
     * Walk through the records and update them if the expression pointed to
     * by condp evaluates to true.
     */
    ret = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp, 0, txp->txid);
    if (ret != RDB_OK)        
        return ret;

    ret = RDB_cursor_first(curp);
    while (ret == RDB_OK) {
        /* Read tuple */
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp, i, &datap, &len);
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
            /* Perform update */
            ret = upd_to_vals(tbp, updc, updv, &tpl, valv, txp);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Get field number from map */
                fieldv[i].no = *(int*) RDB_hashmap_get(&tbp->var.stored.attrmap,
                        updv[i].name, NULL);
                
                /* Get data */
                fieldv[i].datap = RDB_obj_irep(&valv[i], &fieldv[i].len);
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
    ret2 = RDB_destroy_tuple(&tpl);
    if (ret == RDB_OK)
        ret = ret2;
    return ret;
}

static int
update_stored_by_key(RDB_table *tbp, RDB_expression *exp,
        int updc, const RDB_attr_update updv[],
        RDB_transaction *txp)
{
    RDB_field fv;
    RDB_object val;
    RDB_tuple tpl;
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
    ret = RDB_evaluate(exp, NULL, txp, &val);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    /* Convert to a field value */
    fv.datap = RDB_obj_irep(&val, &fv.len);

    /* Read tuple */
    RDB_init_tuple(&tpl);
    ret = _RDB_get_by_pindex(tbp, &val, &tpl, txp);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        goto cleanup;
    }

    ret = upd_to_vals(tbp, updc, updv, &tpl, valv, txp);
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    for (i = 0; i < updc; i++) {
         fieldv[i].no = *(int*) RDB_hashmap_get(&tbp->var.stored.attrmap,
                 updv[i].name, NULL);
         fieldv[i].datap = RDB_obj_irep(&valv[i], &fieldv[i].len);
    }
        
    ret = RDB_update_rec(tbp->var.stored.recmapp, &fv, updc, fieldv,
            txp->txid);
    if (RDB_is_syserr(ret)) {
        RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
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

static int
update_stored(RDB_table *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_transaction *txp)
{
    int i;
    RDB_expression *exprp;
    RDB_bool keyupd = RDB_FALSE;
    RDB_bool sref = RDB_FALSE;

    /* Check if the primary index can be used */
    exprp = condp != NULL ? pindex_expr(tbp, condp) : NULL;
    if (exprp != NULL) {
        return update_stored_by_key(tbp, exprp, updc, updv, txp);
    }

    /* Check if a key attribute is updated */
    for (i = 0; i < updc; i++) {
        if (is_keyattr(updv[i].name, tbp)) {
            keyupd = RDB_TRUE;
            break;
        }
    }

    if (!keyupd) {
        /* Check if one of the expressions refers to the table itself */
        if (condp != NULL)
            sref = _RDB_expr_refers(condp, tbp);
        if (!sref) {
             int i;
        
             for (i = 0; i < updc; i++)
                 sref |= _RDB_expr_refers(updv[i].exp, tbp);
        }
    }

    /* If a key is updated or condp refers to the table, the simple update method cannot be used */
    if (keyupd || sref)
        return update_stored_complex(tbp, exprp, updc, updv, txp);

    return update_stored_simple(tbp, exprp, updc, updv, txp);
}  

int
RDB_update(RDB_table *tbp, RDB_expression *condp, int updc,
                const RDB_attr_update updv[], RDB_transaction *txp)
{
    int i;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /* Typecheck */
    for (i = 0; i < updc; i++) {
        RDB_attr *attrp = _RDB_tuple_type_attr(tbp->typ->var.basetyp,
                updv[i].name);

        if (attrp == NULL)
            return RDB_INVALID_ARGUMENT;
        if (!RDB_type_equals(RDB_expr_type(updv[i].exp), attrp->typ))
            return RDB_TYPE_MISMATCH;
    }

    switch (tbp->kind) {
        case RDB_TB_STORED:
        {
            int ret = update_stored(tbp, condp, updc, updv, txp);
            if (RDB_is_syserr(ret)) {
                RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                RDB_rollback(txp);
            }
            return ret;
        }
        case RDB_TB_UNION:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_MINUS:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_INTERSECT:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            return RDB_NOT_SUPPORTED;
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
    }

    /* should never be reached */
    abort();
}

/* Delete all tuples from table for which condp evaluates to true.
 * If condp is NULL, it is equavalent to true.
 */
static int
delete_stored(RDB_table *tbp, RDB_expression *condp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_cursor *curp;
    RDB_tuple tpl;
    void *datap;
    size_t len;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    RDB_expression *exprp;
    RDB_bool b;

    /* Check if the primary index can be used */
    exprp = condp != NULL ? pindex_expr(tbp, condp) : NULL;
    if (exprp != NULL) {
        RDB_field fv;
        RDB_object val;

        ret = RDB_evaluate(exprp, NULL, txp, &val);
        if (ret != RDB_OK)
            return ret;
        fv.datap = RDB_obj_irep(&val, &fv.len);
        return RDB_delete_rec(tbp->var.stored.recmapp, &fv, txp->txid);
    }

    ret = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp, 0, txp->txid);
    if (ret != RDB_OK)
        return ret;
    ret = RDB_cursor_first(curp);
    if (ret == RDB_NOT_FOUND) {
        RDB_destroy_cursor(curp);
        return RDB_OK;
    }
    
    do {
        RDB_init_tuple(&tpl);
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp, i, &datap, &len);
            if (ret != 0) {
               RDB_destroy_tuple(&tpl);
               goto error;
            }
            RDB_init_obj(&val);
            ret = RDB_irep_to_obj(&val, tpltyp->var.tuple.attrv[i].typ,
                             datap, len);
            if (ret != RDB_OK) {
               RDB_destroy_tuple(&tpl);
               goto error;
            }
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val);
            if (ret != RDB_OK) {
               RDB_destroy_tuple(&tpl);
               goto error;
            }
        }
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            ret = RDB_evaluate_bool(condp, &tpl, txp, &b);
            if (ret != RDB_OK)
                return ret;
        }
        if (b) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                RDB_destroy_tuple(&tpl);
                goto error;
            }
        }
        RDB_destroy_tuple(&tpl);
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

int
RDB_delete(RDB_table *tbp, RDB_expression *condp, RDB_transaction *txp)
{
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            ret = delete_stored(tbp, condp, txp);
            if (RDB_is_syserr(ret)) {
                RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                RDB_rollback(txp);
            }
            return ret;
        case RDB_TB_MINUS:
            return RDB_delete(tbp->var.minus.tbp1, condp, txp);
        case RDB_TB_UNION:
            ret = RDB_delete(tbp->var._union.tbp1, condp, txp);
            if (ret != RDB_OK)
                return ret;
            return RDB_delete(tbp->var._union.tbp2, condp, txp);
        case RDB_TB_INTERSECT:
            ret = RDB_delete(tbp->var.intersect.tbp1, condp, txp);
            if (ret != RDB_OK)
                return ret;
            return RDB_delete(tbp->var.intersect.tbp2, condp, txp);
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_JOIN:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_EXTEND:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_PROJECT:
            return RDB_delete(tbp->var.project.tbp, condp, txp);;
        case RDB_TB_SUMMARIZE:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_RENAME:
            /* !! */
            return RDB_NOT_SUPPORTED;
    }
    /* should never be reached */
    abort();
}


int
RDB_copy_table(RDB_table *dstp, RDB_table *srcp, RDB_transaction *txp)
{
    RDB_transaction tx;
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /* check if types of the two tables match */
    if (!RDB_type_equals(dstp->typ, srcp->typ))
        return RDB_TYPE_MISMATCH;

    /* start subtransaction */
    ret = RDB_begin_tx(&tx, txp->dbp, txp);
    if (ret != RDB_OK)
        return ret;

    ret = move_tuples(dstp, srcp, &tx);
    if (ret != RDB_OK)
        goto error;

    return RDB_commit(&tx);

error:
    RDB_rollback(&tx);
    return ret;
}

static int
aggr_type(RDB_type *tuptyp, RDB_type *attrtyp, RDB_aggregate_op op,
          RDB_type **resultpp)
{
    if (op == RDB_COUNT || op == RDB_COUNTD) {
        *resultpp = &RDB_INTEGER;
        return RDB_OK;
    }
    
    switch (op) {
        /* only to avoid compiler warnings */
        case RDB_COUNTD:
        case RDB_COUNT:

        case RDB_AVGD:
        case RDB_AVG:
            if (!RDB_type_is_numeric(attrtyp))
                return RDB_TYPE_MISMATCH;
            *resultpp = &RDB_RATIONAL;
            break;
        case RDB_SUM:
        case RDB_SUMD:
        case RDB_MAX:
        case RDB_MIN:
            if (!RDB_type_is_numeric(attrtyp))
                return RDB_TYPE_MISMATCH;
            *resultpp = attrtyp;
            break;
        case RDB_ALL:
        case RDB_ANY:
            if (attrtyp != &RDB_BOOLEAN)
                return RDB_TYPE_MISMATCH;
            *resultpp = &RDB_BOOLEAN;
            break;
     }
     return RDB_OK;
}

int
RDB_aggregate(RDB_table *tbp, RDB_aggregate_op op, const char *attrname,
              RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_tuple tpl;
    int ret;
    int count; /* only needed for AVG */

    /* attrname may only be NULL if op == RDB_AVG or table is unary */
    if (attrname == NULL && op != RDB_COUNT) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    ret = aggr_type(tbp->typ->var.basetyp, attrtyp, op, &resultp->typ);
    if (ret != RDB_OK)
        return ret;

    /* initialize result */
    switch (op) {
        case RDB_COUNT:
            resultp->var.int_val = 0;
            break;
        case RDB_AVG:
            if (!RDB_type_is_numeric(attrtyp))
                return RDB_TYPE_MISMATCH;
            count = 0;
            resultp->var.rational_val = 0.0;
            break;
        case RDB_SUM:
            if (attrtyp == &RDB_INTEGER)
                resultp->var.int_val = 0;
            else if (attrtyp == &RDB_RATIONAL)
                resultp->var.rational_val = 0.0;
            else
                return RDB_TYPE_MISMATCH;
            break;
        case RDB_MAX:
            if (attrtyp == &RDB_INTEGER)
                resultp->var.int_val = RDB_INT_MIN;
            else if (attrtyp == &RDB_RATIONAL)
                resultp->var.rational_val = RDB_RATIONAL_MIN;
            else
                return RDB_TYPE_MISMATCH;
            break;
        case RDB_MIN:
            if (attrtyp == &RDB_INTEGER)
                resultp->var.int_val = RDB_INT_MAX;
            else if (attrtyp == &RDB_RATIONAL)
                resultp->var.rational_val = RDB_RATIONAL_MAX;
                return RDB_TYPE_MISMATCH;
            break;
        case RDB_ALL:
            if (attrtyp != &RDB_BOOLEAN)
                return RDB_TYPE_MISMATCH;
            resultp->var.bool_val = RDB_TRUE;
            break;
        case RDB_ANY:
            if (attrtyp != &RDB_BOOLEAN)
                return RDB_TYPE_MISMATCH;
            resultp->var.bool_val = RDB_FALSE;
            break;
        default:
            return RDB_INVALID_ARGUMENT;
        break;
    }

    /* perform aggregation */

    RDB_init_tuple(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        switch (op) {
            case RDB_COUNT:
                resultp->var.int_val++;
                break;
            case RDB_SUM:
                if (attrtyp == &RDB_INTEGER)
                    resultp->var.int_val += RDB_tuple_get_int(&tpl, attrname);
                else
                    resultp->var.rational_val
                            += RDB_tuple_get_rational(&tpl, attrname);
                break;
            case RDB_AVG:
                count++;
                if (attrtyp == &RDB_INTEGER)
                    resultp->var.rational_val
                            += RDB_tuple_get_int(&tpl, attrname);
                else
                    resultp->var.rational_val
                            += RDB_tuple_get_rational(&tpl, attrname);
                break;
            case RDB_MAX:
                if (attrtyp == &RDB_INTEGER) {
                    RDB_int val = RDB_tuple_get_int(&tpl, attrname);
                    
                    if (val > resultp->var.int_val)
                        resultp->var.int_val = val;
                } else {
                    RDB_rational val = RDB_tuple_get_rational(&tpl, attrname);
                    
                    if (val > resultp->var.rational_val)
                        resultp->var.rational_val = val;
                }
                break;
            case RDB_MIN:
                if (attrtyp == &RDB_INTEGER) {
                    RDB_int val = RDB_tuple_get_int(&tpl, attrname);
                    
                    if (val < resultp->var.int_val)
                        resultp->var.int_val = val;
                } else {
                    RDB_rational val = RDB_tuple_get_rational(&tpl, attrname);
                    
                    if (val < resultp->var.rational_val)
                        resultp->var.rational_val = val;
                }
                break;
            case RDB_ALL:
                if (!RDB_tuple_get_bool(&tpl, attrname))
                    resultp->var.bool_val = RDB_FALSE;
                break;
            case RDB_ANY:
                if (RDB_tuple_get_bool(&tpl, attrname))
                    resultp->var.bool_val = RDB_TRUE;
                break;
            default: ;
        }
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback(txp);
        }
        return ret;
    }

    if (op == RDB_AVG) {
        if (count == 0)
            return RDB_AGGREGATE_UNDEFINED;
        resultp->var.rational_val /= count;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

static RDB_expression *
attr_eq(RDB_attr *attrp, const RDB_tuple *tup)
{
    return RDB_eq(RDB_expr_attr(attrp->name, attrp->typ),
                  RDB_obj_const(RDB_tuple_get(tup, attrp->name)));
}

static int
project_contains(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    RDB_bool result;
    int ret;
    
    if (tuptyp->var.tuple.attrc > 0) {
        RDB_expression *condp;
        RDB_table *seltbp;
        int i, ret;

        /* create where-condition */        
        condp = attr_eq(&tuptyp->var.tuple.attrv[0], tup);
        
        for (i = 1; i < tuptyp->var.tuple.attrc; i++) {
            condp = RDB_and(condp, attr_eq(&tuptyp->var.tuple.attrv[0],
                    tup));
        }
        if (condp == NULL)
            return RDB_NO_MEMORY;

        /* create selection table */
        ret = RDB_select(tbp, condp, &seltbp);
        if (ret != RDB_OK) {
            RDB_drop_expr(condp);
            return ret;
        }

        /* check if selection is empty */
        ret = RDB_table_is_empty(seltbp, txp, &result);
        _RDB_drop_table(seltbp, txp, RDB_FALSE);
        if (ret != RDB_OK)
            return ret;
        return result ? RDB_NOT_FOUND : RDB_OK;
    } else {
        /* projection with no attributes */
    
        ret = RDB_table_is_empty(tbp->var.project.tbp, txp, &result);
        if (ret != RDB_OK)
            return ret;
        return result ? RDB_NOT_FOUND : RDB_OK;
    }
}

int
_RDB_find_rename_to(int renc, RDB_renaming renv[], const char *name)
{
    int i;

    for (i = 0; i < renc && strcmp(renv[i].to, name) != 0; i++);
    if (i >= renc)
        return -1; /* not found */
    /* found */
    return i;
}

int    
RDB_rename_contains(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_tuple tpl;
    int i;
    int ret;
    RDB_type *tuptyp = tbp->typ->var.basetyp;

    RDB_init_tuple(&tpl);
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        /* has the attribute been renamed? */
        char *attrname = tuptyp->var.tuple.attrv[i].name;
        int ai = _RDB_find_rename_to(tbp->var.rename.renc, tbp->var.rename.renv,
                attrname);

        if (ai >= 0) { /* Yes, entry found */
            ret = RDB_tuple_set(&tpl, tbp->var.rename.renv[ai].from,
                        RDB_tuple_get(tup, tbp->var.rename.renv[ai].to));
        } else {
            ret = RDB_tuple_set(&tpl, attrname, RDB_tuple_get(tup, attrname));
        }
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret)) {
                RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                RDB_rollback(txp);
            }
            goto error;
        }
    }
    ret = RDB_table_contains(tbp->var.rename.tbp, &tpl, txp);

error:
    RDB_destroy_tuple(&tpl);
    return ret;
}
    
int
RDB_table_contains(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;
    
    switch (tbp->kind) {
        case RDB_TB_STORED:
            {
                int i;
                RDB_type *tuptyp = tbp->typ->var.basetyp;
                int attrcount = tuptyp->var.tuple.attrc;
                RDB_field *fvp;

                fvp = malloc(sizeof(RDB_field) * attrcount);
                if (fvp == NULL)
                    return RDB_NO_MEMORY;
                for (i = 0; i < attrcount; i++) {
                    RDB_object *valp;
                    int fno = *(int*)RDB_hashmap_get(&tbp->var.stored.attrmap,
                            tuptyp->var.tuple.attrv[i].name, NULL);

                    valp = RDB_tuple_get(tup, tuptyp->var.tuple.attrv[i].name);
                    if (valp == NULL) {
                        free(fvp);
                        return RDB_INVALID_ARGUMENT;
                    }
                    if (!RDB_type_equals (RDB_obj_type(valp), 
                            tuptyp->var.tuple.attrv[i].typ)) {
                        free(fvp);
                        return RDB_TYPE_MISMATCH;
                    }
                    fvp[fno].datap = RDB_obj_irep(valp, &fvp[fno].len);
                }
                ret = RDB_contains_rec(tbp->var.stored.recmapp, fvp, txp->txid);
                free(fvp);
                if (RDB_is_syserr(ret)) {
                    RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                    RDB_rollback(txp);
                }
                return ret;
            }
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            ret = RDB_evaluate_bool(tbp->var.select.exprp, tup, txp, &b);
            if (ret != RDB_OK)
                return ret;
            if (!b)
                return RDB_NOT_FOUND;
            return RDB_table_contains(tbp->var.select.tbp, tup, txp);
        case RDB_TB_UNION:
            ret = RDB_table_contains(tbp->var._union.tbp1, tup, txp);
            if (ret == RDB_OK)
                return RDB_OK;
            return RDB_table_contains(tbp->var._union.tbp2, tup, txp);            
        case RDB_TB_MINUS:
            ret = RDB_table_contains(tbp->var.minus.tbp1, tup, txp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_table_contains(tbp->var.minus.tbp2, tup, txp);
            if (ret == RDB_OK)
                return RDB_NOT_FOUND;
            if (ret == RDB_NOT_FOUND)
                return RDB_OK;
            return ret;
        case RDB_TB_INTERSECT:
            ret = RDB_table_contains(tbp->var.intersect.tbp1, tup, txp);
            if (ret != RDB_OK)
                return ret;
            return RDB_table_contains(tbp->var.intersect.tbp2, tup, txp);
        case RDB_TB_JOIN:
            ret = RDB_table_contains(tbp->var.join.tbp1, tup, txp);
            if (ret != RDB_OK)
                return ret;
            return RDB_table_contains(tbp->var.join.tbp2, tup, txp);
        case RDB_TB_EXTEND:
            return RDB_table_contains(tbp->var.extend.tbp, tup, txp);
        case RDB_TB_PROJECT:
            return project_contains(tbp, tup, txp);
        case RDB_TB_SUMMARIZE:
            {
                int ret2;
            
                RDB_qresult *qrp;
                ret = _RDB_table_qresult(tbp, txp, &qrp);
                if (ret != RDB_OK)
                    return ret;
                ret = _RDB_qresult_contains(qrp, tup, txp);
                ret2 = _RDB_drop_qresult(qrp, txp);
                if (ret == RDB_OK)
                    ret =  ret2;
                if (RDB_is_syserr(ret)) {
                    RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                    RDB_rollback(txp);
                }
                return ret;
            }
        case RDB_TB_RENAME:
            return RDB_rename_contains(tbp, tup, txp);
    }
    /* should never be reached */
    abort();
}

int
RDB_extract_tuple(RDB_table *tbp, RDB_tuple *tup, RDB_transaction *txp)
{
    int ret, ret2;
    RDB_qresult *qrp;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback(txp);
        }
        return ret;
    }

    /* Get tuple */
    ret = _RDB_next_tuple(qrp, tup, txp);
    if (ret != RDB_OK)
        goto error;

    /* Check if there are more tuples */
    ret = _RDB_next_tuple(qrp, NULL, txp);
    if (ret != RDB_NOT_FOUND) {
        if (ret == RDB_OK)
            ret = RDB_INVALID_ARGUMENT;
        goto error;
    }

    ret = RDB_OK;

error:
    ret2 = _RDB_drop_qresult(qrp, txp);
    if (ret == RDB_OK)
        ret = ret2;
    if (RDB_is_syserr(ret)) {
        RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
        RDB_rollback(txp);
    }
    return ret;
}

int
RDB_table_is_empty(RDB_table *tbp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_tuple tpl;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_tuple(&tpl);

    ret = _RDB_next_tuple(qrp, &tpl, txp);
    if (ret == RDB_OK)
        *resultp = RDB_FALSE;
    else if (ret == RDB_NOT_FOUND)
        *resultp = RDB_TRUE;
    else {
         RDB_destroy_tuple(&tpl);
        _RDB_drop_qresult(qrp, txp);
        return ret;
    }
    RDB_destroy_tuple(&tpl);
    return _RDB_drop_qresult(qrp, txp);
}

static RDB_string_vec *
dup_keys(int keyc, RDB_string_vec *keyv) {
    RDB_string_vec *newkeyv;
    int i;

    newkeyv = malloc(keyc * sizeof(RDB_string_vec));
    if (newkeyv == NULL) {
        return NULL;
    }
    for (i = 0; i < keyc; i++)
        newkeyv[i].strv = NULL;
    for (i = 0; i < keyc; i++) {
        newkeyv[i].strc = keyv[i].strc;
        newkeyv[i].strv = RDB_dup_strvec(
                keyv[i].strc, keyv[i].strv);
        if (newkeyv[i].strv == NULL) {
            goto error;
        }
    }
    return newkeyv;
error:
    /* free keys */
    for (i = 0; i < keyc; i++) {
        if (newkeyv[i].strv != NULL)
            RDB_free_strvec(newkeyv[i].strc, newkeyv[i].strv);
    }
    return NULL;
}

static RDB_string_vec *
dup_rename_keys(int keyc, RDB_string_vec *keyv, int renc, RDB_renaming renv[]) {
    RDB_string_vec *newkeyv;
    int i, j;

    newkeyv = malloc(keyc * sizeof(RDB_string_vec));
    if (newkeyv == NULL) {
        return NULL;
    }
    for (i = 0; i < keyc; i++)
        newkeyv[i].strv = NULL;
    for (i = 0; i < keyc; i++) {
        newkeyv[i].strc = keyv[i].strc;
        newkeyv[i].strv = malloc(sizeof (RDB_attr) * keyv[i].strc);
        if (newkeyv[i].strv == NULL) {
            goto error;
        }
        for (j = 0; j < keyv[i].strc; j++)
            newkeyv[i].strv[j] = NULL;
        for (j = 0; j < keyv[i].strc; j++) {
            /* Has the attribute been renamed */
            int ai = _RDB_find_rename_from(renc, renv, keyv[i].strv[j]);
            if (ai >= 0) /* Yes */
                newkeyv[i].strv[j] = RDB_dup_str(renv[ai].to);
            else
                newkeyv[i].strv[j] = RDB_dup_str(keyv[i].strv[j]);
            if (newkeyv[i].strv[j] == NULL)
                goto error;
        }
    }
    return newkeyv;
error:
    /* free keys */
    for (i = 0; i < keyc; i++) {
        if (newkeyv[i].strv != NULL)
            RDB_free_strvec(newkeyv[i].strc, newkeyv[i].strv);
    }
    return NULL;
}

int
RDB_select(RDB_table *tbp, RDB_expression *condp, RDB_table **resultpp)
{
    RDB_table *newtbp = malloc(sizeof (RDB_table));
    
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    if (condp->kind == RDB_OP_EQ) {
        RDB_expression *exprp = pindex_expr(tbp, condp);

        if (exprp != NULL) {
            newtbp->kind = RDB_TB_SELECT_PINDEX;
            RDB_init_obj(&newtbp->var.select.val);
            RDB_evaluate(exprp, NULL, NULL, &newtbp->var.select.val);
        } else {
            newtbp->kind = RDB_TB_SELECT;
        }
    } else {
        newtbp->kind = RDB_TB_SELECT;
    }
    newtbp->var.select.tbp = tbp;
    newtbp->var.select.exprp = condp;
    newtbp->typ = tbp->typ;
    newtbp->name = NULL;

    newtbp->keyc = tbp->keyc;
    newtbp->keyv = dup_keys(tbp->keyc, tbp->keyv);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }

    *resultpp = newtbp;

    return RDB_OK;
}

static RDB_string_vec *all_key(RDB_table *tbp) {
    RDB_string_vec *keyv = malloc(sizeof (RDB_string_vec));
    int attrc;
    int i;
    
    if (keyv == NULL)
        return NULL;
    
    attrc = keyv[0].strc =
            tbp->typ->var.basetyp->var.tuple.attrc;
    keyv[0].strv = malloc(sizeof(char *) * attrc);
    if (keyv[0].strv == NULL) {
        free(keyv);
        return NULL;
    }
    for (i = 0; i < attrc; i++)
        keyv[0].strv[i] = NULL;
    for (i = 0; i < attrc; i++) {
        keyv[0].strv[i] = RDB_dup_str(
                tbp->typ->var.basetyp->var.tuple.attrv[i].name);
        if (keyv[0].strv[i] == NULL) {
            goto error;
        }
    }

    return keyv;
error:
    RDB_free_strvec(keyv[0].strc, keyv[0].strv);
    free(keyv);
    return NULL;
}

int
RDB_union(RDB_table *tbp1, RDB_table *tbp2, RDB_table **resultpp)
{
    RDB_table *newtbp;

    if (!RDB_type_equals(tbp1->typ, tbp2->typ))
        return RDB_TYPE_MISMATCH;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->kind = RDB_TB_UNION;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var._union.tbp1 = tbp1;
    newtbp->var._union.tbp2 = tbp2;
    newtbp->typ = tbp1->typ;
    newtbp->name = NULL;

    /* Set keys (The result table becomes all-key.
     * Yes, it can be done better...)
     */
    newtbp->keyc = 1;
    newtbp->keyv = all_key(tbp1);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }

    *resultpp = newtbp;

    return RDB_OK;
}

int
RDB_minus(RDB_table *tbp1, RDB_table *tbp2, RDB_table **result)
{
    RDB_table *newtbp;

    if (!RDB_type_equals(tbp1->typ, tbp2->typ))
        return RDB_TYPE_MISMATCH;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    *result = newtbp;
    newtbp->kind = RDB_TB_MINUS;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.minus.tbp1 = tbp1;
    newtbp->var.minus.tbp2 = tbp2;
    newtbp->typ = tbp1->typ;
    newtbp->name = NULL;

    newtbp->keyc = tbp1->keyc;
    newtbp->keyv = dup_keys(tbp1->keyc, tbp1->keyv);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }

    return RDB_OK;
}

int
RDB_intersect(RDB_table *tbp1, RDB_table *tbp2, RDB_table **result)
{
    RDB_table *newtbp;

    if (!RDB_type_equals(tbp1->typ, tbp2->typ))
        return RDB_TYPE_MISMATCH;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    *result = newtbp;
    newtbp->kind = RDB_TB_INTERSECT;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.intersect.tbp1 = tbp1;
    newtbp->var.intersect.tbp2 = tbp2;
    newtbp->typ = tbp1->typ;
    newtbp->name = NULL;

    newtbp->keyc = tbp1->keyc;
    newtbp->keyv = dup_keys(tbp1->keyc, tbp1->keyv);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }

    return RDB_OK;
}

int
RDB_join(RDB_table *tbp1, RDB_table *tbp2, RDB_table **resultpp)
{
    RDB_table *newtbp;
    int ret;
    int i, j, k;
    RDB_type *tpltyp1 = tbp1->typ->var.basetyp;
    RDB_type *tpltyp2 = tbp2->typ->var.basetyp;
    int attrc1 = tpltyp1->var.tuple.attrc;
    int attrc2 = tpltyp2->var.tuple.attrc;
    int cattrc;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->kind = RDB_TB_JOIN;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.join.tbp1 = tbp1;
    newtbp->var.join.tbp2 = tbp2;
    newtbp->name = NULL;
    
    ret = RDB_join_relation_types(tbp1->typ, tbp2->typ, &newtbp->typ);
    if (ret != RDB_OK) {
        free(newtbp);
        return ret;
    }
    
    newtbp->var.join.common_attrv = malloc(sizeof(char *) * attrc1);
    cattrc = 0;
    for (i = 0; i < attrc1; i++) {
        for (j = 0;
             j < attrc2 && strcmp(tpltyp1->var.tuple.attrv[i].name,
                     tpltyp2->var.tuple.attrv[j].name) != 0;
             j++)
            ;
        if (j < attrc2)
            newtbp->var.join.common_attrv[cattrc++] =
                    tpltyp1->var.tuple.attrv[i].name;
    }
    newtbp->var.join.common_attrc = cattrc;

    /* Candidate keys */
    newtbp->keyc = tbp1->keyc * tbp2->keyc;
    newtbp->keyv = malloc(sizeof (RDB_string_vec) * newtbp->keyc);
    if (newtbp->keyv == NULL)
        goto error;
    for (i = 0; i < tbp1->keyc; i++) {
        for (j = 0; j < tbp2->keyc; j++) {
            RDB_string_vec *attrsp = &newtbp->keyv[i * tbp2->keyc + j];
           
            attrsp->strc = tbp1->keyv[i].strc + tbp2->keyv[j].strc;
            attrsp->strv = malloc(sizeof(char *) * attrsp->strc);
            if (attrsp->strv == NULL)
                goto error;
            for (k = 0; k < attrsp->strc; k++)
                attrsp->strv[k] = NULL;
            for (k = 0; k < tbp1->keyv[i].strc; k++) {
                attrsp->strv[k] = RDB_dup_str(tbp1->keyv[i].strv[k]);
                if (attrsp->strv[k] == NULL)
                    goto error;
            }
            for (k = 0; k < tbp2->keyv[j].strc; k++) {
                attrsp->strv[tbp1->keyv[i].strc + k] =
                        RDB_dup_str(tbp2->keyv[j].strv[k]);
                if (attrsp->strv[tbp1->keyv[i].strc + k] == NULL)
                    goto error;
            }
        }
    }

    *resultpp = newtbp;
    return RDB_OK;

error:
    if (newtbp->keyv != NULL) {
        for (i = 0; i < newtbp->keyc; i++) {
            if (newtbp->keyv[i].strv != NULL)
                RDB_free_strvec(newtbp->keyv[i].strc, newtbp->keyv[i].strv);
        }
    }
    free (newtbp);
    return ret;
}

int
RDB_extend(RDB_table *tbp, int attrc, RDB_virtual_attr attrv[],
        RDB_table **resultpp)
{
    int i;
    int ret;
    RDB_table *newtbp = NULL;
    RDB_attr *attrdefv = NULL;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    *resultpp = newtbp;
    newtbp->name = NULL;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_EXTEND;

    newtbp->keyc = tbp->keyc;
    newtbp->keyv = dup_keys(tbp->keyc, tbp->keyv);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }

    newtbp->var.extend.tbp = tbp;
    newtbp->var.extend.attrc = attrc;
    newtbp->var.extend.attrv = malloc(sizeof(RDB_virtual_attr) * attrc);
    if (newtbp->var.extend.attrv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    attrdefv = malloc(sizeof(RDB_attr) * attrc);
    if (attrdefv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    for (i = 0; i < attrc; i++) {
        if (!_RDB_legal_name(attrv[i].name)) {
            ret = RDB_INVALID_ARGUMENT;
            goto error;
        }
        newtbp->var.extend.attrv[i].name = RDB_dup_str(attrv[i].name);
        newtbp->var.extend.attrv[i].exp = attrv[i].exp;
        attrdefv[i].name = RDB_dup_str(attrv[i].name);
        if (attrdefv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        attrdefv[i].typ = RDB_expr_type(attrv[i].exp);
    }
    newtbp->typ = RDB_extend_relation_type(tbp->typ, attrc, attrdefv);

    for (i = 0; i < attrc; i++)
        free(attrdefv[i].name);       
    free(attrdefv);
    return RDB_OK;
error:
    free(newtbp);
    if (attrdefv != NULL) {
        for (i = 0; i < attrc; i++)
            free(attrdefv[i].name);       
        free(attrdefv);
    }
    for (i = 0; i < newtbp->keyc; i++) {
        if (newtbp->keyv[i].strv != NULL)
            RDB_free_strvec(newtbp->keyv[i].strc, newtbp->keyv[i].strv);
    }
    return ret;
}

static int
check_keyloss(RDB_table *tbp, int attrc, char *attrv[], RDB_bool presv[])
{
    int i, j, k;
    int count = 0;

    for (i = 0; i < tbp->keyc; i++) {
        for (j = 0; j < tbp->keyv[i].strc; j++) {
            /* Search for key attribute in attrv */
            for (k = 0;
                 (k < attrc) && (strcmp(tbp->keyv[i].strv[j], attrv[k]) != 0);
                 k++);
            /* If not found, exit loop */
            if (k >= attrc)
                break;
        }
        /* If the loop didn't terminate prematurely, the key is preserved */
        presv[i] = (RDB_bool) (j >= tbp->keyv[i].strc);
        if (presv[i])
            count++;
    }
    return count;
}

int
RDB_project(RDB_table *tbp, int attrc, char *attrv[], RDB_table **resultpp)
{
    RDB_table *newtbp;
    RDB_bool *presv;
    int keyc;
    int ret;
    int i;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->name = NULL;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_PROJECT;
    newtbp->var.project.tbp = tbp;
    newtbp->keyv = NULL;

    /* Create type */
    ret = RDB_project_relation_type(tbp->typ, attrc, attrv, &newtbp->typ);
    if (ret != RDB_OK) {
        free(newtbp);
        return ret;
    }

    presv = malloc(sizeof(RDB_bool) * tbp->keyc);
    if (presv == NULL) {
        goto error;
    }
    keyc = check_keyloss(tbp, attrc, attrv, presv);
    newtbp->var.project.keyloss = (RDB_bool) (keyc == 0);
    if (newtbp->var.project.keyloss) {
        /* Table is all-key */
        newtbp->keyc = 1;
        newtbp->keyv = all_key(newtbp);
        if (newtbp->keyv == NULL) {
            goto error;
        }
    } else {
        int j;
    
        /* Pick the keys which survived the projection */

        newtbp->keyc = keyc;
        newtbp->keyv = malloc(sizeof (RDB_string_vec) * keyc);
        if (newtbp->keyv == NULL) {
            goto error;
        }

        for (i = 0; i < keyc; i++) {
            newtbp->keyv[i].strv = NULL;
        }

        for (j = i = 0; j < tbp->keyc; j++) {
            if (presv[j]) {
                newtbp->keyv[i].strc = tbp->keyv[j].strc;
                newtbp->keyv[i].strv = RDB_dup_strvec(tbp->keyv[j].strc,
                        tbp->keyv[j].strv);
                if (newtbp->keyv[i].strv == NULL)
                    goto error;
                i++;
            }
        }
    }
    free(presv);

    *resultpp = newtbp;
    return RDB_OK;
error:
    free(presv);

    /* free keys */
    if (newtbp->keyv != NULL) {       
        for (i = 0; i < keyc; i++) {
            if (newtbp->keyv[i].strv != NULL)
                RDB_free_strvec(newtbp->keyv[i].strc, newtbp->keyv[i].strv);
        }
        free (newtbp->keyv);
    }
    RDB_drop_type(newtbp->typ, NULL);
    free(newtbp);

    return RDB_NO_MEMORY;
}

int
RDB_summarize(RDB_table *tb1p, RDB_table *tb2p, int addc, RDB_summarize_add addv[],
              RDB_table **resultpp)
{
    RDB_table *newtbp;
    RDB_type *tuptyp = NULL;
    int i, ai;
    int ret;
    int attrc;
    
    /* Additional attribute for each AVG */
    int avgc;
    char **avgv;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->name = NULL;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_SUMMARIZE;
    newtbp->keyc = tb2p->keyc;
    newtbp->keyv = dup_keys(tb2p->keyc, tb2p->keyv);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }
    newtbp->var.summarize.tb1p = tb1p;
    newtbp->var.summarize.tb2p = tb2p;
    newtbp->typ = NULL;

    newtbp->var.summarize.addc = addc;
    newtbp->var.summarize.addv = malloc(sizeof(RDB_summarize_add) * addc);
    if (newtbp->var.summarize.addv == NULL) {
        free(newtbp->keyv);
        free(newtbp);
        return RDB_NO_MEMORY;
    }
    avgc = 0;
    for (i = 0; i < addc; i++) {
        newtbp->var.summarize.addv[i].name = NULL;
        if (addv[i].op == RDB_AVG)
            avgc++;
    }
    avgv = malloc(avgc * sizeof(char *));
    for (i = 0; i < avgc; i++)
        avgv[i] = NULL;
    if (avgv == NULL) {
        free(newtbp->var.summarize.addv);
        free(newtbp->keyv);
        free(newtbp);
        return RDB_NO_MEMORY;
    }
    ai = 0;
    for (i = 0; i < addc; i++) {
        switch (addv[i].op) {
            case RDB_COUNTD:
            case RDB_SUMD:
            case RDB_AVGD:
                return RDB_NOT_SUPPORTED;
            case RDB_AVG:
                avgv[ai] = malloc(strlen(addv[i].name) + 3);
                if (avgv[ai] == NULL) {
                    ret = RDB_NO_MEMORY;
                    goto error;
                }
                strcpy(avgv[ai], addv[i].name);
                strcat(avgv[ai], AVG_COUNT_SUFFIX);
                ai++;
                break;
            default: ;
        }
        newtbp->var.summarize.addv[i].op = addv[i].op;
        newtbp->var.summarize.addv[i].name = RDB_dup_str(addv[i].name);
        if (newtbp->var.summarize.addv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        newtbp->var.summarize.addv[i].exp = addv[i].exp;
    }

    /* Create type */

    attrc = tb2p->typ->var.basetyp->var.tuple.attrc + addc + avgc;
    tuptyp = malloc(sizeof (RDB_type));
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->var.tuple.attrc = attrc;
    tuptyp->var.tuple.attrv = malloc(attrc * sizeof(RDB_attr));
    for (i = 0; i < addc; i++) {
        RDB_type *typ = addv[i].op == RDB_COUNT ? &RDB_INTEGER
                : RDB_expr_type(addv[i].exp);

        tuptyp->var.tuple.attrv[i].name = RDB_dup_str(addv[i].name);
        if (tuptyp->var.tuple.attrv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        if (addv[i].op == RDB_COUNT) {
            tuptyp->var.tuple.attrv[i].typ = &RDB_INTEGER;
        } else {
            ret = aggr_type(tb1p->typ->var.basetyp, typ,
                        addv[i].op, &tuptyp->var.tuple.attrv[i].typ);
            if (ret != RDB_OK)
                goto error;
        }
        tuptyp->var.tuple.attrv[i].defaultp = NULL;
        tuptyp->var.tuple.attrv[i].options = 0;
    }
    for (i = 0; i < tb2p->typ->var.basetyp->var.tuple.attrc; i++) {
        tuptyp->var.tuple.attrv[addc + i].name =
                tb2p->typ->var.basetyp->var.tuple.attrv[i].name;
        tuptyp->var.tuple.attrv[addc + i].typ =
                tb2p->typ->var.basetyp->var.tuple.attrv[i].typ;
        tuptyp->var.tuple.attrv[addc + i].defaultp = NULL;
        tuptyp->var.tuple.attrv[addc + i].options = 0;
    }
    for (i = 0; i < avgc; i++) {
        tuptyp->var.tuple.attrv[attrc - avgc + i].name = avgv[i];
        tuptyp->var.tuple.attrv[attrc - avgc + i].typ = &RDB_INTEGER;
        tuptyp->var.tuple.attrv[attrc - avgc + i].defaultp = NULL;
        tuptyp->var.tuple.attrv[attrc - avgc + i].options = 0;
    }
        
    newtbp->typ = malloc(sizeof (RDB_type));
    if (newtbp->typ == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    newtbp->typ->kind = RDB_TP_RELATION;
    newtbp->typ->var.basetyp = tuptyp;

    *resultpp = newtbp;
    return RDB_OK;
error:
    if (tuptyp != NULL) {
        free(tuptyp->var.tuple.attrv);
        free(tuptyp);
    }
    if (newtbp->typ != NULL)
        free(newtbp->typ);
    for (i = 0; i < avgc; i++)
        free(avgv[i]);
    free(avgv);
    for (i = 0; i < addc; i++) {
        free(newtbp->var.summarize.addv[i].name);
    }
    free(newtbp->keyv);
    free(newtbp);
    return ret;
}

int
RDB_rename(RDB_table *tbp, int renc, RDB_renaming renv[],
           RDB_table **resultpp)
{
    RDB_table *newtbp;
    int i;
    int ret;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->name = NULL;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_RENAME;
    newtbp->keyc = tbp->keyc;

    ret = RDB_rename_relation_type(tbp->typ, renc, renv, &newtbp->typ);
    if (ret != RDB_OK) {
        free(newtbp);
        return ret;
    }

    newtbp->var.rename.renc = renc;
    newtbp->var.rename.renv = malloc(sizeof (RDB_renaming) * renc);
    if (newtbp->var.rename.renv == NULL) {
        RDB_drop_type(newtbp->typ, NULL);
        free(newtbp);
        return RDB_NO_MEMORY;
    }
    for (i = 0; i < renc; i++) {
        newtbp->var.rename.renv[i].to = newtbp->var.rename.renv[i].from = NULL;
    }
    for (i = 0; i < renc; i++) {
        newtbp->var.rename.renv[i].to = RDB_dup_str(renv[i].to);
        if (newtbp->var.rename.renv[i].to == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        newtbp->var.rename.renv[i].from = RDB_dup_str(renv[i].from);
        if (newtbp->var.rename.renv[i].from == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }
    newtbp->var.rename.tbp = tbp;

    newtbp->keyc = tbp->keyc;
    newtbp->keyv = dup_rename_keys(tbp->keyc, tbp->keyv, renc, renv);
    if (newtbp->keyv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    *resultpp = newtbp;
    return RDB_OK; 
error:
    for (i = 0; i < renc; i++) {
        free(newtbp->var.rename.renv[i].to);
        free(newtbp->var.rename.renv[i].from);
    }
    free(newtbp->var.rename.renv);
    free(newtbp);
    return ret;
}
