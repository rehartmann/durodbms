/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

static int
insert_intersect(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_transaction tx;
    int res, res2;

    /* Try to insert tup into both tables. If one insert fails,
     * the insert into the intersection fails.
     * If only one of the inserts fails because the tuple already exists,
     * the overall insert succeeds. 
     */

    /* Start a subtransaction */
    res = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (res != RDB_OK)
        return res;
            
    res = RDB_insert(tbp->var.intersect.tbp1, tup, txp);
    if (res != RDB_OK && res != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return res;
    }

    res2 = RDB_insert(tbp->var.intersect.tbp2, tup, txp);
    if (res2 != RDB_OK && res2 != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return res2;
    }
    /* If both inserts fail because the tuples exist, the insert fails
     * with an error of RDB_ELEMENT_EXISTS.
     */
    if (res == RDB_ELEMENT_EXISTS && res2 == RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return RDB_ELEMENT_EXISTS;
    }       

    return RDB_commit(&tx);
}

static int
insert_join(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_transaction tx;
    int res, res2;

    /* Try to insert tup into both tables. If one insert fails,
     * the insert into the join fails.
     * If only one of the inserts fails because the tuple already exists,
     * the overall insert succeeds. 
     */

    /* Start a subtransaction */
    res = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (res != RDB_OK)
        return res;
            
    res = RDB_insert(tbp->var.join.tbp1, tup, txp);
    if (res != RDB_OK && res != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return res;
    }

    res2 = RDB_insert(tbp->var.join.tbp2, tup, txp);
    if (res2 != RDB_OK && res2 != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return res2;
    }
    /* If both inserts fail because the tuples exist, the insert fails
     * with an error of RDB_ELEMENT_EXISTS.
     */
    if (res == RDB_ELEMENT_EXISTS && res2 == RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return RDB_ELEMENT_EXISTS;
    }       
    
    return RDB_commit(&tx);
}

int
RDB_insert(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    int res;
    RDB_bool b;

    switch (tbp->kind) {
        case RDB_TB_STORED:
        {
            int i;
            RDB_type *tuptyp = tbp->typ->complex.basetyp;
            int attrcount = tuptyp->complex.tuple.attrc;
            RDB_field *fvp;

            fvp = malloc(sizeof(RDB_field) * attrcount);
            if (fvp == NULL)
                return RDB_NO_MEMORY;
            for (i = 0; i < attrcount; i++) {
                int *fnop;
                RDB_value *valp;
                
                fnop = RDB_hashmap_get(&tbp->var.stored.attrmap,
                        tuptyp->complex.tuple.attrv[i].name, NULL);
                valp = RDB_tuple_get(tup, tuptyp->complex.tuple.attrv[i].name);
                
                /* Typecheck */
                if (!RDB_type_equals(valp->typ,
                                     tuptyp->complex.tuple.attrv[i].type)) {
                     return RDB_TYPE_MISMATCH;
                }
                fvp[*fnop].datap = RDB_value_irep(valp, &fvp[*fnop].len);
            }
            res = RDB_insert_rec(tbp->var.stored.recmapp, fvp, txp->txid);
            free(fvp);
            if (RDB_is_syserr(res)) {
                RDB_rollback(txp);
            } else if (res == RDB_KEY_VIOLATION) {
                if (RDB_table_contains(tbp, tup, txp) == RDB_OK)
                    res = RDB_ELEMENT_EXISTS;
            }
            return res;
        }
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            res = RDB_evaluate_bool(tbp->var.select.exprp, tup, txp, &b);
            if (res != RDB_OK)
                return res;
            if (!b)
                return RDB_PREDICATE_VIOLATION;
            return RDB_insert(tbp->var.select.tbp, tup, txp);
        case RDB_TB_MINUS:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_UNION:
        {
            int res2;

            /* !! may be very inefficient */        
            if (RDB_table_contains(tbp->var._union.tbp1, tup, txp) == RDB_OK
                    || RDB_table_contains(tbp->var._union.tbp2, tup, txp) == RDB_OK)
                return RDB_ELEMENT_EXISTS;
             
            /* Try to insert tup into both tables. The insertion into the union
             * fails if (1) one of the inserts fails for a reason other than
             * a predicate violation or (2) if both inserts fail because of
             * a predicate violation.
             */
            res = RDB_insert(tbp->var._union.tbp1, tup, txp);
            if (res != RDB_OK) {
                if (res != RDB_KEY_VIOLATION && res != RDB_PREDICATE_VIOLATION)
                    return res;
            }
            res2 = RDB_insert(tbp->var._union.tbp2, tup, txp);
            if (res2 != RDB_OK) {
                if (res2 != RDB_KEY_VIOLATION && res2 != RDB_PREDICATE_VIOLATION)
                    return res2;
                /* 2nd insert failed because of a predicate violation, so
                 * it depends on the result of the 1st insert.
                 */
                return res;
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
                RDB_value val;
                RDB_bool iseq;
                
                res = RDB_evaluate(vattrp->exp, tup, txp, &val);
                if (res != RDB_OK)
                    return res;
                iseq = RDB_value_equals(&val, (RDB_value *)RDB_tuple_get(
                        tup, vattrp->name));
                RDB_destroy_value(&val);
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
             /* ... */
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
 * <primary key attribute>=<constant expression> and returns the expression
 * if yes, or NULL if no.
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
upd_to_vals(RDB_table *tbp, int attrc, const RDB_attr_update attrv[],
               RDB_tuple *tplp, RDB_value *valv, RDB_transaction *txp)
{
    int i, res;

    for (i = 0; i < attrc; i++) {
        res = RDB_evaluate(attrv[i].valuep, tplp, txp, &valv[i]);
        if (res != RDB_OK) {
            int j;
            
            for (j = 0; j < i; j++)
                RDB_destroy_value(&valv[i]);
            return res;
        }
    }                
    return RDB_OK;
}

static int
update_stored(RDB_table *tbp, RDB_expression *condp,
        int attrc, const RDB_attr_update attrv[],
        RDB_transaction *txp)
{
    int res;
    int i;
    RDB_cursor *curp;
    RDB_tuple tpl;
    void *datap;
    size_t len;
    RDB_type *tpltyp = tbp->typ->complex.basetyp;
    RDB_expression *exprp;
    RDB_value *valv = malloc(sizeof(RDB_value) * attrc);
    RDB_field *fieldv = malloc(sizeof(RDB_field) * attrc);
    RDB_bool b;

    if (valv == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }

    if (fieldv == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }

    /* Check if the primary index can be used */
    exprp = condp != NULL ? pindex_expr(tbp, condp) : NULL;
    if (exprp != NULL) {
        RDB_field fv;
        RDB_value val;

        /* Read tuple (only needed if the expression refers to
           attributes of the updates table, so there is room
           for optimization) */
        /* ... */

        /* Evaluate condition */
        res = RDB_evaluate(exprp, NULL, txp, &val);
        if (res != RDB_OK) {
            goto error;
        }

        /* Convert to a field value */
        fv.datap = RDB_value_irep(&val, &fv.len);

        res = upd_to_vals(tbp, attrc, attrv, &tpl, valv, txp);
        if (res != RDB_OK) {
            goto error;
        }

        for (i = 0; i < attrc; i++) {
            fieldv[i].no = *(int*) RDB_hashmap_get(&tbp->var.stored.attrmap,
                    attrv[i].name, NULL);
            fieldv[i].datap = RDB_value_irep(&valv[i], &fieldv[i].len);
        }
        
        res = RDB_update_rec(tbp->var.stored.recmapp, &fv, attrc, fieldv,
                txp->txid);
        if (RDB_is_syserr(res)) {
            RDB_rollback(txp);
            return res;
        }
        free(valv);
        free(fieldv);
        return res;
    }

    /*
     * Walk through the records and update them if the expression pointed to
     * by condp evaluates to true.
     * This may not work correctly if a primary key attribute is updated
     * or if the expression refers to the table itself.
     * A possible solution in this case is to perform the update in three steps:
     * 1. Iterate over the records and insert the updated records into
     * a temporary table.
     * 2. Delete the updated records from the original table.
     * 3. Insert the records from the temporary table into the original table.
     * This is currently not implemented.
     */
    res = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp, 0, txp->txid);
    if (res != RDB_OK)        
        return res;
    res = RDB_cursor_first(curp);
    if (res == RDB_NOT_FOUND) {
        RDB_destroy_cursor(curp);
        return RDB_OK;
    }
    
    do {
        RDB_init_tuple(&tpl);
        for (i = 0; i < tpltyp->complex.tuple.attrc; i++) {
            RDB_value val;

            res = RDB_cursor_get(curp, i, &datap, &len);
            if (res != 0) {
               goto error;
            }
            RDB_init_value(&val);
            res = RDB_irep_to_value(&val, tpltyp->complex.tuple.attrv[i].type,
                              datap, len);
            if (res != RDB_OK) {
               goto error;
            }
            res = RDB_tuple_set(&tpl, tpltyp->complex.tuple.attrv[i].name, &val);
            if (res != RDB_OK) {
               goto error;
            }
        }
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            res = RDB_evaluate_bool(condp, &tpl, txp, &b);
            if (res != RDB_OK)
                return res;
        }
        if (b) {
            res = upd_to_vals(tbp, attrc, attrv, &tpl, valv, txp);
            if (res != RDB_OK) {
                goto error;
            }
            for (i = 0; i < attrc; i++) {
                /* Typecheck */
                if (!RDB_type_equals(valv[i].typ,
                        _RDB_tuple_attr_type(tbp->typ->complex.basetyp, attrv[i].name))) {
                    res = RDB_TYPE_MISMATCH;
                    goto error;
                }

                /* Get field number from map */
                fieldv[i].no = *(int*) RDB_hashmap_get(&tbp->var.stored.attrmap,
                        attrv[i].name, NULL);
                
                /* Get data */
                fieldv[i].datap = RDB_value_irep(&valv[i], &fieldv[i].len);
            }
            res = RDB_cursor_set(curp, attrc, fieldv);
            for (i = 0; i < attrc; i++)
                RDB_destroy_value(&valv[i]);
            if (res != RDB_OK) {
                if (RDB_is_syserr(res)) {
                    RDB_rollback(txp);
                }
                goto error;
            }
        }
        RDB_destroy_tuple(&tpl);
        res = RDB_cursor_next(curp);
    } while (res == RDB_OK);
    if (res != RDB_NOT_FOUND)
        goto error;
    free(valv);
    free(fieldv);
    RDB_destroy_cursor(curp);
    return RDB_OK;
error:
    free(valv);
    free(fieldv);

    RDB_destroy_cursor(curp);
    RDB_destroy_tuple(&tpl);
    return res;
}  

int
RDB_update(RDB_table *tbp, RDB_expression *condp, int attrc,
                const RDB_attr_update attrv[], RDB_transaction *txp)
{
    switch (tbp->kind) {
        case RDB_TB_STORED:
            return update_stored(tbp, condp, attrc, attrv, txp);
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
    int res;
    int i;
    RDB_cursor *curp;
    RDB_tuple tpl;
    void *datap;
    size_t len;
    RDB_type *tpltyp = tbp->typ->complex.basetyp;
    RDB_expression *exprp;
    RDB_bool b;

    /* Check if the primary index can be used */
    exprp = condp != NULL ? pindex_expr(tbp, condp) : NULL;
    if (exprp != NULL) {
        RDB_field fv;
        RDB_value val;

        res = RDB_evaluate(exprp, NULL, txp, &val);
        if (res != RDB_OK)
            return res;
        fv.datap = RDB_value_irep(&val, &fv.len);
        res = RDB_delete_rec(tbp->var.stored.recmapp, &fv, txp->txid);
        if (RDB_is_syserr(res)) {
            RDB_rollback(txp);
            return res;
        }
        return res;
    }

    res = RDB_recmap_cursor(&curp, tbp->var.stored.recmapp, 0, txp->txid);
    if (res != RDB_OK)
        return res;
    res = RDB_cursor_first(curp);
    if (res == RDB_NOT_FOUND) {
        RDB_destroy_cursor(curp);
        return RDB_OK;
    }
    
    do {
        RDB_init_tuple(&tpl);
        for (i = 0; i < tpltyp->complex.tuple.attrc; i++) {
            RDB_value val;

            res = RDB_cursor_get(curp, i, &datap, &len);
            if (res != 0) {
               RDB_destroy_tuple(&tpl);
               goto error;
            }
            RDB_init_value(&val);
            res = RDB_irep_to_value(&val, tpltyp->complex.tuple.attrv[i].type,
                             datap, len);
            if (res != RDB_OK) {
               RDB_destroy_tuple(&tpl);
               goto error;
            }
            res = RDB_tuple_set(&tpl, tpltyp->complex.tuple.attrv[i].name, &val);
            if (res != RDB_OK) {
               RDB_destroy_tuple(&tpl);
               goto error;
            }
        }
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            res = RDB_evaluate_bool(condp, &tpl, txp, &b);
            if (res != RDB_OK)
                return res;
        }
        if (b) {
            res = RDB_cursor_delete(curp);
            if (res != RDB_OK) {
                RDB_destroy_tuple(&tpl);
                goto error;
            }
        }
        RDB_destroy_tuple(&tpl);
        res = RDB_cursor_next(curp);
    } while (res == RDB_OK);
    if (res != RDB_NOT_FOUND)
        goto error;
    RDB_destroy_cursor(curp);
    return RDB_OK;
error:
    RDB_destroy_cursor(curp);
    return res;
}  

int
RDB_delete(RDB_table *tbp, RDB_expression *condp, RDB_transaction *txp)
{
    int res;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            return delete_stored(tbp, condp, txp);
        case RDB_TB_MINUS:
            return RDB_delete(tbp->var.minus.tbp1, condp, txp);
        case RDB_TB_UNION:
            res = RDB_delete(tbp->var._union.tbp1, condp, txp);
            if (res != RDB_OK)
                return res;
            return RDB_delete(tbp->var._union.tbp2, condp, txp);
        case RDB_TB_INTERSECT:
            res = RDB_delete(tbp->var.intersect.tbp1, condp, txp);
            if (res != RDB_OK)
                return res;
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
            return RDB_NOT_SUPPORTED;
    }
    /* should never be reached */
    abort();
}

int
RDB_copy_table(RDB_table *dstp, RDB_table *srcp, RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    RDB_tuple tpl;
    RDB_transaction tx;
    int res;

    /* check if types of the two tables match */
    if (!RDB_type_equals(dstp->typ, srcp->typ))
        return RDB_TYPE_MISMATCH;

    /* start subtransaction */
    res = RDB_begin_tx(&tx, txp->dbp, txp);
    if (res != RDB_OK)
        return res;

    /* delete all tuples from destination table */
    res = RDB_delete(dstp, NULL, &tx);
    if (res != RDB_OK)
        goto error;

    /* copy all tuples from source table to destination table */

    res = _RDB_table_qresult(srcp, &qrp, &tx);
    if (res != RDB_OK)
        goto error;

    RDB_init_tuple(&tpl);

    while ((res = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        res = RDB_insert(dstp, &tpl, &tx);
        RDB_destroy_tuple(&tpl);
        if (res != RDB_OK) {
            goto error;
        }
    }
    if (res != RDB_NOT_FOUND)
        goto error;

    _RDB_drop_qresult(qrp, &tx);

    return RDB_commit(&tx);

error:
    if (qrp != NULL)
        _RDB_drop_qresult(qrp, &tx);
    RDB_rollback(&tx);
    return res;
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
        case RDB_COUNT:
        case RDB_COUNTD:
            /* only to avoid compiler warnings */
        case RDB_AVG:
        case RDB_AVGD:
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
              RDB_transaction *txp, RDB_value *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_tuple tpl;
    int res;
    int count; /* only for AVG */

    /* attrname may only be NULL if op == RDB_AVG or table is unary */
    if (attrname == NULL && op != RDB_COUNT) {
        if (tbp->typ->complex.basetyp->complex.tuple.attrc != 1)
            return RDB_ILLEGAL_ARG;
        attrname = tbp->typ->complex.basetyp->complex.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_attr_type(tbp->typ->complex.basetyp, attrname);
        if (attrtyp == NULL)
            return RDB_ILLEGAL_ARG;
    }

    res = aggr_type(tbp->typ->complex.basetyp, attrtyp, op, &resultp->typ);
    if (res != RDB_OK)
        return res;

    /* initialize result */
    switch (op) {
        case RDB_COUNT:
            resultp->var.int_val = 0;
            break;
        case RDB_AVG:
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
            return RDB_ILLEGAL_ARG;
        break;
    }

    /* perform aggregation */

    RDB_init_tuple(&tpl);

    res = _RDB_table_qresult(tbp, &qrp, txp);
    if (res != RDB_OK)
        return res;

    while ((res = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
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
    if (res != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        return res;
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
    return RDB_eq(RDB_expr_attr(attrp->name, attrp->type),
                  RDB_value_const(RDB_tuple_get(tup, attrp->name)));
}

static int
project_contains(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_type *tuptyp = tbp->typ->complex.basetyp;
    RDB_bool result;
    int res;
    
    if (tuptyp->complex.tuple.attrc > 0) {
        RDB_expression *condp;
        RDB_table *seltbp;
        int i, res;

        /* create where-condition */        
        condp = attr_eq(&tuptyp->complex.tuple.attrv[0], tup);
        
        for (i = 1; i < tuptyp->complex.tuple.attrc; i++) {
            condp = RDB_and(condp, attr_eq(&tuptyp->complex.tuple.attrv[0],
                    tup));
        }
        if (condp == NULL)
            return RDB_NO_MEMORY;

        /* create selection table */
        res = RDB_select(tbp, condp, &seltbp);
        if (res != RDB_OK) {
            RDB_drop_expr(condp);
            return res;
        }

        /* check if selection is empty */
        res = RDB_table_is_empty(seltbp, txp, &result);
        _RDB_drop_table(seltbp, txp, RDB_FALSE);
        if (res != RDB_OK)
            return res;
        return result ? RDB_NOT_FOUND : RDB_OK;
    } else {
        /* projection with no attributes */
    
        res = RDB_table_is_empty(tbp->var.project.tbp, txp, &result);
        if (res != RDB_OK)
            return res;
        return result ? RDB_NOT_FOUND : RDB_OK;
    }
}
    
int
RDB_table_contains(RDB_table *tbp, const RDB_tuple *tup, RDB_transaction *txp)
{
    int res;
    RDB_bool b;
    
    switch (tbp->kind) {
        case RDB_TB_STORED:
            {
                int i;
                RDB_type *tuptyp = tbp->typ->complex.basetyp;
                int attrcount = tuptyp->complex.tuple.attrc;
                RDB_field *fvp;

                fvp = malloc(sizeof(RDB_field) * attrcount);
                if (fvp == NULL)
                    return RDB_NO_MEMORY;
                for (i = 0; i < attrcount; i++) {
                    RDB_value* valp;
                    int *fnop = RDB_hashmap_get(&tbp->var.stored.attrmap,
                            tuptyp->complex.tuple.attrv[i].name, NULL);
                    valp = RDB_tuple_get(tup,
                            tuptyp->complex.tuple.attrv[i].name);
                    fvp[*fnop].datap = RDB_value_irep(valp, &fvp[*fnop].len);
                }
                res = RDB_contains_rec(tbp->var.stored.recmapp, fvp, txp->txid);
                free(fvp);
                if (RDB_is_syserr(res)) {
                    RDB_rollback(txp);
                }
                return res;
            }
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            res = RDB_evaluate_bool(tbp->var.select.exprp, tup, txp, &b);
            if (res != RDB_OK)
                return res;
            if (!b)
                return RDB_NOT_FOUND;
            return RDB_table_contains(tbp->var.select.tbp, tup, txp);
        case RDB_TB_UNION:
            res = RDB_table_contains(tbp->var._union.tbp1, tup, txp);
            if (res == RDB_OK)
                return RDB_OK;
            return RDB_table_contains(tbp->var._union.tbp2, tup, txp);            
        case RDB_TB_MINUS:
            res = RDB_table_contains(tbp->var.minus.tbp1, tup, txp);
            if (res != RDB_OK)
                return res;
            res = RDB_table_contains(tbp->var.minus.tbp2, tup, txp);
            if (res == RDB_OK)
                return RDB_NOT_FOUND;
            if (res == RDB_NOT_FOUND)
                return RDB_OK;
            return res;
        case RDB_TB_INTERSECT:
            res = RDB_table_contains(tbp->var.intersect.tbp1, tup, txp);
            if (res != RDB_OK)
                return res;
            return RDB_table_contains(tbp->var.intersect.tbp2, tup, txp);
        case RDB_TB_JOIN:
            res = RDB_table_contains(tbp->var.join.tbp1, tup, txp);
            if (res != RDB_OK)
                return res;
            return RDB_table_contains(tbp->var.join.tbp2, tup, txp);
        case RDB_TB_EXTEND:
            return RDB_table_contains(tbp->var.extend.tbp, tup, txp);
        case RDB_TB_PROJECT:
            return project_contains(tbp, tup, txp);
        case RDB_TB_SUMMARIZE:
            {
                int res2;
            
                RDB_qresult *qrp;
                res = _RDB_table_qresult(tbp, &qrp, txp);
                if (res != RDB_OK)
                    return res;
                res = _RDB_qresult_contains(qrp, tup, txp);
                res2 = _RDB_drop_qresult(qrp, txp);
                if (res2 != RDB_OK)
                    return res2;
                return res;
            }
        case RDB_TB_RENAME:
            /* !! */
            return RDB_NOT_SUPPORTED;
   }
    /* should never be reached */
    abort();
}

int
RDB_table_is_empty(RDB_table *tbp, RDB_transaction *txp, RDB_bool *resultp)
{
    int res;
    RDB_qresult *qrp;
    RDB_tuple tpl;

    res = _RDB_table_qresult(tbp, &qrp, txp);
    if (res != RDB_OK)
        return res;

    RDB_init_tuple(&tpl);

    res = _RDB_next_tuple(qrp, &tpl, txp);
    RDB_destroy_tuple(&tpl);
    if (res == RDB_OK)
        *resultp = RDB_FALSE;
    else if (res == RDB_NOT_FOUND)
        *resultp = RDB_TRUE;
    else {
        _RDB_drop_qresult(qrp, txp);
        return res;
    }
    return _RDB_drop_qresult(qrp, txp);
}

static RDB_key_attrs *
dup_keys(int keyc, RDB_key_attrs *keyv) {
    RDB_key_attrs *newkeyv;
    int i;

    newkeyv = malloc(keyc * sizeof(RDB_key_attrs));
    if (newkeyv == NULL) {
        return NULL;
    }
    for (i = 0; i < keyc; i++)
        newkeyv[i].attrv = NULL;
    for (i = 0; i < keyc; i++) {
        newkeyv[i].attrc = keyv[i].attrc;
        newkeyv[i].attrv = RDB_dup_strvec(
                keyv[i].attrc, keyv[i].attrv);
        if (newkeyv[i].attrv == NULL) {
            goto error;
        }
    }
    return newkeyv;
error:
    /* free keys */
    for (i = 0; i < keyc; i++) {
        if (newkeyv[i].attrv != NULL)
            RDB_free_strvec(newkeyv[i].attrc, newkeyv[i].attrv);
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
            RDB_init_value(&newtbp->var.select.val);
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

static RDB_key_attrs *all_key(RDB_table *tbp) {
    RDB_key_attrs *keyv = malloc(sizeof (RDB_key_attrs));
    int attrc;
    int i;
    
    if (keyv == NULL)
        return NULL;
    
    attrc = keyv[0].attrc =
            tbp->typ->complex.basetyp->complex.tuple.attrc;
    keyv[0].attrv = malloc(sizeof(char *) * attrc);
    if (keyv[0].attrv == NULL) {
        free(keyv);
        return NULL;
    }
    for (i = 0; i < attrc; i++)
        keyv[0].attrv[i] = NULL;
    for (i = 0; i < attrc; i++) {
        keyv[0].attrv[i] = RDB_dup_str(
                tbp->typ->complex.basetyp->complex.tuple.attrv[i].name);
        if (keyv[0].attrv[i] == NULL) {
            goto error;
        }
    }

    return keyv;
error:
    RDB_free_strvec(keyv[0].attrc, keyv[0].attrv);
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
    int res;
    int i, j, k;
    RDB_type *tpltyp1 = tbp1->typ->complex.basetyp;
    RDB_type *tpltyp2 = tbp2->typ->complex.basetyp;
    int attrc1 = tpltyp1->complex.tuple.attrc;
    int attrc2 = tpltyp2->complex.tuple.attrc;
    int cattrc;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->kind = RDB_TB_JOIN;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.join.tbp1 = tbp1;
    newtbp->var.join.tbp2 = tbp2;

    newtbp->typ = malloc(sizeof (RDB_type));
    if (newtbp->typ == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    newtbp->typ->name = NULL;
    newtbp->typ->kind = RDB_TP_RELATION;

    res = RDB_join_tuple_types(tbp1->typ->complex.basetyp,
            tbp2->typ->complex.basetyp, &newtbp->typ->complex.basetyp);
    if (res != RDB_OK)
        goto error;
    newtbp->name = NULL;
    
    newtbp->var.join.common_attrv = malloc(sizeof(char *) * attrc1);
    cattrc = 0;
    for (i = 0; i < attrc1; i++) {
        for (j = 0;
             j < attrc2 && strcmp(tpltyp1->complex.tuple.attrv[i].name,
                     tpltyp2->complex.tuple.attrv[j].name) != 0;
             j++)
            ;
        if (j < attrc2)
            newtbp->var.join.common_attrv[cattrc++] =
                    tpltyp1->complex.tuple.attrv[i].name;
    }
    newtbp->var.join.common_attrc = cattrc;

    /* Candidate keys */
    newtbp->keyc = tbp1->keyc * tbp2->keyc;
    newtbp->keyv = malloc(sizeof (RDB_key_attrs) * newtbp->keyc);
    if (newtbp->keyv == NULL)
        goto error;
    for (i = 0; i < tbp1->keyc; i++) {
        for (j = 0; j < tbp2->keyc; j++) {
            RDB_key_attrs *attrsp = &newtbp->keyv[i * tbp2->keyc + j];
           
            attrsp->attrc = tbp1->keyv[i].attrc + tbp2->keyv[j].attrc;
            attrsp->attrv = malloc(sizeof(char *) * attrsp->attrc);
            if (attrsp->attrv == NULL)
                goto error;
            for (k = 0; k < attrsp->attrc; k++)
                attrsp->attrv[k] = NULL;
            for (k = 0; k < tbp1->keyv[i].attrc; k++) {
                attrsp->attrv[k] = RDB_dup_str(tbp1->keyv[i].attrv[k]);
                if (attrsp->attrv[k] == NULL)
                    goto error;
            }
            for (k = 0; k < tbp2->keyv[j].attrc; k++) {
                attrsp->attrv[tbp1->keyv[i].attrc + k] =
                        RDB_dup_str(tbp2->keyv[j].attrv[k]);
                if (attrsp->attrv[tbp1->keyv[i].attrc + k] == NULL)
                    goto error;
            }
        }
    }

    *resultpp = newtbp;
    return RDB_OK;

error:
    if (newtbp->keyv != NULL) {
        for (i = 0; i < newtbp->keyc; i++) {
            if (newtbp->keyv[i].attrv != NULL)
                RDB_free_strvec(newtbp->keyv[i].attrc, newtbp->keyv[i].attrv);
        }
    }
    free (newtbp);
    return res;
}

int
RDB_extend(RDB_table *tbp, int attrc, RDB_virtual_attr attrv[],
        RDB_table **resultpp)
{
    int i;
    int res;
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
        res = RDB_NO_MEMORY;
        goto error;
    }
    attrdefv = malloc(sizeof(RDB_attr) * attrc);
    if (attrdefv == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    for (i = 0; i < attrc; i++) {
        if (!_RDB_legal_name(attrv[i].name)) {
            res = RDB_ILLEGAL_ARG;
            goto error;
        }
        newtbp->var.extend.attrv[i].name = RDB_dup_str(attrv[i].name);
        newtbp->var.extend.attrv[i].exp = attrv[i].exp;
        attrdefv[i].name = RDB_dup_str(attrv[i].name);
        if (attrdefv[i].name == NULL) {
            res = RDB_NO_MEMORY;
            goto error;
        }
        attrdefv[i].type = RDB_expr_type(attrv[i].exp);
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
        if (newtbp->keyv[i].attrv != NULL)
            RDB_free_strvec(newtbp->keyv[i].attrc, newtbp->keyv[i].attrv);
    }
    return res;
}

static int
check_keyloss(RDB_table *tbp, int attrc, char *attrv[], RDB_bool presv[]) {
    int i, j, k;
    int count = 0;

    for (i = 0; i < tbp->keyc; i++) {
        for (j = 0; j < tbp->keyv[i].attrc; j++) {
            /* Search for key attribute in attrv */
            for (k = 0;
                 (k < attrc) && (strcmp(tbp->keyv[i].attrv[j], attrv[k]) != 0);
                 k++);
            /* If not found, exit loop */
            if (k >= attrc)
                break;
        }
        /* If the loop didn't terminate prematurely, the key is preserved */
        presv[i] = (RDB_bool) (j >= tbp->keyv[i].attrc);
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
    int res;
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
    res = RDB_project_relation_type(tbp->typ, attrc, attrv, &newtbp->typ);
    if (res != RDB_OK) {
        free(newtbp);
        return res;
    }

    presv = malloc(sizeof(RDB_bool) * attrc);
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
        newtbp->keyv = malloc(sizeof (RDB_key_attrs) * keyc);
        if (newtbp->keyv == NULL) {
            goto error;
        }

        for (i = 0; i < tbp->keyc; i++) {
            newtbp->keyv[i].attrv = NULL;
        }
        for (j = i = 0; j < tbp->keyc; j++) {
            if (presv[j]) {
                newtbp->keyv[i].attrc = tbp->keyv[j].attrc;
                newtbp->keyv[i].attrv = RDB_dup_strvec(tbp->keyv[j].attrc,
                        tbp->keyv[j].attrv);
                if (newtbp->keyv[i].attrv == NULL)
                    goto error;
                i++;
            }
        }

        newtbp->keyv = dup_keys(tbp->keyc, tbp->keyv);
    }
    free(presv);
    
    *resultpp = newtbp;
    return RDB_OK;
error:
    free(presv);

    /* free keys */
    if (newtbp->keyv != NULL) {       
        for (i = 0; i < keyc; i++) {
            if (newtbp->keyv[i].attrv != NULL)
                RDB_free_strvec(newtbp->keyv[i].attrc, newtbp->keyv[i].attrv);
        }
        free (newtbp->keyv);
    }
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
    int res;
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
                    res = RDB_NO_MEMORY;
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
            res = RDB_NO_MEMORY;
            goto error;
        }
        newtbp->var.summarize.addv[i].exp = addv[i].exp;
    }

    /* Create type */

    attrc = tb2p->typ->complex.basetyp->complex.tuple.attrc + addc + avgc;
    tuptyp = malloc(sizeof (RDB_type));
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->complex.tuple.attrc = attrc;
    tuptyp->complex.tuple.attrv = malloc(attrc * sizeof(RDB_attr));
    for (i = 0; i < addc; i++) {
        RDB_type *typ = addv[i].op == RDB_COUNT ? &RDB_INTEGER : RDB_expr_type(addv[i].exp);

        tuptyp->complex.tuple.attrv[i].name = addv[i].name;
        if (addv[i].op == RDB_COUNT) {
            tuptyp->complex.tuple.attrv[i].type = &RDB_INTEGER;
        } else {
            res = aggr_type(tb1p->typ->complex.basetyp, typ,
                        addv[i].op, &tuptyp->complex.tuple.attrv[i].type);
            if (res != RDB_OK)
                goto error;
        }
        tuptyp->complex.tuple.attrv[i].defaultp = NULL;
        tuptyp->complex.tuple.attrv[i].options = 0;
    }
    for (i = 0; i < tb2p->typ->complex.basetyp->complex.tuple.attrc; i++) {
        tuptyp->complex.tuple.attrv[addc + i].name =
                tb2p->typ->complex.basetyp->complex.tuple.attrv[i].name;
        tuptyp->complex.tuple.attrv[addc + i].type =
                tb2p->typ->complex.basetyp->complex.tuple.attrv[i].type;
        tuptyp->complex.tuple.attrv[addc + i].defaultp = NULL;
        tuptyp->complex.tuple.attrv[addc + i].options = 0;
    }
    for (i = 0; i < avgc; i++) {
        tuptyp->complex.tuple.attrv[attrc - avgc + i].name = avgv[i];
        tuptyp->complex.tuple.attrv[attrc - avgc + i].type = &RDB_INTEGER;
        tuptyp->complex.tuple.attrv[attrc - avgc + i].defaultp = NULL;
        tuptyp->complex.tuple.attrv[attrc - avgc + i].options = 0;
    }
        
    newtbp->typ = malloc(sizeof (RDB_type));
    if (newtbp->typ == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    newtbp->typ->kind = RDB_TP_RELATION;
    newtbp->typ->complex.basetyp = tuptyp;

    *resultpp = newtbp;
    return RDB_OK;
error:
    if (tuptyp != NULL) {
        free(tuptyp->complex.tuple.attrv);
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
    return res;
}

int
RDB_rename(RDB_table *tbp, int renc, RDB_renaming renv[],
           RDB_table **resultpp)
{
    RDB_table *newtbp;
    int i;
    int res;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->name = NULL;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_SUMMARIZE;
    newtbp->keyc = tbp->keyc;

    /* !! ... */
    
    return RDB_OK; 
}
