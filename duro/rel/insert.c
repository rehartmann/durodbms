/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <string.h>

static int
insert_intersect(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
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
            
    ret = RDB_insert(tbp->var.intersect.tbp1, tup, &tx);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret;
    }

    ret2 = RDB_insert(tbp->var.intersect.tbp2, tup, &tx);
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
insert_join(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    RDB_transaction tx;
    int ret, ret2;

    /*
     * Try to insert tup into both tables. If one insert fails,
     * the insert into the join fails.
     * If only one of the inserts fails because the tuple already exists,
     * the overall insert succeeds. 
     */

    /* Start a subtransaction */
    ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK)
        return ret;
            
    ret = RDB_insert(tbp->var.join.tbp1, tup, &tx);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret;
    }

    ret2 = RDB_insert(tbp->var.join.tbp2, tup, &tx);
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

void
_RDB_set_tuple_type(RDB_object *objp, RDB_type *typ)
{
    int i;

    objp->typ = typ;

    /* Set tuple attribute types */
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        RDB_type *attrtyp = typ->var.tuple.attrv[i].typ;

        if (attrtyp->kind == RDB_TP_TUPLE) {
            _RDB_set_tuple_type(
                    RDB_tuple_get(objp, typ->var.tuple.attrv[i].name),
                    attrtyp);
        }
    }
}

int
RDB_insert(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
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
                if (valp->typ != NULL && !RDB_type_equals(valp->typ,
                                     tuptyp->var.tuple.attrv[i].typ)) {
                     return RDB_TYPE_MISMATCH;
                }

                /* Set type - needed for tuples */
                if (valp->typ == NULL && valp->kind == RDB_OB_TUPLE)
                    _RDB_set_tuple_type(valp, tuptyp->var.tuple.attrv[i].typ);

                _RDB_obj_to_field(&fvp[*fnop], valp);
            }
            ret = RDB_insert_rec(tbp->var.stored.recmapp, fvp, txp->txid);
            if (RDB_is_syserr(ret)) {
                RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                RDB_rollback_all(txp);
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
                RDB_object *valp;
                RDB_bool iseq;
                
                valp = RDB_tuple_get(tup, vattrp->name);
                if (valp == NULL) {
                    return RDB_INVALID_ARGUMENT;
                }
                RDB_init_obj(&val);
                ret = RDB_evaluate(vattrp->exp, tup, txp, &val);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&val);
                    return ret;
                }
                iseq = RDB_obj_equals(&val, valp);
                RDB_destroy_obj(&val);
                if (!iseq)
                    return RDB_PREDICATE_VIOLATION;
            }
        
            /*
             * Insert the tuple (the additional attribute(s) don't do any harm)
             */
             return RDB_insert(tbp->var.extend.tbp, tup, txp);
        }
        case RDB_TB_PROJECT:
             return RDB_insert(tbp->var.project.tbp, tup, txp);
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
