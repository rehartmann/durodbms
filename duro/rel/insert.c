/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <string.h>

static int
insert_stored(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_field *fvp;
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int attrcount = tuptyp->var.tuple.attrc;

    fvp = malloc(sizeof(RDB_field) * attrcount);
    if (fvp == NULL) {
        if (txp != NULL) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(RDB_NO_MEMORY));
        }
        return RDB_NO_MEMORY;
    }
    for (i = 0; i < attrcount; i++) {
        int *fnop;
        RDB_object *valp;
        
        fnop = RDB_hashmap_get(&tbp->var.stored.attrmap,
                tuptyp->var.tuple.attrv[i].name, NULL);
        valp = RDB_tuple_get(tplp, tuptyp->var.tuple.attrv[i].name);

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

        /* Set type - needed for non-scalar attributes */
        if (valp->typ == NULL
                && (valp->kind == RDB_OB_TUPLE || valp->kind == RDB_OB_TABLE
                 || valp->kind == RDB_OB_ARRAY))
            _RDB_set_nonsc_type(valp, tuptyp->var.tuple.attrv[i].typ);
            /* !! check object kind against type? */

        _RDB_obj_to_field(&fvp[*fnop], valp);
    }

    ret = RDB_insert_rec(tbp->var.stored.recmapp, fvp,
            tbp->is_persistent ? txp->txid : NULL);
    if (RDB_is_syserr(ret)) {
        if (txp != NULL) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
    } else if (ret == RDB_KEY_VIOLATION) {
        /* check if the tuple is an element of the table */
        if (RDB_contains_rec(tbp->var.stored.recmapp, fvp,
                tbp->is_persistent ? txp->txid : NULL) == RDB_OK)
            ret = RDB_ELEMENT_EXISTS;
    }            
    free(fvp);
    return ret;
}

static int
insert_union(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int ret, ret2;

    /* !! may be very inefficient */        
    if (RDB_table_contains(tbp->var._union.tbp1, tplp, txp) == RDB_OK
            || RDB_table_contains(tbp->var._union.tbp2, tplp, txp) == RDB_OK)
        return RDB_ELEMENT_EXISTS;
     
    /* Try to insert the tuple into both tables. The insertion into the union
     * fails if (1) one of the inserts fails for a reason other than
     * a predicate violation or (2) if both inserts fail because of
     * a predicate violation.
     */
    ret = RDB_insert(tbp->var._union.tbp1, tplp, txp);
    if (ret != RDB_OK) {
        if (ret != RDB_KEY_VIOLATION && ret != RDB_PREDICATE_VIOLATION)
            return ret;
    }
    ret2 = RDB_insert(tbp->var._union.tbp2, tplp, txp);
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

static int
insert_extend(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    int i;

    /* Check the additional attributes */
    for (i = 0; i < tbp->var.extend.attrc; i++) {
        RDB_virtual_attr *vattrp = &tbp->var.extend.attrv[i];
        RDB_object val;
        RDB_object *valp;
        RDB_bool iseq;
        
        valp = RDB_tuple_get(tplp, vattrp->name);
        if (valp == NULL) {
            return RDB_INVALID_ARGUMENT;
        }
        RDB_init_obj(&val);
        ret = RDB_evaluate(vattrp->exp, tplp, txp, &val);
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
     return RDB_insert(tbp->var.extend.tbp, tplp, txp);
}

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

static int
insert_rename(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_invrename_tuple(tup, tbp->var.rename.renc, tbp->var.rename.renv,
            &tpl);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(tbp->var.rename.tbp, &tpl, txp);

cleanup:
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
insert_wrap(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_invwrap_tuple(tup, tbp->var.wrap.wrapc, tbp->var.wrap.wrapv,
            &tpl);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(tbp->var.wrap.tbp, &tpl, txp);

cleanup:
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
insert_unwrap(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_invunwrap_tuple(tup, tbp->var.unwrap.attrc, tbp->var.unwrap.attrv,
            tbp->var.unwrap.tbp->typ->var.basetyp, &tpl);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(tbp->var.unwrap.tbp, &tpl, txp);

cleanup:
    RDB_destroy_obj(&tpl);
    return ret;
}

void
_RDB_set_nonsc_type(RDB_object *objp, RDB_type *typ)
{
    int i;

    objp->typ = typ;

    /* Set tuple/table attribute types */
    if (typ->kind == RDB_TP_TUPLE) {
        for (i = 0; i < typ->var.tuple.attrc; i++) {
            RDB_type *attrtyp = typ->var.tuple.attrv[i].typ;

            if (attrtyp->kind == RDB_TP_TUPLE || attrtyp->kind == RDB_TP_RELATION) {
                _RDB_set_nonsc_type(
                        RDB_tuple_get(objp, typ->var.tuple.attrv[i].name),
                        attrtyp);
            }
        }
    } else {
        for (i = 0; i < typ->var.basetyp->var.tuple.attrc; i++) {
            RDB_type *attrtyp = typ->var.basetyp->var.tuple.attrv[i].typ;

            if (attrtyp->kind == RDB_TP_TUPLE || attrtyp->kind == RDB_TP_RELATION) {
                _RDB_set_nonsc_type(
                        RDB_tuple_get(objp,
                        typ->var.basetyp->var.tuple.attrv[i].name),
                        attrtyp);
            }
        }
    }
}

int
RDB_insert(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            return insert_stored(tbp, tplp, txp);
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            ret = RDB_evaluate_bool(tbp->var.select.exprp, tplp, txp, &b);
            if (ret != RDB_OK)
                return ret;
            if (!b)
                return RDB_PREDICATE_VIOLATION;
            return RDB_insert(tbp->var.select.tbp, tplp, txp);
        case RDB_TB_MINUS:
            return RDB_NOT_SUPPORTED;
        case RDB_TB_UNION:
            return insert_union(tbp, tplp, txp);
        case RDB_TB_INTERSECT:
            return insert_intersect(tbp, tplp, txp);
        case RDB_TB_JOIN:
            return insert_join(tbp, tplp, txp);
        case RDB_TB_EXTEND:
            return insert_extend(tbp, tplp, txp);
        case RDB_TB_PROJECT:
             return RDB_insert(tbp->var.project.tbp, tplp, txp);
        case RDB_TB_SUMMARIZE:
             return RDB_NOT_SUPPORTED;
        case RDB_TB_RENAME:
             return insert_rename(tbp, tplp, txp);
        case RDB_TB_WRAP:
             return insert_wrap(tbp, tplp, txp);
        case RDB_TB_UNWRAP:
             return insert_unwrap(tbp, tplp, txp);
        case RDB_TB_SDIVIDE:
             return RDB_NOT_SUPPORTED;
    }
    /* should never be reached */
    abort();
}
