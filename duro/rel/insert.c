/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <string.h>

int
_RDB_insert_real(RDB_table *tbp, const RDB_object *tplp,
                   RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_field *fvp;
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int attrcount = tuptyp->var.tuple.attrc;

    fvp = malloc(sizeof(RDB_field) * attrcount);
    if (fvp == NULL) {
        if (txp != NULL) {
            RDB_rollback_all(txp);
        }
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    for (i = 0; i < attrcount; i++) {
        int *fnop;
        RDB_object *valp;
        
        fnop = RDB_hashmap_get(&tbp->stp->attrmap,
                tuptyp->var.tuple.attrv[i].name, NULL);
        valp = RDB_tuple_get(tplp, tuptyp->var.tuple.attrv[i].name);

        /* If there is no value, check if there is a default */
        if (valp == NULL) {
            valp = tuptyp->var.tuple.attrv[i].defaultp;
            if (valp == NULL) {
                ret = RDB_INVALID_ARGUMENT;
                goto cleanup;
            }
        }
        
        /* Typecheck */
        if (valp->typ == NULL) {
            RDB_type *attrtyp = tuptyp->var.tuple.attrv[i].typ;

            switch (valp->kind) {
                case RDB_OB_BOOL:
                case RDB_OB_INT:
                case RDB_OB_RATIONAL:
                case RDB_OB_BIN:
                    ret = RDB_INTERNAL;
                    goto cleanup;
                case RDB_OB_INITIAL:
                    if (!RDB_type_is_scalar(attrtyp)) {
                        ret = RDB_TYPE_MISMATCH;
                        goto cleanup;
                    }
                    break;
                case RDB_OB_TUPLE:
                    if (attrtyp->kind != RDB_TP_TUPLE) {
                        ret = RDB_TYPE_MISMATCH;
                        goto cleanup;
                    }
                    break;
                case RDB_OB_TABLE:
                    if (attrtyp->kind != RDB_TP_RELATION) {
                        ret = RDB_TYPE_MISMATCH;
                        goto cleanup;
                     }
                     break;
                case RDB_OB_ARRAY:
                    if (attrtyp->kind != RDB_TP_ARRAY) {
                        ret = RDB_TYPE_MISMATCH;
                        goto cleanup;
                    }
                    break;
            }
        } else {
            if (!RDB_type_equals(valp->typ, tuptyp->var.tuple.attrv[i].typ))
                return RDB_TYPE_MISMATCH;
        }

        /* Set type - needed for tuple and array attributes */
        if (valp->typ == NULL
                && (valp->kind == RDB_OB_TUPLE
                 || valp->kind == RDB_OB_ARRAY))
            valp->typ =  tuptyp->var.tuple.attrv[i].typ;
            /* !! check object kind against type? */

        ret = _RDB_obj_to_field(&fvp[*fnop], valp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_insert_rec(tbp->stp->recmapp, fvp,
            tbp->is_persistent ? txp->txid : NULL);
    if (RDB_is_syserr(ret)) {
        if (txp != NULL) {
            RDB_errmsg(txp->dbp->dbrootp->envp, "cannot insert record: %s",
                    RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
    } else if (ret == RDB_KEY_VIOLATION) {
        /* check if the tuple is an element of the table */
        if (RDB_contains_rec(tbp->stp->recmapp, fvp,
                tbp->is_persistent ? txp->txid : NULL) == RDB_OK)
            ret = RDB_ELEMENT_EXISTS;
    }            

cleanup:
    free(fvp);
    return ret;
}

static int
insert_union(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int ret, ret2;

    /* !! may be very inefficient */        
    if (RDB_table_contains(tbp->var._union.tb1p, tplp, txp) == RDB_OK
            || RDB_table_contains(tbp->var._union.tb2p, tplp, txp) == RDB_OK)
        return RDB_ELEMENT_EXISTS;
     
    /* Try to insert the tuple into both tables. The insertion into the union
     * fails if (1) one of the inserts fails for a reason other than
     * a predicate violation or (2) if both inserts fail because of
     * a predicate violation.
     */
    ret = RDB_insert(tbp->var._union.tb1p, tplp, txp);
    if (ret != RDB_OK) {
        if (ret != RDB_KEY_VIOLATION && ret != RDB_PREDICATE_VIOLATION)
            return ret;
    }
    ret2 = RDB_insert(tbp->var._union.tb2p, tplp, txp);
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
        ret = RDB_obj_equals(&val, valp, txp, &iseq);
        RDB_destroy_obj(&val);
        if (ret != RDB_OK)
            return ret;
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
            
    ret = RDB_insert(tbp->var.intersect.tb1p, tup, &tx);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret;
    }

    ret2 = RDB_insert(tbp->var.intersect.tb2p, tup, &tx);
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
            
    ret = RDB_insert(tbp->var.join.tb1p, tup, &tx);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret;
    }

    ret2 = RDB_insert(tbp->var.join.tb2p, tup, &tx);
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

static int
check_insert_empty_real(RDB_table *chtbp, RDB_table *tbp, const RDB_object *tplp,
        RDB_transaction *txp)
{
    switch (tbp->kind) {
        case RDB_TB_REAL:
            /* since chtbp != tbp */
            return RDB_OK;
        case RDB_TB_SELECT:
        case RDB_TB_UNION:
        case RDB_TB_INTERSECT:
        case RDB_TB_JOIN:
        case RDB_TB_EXTEND:
        case RDB_TB_PROJECT:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_RENAME:
        case RDB_TB_WRAP:
        case RDB_TB_UNWRAP:
        case RDB_TB_MINUS:
        case RDB_TB_GROUP:
        case RDB_TB_UNGROUP:
        case RDB_TB_SDIVIDE:
            return (RDB_table_refers(tbp, chtbp)) ? RDB_MAYBE : RDB_OK;
    }
    return RDB_OK;
}

/*
 * Check if inserting tplp into tbp will result in an insertion into chtbp.
 * Returning RDB_INSERT_TUPLE indicated that only itplp is inserted into chtbp.
 */
static int
check_insert_inserted(RDB_table *chtbp, RDB_table *tbp, const RDB_object *tplp,
        RDB_transaction *txp, RDB_object *itplp)
{
    int ret;

    switch(chtbp->kind) {
        case RDB_TB_REAL:
             if (chtbp == tbp) {
                 ret = RDB_copy_obj(itplp, tplp);
                 if (ret != RDB_OK)
                     return ret;
                 return RDB_TUPLE_INSERTED;
            }
            return RDB_table_refers(tbp, chtbp) ? RDB_MAYBE : RDB_OK;
        case RDB_TB_SELECT:
        case RDB_TB_UNION:
        case RDB_TB_INTERSECT:
        case RDB_TB_JOIN:
        case RDB_TB_EXTEND:
        case RDB_TB_PROJECT:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_RENAME:
        case RDB_TB_WRAP:
        case RDB_TB_UNWRAP:
        case RDB_TB_MINUS:
        case RDB_TB_GROUP:
        case RDB_TB_UNGROUP:
        case RDB_TB_SDIVIDE:
            return RDB_MAYBE;
    }
    abort();
}

static int
check_insert_empty_select(RDB_table *chtbp, RDB_table *tbp, const RDB_object *tplp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_bool b;

    RDB_init_obj(&tpl);
    ret = check_insert_inserted(chtbp->var.select.tbp, tbp, tplp, txp, &tpl);
    switch (ret) {
        case RDB_OK:
        case RDB_MAYBE:
            break;
        case RDB_PREDICATE_VIOLATION:
            /*
             * Tuple will be inserted into chtbp->var.select.tbp, but we can't
             * tell if it appears in chtbp too
             */
            ret = RDB_MAYBE;
            break;
        case RDB_TUPLE_INSERTED:
            /*
             * Tuple will be inserted into chtbp - check if it will appear
             * in chtbp->var.select.tbp
             */
            ret = RDB_evaluate_bool(chtbp->var.select.exp, &tpl, txp, &b);
            if (ret == RDB_OK) {
                ret = b ? RDB_PREDICATE_VIOLATION : RDB_OK;
            }
            break;
        default: ;
    }
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
check_insert_empty(RDB_table *chtbp, RDB_table *tbp, const RDB_object *tplp,
        RDB_transaction *txp)
{
    if (chtbp == tbp)
        return RDB_PREDICATE_VIOLATION;
    switch (chtbp->kind) {
        case RDB_TB_REAL:
           return check_insert_empty_real(chtbp, tbp, tplp, txp);
        case RDB_TB_SELECT:
           return check_insert_empty_select(chtbp, tbp, tplp, txp);
        case RDB_TB_UNION:
        case RDB_TB_INTERSECT:
        case RDB_TB_JOIN:
        case RDB_TB_EXTEND:
        case RDB_TB_PROJECT:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_RENAME:
        case RDB_TB_WRAP:
        case RDB_TB_UNWRAP:
        case RDB_TB_MINUS:
        case RDB_TB_GROUP:
        case RDB_TB_UNGROUP:
        case RDB_TB_SDIVIDE:
            return RDB_MAYBE;
    }
    abort();
}

int
RDB_insert(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_bool b;
    RDB_dbroot *dbrootp;
    RDB_constraint *constrp;
    RDB_transaction tx;
    int ccount = 0;
    RDB_bool *checkv = NULL;
    RDB_bool need_subtx = RDB_FALSE;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    if (txp != NULL) {
        dbrootp = RDB_tx_db(txp)->dbrootp;

        ccount = _RDB_constraint_count(dbrootp);
        if (ccount > 0) {
            checkv = malloc(ccount * sizeof (RDB_bool));
            if (checkv == NULL)
                return RDB_NO_MEMORY;
        }

        if (!dbrootp->constraints_read) {
            ret = _RDB_read_constraints(txp);
            if (ret != RDB_OK) {
                free(checkv);
                return ret;
            }
            dbrootp->constraints_read = RDB_TRUE;
        }

        /*
         * Identify the constraints which must be checked
         */
        constrp = dbrootp->first_constrp;
        for (i = 0; i < ccount; i++) {
            if (constrp->empty_tbp != NULL) {
                ret = check_insert_empty(constrp->empty_tbp, tbp, tplp, txp);
                if (ret == RDB_PREDICATE_VIOLATION) {
                    free(checkv);
                    return RDB_PREDICATE_VIOLATION;
                }
                checkv[i] = (RDB_bool) (ret == RDB_MAYBE);
            } else {
                checkv[i] = RDB_TRUE;
            }
            if (checkv[i])
                need_subtx = RDB_TRUE;
            constrp = constrp->nextp;
        }

        if (need_subtx) {
            ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
            if (ret != RDB_OK) {
                free(checkv);
                return ret;
            }
            txp = &tx;
        }
    }

    /*
     * Execute insert
     */
    switch (tbp->kind) {
        case RDB_TB_REAL:
            ret = _RDB_insert_real(tbp, tplp, txp);
            break;
        case RDB_TB_SELECT:
            ret = RDB_evaluate_bool(tbp->var.select.exp, tplp, txp, &b);
            if (ret != RDB_OK)
                break;
            if (!b) {
                ret = RDB_PREDICATE_VIOLATION;
                break;
            }
            ret = RDB_insert(tbp->var.select.tbp, tplp, txp);
            break;
        case RDB_TB_MINUS:
            ret = RDB_NOT_SUPPORTED;
            break;
        case RDB_TB_UNION:
            ret = insert_union(tbp, tplp, txp);
            break;
        case RDB_TB_INTERSECT:
            ret = insert_intersect(tbp, tplp, txp);
            break;
        case RDB_TB_JOIN:
            ret = insert_join(tbp, tplp, txp);
            break;
        case RDB_TB_EXTEND:
            ret = insert_extend(tbp, tplp, txp);
            break;
        case RDB_TB_PROJECT:
            ret = RDB_insert(tbp->var.project.tbp, tplp, txp);
            break;
        case RDB_TB_SUMMARIZE:
            ret = RDB_NOT_SUPPORTED;
            break;
        case RDB_TB_RENAME:
            ret = insert_rename(tbp, tplp, txp);
            break;
        case RDB_TB_WRAP:
            ret = insert_wrap(tbp, tplp, txp);
            break;
        case RDB_TB_UNWRAP:
            ret = insert_unwrap(tbp, tplp, txp);
            break;
        case RDB_TB_GROUP:
        case RDB_TB_UNGROUP:
        case RDB_TB_SDIVIDE:
            ret = RDB_NOT_SUPPORTED;
            break;
    }
    if (ret != RDB_OK) {
        free(checkv);
        if (need_subtx)
            RDB_rollback(&tx);
        return ret;
    }

    /*
     * Check constraints, if necessary
     */
    if (txp != NULL) {
        constrp = dbrootp->first_constrp;
        for (i = 0; i < ccount; i++) {
            if (checkv[i]) {
                ret = RDB_evaluate_bool(constrp->exp, NULL, txp, &b);
                if (ret != RDB_OK) {
                    free(checkv);
                    if (need_subtx)
                        RDB_rollback(&tx);
                    return ret;
                }
                if (!b) {
                    free(checkv);
                    if (need_subtx)
                        RDB_rollback(&tx);
                    return RDB_PREDICATE_VIOLATION;
                }
            }
            constrp = constrp->nextp;
        }
    }
    free(checkv);
    if (need_subtx)
        return RDB_commit(&tx);
    return RDB_OK;
}
