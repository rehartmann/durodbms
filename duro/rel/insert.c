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
            _RDB_handle_syserr(txp, ret);
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
insert(RDB_table *, const RDB_object *tplp, RDB_transaction *);

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
    ret = insert(tbp->var._union.tb1p, tplp, txp);
    if (ret != RDB_OK) {
        if (ret != RDB_KEY_VIOLATION && ret != RDB_PREDICATE_VIOLATION)
            return ret;
    }
    ret2 = insert(tbp->var._union.tb2p, tplp, txp);
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
     return insert(tbp->var.extend.tbp, tplp, txp);
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
            
    ret = insert(tbp->var.intersect.tb1p, tup, &tx);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret;
    }

    ret2 = insert(tbp->var.intersect.tb2p, tup, &tx);
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
            
    ret = insert(tbp->var.join.tb1p, tup, &tx);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        RDB_rollback(&tx);
        return ret;
    }

    ret2 = insert(tbp->var.join.tb2p, tup, &tx);
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

    ret = insert(tbp->var.rename.tbp, &tpl, txp);

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

    ret = insert(tbp->var.wrap.tbp, &tpl, txp);

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

    ret = insert(tbp->var.unwrap.tbp, &tpl, txp);

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
            return (_RDB_table_refers(tbp, chtbp)) ? RDB_MAYBE : RDB_OK;
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
    int i;
    RDB_object tpl2;
    RDB_type *tuptyp;

    switch(chtbp->kind) {
        case RDB_TB_REAL:
            if (chtbp == tbp) {
                 ret = RDB_copy_obj(itplp, tplp);
                 if (ret != RDB_OK)
                     return ret;
                 return RDB_TUPLE_INSERTED;
            }
            return _RDB_table_refers(tbp, chtbp) ? RDB_MAYBE : RDB_OK;
        case RDB_TB_PROJECT:
            RDB_init_obj(&tpl2);
            ret = check_insert_inserted(chtbp->var.project.tbp, tbp, tplp,
                    txp, &tpl2);
            switch (ret) {
                case RDB_OK:
                case RDB_MAYBE:
                case RDB_PREDICATE_VIOLATION:
                    break;
                case RDB_TUPLE_INSERTED:
                    /* Copy attributes */
                    tuptyp = chtbp->var.project.tbp->typ->var.basetyp;
                    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
                        ret = RDB_tuple_set(itplp,
                                tuptyp->var.tuple.attrv[i].name,
                                RDB_tuple_get(&tpl2,
                                        tuptyp->var.tuple.attrv[i].name));
                        if (ret != RDB_OK) {
                            RDB_destroy_obj(&tpl2);
                            return ret;
                        }
                    }
                    ret = RDB_TUPLE_INSERTED;
                    break;
                default: ;
            }
            RDB_destroy_obj(&tpl2);
            return ret;                    
        case RDB_TB_RENAME:
        case RDB_TB_EXTEND:
        case RDB_TB_SELECT:
        case RDB_TB_UNION:
        case RDB_TB_INTERSECT:
        case RDB_TB_JOIN:
        case RDB_TB_SUMMARIZE:
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

/*
 * Check if inserting a tuple into tbp deletes a tuple from chtbp.
 * May return RDB_OK or RDB_MAYBE.
 */
static int
check_insert_deleted(RDB_table *chtbp, RDB_table *tbp, RDB_transaction *txp)
{
    switch(chtbp->kind) {
        case RDB_TB_REAL:
            return RDB_OK;
        case RDB_TB_RENAME:
            return check_insert_deleted(chtbp->var.rename.tbp, tbp, txp);
        case RDB_TB_EXTEND:
            return check_insert_deleted(chtbp->var.extend.tbp, tbp, txp);
        case RDB_TB_SELECT:
            return check_insert_deleted(chtbp->var.select.tbp, tbp, txp);
        case RDB_TB_PROJECT:
            return check_insert_deleted(chtbp->var.project.tbp, tbp, txp);
        case RDB_TB_WRAP:
            return check_insert_deleted(chtbp->var.wrap.tbp, tbp, txp);
        case RDB_TB_UNWRAP:
            return check_insert_deleted(chtbp->var.unwrap.tbp, tbp, txp);
        case RDB_TB_GROUP:
            return check_insert_deleted(chtbp->var.group.tbp, tbp, txp);
        case RDB_TB_UNGROUP:
            return check_insert_deleted(chtbp->var.ungroup.tbp, tbp, txp);
        case RDB_TB_UNION:
        case RDB_TB_INTERSECT:
        case RDB_TB_JOIN:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_MINUS:
        case RDB_TB_SDIVIDE:
            return RDB_MAYBE;
    }
    abort();
}

static int
check_insert_empty_minus(RDB_table *chtbp, RDB_table *tbp,
        const RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl1;
    RDB_object tpl2;
    RDB_bool iseq;

    /*
     * Check if the insert may delete tuples from table #2
     */
    ret = check_insert_deleted(chtbp->var.minus.tb2p, tbp, txp);
    if (ret == RDB_PREDICATE_VIOLATION || ret == RDB_MAYBE)
        return RDB_MAYBE;
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl1);
    ret = check_insert_inserted(chtbp->var.minus.tb1p, tbp, tplp, txp, &tpl1);
    switch (ret) {
        case RDB_OK:
        case RDB_MAYBE:
            break;
        case RDB_PREDICATE_VIOLATION:
            /*
             * Tuples will be inserted into table #1, but is is not known
             * which ones. The may have been inserted into table #2 too.
             */
            ret = RDB_MAYBE;
            break;
        case RDB_TUPLE_INSERTED:
            /*
             * Tuple tpl1 will be inserted into table #1.
             * Check if it is already in table #2.
             */
             ret = RDB_table_contains(chtbp->var.minus.tb2p, &tpl1, txp);
             if (ret == RDB_OK || ret != RDB_NOT_FOUND)
                 break;
            /*
             * Check if it will be inserted into table #2 too.
             */
            RDB_init_obj(&tpl2);
            ret = check_insert_inserted(chtbp->var.minus.tb2p, tbp, tplp,
                    txp, &tpl2);
            switch (ret) {
                case RDB_OK:
                    ret = RDB_PREDICATE_VIOLATION;
                    break;
                case RDB_MAYBE:
                case RDB_PREDICATE_VIOLATION:
                    ret = RDB_MAYBE;
                    break;
                case RDB_TUPLE_INSERTED:
                    /* Compare tuples */
                    ret = RDB_obj_equals(&tpl1, &tpl2, txp, &iseq);
                    if (ret != RDB_OK)
                        break;
                    ret = iseq ? RDB_OK : RDB_PREDICATE_VIOLATION;
                    break;
                default: ;
            }
            RDB_destroy_obj(&tpl2);
            break;
        default: ;
    }
    RDB_destroy_obj(&tpl1);
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
        case RDB_TB_RENAME:
            return check_insert_empty(chtbp->var.rename.tbp, tbp, tplp, txp);
        case RDB_TB_EXTEND:
            return check_insert_empty(chtbp->var.extend.tbp, tbp, tplp, txp);
        case RDB_TB_MINUS:
            return check_insert_empty_minus(chtbp, tbp, tplp, txp);
        case RDB_TB_WRAP:
            return check_insert_empty(chtbp->var.wrap.tbp, tbp, tplp, txp);
        case RDB_TB_UNWRAP:
            return check_insert_empty(chtbp->var.unwrap.tbp, tbp, tplp, txp);
        case RDB_TB_GROUP:
            return check_insert_empty(chtbp->var.group.tbp, tbp, tplp, txp);
        case RDB_TB_UNGROUP:
            return check_insert_empty(chtbp->var.ungroup.tbp, tbp, tplp, txp);
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

static int
insert(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

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
            ret = insert(tbp->var.select.tbp, tplp, txp);
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
            ret = insert(tbp->var.project.tbp, tplp, txp);
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
    return ret;
}

int
RDB_insert(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    RDB_dbroot *dbrootp;
    RDB_constraint *constrp;
    RDB_transaction tx;
    RDB_constraint *checklistp = NULL;
    RDB_bool need_subtx = RDB_FALSE;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    if (txp != NULL) {
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
                ret = check_insert_empty(constrp->empty_tbp, tbp, tplp, txp);
                if (ret != RDB_OK && ret != RDB_MAYBE)
                    goto cleanup;
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
    }

    ret = insert(tbp, tplp, txp);
    if (ret != RDB_OK) {
        if (need_subtx)
            RDB_rollback(&tx);
        goto cleanup;
    }

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
    return ret;
}
