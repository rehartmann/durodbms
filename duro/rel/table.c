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

RDB_type *
RDB_table_type(const RDB_table *tbp)
{
    return tbp->typ;
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
RDB_expression *
_RDB_pindex_expr(RDB_table *tbp, RDB_expression *exprp)
{
    if (tbp->kind != RDB_TB_STORED || _RDB_pkey_len(tbp) != 1
            || exprp->kind != RDB_EX_EQ)
        return NULL;
    if (exprp->var.op.arg1->kind == RDB_EX_ATTR
            && RDB_expr_is_const(exprp->var.op.arg2)
            && attr_is_pindex(tbp, exprp->var.op.arg1->var.attr.name)) {
        return exprp->var.op.arg2;
    }
    if (exprp->var.op.arg2->kind == RDB_EX_ATTR
            && RDB_expr_is_const(exprp->var.op.arg1)
            && attr_is_pindex(tbp, exprp->var.op.arg2->var.attr.name)) {
        return exprp->var.op.arg1;
    }
    return NULL;
}

int
_RDB_move_tuples(RDB_table *dstp, RDB_table *srcp, RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    RDB_object tpl;
    int ret;

    /* delete all tuples from destination table */
    ret = RDB_delete(dstp, NULL, txp);
    if (ret != RDB_OK)
        return ret;

    /* copy all tuples from source table to destination table */
    ret = _RDB_table_qresult(srcp, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);

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
    RDB_destroy_obj(&tpl);
    return ret;
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
    RDB_object tpl;
    void *datap;
    size_t len;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    RDB_expression *exprp;
    RDB_bool b;

    /* Check if the primary index can be used */
    exprp = condp != NULL ? _RDB_pindex_expr(tbp, condp) : NULL;
    if (exprp != NULL) {
        RDB_field fv;
        RDB_object val;

        ret = RDB_evaluate(exprp, NULL, txp, &val);
        if (ret != RDB_OK)
            return ret;
        _RDB_obj_to_field(&fv, &val);
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
        RDB_init_obj(&tpl);
        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp, i, &datap, &len);
            if (ret != 0) {
               RDB_destroy_obj(&tpl);
               goto error;
            }
            RDB_init_obj(&val);
            ret = RDB_irep_to_obj(&val, tpltyp->var.tuple.attrv[i].typ,
                             datap, len);
            if (ret != RDB_OK) {
               RDB_destroy_obj(&tpl);
               goto error;
            }
            ret = RDB_tuple_set(&tpl, tpltyp->var.tuple.attrv[i].name, &val);
            if (ret != RDB_OK) {
               RDB_destroy_obj(&tpl);
               goto error;
            }
        }
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            ret = RDB_evaluate_bool(condp, &tpl, txp, &b);
            if (ret != RDB_OK)
                goto error;
        }
        if (b) {
            ret = RDB_cursor_delete(curp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                goto error;
            }
        }
        RDB_destroy_obj(&tpl);
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
                RDB_rollback_all(txp);
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
            return RDB_delete(tbp->var.project.tbp, condp, txp);
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

    ret = _RDB_move_tuples(dstp, srcp, &tx);
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
RDB_all(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    /* initialize result */
    *resultp = RDB_TRUE;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (!RDB_tuple_get_bool(&tpl, attrname))
            *resultp = RDB_FALSE;
    }

    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_any(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    /* initialize result */
    *resultp = RDB_FALSE;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (RDB_tuple_get_bool(&tpl, attrname))
            *resultp = RDB_TRUE;
    }

    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_max(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = RDB_INT_MIN;
    else if (attrtyp == &RDB_RATIONAL)
        resultp->var.rational_val = RDB_RATIONAL_MIN;
    else
        return RDB_TYPE_MISMATCH;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(&tpl, attrname);
             
            if (val > resultp->var.int_val)
                 resultp->var.int_val = val;
        } else {
            RDB_rational val = RDB_tuple_get_rational(&tpl, attrname);
             
            if (val > resultp->var.rational_val)
                resultp->var.rational_val = val;
        }
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_min(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = RDB_INT_MAX;
    else if (attrtyp == &RDB_RATIONAL)
        resultp->var.rational_val = RDB_RATIONAL_MAX;
    else
        return RDB_TYPE_MISMATCH;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(&tpl, attrname);
             
            if (val < resultp->var.int_val)
                 resultp->var.int_val = val;
        } else {
            RDB_rational val = RDB_tuple_get_rational(&tpl, attrname);
             
            if (val < resultp->var.rational_val)
                resultp->var.rational_val = val;
        }
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_sum(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    /* initialize result */
    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = 0;
    else if (attrtyp == &RDB_RATIONAL)
        resultp->var.rational_val = 0.0;
    else
       return RDB_TYPE_MISMATCH;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (attrtyp == &RDB_INTEGER)
            resultp->var.int_val += RDB_tuple_get_int(&tpl, attrname);
        else
            resultp->var.rational_val
                            += RDB_tuple_get_rational(&tpl, attrname);
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_avg(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_rational *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;
    int count;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    if (!RDB_type_is_numeric(attrtyp))
        return RDB_TYPE_MISMATCH;
    count = 0;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        count++;
        if (attrtyp == &RDB_INTEGER)
            *resultp += RDB_tuple_get_int(&tpl, attrname);
        else
            *resultp += RDB_tuple_get_rational(&tpl, attrname);
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    if (count == 0)
        return RDB_AGGREGATE_UNDEFINED;
    *resultp /= count;

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_extract_tuple(RDB_table *tbp, RDB_object *tup, RDB_transaction *txp)
{
    int ret, ret2;
    RDB_qresult *qrp;
    RDB_object tpl;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            RDB_rollback_all(txp);
        }
        return ret;
    }

    /* Get tuple */
    ret = _RDB_next_tuple(qrp, tup, txp);
    if (ret != RDB_OK)
        goto error;

    /* Check if there are more tuples */
    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp, &tpl, txp);
    RDB_destroy_obj(&tpl);
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
        RDB_rollback_all(txp);
    }
    return ret;
}

int
RDB_table_is_empty(RDB_table *tbp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);

    ret = _RDB_next_tuple(qrp, &tpl, txp);
    if (ret == RDB_OK)
        *resultp = RDB_FALSE;
    else if (ret == RDB_NOT_FOUND)
        *resultp = RDB_TRUE;
    else {
         RDB_destroy_obj(&tpl);
        _RDB_drop_qresult(qrp, txp);
        return ret;
    }
    RDB_destroy_obj(&tpl);
    return _RDB_drop_qresult(qrp, txp);
}

int
RDB_cardinality(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    int count;
    RDB_qresult *qrp;
    RDB_object tpl;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);

    count = 0;
    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        count++;
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        return ret;
    }

    ret = _RDB_drop_qresult(qrp, txp);
    if (ret != RDB_OK)
        return ret;
    return count;
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
    if (condp->kind == RDB_EX_EQ) {
        RDB_expression *exprp = _RDB_pindex_expr(tbp, condp);

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

    /*
     * Set keys. The result table becomes all-key.
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
        attrdefv[i].typ = RDB_expr_type(attrv[i].exp, tbp->typ->var.basetyp);
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
        free(newtbp->keyv);
    }
    RDB_drop_type(newtbp->typ, NULL);
    free(newtbp);

    return RDB_NO_MEMORY;
}

int
RDB_remove(RDB_table *tbp, int attrc, char *attrv[], RDB_table **resultpp)
{
    int ret;
    int i, j;
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int baseattrc = tuptyp->var.tuple.attrc;
    char **resattrv;

    if (attrc > baseattrc)
        return RDB_INVALID_ARGUMENT;

    /* Allocate vector of remaining attributes */
    resattrv = malloc((baseattrc - attrc) * sizeof (char *));
    if (resattrv == NULL)
        return RDB_NO_MEMORY;

    /* Get the table attributes which are not in attrv */
    for (i = 0, j = 0; i < baseattrc && j < baseattrc - attrc; i++) {
        if (RDB_find_str(attrc, attrv, tuptyp->var.tuple.attrv[i].name) == -1) {
            if (j == baseattrc - attrc) {
                /* Not-existing attribute in attrv */
                ret = RDB_INVALID_ARGUMENT;
                goto cleanup;
            }                
            resattrv[j++] = tuptyp->var.tuple.attrv[i].name;
        }
    }

    ret = RDB_project(tbp, baseattrc - attrc, resattrv, resultpp);

cleanup:
    free(resattrv);
    return ret;
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
                : RDB_expr_type(addv[i].exp, tb1p->typ->var.basetyp);

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

int
RDB_wrap(RDB_table *tbp, int wrapc, RDB_wrapping wrapv[],
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
    newtbp->kind = RDB_TB_WRAP;

    newtbp->keyc = 1;
    newtbp->keyv = all_key(tbp);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }

    ret = RDB_wrap_relation_type(tbp->typ, wrapc, wrapv, &newtbp->typ);
    if (ret != RDB_OK) {
        free(newtbp);
        return ret;
    }

    newtbp->var.wrap.wrapc = wrapc;
    newtbp->var.wrap.wrapv = malloc(sizeof (RDB_wrapping) * wrapc);
    if (newtbp->var.wrap.wrapv == NULL) {
        RDB_drop_type(newtbp->typ, NULL);
        free(newtbp);
        return RDB_NO_MEMORY;
    }
    for (i = 0; i < wrapc; i++) {
        newtbp->var.wrap.wrapv[i].attrname = NULL;
        newtbp->var.wrap.wrapv[i].attrv = NULL;
    }
    for (i = 0; i < wrapc; i++) {
        newtbp->var.wrap.wrapv[i].attrname = RDB_dup_str(wrapv[i].attrname);
        if (newtbp->var.wrap.wrapv[i].attrname == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        newtbp->var.wrap.wrapv[i].attrc = wrapv[i].attrc;
        newtbp->var.wrap.wrapv[i].attrv = RDB_dup_strvec(wrapv[i].attrc,
                wrapv[i].attrv);
        if (newtbp->var.wrap.wrapv[i].attrv == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }
    newtbp->var.wrap.tbp = tbp;

    *resultpp = newtbp;
    return RDB_OK; 

error:
    for (i = 0; i < newtbp->keyc; i++)
        RDB_free_strvec(newtbp->keyv[i].strc, newtbp->keyv[i].strv);
    free(newtbp->keyv);
    RDB_drop_type(newtbp->typ, NULL);
    for (i = 0; i < wrapc; i++) {
        free(newtbp->var.wrap.wrapv[i].attrname);
        if (newtbp->var.wrap.wrapv[i].attrv != NULL)
            RDB_free_strvec(newtbp->var.wrap.wrapv[i].attrc,
                    newtbp->var.wrap.wrapv[i].attrv);
    }
    free(newtbp->var.wrap.wrapv);
    free(newtbp);
    return ret;
}

int
RDB_unwrap(RDB_table *tbp, int attrc, char *attrv[],
        RDB_table **resultpp)
{
    RDB_table *newtbp;
    int ret;
    int i;

    newtbp = malloc(sizeof (RDB_table));
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->name = NULL;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_UNWRAP;
    newtbp->typ = NULL;

    newtbp->keyc = 1;
    newtbp->keyv = all_key(tbp);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }

    ret = RDB_unwrap_relation_type(tbp->typ, attrc, attrv, &newtbp->typ);
    if (ret != RDB_OK) {
        goto error;
    }

    newtbp->var.unwrap.attrc = attrc;
    newtbp->var.unwrap.attrv = RDB_dup_strvec(attrc, attrv);
    if (newtbp->var.unwrap.attrv == NULL) {    
        RDB_drop_type(newtbp->typ, NULL);
        free(newtbp);
        ret = RDB_NO_MEMORY;
        goto error;
    }
    newtbp->var.unwrap.tbp = tbp;

    *resultpp = newtbp;
    return RDB_OK;

error:
    for (i = 0; i < newtbp->keyc; i++)
        RDB_free_strvec(newtbp->keyv[i].strc, newtbp->keyv[i].strv);
    free(newtbp->keyv);
    if (newtbp->typ != NULL)
        RDB_drop_type(newtbp->typ, NULL);
    free(newtbp);
    return ret;
}

int
RDB_sdivide(RDB_table *tb1p, RDB_table *tb2p, RDB_table *tb3p,
        RDB_table **resultpp)
{
    int ret;
    RDB_type *typ;
    RDB_table *newtbp;

    /*
     * Table 1 JOIN table 2 must be of same type as table 3
     */
    ret = RDB_join_relation_types(tb1p->typ, tb2p->typ, &typ);
    if (ret != RDB_OK) {
        return ret;
    }

    if (!RDB_type_equals(typ, tb3p->typ)) {
        RDB_drop_type(typ, NULL);
        return RDB_INVALID_ARGUMENT;
    }
    RDB_drop_type(typ, NULL);
    
    newtbp = malloc(sizeof (RDB_table));    
    if (newtbp == NULL)
        return RDB_NO_MEMORY;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.sdivide.tb1p = tb1p;
    newtbp->var.sdivide.tb2p = tb2p;
    newtbp->var.sdivide.tb3p = tb3p;
    newtbp->typ = tb1p->typ;
    newtbp->name = NULL;
    newtbp->kind = RDB_TB_SDIVIDE;

    newtbp->keyc = tb1p->keyc;
    newtbp->keyv = dup_keys(tb1p->keyc, tb1p->keyv);
    if (newtbp->keyv == NULL) {
        free(newtbp);
        return RDB_NO_MEMORY;
    }

    *resultpp = newtbp;

    return RDB_OK;
}
