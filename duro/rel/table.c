/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

RDB_table *
_RDB_new_table(void)
{
    RDB_table *tbp = malloc(sizeof (RDB_table));
    if (tbp == NULL) {
        return NULL;
    }
    tbp->name = NULL;
    tbp->refcount = 0;
    tbp->optimized = RDB_FALSE;
    return tbp;
}

/*
 * Creates a stored table, but not the recmap and the indexes
 * and does not insert the table into the catalog.
 * reltyp is consumed on success (must not be freed by caller).
 */
int
_RDB_new_stored_table(const char *name, RDB_bool persistent,
                RDB_type *reltyp,
                int keyc, RDB_string_vec keyv[], RDB_bool usr,
                RDB_table **tbpp)
{
    int ret, i;
    RDB_table *tbp = _RDB_new_table();

    if (tbp == NULL)
        return RDB_NO_MEMORY;
    *tbpp = tbp;
    tbp->is_user = usr;
    tbp->is_persistent = persistent;
    tbp->keyv = NULL;

    RDB_init_hashmap(&tbp->var.stored.attrmap, RDB_DFL_MAP_CAPACITY);

    tbp->kind = RDB_TB_STORED;
    if (name != NULL) {
        tbp->name = RDB_dup_str(name);
        if (tbp->name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }

    /* copy candidate keys */
    tbp->keyc = keyc;
    tbp->keyv = malloc(sizeof(RDB_attr) * keyc);
    for (i = 0; i < keyc; i++) {
        tbp->keyv[i].strv = NULL;
    }
    for (i = 0; i < keyc; i++) {
        tbp->keyv[i].strc = keyv[i].strc;
        tbp->keyv[i].strv = RDB_dup_strvec(keyv[i].strc, keyv[i].strv);
        if (tbp->keyv[i].strv == NULL)
            goto error;
    }
    tbp->var.stored.indexc = 0;

    tbp->typ = reltyp;

    return RDB_OK;

error:
    /* clean up */
    if (tbp != NULL) {
        free(tbp->name);
        for (i = 0; i < keyc; i++) {
            if (tbp->keyv[i].strv != NULL) {
                RDB_free_strvec(tbp->keyv[i].strc, tbp->keyv[i].strv);
            }
        }
        free(tbp->keyv);
        RDB_destroy_hashmap(&tbp->var.stored.attrmap);
        free(tbp);
    }
    return ret;
}

RDB_type *
RDB_table_type(const RDB_table *tbp)
{
    return tbp->typ;
}

int
_RDB_move_tuples(RDB_table *dstp, RDB_table *srcp, RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    RDB_object tpl;
    int ret;

    /*
     * Copy all tuples from source table to destination table
     */
    ret = _RDB_table_qresult(srcp, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (dstp->kind == RDB_TB_STORED && !dstp->is_persistent)
            ret = RDB_insert(dstp, &tpl, NULL);
        else
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

    /* Delete all tuples from destination table */
    ret = RDB_delete(dstp, NULL, &tx);
    if (ret != RDB_OK)
        goto error;

    ret = _RDB_move_tuples(dstp, srcp, &tx);
    if (ret != RDB_OK)
        goto error;

    return RDB_commit(&tx);

error:
    RDB_rollback(&tx);
    return ret;
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
RDB_extract_tuple(RDB_table *tbp, RDB_transaction *txp, RDB_object *tplp)
{
    int ret, ret2;
    RDB_qresult *qrp;
    RDB_object tpl;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    RDB_init_obj(&tpl);

    /* Get tuple */
    ret = _RDB_next_tuple(qrp, tplp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Check if there are more tuples */
    ret = _RDB_next_tuple(qrp, &tpl, txp);
    if (ret != RDB_NOT_FOUND) {
        if (ret == RDB_OK)
            ret = RDB_INVALID_ARGUMENT;
        goto cleanup;
    }

    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);

    ret2 = _RDB_drop_qresult(qrp, txp);
    if (ret == RDB_OK)
        ret = ret2;
    if (RDB_is_syserr(ret) || RDB_is_syserr(ret2)) {
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

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    RDB_init_obj(&tpl);

    ret = _RDB_next_tuple(qrp, &tpl, txp);
    if (ret == RDB_OK)
        *resultp = RDB_FALSE;
    else if (ret == RDB_NOT_FOUND)
        *resultp = RDB_TRUE;
    else {
         RDB_destroy_obj(&tpl);
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
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

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    RDB_init_obj(&tpl);

    count = 0;
    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        count++;
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        goto error;
    }

    ret = _RDB_drop_qresult(qrp, txp);
    if (ret != RDB_OK)
        goto error;

    if (tbp->kind == RDB_TB_STORED)
        tbp->var.stored.est_cardinality = count;

    return count;

error:
    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}

int
RDB_subset(RDB_table *tb1p, RDB_table *tb2p, RDB_transaction *txp,
           RDB_bool *resultp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    if (!RDB_type_equals(tb1p->typ, tb2p->typ))
        return RDB_TYPE_MISMATCH;

    ret = _RDB_table_qresult(tb1p, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    RDB_init_obj(&tpl);

    *resultp = RDB_TRUE;
    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, txp);
        if (ret == RDB_NOT_FOUND) {
            *resultp = RDB_FALSE;
            break;
        }
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret)) {
                RDB_rollback_all(txp);
            }
            RDB_destroy_obj(&tpl);
            _RDB_drop_qresult(qrp, txp);
            goto error;
        }
    }

    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND && ret != RDB_OK) {
        _RDB_drop_qresult(qrp, txp);
        goto error;
    }
    ret = _RDB_drop_qresult(qrp, txp);
    if (ret != RDB_OK)
        goto error;
    return RDB_OK;

error:
    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}

int
RDB_table_equals(RDB_table *tb1p, RDB_table *tb2p, RDB_transaction *txp,
        RDB_bool *resp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;
    int cnt = RDB_cardinality(tb1p, txp);

    /*
     * Check if both tables have same cardinality
     */
    if (cnt < 0)
        return cnt;
    ret =  RDB_cardinality(tb2p, txp);
    if (ret < 0)
        return ret;
    if (ret != cnt) {
        *resp = RDB_FALSE;
        return RDB_OK;
    }

    /*
     * Check if all tuples from table #1 are in table #2
     * (The implementation is quite inefficient if table #2
     * is a SUMMARIZE PER or GROUP table)
     */
    ret = _RDB_table_qresult(tb1p, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);
    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, txp);
        if (ret == RDB_NOT_FOUND) {
            *resp = RDB_FALSE;
            RDB_destroy_obj(&tpl);
            return _RDB_drop_qresult(qrp, txp);
        } else if (ret != RDB_OK) {
            goto error;
        }
    }

    *resp = RDB_TRUE;
    RDB_destroy_obj(&tpl);
    return _RDB_drop_qresult(qrp, txp);

error:
    RDB_destroy_obj(&tpl);
    _RDB_drop_qresult(qrp, txp);
    return ret;
}
