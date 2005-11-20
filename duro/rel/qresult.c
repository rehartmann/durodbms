/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "typeimpl.h"
#include <gen/hashtabit.h>
#include <gen/strfns.h>
#include <string.h>

static int
init_summ_table(RDB_qresult *qresp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /*
     * Initialize table from table #2
     */

    ret = _RDB_table_qresult(qresp->tbp->var.summarize.tb2p, ecp, txp, &qrp);
    if (ret != RDB_OK) {
        return ret;
    }
    ret = _RDB_duprem(qrp, ecp);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&tpl);
    for(;;) {
        int i;

        ret = _RDB_next_tuple(qrp, &tpl, ecp, txp);
        if (ret != RDB_OK)
            break;

        /* Extend tuple */
        for (i = 0; i < qresp->tbp->var.summarize.addc; i++) {
            char *name = qresp->tbp->var.summarize.addv[i].name;
            char *cname;
            RDB_type *typ;

            switch (qresp->tbp->var.summarize.addv[i].op) {
                case RDB_COUNT:
                case RDB_COUNTD:
                    ret = RDB_tuple_set_int(&tpl, name, 0, ecp);
                    break;
                case RDB_AVG:
                case RDB_AVGD:
                    ret = RDB_tuple_set_rational(&tpl, name, 0, ecp);
                    if (ret != RDB_OK)
                       break;
                    cname = malloc(strlen(name) + 3);
                    if (cname == NULL) {
                        RDB_raise_no_memory(ecp);
                        ret = RDB_ERROR;
                        break;
                    }
                    strcpy(cname, name);
                    strcat(cname, AVG_COUNT_SUFFIX);
                    ret = RDB_tuple_set_int(&tpl, cname, 0, ecp);
                    free(cname);
                    break;
                case RDB_SUM:
                case RDB_SUMD:
                    typ = RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp,
                            ecp, txp);
                    if (typ == NULL)
                        goto error;
                    if (typ == &RDB_INTEGER)
                        ret = RDB_tuple_set_int(&tpl, name, 0, ecp);
                    else
                        ret = RDB_tuple_set_rational(&tpl, name, 0.0, ecp);
                    break;
                case RDB_MAX:
                    typ = RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp,
                            ecp, txp);
                    if (typ == NULL)
                        goto error;
                    if (typ == &RDB_INTEGER)
                        ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MIN, ecp);
                    else
                        ret = RDB_tuple_set_rational(&tpl, name,
                                RDB_RATIONAL_MIN, ecp);
                    break;
                case RDB_MIN:
                    typ = RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp,
                            ecp, txp);
                    if (typ == NULL)
                        goto error;
                    if (typ == &RDB_INTEGER)
                        ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MAX, ecp);
                    else
                        ret = RDB_tuple_set_rational(&tpl, name,
                                RDB_RATIONAL_MAX, ecp);
                    break;
                case RDB_ALL:
                    ret = RDB_tuple_set_bool(&tpl, name, RDB_TRUE, ecp);
                    break;
                case RDB_ANY:
                    ret = RDB_tuple_set_bool(&tpl, name, RDB_FALSE, ecp);
                    break;
            }
            if (ret != RDB_OK)
                goto error;
        } /* for */
        ret = RDB_insert(qresp->matp, &tpl, ecp, txp);
        if (ret != RDB_OK)
            goto error;
    };

    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
            goto error;
        }
        RDB_clear_err(ecp);
    }

    RDB_destroy_obj(&tpl, ecp);
    return _RDB_drop_qresult(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    _RDB_drop_qresult(qrp, ecp, txp);

    return RDB_ERROR;
}

struct _RDB_summval {
    RDB_object val;

    /* for AVG */
    int fvidx;
    RDB_int count;
};

static void
summ_step(struct _RDB_summval *svalp, const RDB_object *addvalp, RDB_aggregate_op op)
{
    switch (op) {
        case RDB_COUNT:
            svalp->val.var.int_val++;
            break;
        case RDB_COUNTD:
            break;
        case RDB_AVG:
                svalp->val.var.rational_val =
                        (svalp->val.var.rational_val * svalp->count
                        + addvalp->var.rational_val)
                        / (svalp->count + 1);
                svalp->count++;
            break;
        case RDB_AVGD:
            break;
        case RDB_SUM:
            if (svalp->val.typ == &RDB_INTEGER)
                svalp->val.var.int_val += addvalp->var.int_val;
            else
                svalp->val.var.rational_val += addvalp->var.rational_val;
            break;
        case RDB_SUMD:
            break;
        case RDB_MAX:
            if (svalp->val.typ == &RDB_INTEGER) {
                if (addvalp->var.int_val > svalp->val.var.int_val)
                    svalp->val.var.int_val = addvalp->var.int_val;
            } else {
                if (addvalp->var.rational_val > svalp->val.var.rational_val)
                    svalp->val.var.rational_val = addvalp->var.rational_val;
            }
            break;
        case RDB_MIN:
            if (svalp->val.typ == &RDB_INTEGER) {
                if (addvalp->var.int_val < svalp->val.var.int_val)
                    svalp->val.var.int_val = addvalp->var.int_val;
            } else {
                if (addvalp->var.rational_val < svalp->val.var.rational_val)
                    svalp->val.var.rational_val = addvalp->var.rational_val;
            }
            break;
        case RDB_ANY:
            if (addvalp->var.bool_val)
                svalp->val.var.bool_val = RDB_TRUE;
            break;
        case RDB_ALL: ;
            if (!addvalp->var.bool_val)
                svalp->val.var.bool_val = RDB_FALSE;
            break;
    }
}

static int
do_summarize(RDB_qresult *qresp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_field *keyfv, *nonkeyfv;
    int ret;
    struct _RDB_summval *svalv;
    RDB_object addval;
    int keyfc = _RDB_pkey_len(qresp->tbp);
    int addc = qresp->tbp->var.summarize.addc;
    int avgc = 0;
    int i;

    for (i = 0; i < addc; i++) {
        if (qresp->tbp->var.summarize.addv[i].op == RDB_AVG)
            avgc++;
    }
    keyfv = malloc(sizeof (RDB_field) * keyfc);
    nonkeyfv = malloc(sizeof (RDB_field) * (addc + avgc));
    svalv = malloc(sizeof (struct _RDB_summval) * addc);
    if (keyfv == NULL || nonkeyfv == NULL || svalv == NULL) {
        free(keyfv);
        free(nonkeyfv);
        free(svalv);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /*
     * Iterate over table 1, modifying the materialized table
     */

    ret = _RDB_table_qresult(qresp->tbp->var.summarize.tb1p, ecp, txp, &qrp);
    if (ret != RDB_OK) {
        return ret;
    }
    ret = _RDB_duprem(qrp, ecp);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&addval);
    for (i = 0; i < addc; i++)
        RDB_init_obj(&svalv[i].val);
    do {
        ret = _RDB_next_tuple(qrp, &tpl, ecp, txp);
        if (ret == RDB_OK) {
            int ai;

            /* Build key */
            for (i = 0; i < keyfc; i++) {
                ret = _RDB_obj_to_field(&keyfv[i],
                        RDB_tuple_get(&tpl, qresp->tbp->keyv[0].strv[i]), ecp);
                if (ret != RDB_OK)
                    return ret;
            }

            /* Read added attributes from table #2 */
            ai = 0;
            for (i = 0; i < addc; i++) {
                char *attrname = qresp->tbp->var.summarize.addv[i].name;

                nonkeyfv[i].no = *_RDB_field_no(qresp->matp->stp, attrname);
                if (qresp->tbp->var.summarize.addv[i].op == RDB_AVG) {
                    char *cattrname = malloc(strlen(attrname) + 3);
                    if (cattrname == NULL) {
                        RDB_raise_no_memory(ecp);
                        ret = RDB_ERROR;
                        goto cleanup;
                    }
                    strcpy(cattrname, attrname);
                    strcat(cattrname, AVG_COUNT_SUFFIX);
                    nonkeyfv[addc + ai].no = *_RDB_field_no(qresp->matp->stp,
                                    cattrname);
                    free(cattrname);
                    svalv[i].fvidx = addc + ai;
                    ai++;
                }
            }
            ret = RDB_get_fields(qresp->matp->stp->recmapp, keyfv,
                    addc + avgc, NULL, nonkeyfv);
            if (ret == RDB_OK) {
                /* A corresponding tuple in table 2 has been found */
                for (i = 0; i < addc; i++) {
                    RDB_summarize_add *summp = &qresp->tbp->var.summarize.addv[i];
                    RDB_type *typ;

                    if (summp->op == RDB_COUNT) {
                        ret = RDB_irep_to_obj(&svalv[i].val, &RDB_INTEGER,
                                nonkeyfv[i].datap, nonkeyfv[i].len, ecp);
                    } else {
                        typ = RDB_expr_type(summp->exp,
                                qresp->tbp->var.summarize.tb1p->typ->var.basetyp,
                                ecp, txp);
                        if (typ == NULL)
                            goto cleanup;
                        ret = RDB_irep_to_obj(&svalv[i].val, typ,
                                nonkeyfv[i].datap, nonkeyfv[i].len, ecp);
                        if (ret != RDB_OK)
                            goto cleanup;
                        ret = RDB_evaluate(summp->exp, &tpl, ecp, txp,
                                &addval);
                        if (ret != RDB_OK)
                            goto cleanup;
                        /* If it's AVG, get count */
                        if (summp->op == RDB_AVG) {
                            memcpy(&svalv[i].count, nonkeyfv[svalv[i].fvidx].datap,
                                    sizeof(RDB_int));
                        }
                    }
                    summ_step(&svalv[i], &addval, summp->op);

                    ret = _RDB_obj_to_field(&nonkeyfv[i], &svalv[i].val, ecp);
                    if (ret != RDB_OK)
                        return ret;

                    /* If it's AVG, store count */
                    if (summp->op == RDB_AVG) {
                        nonkeyfv[svalv[i].fvidx].datap = &svalv[i].count;
                        nonkeyfv[svalv[i].fvidx].len = sizeof(RDB_int);
                        nonkeyfv[svalv[i].fvidx].copyfp = memcpy;
                    }
                }
                _RDB_cmp_ecp = ecp;
                ret = RDB_update_rec(qresp->matp->stp->recmapp, keyfv,
                        addc + avgc, nonkeyfv, NULL);
                if (ret != RDB_OK) {
                    _RDB_handle_errcode(ret, ecp, txp);
                    goto cleanup;
                }
            } else {
                _RDB_handle_errcode(ret, ecp, txp);
                if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
                    goto cleanup;
                }
            }
        }
    } while (ret == RDB_OK);

    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        ret = RDB_OK;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&addval, ecp);
    for (i = 0; i < addc; i++)
        RDB_destroy_obj(&svalv[i].val, ecp);
    _RDB_drop_qresult(qrp, ecp, txp);
    free(keyfv);
    free(nonkeyfv);
    free(svalv);

    return ret;
}

static int
do_group(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *newqrp;
    RDB_object tpl;
    RDB_field *keyfv;
    RDB_field gfield;
    RDB_object gval;
    int ret;
    int i;
    RDB_type *greltyp = _RDB_tuple_type_attr(qrp->tbp->typ->var.basetyp,
                        qrp->tbp->var.group.gattr)->typ;
    int keyfc;
    
    keyfc = _RDB_pkey_len(qrp->tbp);

    keyfv = malloc(sizeof (RDB_field) * keyfc);
    if (keyfv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /*
     * Iterate over the original table, modifying the materialized table
     */

    ret = _RDB_table_qresult(qrp->tbp->var.group.tbp, ecp, txp, &newqrp);
    if (ret != RDB_OK) {
        return ret;
    }
    ret = _RDB_duprem(qrp, ecp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);
    RDB_init_obj(&gval);
    do {
        ret = _RDB_next_tuple(newqrp, &tpl, ecp, txp);
        if (ret == RDB_OK) {
            /* Build key */
            for (i = 0; i < keyfc; i++) {
                ret = _RDB_obj_to_field(&keyfv[i],
                        RDB_tuple_get(&tpl, qrp->tbp->keyv[0].strv[i]), ecp);
                if (ret != RDB_OK)
                    goto cleanup;
            }

            if (qrp->matp->stp != NULL) {
                gfield.no = *_RDB_field_no(qrp->matp->stp,
                        qrp->tbp->var.group.gattr);

                /* Try to read tuple of the materialized table */
                ret = RDB_get_fields(qrp->matp->stp->recmapp, keyfv,
                        1, NULL, &gfield);
                _RDB_handle_errcode(ret, ecp, txp);
            } else {
                RDB_raise_not_found("", ecp);
                ret = RDB_ERROR;
            }
            if (ret == RDB_OK) {
                /*
                 * A tuple has been found, add read tuple to
                 * relation-valued attribute
                 */

                /* Get relation-valued attribute */
                ret = RDB_irep_to_obj(&gval, greltyp, gfield.datap,
                        gfield.len, ecp);
                if (ret != RDB_OK)
                    goto cleanup;

                /* Insert tuple (not-grouped attributes will be ignored) */
                ret = RDB_insert(RDB_obj_table(&gval), &tpl, ecp, txp);
                if (ret != RDB_OK)
                    goto cleanup;

                /* Update materialized table */
                ret = _RDB_obj_to_field(&gfield, &gval, ecp);
                if (ret != RDB_OK)
                    goto cleanup;
                _RDB_cmp_ecp = ecp;
                ret = RDB_update_rec(qrp->matp->stp->recmapp, keyfv,
                        1, &gfield, NULL);
                if (ret != RDB_OK)
                    goto cleanup;
            } else if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                /*
                 * A tuple has not been found, build tuple and insert it
                 */
                RDB_table *gtbp;

                RDB_clear_err(ecp);
                gtbp = RDB_create_table(NULL, RDB_FALSE,
                               greltyp->var.basetyp->var.tuple.attrc,
                               greltyp->var.basetyp->var.tuple.attrv,
                               0, NULL, ecp, NULL);
                if (gtbp == NULL)
                    goto cleanup;

                ret = RDB_insert(gtbp, &tpl, ecp, NULL);
                if (ret != RDB_OK)
                    goto cleanup;

                /*
                 * Set then attribute first, then assign the table value to it
                 */
                ret = RDB_tuple_set(&tpl, qrp->tbp->var.group.gattr, &gval, ecp);
                if (ret != RDB_OK)
                    goto cleanup;
                RDB_table_to_obj(RDB_tuple_get(&tpl, qrp->tbp->var.group.gattr),
                                 gtbp, ecp);

                ret = RDB_insert(qrp->matp, &tpl, ecp, NULL);
                if (ret != RDB_OK)
                    goto cleanup;
            } else {
                goto cleanup;
            }
        }
    } while (ret == RDB_OK);

    if (ret != RDB_OK
            && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        ret = RDB_OK;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&gval, ecp);
    _RDB_drop_qresult(newqrp, ecp, txp);
    free(keyfv);

    return ret;
}

static int
stored_qresult(RDB_qresult *qrp, RDB_table *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (tbp->stp == NULL) {
        /*
         * Table has no physical representation, which means it is empty
         */
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }

    qrp->uses_cursor = RDB_TRUE;
    /* !! delay after first call to _RDB_qresult_next()? */
    ret = RDB_recmap_cursor(&qrp->var.curp, tbp->stp->recmapp,
                    RDB_FALSE, tbp->is_persistent ? txp->txid : NULL);
    if (ret != RDB_OK) {
        if (txp != NULL && (ret > 0 || ret < -1000)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, "cannot create cursor: %s",
                    db_strerror(ret));
        }
        _RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    ret = RDB_cursor_first(qrp->var.curp);
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
index_qresult(RDB_qresult *qrp, _RDB_tbindex *indexp,
        RDB_transaction *txp)
{
    int ret;

    /* !! delay after first call to _RDB_qresult_next()? */
    qrp->uses_cursor = RDB_TRUE;
    qrp->matp = NULL;
    ret = RDB_index_cursor(&qrp->var.curp, indexp->idxp, RDB_FALSE,
            txp != NULL ? txp->txid : NULL);
    if (ret != RDB_OK) {
        if (txp != NULL && (ret > 0 || ret < -1000)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, "cannot create cursor: %s",
                    db_strerror(ret));
        }
        return ret;
    }
    ret = RDB_cursor_first(qrp->var.curp);
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        ret = RDB_OK;
    }
    return ret;
}

static int
select_index_qresult(RDB_qresult *qrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_field *fv;
    int flags = 0;
    _RDB_tbindex *indexp = qrp->tbp->var.select.tbp->var.project.indexp;

    /*
     * If the index is unique, there is nothing to do
     */
    if (indexp->unique) {
        qrp->uses_cursor = RDB_FALSE;
        qrp->matp = NULL;
        qrp->var.virtual.qrp = NULL;
        return RDB_OK;
    }

    fv  = malloc(sizeof (RDB_field) * indexp->attrc);

    if (fv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    qrp->uses_cursor = RDB_TRUE;
    qrp->matp = NULL;
    ret = RDB_index_cursor(&qrp->var.curp, indexp->idxp,
            RDB_FALSE, qrp->tbp->var.select.tbp->var.project.tbp->is_persistent ?
            txp->txid : NULL);
    if (ret != RDB_OK) {
        if (txp != NULL && (ret > 0 || ret < -1000)) {
            RDB_errmsg(txp->dbp->dbrootp->envp, "cannot create cursor: %s",
                    db_strerror(ret));
        }
        free(fv);
        return ret;
    }

    for (i = 0; i < qrp->tbp->var.select.objpc; i++) {
        ret = _RDB_obj_to_field(&fv[i], qrp->tbp->var.select.objpv[i], ecp);
        if (ret != RDB_OK) {
            free(fv);
            return ret;
        }
    }
    for (i = qrp->tbp->var.select.objpc; i < indexp->attrc;
            i++) {
        fv[i].len = 0;
    }

    if (qrp->tbp->var.select.objpc != indexp->attrc
            || !qrp->tbp->var.select.all_eq)
        flags = RDB_REC_RANGE;

    ret = RDB_cursor_seek(qrp->var.curp, qrp->tbp->var.select.objpc, fv, flags);
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        ret = RDB_OK;
    }

    free(fv);
    return ret;
}

static int
summarize_qresult(RDB_qresult *qresp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_type *tpltyp = qresp->tbp->typ->var.basetyp;

    /* Need keys */
    if (qresp->tbp->keyv == NULL) {
        ret = RDB_table_keys(qresp->tbp, ecp, NULL);
        if (ret < 0)
            return ret;
    }

    /* create materialized table */
    qresp->matp = _RDB_create_table(NULL, RDB_FALSE, tpltyp->var.tuple.attrc,
                        tpltyp->var.tuple.attrv,
                        qresp->tbp->keyc, qresp->tbp->keyv, ecp, NULL);
    if (qresp->matp == NULL)
        return RDB_ERROR;

    ret = init_summ_table(qresp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, ecp, txp);
        return ret;
    }

    /* summarize over table 1 */
    ret = do_summarize(qresp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, ecp, txp);
        return ret;
    }

    ret = stored_qresult(qresp, qresp->matp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, ecp, txp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
group_qresult(RDB_qresult *qresp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *tuptyp = qresp->tbp->typ->var.basetyp;
    int ret;

    /* Need keys */
    if (qresp->tbp->keyv == NULL) {
        ret = RDB_table_keys(qresp->tbp, ecp, NULL);
        if (ret < 0)
            return ret;
    }

    /* create materialized table */
    qresp->matp = RDB_create_table(NULL, RDB_FALSE,
                        tuptyp->var.tuple.attrc, tuptyp->var.tuple.attrv,
                        qresp->tbp->keyc, qresp->tbp->keyv, ecp, NULL);
    if (qresp->matp == NULL)
        return RDB_ERROR;

    /* do the grouping */
    ret = do_group(qresp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, ecp, txp);
        return ret;
    }

    ret = stored_qresult(qresp, qresp->matp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, ecp, txp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
join_qresult(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    qrp->uses_cursor = RDB_FALSE;
    qrp->matp = NULL;
    /* Create qresult for the first table */
    ret = _RDB_table_qresult(qrp->tbp->var.join.tb1p, ecp, txp,
            &qrp->var.virtual.qrp);
    if (ret != RDB_OK)
        return ret;

    qrp->var.virtual.tpl_valid = RDB_FALSE;
    if (qrp->tbp->var.join.tb2p->kind != RDB_TB_PROJECT
            || qrp->tbp->var.join.tb2p->var.project.indexp == NULL) {
        /* Create qresult for 2nd table */
        ret = _RDB_table_qresult(qrp->tbp->var.join.tb2p, ecp, txp,
                &qrp->var.virtual.qr2p);
        if (ret != RDB_OK) {
            _RDB_drop_qresult(qrp->var.virtual.qr2p, ecp, txp);
            return ret;
        }
    } else if (!qrp->tbp->var.join.tb2p->var.project.indexp->unique) {
        /* Create index qresult for 2nd table */
        qrp->var.virtual.qr2p = malloc(sizeof (RDB_qresult));
        if (qrp->var.virtual.qr2p == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        qrp->var.virtual.qr2p->tbp = qrp->tbp->var.join.tb2p;
        qrp->var.virtual.qr2p->endreached = RDB_FALSE;
        qrp->var.virtual.qr2p->matp = NULL;

        qrp->var.virtual.qr2p->uses_cursor = RDB_TRUE;
        qrp->var.virtual.qr2p->matp = NULL;
        ret = RDB_index_cursor(&qrp->var.virtual.qr2p->var.curp,
                qrp->tbp->var.join.tb2p->var.project.indexp->idxp, RDB_FALSE,
                qrp->tbp->var.join.tb2p->var.project.tbp->is_persistent ?
                        txp->txid : NULL);
        if (ret != RDB_OK) {
            free(qrp->var.virtual.qr2p);
            if (txp != NULL && (ret > 0 || ret < -1000)) {
                RDB_errmsg(txp->dbp->dbrootp->envp, "cannot create cursor: %s",
                        db_strerror(ret));
            }
            return ret;
        }
    } else {
        qrp->var.virtual.qr2p = NULL;
    }
    return RDB_OK;
}

static int
init_qresult(RDB_qresult *qrp, RDB_table *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    qrp->tbp = tbp;
    qrp->endreached = RDB_FALSE;
    qrp->matp = NULL;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            ret = stored_qresult(qrp, tbp, ecp, txp);
            break;
        case RDB_TB_SELECT:
            if (tbp->var.select.objpc == 0) {
                qrp->uses_cursor = RDB_FALSE;
                qrp->matp = NULL;
                qrp->var.virtual.qr2p = NULL;
                ret = _RDB_table_qresult(tbp->var.select.tbp, ecp, txp,
                        &qrp->var.virtual.qrp);
            } else {
                ret = select_index_qresult(qrp, ecp, txp);
            }
            break;
        case RDB_TB_UNION:
            qrp->uses_cursor = RDB_FALSE;
            qrp->matp = NULL;
            qrp->var.virtual.qr2p = NULL;
            ret = _RDB_table_qresult(tbp->var._union.tb1p, ecp, txp,
                    &qrp->var.virtual.qrp);
            break;
        case RDB_TB_MINUS:
            qrp->uses_cursor = RDB_FALSE;
            qrp->var.virtual.qr2p = NULL;
            qrp->matp = NULL;
            ret = _RDB_table_qresult(tbp->var.minus.tb1p, ecp, txp,
                    &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.minus.tb2p, ecp, txp,
                    &qrp->var.virtual.qr2p);
            break;
        case RDB_TB_INTERSECT:
            qrp->uses_cursor = RDB_FALSE;
            qrp->var.virtual.qr2p = NULL;
            qrp->matp = NULL;
            ret = _RDB_table_qresult(tbp->var.intersect.tb1p, ecp, txp,
                    &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.minus.tb2p, ecp, txp,
                    &qrp->var.virtual.qr2p);
            break;
        case RDB_TB_JOIN:
            ret = join_qresult(qrp, ecp, txp);
            break;
        case RDB_TB_EXTEND:
            qrp->uses_cursor = RDB_FALSE;
            qrp->var.virtual.qr2p = NULL;
            qrp->matp = NULL;
            ret = _RDB_table_qresult(tbp->var.extend.tbp, ecp, txp,
                    &qrp->var.virtual.qrp);
            break;
        case RDB_TB_PROJECT:
            if (tbp->var.project.indexp == NULL
                    || tbp->var.project.indexp->idxp == NULL) {
                qrp->uses_cursor = RDB_FALSE;
                qrp->var.virtual.qr2p = NULL;
                qrp->matp = NULL;
                ret = _RDB_table_qresult(tbp->var.project.tbp, ecp, txp,
                        &qrp->var.virtual.qrp);
            } else {
                ret = index_qresult(qrp, tbp->var.project.indexp, txp);
            }
            break;
        case RDB_TB_SUMMARIZE:
            ret = summarize_qresult(qrp, ecp, txp);
            break;
        case RDB_TB_RENAME:
            qrp->uses_cursor = RDB_FALSE;
            qrp->var.virtual.qr2p = NULL;
            qrp->matp = NULL;
            ret = _RDB_table_qresult(tbp->var.rename.tbp, ecp, txp,
                    &qrp->var.virtual.qrp);
            break;
        case RDB_TB_WRAP:
            qrp->uses_cursor = RDB_FALSE;
            qrp->var.virtual.qr2p = NULL;
            qrp->matp = NULL;
            ret = _RDB_table_qresult(tbp->var.wrap.tbp, ecp, txp,
                    &qrp->var.virtual.qrp);
            break;
        case RDB_TB_UNWRAP:
            qrp->uses_cursor = RDB_FALSE;
            qrp->var.virtual.qr2p = NULL;
            qrp->matp = NULL;
            ret = _RDB_table_qresult(tbp->var.unwrap.tbp, ecp, txp,
                    &qrp->var.virtual.qrp);
            break;
        case RDB_TB_GROUP:
            ret = group_qresult(qrp, ecp, txp);
            break;
        case RDB_TB_UNGROUP:
            qrp->uses_cursor = RDB_FALSE;
            qrp->var.virtual.qr2p = NULL;
            qrp->matp = NULL;
            ret = _RDB_table_qresult(tbp->var.ungroup.tbp, ecp, txp,
                    &qrp->var.virtual.qrp);
            qrp->var.virtual.tpl_valid = RDB_FALSE;
            break;
        case RDB_TB_SDIVIDE:
            qrp->uses_cursor = RDB_FALSE;
            qrp->matp = NULL;
            /* Create qresults for table #1 and table #3 */
            ret = _RDB_table_qresult(tbp->var.sdivide.tb1p, ecp, txp,
                    &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.sdivide.tb3p, ecp, txp,
                    &qrp->var.virtual.qr2p);
            if (ret != RDB_OK) {
                _RDB_drop_qresult(qrp->var.virtual.qr2p, ecp, txp);
                break;
            }
    }
    return ret;
}

static RDB_bool
qr_dups(RDB_table *tbp, RDB_exec_context *ecp)
{
    switch (tbp->kind) {
        case RDB_TB_REAL:
            return RDB_FALSE;
        case RDB_TB_SELECT:
            return qr_dups(tbp->var.select.tbp, ecp);
        case RDB_TB_UNION:
            return RDB_TRUE;
        case RDB_TB_MINUS:
            return qr_dups(tbp->var.minus.tb1p, ecp);
        case RDB_TB_INTERSECT:
            return qr_dups(tbp->var.intersect.tb1p, ecp);
        case RDB_TB_JOIN:
            return (RDB_bool) (qr_dups(tbp->var.join.tb1p, ecp)
                    || qr_dups(tbp->var.join.tb2p, ecp));
        case RDB_TB_EXTEND:
            return qr_dups(tbp->var.extend.tbp, ecp);
        case RDB_TB_PROJECT:
            if (tbp->keyv == NULL) {
                /* Get keys and set keyloss flag */
                RDB_table_keys(tbp, ecp, NULL);
            }
            return tbp->var.project.keyloss;
        case RDB_TB_SUMMARIZE:
            return RDB_FALSE;
        case RDB_TB_RENAME:
            return qr_dups(tbp->var.rename.tbp, ecp);
        case RDB_TB_WRAP:
            return qr_dups(tbp->var.wrap.tbp, ecp);
        case RDB_TB_UNWRAP:
            return qr_dups(tbp->var.unwrap.tbp, ecp);
        case RDB_TB_GROUP:
            return RDB_FALSE;
        case RDB_TB_UNGROUP:
            return qr_dups(tbp->var.ungroup.tbp, ecp);
        case RDB_TB_SDIVIDE:
            return qr_dups(tbp->var.sdivide.tb1p, ecp);
    }
    abort();
}

int
_RDB_duprem(RDB_qresult *qrp, RDB_exec_context *ecp)
{
    RDB_bool rd = qr_dups(qrp->tbp, ecp);
    if (RDB_get_err(ecp) != NULL)
        return RDB_ERROR;

    /*
     * Add duplicate remover only if the qresult may return duplicates
     */
    if (rd) {
        RDB_string_vec keyattrs;
        int i;
        RDB_type *tuptyp = qrp->tbp->typ->var.basetyp;

        keyattrs.strc = tuptyp->var.tuple.attrc;
        keyattrs.strv = malloc(sizeof (char *) * keyattrs.strc);

        for (i = 0; i < keyattrs.strc; i++)
            keyattrs.strv[i] = tuptyp->var.tuple.attrv[i].name;

        /* Create materialized (all-key) table */
        qrp->matp = RDB_create_table(NULL, RDB_FALSE,
                tuptyp->var.tuple.attrc, tuptyp->var.tuple.attrv,
                1, &keyattrs, ecp, NULL);
        free(keyattrs.strv);
        if (qrp->matp == NULL)
            return RDB_ERROR;
    }
    return RDB_OK;
}

int
_RDB_table_qresult(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_qresult **qrpp)
{
    int ret;

    *qrpp = malloc(sizeof (RDB_qresult));
    if (*qrpp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    ret = init_qresult(*qrpp, tbp, ecp, txp);
    if (ret != RDB_OK)
        free(*qrpp);
    return ret;
}

/*
static void
print_mem_stat(RDB_environment *envp)
{
    DB_MPOOL_STAT *statp;
    int ret;

    ret = envp->envp->memp_stat(envp->envp, &statp, NULL, 0);
    if (ret != 0) {
        fputs(db_strerror(ret), stderr);
        abort();
    }
    fprintf(stderr, "st_cache_miss = %d\n", statp->st_cache_miss);
    free(statp);
}
*/

/*
 * Creates a qresult which sorts a table.
 */
int
_RDB_sorter(RDB_table *tbp, RDB_qresult **qrespp, RDB_exec_context *ecp,
        RDB_transaction *txp, int seqitc, const RDB_seq_item seqitv[])
{
    RDB_string_vec key;
    RDB_bool *ascv = NULL;
    int ret;
    int i;
    RDB_qresult *tmpqrp;
    RDB_object tpl;
    FILE *oerrfilep;
    RDB_errfn *oerrfp;
    void *errarg;
    RDB_environment *envp = RDB_db_env(RDB_tx_db(txp));
    RDB_qresult *qresp = malloc(sizeof (RDB_qresult));

    if (qresp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    key.strc = seqitc;
    key.strv = malloc(sizeof (char *) * seqitc);
    if (key.strv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    ascv = malloc(sizeof (RDB_bool) * seqitc);
    if (ascv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    qresp->tbp = NULL;
    qresp->endreached = RDB_FALSE;

    for (i = 0; i < seqitc; i++) {
        key.strv[i] = seqitv[i].attrname;
        ascv[i] = seqitv[i].asc;
    }

    /*
     * Create a sorted RDB_table
     */

    qresp->matp = _RDB_new_rtable(NULL, RDB_FALSE,
            tbp->typ->var.basetyp->var.tuple.attrc,
            tbp->typ->var.basetyp->var.tuple.attrv, 1, &key, RDB_TRUE, ecp);
    if (qresp->matp == NULL)
        goto error;

    ret = _RDB_create_stored_table(qresp->matp, txp->dbp->dbrootp->envp,
            ascv, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    /*
     * Copy tuples into the newly created RDB_table
     */

    ret = _RDB_table_qresult(tbp, ecp, txp, &tmpqrp);
    if (ret != RDB_OK)
        goto error;

    oerrfilep = RDB_get_errfile(envp);
    oerrfp = RDB_get_errfn(envp, &errarg);

    RDB_init_obj(&tpl);
    while ((ret = _RDB_next_tuple(tmpqrp, &tpl, ecp, txp)) == RDB_OK) {
        /*
         * We don't want to see the error msg about duplicate data items,
         * so temporarily disable error logging
         */
        RDB_set_errfile(envp, NULL);
        RDB_set_errfn(envp, NULL, NULL);

        ret = RDB_insert(qresp->matp, &tpl, ecp, txp);
        /* Re-enable error logging */
        RDB_set_errfile(envp, oerrfilep);
        RDB_set_errfn(envp, oerrfp, errarg);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_ELEMENT_EXISTS_ERROR) {
                RDB_destroy_obj(&tpl, ecp);
                goto error;
            }
            RDB_clear_err(ecp);
        }
    }
    RDB_destroy_obj(&tpl, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);
    ret = _RDB_drop_qresult(tmpqrp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    ret = stored_qresult(qresp, qresp->matp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    free(key.strv);
    free(ascv);

    *qrespp = qresp;
    return RDB_OK;

error:
    if (key.strv != NULL)
        free(key.strv);
    if (ascv != NULL)
        free(ascv);
    if (qresp->matp != NULL)
        RDB_drop_table(qresp->matp, ecp, NULL);
    free(qresp);

    return RDB_ERROR;
}

int
_RDB_get_by_cursor(RDB_table *tbp, RDB_cursor *curp, RDB_type *tpltyp,
        RDB_object *tplp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_object val;
    RDB_int fno;
    RDB_attr *attrp;
    void *datap;
    size_t len;

    for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
        attrp = &tpltyp->var.tuple.attrv[i];

        fno = *_RDB_field_no(tbp->stp, attrp->name);
        ret = RDB_cursor_get(curp, fno, &datap, &len);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }
        RDB_init_obj(&val);
        ret = RDB_tuple_set(tplp, attrp->name, &val, ecp);
        RDB_destroy_obj(&val, ecp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
        ret = RDB_irep_to_obj(RDB_tuple_get(tplp, attrp->name),
                attrp->typ, datap, len, ecp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
next_stored_tuple(RDB_qresult *qrp, RDB_table *tbp, RDB_object *tplp,
        RDB_bool asc, RDB_bool dup, RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (qrp->endreached) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (tplp != NULL) {
        ret = _RDB_get_by_cursor(tbp, qrp->var.curp, tpltyp, tplp, ecp, txp);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }
    }
    if (asc) {
        ret = RDB_cursor_next(qrp->var.curp, dup ? RDB_REC_DUP : 0);
    } else {
        if (dup) {
            RDB_raise_invalid_argument("", ecp);
            return RDB_ERROR;
        }
        ret = RDB_cursor_prev(qrp->var.curp);
    }
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
next_ungroup_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_table *attrtbp;
    RDB_hashtable_iter hiter;

    /* If no tuple has been read, read first tuple */
    if (!qrp->var.virtual.tpl_valid) {
        RDB_init_obj(&qrp->var.virtual.tpl);
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl,
                ecp, txp);
        if (ret != RDB_OK)
            return ret;
        qrp->var.virtual.tpl_valid = RDB_TRUE;
    }

    /* If there is no 2nd qresult, open it from relation-valued attribute */
    if (qrp->var.virtual.qr2p == NULL) {
        attrtbp = RDB_tuple_get(&qrp->var.virtual.tpl,
                qrp->tbp->var.ungroup.attr)->var.tbp;

        ret = _RDB_table_qresult(attrtbp, ecp, txp, &qrp->var.virtual.qr2p);
        if (ret != RDB_OK)
            return ret;
    }

    /* Read next element of relation-valued attribute */
    ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp, ecp, txp);
    while (ret == DB_NOTFOUND) {
        /* Destroy qresult over relation-valued attribute */
        ret = _RDB_drop_qresult(qrp->var.virtual.qr2p, ecp, txp);
        qrp->var.virtual.qr2p = NULL;
        if (ret != RDB_OK) {
            return ret;
        }

        /* Read next tuple */
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return ret;
        }

        attrtbp = RDB_tuple_get(&qrp->var.virtual.tpl,
                qrp->tbp->var.ungroup.attr)->var.tbp;

        /* Create qresult over relation-valued attribute */
        ret = _RDB_table_qresult(attrtbp, ecp, txp, &qrp->var.virtual.qr2p);
        if (ret != RDB_OK) {
            return ret;
        }

        ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp, ecp, txp);
    }
    if (ret != RDB_OK) {
        return ret;
    }

    /* Merge tuples, skipping the relation-valued attribute */
    RDB_init_hashtable_iter(&hiter,
            (RDB_hashtable *) &qrp->var.virtual.tpl.var.tpl_tab);
    for (;;) {
        /* Get next attribute */
        tuple_entry *entryp = RDB_hashtable_next(&hiter);
        if (entryp == NULL)
            break;

        if (strcmp(entryp->key, qrp->tbp->var.ungroup.attr) != 0)
            RDB_tuple_set(tplp, entryp->key, &entryp->obj, ecp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    return RDB_OK;
}

static int
next_join_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;

    /* tuple type of first ('outer') table */
    RDB_type *tpltyp1 = qrp->tbp->var.join.tb1p->typ->var.basetyp;

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->var.virtual.tpl_valid) {
        RDB_init_obj(&qrp->var.virtual.tpl);
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl,
                ecp, txp);
        if (ret != RDB_OK)
            return ret;
        qrp->var.virtual.tpl_valid = RDB_TRUE;
    }
        
    for (;;) {
        /* read next 'inner' tuple */
        ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp, ecp, txp);
        if (ret != RDB_OK && RDB_obj_type(RDB_get_err(ecp))
                != &RDB_NOT_FOUND_ERROR) {
            return RDB_ERROR;
        }
        if (ret == RDB_OK) {
            /* Compare common attributes */
            RDB_bool iseq = RDB_TRUE;
            RDB_type *tpltyp = qrp->tbp->var.join.tb1p->typ->var.basetyp;

            /*
             * If the values of the common attributes are not equal,
             * skip to next tuple
             */
            for (i = 0; (i < tpltyp->var.tuple.attrc) && iseq; i++) {
                RDB_bool b;
                char *attrname = tpltyp->var.tuple.attrv[i].name;

                if (RDB_type_attr_type(qrp->tbp->var.join.tb2p->typ, attrname)
                        != NULL) {
                    ret = RDB_obj_equals(
                            RDB_tuple_get(&qrp->var.virtual.tpl, attrname),
                            RDB_tuple_get(tplp, attrname), ecp, txp, &b);
                    if (ret != RDB_OK)
                        return ret;
                    if (!b)
                        iseq = RDB_FALSE;
                }
            }

            /* If common attributes are equal, leave the loop,
             * otherwise read next tuple
             */
            if (iseq)
                break;
            continue;
        } else {
            RDB_clear_err(ecp);
        }

        /* reset nested qresult */
        ret = _RDB_reset_qresult(qrp->var.virtual.qr2p, ecp, txp);
        if (ret != RDB_OK) {
            return ret;
        }

        /* read next 'outer' tuple */
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return ret;
        }
    }
    
    /* join the two tuples into tplp */
    for (i = 0; i < tpltyp1->var.tuple.attrc; i++) {
         ret = RDB_tuple_set(tplp, tpltyp1->var.tuple.attrv[i].name,
                       RDB_tuple_get(&qrp->var.virtual.tpl,
                       tpltyp1->var.tuple.attrv[i].name), ecp);
         if (ret != RDB_OK) {
             return ret;
         }
    }

    return RDB_OK;
}

static int
seek_index_qresult(RDB_qresult *qrp, _RDB_tbindex *indexp,
        const RDB_object *tplp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_field *fv = malloc(sizeof (RDB_field) * indexp->attrc);
    if (fv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < indexp->attrc; i++) {
        ret = _RDB_obj_to_field(&fv[i],
                RDB_tuple_get(tplp, indexp->attrv[i].attrname), ecp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_cursor_seek(qrp->var.curp, indexp->attrc, fv, 0);
    if (ret == RDB_OK) {
        qrp->endreached = RDB_FALSE;
    } else {
        qrp->endreached = RDB_TRUE;
    }

cleanup:
    free(fv);

    return ret;
}

static int
next_join_tuple_nuix(RDB_qresult *qrp, RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    _RDB_tbindex *indexp = qrp->tbp->var.join.tb2p->var.project.indexp;

    /* tuple type of first ('outer') table */
    RDB_type *tpltyp1 = qrp->tbp->var.join.tb1p->typ->var.basetyp;

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->var.virtual.tpl_valid) {
        RDB_init_obj(&qrp->var.virtual.tpl);
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl,
                ecp, txp);
        if (ret != RDB_OK)
            return ret;
        qrp->var.virtual.tpl_valid = RDB_TRUE;

        /* Set cursor position */
        ret = seek_index_qresult(qrp->var.virtual.qr2p, indexp,
                &qrp->var.virtual.tpl, ecp);
        if (ret != RDB_OK)
            return ret;
    }
        
    for (;;) {
        /* read next 'inner' tuple */
        ret = next_stored_tuple(qrp->var.virtual.qr2p,
                qrp->var.virtual.qr2p->tbp->var.project.tbp, tplp, RDB_TRUE,
                RDB_TRUE, qrp->var.virtual.qr2p->tbp->typ->var.basetyp, ecp,
                txp);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        }
        if (ret == RDB_OK) {
            /* Compare common attributes */
            RDB_bool iseq = RDB_TRUE;
            RDB_type *tpltyp = qrp->tbp->var.join.tb1p->typ->var.basetyp;

            /*
             * If the values of the common attributes are not equal,
             * skip to next tuple
             */
            for (i = 0; (i < tpltyp->var.tuple.attrc) && iseq; i++) {
                RDB_bool b;
                char *attrname = tpltyp->var.tuple.attrv[i].name;

                if (RDB_type_attr_type(qrp->tbp->var.join.tb2p->typ, attrname)
                        != NULL) {
                    int j;

                    /* Don't compare index attributes */
                    for (j = 0;
                         j < indexp->attrc && strcmp(attrname,
                                 indexp->attrv[j].attrname) != 0;
                         j++);
                    if (j >= indexp->attrc) {
                        ret = RDB_obj_equals(RDB_tuple_get(
                                &qrp->var.virtual.tpl, attrname),
                                RDB_tuple_get(tplp, attrname), ecp, txp, &b);
                        if (ret != RDB_OK)
                            return ret;
                        if (!b)
                            iseq = RDB_FALSE;
                    }
                }
            }

            /* If common attributes are equal, leave the loop,
             * otherwise read next tuple
             */
            if (iseq)
                break;
            continue;
        }

        /* read next 'outer' tuple */
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return ret;
        }

        /* reset cursor */
        ret = seek_index_qresult(qrp->var.virtual.qr2p, indexp,
                &qrp->var.virtual.tpl, ecp);
        if (ret != RDB_OK)
            return ret;
    }
    
    /* join the two tuples into tplp */
    for (i = 0; i < tpltyp1->var.tuple.attrc; i++) {
         ret = RDB_tuple_set(tplp, tpltyp1->var.tuple.attrv[i].name,
                       RDB_tuple_get(&qrp->var.virtual.tpl,
                       tpltyp1->var.tuple.attrv[i].name), ecp);
         if (ret != RDB_OK) {
             return ret;
         }
    }

    return RDB_OK;
}

static int
next_join_tuple_uix(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
    RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_bool match;
    RDB_object tpl;
    RDB_type *tpltyp;
    RDB_object **objpv;
    _RDB_tbindex *indexp = qrp->tbp->var.join.tb2p->var.project.indexp;

    objpv = malloc(sizeof(RDB_object *) * indexp->attrc);
    if (objpv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    do {
        match = RDB_TRUE;
    
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;

        for (i = 0; i < indexp->attrc; i++) {
            objpv[i] = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
        }
        ret = _RDB_get_by_uindex(qrp->tbp->var.join.tb2p->var.project.tbp,
                objpv, indexp, qrp->tbp->var.join.tb2p->typ->var.basetyp,
                ecp, txp, &tpl);
        if (ret == DB_NOTFOUND) {
            match = RDB_FALSE;
            continue;
        }
        if (ret != RDB_OK)
            goto cleanup;

        tpltyp = qrp->tbp->var.join.tb2p->typ->var.basetyp;
        for (i = 0; i < tpltyp->var.tuple.attrc;
                i++) {
            char *attrname = tpltyp->var.tuple.attrv[i].name;

            /* Check if the attribute appears in both tables */
            if (RDB_type_attr_type(qrp->tbp->var.join.tb1p->typ, attrname)
                    == NULL) {
                /* No, so copy it */
                ret = RDB_tuple_set(tplp, attrname,
                        RDB_tuple_get(&tpl, attrname), ecp);
                if (ret != RDB_OK)
                    return ret;
            } else {
                /* Yes, so compare attribute values if the attribute
                 * doesn't appear in the index
                 */
                int j;

                for (j = 0; j < indexp->attrc
                        && strcmp(indexp->attrv[j].attrname, attrname) != 0;
                        j++);
                if (j >= indexp->attrc) {
                    ret = RDB_obj_equals(RDB_tuple_get(&tpl, attrname),
                            RDB_tuple_get(tplp, attrname), ecp, txp, &match);
                    if (ret != RDB_OK)
                        goto cleanup;
                    if (!match)
                        break;
                }
            }
        }
    } while (!match);

    ret = RDB_OK;

cleanup:
    free(objpv);

    RDB_destroy_obj(&tpl, ecp);

    return ret;
}

/*
 * Read a tuple from a table using the unique index given by indexp,
 * using the values given by objpv as a key.
 * Read only the attributes in tpltyp.
 */
int
_RDB_get_by_uindex(RDB_table *tbp, RDB_object *objpv[], _RDB_tbindex *indexp,
        RDB_type *tpltyp, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *tplp)
{
    RDB_field *fv;
    RDB_field *resfv;
    int i;
    int ret;
    int keylen = indexp->attrc;

    resfv = malloc(sizeof (RDB_field)
            * (tpltyp->var.tuple.attrc - keylen));
    fv = malloc(sizeof (RDB_field) * keylen);
    if (fv == NULL || resfv == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Convert data to fields
     */    
    for (i = 0; i < keylen; i++) {
        ret = _RDB_obj_to_field(&fv[i], objpv[i], ecp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    /*
     * Set 'no' fields of resfv and Read fields
     */
    if (indexp->idxp == NULL) {
        int rfi = 0;

        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            int fno = *_RDB_field_no(tbp->stp, tpltyp->var.tuple.attrv[i].name);

            if (fno >= keylen)
                resfv[rfi++].no = fno;
        }
        ret = RDB_get_fields(tbp->stp->recmapp, fv,
                             tpltyp->var.tuple.attrc - keylen,
                             tbp->is_persistent ? txp->txid : NULL, resfv);
    } else {
        int rfi = 0;

        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            int j;
            int fno = *_RDB_field_no(tbp->stp, tpltyp->var.tuple.attrv[i].name);

            /* Search field number in index */
            for (j = 0; j < keylen && indexp->idxp->fieldv[j] != fno; j++);

            /* If not found, so the field must be read from the DB */
            if (j >= keylen)
                resfv[rfi++].no = fno;
        }
        ret = RDB_index_get_fields(indexp->idxp, fv,
                             tpltyp->var.tuple.attrc - keylen,
                             tbp->is_persistent ? txp->txid : NULL, resfv);
    }
    
    if (ret != RDB_OK) {
        goto cleanup;
    }

    /*
     * Set key attributes
     */
    for (i = 0; i < indexp->attrc; i++) {
        ret = RDB_tuple_set(tplp, indexp->attrv[i].attrname, objpv[i], ecp);
        if (ret != RDB_OK)
            return ret;
    }

    /*
     * Set non-key attributes
     */
    for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
        int rfi; /* Index in resv, -1 if key attr */
        char *attrname = tpltyp->var.tuple.attrv[i].name;
        RDB_int fno = *_RDB_field_no(tbp->stp, attrname);

        /* Search field number in resfv */
        rfi = 0;
        while (rfi < tpltyp->var.tuple.attrc - keylen
                && resfv[rfi].no != fno)
            rfi++;
        if (rfi >= tpltyp->var.tuple.attrc - keylen)
            rfi = -1;

        if (rfi != -1) {
            /* non-key attribute */
            RDB_object val;

            RDB_init_obj(&val);
            ret = RDB_irep_to_obj(&val, tpltyp->var.tuple.attrv[i].typ,
                    resfv[rfi].datap, resfv[rfi].len, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val, ecp);
                goto cleanup;
            }
            ret = RDB_tuple_set(tplp, attrname, &val, ecp);
            RDB_destroy_obj(&val, ecp);
            if (ret != RDB_OK)
                goto cleanup;
        }
    }
    ret = RDB_OK;

cleanup:
    free(fv);
    free(resfv);
    return ret;
}

static int
next_select_index(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    _RDB_tbindex *indexp = qrp->tbp->var.select.tbp->var.project.indexp;

    if (indexp->unique) {
        ret = _RDB_get_by_uindex(qrp->tbp->var.select.tbp->var.project.tbp,
                qrp->tbp->var.select.objpv, indexp,
                qrp->tbp->typ->var.basetyp, ecp, txp, tplp);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }

        qrp->endreached = RDB_TRUE;
    } else {
        RDB_bool rtup;
        RDB_bool dup = RDB_FALSE;

        if (qrp->tbp->var.select.objpc == indexp->attrc
                && qrp->tbp->var.select.all_eq)
            dup = RDB_TRUE;

        tplp->typ = qrp->tbp->typ->var.basetyp;
        do {
            ret = next_stored_tuple(qrp,
                    qrp->tbp->var.select.tbp->var.project.tbp,
                    tplp, qrp->tbp->var.select.asc, dup,
                    qrp->tbp->var.select.tbp->typ->var.basetyp, ecp, txp);
            if (ret != RDB_OK)
                return ret;
            if (qrp->tbp->var.select.all_eq)
                rtup = RDB_TRUE;
            else {
                if (qrp->tbp->var.select.stopexp != NULL) {
                    ret = RDB_evaluate_bool(qrp->tbp->var.select.stopexp, tplp,
                            ecp, txp, &rtup);
                    if (ret != RDB_OK)
                        return ret;
                    if (!rtup) {
                        qrp->endreached = RDB_TRUE;
                        RDB_raise_not_found("", ecp);
                        return RDB_ERROR;
                    }
                }
                /*
                 * Check condition, because it could be an open start
                 */
                ret = RDB_evaluate_bool(qrp->tbp->var.select.exp, tplp, ecp, txp,
                        &rtup);
                if (ret != RDB_OK)
                    return ret;
            }
        } while (!rtup);
    }

    return RDB_OK;
}

static int
next_project_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i, ret;
    RDB_object *valp;
    RDB_type *tpltyp = qrp->tbp->typ->var.basetyp;
            
    /* Get tuple */
    if (qrp->uses_cursor) {
        if (tplp != NULL) {
            ret = _RDB_get_by_cursor(qrp->tbp->var.project.tbp, qrp->var.curp,
                    tpltyp, tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        }
        ret = RDB_cursor_next(qrp->var.curp, 0);
        if (ret == DB_NOTFOUND)
            qrp->endreached = RDB_TRUE;
        else if (ret != RDB_OK)
            return ret;
    } else {
        RDB_object tpl;

        RDB_init_obj(&tpl);

        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }

        if (tplp != NULL) {
            /* Copy attributes into new tuple */
            for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
                char *attrnamp = tpltyp->var.tuple.attrv[i].name;

                valp = RDB_tuple_get(&tpl, attrnamp);
                RDB_tuple_set(tplp, attrnamp, valp, ecp);
            }
        }

        RDB_destroy_obj(&tpl, ecp);
    }
    return RDB_OK;
}

static int
next_rename_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    ret = RDB_rename_tuple(&tpl, qrp->tbp->var.rename.renc,
                           qrp->tbp->var.rename.renv, ecp, tplp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static int
next_wrap_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    ret = RDB_wrap_tuple(&tpl, qrp->tbp->var.wrap.wrapc,
                           qrp->tbp->var.wrap.wrapv, ecp, tplp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static int
next_unwrap_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    ret = RDB_unwrap_tuple(&tpl, qrp->tbp->var.unwrap.attrc,
                           qrp->tbp->var.unwrap.attrv, ecp, tplp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static int
destroy_qresult(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (qrp->tbp != NULL && qrp->tbp->kind == RDB_TB_REAL
            && qrp->tbp->stp == NULL) {
        /* Real table w/o physical rep */
        return RDB_OK;
    }

    if (qrp->uses_cursor) {
        ret = RDB_destroy_cursor(qrp->var.curp);
    } else if (qrp->var.virtual.qrp != NULL) {
        ret = _RDB_drop_qresult(qrp->var.virtual.qrp, ecp, txp);
        if (qrp->var.virtual.qr2p != NULL)
            _RDB_drop_qresult(qrp->var.virtual.qr2p, ecp, txp);
        if (qrp->tbp->kind == RDB_TB_JOIN || qrp->tbp->kind == RDB_TB_UNGROUP) {
            if (qrp->var.virtual.tpl_valid)
                RDB_destroy_obj(&qrp->var.virtual.tpl, ecp);
        }
    } else {
        ret = RDB_OK;
    }

    if (qrp->matp != NULL)
        RDB_drop_table(qrp->matp, ecp, txp);
    return ret;
}

/*
 * Given T1 DIVIDE T2 BY T3, tplp representing a tuple from T1.
 * Return RDB_OK if the tuple is an element of the result,
 * RDB_NOT_FOUND if not.
 * If qr2p is not NULL, it points to a RDB_qresult containing
 * the tuples from T3.
 */
int
_RDB_sdivide_preserves(RDB_table *tbp, const RDB_object *tplp,
        RDB_qresult *qr3p, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_bool *resultp)
{
    int ret;
    int i;
    RDB_object tpl2;
    RDB_qresult qr;
    RDB_bool matchall = RDB_TRUE;

    /*
     * Join this tuple with all tuples from table 2 and set matchall to RDB_FALSE
     * if not all the result tuples are an element of table 3
     */

    ret = init_qresult(&qr, tbp->var.sdivide.tb2p, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    for (;;) {
        RDB_bool b;
        RDB_type *tb1tuptyp = tbp->var.sdivide.tb1p->typ->var.basetyp;
        RDB_bool match = RDB_TRUE;

        RDB_init_obj(&tpl2);

        ret = _RDB_next_tuple(&qr, &tpl2, ecp, txp);
        if (ret != RDB_OK)
            break;

        /* Join *tplp and tpl2 into tpl2 */
        for (i = 0; i < tb1tuptyp->var.tuple.attrc; i++) {
             RDB_object *objp = RDB_tuple_get(tplp,
                     tb1tuptyp->var.tuple.attrv[i].name);
             RDB_object *dstobjp = RDB_tuple_get(&tpl2,
                     tb1tuptyp->var.tuple.attrv[i].name);

             if (dstobjp != NULL) {
                 RDB_bool b;

                 ret = RDB_obj_equals(objp, dstobjp, ecp, txp, &b);
                 if (ret != RDB_OK) {
                     destroy_qresult(&qr, ecp, txp);
                     RDB_destroy_obj(&tpl2, ecp);
                     return ret;
                 }
                 if (!b) {                     
                     match = RDB_FALSE;
                     break;
                 }
             } else {
                 ret = RDB_tuple_set(&tpl2,
                         tb1tuptyp->var.tuple.attrv[i].name, objp, ecp);
                 if (ret != RDB_OK) {
                     destroy_qresult(&qr, ecp, txp);
                     RDB_destroy_obj(&tpl2, ecp);
                     return ret;
                 }
             }
        }
        if (!match)
            continue;

        if (qr3p != NULL) {
            ret = _RDB_qresult_contains(qr3p, &tpl2, ecp, txp, &b);
        } else {
            ret = RDB_table_contains(tbp->var.sdivide.tb3p, &tpl2, ecp, txp,
                    &b);
        }
        RDB_destroy_obj(&tpl2, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        if (!b) {
            matchall = RDB_FALSE;
            break;
        }
    }
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
            destroy_qresult(&qr, ecp, txp);
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);
    }

    ret = destroy_qresult(&qr, ecp, txp);
    if (ret != RDB_OK)
        return ret;

    *resultp = matchall;
    return RDB_OK;
}

static int
next_sdivide_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    do {
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, ecp, txp);
        if (ret != RDB_OK) {
            return ret;
        }

        ret = _RDB_sdivide_preserves(qrp->tbp, tplp, qrp->var.virtual.qr2p,
                ecp, txp, &b);
        if (ret != RDB_OK)
            return ret;
    } while (!b);

    return RDB_OK;
}

static int
next_select_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool expres;

    do {
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            break;
        ret = RDB_evaluate_bool(qrp->tbp->var.select.exp, tplp, ecp, txp, &expres);
        if (ret != RDB_OK)
            break;
    } while (!expres);

    return ret;
}

int
_RDB_next_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool b;
    RDB_table *tbp = qrp->tbp;

    if (qrp->endreached) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (tbp == NULL) {
        /* It's a sorter */
        return next_stored_tuple(qrp, qrp->matp, tplp, RDB_TRUE, RDB_FALSE,
                qrp->matp->typ->var.basetyp, ecp, txp);
    }

    do {
        RDB_clear_err(ecp);
        switch (tbp->kind) {
            case RDB_TB_REAL:
                return next_stored_tuple(qrp, qrp->tbp, tplp, RDB_TRUE, RDB_FALSE,
                        qrp->tbp->typ->var.basetyp, ecp, txp);
            case RDB_TB_SELECT:
                if (qrp->tbp->var.select.objpc > 0)
                    ret = next_select_index(qrp, tplp, ecp, txp);
                else
                    ret = next_select_tuple(qrp, tplp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;                
                break;
            case RDB_TB_UNION:
                if (qrp->var.virtual.qr2p == NULL) {
                    ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, ecp, txp);
                    if (ret != RDB_OK) {
                        if (RDB_obj_type(RDB_get_err(ecp))
                                == &RDB_NOT_FOUND_ERROR) {
                            /* Switch to second table */
                            ret = _RDB_table_qresult(tbp->var._union.tb2p,
                                    ecp, txp, &qrp->var.virtual.qr2p);
                            if (ret != RDB_OK)
                                return ret;
                            ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp,
                                    ecp, txp);
                            if (ret != RDB_OK)
                                return ret;
                        } else {
                            return RDB_ERROR;
                        }
                    }
                } else {
                    ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp, ecp,
                            txp);
                    if (ret != RDB_OK)
                        return ret;
                }
                break;
            case RDB_TB_MINUS:
                do {
                    ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, ecp, txp);
                    if (ret != RDB_OK)
                        return ret;
                    ret = _RDB_qresult_contains(qrp->var.virtual.qr2p, tplp,
                            ecp, txp, &b);
                    if (ret != RDB_OK) {
                        return ret;
                    }
                } while (b);
                break;
            case RDB_TB_INTERSECT:
                do {
                    ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, ecp, txp);
                    if (ret != RDB_OK)
                        return ret;
                    ret = _RDB_qresult_contains(qrp->var.virtual.qr2p, tplp,
                            ecp, txp, &b);
                    if (ret != RDB_OK) {
                        return ret;
                    }
                } while (!b);
                break;
            case RDB_TB_JOIN:
                if (tbp->var.join.tb2p->kind != RDB_TB_PROJECT
                        || tbp->var.join.tb2p->var.project.indexp == NULL) {
                    ret = next_join_tuple(qrp, tplp, ecp, txp);
                    if (ret != RDB_OK)
                        return ret;
                    break;
                }
                if (tbp->var.join.tb2p->var.project.indexp->unique) {
                    ret = next_join_tuple_uix(qrp, tplp, ecp, txp);
                    if (ret != RDB_OK)
                        return ret;
                    break;
                }
                ret = next_join_tuple_nuix(qrp, tplp, ecp, txp);            
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_TB_EXTEND:
                ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = RDB_extend_tuple(tplp, tbp->var.extend.attrc,
                         tbp->var.extend.attrv, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_TB_PROJECT:
                ret = next_project_tuple(qrp, tplp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_TB_SUMMARIZE:
                {
                    int i;
                    char *cname;
                    RDB_int count;
                
                    ret = next_stored_tuple(qrp, qrp->matp, tplp, RDB_TRUE,
                            RDB_FALSE, qrp->tbp->typ->var.basetyp, ecp, txp);
                    if (ret != RDB_OK)
                        return RDB_ERROR;
                    /* check AVG counts */
                    for (i = 0; i < qrp->tbp->var.summarize.addc; i++) {
                        RDB_summarize_add *summp = &qrp->tbp->var.summarize.addv[i];
                        if (summp->op == RDB_AVG) {
                            cname = malloc(strlen(summp->name) + 3);
                            if (cname == NULL) {
                                RDB_raise_no_memory(ecp);
                                return RDB_ERROR;
                            }
                            strcpy (cname, summp->name);
                            strcat (cname, AVG_COUNT_SUFFIX);
                            count = RDB_tuple_get_int(tplp, cname);
                            free(cname);
                            if (count == 0) {
                                RDB_raise_aggregate_undefined(ecp);
                                return RDB_ERROR;
                            }
                        }
                    }
                }
                break;
            case RDB_TB_RENAME:
                ret = next_rename_tuple(qrp, tplp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_TB_WRAP:
                ret = next_wrap_tuple(qrp, tplp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_TB_UNWRAP:
                ret = next_unwrap_tuple(qrp, tplp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_TB_GROUP:
                ret = next_stored_tuple(qrp, qrp->matp, tplp, RDB_TRUE, RDB_FALSE,
                        qrp->tbp->typ->var.basetyp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_TB_UNGROUP:
                ret = next_ungroup_tuple(qrp, tplp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
            case RDB_TB_SDIVIDE:
                ret = next_sdivide_tuple(qrp, tplp, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
                break;
        }

        /* Check for duplicate, if necessary */
        if (qrp->matp != NULL && tbp->kind != RDB_TB_SUMMARIZE
                && tbp->kind != RDB_TB_GROUP) {
            ret = _RDB_insert_real(qrp->matp, tplp, ecp, txp);
            if (ret != RDB_OK && RDB_obj_type(RDB_get_err(ecp))
                    != &RDB_ELEMENT_EXISTS_ERROR) {
                return RDB_ERROR;
            }
        } else {
            ret = RDB_OK;
        }
    } while (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp))
            == &RDB_ELEMENT_EXISTS_ERROR);
    return RDB_OK;
}

int
_RDB_reset_qresult(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_table *tbp = qrp->tbp;

    if (tbp == NULL || qrp->tbp->kind == RDB_TB_REAL
            || qrp->tbp->kind == RDB_TB_SUMMARIZE) {
        /* Sorter, stored table or SUMMARIZE PER - reset cursor */
        ret = RDB_cursor_first(qrp->var.curp);
        if (ret == DB_NOTFOUND) {
            qrp->endreached = RDB_TRUE;
            ret = RDB_OK;
        } else {
            qrp->endreached = RDB_FALSE;
        }
        return ret;
    }
    ret = destroy_qresult(qrp, ecp, txp);
    if (ret != RDB_OK)
        return ret;
    return init_qresult(qrp, tbp, ecp, txp);
}

int
_RDB_qresult_contains(RDB_qresult *qrp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    int i;
    int ret;
    RDB_object **objpv;
    int kattrc;
    RDB_object tpl;

    if (qrp->tbp->kind != RDB_TB_SUMMARIZE && qrp->tbp->kind != RDB_TB_GROUP)
        return RDB_table_contains(qrp->tbp, tplp, ecp, txp, resultp);

    /*
     * Check if the table contains the tuple by
     * getting the non-key attributes and comparing them.
     * (RDB_recmap_contains() cannot be used in all cases because
     * of the additional count attributes for AVG).
     */

    kattrc = _RDB_pkey_len(qrp->tbp);
    objpv = malloc(sizeof (RDB_object *) * kattrc);
    if (objpv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    for (i = 0; i < kattrc; i++) {
        RDB_object *attrobjp = RDB_tuple_get(tplp, qrp->matp->keyv[0].strv[i]);

        if (attrobjp == NULL) {
            RDB_raise_invalid_argument("invalid key", ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        objpv[i] = attrobjp;
    }
        
    ret = _RDB_get_by_uindex(qrp->matp, objpv, &qrp->matp->stp->indexv[0],
            qrp->matp->typ->var.basetyp, ecp, txp, &tpl);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            *resultp = RDB_FALSE;
            RDB_clear_err(ecp);
            ret = RDB_OK;
        }
        goto cleanup;
    }

    if (qrp->tbp->kind == RDB_TB_SUMMARIZE) {
        /* compare ADD attributes */
        for (i = 0; i < qrp->tbp->var.summarize.addc; i++) {
            char *attrname = qrp->tbp->var.summarize.addv[i].name;

            ret = RDB_obj_equals(RDB_tuple_get(tplp, attrname),
                    RDB_tuple_get(&tpl, attrname), ecp, txp, resultp);
            if (ret != RDB_OK || !*resultp) {
                goto cleanup;
            }
        }
    } else {
        char *attrname = qrp->tbp->var.group.gattr;

        ret = RDB_obj_equals(RDB_tuple_get(tplp, attrname),
                RDB_tuple_get(&tpl, attrname), ecp, txp, resultp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
        if (!*resultp) { /* !! superfluous? */
            goto cleanup;
        }
    }

    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    free(objpv);
    
    return ret;
}

int
_RDB_drop_qresult(RDB_qresult *qrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret = destroy_qresult(qrp, ecp, txp);

    free(qrp);

    return ret;
}
