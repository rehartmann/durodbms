/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "typeimpl.h"
#include <gen/hashmapit.h>
#include <gen/strfns.h>
#include <string.h>

static int
init_summ_table(RDB_qresult *qresp, RDB_transaction *txp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /*
     * Initialize table from table #2
     */

    ret = _RDB_table_qresult(qresp->tbp->var.summarize.tb2p, txp, &qrp);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&tpl);
    for(;;) {
        int i;

        ret = _RDB_next_tuple(qrp, &tpl, txp);
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
                    ret = RDB_tuple_set_int(&tpl, name, 0);
                    break;
                case RDB_AVG:
                case RDB_AVGD:
                    ret = RDB_tuple_set_rational(&tpl, name, 0);
                    if (ret != RDB_OK)
                       break;
                    cname = malloc(strlen(name) + 3);
                    if (cname == NULL) {
                        ret = RDB_NO_MEMORY;
                        break;
                    }
                    strcpy(cname, name);
                    strcat(cname, AVG_COUNT_SUFFIX);
                    ret = RDB_tuple_set_int(&tpl, cname, 0);
                    free(cname);
                    break;
                case RDB_SUM:
                case RDB_SUMD:
                    ret = RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp,
                            &typ);
                    if (ret != RDB_OK)
                        goto error;
                    if (typ == &RDB_INTEGER)
                        ret = RDB_tuple_set_int(&tpl, name, 0);
                    else
                        ret = RDB_tuple_set_rational(&tpl, name, 0.0);
                    break;
                case RDB_MAX:
                    ret = RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp,
                            &typ);
                    if (ret != RDB_OK)
                        goto error;
                    if (typ == &RDB_INTEGER)
                        ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MIN);
                    else
                        ret = RDB_tuple_set_rational(&tpl, name, RDB_RATIONAL_MIN);
                    break;
                case RDB_MIN:
                    ret = RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp,
                            &typ);
                    if (ret != RDB_OK)
                        goto error;
                    if (typ == &RDB_INTEGER)
                        ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MAX);
                    else
                        ret = RDB_tuple_set_rational(&tpl, name, RDB_RATIONAL_MAX);
                    break;
                case RDB_ALL:
                    ret = RDB_tuple_set_bool(&tpl, name, RDB_TRUE);
                    break;
                case RDB_ANY:
                    ret = RDB_tuple_set_bool(&tpl, name, RDB_FALSE);
                    break;
            }
            if (ret != RDB_OK)
                goto error;
        } /* for */
        ret = RDB_insert(qresp->matp, &tpl, txp);
        if (ret != RDB_OK)
            goto error;
    };

    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;

error:
    RDB_destroy_obj(&tpl);
    _RDB_drop_qresult(qrp, txp);

    return ret;
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
do_summarize(RDB_qresult *qresp, RDB_transaction *txp)
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
        return RDB_NO_MEMORY;
    }

    /*
     * Iterate over table 1, modifying the materialized table
     */

    ret = _RDB_table_qresult(qresp->tbp->var.summarize.tb1p, txp, &qrp);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&addval);
    for (i = 0; i < addc; i++)
        RDB_init_obj(&svalv[i].val);
    do {
        ret = _RDB_next_tuple(qrp, &tpl, txp);
        if (ret == RDB_OK) {
            int ai;

            /* Build key */
            for (i = 0; i < keyfc; i++) {
                ret = _RDB_obj_to_field(&keyfv[i],
                        RDB_tuple_get(&tpl, qresp->tbp->keyv[0].strv[i]));
                if (ret != RDB_OK)
                    return ret;
            }

            /* Read added attributes from table #2 */
            ai = 0;
            for (i = 0; i < addc; i++) {
                char *attrname = qresp->tbp->var.summarize.addv[i].name;

                nonkeyfv[i].no = *(RDB_int *)RDB_hashmap_get(
                        &qresp->matp->var.stored.attrmap, attrname, NULL);
                if (qresp->tbp->var.summarize.addv[i].op == RDB_AVG) {
                    char *cattrname = malloc(strlen(attrname) + 3);
                    if (cattrname == NULL) {
                        ret = RDB_NO_MEMORY;
                        goto cleanup;
                    }
                    strcpy(cattrname, attrname);
                    strcat(cattrname, AVG_COUNT_SUFFIX);
                    nonkeyfv[addc + ai].no = *(RDB_int *)RDB_hashmap_get(
                        &qresp->matp->var.stored.attrmap, cattrname, NULL);
                    free(cattrname);
                    svalv[i].fvidx = addc + ai;
                    ai++;
                }
            }
            ret = RDB_get_fields(qresp->matp->var.stored.recmapp, keyfv,
                    addc + avgc, qresp->matp->is_persistent ? txp->txid : NULL,
                    nonkeyfv);
            if (ret == RDB_OK) {
                /* A corresponding tuple in table 2 has been found */
                for (i = 0; i < addc; i++) {
                    RDB_summarize_add *summp = &qresp->tbp->var.summarize.addv[i];
                    RDB_type *typ;

                    if (summp->op == RDB_COUNT) {
                        ret = RDB_irep_to_obj(&svalv[i].val, &RDB_INTEGER,
                                nonkeyfv[i].datap, nonkeyfv[i].len);
                    } else {
                        ret = RDB_expr_type(summp->exp,
                                qresp->tbp->var.summarize.tb1p->typ->var.basetyp,
                                &typ);
                        if (ret != RDB_OK)
                            goto cleanup;
                        ret = RDB_irep_to_obj(&svalv[i].val, typ,
                                nonkeyfv[i].datap, nonkeyfv[i].len);
                        if (ret != RDB_OK)
                            goto cleanup;
                        ret = RDB_evaluate(summp->exp, &tpl, txp, &addval);
                        if (ret != RDB_OK)
                            goto cleanup;
                        /* If it's AVG, get count */
                        if (summp->op == RDB_AVG) {
                            memcpy(&svalv[i].count, nonkeyfv[svalv[i].fvidx].datap,
                                    sizeof(RDB_int));
                        }
                    }
                    summ_step(&svalv[i], &addval, summp->op);

                    ret = _RDB_obj_to_field(&nonkeyfv[i], &svalv[i].val);
                    if (ret != RDB_OK)
                        return ret;

                    /* If it's AVG, store count */
                    if (summp->op == RDB_AVG) {
                        nonkeyfv[svalv[i].fvidx].datap = &svalv[i].count;
                        nonkeyfv[svalv[i].fvidx].len = sizeof(RDB_int);
                        nonkeyfv[svalv[i].fvidx].copyfp = memcpy;
                    }
                }
                ret = RDB_update_rec(qresp->matp->var.stored.recmapp, keyfv,
                        addc + avgc, nonkeyfv,
                        qresp->matp->is_persistent ? txp->txid : NULL);
                if (ret != RDB_OK) {
                    goto cleanup;
                }
            } else if (ret != RDB_NOT_FOUND)
                goto cleanup;
        }
    } while (ret == RDB_OK);

    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&addval);
    for (i = 0; i < addc; i++)
        RDB_destroy_obj(&svalv[i].val);
    _RDB_drop_qresult(qrp, txp);
    free(keyfv);
    free(nonkeyfv);
    free(svalv);

    return ret;
}

static int
do_group(RDB_qresult *qrp, RDB_transaction *txp)
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
        return RDB_NO_MEMORY;
    }

    /*
     * Iterate over the original table, modifying the materialized table
     */

    ret = _RDB_table_qresult(qrp->tbp->var.group.tbp, txp, &newqrp);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&gval);
    do {
        ret = _RDB_next_tuple(newqrp, &tpl, txp);
        if (ret == RDB_OK) {
            /* Build key */
            for (i = 0; i < keyfc; i++) {
                ret = _RDB_obj_to_field(&keyfv[i],
                        RDB_tuple_get(&tpl, qrp->tbp->keyv[0].strv[i]));
                if (ret != RDB_OK)
                    goto cleanup;
            }

            gfield.no = *(RDB_int *)RDB_hashmap_get(
                    &qrp->matp->var.stored.attrmap,
                    qrp->tbp->var.group.gattr, NULL);

            /* Try to read tuple of the materialized table */
            ret = RDB_get_fields(qrp->matp->var.stored.recmapp, keyfv,
                    1, qrp->matp->is_persistent ? txp->txid : NULL,
                    &gfield);
            if (ret == RDB_OK) {
                /*
                 * A tuple has been found, add read tuple to
                 * relation-valued attribute
                 */

                /* Get relation-valued attribute */
                ret = RDB_irep_to_obj(&gval, greltyp, gfield.datap, gfield.len);
                if (ret != RDB_OK)
                    goto cleanup;

                /* Insert tuple (not-grouped attributes will be ignored) */
                ret = RDB_insert(RDB_obj_table(&gval), &tpl, txp);
                if (ret != RDB_OK)
                    goto cleanup;

                /* Update materialized table */
                ret = _RDB_obj_to_field(&gfield, &gval);
                if (ret != RDB_OK)
                    goto cleanup;
                ret = RDB_update_rec(qrp->matp->var.stored.recmapp, keyfv,
                        1, &gfield,
                        qrp->matp->is_persistent ? txp->txid : NULL);
                if (ret != RDB_OK)
                    goto cleanup;
            } else if (ret == RDB_NOT_FOUND) {
                /*
                 * A tuple has been found, build tuple and insert it
                 */
                RDB_table *gtbp;

                ret = RDB_create_table(NULL, RDB_FALSE,
                               greltyp->var.basetyp->var.tuple.attrc,
                               greltyp->var.basetyp->var.tuple.attrv,
                               0, NULL, NULL, &gtbp);
                if (ret != RDB_OK)
                    goto cleanup;

                ret = RDB_insert(gtbp, &tpl, NULL);
                if (ret != RDB_OK)
                    goto cleanup;

                RDB_table_to_obj(&gval, gtbp);
                ret = RDB_tuple_set(&tpl, qrp->tbp->var.group.gattr, &gval);
                if (ret != RDB_OK)
                    goto cleanup;

                ret = RDB_insert(qrp->matp, &tpl, NULL);
                if (ret != RDB_OK)
                    goto cleanup;
            } else {
                goto cleanup;
            }
        }
    } while (ret == RDB_OK);

    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&gval);
    _RDB_drop_qresult(newqrp, txp);
    free(keyfv);

    return ret;
}

static int
stored_qresult(RDB_qresult *qresp, RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    /* !! delay after first call to _RDB_qresult_next()? */
    ret = RDB_recmap_cursor(&qresp->var.curp, tbp->var.stored.recmapp,
                    RDB_FALSE, tbp->is_persistent ? txp->txid : NULL);
    if (ret != RDB_OK) {
        if (txp != NULL) {
            RDB_errmsg(txp->dbp->dbrootp->envp, "cannot create cursor: %s",
                    RDB_strerror(ret));
        }
        return ret;
    }
    ret = RDB_cursor_first(qresp->var.curp);
    if (ret == RDB_NOT_FOUND) {
        qresp->endreached = RDB_TRUE;
        ret = RDB_OK;
    }
    return ret;
}

static int
select_index_qresult(RDB_qresult *qrp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_field *fv;
    int flags = 0;

    /*
     * If the index is unique, there is nothing to do
     */
    if (qrp->tbp->var.select.indexp->unique)
        return RDB_OK;

    fv  = malloc(sizeof (RDB_field) * qrp->tbp->var.select.indexp->attrc);

    if (fv == NULL)
        return RDB_NO_MEMORY;

    ret = RDB_index_cursor(&qrp->var.curp, qrp->tbp->var.select.indexp->idxp,
            RDB_FALSE, qrp->tbp->var.select.tbp->is_persistent ? txp->txid : NULL);
    if (ret != RDB_OK) {
        if (txp != NULL) {
            RDB_errmsg(txp->dbp->dbrootp->envp, "cannot create cursor: %s",
                    RDB_strerror(ret));
        }
        free(fv);
        return ret;
    }

    for (i = 0; i < qrp->tbp->var.select.objpc; i++) {
        ret = _RDB_obj_to_field(&fv[i], qrp->tbp->var.select.objpv[i]);
        if (ret != RDB_OK) {
            free(fv);
            return ret;
        }
    }
    for (i = qrp->tbp->var.select.objpc; i < qrp->tbp->var.select.indexp->attrc;
            i++) {
        fv[i].len = 0;
    }

    if (qrp->tbp->var.select.objpc != qrp->tbp->var.select.indexp->attrc
            || !qrp->tbp->var.select.all_eq)
        flags = RDB_REC_RANGE;

    ret = RDB_cursor_seek(qrp->var.curp, qrp->tbp->var.select.objpc, fv, flags);
    if (ret == RDB_NOT_FOUND) {
        qrp->endreached = RDB_TRUE;
        ret = RDB_OK;
    }

    free(fv);
    return ret;
}

static int
summarize_qresult(RDB_qresult *qresp, RDB_transaction *txp)
{
    int ret;
    RDB_type *tuptyp = qresp->tbp->typ->var.basetyp;

    /* Need keys */
    if (qresp->tbp->keyv == NULL) {
        ret = RDB_table_keys(qresp->tbp, NULL);
        if (ret < 0)
            return ret;
    }

    /* create materialized table */
    ret = _RDB_create_table(NULL, RDB_FALSE,
                        tuptyp->var.tuple.attrc,
                        tuptyp->var.tuple.attrv,
                        qresp->tbp->keyc, qresp->tbp->keyv,
                        NULL, &qresp->matp);
    if (ret != RDB_OK)
        return ret;

    ret = init_summ_table(qresp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, txp);
        return ret;
    }

    /* summarize over table 1 */
    ret = do_summarize(qresp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, txp);
        return ret;
    }

    ret = stored_qresult(qresp, qresp->matp, txp);

    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, txp);
        return ret;
    }

    return RDB_OK;
}

static int
group_qresult(RDB_qresult *qresp, RDB_transaction *txp)
{
    RDB_type *tuptyp = qresp->tbp->typ->var.basetyp;
    int ret;

    /* Need keys */
    if (qresp->tbp->keyv == NULL) {
        ret = RDB_table_keys(qresp->tbp, NULL);
        if (ret < 0)
            return ret;
    }

    /* create materialized table */
    ret = RDB_create_table(NULL, RDB_FALSE,
                        tuptyp->var.tuple.attrc, tuptyp->var.tuple.attrv,
                        qresp->tbp->keyc, qresp->tbp->keyv,
                        NULL, &qresp->matp);
    if (ret != RDB_OK)
        return ret;

    /* do the grouping */
    ret = do_group(qresp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, txp);
        return ret;
    }

    ret = stored_qresult(qresp, qresp->matp, txp);

    if (ret != RDB_OK) {
        RDB_drop_table(qresp->matp, txp);
        return ret;
    }

    return RDB_OK;
}

static int
join_qresult(RDB_qresult *qrp, RDB_transaction *txp)
{
    /* Create qresult for the first table */
    int ret = _RDB_table_qresult(qrp->tbp->var.join.tb1p,
            txp, &qrp->var.virtual.qrp);
    if (ret != RDB_OK)
        return ret;

    qrp->var.virtual.tpl_valid = RDB_FALSE;
    if (qrp->tbp->var.join.indexp == NULL) {
        /* Create qresult for 2nd table */
        ret = _RDB_table_qresult(qrp->tbp->var.join.tb2p,
                txp, &qrp->var.virtual.qr2p);
        if (ret != RDB_OK) {
            _RDB_drop_qresult(qrp->var.virtual.qr2p, txp);
            return ret;
        }
    } else if (!qrp->tbp->var.join.indexp->unique) {
        /* Create index qresult for 2nd table */
        qrp->var.virtual.qr2p = malloc(sizeof (RDB_qresult));
        if (qrp->var.virtual.qr2p == NULL)
            return RDB_NO_MEMORY;          
        qrp->var.virtual.qr2p->tbp = qrp->tbp->var.join.tb2p;
        qrp->var.virtual.qr2p->endreached = RDB_FALSE;
        qrp->var.virtual.qr2p->matp = NULL;

        ret = RDB_index_cursor(&qrp->var.virtual.qr2p->var.curp,
                qrp->tbp->var.join.indexp->idxp, RDB_FALSE,
                qrp->tbp->var.join.tb2p->is_persistent ? txp->txid : NULL);
        if (ret != RDB_OK) {
            free(qrp->var.virtual.qr2p);
            if (txp != NULL) {
                RDB_errmsg(txp->dbp->dbrootp->envp, "cannot create cursor: %s",
                        RDB_strerror(ret));
            }
            return ret;
        }
    }
    return RDB_OK;
}

static int
init_qresult(RDB_qresult *qrp, RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    qrp->tbp = tbp;
    qrp->endreached = RDB_FALSE;
    qrp->matp = NULL;
    qrp->var.virtual.qr2p = NULL;

    if (tbp->kind != RDB_TB_STORED) {
        if (!tbp->optimized) {
            ret = _RDB_optimize(tbp, txp);
            if (ret != RDB_OK)
                return ret;
        }
    }

    switch (tbp->kind) {
        case RDB_TB_STORED:
            ret = stored_qresult(qrp, tbp, txp);
            break;
        case RDB_TB_SELECT:
            ret = _RDB_table_qresult(tbp->var.select.tbp,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_SELECT_INDEX:
            ret = select_index_qresult(qrp, txp);
            break;
        case RDB_TB_UNION:
            ret = _RDB_table_qresult(tbp->var._union.tb1p,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_MINUS:
            ret = _RDB_table_qresult(tbp->var.minus.tb1p,
                    txp, &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.minus.tb2p,
                    txp, &qrp->var.virtual.qr2p);
            break;
        case RDB_TB_INTERSECT:
            ret = _RDB_table_qresult(tbp->var.intersect.tb1p,
                    txp, &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.minus.tb2p,
                    txp, &qrp->var.virtual.qr2p);
            break;
        case RDB_TB_JOIN:
            ret = join_qresult(qrp, txp);
            break;
        case RDB_TB_EXTEND:
            ret = _RDB_table_qresult(tbp->var.extend.tbp,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_PROJECT:
            /* Need keys */
            if (tbp->keyv == NULL) {
                ret = RDB_table_keys(tbp, NULL);
                if (ret < 0)
                    break;
            }
            if (tbp->var.project.keyloss) {
                RDB_string_vec keyattrs;
                int i;
                RDB_type *tuptyp = tbp->typ->var.basetyp;

                keyattrs.strc = tuptyp->var.tuple.attrc;
                keyattrs.strv = malloc(sizeof (char *) * keyattrs.strc);

                for (i = 0; i < keyattrs.strc; i++)
                    keyattrs.strv[i] = tuptyp->var.tuple.attrv[i].name;

                /* Create materialized (all-key) table */
                ret = RDB_create_table(NULL, RDB_FALSE,
                        tuptyp->var.tuple.attrc,
                        tuptyp->var.tuple.attrv,
                        1, &keyattrs, NULL, &qrp->matp);
                free(keyattrs.strv);
                if (ret != RDB_OK)
                    break;
            }

            ret = _RDB_table_qresult(tbp->var.project.tbp,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_SUMMARIZE:
            ret = summarize_qresult(qrp, txp);
            break;
        case RDB_TB_RENAME:
            ret = _RDB_table_qresult(tbp->var.rename.tbp,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_WRAP:
            ret = _RDB_table_qresult(tbp->var.wrap.tbp,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_UNWRAP:
            ret = _RDB_table_qresult(tbp->var.unwrap.tbp,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_GROUP:
            ret = group_qresult(qrp, txp);
            break;
        case RDB_TB_UNGROUP:
            ret = _RDB_table_qresult(tbp->var.ungroup.tbp,
                    txp, &qrp->var.virtual.qrp);
            qrp->var.virtual.tpl_valid = RDB_FALSE;
            break;
        case RDB_TB_SDIVIDE:
            /* Create qresults for table 1 and table 3 */
            ret = _RDB_table_qresult(tbp->var.sdivide.tb1p,
                    txp, &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.sdivide.tb3p,
                    txp, &qrp->var.virtual.qr2p);
            if (ret != RDB_OK) {
                _RDB_drop_qresult(qrp->var.virtual.qr2p, txp);
                break;
            }
    }
    return ret;
}

int
_RDB_table_qresult(RDB_table *tbp, RDB_transaction *txp, RDB_qresult **qrpp)
{
    int ret;

    *qrpp = malloc(sizeof (RDB_qresult));
    if (*qrpp == NULL)
        return RDB_NO_MEMORY;
    ret = init_qresult(*qrpp, tbp, txp);
    if (ret != RDB_OK)
        free(*qrpp);
    return ret;
}

/*
 * Creates a qresult which sorts a table.
 */
int
_RDB_sorter(RDB_table *tbp, RDB_qresult **qrespp, RDB_transaction *txp,
            int seqitc, const RDB_seq_item seqitv[])
{
    RDB_string_vec key;
    RDB_bool *ascv = NULL;
    int ret;
    int i;
    RDB_qresult *tmpqrp;
    RDB_object tpl;
    RDB_qresult *qresp = malloc(sizeof (RDB_qresult));

    if (qresp == NULL) {
        return RDB_NO_MEMORY;
    }

    key.strc = seqitc;
    key.strv = malloc(sizeof (char *) * seqitc);
    if (key.strv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    ascv = malloc(sizeof (RDB_bool) * seqitc);
    if (ascv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    qresp->tbp = NULL;
    qresp->matp = NULL;
    qresp->endreached = RDB_FALSE;

    for (i = 0; i < seqitc; i++) {
        key.strv[i] = seqitv[i].attrname;
        ascv[i] = seqitv[i].asc;
    }

    /*
     * Create a sorted RDB_table
     */

    ret = _RDB_new_stored_table(NULL, RDB_FALSE,
            tbp->typ->var.basetyp->var.tuple.attrc,
            tbp->typ->var.basetyp->var.tuple.attrv, 1, &key, RDB_TRUE,
            &qresp->matp);
    if (ret != RDB_OK)
        goto error;

    ret = _RDB_create_table_storage(qresp->matp, txp->dbp->dbrootp->envp,
            ascv, txp);
    if (ret != RDB_OK)
        goto error;

    /*
     * Copy tuples into the newly created RDB_table
     */

    ret = _RDB_table_qresult(tbp, txp, &tmpqrp);
    if (ret != RDB_OK)
        goto error;

    RDB_init_obj(&tpl);
    while ((ret = _RDB_next_tuple(tmpqrp, &tpl, txp)) == RDB_OK) {
        int ret2 = RDB_insert(qresp->matp, &tpl, txp);
        if (ret2 != RDB_OK) {
            RDB_destroy_obj(&tpl);
            ret = ret2;
            goto error;
        }
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND)
        goto error;
    ret = _RDB_drop_qresult(tmpqrp, txp);
    if (ret != RDB_OK)
        goto error;

    ret = stored_qresult(qresp, qresp->matp, txp);
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
        RDB_drop_table(qresp->matp, NULL);
    free(qresp);

    return ret;
}

int
_RDB_get_by_cursor(RDB_table *tbp, RDB_cursor *curp, RDB_object *tplp)
{
    int i;
    int ret;
    RDB_object val;
    RDB_int fno;
    RDB_attr *attrp;
    void *datap;
    size_t len;
    RDB_type *tuptyp = tbp->typ->var.basetyp;

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        attrp = &tuptyp->var.tuple.attrv[i];

        fno = *(RDB_int *)RDB_hashmap_get(&tbp->var.stored.attrmap,
                                      attrp->name, NULL);
        ret = RDB_cursor_get(curp, fno, &datap, &len);
        if (ret != RDB_OK) {
            return ret;
        }
        RDB_init_obj(&val);
        ret = RDB_irep_to_obj(&val, attrp->typ, datap, len);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&val);
            return ret;
        }
        ret = RDB_tuple_set(tplp, attrp->name, &val);
        RDB_destroy_obj(&val);
        if (ret != RDB_OK) {
            return ret;
        }
    }
    return RDB_OK;
}

static int
next_stored_tuple(RDB_qresult *qrp, RDB_table *tbp, RDB_object *tplp,
        RDB_bool asc, RDB_bool dup)
{
    int ret;

    if (qrp->endreached)
        return RDB_NOT_FOUND;

    if (tplp != NULL) {
        ret = _RDB_get_by_cursor(tbp, qrp->var.curp, tplp);
        if (ret != RDB_OK)
            return ret;
    }
    if (asc) {
        ret = RDB_cursor_next(qrp->var.curp, dup ? RDB_REC_DUP : 0);
    } else {
        if (dup)
            return RDB_INVALID_ARGUMENT;
        ret = RDB_cursor_prev(qrp->var.curp);
    }
    if (ret == RDB_NOT_FOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    return ret;
}

static int
next_ungroup_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    RDB_table *attrtbp;
    RDB_hashmap_iter hiter;
    char *key;

    /* If no tuple has been read, read first tuple */
    if (!qrp->var.virtual.tpl_valid) {
        RDB_init_obj(&qrp->var.virtual.tpl);
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl, txp);
        if (ret != RDB_OK)
            return ret;
        qrp->var.virtual.tpl_valid = RDB_TRUE;
    }

    /* If there is no 2nd qresult, open it from relation-valued attribute */
    if (qrp->var.virtual.qr2p == NULL) {
        attrtbp = RDB_tuple_get(&qrp->var.virtual.tpl,
                qrp->tbp->var.ungroup.attr)->var.tbp;

        ret = _RDB_table_qresult(attrtbp, txp, &qrp->var.virtual.qr2p);
        if (ret != RDB_OK)
            return ret;
    }

    /* Read next element of relation-valued attribute */
    ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp, txp);
    while (ret == RDB_NOT_FOUND) {
        /* Destroy qresult over relation-valued attribute */
        ret = _RDB_drop_qresult(qrp->var.virtual.qr2p, txp);
        qrp->var.virtual.qr2p = NULL;
        if (ret != RDB_OK) {
            return ret;
        }

        /* Read next tuple */
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl, txp);
        if (ret != RDB_OK) {
            return ret;
        }

        attrtbp = RDB_tuple_get(&qrp->var.virtual.tpl,
                qrp->tbp->var.ungroup.attr)->var.tbp;

        /* Create qresult over relation-valued attribute */
        ret = _RDB_table_qresult(attrtbp, txp, &qrp->var.virtual.qr2p);
        if (ret != RDB_OK) {
            return ret;
        }

        ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp, txp);
    }
    if (ret != RDB_OK) {
        return ret;
    }

    /* Merge tuples, skipping the relation-valued attribute */
    RDB_init_hashmap_iter(&hiter, (RDB_hashmap *) &qrp->var.virtual.tpl.var.tpl_map);
    for (;;) {
        /* Get next attribute */
        RDB_object *srcattrp = (RDB_object *) RDB_hashmap_next(&hiter, &key,
                NULL);
        if (srcattrp == NULL)
            break;

        if (strcmp(key, qrp->tbp->var.ungroup.attr) != 0)
            RDB_tuple_set(tplp, key, srcattrp);
    }
    RDB_destroy_hashmap_iter(&hiter);

    return RDB_OK;
}

static int
next_join_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    int i;

    /* tuple type of first ('outer') table */
    RDB_type *tpltyp1 = qrp->tbp->var.join.tb1p->typ->var.basetyp;

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->var.virtual.tpl_valid) {
        RDB_init_obj(&qrp->var.virtual.tpl);
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl, txp);
        if (ret != RDB_OK)
            return ret;
        qrp->var.virtual.tpl_valid = RDB_TRUE;
    }
        
    for (;;) {
        /* read next 'inner' tuple */
        ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp, txp);
        if (ret != RDB_NOT_FOUND && ret != RDB_OK) {
            return ret;
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
                            RDB_tuple_get(tplp, attrname), &b);
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
        }

        /* reset nested qresult */
        ret = _RDB_reset_qresult(qrp->var.virtual.qr2p, txp);
        if (ret != RDB_OK) {
            return ret;
        }

        /* read next 'outer' tuple */
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl, txp);
        if (ret != RDB_OK) {
            return ret;
        }
    }
    
    /* join the two tuples into tplp */
    for (i = 0; i < tpltyp1->var.tuple.attrc; i++) {
         ret = RDB_tuple_set(tplp, tpltyp1->var.tuple.attrv[i].name,
                       RDB_tuple_get(&qrp->var.virtual.tpl,
                       tpltyp1->var.tuple.attrv[i].name));
         if (ret != RDB_OK) {
             return ret;
         }
    }

    return RDB_OK;
}

static int
seek_index_qresult(RDB_qresult *qrp, _RDB_tbindex *indexp,
        const RDB_object *tplp)
{
    int i;
    int ret;
    RDB_field *fv = malloc(sizeof (RDB_field) * indexp->attrc);
    if (fv == NULL) {
        return RDB_NO_MEMORY;
    }

    for (i = 0; i < indexp->attrc; i++) {
        ret = _RDB_obj_to_field(&fv[i],
                RDB_tuple_get(tplp, indexp->attrv[i].attrname));
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
next_join_tuple_nuix(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    int i;

    /* tuple type of first ('outer') table */
    RDB_type *tpltyp1 = qrp->tbp->var.join.tb1p->typ->var.basetyp;

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->var.virtual.tpl_valid) {
        RDB_init_obj(&qrp->var.virtual.tpl);
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl, txp);
        if (ret != RDB_OK)
            return ret;
        qrp->var.virtual.tpl_valid = RDB_TRUE;

        /* Set cursor position */
        ret = seek_index_qresult(qrp->var.virtual.qr2p,
                qrp->tbp->var.join.indexp, &qrp->var.virtual.tpl);
        if (ret != RDB_OK)
            return ret;
    }
        
    for (;;) {
        /* read next 'inner' tuple */
        ret = next_stored_tuple(qrp->var.virtual.qr2p,
                qrp->var.virtual.qr2p->tbp, tplp, RDB_TRUE, RDB_TRUE);
        if (ret != RDB_NOT_FOUND && ret != RDB_OK) {
            return ret;
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
                    for (j = 0; j < qrp->tbp->var.join.indexp->attrc
                            && strcmp(attrname,
                            qrp->tbp->var.join.indexp->attrv[j].attrname) != 0;
                            j++);
                    if (j >= qrp->tbp->var.join.indexp->attrc) {
                        ret = RDB_obj_equals(RDB_tuple_get(
                                &qrp->var.virtual.tpl, attrname),
                                RDB_tuple_get(tplp, attrname), &b);
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
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl, txp);
        if (ret != RDB_OK) {
            return ret;
        }

        /* reset cursor */
        ret = seek_index_qresult(qrp->var.virtual.qr2p,
                qrp->tbp->var.join.indexp, &qrp->var.virtual.tpl);
        if (ret != RDB_OK)
            return ret;
    }
    
    /* join the two tuples into tplp */
    for (i = 0; i < tpltyp1->var.tuple.attrc; i++) {
         ret = RDB_tuple_set(tplp, tpltyp1->var.tuple.attrv[i].name,
                       RDB_tuple_get(&qrp->var.virtual.tpl,
                       tpltyp1->var.tuple.attrv[i].name));
         if (ret != RDB_OK) {
             return ret;
         }
    }

    return RDB_OK;
}

static int
next_join_tuple_uix(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_bool match;
    RDB_object tpl;
    RDB_type *tpltyp;
    RDB_object **objpv;

    objpv = malloc(sizeof(RDB_object *)
            * qrp->tbp->var.join.indexp->attrc);
    if (objpv == NULL)
        return RDB_NO_MEMORY;

    RDB_init_obj(&tpl);

    do {
        match = RDB_TRUE;
    
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, txp);
        if (ret != RDB_OK)
            goto cleanup;

        for (i = 0; i < qrp->tbp->var.join.indexp->attrc; i++) {
            objpv[i] = RDB_tuple_get(tplp,
                    qrp->tbp->var.join.indexp->attrv[i].attrname);
        }
        ret = _RDB_get_by_uindex(qrp->tbp->var.join.tb2p, objpv,
                qrp->tbp->var.join.indexp, txp, &tpl);
        if (ret == RDB_NOT_FOUND) {
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
                        RDB_tuple_get(&tpl, attrname));
                if (ret != RDB_OK)
                    return ret;
            } else {
                /* Yes, so compare attribute values if the attribute
                 * doesn't appear in the index
                 */
                int j;

                for (j = 0; j < qrp->tbp->var.join.indexp->attrc
                        && strcmp(qrp->tbp->var.join.indexp->attrv[j].attrname,
                                  attrname) != 0;
                        j++);
                if (j >= qrp->tbp->var.join.indexp->attrc) {
                    ret = RDB_obj_equals(RDB_tuple_get(&tpl, attrname),
                            RDB_tuple_get(tplp, attrname), &match);
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

    RDB_destroy_obj(&tpl);

    return ret;
}

int
_RDB_get_by_uindex(RDB_table *tbp, RDB_object *objpv[], _RDB_tbindex *indexp,
        RDB_transaction *txp, RDB_object *tplp)

{
    RDB_field *fv;
    RDB_field *resfv;
    int i;
    int ret;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    int keylen = indexp->attrc;

    resfv = malloc(sizeof (RDB_field)
            * (tpltyp->var.tuple.attrc - keylen));
    fv = malloc(sizeof (RDB_field) * keylen);
    if (fv == NULL || resfv == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    /*
     * Convert data to fields
     */    
    for (i = 0; i < keylen; i++) {
        ret = _RDB_obj_to_field(&fv[i], objpv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }

    /*
     * Read fields
     */
    if (indexp->idxp == NULL) {
        for (i = 0; i < tpltyp->var.tuple.attrc - keylen; i++) {
            resfv[i].no = keylen + i;
        }
        ret = RDB_get_fields(tbp->var.stored.recmapp, fv,
                             tpltyp->var.tuple.attrc - keylen,
                             tbp->is_persistent ? txp->txid : NULL, resfv);
    } else {
        int rfi = 0;

        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            int j = 0;

            /* Search field number in index */
            while (j < keylen && indexp->idxp->fieldv[j] != i)
                j++;

            /* Not found, so the field must be read from the DB */
            if (j >= keylen)
                resfv[rfi++].no = i;
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
        ret = RDB_tuple_set(tplp, indexp->attrv[i].attrname, objpv[i]);
        if (ret != RDB_OK)
            return ret;
    }

    /*
     * Set non-key attributes
     */
    for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
        int rfi; /* Index in resv, -1 if key attr */
        char *attrname = tpltyp->var.tuple.attrv[i].name;
        RDB_int fno = *(RDB_int *)RDB_hashmap_get(
                    &tbp->var.stored.attrmap, attrname, NULL);

        if (indexp->idxp == NULL) {
            if (fno < keylen)
                rfi = -1;
            else
                rfi = fno - keylen;
        } else {
            /* Search field number in resfv */
            rfi = 0;
            while (rfi < tpltyp->var.tuple.attrc - keylen
                    && resfv[rfi].no != fno)
                rfi++;
            if (rfi >= tpltyp->var.tuple.attrc - keylen)
                rfi = -1;
        }

        if (rfi != -1) {
            /* non-key attribute */
            RDB_object val;

            RDB_init_obj(&val);
            ret = RDB_irep_to_obj(&val, tpltyp->var.tuple.attrv[i].typ,
                    resfv[rfi].datap, resfv[rfi].len);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                goto cleanup;
            }
            ret = RDB_tuple_set(tplp, attrname, &val);
            RDB_destroy_obj(&val);
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
next_select_index(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    int ret;

    if (qrp->tbp->var.select.indexp->unique) {
        ret = _RDB_get_by_uindex(qrp->tbp->var.select.tbp,
                qrp->tbp->var.select.objpv,
                qrp->tbp->var.select.indexp, txp, tplp);
        if (ret != RDB_OK)
            return ret;

        qrp->endreached = RDB_TRUE;
    } else {
        RDB_bool rtup;
        RDB_bool dup = RDB_FALSE;

        if (qrp->tbp->var.select.objpc == qrp->tbp->var.select.indexp->attrc
                && qrp->tbp->var.select.all_eq)
            dup = RDB_TRUE;

        do {
            ret = next_stored_tuple(qrp, qrp->tbp->var.select.tbp, tplp,
                    qrp->tbp->var.select.asc, dup);
            if (ret != RDB_OK)
                return ret;
            if (qrp->tbp->var.select.all_eq)
                rtup = RDB_TRUE;
            else {
                if (qrp->tbp->var.select.stopexp != NULL) {
                    ret = RDB_evaluate_bool(qrp->tbp->var.select.stopexp, tplp,
                            txp, &rtup);
                    if (ret != RDB_OK)
                        return ret;
                    if (!rtup) {
                        qrp->endreached = RDB_TRUE;
                        return RDB_NOT_FOUND;
                    }
                }
                /*
                 * Check condition, because it could be an open start
                 */
                ret = RDB_evaluate_bool(qrp->tbp->var.select.exp, tplp, txp,
                        &rtup);
                if (ret != RDB_OK)
                    return ret;
            }
        } while (!rtup);
    }

    return RDB_OK;
}

static int
next_project_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    RDB_object tpl;
    int i, ret;
    RDB_object *valp;
    RDB_type *tuptyp = qrp->tbp->typ->var.basetyp;
            
    RDB_init_obj(&tpl);

    do {
        /* Get tuple */
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }

        if (tplp != NULL) {
            /* Copy attributes into new tuple */
            for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
                char *attrnamp = tuptyp->var.tuple.attrv[i].name;

                valp = RDB_tuple_get(&tpl, attrnamp);
                RDB_tuple_set(tplp, attrnamp, valp);
            }
        }

        /* eliminate duplicates, if necessary */
        if (qrp->tbp->var.project.keyloss) {
            ret = RDB_insert(qrp->matp, &tpl, txp);
        } else {
            ret = RDB_OK;
        }
    } while (ret == RDB_ELEMENT_EXISTS);

    RDB_destroy_obj(&tpl);
    return RDB_OK;
}

static int
next_rename_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    ret = RDB_rename_tuple(&tpl, qrp->tbp->var.rename.renc,
                           qrp->tbp->var.rename.renv, tplp);
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
next_wrap_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    ret = RDB_wrap_tuple(&tpl, qrp->tbp->var.wrap.wrapc,
                           qrp->tbp->var.wrap.wrapv, tplp);
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
next_unwrap_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    ret = RDB_unwrap_tuple(&tpl, qrp->tbp->var.unwrap.attrc,
                           qrp->tbp->var.unwrap.attrv, tplp);
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
destroy_qresult(RDB_qresult *qrp, RDB_transaction *txp)
{
    int ret;

    if (qrp->tbp == NULL || qrp->tbp->kind == RDB_TB_STORED
            || qrp->tbp->kind == RDB_TB_SUMMARIZE
            || qrp->tbp->kind == RDB_TB_GROUP
            || (qrp->tbp->kind == RDB_TB_SELECT_INDEX
                && !qrp->tbp->var.select.indexp->unique)) {
        /* Sorter, stored table, SUMMARIZE PER, or GROUP */
        ret = RDB_destroy_cursor(qrp->var.curp);
    } else if (qrp->tbp->kind != RDB_TB_SELECT_INDEX) {
        ret = _RDB_drop_qresult(qrp->var.virtual.qrp, txp);
        if (qrp->var.virtual.qr2p != NULL)
            _RDB_drop_qresult(qrp->var.virtual.qr2p, txp);
        if (qrp->tbp->kind == RDB_TB_JOIN || qrp->tbp->kind == RDB_TB_UNGROUP) {
            if (qrp->var.virtual.tpl_valid)
                RDB_destroy_obj(&qrp->var.virtual.tpl);
        }
    } else {
        ret = RDB_OK;
    }

    if (qrp->matp != NULL)
        RDB_drop_table(qrp->matp, txp);
    return RDB_OK;
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
        RDB_qresult *qr3p, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl2;
    RDB_qresult qr;
    RDB_bool matchall = RDB_TRUE;

    /*
     * Join this tuple with all tuples from table 2 and set matchall to RDB_FALSE
     * if not all the result tuples are found in table 3
     */

    ret = init_qresult(&qr, tbp->var.sdivide.tb2p, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    for (;;) {
        RDB_type *tb1tuptyp = tbp->var.sdivide.tb1p->typ->var.basetyp;
        RDB_bool match = RDB_TRUE;

        RDB_init_obj(&tpl2);

        ret = _RDB_next_tuple(&qr, &tpl2, txp);
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

                 ret = RDB_obj_equals(objp, dstobjp, &b);
                 if (ret != RDB_OK) {
                     destroy_qresult(&qr, txp);
                     RDB_destroy_obj(&tpl2);
                     return ret;
                 }
                 if (!b) {                     
                     match = RDB_FALSE;
                     break;
                 }
             } else {
                 ret = RDB_tuple_set(&tpl2,
                         tb1tuptyp->var.tuple.attrv[i].name, objp);
                 if (ret != RDB_OK) {
                     destroy_qresult(&qr, txp);
                     RDB_destroy_obj(&tpl2);
                     return ret;
                 }
             }
        }
        if (!match)
            continue;

        if (qr3p != NULL)
            ret = _RDB_qresult_contains(qr3p, &tpl2, txp);
        else
            ret = RDB_table_contains(tbp->var.sdivide.tb3p, &tpl2, txp);
        RDB_destroy_obj(&tpl2);
        if (ret != RDB_OK) {
            matchall = RDB_FALSE;
            break;
        }
    }
    if (ret != RDB_NOT_FOUND) {
        destroy_qresult(&qr, txp);
        return ret;
    }
    
    ret = destroy_qresult(&qr, txp);
    if (ret != RDB_OK)
        return ret;

    return matchall ? RDB_OK : RDB_NOT_FOUND;
}

static int
next_sdivide_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    int ret;

    do {
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, txp);
        if (ret != RDB_OK) {
            return ret;
        }

        ret = _RDB_sdivide_preserves(qrp->tbp, tplp, qrp->var.virtual.qr2p,
                txp);
        if (ret != RDB_OK && ret != RDB_NOT_FOUND)
            return ret;

    } while (ret == RDB_NOT_FOUND);

    return RDB_OK;
}

static int
next_select_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_bool expres;

    if (tplp == NULL)
        RDB_init_obj(&tpl);

    do {
        ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp != NULL ? tplp : &tpl,
                txp);
        if (ret != RDB_OK)
            break;
        ret = RDB_evaluate_bool(qrp->tbp->var.select.exp, tplp != NULL ? tplp : &tpl,
                txp, &expres);
        if (ret != RDB_OK)
            break;
    } while (!expres);

    if (tplp == NULL)
        RDB_destroy_obj(&tpl);
    return ret;
}

int
_RDB_next_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    RDB_table *tbp = qrp->tbp;

    if (qrp->endreached)
        return RDB_NOT_FOUND;

    if (tbp == NULL) {
        /* It's a sorter */
        return next_stored_tuple(qrp, qrp->matp, tplp, RDB_TRUE, RDB_FALSE);
    }

    switch (tbp->kind) {
        case RDB_TB_STORED:
            return next_stored_tuple(qrp, qrp->tbp, tplp, RDB_TRUE, RDB_FALSE);
        case RDB_TB_SELECT:
            return next_select_tuple(qrp, tplp, txp);
        case RDB_TB_SELECT_INDEX:
            return next_select_index(qrp, tplp, txp);
        case RDB_TB_UNION:
            for(;;) {
                if (qrp->var.virtual.qr2p == NULL) {
                    ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, txp);
                    if (ret != RDB_NOT_FOUND)
                        return ret;
                    /* switch to second table */
                    ret = _RDB_table_qresult(tbp->var._union.tb2p,
                                txp, &qrp->var.virtual.qr2p);
                    if (ret != RDB_OK)
                        return ret;
                } else {
                    ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tplp, txp);
                    if (ret != RDB_OK)
                        return ret;
                    /* skip tuples which are contained in the first table */
                    ret = _RDB_qresult_contains(qrp->var.virtual.qrp,
                                tplp, txp);
                    if (ret == RDB_NOT_FOUND)
                        return RDB_OK;
                    if (ret != RDB_OK)
                        return ret;
                }
            };
            break;
        case RDB_TB_MINUS:
            do {
                ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = _RDB_qresult_contains(qrp->var.virtual.qr2p, tplp, txp);
                if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
                    return ret;
                }
            } while (ret == RDB_OK);
            break;
        case RDB_TB_INTERSECT:
            do {
                ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = _RDB_qresult_contains(qrp->var.virtual.qr2p, tplp, txp);
                if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
                    return ret;
                }
            } while (ret == RDB_NOT_FOUND);
            break;
        case RDB_TB_JOIN:
            if (tbp->var.join.indexp == NULL)
                return next_join_tuple(qrp, tplp, txp);
            if (tbp->var.join.indexp->unique)
                return next_join_tuple_uix(qrp, tplp, txp);
            return next_join_tuple_nuix(qrp, tplp, txp);            
        case RDB_TB_EXTEND:
            ret = _RDB_next_tuple(qrp->var.virtual.qrp, tplp, txp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_extend_tuple(tplp, tbp->var.extend.attrc,
                     tbp->var.extend.attrv, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            return next_project_tuple(qrp, tplp, txp);
        case RDB_TB_SUMMARIZE:
            {
                int i;
                char *cname;
                RDB_int count;
            
                ret = next_stored_tuple(qrp, qrp->matp, tplp, RDB_TRUE, RDB_FALSE);
                if (ret != RDB_OK)
                    return ret;
                /* check AVG counts */
                for (i = 0; i < qrp->tbp->var.summarize.addc; i++) {
                    RDB_summarize_add *summp = &qrp->tbp->var.summarize.addv[i];
                    if (summp->op == RDB_AVG) {
                        cname = malloc(strlen(summp->name) + 3);
                        strcpy (cname, summp->name);
                        strcat (cname, AVG_COUNT_SUFFIX);
                        count = RDB_tuple_get_int(tplp, cname);
                        free(cname);
                        if (count == 0)
                            return RDB_AGGREGATE_UNDEFINED;
                    }
                }
            }
            break;
        case RDB_TB_RENAME:
            return next_rename_tuple(qrp, tplp, txp);
        case RDB_TB_WRAP:
            return next_wrap_tuple(qrp, tplp, txp);
        case RDB_TB_UNWRAP:
            return next_unwrap_tuple(qrp, tplp, txp);
        case RDB_TB_GROUP:
            return next_stored_tuple(qrp, qrp->matp, tplp, RDB_TRUE, RDB_FALSE);
        case RDB_TB_UNGROUP:
            return next_ungroup_tuple(qrp, tplp, txp);
        case RDB_TB_SDIVIDE:
            return next_sdivide_tuple(qrp, tplp, txp);
    }
    return RDB_OK;
}

int
_RDB_reset_qresult(RDB_qresult *qrp, RDB_transaction *txp)
{
    int ret;
    RDB_table *tbp = qrp->tbp;

    if (tbp == NULL || qrp->tbp->kind == RDB_TB_STORED
            || qrp->tbp->kind == RDB_TB_SUMMARIZE) {
        /* Sorter, stored table or SUMMARIZE PER - reset cursor */
        ret = RDB_cursor_first(qrp->var.curp);
        if (ret == RDB_NOT_FOUND) {
            qrp->endreached = RDB_TRUE;
            ret = RDB_OK;
        } else {
            qrp->endreached = RDB_FALSE;
        }
        return ret;
    }
    ret = destroy_qresult(qrp, txp);
    if (ret != RDB_OK)
        return ret;
    return init_qresult(qrp, tbp, txp);
}

int
_RDB_qresult_contains(RDB_qresult *qrp, const RDB_object *tplp,
                      RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_bool b;
    RDB_object **objpv;
    int kattrc;
    RDB_object tpl;

    if (qrp->tbp->kind != RDB_TB_SUMMARIZE && qrp->tbp->kind != RDB_TB_GROUP)
        return RDB_table_contains(qrp->tbp, tplp, txp);

    /*
     * Check if the table contains the tuple by
     * getting the non-key attributes and comparing them.
     * (RDB_recmap_contains() cannot be used in all cases because
     * of the additional count attributes for AVG).
     */

    kattrc = _RDB_pkey_len(qrp->tbp);
    objpv = malloc(sizeof (RDB_object *) * kattrc);
    if (objpv == NULL)
        return RDB_NO_MEMORY;

    RDB_init_obj(&tpl);

    for (i = 0; i < kattrc; i++) {
        RDB_object *attrobjp = RDB_tuple_get(tplp, qrp->matp->keyv[0].strv[i]);

        if (attrobjp == NULL) {
            ret = RDB_INVALID_ARGUMENT;
            goto error;
        }
        objpv[i] = attrobjp;
    }
        
    ret = _RDB_get_by_uindex(qrp->matp, objpv, &qrp->matp->var.stored.indexv[0],
            txp, &tpl);
    if (ret != RDB_OK) /* handles RDB_NOT_FOUND too */
        goto error;

    if (qrp->tbp->kind == RDB_TB_SUMMARIZE) {
        /* compare ADD attributes */
        for (i = 0; i < qrp->tbp->var.summarize.addc; i++) {
            char *attrname = qrp->tbp->var.summarize.addv[i].name;

            ret = RDB_obj_equals(RDB_tuple_get(tplp, attrname),
                    RDB_tuple_get(&tpl, attrname), &b);
            if (ret != RDB_OK)
                goto error;
            if (!b) {
                ret = RDB_NOT_FOUND;
                goto error;
            }
        }
    } else {
        char *attrname = qrp->tbp->var.group.gattr;

        ret = RDB_obj_equals(RDB_tuple_get(tplp, attrname),
                RDB_tuple_get(&tpl, attrname), &b);
        if (ret != RDB_OK)
            goto error;
        if (!b) {
            ret = RDB_NOT_FOUND;
            goto error;
        }
    }

    ret = RDB_OK;

error:
    RDB_destroy_obj(&tpl);
    free(objpv);
    
    return ret;
}

int
_RDB_drop_qresult(RDB_qresult *qrp, RDB_transaction *txp)
{
    int ret = destroy_qresult(qrp, txp);

    free(qrp);

    return ret;
}
