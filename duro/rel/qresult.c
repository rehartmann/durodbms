/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "typeimpl.h"
#include <string.h>

static int
init_summ_table(RDB_qresult *qresp, RDB_transaction *txp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /*
     * initialize table from table 2
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
                    if (RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp)
                            == &RDB_INTEGER)
                        ret = RDB_tuple_set_int(&tpl, name, 0);
                    else
                        ret = RDB_tuple_set_rational(&tpl, name, 0.0);
                    break;
                case RDB_MAX:
                    if (RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp)
                            == &RDB_INTEGER)
                        ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MIN);
                    else
                        ret = RDB_tuple_set_rational(&tpl, name, RDB_RATIONAL_MIN);
                    break;
                case RDB_MIN:
                    if (RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp,
                            qresp->tbp->var.summarize.tb1p->typ->var.basetyp)
                            == &RDB_INTEGER)
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
                _RDB_obj_to_field(&keyfv[i],
                        RDB_tuple_get(&tpl, qresp->tbp->keyv[0].strv[i]));
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
                        goto error;
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
                                 addc + avgc, txp->txid, nonkeyfv);
            if (ret == RDB_OK) {
                /* A corresponding tuple in table 2 has been found */
                for (i = 0; i < addc; i++) {
                    RDB_summarize_add *summp = &qresp->tbp->var.summarize.addv[i];

                    if (summp->op == RDB_COUNT) {
                        ret = RDB_irep_to_obj(&svalv[i].val, &RDB_INTEGER,
                                nonkeyfv[i].datap, nonkeyfv[i].len);
                    } else {
                        ret = RDB_irep_to_obj(&svalv[i].val,
                                RDB_expr_type(summp->exp,
                                qresp->tbp->var.summarize.tb1p->typ->var.basetyp),
                                nonkeyfv[i].datap, nonkeyfv[i].len);
                        if (ret != RDB_OK)
                            goto error;
                        ret = RDB_evaluate(summp->exp, &tpl, txp, &addval);
                        if (ret != RDB_OK)
                            goto error;
                        /* If it's AVG, get count */
                        if (summp->op == RDB_AVG) {
                            memcpy(&svalv[i].count, nonkeyfv[svalv[i].fvidx].datap,
                                    sizeof(RDB_int));
                        }
                    }
                    summ_step(&svalv[i], &addval, summp->op);

                    _RDB_obj_to_field(&nonkeyfv[i], &svalv[i].val);

                    /* If it's AVG, store count */
                    if (summp->op == RDB_AVG) {
                        nonkeyfv[svalv[i].fvidx].datap = &svalv[i].count;
                        nonkeyfv[svalv[i].fvidx].len = sizeof(RDB_int);
                        nonkeyfv[svalv[i].fvidx].copyfp = memcpy;
                    }
                }
                ret = RDB_update_rec(qresp->matp->var.stored.recmapp, keyfv,
                    addc + avgc, nonkeyfv, txp->txid);
                if (ret != RDB_OK) {
                    if (RDB_is_syserr(ret)) {
                        RDB_rollback_all(txp);
                    }
                    goto error;
                }
            } else if (ret != RDB_NOT_FOUND)
                goto error;
        }
    } while (ret == RDB_OK);

    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;

error:
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
stored_qresult(RDB_qresult *qresp, RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    /* !! delay after first call to _RDB_qresult_next()? */
    ret = RDB_recmap_cursor(&qresp->var.curp, tbp->var.stored.recmapp,
                    0, txp != NULL ? txp->txid : NULL);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }
    ret = RDB_cursor_first(qresp->var.curp);
    if (ret == RDB_NOT_FOUND) {
        qresp->endreached = 1;
        ret = RDB_OK;
    } else if (ret != RDB_OK) {
        RDB_destroy_cursor(qresp->var.curp);
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }
    return ret;
}

static int
summarize_qresult(RDB_qresult *qresp, RDB_transaction *txp)
{
    RDB_type *tuptyp = qresp->tbp->typ->var.basetyp;
    int ret;

    /* create materialized table */
    ret = _RDB_create_table(NULL, RDB_FALSE,
                        tuptyp->var.tuple.attrc,
                        tuptyp->var.tuple.attrv,
                        qresp->tbp->keyc, qresp->tbp->keyv,
                        txp, &qresp->matp);
    if (ret != RDB_OK)
        return ret;

    ret = init_summ_table(qresp, txp);
    if (ret != RDB_OK) {
        _RDB_drop_rtable(qresp->matp, txp);
        return ret;
    }

    /* summarize over table 1 */
    ret = do_summarize(qresp, txp);
    if (ret != RDB_OK) {
        _RDB_drop_rtable(qresp->matp, txp);
        return ret;
    }

    ret = stored_qresult(qresp, qresp->matp, txp);

    if (ret != RDB_OK) {
        _RDB_drop_rtable(qresp->matp, txp);
        return ret;
    }

    return RDB_OK;
}

static int
init_qresult(RDB_qresult *qrp, RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    qrp->tbp = tbp;
    qrp->endreached = 0;
    qrp->matp = NULL;
    qrp->var.virtual.qr2p = NULL;
    switch (tbp->kind) {
        case RDB_TB_STORED:
            ret = stored_qresult(qrp, tbp, txp);
            break;
        case RDB_TB_SELECT:
            ret = _RDB_table_qresult(tbp->var.select.tbp,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_SELECT_PINDEX:
            /* nothing special to do */
            return RDB_OK;
        case RDB_TB_UNION:
            ret = _RDB_table_qresult(tbp->var._union.tbp1,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_MINUS:
            ret = _RDB_table_qresult(tbp->var.minus.tbp1,
                    txp, &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.minus.tbp2,
                    txp, &qrp->var.virtual.qr2p);
            break;
        case RDB_TB_INTERSECT:
            ret = _RDB_table_qresult(tbp->var.intersect.tbp1,
                    txp, &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.minus.tbp2,
                    txp, &qrp->var.virtual.qr2p);
            break;
        case RDB_TB_JOIN:
            /* create qresults for each of the two tables */
            ret = _RDB_table_qresult(tbp->var.join.tbp1,
                    txp, &qrp->var.virtual.qrp);
            if (ret != RDB_OK)
                break;
            ret = _RDB_table_qresult(tbp->var.join.tbp2,
                    txp, &qrp->var.virtual.qr2p);
            if (ret != RDB_OK) {
                _RDB_drop_qresult(qrp->var.virtual.qr2p, txp);
                break;
            }
            qrp->var.virtual.tpl_valid = RDB_FALSE;
            break;
        case RDB_TB_EXTEND:
            ret = _RDB_table_qresult(tbp->var.extend.tbp,
                    txp, &qrp->var.virtual.qrp);
            break;
        case RDB_TB_PROJECT:
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
                        1, &keyattrs, txp, &qrp->matp);
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
            int seqitc, RDB_seq_item seqitv[])
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
    qresp->endreached = 0;

    for (i = 0; i < seqitc; i++) {
        key.strv[i] = seqitv[i].attrname;
        ascv[i] = seqitv[i].asc;
    }

    /*
     * Create a sorted RDB_table
     */

    ret = _RDB_new_stored_table(NULL, RDB_FALSE, tbp->typ, 1,
                &key, RDB_TRUE, &qresp->matp);
    if (ret != RDB_OK)
        goto error;

    ret = _RDB_open_table(qresp->matp, seqitc, key.strv, RDB_TRUE, txp,
            txp->dbp->dbrootp->envp, ascv);
    if (ret != RDB_OK)
        goto error;

    /*
     * Copy tuples into the newly created RDB_table
     */

    ret = _RDB_table_qresult(tbp, txp, &tmpqrp);
    if (ret != RDB_OK)
        goto error;

    RDB_init_obj(&tpl);
    while ((ret = _RDB_next_tuple(tmpqrp, &tpl, txp)) == RDB_OK)
        RDB_insert(qresp->matp, &tpl, txp);
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
        _RDB_free_table(qresp->matp, NULL);
    free(qresp);
    
    return ret;
}

static int
next_stored_tuple(RDB_qresult *qrp, RDB_table *tbp, RDB_object *tup)
{
    int i;
    int ret;
    void *datap;
    size_t len;
    RDB_type *tuptyp = tbp->typ->var.basetyp;

    if (tup != NULL) {
        for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
            RDB_object val;
            RDB_int fno;
            RDB_attr *attrp = &tuptyp->var.tuple.attrv[i];

            fno = *(RDB_int *)RDB_hashmap_get(&tbp->var.stored.attrmap,
                                          attrp->name, NULL);
            ret = RDB_cursor_get(qrp->var.curp, fno, &datap, &len);
            if (ret != RDB_OK) {
                return ret;
            }
            RDB_init_obj(&val);
            ret = RDB_irep_to_obj(&val, attrp->typ, datap, len);
            if (ret != RDB_OK) {
                return ret;
            }
            ret = RDB_tuple_set(tup, attrp->name, &val);
            if (ret != RDB_OK) {
                return ret;
            }
        }
    }
    ret = RDB_cursor_next(qrp->var.curp);
    if (ret == RDB_NOT_FOUND) {
        qrp->endreached = 1;
        return RDB_OK;
    }
    return ret;
}

static int
next_join_tuple(RDB_qresult *qrp, RDB_object *tup, RDB_transaction *txp)
{
    int ret;
    int i;

    /* tuple type of first ('outer') table */
    RDB_type *tpltyp1 = qrp->tbp->var.join.tbp1->typ->var.basetyp;
            
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
        ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tup, txp);
        if (ret != RDB_NOT_FOUND && ret != RDB_OK) {
            return ret;
        }
        if (ret == RDB_OK) {
            /* compare common attributes */
            RDB_bool iseq = RDB_TRUE;

            for (i = 0; (i < qrp->tbp->var.join.common_attrc) && iseq; i++) {
                RDB_object *valp;
                RDB_object *valp2;

                valp = RDB_tuple_get(&qrp->var.virtual.tpl,
                        qrp->tbp->var.join.common_attrv[i]);
                valp2 = RDB_tuple_get(tup,
                        qrp->tbp->var.join.common_attrv[i]);

                /* if the attribute values are not equal, skip to next tuple */
                if (!RDB_obj_equals(valp, valp2))
                    iseq = RDB_FALSE;
            }

            /* if common attributes are equal, leave the loop,
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
    
    /* join the two tuples into tup */
    for (i = 0; i < tpltyp1->var.tuple.attrc; i++) {
         ret = RDB_tuple_set(tup, tpltyp1->var.tuple.attrv[i].name,
                       RDB_tuple_get(&qrp->var.virtual.tpl,
                       tpltyp1->var.tuple.attrv[i].name));
         if (ret != RDB_OK) {
             return ret;
         }
    }

    return RDB_OK;
}

int
_RDB_get_by_pindex(RDB_table *tbp, RDB_object valv[], RDB_object *tup, RDB_transaction *txp)
{
    RDB_field *fv;
    RDB_field *resfv;
    int i;
    int ret;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    int pkeylen = _RDB_pkey_len(tbp);

    resfv = malloc(sizeof (RDB_field)
            * (tpltyp->var.tuple.attrc - pkeylen));
    fv = malloc(sizeof (RDB_field) * pkeylen);
    if (fv == NULL || resfv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    
    for (i = 0; i < pkeylen; i++) {
        _RDB_obj_to_field(&fv[i], &valv[i]);
    }
    for (i = 0; i < tpltyp->var.tuple.attrc - pkeylen; i++) {
        resfv[i].no = pkeylen + i;
    }
    ret = RDB_get_fields(tbp->var.stored.recmapp, fv,
                         tpltyp->var.tuple.attrc - pkeylen,
                         txp->txid, resfv);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        goto error;
    }

    /* set key fields */
    for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
        char *attrname = tpltyp->var.tuple.attrv[i].name;
        RDB_int fno = *(RDB_int *)RDB_hashmap_get(
                    &tbp->var.stored.attrmap, attrname, NULL);

        if (fno < pkeylen) {
            /* key attribute */
            ret = RDB_tuple_set(tup, attrname, &valv[fno]);
            if (ret != RDB_OK)
                return ret;
        } else {
            /* non-key attribute */
            RDB_object val;

            RDB_init_obj(&val);
            ret = RDB_irep_to_obj(&val, tpltyp->var.tuple.attrv[i].typ,
                    resfv[fno - pkeylen].datap, resfv[fno - pkeylen].len);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                goto error;
            }
            ret = RDB_tuple_set(tup, attrname, &val);
            RDB_destroy_obj(&val);
            if (ret != RDB_OK)
                goto error;
        }
    }
    ret = RDB_OK;
error:
    free(fv);
    free(resfv);
    return ret;
}

static int
next_select_pindex(RDB_qresult *qrp, RDB_object *tup, RDB_transaction *txp)
{
    return _RDB_get_by_pindex(qrp->tbp->var.select.tbp, &qrp->tbp->var.select.val,
                         tup, txp);
}

static int
next_project_tuple(RDB_qresult *qrp, RDB_object *tup, RDB_transaction *txp)
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

        if (tup != NULL) {            
            /* Copy attributes into new tuple */
            for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
                char *attrnamp = tuptyp->var.tuple.attrv[i].name;
            
                valp = RDB_tuple_get(&tpl, attrnamp);
                RDB_tuple_set(tup, attrnamp, valp);
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
next_rename_tuple(RDB_qresult *qrp, RDB_object *tup, RDB_transaction *txp)
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
                           qrp->tbp->var.rename.renv, tup);
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
next_wrap_tuple(RDB_qresult *qrp, RDB_object *tup, RDB_transaction *txp)
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
                           qrp->tbp->var.wrap.wrapv, tup);
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
next_unwrap_tuple(RDB_qresult *qrp, RDB_object *tup, RDB_transaction *txp)
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
                           qrp->tbp->var.unwrap.attrv, tup);
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
destroy_qresult(RDB_qresult *qrp, RDB_transaction *txp)
{
    int ret;

    if (qrp->tbp == NULL || qrp->tbp->kind == RDB_TB_STORED
            || qrp->tbp->kind == RDB_TB_SUMMARIZE) {
        /* Sorter, stored table or SUMMARIZE PER */
        ret = RDB_destroy_cursor(qrp->var.curp);
    } else if (qrp->tbp->kind != RDB_TB_SELECT_PINDEX) {
        ret = _RDB_drop_qresult(qrp->var.virtual.qrp, txp);
        if (qrp->tbp->kind == RDB_TB_JOIN) {
            if (qrp->var.virtual.tpl_valid)
                RDB_destroy_obj(&qrp->var.virtual.tpl);
        }
        if (qrp->var.virtual.qr2p != NULL)
            _RDB_drop_qresult(qrp->var.virtual.qr2p, txp);
    } else {
        ret = RDB_OK;
    }

    if (qrp->matp != NULL)
        _RDB_drop_rtable(qrp->matp, txp);
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
     * Join this tuple with all tuple from table 2 and set matchall to RDB_FALSE
     * if not all the result tuples are found in table 3
     */

    ret = init_qresult(&qr, tbp->var.sdivide.tb2p, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    for (;;) {
        RDB_init_obj(&tpl2);
        ret = _RDB_next_tuple(&qr, &tpl2, txp);
        if (ret != RDB_OK)
            break;
        RDB_type *tb1tuptyp = tbp->var.sdivide.tb1p->typ->var.basetyp;
        RDB_bool match = RDB_TRUE;

        /* Join *tplp and tpl2 into tpl2 */
        for (i = 0; i < tb1tuptyp->var.tuple.attrc; i++) {
             RDB_object *objp = RDB_tuple_get(tplp,
                     tb1tuptyp->var.tuple.attrv[i].name);
             RDB_object *dstobjp = RDB_tuple_get(&tpl2,
                     tb1tuptyp->var.tuple.attrv[i].name);

             if (dstobjp != NULL && !RDB_obj_equals(objp, dstobjp)) {
                 match = RDB_FALSE;
                 break;
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

int
_RDB_next_tuple(RDB_qresult *qrp, RDB_object *tup, RDB_transaction *txp)
{
    int ret;
    RDB_table *tbp = qrp->tbp;

    if (qrp->endreached)
        return RDB_NOT_FOUND;

    if (tbp == NULL) {
        /* It's a sorter */
        ret = next_stored_tuple(qrp, qrp->matp, tup);
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }

    switch (tbp->kind) {
        RDB_bool expres;

        case RDB_TB_STORED:
            ret = next_stored_tuple(qrp, qrp->tbp, tup);
            if (RDB_is_syserr(ret))
                RDB_rollback_all(txp);
            return ret;
        case RDB_TB_SELECT:
            do {
                ret = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
                if (ret != RDB_OK)
                    break;
                ret = RDB_evaluate_bool(tbp->var.select.exprp, tup, txp, &expres);
                if (ret != RDB_OK)
                    break;
            } while (!expres);
            if (ret == RDB_NOT_FOUND) {
                return RDB_NOT_FOUND;
            } else if (ret != RDB_OK) {
                return ret;
            }
            break;
        case RDB_TB_SELECT_PINDEX:
            ret = next_select_pindex(qrp, tup, txp);
            qrp->endreached = 1;
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNION:
            for(;;) {
                if (qrp->var.virtual.qr2p == NULL) {
                    ret = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
                    if (ret != RDB_NOT_FOUND)
                        return ret;
                    /* switch to second table */
                    ret = _RDB_table_qresult(tbp->var._union.tbp2,
                                txp, &qrp->var.virtual.qr2p);
                    if (ret != RDB_OK)
                        return ret;
                } else {
                    ret = _RDB_next_tuple(qrp->var.virtual.qr2p, tup, txp);
                    if (ret != RDB_OK)
                        return ret;
                    /* skip tuples which are contained in the first table */
                    ret = _RDB_qresult_contains(qrp->var.virtual.qrp,
                                tup, txp);
                    if (ret == RDB_NOT_FOUND)
                        return RDB_OK;
                    if (ret != RDB_OK)
                        return ret;
                }
            };
            break;
        case RDB_TB_MINUS:
            do {
                ret = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = _RDB_qresult_contains(qrp->var.virtual.qr2p, tup, txp);
                if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
                    return ret;
                }
            } while (ret == RDB_OK);
            break;
        case RDB_TB_INTERSECT:
            do {
                ret = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = _RDB_qresult_contains(qrp->var.virtual.qr2p, tup, txp);
                if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
                    return ret;
                }
            } while (ret == RDB_NOT_FOUND);
            break;
        case RDB_TB_JOIN:
            return next_join_tuple(qrp, tup, txp);
        case RDB_TB_EXTEND:
            ret = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_extend_tuple(tup, tbp->var.extend.attrc,
                     tbp->var.extend.attrv, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            return next_project_tuple(qrp, tup, txp);
        case RDB_TB_SUMMARIZE:
            {
                int i;
                char *cname;
                RDB_int count;
            
                ret = next_stored_tuple(qrp, qrp->matp, tup);
                if (ret != RDB_OK)
                    return ret;
                /* check AVG counts */
                for (i = 0; i < qrp->tbp->var.summarize.addc; i++) {
                    RDB_summarize_add *summp = &qrp->tbp->var.summarize.addv[i];
                    if (summp->op == RDB_AVG) {
                        cname = malloc(strlen(summp->name) + 3);
                        strcpy (cname, summp->name);
                        strcat (cname, AVG_COUNT_SUFFIX);
                        count = RDB_tuple_get_int(tup, cname);
                        free(cname);
                        if (count == 0)
                            return RDB_AGGREGATE_UNDEFINED;
                    }
                }
            }
            break;
        case RDB_TB_RENAME:
            return next_rename_tuple(qrp, tup, txp);
        case RDB_TB_WRAP:
            return next_wrap_tuple(qrp, tup, txp);
        case RDB_TB_UNWRAP:
            return next_unwrap_tuple(qrp, tup, txp);
        case RDB_TB_SDIVIDE:
            return next_sdivide_tuple(qrp, tup, txp);
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
            qrp->endreached = 1;
            ret = RDB_OK;
        } else if (ret != RDB_OK) {
            if (RDB_is_syserr(ret))
                RDB_rollback_all(txp);
        } else {
            qrp->endreached = 0;
        }
        return ret;
    }
    ret = destroy_qresult(qrp, txp);
    if (ret != RDB_OK)
        return ret;
    return init_qresult(qrp, tbp, txp);
}

int
_RDB_qresult_contains(RDB_qresult *qrp, const RDB_object *tup,
                      RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_object *valv;
    int kattrc;
    RDB_object tpl;

    if (qrp->tbp->kind != RDB_TB_SUMMARIZE)
        return RDB_table_contains(qrp->tbp, tup, txp);

    /*
     * Check if SUMMARIZE table contains the tuple by
     * getting the non-key attributes and comparing them.
     * (RDB_recmap_contains() cannot be used in all cases because
     * of the additional count attributes for AVG).
     */

    kattrc = _RDB_pkey_len(qrp->tbp);
    valv = malloc(sizeof (RDB_object) * kattrc);
    if (valv == NULL)
        return RDB_NO_MEMORY;

    RDB_init_obj(&tpl);

    for (i = 0; i < kattrc; i++) {
        RDB_init_obj(&valv[i]);
    }

    for (i = 0; i < kattrc; i++) {
        ret = RDB_copy_obj(&valv[i], RDB_tuple_get(
                tup, qrp->matp->keyv[0].strv[i]));
        if (ret != RDB_OK)
            goto error;
    }
        
    ret = _RDB_get_by_pindex(qrp->matp, valv, &tpl, txp);
    if (ret != RDB_OK) /* handles RDB_NOT_FOUND too */
        goto error;

    /* compare ADD attributes */
    for (i = 0; i < qrp->tbp->var.summarize.addc; i++) {
        char *attrname = qrp->tbp->var.summarize.addv[i].name;

        if (!RDB_obj_equals(RDB_tuple_get(tup, attrname),
                RDB_tuple_get(&tpl, attrname))) {
            ret = RDB_NOT_FOUND;
            goto error;
        }
    }
    ret = RDB_OK;

error:
    RDB_destroy_obj(&tpl);
    for (i = 0; i < kattrc; i++)
        RDB_destroy_obj(&valv[i]);
    free(valv);
    
    return ret;
}

int
_RDB_drop_qresult(RDB_qresult *qrp, RDB_transaction *txp)
{
    int ret = destroy_qresult(qrp, txp);

    free(qrp);

    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}
