/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "typeimpl.h"
#include <string.h>

static int
init_summ_table(RDB_qresult *qresp, RDB_transaction *txp)
{
    RDB_qresult *qrp;
    RDB_tuple tpl;
    int res;

    /*
     * initialize table from table 2
     */

    res = _RDB_table_qresult(qresp->tbp->var.summarize.tb2p, &qrp, txp);
    if (res != RDB_OK) {
        return res;
    }

    RDB_init_tuple(&tpl);
    for(;;) {
        int i;

        res = _RDB_next_tuple(qrp, &tpl, txp);
        if (res != RDB_OK)
            break;

        /* Extend tuple */
        for (i = 0; i < qresp->tbp->var.summarize.addc; i++) {
            char *name = qresp->tbp->var.summarize.addv[i].name;
            char *cname;

            switch (qresp->tbp->var.summarize.addv[i].op) {
                case RDB_COUNT:
                case RDB_COUNTD:
                    res = RDB_tuple_set_int(&tpl, name, 0);
                    break;
                case RDB_AVG:
                case RDB_AVGD:
                    res = RDB_tuple_set_rational(&tpl, name, 0);
                    if (res != RDB_OK)
                       break;
                    cname = malloc(strlen(name) + 3);
                    if (cname == NULL) {
                        res = RDB_NO_MEMORY;
                        break;
                    }
                    strcpy(cname, name);
                    strcat(cname, AVG_COUNT_SUFFIX);
                    res = RDB_tuple_set_int(&tpl, cname, 0);
                    free(cname);
                    break;
                case RDB_SUM:
                case RDB_SUMD:
                    if (RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp)
                            == &RDB_INTEGER)
                        res = RDB_tuple_set_int(&tpl, name, 0);
                    else
                        res = RDB_tuple_set_rational(&tpl, name, 0.0);
                    break;
                case RDB_MAX:
                    if (RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp)
                            == &RDB_INTEGER)
                        res = RDB_tuple_set_int(&tpl, name, RDB_INT_MIN);
                    else
                        res = RDB_tuple_set_rational(&tpl, name, RDB_RATIONAL_MIN);
                    break;
                case RDB_MIN:
                    if (RDB_expr_type(qresp->tbp->var.summarize.addv[i].exp)
                            == &RDB_INTEGER)
                        res = RDB_tuple_set_int(&tpl, name, RDB_INT_MAX);
                    else
                        res = RDB_tuple_set_rational(&tpl, name, RDB_RATIONAL_MAX);
                    break;
                case RDB_ALL:
                    res = RDB_tuple_set_bool(&tpl, name, RDB_TRUE);
                    break;
                case RDB_ANY:
                    res = RDB_tuple_set_bool(&tpl, name, RDB_FALSE);
                    break;
            }
            if (res != RDB_OK)
                goto error;
        } /* for */
        res = RDB_insert(qresp->matp, &tpl, txp);
        if (res != RDB_OK)
            goto error;
    };

    if (res == RDB_NOT_FOUND)
        res = RDB_OK;

error:
    RDB_destroy_tuple(&tpl);
    _RDB_drop_qresult(qrp, txp);

    return res;
}

struct _RDB_summval {
    RDB_value val;

    /* for AVG */
    int fvidx;
    RDB_int count;
};

static void
summ_step(struct _RDB_summval *svalp, const RDB_value *addvalp, RDB_aggregate_op op)
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
    RDB_tuple tpl;
    RDB_field *keyfv, *nonkeyfv;
    int res;
    struct _RDB_summval *svalv;
    RDB_value addval;
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

    res = _RDB_table_qresult(qresp->tbp->var.summarize.tb1p, &qrp, txp);
    if (res != RDB_OK) {
        return res;
    }

    RDB_init_tuple(&tpl);
    RDB_init_value(&addval);
    for (i = 0; i < addc; i++)
        RDB_init_value(&svalv[i].val);
    do {
        res = _RDB_next_tuple(qrp, &tpl, txp);
        if (res == RDB_OK) {
            int ai;
        
            /* Build key */
            for (i = 0; i < keyfc; i++) {
                keyfv[i].datap = RDB_value_irep(
                        RDB_tuple_get(&tpl, qresp->tbp->keyv[0].attrv[i]),
                        &keyfv[i].len);
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
                        res = RDB_NO_MEMORY;
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
            res = RDB_get_fields(qresp->matp->var.stored.recmapp, keyfv,
                                 addc + avgc, txp->txid, nonkeyfv);
            if (res == RDB_OK) {
                /* A corresponding tuple in table 2 has been found */
                for (i = 0; i < addc; i++) {
                    RDB_summarize_add *summp = &qresp->tbp->var.summarize.addv[i];

                    if (summp->op == RDB_COUNT) {
                        res = RDB_irep_to_value(&svalv[i].val, &RDB_INTEGER,
                                nonkeyfv[i].datap, nonkeyfv[i].len);
                    } else {
                        res = RDB_irep_to_value(&svalv[i].val, RDB_expr_type(summp->exp),
                                nonkeyfv[i].datap, nonkeyfv[i].len);
                        if (res != RDB_OK)
                            goto error;
                        res = RDB_evaluate(summp->exp, &tpl, txp, &addval);
                        if (res != RDB_OK)
                            goto error;
                        /* If it's AVG, get count */
                        if (summp->op == RDB_AVG) {
                            memcpy(&svalv[i].count, nonkeyfv[svalv[i].fvidx].datap,
                                    sizeof(RDB_int));
                        }
                    }
                    summ_step(&svalv[i], &addval, summp->op);

                    nonkeyfv[i].datap = RDB_value_irep(&svalv[i].val, &nonkeyfv[i].len);

                    /* If it's AVG, store count */
                    if (summp->op == RDB_AVG) {
                        nonkeyfv[svalv[i].fvidx].datap = &svalv[i].count;
                        nonkeyfv[svalv[i].fvidx].len = sizeof(RDB_int);
                    }                        
                }
                res = RDB_update_rec(qresp->matp->var.stored.recmapp, keyfv,
                    addc + avgc, nonkeyfv, txp->txid);
                if (res != RDB_OK) {
                    if (RDB_is_syserr(res)) {
                        RDB_rollback(txp);
                    }
                    goto error;
                }
            } else if (res != RDB_NOT_FOUND)
                goto error;
        }
    } while (res == RDB_OK);

    if (res == RDB_NOT_FOUND)
        res = RDB_OK;

error:
    RDB_destroy_tuple(&tpl);
    RDB_destroy_value(&addval);
    for (i = 0; i < addc; i++)
        RDB_destroy_value(&svalv[i].val);
    _RDB_drop_qresult(qrp, txp);
    free(keyfv);
    free(nonkeyfv);
    free(svalv);
    return res;
}

static int
stored_qresult(RDB_qresult *qresp, RDB_table *tbp, RDB_transaction *txp)
{
    int res;

    /* !! delay after first call to _RDB_qresult_next() */
    res = RDB_recmap_cursor(&qresp->var.curp, tbp->var.stored.recmapp,
                    0, txp->txid);
    if (res != RDB_OK) {
        if (RDB_is_syserr(res))
            RDB_rollback(txp);
        return res;
    }
    res = RDB_cursor_first(qresp->var.curp);
    if (res == RDB_NOT_FOUND) {
        qresp->endreached = 1;
        res = RDB_OK;
    } else if (res != RDB_OK) {
        RDB_destroy_cursor(qresp->var.curp);
        if (RDB_is_syserr(res))
            RDB_rollback(txp);
        return res;
    }
    return res;
}

static int
summarize_qresult(RDB_qresult *qresp, RDB_transaction *txp)
{
    RDB_type *tuptyp = qresp->tbp->typ->var.basetyp;
    int res;

    /* create materialized table */
    res = _RDB_create_table(NULL, RDB_FALSE,
                        tuptyp->var.tuple.attrc,
                        tuptyp->var.tuple.attrv,
                        qresp->tbp->keyc, qresp->tbp->keyv,
                        txp, &qresp->matp);
    if (res != RDB_OK)
        return res;

    res = init_summ_table(qresp, txp);
    if (res != RDB_OK) {
        _RDB_drop_rtable(qresp->matp, txp);
        return res;
    }

    /* summarize over table 1 */
    res = do_summarize(qresp, txp);
    if (res != RDB_OK) {
        _RDB_drop_rtable(qresp->matp, txp);
        return res;
    }

    res = stored_qresult(qresp, qresp->matp, txp);

    if (res != RDB_OK) {
        _RDB_drop_rtable(qresp->matp, txp);
        return res;
    }

    return RDB_OK;
}

int
_RDB_table_qresult(RDB_table *tbp, RDB_qresult **qrespp, RDB_transaction *txp)
{
    RDB_qresult *qresp = malloc(sizeof (RDB_qresult));
    int res;
    
    if (qresp == NULL)
        return RDB_NO_MEMORY;
    *qrespp = qresp;
    qresp->tbp = tbp;
    qresp->endreached = 0;
    qresp->matp = NULL;
    qresp->var.virtual.qr2p = NULL;
    switch (tbp->kind) {
        case RDB_TB_STORED:
            return stored_qresult(qresp, tbp, txp);
            break;
        case RDB_TB_SELECT:
            res = _RDB_table_qresult(tbp->var.select.tbp,
                    &qresp->var.virtual.qrp, txp);
            break;
        case RDB_TB_SELECT_PINDEX:
            /* nothing special to do */
            return RDB_OK;
        case RDB_TB_UNION:
            res = _RDB_table_qresult(tbp->var._union.tbp1,
                    &qresp->var.virtual.qrp, txp);
            break;
        case RDB_TB_MINUS:
            res = _RDB_table_qresult(tbp->var.minus.tbp1,
                    &qresp->var.virtual.qrp, txp);
            if (res != RDB_OK)
                break;
            res = _RDB_table_qresult(tbp->var.minus.tbp2,
                    &qresp->var.virtual.qr2p, txp);
            break;
        case RDB_TB_INTERSECT:
            res = _RDB_table_qresult(tbp->var.intersect.tbp1,
                    &qresp->var.virtual.qrp, txp);
            if (res != RDB_OK)
                break;
            res = _RDB_table_qresult(tbp->var.minus.tbp2,
                    &qresp->var.virtual.qr2p, txp);
            break;
        case RDB_TB_JOIN:
            /* create qresults for each of the two tables */
            res = _RDB_table_qresult(tbp->var.join.tbp1,
                    &qresp->var.virtual.qrp, txp);
            if (res != RDB_OK)
                break;
            res = _RDB_table_qresult(tbp->var.join.tbp2,
                    &qresp->var.virtual.qr2p, txp);
            if (res != RDB_OK) {
                _RDB_drop_qresult(qresp->var.virtual.qr2p, txp);
                break;
            }
            qresp->var.virtual.tpl_valid = RDB_FALSE;
            break;
        case RDB_TB_EXTEND:
            res = _RDB_table_qresult(tbp->var.extend.tbp,
                    &qresp->var.virtual.qrp, txp);
            break;
        case RDB_TB_PROJECT:
            if (tbp->var.project.keyloss) {
                RDB_key_attrs keyattrs;
                int i;
                RDB_type *tuptyp = tbp->typ->var.basetyp;

                keyattrs.attrc = tuptyp->var.tuple.attrc;
                keyattrs.attrv = malloc(sizeof (char *) * keyattrs.attrc);
                
                for (i = 0; i < keyattrs.attrc; i++)
                    keyattrs.attrv[i] = tuptyp->var.tuple.attrv[i].name;

                /* Create materialized (all-key) table */
                res = _RDB_create_table(NULL, RDB_FALSE,
                        tuptyp->var.tuple.attrc,
                        tuptyp->var.tuple.attrv,
                        1, &keyattrs, txp, &qresp->matp);
                free(keyattrs.attrv);
                if (res != RDB_OK)
                    break;                    
            }

            res = _RDB_table_qresult(tbp->var.project.tbp,
                    &qresp->var.virtual.qrp, txp);
            break;
        case RDB_TB_SUMMARIZE:
            res = summarize_qresult(qresp, txp);
            break;
        case RDB_TB_RENAME:
            res = _RDB_table_qresult(tbp->var.rename.tbp,
                    &qresp->var.virtual.qrp, txp);
            break;
    }
    if (res != RDB_OK)
        free(qresp);
    return res;
}

static int
next_stored_tuple(RDB_qresult *qrp, RDB_table *tbp, RDB_tuple *tup)
{
    int i;
    int res;
    void *datap;
    size_t len;
    RDB_type *tuptyp = tbp->typ->var.basetyp;

    if (tup != NULL) {
        for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        RDB_value val;
            RDB_int fno;
            RDB_attr *attrp = &tuptyp->var.tuple.attrv[i];

            fno = *(RDB_int *)RDB_hashmap_get(&tbp->var.stored.attrmap,
                                          attrp->name, NULL);
            res = RDB_cursor_get(qrp->var.curp, fno, &datap, &len);
            if (res != RDB_OK) {
                return res;
            }
            RDB_init_value(&val);
            res = RDB_irep_to_value(&val, attrp->type, datap, len);
            if (res != RDB_OK) {
                return res;
            }
            res = RDB_tuple_set(tup, attrp->name, &val);
            if (res != RDB_OK) {
                return res;
            }
        }
    }
    res = RDB_cursor_next(qrp->var.curp);
    if (res == RDB_NOT_FOUND) {
        qrp->endreached = 1;
        return RDB_OK;
    }
    return res;
}

static int
next_join_tuple(RDB_qresult *qrp, RDB_tuple *tup, RDB_transaction *txp)
{
    int res;
    int i;
    
    /* tuple type of first ('outer') table */
    RDB_type *tpltyp1 = qrp->tbp->var.join.tbp1->typ->var.basetyp;
            
    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->var.virtual.tpl_valid) {
        RDB_init_tuple(&qrp->var.virtual.tpl);
        res = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl, txp);
        if (res != RDB_OK)
            return res;
        qrp->var.virtual.tpl_valid = RDB_TRUE;
    }
        
    for (;;) {
        /* read next 'inner' tuple */
        res = _RDB_next_tuple(qrp->var.virtual.qr2p, tup, txp);
        if (res != RDB_NOT_FOUND && res != RDB_OK) {
            return res;
        }
        if (res == RDB_OK) {
            /* compare common attributes */
            RDB_bool iseq = RDB_TRUE;

            for (i = 0; (i < qrp->tbp->var.join.common_attrc) && iseq; i++) {
                RDB_value *valp;
                RDB_value *valp2;

                valp = RDB_tuple_get(&qrp->var.virtual.tpl,
                        qrp->tbp->var.join.common_attrv[i]);
                valp2 = RDB_tuple_get(tup,
                        qrp->tbp->var.join.common_attrv[i]);

                /* if the attribute values are not equal, skip to next tuple */
                if (!RDB_value_equals(valp, valp2))
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
        _RDB_drop_qresult(qrp->var.virtual.qr2p, txp);
        res = _RDB_table_qresult(qrp->tbp->var.join.tbp2,
                &qrp->var.virtual.qr2p, txp);
        if (res != RDB_OK) {
            return res;
        }

        /* read next 'outer' tuple */
        res = _RDB_next_tuple(qrp->var.virtual.qrp, &qrp->var.virtual.tpl, txp);
        if (res != RDB_OK) {
            return res;
        }
    }      
    
    /* join the two tuples into tup */
    for (i = 0; i < tpltyp1->var.tuple.attrc; i++) {
         res = RDB_tuple_set(tup, tpltyp1->var.tuple.attrv[i].name,
                       RDB_tuple_get(&qrp->var.virtual.tpl,
                       tpltyp1->var.tuple.attrv[i].name));
         if (res != RDB_OK) {
             return res;
         }
    }
    
    return RDB_OK;
}

static int
get_by_pindex(RDB_table *tbp, RDB_value valv[], RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_field *fv;
    RDB_field *resfv;
    int i;
    int res;
    RDB_type *tpltyp = tbp->typ->var.basetyp;
    int pkeylen = _RDB_pkey_len(tbp);

    resfv = malloc(sizeof (RDB_field)
            * (tpltyp->var.tuple.attrc - pkeylen));
    fv = malloc(sizeof (RDB_field) * pkeylen);
    if (fv == NULL || resfv == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    
    for (i = 0; i < pkeylen; i++) {
        fv[i].datap = RDB_value_irep(&valv[i], &fv[i].len);
    }
    for (i = 0; i < tpltyp->var.tuple.attrc - pkeylen; i++) {
        resfv[i].no = pkeylen + i;
    }
    res = RDB_get_fields(tbp->var.stored.recmapp, fv,
                         tpltyp->var.tuple.attrc - pkeylen,
                         txp->txid, resfv);
    if (res != RDB_OK) {
        if (RDB_is_syserr(res)) {
            RDB_rollback(txp);
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
            res = RDB_tuple_set(tup, attrname, &valv[fno]);
            if (res != RDB_OK)
                return res;
        } else {
            /* non-key attribute */
            RDB_value val;

            RDB_init_value(&val);
            res = RDB_irep_to_value(&val, tpltyp->var.tuple.attrv[i].type,
                    resfv[fno - pkeylen].datap, resfv[fno - pkeylen].len);
            if (res != RDB_OK) {
                RDB_destroy_value(&val);
                goto error;
            }
            res = RDB_tuple_set(tup, attrname, &val);
            RDB_destroy_value(&val);
            if (res != RDB_OK)
                goto error;
        }
    }
    res = RDB_OK;
error:
    free(fv);
    free(resfv);
    return res;
}

static int
next_select_pindex(RDB_qresult *qrp, RDB_tuple *tup, RDB_transaction *txp)
{
    return get_by_pindex(qrp->tbp->var.select.tbp, &qrp->tbp->var.select.val,
                         tup, txp);
}

static int
next_project_tuple(RDB_qresult *qrp, RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_tuple tpl;
    int i, res;
    RDB_value *valp;
    RDB_type *tuptyp = qrp->tbp->typ->var.basetyp;
            
    RDB_init_tuple(&tpl);

    do {            
        /* Get tuple */
        res = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, txp);
        if (res != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return res;
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
            res = RDB_insert(qrp->matp, &tpl, txp);
        } else {
            res = RDB_OK;
        }
    } while (res == RDB_ELEMENT_EXISTS);

    RDB_destroy_tuple(&tpl);
    return RDB_OK;
}

static int
next_rename_tuple(RDB_qresult *qrp, RDB_tuple *tup, RDB_transaction *txp)
{
    RDB_tuple tpl;
    int res;

    RDB_init_tuple(&tpl);
    res = _RDB_next_tuple(qrp->var.virtual.qrp, &tpl, txp);
    if (res != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        return res;
    }
    res = RDB_tuple_rename(&tpl, qrp->tbp->var.rename.renc,
                           qrp->tbp->var.rename.renv, tup);
    RDB_destroy_tuple(&tpl);
    return res;
}

int
_RDB_next_tuple(RDB_qresult *qrp, RDB_tuple *tup, RDB_transaction *txp)
{
    int res;
    RDB_table *tbp = qrp->tbp;

    if (qrp->endreached)
        return RDB_NOT_FOUND;
    switch (tbp->kind) {
        RDB_bool expres;

        case RDB_TB_STORED:
            return next_stored_tuple(qrp, qrp->tbp, tup);
        case RDB_TB_SELECT:
            do {
                res = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
                if (res != RDB_OK)
                    break;
                res = RDB_evaluate_bool(tbp->var.select.exprp, tup, txp, &expres);
                if (res != RDB_OK)
                    break;
            } while (!expres);
            if (res == RDB_NOT_FOUND) {
                return RDB_NOT_FOUND;
            } else if (res != RDB_OK) {
                return res;
            }
            break;
        case RDB_TB_SELECT_PINDEX:
            res = next_select_pindex(qrp, tup, txp);
            qrp->endreached = 1;
            if (res != RDB_OK)
                return res;
            break;
        case RDB_TB_UNION:
            for(;;) {
                if (qrp->var.virtual.qr2p == NULL) {
                    res = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
                    if (res != RDB_NOT_FOUND)
                        return res;
                    /* switch to second table */
                    res = _RDB_table_qresult(tbp->var._union.tbp2,
                                &qrp->var.virtual.qr2p, txp);
                    if (res != RDB_OK)
                        return res;
                } else {
                    res = _RDB_next_tuple(qrp->var.virtual.qr2p, tup, txp);
                    if (res != RDB_OK)
                        return res;
                    /* skip tuples which are contained in the first table */
                    res = _RDB_qresult_contains(qrp->var.virtual.qrp,
                                tup, txp);
                    if (res == RDB_NOT_FOUND)
                        return RDB_OK;
                    if (res != RDB_OK)
                        return res;
                }
            };
            break;
        case RDB_TB_MINUS:
            do {
                res = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
                if (res != RDB_OK)
                    return res;
                res = _RDB_qresult_contains(qrp->var.virtual.qr2p, tup, txp);
                if (res != RDB_OK && res != RDB_NOT_FOUND) {
                    return res;
                }
            } while (res == RDB_OK);
            break;
        case RDB_TB_INTERSECT:
            do {
                res = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
                if (res != RDB_OK)
                    return res;
                res = _RDB_qresult_contains(qrp->var.virtual.qr2p, tup, txp);
                if (res != RDB_OK && res != RDB_NOT_FOUND) {
                    return res;
                }
            } while (res == RDB_NOT_FOUND);
            break;
        case RDB_TB_JOIN:
            return next_join_tuple(qrp, tup, txp);
        case RDB_TB_EXTEND:
            res = _RDB_next_tuple(qrp->var.virtual.qrp, tup, txp);
            if (res != RDB_OK)
                return res;
            res = RDB_tuple_extend(tup, tbp->var.extend.attrc,
                     tbp->var.extend.attrv, txp);
            if (res != RDB_OK)
                return res;
            break;
        case RDB_TB_PROJECT:
            return next_project_tuple(qrp, tup, txp);
        case RDB_TB_SUMMARIZE:
            {
                int i;
                char *cname;
                RDB_int count;
            
                res = next_stored_tuple(qrp, qrp->matp, tup);
                if (res != RDB_OK)
                    return res;
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
    }
    return RDB_OK;
}

int
_RDB_qresult_contains(RDB_qresult *qrp, const RDB_tuple *tup,
                      RDB_transaction *txp)
{
    int i;
    int res;
    RDB_value *valv;
    int kattrc;
    RDB_tuple tpl;

    if (qrp->tbp->kind != RDB_TB_SUMMARIZE)
        return RDB_table_contains(qrp->tbp, tup, txp);

    /* Check if SUMMARIZE table contains the tuple by
     * getting the non-key attributes and comparing them.
     * (RDB_recmap_contains() cannot be used in all cases because
     * of the additional count attributes for AVG).
     */

    kattrc = _RDB_pkey_len(qrp->tbp);
    valv = malloc(sizeof (RDB_value) * kattrc);
    if (valv == NULL)
        return RDB_NO_MEMORY;

    RDB_init_tuple(&tpl);

    for (i = 0; i < kattrc; i++) {
        RDB_init_value(&valv[i]);
    }

    for (i = 0; i < kattrc; i++) {
        res = RDB_copy_value(&valv[i], RDB_tuple_get(
                tup, qrp->matp->keyv[0].attrv[i]));
        if (res != RDB_OK)
            goto error;
    }
        
    res = get_by_pindex(qrp->matp, valv, &tpl, txp);
    if (res != RDB_OK) /* handles RDB_NOT_FOUND too */
        goto error;

    /* compare ADD attributes */
    for (i = 0; i < qrp->tbp->var.summarize.addc; i++) {
        char *attrname = qrp->tbp->var.summarize.addv[i].name;

        if (!RDB_value_equals(RDB_tuple_get(tup, attrname),
                RDB_tuple_get(&tpl, attrname))) {
            res = RDB_NOT_FOUND;
            goto error;
        }
    }
    res = RDB_OK;

error:
    RDB_destroy_tuple(&tpl);
    for (i = 0; i < kattrc; i++)
        RDB_destroy_value(&valv[i]);
    free(valv);
    
    return res;
}

int
_RDB_drop_qresult(RDB_qresult *qrp, RDB_transaction *txp)
{
    int res;

    if (qrp->tbp->kind == RDB_TB_STORED || qrp->tbp->kind == RDB_TB_SUMMARIZE) {
        res = RDB_destroy_cursor(qrp->var.curp);
        if (RDB_is_syserr(res))
            RDB_rollback(txp);
    } else if (qrp->tbp->kind != RDB_TB_SELECT_PINDEX) {
        res = _RDB_drop_qresult(qrp->var.virtual.qrp, txp);
        if (qrp->tbp->kind == RDB_TB_JOIN) {
            if (qrp->var.virtual.tpl_valid)
                RDB_destroy_tuple(&qrp->var.virtual.tpl);
        }
    }

    if (qrp->var.virtual.qr2p != NULL)
        _RDB_drop_qresult(qrp->var.virtual.qr2p, txp);
    if (qrp->matp != NULL)
        _RDB_drop_rtable(qrp->matp, txp);

    free(qrp);
    return res;
}
