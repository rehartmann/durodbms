/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "typeimpl.h"

int
_RDB_table_qresult(RDB_table *tbp, RDB_qresult **qrespp,
        RDB_transaction *txp)
{
    RDB_qresult *qresp = malloc(sizeof (RDB_qresult));
    int res;
    
    if (qresp == NULL)
        return RDB_NO_MEMORY;
    *qrespp = qresp;
    qresp->tablep = tbp;
    qresp->endreached = 0;
    qresp->txp = txp;
    qresp->matp = NULL;
    switch (tbp->kind) {
        case RDB_TB_STORED:
            res = RDB_recmap_cursor(&qresp->var.curp, tbp->var.stored.recmapp,
                    0, txp->txid);
            if (res != RDB_OK)
                break;
            res = RDB_cursor_first(qresp->var.curp);
            if (res == RDB_NOT_FOUND) {
                qresp->endreached = 1;
                res = RDB_OK;
            }
            else if (res != RDB_OK) {
                RDB_destroy_cursor(qresp->var.curp);
            }
            break;
        case RDB_TB_SELECT:
            res = _RDB_table_qresult(tbp->var.select.tbp,
                    &qresp->var.virtual.itp, txp);
            break;
        case RDB_TB_SELECT_PINDEX:
            /* nothing special to do */
            return RDB_OK;
        case RDB_TB_UNION:
            qresp->var.virtual.tbno = 0;
            res = _RDB_table_qresult(tbp->var._union.tbp1,
                    &qresp->var.virtual.itp, txp);
            break;
        case RDB_TB_MINUS:
            res = _RDB_table_qresult(tbp->var.minus.tbp1,
                    &qresp->var.virtual.itp, txp);
            break;
        case RDB_TB_INTERSECT:
            res = _RDB_table_qresult(tbp->var.intersect.tbp1,
                    &qresp->var.virtual.itp, txp);
            break;
        case RDB_TB_JOIN:
            /* create qresults for each of the two tables */
            res = _RDB_table_qresult(tbp->var.join.tbp1,
                    &qresp->var.virtual.itp, txp);
            if (res != RDB_OK)
                break;
            res = _RDB_table_qresult(tbp->var.join.tbp2,
                    &qresp->var.virtual.nestedp, txp);
            if (res != RDB_OK) {
                _RDB_drop_qresult(qresp->var.virtual.nestedp);
                break;
            }
            qresp->var.virtual.tpl_valid = RDB_FALSE;
            break;
        case RDB_TB_EXTEND:
            res = _RDB_table_qresult(tbp->var.extend.tbp,
                    &qresp->var.virtual.itp, txp);
            break;
        case RDB_TB_PROJECT:
            if (tbp->var.project.keyloss) {
                RDB_key_attrs keyattrs;
                int i;
                RDB_type *tuptyp = tbp->typ->complex.basetyp;

                keyattrs.attrc = tuptyp->complex.tuple.attrc;
                keyattrs.attrv = malloc(sizeof (char *) * keyattrs.attrc);
                
                for (i = 0; i < keyattrs.attrc; i++)
                    keyattrs.attrv[i] = tuptyp->complex.tuple.attrv[i].name;

                /* Create materialized (all-key) table */
                res = _RDB_create_table(NULL, RDB_FALSE,
                        tuptyp->complex.tuple.attrc,
                        tuptyp->complex.tuple.attrv,
                        1, &keyattrs, txp, &qresp->matp);
                free(keyattrs.attrv);
                if (res != RDB_OK)
                    break;                    
            }

            res = _RDB_table_qresult(tbp->var.project.tbp,
                    &qresp->var.virtual.itp, txp);
            break;
    }
    if (res != RDB_OK)
        free(qresp);
    return res;
}

static int
next_stored_tuple(RDB_qresult *itp, RDB_tuple *tup)
{
    int i;
    int res;
    void *datap;
    size_t len;
    RDB_table *tbp = itp->tablep;
    RDB_type *tpltyp = tbp->typ->complex.basetyp;

    for (i = 0; i < tpltyp->complex.tuple.attrc; i++) {
        RDB_value val;

        res = RDB_cursor_get(itp->var.curp, i, &datap, &len);
        if (res != 0) {
            return res;
        }
        RDB_init_value(&val);
        res = RDB_irep_to_value(&val, tpltyp->complex.tuple.attrv[i].type,
                                datap, len);
        if (res != RDB_OK) {
            return res;
        }
        res = RDB_tuple_set(tup, tpltyp->complex.tuple.attrv[i].name, &val);
        if (res != RDB_OK) {
            return res;
        }
    }
    res = RDB_cursor_next(itp->var.curp);
    if (res == RDB_NOT_FOUND) {
        itp->endreached = 1;
        return RDB_OK;
    }
    return res;
}

static int
next_join_tuple(RDB_qresult *itp, RDB_tuple *tup)
{
    int res;
    int i;
    
    /* tuple type of first ('outer') table */
    RDB_type *tpltyp1 = itp->tablep->var.join.tbp1->typ->complex.basetyp;
            
    /* read first 'outer' tuple, if it's the first invocation */
    if (!itp->var.virtual.tpl_valid) {
        RDB_init_tuple(&itp->var.virtual.tpl);
        res = _RDB_next_tuple(itp->var.virtual.itp, &itp->var.virtual.tpl);
        if (res != RDB_OK)
            return res;
        itp->var.virtual.tpl_valid = RDB_TRUE;
    }
        
    for (;;) {
        /* read next 'inner' tuple */
        res = _RDB_next_tuple(itp->var.virtual.nestedp, tup);
        if (res != RDB_NOT_FOUND && res != RDB_OK) {
            return res;
        }
        if (res == RDB_OK) {
            /* compare common attributes */
            RDB_bool iseq = RDB_TRUE;

            for (i = 0; (i < itp->tablep->var.join.common_attrc) && iseq; i++) {
                RDB_value *valp;
                RDB_value *valp2;

                valp = RDB_tuple_get(&itp->var.virtual.tpl,
                        itp->tablep->var.join.common_attrv[i]);
                valp2 = RDB_tuple_get(tup,
                        itp->tablep->var.join.common_attrv[i]);

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
        _RDB_drop_qresult(itp->var.virtual.nestedp);
        res = _RDB_table_qresult(itp->tablep->var.join.tbp2,
                &itp->var.virtual.nestedp, itp->txp);
        if (res != RDB_OK) {
            return res;
        }

        /* read next 'outer' tuple */
        res = _RDB_next_tuple(itp->var.virtual.itp, &itp->var.virtual.tpl);
        if (res != RDB_OK) {
            return res;
        }
    }      
    
    /* join the two tuples into tup */
    for (i = 0; i < tpltyp1->complex.tuple.attrc; i++) {
         res = RDB_tuple_set(tup, tpltyp1->complex.tuple.attrv[i].name,
                       RDB_tuple_get(&itp->var.virtual.tpl,
                       tpltyp1->complex.tuple.attrv[i].name));
         if (res != RDB_OK) {
             return res;
         }
    }
    
    return RDB_OK;
}

static int
next_select_pindex(RDB_qresult *itp, RDB_tuple *tup)
{
    int res;
    int i;
    RDB_field fval;
    RDB_type *tpltyp = itp->tablep->typ->complex.basetyp;
    RDB_field *resfieldv = malloc(sizeof (RDB_field)
            * (tpltyp->complex.tuple.attrc - 1));

    if (resfieldv == NULL)
        return RDB_NO_MEMORY;
    
    fval.datap = RDB_value_irep(&itp->tablep->var.select.val, &fval.len);

    for (i = 1; i < tpltyp->complex.tuple.attrc; i++) {
        resfieldv[i - 1].no = i;
    }
    
    res = RDB_get_fields(itp->tablep->var.select.tbp->var.stored.recmapp,
                         &fval, tpltyp->complex.tuple.attrc - 1,
                         itp->txp->txid, resfieldv);
    if (res != RDB_OK) {
        free(resfieldv);
        if (res == RDB_DEADLOCK) {
            RDB_rollback(itp->txp);
            res = RDB_DEADLOCK;
        }
        return res;
    }

    /* set key field */
    res = RDB_tuple_set(tup, tpltyp->complex.tuple.attrv[0].name,
                 &itp->tablep->var.select.val);
    if (res != RDB_OK) {
        return res;
    }

    /* set data fields */
    for (i = 1; i < tpltyp->complex.tuple.attrc; i++) {
        RDB_value val;

        RDB_init_value(&val);
        res = RDB_irep_to_value(&val, tpltyp->complex.tuple.attrv[i].type,
                resfieldv[i - 1].datap, resfieldv[i - 1].len);
        if (res != RDB_OK) {
            free(resfieldv);
            return res;
        }
        res = RDB_tuple_set(tup, tpltyp->complex.tuple.attrv[i].name, &val);
        if (res != RDB_OK) {
            free(resfieldv);
            return res;
        }
    }
    free(resfieldv);
    return RDB_OK;
}

static int
next_project_tuple(RDB_qresult *itp, RDB_tuple *tup)
{
    RDB_tuple tpl;
    int i, res;
    RDB_value *valp;
    RDB_type *tuptyp = itp->tablep->typ->complex.basetyp;
            
    RDB_init_tuple(&tpl);

    do {            
        /* Get tuple */
        res = _RDB_next_tuple(itp->var.virtual.itp, &tpl);
        if (res != RDB_OK) {
            RDB_deinit_tuple(&tpl);
            return res;
        }
            
        /* Copy attributes into new tuple */
        for (i = 0; i < tuptyp->complex.tuple.attrc; i++) {
            char *attrnamp = tuptyp->complex.tuple.attrv[i].name;
            
            valp = RDB_tuple_get(&tpl, attrnamp);
            RDB_tuple_set(tup, attrnamp, valp);
        }
            
        /* eliminate duplicates, if necessary */
        if (itp->tablep->var.project.keyloss) {
            res = RDB_insert(itp->matp, tup, itp->txp);
        } else {
            res = RDB_OK;
        }
    } while (res == RDB_ELEMENT_EXISTS);

    return RDB_OK;
}

int
_RDB_next_tuple(RDB_qresult *itp, RDB_tuple *tup)
{
    int res;
    RDB_table *tbp = itp->tablep;

    if (itp->endreached)
        return RDB_NOT_FOUND;
    switch (tbp->kind) {
        RDB_bool expres;

        case RDB_TB_STORED:
            res = next_stored_tuple(itp, tup);
            if (res != RDB_OK)
                return res;
            break;
        case RDB_TB_SELECT:
            do {
                res = _RDB_next_tuple(itp->var.virtual.itp, tup);
                if (res != RDB_OK)
                    break;
                res = RDB_evaluate_bool(tbp->var.select.exprp, tup, itp->txp,
                                        &expres);
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
            res = next_select_pindex(itp, tup);
            itp->endreached = 1;
            if (res != RDB_OK)
                return res;
            break;
        case RDB_TB_UNION:
            for(;;) {
                res = _RDB_next_tuple(itp->var.virtual.itp, tup);
                if (res != RDB_OK) {
                    if (res != RDB_NOT_FOUND)
                        return res;
                    if (itp->var.virtual.tbno == 1)
                        return RDB_NOT_FOUND;

                    /* switch to second table */
                    itp->var.virtual.tbno = 1;
                    _RDB_drop_qresult(itp->var.virtual.itp);
                    res = _RDB_table_qresult(tbp->var._union.tbp2,
                            &itp->var.virtual.itp, itp->txp);
                    if (res != RDB_OK)
                        return res;
                } else {
                    if (itp->var.virtual.tbno == 0)
                        return RDB_OK;
                    else {
                        /* skip tuples which are contained in the first table */
                        res = RDB_table_contains(tbp->var._union.tbp1, tup,
                                itp->txp);
                        if (res == RDB_NOT_FOUND)
                            return RDB_OK;
                        if (res != RDB_OK)
                            return res;
                    }
                }
            };
            break;
        case RDB_TB_MINUS:
            do {
                res = _RDB_next_tuple(itp->var.virtual.itp, tup);
                if (res != RDB_OK)
                    return res;
                res = RDB_table_contains(tbp->var.minus.tbp2, tup, itp->txp);
                if (res != RDB_OK && res != RDB_NOT_FOUND) {
                    return res;
                }
            } while (res == RDB_OK);
            break;
        case RDB_TB_INTERSECT:
            do {
                res = _RDB_next_tuple(itp->var.virtual.itp, tup);
                if (res != RDB_OK)
                    return res;
                res = RDB_table_contains(tbp->var.intersect.tbp2, tup, itp->txp);
                if (res != RDB_OK && res != RDB_NOT_FOUND) {
                    return res;
                }
            } while (res == RDB_NOT_FOUND);
            break;
        case RDB_TB_JOIN:
            res = next_join_tuple(itp, tup);
            if (res != RDB_OK)
                return res;
            break;
        case RDB_TB_EXTEND:
            res = _RDB_next_tuple(itp->var.virtual.itp, tup);
            if (res != RDB_OK)
                return res;
            res = RDB_tuple_extend(tup, tbp->var.extend.attrc,
                     tbp->var.extend.attrv, itp->txp);
            if (res != RDB_OK)
                return res;
            break;
        case RDB_TB_PROJECT:
            return next_project_tuple(itp, tup);
    }
    return RDB_OK;
}

int
_RDB_drop_qresult(RDB_qresult *itp)
{
    int res;

    if (itp->tablep->kind == RDB_TB_STORED) {
        res = RDB_destroy_cursor(itp->var.curp);
        if (res == RDB_DEADLOCK)
            RDB_rollback(itp->txp);
    } else if (itp->tablep->kind != RDB_TB_SELECT_PINDEX) {
        res = _RDB_drop_qresult(itp->var.virtual.itp);
        if (itp->tablep->kind == RDB_TB_JOIN) {
            res = _RDB_drop_qresult(itp->var.virtual.nestedp);
            if (itp->var.virtual.tpl_valid)
                RDB_deinit_tuple(&itp->var.virtual.tpl);
        }
    }

    if (itp->matp != NULL)
        _RDB_drop_rtable(itp->matp, itp->txp);

    free(itp);
    return res;
}
