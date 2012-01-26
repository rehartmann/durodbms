/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "qresult.h"
#include "insert.h"
#include "internal.h"
#include "delete.h"
#include "stable.h"
#include "typeimpl.h"
#include <gen/hashtabit.h>
#include <gen/strfns.h>
#include <string.h>
#include <assert.h>

static int
init_qresult(RDB_qresult *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

RDB_qresult *
_RDB_expr_qresult(RDB_expression *exp, RDB_exec_context *,
        RDB_transaction *);

static int
join_qresult(RDB_qresult *qrp, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *arg2p;
    
    /* Create qresult for the first table */   
    qrp->exp = exp;
    qrp->nested = RDB_TRUE;
    qrp->var.children.qrp = _RDB_expr_qresult(qrp->exp->var.op.args.firstp,
            ecp, txp);
    if (qrp->var.children.qrp == NULL)
        return RDB_ERROR;

    qrp->var.children.tpl_valid = RDB_FALSE;

    /* Create qresult for 2nd table, except if the primary index is used */
    arg2p = qrp->exp->var.op.args.firstp->nextp;
    if (arg2p->kind != RDB_EX_TBP || arg2p->var.tbref.indexp == NULL
            || !arg2p->var.tbref.indexp->unique) {
        qrp->var.children.qr2p = _RDB_expr_qresult(arg2p, ecp, txp);
        if (qrp->var.children.qr2p == NULL) {
            _RDB_drop_qresult(qrp->var.children.qrp, ecp, txp);
            return RDB_ERROR;
        }
    } else {
        qrp->var.children.qr2p = NULL;
    }
    return RDB_OK;
}

struct _RDB_summval {
    RDB_object val;
};

static void
summ_step(struct _RDB_summval *svalp, const RDB_object *addvalp,
        const char *opname, RDB_int count)
{
    if (strcmp(opname, "COUNT") == 0) {
        svalp->val.var.int_val++;
    } else if (strcmp(opname, "AVG") == 0) {
        svalp->val.var.float_val =
                (svalp->val.var.float_val * count
                + addvalp->var.float_val)
                / (count + 1);
    } else if (strcmp(opname, "SUM") == 0) {
            if (svalp->val.typ == &RDB_INTEGER)
                svalp->val.var.int_val += addvalp->var.int_val;
            else
                svalp->val.var.float_val += addvalp->var.float_val;
    } else if (strcmp(opname, "MAX") == 0) {
            if (svalp->val.typ == &RDB_INTEGER) {
                if (addvalp->var.int_val > svalp->val.var.int_val)
                    svalp->val.var.int_val = addvalp->var.int_val;
            } else {
                if (addvalp->var.float_val > svalp->val.var.float_val)
                    svalp->val.var.float_val = addvalp->var.float_val;
            }
    } else if (strcmp(opname, "MIN") == 0) {
        if (svalp->val.typ == &RDB_INTEGER) {
            if (addvalp->var.int_val < svalp->val.var.int_val)
                svalp->val.var.int_val = addvalp->var.int_val;
        } else {
            if (addvalp->var.float_val < svalp->val.var.float_val)
                svalp->val.var.float_val = addvalp->var.float_val;
        }
    } else if (strcmp(opname, "ANY") == 0) {
        if (addvalp->var.bool_val)
            svalp->val.var.bool_val = RDB_TRUE;
    } else if (strcmp(opname, "ALL") == 0) {
        if (!addvalp->var.bool_val)
            svalp->val.var.bool_val = RDB_FALSE;
    }
}

static int
do_summarize(RDB_qresult *qrp, RDB_type *tb1typ, RDB_bool hasavg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *lqrp;
    RDB_object tpl;
    RDB_field *keyfv, *nonkeyfv;
    int ret;
    struct _RDB_summval *svalv;
    RDB_expression *argp;
    int keyfc = _RDB_pkey_len(qrp->matp);
    int addc = (RDB_expr_list_length(&qrp->exp->var.op.args) - 2) / 2;
    int i;
    int avgc = hasavg ? 1 : 0;

    keyfv = RDB_alloc(sizeof (RDB_field) * keyfc, ecp);
    nonkeyfv = RDB_alloc(sizeof (RDB_field) * (addc + avgc), ecp);
    svalv = RDB_alloc(sizeof (struct _RDB_summval) * addc, ecp);
    if (keyfv == NULL || nonkeyfv == NULL || svalv == NULL) {
        RDB_free(keyfv);
        RDB_free(nonkeyfv);
        RDB_free(svalv);
        return RDB_ERROR;
    }

    /*
     * Iterate over table 1, modifying the materialized table
     */

    lqrp = _RDB_expr_qresult(qrp->exp->var.op.args.firstp, ecp, txp);
    if (lqrp == NULL) {
        return RDB_ERROR;
    }
    ret = _RDB_duprem(lqrp, ecp, txp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    for (i = 0; i < addc; i++)
        RDB_init_obj(&svalv[i].val);
    do {
        int fvidx;
        RDB_int count;

        ret = _RDB_next_tuple(lqrp, &tpl, ecp, txp);
        if (ret == RDB_OK) {
            /* Build key */
            for (i = 0; i < keyfc; i++) {
                RDB_object *attrobjp = RDB_tuple_get(&tpl,
                                qrp->matp->var.tb.keyv[0].strv[i]);
                attrobjp->store_typ = attrobjp->typ;
                ret = _RDB_obj_to_field(&keyfv[i], attrobjp, ecp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
            }

            /* Read added attributes from table #2 */
            argp = qrp->exp->var.op.args.firstp->nextp;
            for (i = 0; i < addc; i++) {
                argp = argp->nextp->nextp;

                nonkeyfv[i].no = *_RDB_field_no(qrp->matp->var.tb.stp,
                        RDB_obj_string(&argp->var.obj));
            }

            if (hasavg) {
                nonkeyfv[addc].no = *_RDB_field_no(qrp->matp->var.tb.stp,
                                AVG_COUNT);
                fvidx = addc;
            }

            ret = RDB_get_fields(qrp->matp->var.tb.stp->recmapp, keyfv,
                    addc + avgc, NULL, nonkeyfv);
            if (ret == RDB_OK) {
                /* If AVG, get count */
                if (hasavg) {
                    memcpy(&count, nonkeyfv[fvidx].datap, sizeof(RDB_int));
                }

                /* A corresponding tuple in table 2 has been found */
                argp = qrp->exp->var.op.args.firstp->nextp->nextp;
                for (i = 0; i < addc; i++) {
                    RDB_type *typ;
                    RDB_object addval;
                    char *opname = argp->var.op.name;

                    RDB_init_obj(&addval);
                    if (strcmp(opname, "COUNT") == 0) {
                        ret = RDB_irep_to_obj(&svalv[i].val, &RDB_INTEGER,
                                nonkeyfv[i].datap, nonkeyfv[i].len, ecp);
                    } else {
                        RDB_expression *exp = argp->var.op.args.firstp;
                        typ = _RDB_expr_type(exp, tb1typ->var.basetyp, ecp, txp);
                        if (typ == NULL) {
                            RDB_destroy_obj(&addval, ecp);
                            ret = RDB_ERROR;
                            goto cleanup;
                        }
                        ret = RDB_irep_to_obj(&svalv[i].val, typ,
                                nonkeyfv[i].datap, nonkeyfv[i].len, ecp);
                        if (ret != RDB_OK) {
                            RDB_destroy_obj(&addval, ecp);
                            goto cleanup;
                        }
                        ret = RDB_evaluate(exp, &_RDB_tpl_get, &tpl, ecp, txp,
                                &addval);
                        if (ret != RDB_OK) {
                            RDB_destroy_obj(&addval, ecp);
                            goto cleanup;
                        }
                    }
                    summ_step(&svalv[i], &addval, opname, count);
                    RDB_destroy_obj(&addval, ecp);

                    svalv[i].val.store_typ = svalv[i].val.typ;
                    ret = _RDB_obj_to_field(&nonkeyfv[i], &svalv[i].val, ecp);
                    if (ret != RDB_OK)
                        return RDB_ERROR;
                    argp = argp->nextp->nextp;
                }
                if (hasavg) {
                    /* Store count */
                    count++;
                    nonkeyfv[fvidx].datap = &count;
                    nonkeyfv[fvidx].len = sizeof(RDB_int);
                    nonkeyfv[fvidx].copyfp = memcpy;
                }

                _RDB_cmp_ecp = ecp;
                ret = RDB_update_rec(qrp->matp->var.tb.stp->recmapp, keyfv,
                        addc + avgc, nonkeyfv, NULL);
                if (ret != RDB_OK) {
                    RDB_errcode_to_error(ret, ecp, txp);
                    goto cleanup;
                }
            } else {
                RDB_errcode_to_error(ret, ecp, txp);
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
    for (i = 0; i < addc; i++)
        RDB_destroy_obj(&svalv[i].val, ecp);
    _RDB_drop_qresult(lqrp, ecp, txp);
    RDB_free(keyfv);
    RDB_free(nonkeyfv);
    RDB_free(svalv);

    return ret;
}

static int
init_stored_qresult(RDB_qresult *qrp, RDB_object *tbp, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    qrp->exp = exp;
    qrp->nested = RDB_FALSE;
    qrp->var.stored.tbp = tbp;
    if (tbp->var.tb.stp == NULL) {
        /*
         * Table has no physical representation, which means it is empty
         */
        qrp->endreached = RDB_TRUE;
        qrp->var.stored.curp = NULL;
        return RDB_OK;
    }

    /* !! delay after first call to _RDB_qresult_next()? */
    ret = RDB_recmap_cursor(&qrp->var.stored.curp, tbp->var.tb.stp->recmapp,
                    RDB_FALSE, tbp->var.tb.is_persistent ? txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        return RDB_ERROR;
    }
    ret = RDB_cursor_first(qrp->var.stored.curp);
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
init_summ_table(RDB_qresult *qrp, RDB_type *tb1typ, RDB_bool hasavg, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_qresult *lqrp;
    RDB_object tpl;
    RDB_expression *argp;

    /*
     * Initialize table from table #2
     */

    lqrp = _RDB_expr_qresult(qrp->exp->var.op.args.firstp->nextp, ecp, txp);
    if (lqrp == NULL) {
        return RDB_ERROR;
    }
    ret = _RDB_duprem(lqrp, ecp, txp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    for(;;) {
        ret = _RDB_next_tuple(lqrp, &tpl, ecp, txp);
        if (ret != RDB_OK)
            break;

        /* Extend tuple */
        argp = qrp->exp->var.op.args.firstp->nextp->nextp;
        while (argp != NULL) {
            char *name = RDB_obj_string(&argp->nextp->var.obj);
            RDB_expression *opexp = argp;
            char *opname = opexp->var.op.name;
            RDB_type *typ;

            if (strcmp(opname, "COUNT") == 0) {
                ret = RDB_tuple_set_int(&tpl, name, 0, ecp);
            } else if (strcmp(opname, "AVG") == 0) {
                ret = RDB_tuple_set_float(&tpl, name, 0, ecp);
            } else if (strcmp(opname, "SUM") == 0) {
                typ = _RDB_expr_type(opexp->var.op.args.firstp,
                        tb1typ->var.basetyp, ecp, txp);
                if (typ == NULL)
                    goto error;
                if (typ == &RDB_INTEGER)
                    ret = RDB_tuple_set_int(&tpl, name, 0, ecp);
                else
                    ret = RDB_tuple_set_float(&tpl, name, 0.0, ecp);
            } else if (strcmp(opname, "MAX") == 0) {
                typ = _RDB_expr_type(opexp->var.op.args.firstp,
                        tb1typ->var.basetyp,
                        ecp, txp);
                if (typ == NULL)
                    goto error;
                if (typ == &RDB_INTEGER)
                    ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MIN, ecp);
                else {
                    ret = RDB_tuple_set_float(&tpl, name,
                            RDB_FLOAT_MIN, ecp);
                }
            } else if (strcmp(opname, "MIN") == 0) {
                typ = _RDB_expr_type(opexp->var.op.args.firstp,
                        tb1typ->var.basetyp, ecp, txp);
                if (typ == NULL)
                    goto error;
                if (typ == &RDB_INTEGER)
                    ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MAX, ecp);
                else {
                    ret = RDB_tuple_set_float(&tpl, name,
                            RDB_FLOAT_MAX, ecp);
                }
            } else if (strcmp(opname, "ALL") == 0) {
                ret = RDB_tuple_set_bool(&tpl, name, RDB_TRUE, ecp);
            } else if (strcmp(opname, "ANY") == 0) {
                ret = RDB_tuple_set_bool(&tpl, name, RDB_FALSE, ecp);
            }
            if (ret != RDB_OK)
                goto error;
            argp = argp->nextp->nextp;
        } /* for */

        if (hasavg) {
            if (RDB_tuple_set_int(&tpl, AVG_COUNT, 0, ecp) != RDB_OK)
                goto error;
        }

        ret = RDB_insert(qrp->matp, &tpl, ecp, txp);
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
    return _RDB_drop_qresult(lqrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    _RDB_drop_qresult(lqrp, ecp, txp);

    return RDB_ERROR;
}

static int
summarize_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    RDB_bool hasavg;
    RDB_string_vec key;
    RDB_expression *argp;
    RDB_type *tb1typ;
    RDB_type *tb2typ = NULL;
    RDB_type *reltyp = NULL;

    key.strv = NULL;

    qrp->matp = NULL;
    qrp->exp = exp;
    qrp->nested = RDB_FALSE;

    tb1typ = RDB_expr_type(exp->var.op.args.firstp, NULL, NULL, ecp, txp);
    if (tb1typ == NULL)
        return RDB_ERROR;

    tb2typ = RDB_expr_type(exp->var.op.args.firstp->nextp, NULL, NULL, ecp, txp);
    if (tb2typ == NULL)
        goto error;

    reltyp = RDB_summarize_type(&exp->var.op.args, 0, NULL, ecp, txp);
    if (reltyp == NULL)
        goto error;

    /* If AVG, extend tuple type by count */
    hasavg = RDB_FALSE;
    argp = exp->var.op.args.firstp->nextp->nextp;
    while (argp != NULL) {
        if (strcmp(argp->var.op.name, "AVG") == 0) {
            hasavg = RDB_TRUE;
            break;
        }
        argp = argp->nextp->nextp;
    }
    if (hasavg) {
        int attrc = reltyp->var.basetyp->var.tuple.attrc + 1;
        RDB_attr *attrv = RDB_realloc(reltyp->var.basetyp->var.tuple.attrv,
                attrc * sizeof (RDB_attr), ecp);
        if (attrv == NULL) {
            goto error;
        }
        attrv[attrc - 1].name = RDB_dup_str(AVG_COUNT);
        if (attrv[attrc - 1].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        attrv[attrc - 1].typ = &RDB_INTEGER;
        attrv[attrc - 1].defaultp = NULL;
        attrv[attrc - 1].options = 0;
        reltyp->var.basetyp->var.tuple.attrc = attrc;
        reltyp->var.basetyp->var.tuple.attrv = attrv;
    }

    /* Key consists of table #2 attributes */
    key.strc = tb2typ->var.basetyp->var.tuple.attrc;
    key.strv = RDB_alloc(sizeof (char *) * key.strc, ecp);
    if (key.strv == NULL) {
        goto error;
    }
    for (i = 0; i < key.strc; i++) {
        key.strv[0] = tb2typ->var.basetyp->var.tuple.attrv[i].name;
    }

    /* create materialized table */
    qrp->matp = _RDB_new_obj(ecp);
    if (qrp->matp == NULL)
        goto error;

    if (_RDB_init_table(qrp->matp, NULL, RDB_FALSE, reltyp, 1, &key, RDB_TRUE,
            NULL, ecp) != RDB_OK)
        goto error;

    if (init_summ_table(qrp, tb1typ, hasavg, ecp, txp) != RDB_OK) {
        goto error;
    }

    /* summarize over table 1 */
    if (do_summarize(qrp, tb1typ, hasavg, ecp, txp) != RDB_OK) {
        goto error;
    }

    if (init_stored_qresult(qrp, qrp->matp, NULL, ecp, txp) != RDB_OK) {
        goto error;
    }

    RDB_free(key.strv);
    return RDB_OK;

error:
    RDB_free(key.strv);
    if (qrp->matp != NULL) {
        RDB_drop_table(qrp->matp, ecp, txp);
    } else if (reltyp != NULL) {
        RDB_drop_type(reltyp, ecp, NULL);
    }
    return RDB_ERROR;
}

static int
sdivide_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    qrp->exp = exp;
    qrp->nested = RDB_TRUE;
    qrp->var.children.tpl_valid = RDB_FALSE;

    /* Create qresults for table #1 and table #3 */
    qrp->var.children.qrp = _RDB_expr_qresult(qrp->exp->var.op.args.firstp,
            ecp, txp);
    if (qrp->var.children.qrp == NULL)
        return RDB_ERROR;
    qrp->var.children.qr2p = _RDB_expr_qresult(
            qrp->exp->var.op.args.firstp->nextp->nextp, ecp, txp);
    if (qrp->var.children.qr2p == NULL) {
        _RDB_drop_qresult(qrp->var.children.qrp, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
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
    int keyfc;
    char *gattrname = RDB_obj_string(&qrp->exp->var.op.args.lastp->var.obj);
    RDB_type *greltyp = _RDB_tuple_type_attr(qrp->matp->typ->var.basetyp,
                        gattrname)->typ;

    keyfc = _RDB_pkey_len(qrp->matp);

    keyfv = RDB_alloc(sizeof (RDB_field) * keyfc, ecp);
    if (keyfv == NULL) {
        return RDB_ERROR;
    }

    /*
     * Iterate over the original table, modifying the materialized table
     */

    newqrp = _RDB_expr_qresult(qrp->exp->var.op.args.firstp, ecp, txp);
    if (newqrp == NULL) {
        return RDB_ERROR;
    }
    if (_RDB_duprem(qrp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&gval);
    do {
        ret = _RDB_next_tuple(newqrp, &tpl, ecp, txp);
        if (ret == RDB_OK) {
            /* Build key */
            for (i = 0; i < keyfc; i++) {
                RDB_object *attrobjp = RDB_tuple_get(&tpl,
                        qrp->matp->var.tb.keyv[0].strv[i]);
                attrobjp->store_typ = attrobjp->typ;                
                ret = _RDB_obj_to_field(&keyfv[i], attrobjp, ecp);
                if (ret != RDB_OK)
                    goto cleanup;
            }

            if (qrp->matp->var.tb.stp != NULL) {
                gfield.no = *_RDB_field_no(qrp->matp->var.tb.stp, gattrname);

                /* Try to read tuple of the materialized table */
                ret = RDB_get_fields(qrp->matp->var.tb.stp->recmapp, keyfv,
                        1, NULL, &gfield);
                RDB_errcode_to_error(ret, ecp, txp);
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
                ret = RDB_insert(&gval, &tpl, ecp, txp);
                if (ret != RDB_OK)
                    goto cleanup;

                /* Update materialized table */
                gval.store_typ = greltyp;
                ret = _RDB_obj_to_field(&gfield, &gval, ecp);
                if (ret != RDB_OK)
                    goto cleanup;
                _RDB_cmp_ecp = ecp;
                ret = RDB_update_rec(qrp->matp->var.tb.stp->recmapp, keyfv,
                        1, &gfield, NULL);
                if (ret != RDB_OK)
                    goto cleanup;
            } else if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                /*
                 * A tuple has not been found, build tuple and insert it
                 */
                RDB_object gtb;
                RDB_type *reltyp;

                RDB_clear_err(ecp);
                reltyp = RDB_dup_nonscalar_type(greltyp, ecp);
                if (reltyp == NULL)
                    goto cleanup;
                RDB_init_obj(&gtb);
                if (RDB_init_table_from_type(&gtb, NULL, reltyp, 0, NULL, ecp)
                        != RDB_OK) {
                    RDB_destroy_obj(&gtb, ecp);
                    RDB_drop_type(reltyp, ecp, NULL);
                    goto cleanup;
                }

                ret = RDB_insert(&gtb, &tpl, ecp, NULL);
                if (ret != RDB_OK)
                    goto cleanup;

                /*
                 * Set then attribute first, then assign the table value to it
                 */
                ret = RDB_tuple_set(&tpl, gattrname, &gtb, ecp);
                if (ret != RDB_OK)
                    goto cleanup;

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
    RDB_free(keyfv);

    return ret;
}

static int
group_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_string_vec *keyv;
    int keyc;
    int ret;
    RDB_bool freekeys;
    RDB_type *reltyp = RDB_expr_type(exp, NULL, NULL, ecp, txp);
    if (reltyp == NULL)
        return RDB_ERROR;

    qrp->exp = exp;
    qrp->nested = RDB_FALSE;

    /* Need keys */
    keyc = _RDB_infer_keys(exp, ecp, &keyv, &freekeys);
    if (keyc == RDB_ERROR) {
        return RDB_ERROR;
    }

    /* create materialized table */
    qrp->matp = _RDB_new_obj(ecp);
    if (qrp->matp == NULL) {
        _RDB_free_obj(qrp->matp, ecp);
        return RDB_ERROR;
    }

    reltyp = RDB_dup_nonscalar_type(reltyp, NULL);
    if (reltyp == NULL) {
        _RDB_free_obj(qrp->matp, ecp);
        return RDB_ERROR;
    }

    ret = _RDB_init_table(qrp->matp, NULL, RDB_FALSE, reltyp,
            keyc, keyv, RDB_TRUE, NULL, ecp);
    if (freekeys)
        _RDB_free_keys(keyc, keyv);
    if (ret != RDB_OK) {
        RDB_drop_type(reltyp, ecp, NULL);
        RDB_drop_table(qrp->matp, ecp, txp);
        return RDB_ERROR;
    }

    /* do the grouping */
    if (do_group(qrp, ecp, txp) != RDB_OK) {
        RDB_drop_table(qrp->matp, ecp, txp);
        return RDB_ERROR;
    }

    if (init_stored_qresult(qrp, qrp->matp, NULL, ecp, txp) != RDB_OK) {
        RDB_drop_table(qrp->matp, ecp, txp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
init_index_qresult(RDB_qresult *qrp, RDB_object *tbp, _RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    /* !! delay after first call to _RDB_qresult_next()? */
    qrp->endreached = RDB_FALSE;
    qrp->exp = NULL;
    qrp->nested = RDB_FALSE;
    qrp->var.stored.tbp = tbp;
    qrp->matp = NULL;
    ret = RDB_index_cursor(&qrp->var.stored.curp, indexp->idxp, RDB_FALSE,
            txp != NULL ? txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        return RDB_ERROR;
    }
    ret = RDB_cursor_first(qrp->var.stored.curp);
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
init_where_index_qresult(RDB_qresult *qrp, RDB_expression *texp,
        _RDB_tbindex *indexp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_field *fv;
    int flags = 0;

    qrp->exp = texp;
    qrp->nested = RDB_FALSE;
    qrp->matp = NULL;
    if (texp->var.op.args.firstp->kind == RDB_EX_TBP) {
        qrp->var.stored.tbp = texp->var.op.args.firstp->var.tbref.tbp;
    } else {
        qrp->var.stored.tbp = texp->var.op.args.firstp->var.op.args.firstp
                ->var.tbref.tbp;
    }

    if (indexp->unique) {
        qrp->var.stored.curp = NULL;
        return RDB_OK;
    }

    ret = RDB_index_cursor(&qrp->var.stored.curp, indexp->idxp, RDB_FALSE,
            qrp->var.stored.tbp->var.tb.is_persistent ? txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        return RDB_ERROR;
    }

    if (texp->var.op.optinfo.objpc != indexp->attrc
            || !texp->var.op.optinfo.all_eq) {
        flags = RDB_REC_RANGE;
    }

    fv = RDB_alloc(sizeof (RDB_field) * indexp->attrc, ecp);
    if (fv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < texp->var.op.optinfo.objpc; i++) {
        ret = _RDB_obj_to_field(&fv[i], texp->var.op.optinfo.objpv[i], ecp);
        if (ret != RDB_OK) {
            RDB_free(fv);
            return RDB_ERROR;
        }
    }

    ret = RDB_cursor_seek(qrp->var.stored.curp, texp->var.op.optinfo.objpc,
            fv, flags);
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        ret = RDB_OK;
    }

    RDB_free(fv);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
init_expr_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (exp->kind == RDB_EX_OBJ) {
        return init_qresult(qrp, &exp->var.obj, ecp, txp);
    }
    if (exp->kind == RDB_EX_TBP) {
        if (exp->var.tbref.indexp != NULL)
            return init_index_qresult(qrp, exp->var.tbref.tbp,
                    exp->var.tbref.indexp, ecp, txp);
        return init_qresult(qrp, exp->var.tbref.tbp, ecp, txp);
    }
    if (exp->kind == RDB_EX_VAR && txp != NULL) {
        RDB_object *tbp = RDB_get_table(exp->var.varname, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;        
    }
    if (exp->kind != RDB_EX_RO_OP) {
        RDB_raise_invalid_argument(
                "invalid expression, must be object or operator", ecp);
        return RDB_ERROR;
    }

    qrp->endreached = RDB_FALSE;
    qrp->matp = NULL;

    if (strcmp(exp->var.op.name, "WHERE") == 0
            && exp->var.op.optinfo.objpc > 0) {
        /* Check for index */
        _RDB_tbindex *indexp;
        if (exp->var.op.args.firstp->kind == RDB_EX_TBP) {
            indexp = exp->var.op.args.firstp->var.tbref.indexp;
        } else if (exp->var.op.args.firstp->kind == RDB_EX_RO_OP
                && strcmp (exp->var.op.args.firstp->var.op.name, "PROJECT") == 0
                && exp->var.op.args.firstp->var.op.args.firstp->kind == RDB_EX_TBP) {
            indexp = exp->var.op.args.firstp->var.op.args.firstp->var.tbref.indexp;
        }
        return init_where_index_qresult(qrp, exp, indexp, ecp, txp);
    }

    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        RDB_expression *texp = exp->var.op.args.firstp;
        
        qrp->var.children.tpl_valid = RDB_FALSE;
        if (texp->kind == RDB_EX_TBP
                && texp->var.tbref.tbp->kind == RDB_OB_TABLE
                && texp->var.tbref.tbp->var.tb.stp != NULL) {
            if (init_stored_qresult(qrp, texp->var.tbref.tbp, exp, ecp, txp)
                    != RDB_OK) {
                return RDB_ERROR;
            }
        } else {
            qrp->exp = exp;
            qrp->nested = RDB_TRUE;
            qrp->var.children.qrp = _RDB_expr_qresult(exp->var.op.args.firstp, ecp, txp);
            if (qrp->var.children.qrp == NULL)
                return RDB_ERROR;
            qrp->var.children.qr2p = NULL;
        }
        return RDB_OK;        
    }
    if ((strcmp(exp->var.op.name, "WHERE") == 0)
            || (strcmp(exp->var.op.name, "UNION") == 0)
            || (strcmp(exp->var.op.name, "MINUS") == 0)
            || (strcmp(exp->var.op.name, "SEMIMINUS") == 0)
            || (strcmp(exp->var.op.name, "INTERSECT") == 0)
            || (strcmp(exp->var.op.name, "SEMIJOIN") == 0)
            || (strcmp(exp->var.op.name, "EXTEND") == 0)
            || (strcmp(exp->var.op.name, "RENAME") == 0)
            || (strcmp(exp->var.op.name, "WRAP") == 0)
            || (strcmp(exp->var.op.name, "UNWRAP") == 0)
            || (strcmp(exp->var.op.name, "UNGROUP") == 0)) {
        qrp->exp = exp;
        qrp->nested = RDB_TRUE;
        qrp->var.children.tpl_valid = RDB_FALSE;
        qrp->var.children.qrp = _RDB_expr_qresult(exp->var.op.args.firstp, ecp, txp);
        if (qrp->var.children.qrp == NULL)
            return RDB_ERROR;
        qrp->var.children.qr2p = NULL;
        return RDB_OK;        
    }
    if (strcmp(exp->var.op.name, "GROUP") == 0) {
        return group_qresult(qrp, exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "JOIN") == 0) {
        return join_qresult(qrp, exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "SUMMARIZE") == 0) {
        return summarize_qresult(qrp, exp, ecp, txp);
    }    
    if (strcmp(exp->var.op.name, "DIVIDE") == 0) {
        return sdivide_qresult(qrp, exp, ecp, txp);
    }    
    RDB_raise_operator_not_found(exp->var.op.name, ecp);
    return RDB_ERROR;
}

RDB_qresult *
_RDB_expr_qresult(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_qresult *qrp;

    qrp = RDB_alloc(sizeof (RDB_qresult), ecp);
    if (qrp == NULL) {
        return NULL;
    }
    if (init_expr_qresult(qrp, exp, ecp, txp) != RDB_OK) {
        RDB_free(qrp);
        return NULL;
    }
    return qrp;
}

static int
init_qresult(RDB_qresult *qrp, RDB_object *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    qrp->endreached = RDB_FALSE;
    qrp->matp = NULL;

    if (tbp->var.tb.exp == NULL) {
        return init_stored_qresult(qrp, tbp, NULL, ecp, txp);
    }
    return init_expr_qresult(qrp, tbp->var.tb.exp, ecp, txp);
}

static int
expr_dups(RDB_expression *exp, RDB_exec_context *ecp, RDB_bool *resp)
{
    if (exp->kind == RDB_EX_OBJ) {
        assert(exp->var.obj.kind == RDB_OB_TABLE && exp->var.obj.var.tb.exp == NULL);
        *resp = RDB_FALSE;
        return RDB_OK;
    }
    if (exp->kind == RDB_EX_TBP) {
        exp = exp->var.tbref.tbp->var.tb.exp;
        if (exp == NULL) {
            *resp = RDB_FALSE;
            return RDB_OK;
        }
    }

    if (strcmp(exp->var.op.name, "WHERE") == 0
            || strcmp(exp->var.op.name, "MINUS") == 0
            || strcmp(exp->var.op.name, "SEMIMINUS") == 0
            || strcmp(exp->var.op.name, "INTERSECT") == 0
            || strcmp(exp->var.op.name, "SEMIJOIN") == 0
            || strcmp(exp->var.op.name, "EXTEND") == 0
            || strcmp(exp->var.op.name, "RENAME") == 0
            || strcmp(exp->var.op.name, "EXTEND") == 0
            || strcmp(exp->var.op.name, "RENAME") == 0
            || strcmp(exp->var.op.name, "WRAP") == 0
            || strcmp(exp->var.op.name, "UNWRAP") == 0
            || strcmp(exp->var.op.name, "UNGROUP") == 0
            || strcmp(exp->var.op.name, "DIVIDE") == 0) {
        return expr_dups(exp->var.op.args.firstp, ecp, resp);
    } else if (strcmp(exp->var.op.name, "UNION") == 0) {
        *resp = RDB_TRUE;
        return RDB_OK;
    } else if (strcmp(exp->var.op.name, "JOIN") == 0) {
        if (expr_dups(exp->var.op.args.firstp, ecp, resp) != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;
        if (expr_dups(exp->var.op.args.firstp->nextp, ecp, resp) != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;
    } else if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        int keyc, newkeyc;
        RDB_string_vec *keyv;
        RDB_bool freekey;

        if (expr_dups(exp->var.op.args.firstp, ecp, resp) != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;

        keyc = _RDB_infer_keys(exp->var.op.args.firstp, ecp, &keyv, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;

        newkeyc = _RDB_check_project_keyloss(exp, keyc, keyv, NULL, ecp);
        if (newkeyc == RDB_ERROR) {
            if (freekey) {
                _RDB_free_keys(keyc, keyv);
            }
            return RDB_ERROR;
        }
        *resp = (RDB_bool) (newkeyc == 0);
        if (freekey) {
            _RDB_free_keys(keyc, keyv);
        }
        return RDB_OK;
    }
    *resp = RDB_FALSE;
    return RDB_OK;
}

static int
qr_dups(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_bool *resp)
{
    if (qrp->exp == NULL) {
        *resp = RDB_FALSE;
        return RDB_OK;
    }
    return expr_dups(qrp->exp, ecp, resp);
}

int
_RDB_duprem(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_bool rd;

    if (qr_dups(qrp, ecp, &rd) != RDB_OK)
        return RDB_ERROR;

    /*
     * Add duplicate remover only if the qresult may return duplicates
     */
    if (rd) {
        int ret;

        /* rd can only be true for virtual tables */
        RDB_type *reltyp = RDB_expr_type(qrp->exp, NULL, NULL, ecp, txp);
        if (reltyp == NULL)
            return RDB_ERROR;
        reltyp = RDB_dup_nonscalar_type(reltyp, ecp);
        if (reltyp == NULL)
            return RDB_ERROR;

        /* Create materialized (all-key) table */
        qrp->matp = _RDB_new_obj(ecp);
        if (qrp->matp == NULL) {
            RDB_drop_type(reltyp, ecp, NULL);
            return RDB_ERROR;
        }            
        ret = _RDB_init_table(qrp->matp, NULL, RDB_FALSE, reltyp, 0, NULL,
                RDB_TRUE, NULL, ecp);
        if (ret != RDB_OK) {
            RDB_drop_type(reltyp, ecp, NULL);
            _RDB_free_obj(qrp->matp, ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

RDB_qresult *
_RDB_table_qresult(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *qrp;

    qrp = RDB_alloc(sizeof (RDB_qresult), ecp);
    if (qrp == NULL) {
        return NULL;
    }
    if (init_qresult(qrp, tbp, ecp, txp) != RDB_OK) {
        RDB_free(qrp);
        return NULL;
    }
    return qrp;
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
    RDB_free(statp);
}
*/

/*
 * Creates a qresult which sorts a table.
 */
int
_RDB_sorter(RDB_expression *texp, RDB_qresult **qrpp, RDB_exec_context *ecp,
        RDB_transaction *txp, int seqitc, const RDB_seq_item seqitv[])
{
    RDB_string_vec key;
    RDB_bool *ascv = NULL;
    int ret;
    int i;
    RDB_qresult *tmpqrp;
    RDB_object tpl;
    RDB_type *typ = NULL;
    RDB_qresult *qrp = RDB_alloc(sizeof (RDB_qresult), ecp);
    if (qrp == NULL) {
        return RDB_ERROR;
    }

    key.strc = seqitc;
    key.strv = RDB_alloc(sizeof (char *) * seqitc, ecp);
    if (key.strv == NULL) {
        goto error;
    }

    ascv = RDB_alloc(sizeof (RDB_bool) * seqitc, ecp);
    if (ascv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    qrp->nested = RDB_FALSE;
    qrp->var.stored.tbp = NULL;
    qrp->endreached = RDB_FALSE;

    for (i = 0; i < seqitc; i++) {
        key.strv[i] = seqitv[i].attrname;
        ascv[i] = seqitv[i].asc;
    }

    /*
     * Create a sorted RDB_table
     */

    typ = RDB_expr_type(texp, NULL, NULL, ecp, txp);
    if (typ == NULL)
        goto error;
    typ = RDB_dup_nonscalar_type(typ, ecp);
    if (typ == NULL)
        goto error;

    qrp->matp = _RDB_new_rtable(NULL, RDB_FALSE,
            typ, 1, &key, RDB_TRUE, ecp);
    if (qrp->matp == NULL) {
        RDB_drop_type(typ, ecp, NULL);
        goto error;
    }

    if (_RDB_create_stored_table(qrp->matp,
            txp!= NULL ? txp->dbp->dbrootp->envp : NULL, ascv, ecp, txp) != RDB_OK)
        goto error;

    /*
     * Copy tuples into the newly created RDB_table
     */

    tmpqrp = _RDB_expr_qresult(texp, ecp, txp);
    if (tmpqrp == NULL)
        goto error;

    /*
     * The duplicate elimination can produce a error message
     * "Duplicate data items are not supported with sorted data"
     * in the error log. This is expected behaviour.
     */
    RDB_init_obj(&tpl);
    while ((ret = _RDB_next_tuple(tmpqrp, &tpl, ecp, txp)) == RDB_OK) {
        ret = RDB_insert(qrp->matp, &tpl, ecp, NULL);
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

    ret = init_stored_qresult(qrp, qrp->matp, NULL, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    RDB_free(key.strv);
    RDB_free(ascv);

    *qrpp = qrp;
    return RDB_OK;

error:
    if (key.strv != NULL)
        RDB_free(key.strv);
    if (ascv != NULL)
        RDB_free(ascv);
    if (qrp->matp != NULL)
        RDB_drop_table(qrp->matp, ecp, NULL);
    RDB_free(qrp);

    return RDB_ERROR;
}

int
_RDB_get_by_cursor(RDB_object *tbp, RDB_cursor *curp, RDB_type *tpltyp,
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

        fno = *_RDB_field_no(tbp->var.tb.stp, attrp->name);
        ret = RDB_cursor_get(curp, fno, &datap, &len);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp, txp);
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
next_stored_tuple(RDB_qresult *qrp, RDB_object *tbp, RDB_object *tplp,
        RDB_bool asc, RDB_bool dup, RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (qrp->endreached) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (tplp != NULL) {
        ret = _RDB_get_by_cursor(tbp, qrp->var.stored.curp, tpltyp, tplp, ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (asc) {
        ret = RDB_cursor_next(qrp->var.stored.curp, dup ? RDB_REC_DUP : 0);
    } else {
        if (dup) {
            RDB_raise_invalid_argument("", ecp);
            return RDB_ERROR;
        }
        ret = RDB_cursor_prev(qrp->var.stored.curp);
    }
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
next_ungroup_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object *attrtbp;
    RDB_hashtable_iter hiter;
    char *attrname = RDB_obj_string(&qrp->exp->var.op.args.firstp->nextp->var.obj);

    /* If no tuple has been read, read first tuple */
    if (!qrp->var.children.tpl_valid) {
        RDB_init_obj(&qrp->var.children.tpl);
        ret = _RDB_next_tuple(qrp->var.children.qrp, &qrp->var.children.tpl,
                ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        qrp->var.children.tpl_valid = RDB_TRUE;
    }

    /* If there is no 2nd qresult, open it from relation-valued attribute */
    if (qrp->var.children.qr2p == NULL) {
        attrtbp = RDB_tuple_get(&qrp->var.children.tpl, attrname);

        qrp->var.children.qr2p = _RDB_table_qresult(attrtbp, ecp, txp);
        if (qrp->var.children.qr2p == NULL)
            return RDB_ERROR;
    }

    /* Read next element of relation-valued attribute */
    ret = _RDB_next_tuple(qrp->var.children.qr2p, tplp, ecp, txp);
    while (ret == RDB_ERROR
            && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        /* Destroy qresult over relation-valued attribute */
        ret = _RDB_drop_qresult(qrp->var.children.qr2p, ecp, txp);
        qrp->var.children.qr2p = NULL;
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* Read next tuple */
        ret = _RDB_next_tuple(qrp->var.children.qrp, &qrp->var.children.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        attrtbp = RDB_tuple_get(&qrp->var.children.tpl, attrname);

        /* Create qresult over relation-valued attribute */
        qrp->var.children.qr2p = _RDB_table_qresult(attrtbp, ecp, txp);
        if (qrp->var.children.qr2p == NULL) {
            return RDB_ERROR;
        }

        ret = _RDB_next_tuple(qrp->var.children.qr2p, tplp, ecp, txp);
    }
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    /* Merge tuples, skipping the relation-valued attribute */
    RDB_init_hashtable_iter(&hiter,
            (RDB_hashtable *) &qrp->var.children.tpl.var.tpl_tab);
    for (;;) {
        /* Get next attribute */
        tuple_entry *entryp = RDB_hashtable_next(&hiter);
        if (entryp == NULL)
            break;

        if (strcmp(entryp->key, attrname) != 0)
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

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->var.children.tpl_valid) {
        RDB_init_obj(&qrp->var.children.tpl);
        if (_RDB_next_tuple(qrp->var.children.qrp, &qrp->var.children.tpl,
                ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
        qrp->var.children.tpl_valid = RDB_TRUE;
    }

    RDB_destroy_obj(tplp, ecp);
    RDB_init_obj(tplp);
    for (;;) {
        /* Read next 'inner' tuple */
        ret = _RDB_next_tuple(qrp->var.children.qr2p, tplp, ecp, txp);
        if (ret != RDB_OK && RDB_obj_type(RDB_get_err(ecp))
                != &RDB_NOT_FOUND_ERROR) {
            return RDB_ERROR;
        }
        if (ret == RDB_OK) {
            RDB_bool iseq;

            /* Compare common attributes */
            ret = _RDB_tuple_matches(tplp, &qrp->var.children.tpl, ecp, txp, &iseq);
            if (ret != RDB_OK)
                return RDB_ERROR;

            /* 
             * If common attributes are equal, leave the loop,
             * otherwise read next tuple
             */
            if (iseq)
                break;
            continue;
        }
        RDB_clear_err(ecp);

        /* reset nested qresult */
        ret = _RDB_reset_qresult(qrp->var.children.qr2p, ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* read next 'outer' tuple */
        ret = _RDB_next_tuple(qrp->var.children.qrp, &qrp->var.children.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    }

    /* join the two tuples into tplp */
    return RDB_add_tuple(tplp, &qrp->var.children.tpl, ecp, txp);
}

static int
find_str_exp(RDB_expression *exp, const char *str)
{
    int i = 0;
    do {
        if (strcmp(RDB_obj_string(&exp->var.obj), str) == 0)
            return i;
        exp = exp->nextp;
        ++i;
    } while (exp != NULL);
    return -1;
}

static int
next_unwrap_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_hashtable_iter it;
    tuple_entry *entryp;
    RDB_expression *argp;
    RDB_expression *exp = qrp->exp;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.children.qrp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    argp = exp->var.op.args.firstp->nextp;
    while (argp != NULL) {
        char *attrname = RDB_obj_string(&argp->var.obj);
        RDB_object *wtplp = RDB_tuple_get(&tpl, attrname);

        if (wtplp == NULL) {
            RDB_raise_name(attrname, ecp);
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        if (wtplp->kind != RDB_OB_TUPLE) {
            RDB_raise_invalid_argument("attribute is not tuple", ecp);
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }

        ret = _RDB_copy_tuple(tplp, wtplp, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        argp = argp->nextp;
    }
    
    /* Copy remaining attributes */
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&tpl.var.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        /* Copy attribute if it does not appear in attrv */
        if (find_str_exp(exp->var.op.args.firstp->nextp, entryp->key) == -1) {
            ret = RDB_tuple_set(tplp, entryp->key, &entryp->obj, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_hashtable_iter(&it);
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
        }
    }
    RDB_destroy_hashtable_iter(&it);

    RDB_destroy_obj(&tpl, ecp);
    return RDB_OK;
}

static int
seek_index_qresult(RDB_qresult *qrp, _RDB_tbindex *indexp,
        const RDB_object *tplp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_field *fv = RDB_alloc(sizeof (RDB_field) * indexp->attrc, ecp);
    if (fv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < indexp->attrc; i++) {
        RDB_object *attrobjp = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
        attrobjp->store_typ = attrobjp->typ;
        ret = _RDB_obj_to_field(&fv[i], attrobjp, ecp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_cursor_seek(qrp->var.stored.curp, indexp->attrc, fv, 0);
    if (ret == RDB_OK) {
        qrp->endreached = RDB_FALSE;
    } else {
        qrp->endreached = RDB_TRUE;
    }

cleanup:
    RDB_free(fv);

    return ret;
}

static int
next_join_tuple_nuix(RDB_qresult *qrp, RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    _RDB_tbindex *indexp = qrp->exp->var.op.args.firstp->nextp->var.tbref.indexp;

    /* read first 'outer' tuple, if it's the first invocation */
    if (!qrp->var.children.tpl_valid) {
        RDB_init_obj(&qrp->var.children.tpl);
        ret = _RDB_next_tuple(qrp->var.children.qrp, &qrp->var.children.tpl,
                ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        qrp->var.children.tpl_valid = RDB_TRUE;

        /* Set cursor position */
        ret = seek_index_qresult(qrp->var.children.qr2p, indexp,
                &qrp->var.children.tpl, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    RDB_destroy_obj(tplp, ecp);
    RDB_init_obj(tplp);

    for (;;) {
        /* read next 'inner' tuple */
        ret = next_stored_tuple(qrp->var.children.qr2p,
                qrp->var.children.qr2p->var.stored.tbp, tplp, RDB_TRUE, RDB_TRUE,
                qrp->var.children.qr2p->var.stored.tbp->typ->var.basetyp,
                ecp, txp);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        } else {
            RDB_bool match;
            
            if (_RDB_tuple_matches(tplp, &qrp->var.children.tpl, ecp, txp,
                    &match) != RDB_OK) {
                return RDB_ERROR;
            }

            /* 
             * If common attributes are equal, leave the loop,
             * otherwise read next tuple
             */
            if (match)
                break;
            continue;
        }

        /* read next 'outer' tuple */
        ret = _RDB_next_tuple(qrp->var.children.qrp, &qrp->var.children.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* reset cursor */
        ret = seek_index_qresult(qrp->var.children.qr2p, indexp,
                &qrp->var.children.tpl, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }
    
    /* join the two tuples into tplp */
    return RDB_add_tuple(tplp, &qrp->var.children.tpl, ecp, txp);
}

static int
next_join_tuple_uix(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
    RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl;
    RDB_object **objpv;
    RDB_bool match = RDB_FALSE;
    _RDB_tbindex *indexp = qrp->exp->var.op.args.firstp->nextp->var.tbref.indexp;

    objpv = RDB_alloc(sizeof(RDB_object *) * indexp->attrc, ecp);
    if (objpv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(tplp, ecp);
    RDB_init_obj(tplp);

    RDB_init_obj(&tpl);

    do {
        ret = _RDB_next_tuple(qrp->var.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;

        for (i = 0; i < indexp->attrc; i++) {
            objpv[i] = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
            objpv[i]->store_typ = objpv[i]->typ;
        }
        ret = _RDB_get_by_uindex(qrp->exp->var.op.args.firstp->nextp->var.tbref.tbp,
                objpv, indexp,
                qrp->exp->var.op.args.firstp->nextp->var.tbref.tbp->typ->var.basetyp,
                ecp, txp, &tpl);
        if (ret == RDB_ERROR) {
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                RDB_clear_err(ecp);
                continue;
            }
            goto cleanup;
        }

        ret = _RDB_tuple_matches(tplp, &tpl, ecp, txp, &match);
        if (ret != RDB_OK)
            goto cleanup;
    } while (!match);

    ret = RDB_add_tuple(tplp, &tpl, ecp, txp);

cleanup:
    RDB_free(objpv);
    RDB_destroy_obj(&tpl, ecp);

    return ret;
}

/*
 * Read a tuple from a table using the unique index given by indexp,
 * using the values given by objpv as a key.
 * Read only the attributes in tpltyp.
 */
int
_RDB_get_by_uindex(RDB_object *tbp, RDB_object *objpv[], _RDB_tbindex *indexp,
        RDB_type *tpltyp, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *tplp)
{
    RDB_field *fv;
    RDB_field *resfv = NULL;
    int i;
    int ret;
    int keylen = indexp->attrc;
    int resfc = tpltyp->var.tuple.attrc - keylen;

    if (resfc > 0) {
        resfv = RDB_alloc(sizeof (RDB_field) * resfc, ecp);
        if (resfv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    }
    fv = RDB_alloc(sizeof (RDB_field) * keylen, ecp);
    if (fv == NULL) {
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
            int fno = *_RDB_field_no(tbp->var.tb.stp, tpltyp->var.tuple.attrv[i].name);

            if (fno >= keylen)
                resfv[rfi++].no = fno;
        }
        ret = RDB_get_fields(tbp->var.tb.stp->recmapp, fv, resfc,
                             tbp->var.tb.is_persistent ? txp->txid : NULL, resfv);
    } else {
        int rfi = 0;

        for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
            int j;
            int fno = *_RDB_field_no(tbp->var.tb.stp, tpltyp->var.tuple.attrv[i].name);

            /* Search field number in index */
            for (j = 0; j < keylen && indexp->idxp->fieldv[j] != fno; j++);

            /* If not found, so the field must be read from the DB */
            if (j >= keylen)
                resfv[rfi++].no = fno;
        }
        ret = RDB_index_get_fields(indexp->idxp, fv, resfc,
                tbp->var.tb.is_persistent ? txp->txid : NULL, resfv);
    }

    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Set key attributes
     */
    for (i = 0; i < indexp->attrc; i++) {
        ret = RDB_tuple_set(tplp, indexp->attrv[i].attrname, objpv[i], ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    /*
     * Set non-key attributes
     */
    for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
        int rfi; /* Index in resv, -1 if key attr */
        char *attrname = tpltyp->var.tuple.attrv[i].name;
        RDB_int fno = *_RDB_field_no(tbp->var.tb.stp, attrname);

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
    RDB_free(fv);
    RDB_free(resfv);
    return ret;
}

static int
next_project_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object *valp;
    RDB_expression *argp;

    /* Get tuple */
    if (!qrp->nested) {
        if (tplp != NULL) {
            ret = _RDB_get_by_cursor(qrp->var.stored.tbp, qrp->var.stored.curp,
                    RDB_expr_type(qrp->exp, NULL, NULL, ecp, txp)->var.basetyp,
                    tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        }
        ret = RDB_cursor_next(qrp->var.stored.curp, 0);
        if (ret == DB_NOTFOUND)
            qrp->endreached = RDB_TRUE;
        else if (ret != RDB_OK)
            return RDB_ERROR;
    } else {
        RDB_object tpl;

        RDB_init_obj(&tpl);

        ret = _RDB_next_tuple(qrp->var.children.qrp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }

        if (tplp != NULL) {
            /* Copy attributes into new tuple */
            argp = qrp->exp->var.op.args.firstp->nextp;
            while (argp != NULL) {
                char *attrname = RDB_obj_string(&argp->var.obj);

                valp = RDB_tuple_get(&tpl, attrname);
                RDB_tuple_set(tplp, attrname, valp, ecp);
                argp = argp->nextp;
            }
        }

        RDB_destroy_obj(&tpl, ecp);
    }
    return RDB_OK;
}

static int
next_where_index(RDB_qresult *qrp, RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    _RDB_tbindex *indexp;
    RDB_bool rtup;
    RDB_type *tpltyp;
    RDB_type *reltyp = NULL;
    RDB_bool dup = RDB_FALSE;

    if (qrp->exp->var.op.args.firstp->kind == RDB_EX_TBP) {
        indexp = qrp->exp->var.op.args.firstp->var.tbref.indexp;
        tpltyp = qrp->var.stored.tbp->typ->var.basetyp;
    } else {
        indexp = qrp->exp->var.op.args.firstp->var.op.args.firstp->var.tbref.indexp;
        reltyp = RDB_expr_type(qrp->exp->var.op.args.firstp, NULL, NULL, ecp, txp);
        if (reltyp == NULL)
            return RDB_ERROR;
        tpltyp = reltyp->var.basetyp;
    }

    if (qrp->var.stored.curp == NULL) {
        ret = _RDB_get_by_uindex(qrp->var.stored.tbp,
                qrp->exp->var.op.optinfo.objpv, indexp,
                tpltyp, ecp, txp, tplp);
        if (ret != RDB_OK) {
            goto error;
        }

        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }

    if (qrp->exp->var.op.optinfo.objpc == indexp->attrc
            && qrp->exp->var.op.optinfo.all_eq)
        dup = RDB_TRUE;

    do {
        ret = next_stored_tuple(qrp, qrp->var.stored.tbp, tplp,
                qrp->exp->var.op.optinfo.asc, dup, tpltyp, ecp, txp);
        if (ret != RDB_OK)
            goto error;
        if (qrp->exp->var.op.optinfo.all_eq)
            rtup = RDB_TRUE;
        else {
            if (qrp->exp->var.op.optinfo.stopexp != NULL) {
                ret = RDB_evaluate_bool(qrp->exp->var.op.optinfo.stopexp,
                        &_RDB_tpl_get, tplp, ecp, txp, &rtup);
                if (ret != RDB_OK)
                    goto error;
                if (!rtup) {
                    qrp->endreached = RDB_TRUE;
                    RDB_raise_not_found("", ecp);
                    goto error;
                }
            }
            /*
             * Check condition, because it could be an open start
             */
            ret = RDB_evaluate_bool(qrp->exp->var.op.args.firstp->nextp, &_RDB_tpl_get,
                    tplp, ecp, txp, &rtup);
            if (ret != RDB_OK)
                goto error;
        }
    } while (!rtup);

    return RDB_OK;

error:
    return RDB_ERROR;
}

static int
find_str(RDB_object *arrp, const char *str, RDB_exec_context *ecp)
{
    int i;
    RDB_object *objp;
    int len = RDB_array_length(arrp, ecp);
    if (len == RDB_ERROR)
        return RDB_ERROR;
        
    i = 0;
    while (i < len) {
        objp = RDB_array_get(arrp, (RDB_int) i, ecp);
        if (objp == NULL)
            return RDB_ERROR;

        if (strcmp(RDB_obj_string(objp), str) == 0)
            return i;

        i++;
    }
    return RDB_INT_MAX;
}

int
wrap_tuple(const RDB_object *tplp, RDB_expression *exp,
               RDB_exec_context *ecp, RDB_object *restplp)
{
    int i, j;
    int ret;
    RDB_object tpl;
    RDB_object *objp;
    RDB_hashtable_iter it;
    tuple_entry *entryp;
    RDB_expression *argp;
    int wrapc = (RDB_expr_list_length(&exp->var.op.args) - 1) / 2;

    RDB_init_obj(&tpl);

    /* Wrap attributes */
    argp = exp->var.op.args.firstp->nextp;
    for (i = 0; i < wrapc; i++) {
        char *wrattrname;
        RDB_object *attrp;
        char *attrname = RDB_obj_string(&argp->nextp->var.obj);
        RDB_int len = RDB_array_length(&argp->var.obj, ecp);
        if (len == RDB_ERROR) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        for (j = 0; j < len; j++) {
            objp = RDB_array_get(&argp->var.obj,
                    j, ecp);
            if (objp == NULL) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;                
            }
            wrattrname = RDB_obj_string(objp);

            attrp = RDB_tuple_get(tplp, wrattrname);

            if (attrp == NULL) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_name(wrattrname, ecp);
                return RDB_ERROR;
            }

            ret = RDB_tuple_set(&tpl, wrattrname, attrp, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return ret;
            }
        }
        RDB_tuple_set(restplp, attrname, &tpl, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        argp = argp->nextp->nextp;
    }
    RDB_destroy_obj(&tpl, ecp);

    /* Copy attributes which have not been wrapped */
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&tplp->var.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        int i;
        /* char *attrname = RDB_obj_string(&exp->var.op.argv[2 + 2 * i]->var.obj); */

        i = 0;
        ret = RDB_INT_MAX;
        argp = exp->var.op.args.firstp->nextp;
        while (i < wrapc && ret == RDB_INT_MAX) {
            ret = find_str(&argp->var.obj, entryp->key,
                    ecp);
            if (ret == RDB_ERROR) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
            argp = argp->nextp->nextp;
        }
        if (i == wrapc) {
            /* Attribute not found, copy */
            ret = RDB_tuple_set(restplp, entryp->key, &entryp->obj, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_hashtable_iter(&it);
                return RDB_ERROR;
            }
        }
    }
    RDB_destroy_hashtable_iter(&it);

    return RDB_OK;
}

static int
next_wrap_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.children.qrp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    ret = wrap_tuple(&tpl, qrp->exp, ecp, tplp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static int
destroy_qresult(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (qrp->nested) {
        int ret2;

        ret = _RDB_drop_qresult(qrp->var.children.qrp, ecp, txp);
        if (qrp->var.children.qr2p != NULL) {
            ret2 = _RDB_drop_qresult(qrp->var.children.qr2p, ecp, txp);
            if (ret2 != RDB_OK)
                ret = ret2;
        }
        if (qrp->var.children.tpl_valid)
            RDB_destroy_obj(&qrp->var.children.tpl, ecp);
    } else if (qrp->var.stored.curp != NULL) {
        ret = RDB_destroy_cursor(qrp->var.stored.curp);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp, txp);
            ret = RDB_ERROR;
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
 * Strore RDB_TRUE in *resultp if the tuple is an element of the result,
 * RDB_FALSE if not.
 * If qr2p is not NULL, it points to a RDB_qresult containing
 * the tuples from T3.
 */
int
_RDB_sdivide_preserves(RDB_expression *exp, const RDB_object *tplp,
        RDB_qresult *qr3p, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_bool *resultp)
{
    int ret;
    int i;
    RDB_object tpl2;
    RDB_qresult qr;
    RDB_type *tb1tuptyp;
    RDB_bool matchall = RDB_TRUE;
    RDB_type *tb1typ = RDB_expr_type(exp->var.op.args.firstp, NULL, NULL, ecp, txp);
    if (tb1typ == NULL)
        return RDB_ERROR;
    if (tb1typ->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("argument #1 of DIVIDE must be a relation",
                ecp);
        return RDB_ERROR;
    }
    tb1tuptyp = tb1typ->var.basetyp;

    /*
     * Join this tuple with all tuples from table 2 and set matchall to RDB_FALSE
     * if not all the result tuples are an element of table 3
     */

    if (init_expr_qresult(&qr, exp->var.op.args.firstp->nextp, ecp, txp) != RDB_OK) {
        return RDB_ERROR;
    }

    for (;;) {
        RDB_bool b;
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
                     return RDB_ERROR;
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
                     return RDB_ERROR;
                 }
             }
        }
        if (!match)
            continue;

        ret = _RDB_expr_matching_tuple(exp->var.op.args.firstp->nextp->nextp,
                &tpl2, ecp, txp, &b);
        RDB_destroy_obj(&tpl2, ecp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
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

    if (destroy_qresult(&qr, ecp, txp) != RDB_OK) {
        return RDB_ERROR;
    }

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
        ret = _RDB_next_tuple(qrp->var.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        ret = _RDB_sdivide_preserves(qrp->exp, tplp, qrp->var.children.qr2p,
                ecp, txp, &b);
        if (ret != RDB_OK)
            return RDB_ERROR;
    } while (!b);

    return RDB_OK;
}

static int
next_where_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool expres;

    do {
        ret = _RDB_next_tuple(qrp->var.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            break;
        ret = RDB_evaluate_bool(qrp->exp->var.op.args.firstp->nextp,
                &_RDB_tpl_get, tplp, ecp, txp, &expres);
        if (ret != RDB_OK)
            break;
    } while (!expres);

    return ret;
}

static int
next_rename_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;
    RDB_hashtable_iter it;
    tuple_entry *entryp;
    RDB_expression *exp = qrp->exp;

    RDB_init_obj(&tpl);
    ret = _RDB_next_tuple(qrp->var.children.qrp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    RDB_init_hashtable_iter(&it, (RDB_hashtable *) &tpl.var.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        /* Search for attribute in rename arguments */
        char *nattrname = _RDB_rename_attr(entryp->key, exp);

        if (nattrname != NULL) {
            /* Found - copy and rename attribute */
            ret = RDB_tuple_set(tplp, nattrname, &entryp->obj, ecp);
        } else {
            /* Not found - copy only */
            ret = RDB_tuple_set(tplp, entryp->key, &entryp->obj, ecp);
        }
        if (ret != RDB_OK) {
            RDB_destroy_hashtable_iter(&it);
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }        
    }
    RDB_destroy_hashtable_iter(&it);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_OK;
}

static int
next_semiminus_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    do {
        ret = _RDB_next_tuple(qrp->var.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        ret = _RDB_expr_matching_tuple(qrp->exp->var.op.args.firstp->nextp,
                tplp, ecp, txp, &b);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    } while (b);
    return RDB_OK;
}

static int
next_union_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (!qrp->var.children.qrp->endreached) {
        ret = _RDB_next_tuple(qrp->var.children.qrp, tplp, ecp, txp);
        if (ret == RDB_OK || RDB_obj_type(RDB_get_err(ecp))
                    != &RDB_NOT_FOUND_ERROR)
            return ret;
    }
    RDB_clear_err(ecp);

    /* Switch to second table */
    if (qrp->var.children.qr2p == NULL) {
        qrp->var.children.qr2p = _RDB_expr_qresult(
                qrp->exp->var.op.args.firstp->nextp, ecp, txp);
        if (qrp->var.children.qr2p == NULL)
            return RDB_ERROR;
    }
    return _RDB_next_tuple(qrp->var.children.qr2p, tplp, ecp, txp);
}

static int
next_semijoin_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool b;
    do {
        ret = _RDB_next_tuple(qrp->var.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        ret = _RDB_expr_matching_tuple(qrp->exp->var.op.args.firstp->nextp,
                tplp, ecp, txp, &b);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    } while (!b);
    return RDB_OK;
}

static int
next_extend_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object obj;
    RDB_expression *argp;
    
    ret = _RDB_next_tuple(qrp->var.children.qrp, tplp, ecp, txp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    argp = qrp->exp->var.op.args.firstp->nextp;
    while (argp != NULL) {
        RDB_init_obj(&obj);
        if (RDB_evaluate(argp, &_RDB_tpl_get, tplp, ecp,
                txp, &obj) != RDB_OK) {
            RDB_destroy_obj(&obj, ecp);
            return RDB_ERROR;
        }
        ret = RDB_tuple_set(tplp, RDB_obj_string(
                &argp->nextp->var.obj),
                &obj, ecp);
        RDB_destroy_obj(&obj, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        argp = argp->nextp->nextp;
    }
    return RDB_OK;
}

int
_RDB_next_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
	int ret;

    if (qrp->endreached) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (qrp->exp == NULL) {
        RDB_type *tpltyp;
        if (qrp->var.stored.tbp == NULL) {
            /* It's a sorter */
           return next_stored_tuple(qrp, qrp->matp, tplp, RDB_TRUE, RDB_FALSE,
                    qrp->matp->typ->var.basetyp, ecp, txp);
	    }
        tpltyp = qrp->var.stored.tbp->typ->kind == RDB_TP_RELATION ?
                qrp->var.stored.tbp->typ->var.basetyp
                : qrp->var.stored.tbp->typ->var.scalar.arep->var.basetyp;
        return next_stored_tuple(qrp, qrp->var.stored.tbp, tplp, RDB_TRUE,
                RDB_FALSE, tpltyp, ecp, txp);
    }

    if (qrp->exp->kind != RDB_EX_RO_OP) {
    	RDB_raise_internal("invalid qresult: table ist virtual, but no operator",
    	        ecp);
    	return RDB_ERROR;
    }

    do {
        /* ELEMENT_EXISTS can be active */
        RDB_clear_err(ecp);

        if (qrp->endreached) {
            RDB_raise_not_found("", ecp);
            return RDB_ERROR;
        }

        if (strcmp(qrp->exp->var.op.name, "WHERE") == 0) {
            if (qrp->nested) {
                if (next_where_tuple(qrp, tplp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            } else {
                if (next_where_index(qrp, tplp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }                
        } else if (strcmp(qrp->exp->var.op.name, "PROJECT") == 0) {
            if (next_project_tuple(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->var.op.name, "RENAME") == 0) {
            ret = next_rename_tuple(qrp, tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->var.op.name, "JOIN") == 0) {
            if (qrp->exp->var.op.args.firstp->nextp->kind == RDB_EX_TBP
                    && qrp->exp->var.op.args.firstp->nextp->var.tbref.indexp != NULL) {
                _RDB_tbindex *indexp = qrp->exp->var.op.args.firstp->nextp->var.tbref.indexp;
                if (indexp->unique) {
                    ret = next_join_tuple_uix(qrp, tplp, ecp, txp);
                } else {
                    ret = next_join_tuple_nuix(qrp, tplp, ecp, txp);
                }
            } else {
                ret = next_join_tuple(qrp, tplp, ecp, txp);
            }
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else if ((strcmp(qrp->exp->var.op.name, "MINUS") == 0)
                || (strcmp(qrp->exp->var.op.name, "SEMIMINUS") == 0)) {
            if (next_semiminus_tuple(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->var.op.name, "UNION") == 0) {
            ret = next_union_tuple(qrp, tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else if ((strcmp(qrp->exp->var.op.name, "INTERSECT") == 0)
                || (strcmp(qrp->exp->var.op.name, "SEMIJOIN") == 0)) {
            ret = next_semijoin_tuple(qrp, tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->var.op.name, "EXTEND") == 0) {
            ret = next_extend_tuple(qrp, tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->var.op.name, "WRAP") == 0) {
            ret = next_wrap_tuple(qrp, tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->var.op.name, "UNWRAP") == 0) {
            ret = next_unwrap_tuple(qrp, tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->var.op.name, "DIVIDE") == 0) {
            ret = next_sdivide_tuple(qrp, tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->var.op.name, "UNGROUP") == 0) {
            if (next_ungroup_tuple(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else {
            RDB_raise_internal(qrp->exp->var.op.name, ecp);
            return RDB_ERROR;
        }

        /* Check for duplicate, if necessary */
        if (qrp->matp != NULL) {
            ret = _RDB_insert_real(qrp->matp, tplp, ecp, txp);
            if (ret != RDB_OK) {
                if (RDB_obj_type(RDB_get_err(ecp))
                        != &RDB_ELEMENT_EXISTS_ERROR) {
                    return RDB_ERROR;
                }
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

    if (qrp->nested) {
        ret = _RDB_reset_qresult(qrp->var.children.qrp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        if (qrp->var.children.qr2p != NULL) {
            ret = _RDB_reset_qresult(qrp->var.children.qr2p, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        }
        if (qrp->var.children.tpl_valid) {
            RDB_destroy_obj(&qrp->var.children.tpl, ecp);
            qrp->var.children.tpl_valid = RDB_FALSE;
        }
    } else {
        assert(qrp->var.stored.curp != NULL);
        /* Sorter, stored table or SUMMARIZE PER - reset cursor */
        ret = RDB_cursor_first(qrp->var.stored.curp);
        if (ret == DB_NOTFOUND) {
            qrp->endreached = RDB_TRUE;
            ret = RDB_OK;
        } else {
            qrp->endreached = RDB_FALSE;
        }
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp, txp);
            return RDB_ERROR;
        }
    }
    if (qrp->exp != NULL && qrp->matp != NULL) {
        /* Clear materialized result */
        if (_RDB_delete_real(qrp->matp, NULL, ecp, txp) == (RDB_int) RDB_ERROR)
            return RDB_ERROR;
    }
    return RDB_OK;
}

int
_RDB_drop_qresult(RDB_qresult *qrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret = destroy_qresult(qrp, ecp, txp);

    RDB_free(qrp);

    return ret;
}
