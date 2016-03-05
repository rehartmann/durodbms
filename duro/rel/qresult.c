/*
 * Functions for reading tuples using a RDB_qresult structure.
 *
 * Copyright (C) 2003-2009, 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "qresult.h"
#include "qr_stored.h"
#include "qr_join.h"
#include "qr_tclose.h"
#include "internal.h"
#include "insert.h"
#include "delete.h"
#include "optimize.h"
#include "stable.h"
#include "typeimpl.h"
#include "transform.h"
#include "obj/key.h"
#include "obj/objinternal.h"
#include <gen/hashtabit.h>
#include <gen/strfns.h>

#include <string.h>

static int
init_qresult(RDB_qresult *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

RDB_qresult *
RDB_expr_qresult(RDB_expression *exp, RDB_exec_context *,
        RDB_transaction *);

struct RDB_summval {
    RDB_object val;
};

static int
summ_step(struct RDB_summval *svalp, const RDB_object *addvalp,
        const char *opname, RDB_int count, RDB_exec_context *ecp)
{
    if (strcmp(opname, "count") == 0) {
        svalp->val.val.int_val++;
    } else if (strcmp(opname, "avg") == 0) {
        svalp->val.val.float_val =
                (svalp->val.val.float_val * count
                + addvalp->val.float_val)
                / (count + 1);
    } else if (strcmp(opname, "sum") == 0) {
        if (svalp->val.typ == &RDB_INTEGER) {
            if (addvalp->val.int_val > 0) {
                if (svalp->val.val.int_val > RDB_INT_MAX - addvalp->val.int_val) {
                    RDB_raise_type_constraint_violation("integer overflow", ecp);
                    return RDB_ERROR;
                }
            } else {
                if (svalp->val.val.int_val < RDB_INT_MIN - addvalp->val.int_val) {
                    RDB_raise_type_constraint_violation("integer overflow", ecp);
                    return RDB_ERROR;
                }
            }
            svalp->val.val.int_val += addvalp->val.int_val;
        } else
            svalp->val.val.float_val += addvalp->val.float_val;
    } else if (strcmp(opname, "max") == 0) {
            if (svalp->val.typ == &RDB_INTEGER) {
                if (addvalp->val.int_val > svalp->val.val.int_val)
                    svalp->val.val.int_val = addvalp->val.int_val;
            } else {
                if (addvalp->val.float_val > svalp->val.val.float_val)
                    svalp->val.val.float_val = addvalp->val.float_val;
            }
    } else if (strcmp(opname, "min") == 0) {
        if (svalp->val.typ == &RDB_INTEGER) {
            if (addvalp->val.int_val < svalp->val.val.int_val)
                svalp->val.val.int_val = addvalp->val.int_val;
        } else {
            if (addvalp->val.float_val < svalp->val.val.float_val)
                svalp->val.val.float_val = addvalp->val.float_val;
        }
    } else if (strcmp(opname, "any") == 0) {
        if (addvalp->val.bool_val)
            svalp->val.val.bool_val = RDB_TRUE;
    } else if (strcmp(opname, "all") == 0) {
        if (!addvalp->val.bool_val)
            svalp->val.val.bool_val = RDB_FALSE;
    }
    return RDB_OK;
}

static int
do_summarize(RDB_qresult *qrp, RDB_type *tb1typ, RDB_bool hasavg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *lqrp;
    RDB_object tpl;
    RDB_field *keyfv, *nonkeyfv;
    int ret;
    struct RDB_summval *svalv;
    RDB_expression *argp;
    int keyfc = RDB_pkey_len(qrp->matp);
    int addc = (RDB_expr_list_length(&qrp->exp->def.op.args) - 2) / 2;
    int i;
    int avgc = hasavg ? 1 : 0;

    keyfv = RDB_alloc(sizeof (RDB_field) * keyfc, ecp);
    nonkeyfv = RDB_alloc(sizeof (RDB_field) * (addc + avgc), ecp);
    svalv = RDB_alloc(sizeof (struct RDB_summval) * addc, ecp);
    if (keyfv == NULL || nonkeyfv == NULL || svalv == NULL) {
        RDB_free(keyfv);
        RDB_free(nonkeyfv);
        RDB_free(svalv);
        return RDB_ERROR;
    }

    /*
     * Iterate over table 1, modifying the materialized table
     */

    lqrp = RDB_expr_qresult(qrp->exp->def.op.args.firstp, ecp, txp);
    if (lqrp == NULL) {
        return RDB_ERROR;
    }
    if (RDB_duprem(lqrp, ecp, txp) != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    for (i = 0; i < addc; i++)
        RDB_init_obj(&svalv[i].val);
    do {
        int fvidx;
        RDB_int count;

        ret = RDB_next_tuple(lqrp, &tpl, ecp, txp);
        if (ret == RDB_OK) {
            /* Build key */
            for (i = 0; i < keyfc; i++) {
                RDB_object *attrobjp = RDB_tuple_get(&tpl,
                                qrp->matp->val.tbp->keyv[0].strv[i]);
                attrobjp->store_typ = attrobjp->typ;
                ret = RDB_obj_to_field(&keyfv[i], attrobjp, ecp);
                if (ret != RDB_OK)
                    goto cleanup;
            }

            /* Read added attributes from table #2 */
            argp = qrp->exp->def.op.args.firstp->nextp;
            for (i = 0; i < addc; i++) {
                argp = argp->nextp->nextp;

                nonkeyfv[i].no = *RDB_field_no(qrp->matp->val.tbp->stp,
                        RDB_obj_string(&argp->def.obj));
            }

            if (hasavg) {
                nonkeyfv[addc].no = *RDB_field_no(qrp->matp->val.tbp->stp,
                                AVG_COUNT);
                fvidx = addc;
            }

            ret = RDB_get_fields(qrp->matp->val.tbp->stp->recmapp, keyfv,
                    addc + avgc, NULL, nonkeyfv);
            if (ret == RDB_OK) {
                /* If AVG, get count */
                if (hasavg) {
                    memcpy(&count, nonkeyfv[fvidx].datap, sizeof(RDB_int));
                }

                /* A corresponding tuple in table 2 has been found */
                argp = qrp->exp->def.op.args.firstp->nextp->nextp;
                for (i = 0; i < addc; i++) {
                    RDB_type *typ;
                    RDB_object addval;
                    char *opname = argp->def.op.name;

                    RDB_init_obj(&addval);
                    if (strcmp(opname, "count") == 0) {
                        ret = RDB_irep_to_obj(&svalv[i].val, &RDB_INTEGER,
                                nonkeyfv[i].datap, nonkeyfv[i].len, ecp);
                    } else {
                        RDB_expression *exp = argp->def.op.args.firstp;
                        typ = RDB_expr_type_tpltyp(exp, tb1typ->def.basetyp,
                                NULL, NULL, NULL, ecp, txp);
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
                        ret = RDB_evaluate(exp, &RDB_tpl_get, &tpl, NULL, ecp, txp,
                                &addval);
                        if (ret != RDB_OK) {
                            RDB_destroy_obj(&addval, ecp);
                            goto cleanup;
                        }
                    }
                    ret = summ_step(&svalv[i], &addval, opname, count, ecp);
                    if (ret != RDB_OK)
                        goto cleanup;
                    RDB_destroy_obj(&addval, ecp);

                    svalv[i].val.store_typ = svalv[i].val.typ;
                    ret = RDB_obj_to_field(&nonkeyfv[i], &svalv[i].val, ecp);
                    if (ret != RDB_OK)
                        goto cleanup;
                    argp = argp->nextp->nextp;
                }
                if (hasavg) {
                    /* Store count */
                    count++;
                    nonkeyfv[fvidx].datap = &count;
                    nonkeyfv[fvidx].len = sizeof(RDB_int);
                    nonkeyfv[fvidx].copyfp = memcpy;
                }

                RDB_cmp_ecp = ecp;
                ret = RDB_update_rec(qrp->matp->val.tbp->stp->recmapp, keyfv,
                        addc + avgc, nonkeyfv, NULL);
                if (ret != RDB_OK) {
                    RDB_handle_errcode(ret, ecp, txp);
                    goto cleanup;
                }
            } else {
                RDB_handle_errcode(ret, ecp, txp);
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
    RDB_del_qresult(lqrp, ecp, txp);
    RDB_free(keyfv);
    RDB_free(nonkeyfv);
    RDB_free(svalv);

    return ret;
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

    lqrp = RDB_expr_qresult(qrp->exp->def.op.args.firstp->nextp, ecp, txp);
    if (lqrp == NULL) {
        return RDB_ERROR;
    }
    ret = RDB_duprem(lqrp, ecp, txp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    for(;;) {
        ret = RDB_next_tuple(lqrp, &tpl, ecp, txp);
        if (ret != RDB_OK)
            break;

        /* Extend tuple */
        argp = qrp->exp->def.op.args.firstp->nextp->nextp;
        while (argp != NULL) {
            char *name = RDB_obj_string(&argp->nextp->def.obj);
            RDB_expression *opexp = argp;
            char *opname = opexp->def.op.name;
            RDB_type *typ;

            if (strcmp(opname, "count") == 0) {
                ret = RDB_tuple_set_int(&tpl, name, 0, ecp);
            } else if (strcmp(opname, "avg") == 0) {
                ret = RDB_tuple_set_float(&tpl, name, 0, ecp);
            } else if (strcmp(opname, "sum") == 0) {
                typ = RDB_expr_type_tpltyp(opexp->def.op.args.firstp,
                        tb1typ->def.basetyp, NULL, NULL, NULL, ecp, txp);
                if (typ == NULL)
                    goto error;
                if (typ == &RDB_INTEGER)
                    ret = RDB_tuple_set_int(&tpl, name, 0, ecp);
                else
                    ret = RDB_tuple_set_float(&tpl, name, 0.0, ecp);
            } else if (strcmp(opname, "max") == 0) {
                typ = RDB_expr_type_tpltyp(opexp->def.op.args.firstp,
                        tb1typ->def.basetyp, NULL, NULL, NULL, ecp, txp);
                if (typ == NULL)
                    goto error;
                if (typ == &RDB_INTEGER)
                    ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MIN, ecp);
                else {
                    ret = RDB_tuple_set_float(&tpl, name,
                            RDB_FLOAT_MIN, ecp);
                }
            } else if (strcmp(opname, "min") == 0) {
                typ = RDB_expr_type_tpltyp(opexp->def.op.args.firstp,
                        tb1typ->def.basetyp, NULL, NULL, NULL, ecp, txp);
                if (typ == NULL)
                    goto error;
                if (typ == &RDB_INTEGER)
                    ret = RDB_tuple_set_int(&tpl, name, RDB_INT_MAX, ecp);
                else {
                    ret = RDB_tuple_set_float(&tpl, name,
                            RDB_FLOAT_MAX, ecp);
                }
            } else if (strcmp(opname, "all") == 0) {
                ret = RDB_tuple_set_bool(&tpl, name, RDB_TRUE, ecp);
            } else if (strcmp(opname, "any") == 0) {
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
    return RDB_del_qresult(lqrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_del_qresult(lqrp, ecp, txp);

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

    tb1typ = RDB_expr_type(exp->def.op.args.firstp, NULL, NULL,
            NULL, ecp, txp);
    if (tb1typ == NULL)
        return RDB_ERROR;

    tb2typ = RDB_expr_type(exp->def.op.args.firstp->nextp, NULL, NULL,
            NULL, ecp, txp);
    if (tb2typ == NULL)
        goto error;

    reltyp = RDB_summarize_type(&exp->def.op.args, NULL, NULL, ecp, txp);
    if (reltyp == NULL)
        goto error;

    /* If AVG, extend tuple type by count */
    hasavg = RDB_FALSE;
    argp = exp->def.op.args.firstp->nextp->nextp;
    while (argp != NULL) {
        if (strcmp(argp->def.op.name, "avg") == 0) {
            hasavg = RDB_TRUE;
            break;
        }
        argp = argp->nextp->nextp;
    }
    if (hasavg) {
        int attrc = reltyp->def.basetyp->def.tuple.attrc + 1;
        RDB_attr *attrv = RDB_realloc(reltyp->def.basetyp->def.tuple.attrv,
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
        reltyp->def.basetyp->def.tuple.attrc = attrc;
        reltyp->def.basetyp->def.tuple.attrv = attrv;
    }

    /* Key consists of table #2 attributes */
    key.strc = tb2typ->def.basetyp->def.tuple.attrc;
    key.strv = RDB_alloc(sizeof (char *) * key.strc, ecp);
    if (key.strv == NULL) {
        goto error;
    }
    for (i = 0; i < key.strc; i++) {
        key.strv[0] = tb2typ->def.basetyp->def.tuple.attrv[i].name;
    }

    /* Create materialized table */
    qrp->matp = RDB_new_obj(ecp);
    if (qrp->matp == NULL)
        goto error;

    if (RDB_init_table_i(qrp->matp, NULL, RDB_FALSE, reltyp, 1, &key,
            0, NULL, RDB_TRUE, NULL, ecp) != RDB_OK)
        goto error;

    if (init_summ_table(qrp, tb1typ, hasavg, ecp, txp) != RDB_OK) {
        goto error;
    }

    /* summarize over table 1 */
    if (do_summarize(qrp, tb1typ, hasavg, ecp, txp) != RDB_OK) {
        goto error;
    }

    if (RDB_init_stored_qresult(qrp, qrp->matp, NULL, ecp, txp) != RDB_OK) {
        goto error;
    }

    RDB_free(key.strv);
    return RDB_OK;

error:
    RDB_free(key.strv);
    if (qrp->matp != NULL) {
        RDB_drop_table(qrp->matp, ecp, txp);
    } else if (reltyp != NULL) {
        RDB_del_nonscalar_type(reltyp, ecp);
    }
    return RDB_ERROR;
} /* summarize_qresult */

static int
sdivide_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    qrp->exp = exp;
    qrp->nested = RDB_TRUE;
    qrp->val.children.tpl_valid = RDB_FALSE;

    /* Create qresults for table #1 and table #3 */
    qrp->val.children.qrp = RDB_expr_qresult(qrp->exp->def.op.args.firstp,
            ecp, txp);
    if (qrp->val.children.qrp == NULL)
        return RDB_ERROR;
    qrp->val.children.qr2p = RDB_expr_qresult(
            qrp->exp->def.op.args.firstp->nextp->nextp, ecp, txp);
    if (qrp->val.children.qr2p == NULL) {
        RDB_del_qresult(qrp->val.children.qrp, ecp, txp);
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
    char *gattrname = RDB_obj_string(&qrp->exp->def.op.args.lastp->def.obj);
    RDB_type *greltyp = RDB_tuple_type_attr(qrp->matp->typ->def.basetyp,
                        gattrname)->typ;

    keyfc = RDB_pkey_len(qrp->matp);

    keyfv = RDB_alloc(sizeof (RDB_field) * keyfc, ecp);
    if (keyfv == NULL) {
        return RDB_ERROR;
    }

    /*
     * Iterate over the original table, modifying the materialized table
     */

    newqrp = RDB_expr_qresult(qrp->exp->def.op.args.firstp, ecp, txp);
    if (newqrp == NULL) {
        return RDB_ERROR;
    }
    if (RDB_duprem(qrp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&gval);
    do {
        ret = RDB_next_tuple(newqrp, &tpl, ecp, txp);
        if (ret == RDB_OK) {
            /* Build key */
            for (i = 0; i < keyfc; i++) {
                RDB_object *attrobjp = RDB_tuple_get(&tpl,
                        qrp->matp->val.tbp->keyv[0].strv[i]);
                attrobjp->store_typ = attrobjp->typ;                
                ret = RDB_obj_to_field(&keyfv[i], attrobjp, ecp);
                if (ret != RDB_OK)
                    goto cleanup;
            }

            if (qrp->matp->val.tbp->stp != NULL) {
                gfield.no = *RDB_field_no(qrp->matp->val.tbp->stp, gattrname);

                /* Try to read tuple of the materialized table */
                ret = RDB_get_fields(qrp->matp->val.tbp->stp->recmapp, keyfv,
                        1, NULL, &gfield);
                RDB_handle_errcode(ret, ecp, txp);
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
                ret = RDB_obj_to_field(&gfield, &gval, ecp);
                if (ret != RDB_OK)
                    goto cleanup;
                RDB_cmp_ecp = ecp;
                ret = RDB_update_rec(qrp->matp->val.tbp->stp->recmapp, keyfv,
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
                if (RDB_init_table_from_type(&gtb, NULL, reltyp, 0, NULL,
                        0, NULL, ecp) != RDB_OK) {
                    RDB_destroy_obj(&gtb, ecp);
                    RDB_del_nonscalar_type(reltyp, ecp);
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
    RDB_del_qresult(newqrp, ecp, txp);
    RDB_free(keyfv);

    return ret;
} /* do_group */

static int
group_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_string_vec *keyv;
    int keyc;
    int ret;
    RDB_bool freekeys;
    RDB_type *reltyp = RDB_expr_type(exp, NULL, NULL, NULL, ecp, txp);
    if (reltyp == NULL)
        return RDB_ERROR;

    qrp->exp = exp;
    qrp->nested = RDB_FALSE;

    /* Need keys */
    keyc = RDB_infer_keys(exp, NULL, NULL, NULL, ecp, txp, &keyv, &freekeys);
    if (keyc == RDB_ERROR) {
        return RDB_ERROR;
    }

    /* create materialized table */
    qrp->matp = RDB_new_obj(ecp);
    if (qrp->matp == NULL) {
        RDB_free_obj(qrp->matp, ecp);
        return RDB_ERROR;
    }

    reltyp = RDB_dup_nonscalar_type(reltyp, NULL);
    if (reltyp == NULL) {
        RDB_free_obj(qrp->matp, ecp);
        return RDB_ERROR;
    }

    ret = RDB_init_table_i(qrp->matp, NULL, RDB_FALSE, reltyp,
            keyc, keyv, 0, NULL, RDB_TRUE, NULL, ecp);
    if (freekeys)
        RDB_free_keys(keyc, keyv);
    if (ret != RDB_OK) {
        RDB_del_nonscalar_type(reltyp, ecp);
        RDB_drop_table(qrp->matp, ecp, txp);
        return RDB_ERROR;
    }

    /* do the grouping */
    if (do_group(qrp, ecp, txp) != RDB_OK) {
        RDB_drop_table(qrp->matp, ecp, txp);
        return RDB_ERROR;
    }

    if (RDB_init_stored_qresult(qrp, qrp->matp, NULL, ecp, txp) != RDB_OK) {
        RDB_drop_table(qrp->matp, ecp, txp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
init_index_qresult(RDB_qresult *qrp, RDB_object *tbp, RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (RDB_TB_CHECK & tbp->val.tbp->flags) {
        if (RDB_check_table(tbp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }

    qrp->endreached = RDB_FALSE;
    qrp->exp = NULL;
    qrp->nested = RDB_FALSE;
    qrp->val.stored.tbp = tbp;
    qrp->matp = NULL;
    ret = RDB_index_cursor(&qrp->val.stored.curp, indexp->idxp, RDB_FALSE,
            txp != NULL ? txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    ret = RDB_cursor_first(qrp->val.stored.curp);
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
init_where_index_qresult(RDB_qresult *qrp, RDB_expression *texp,
        RDB_tbindex *indexp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_field *fv;
    int flags = 0;

    qrp->exp = texp;
    qrp->nested = RDB_FALSE;
    qrp->matp = NULL;
    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        qrp->val.stored.tbp = texp->def.op.args.firstp->def.tbref.tbp;
    } else {
        qrp->val.stored.tbp = texp->def.op.args.firstp->def.op.args.firstp
                ->def.tbref.tbp;
    }

    if (RDB_table_is_persistent(qrp->val.stored.tbp) && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (indexp->unique) {
        qrp->val.stored.curp = NULL;
        return RDB_OK;
    }

    ret = RDB_index_cursor(&qrp->val.stored.curp, indexp->idxp, RDB_FALSE,
            RDB_table_is_persistent(qrp->val.stored.tbp) ? txp->txid : NULL);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }

    if (texp->def.op.optinfo.objc != indexp->attrc
            || !texp->def.op.optinfo.all_eq) {
        flags = RDB_REC_RANGE;
    }

    fv = RDB_alloc(sizeof (RDB_field) * indexp->attrc, ecp);
    if (fv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < texp->def.op.optinfo.objc; i++) {
        ret = RDB_obj_to_field(&fv[i], texp->def.op.optinfo.objpv[i], ecp);
        if (ret != RDB_OK) {
            RDB_free(fv);
            return RDB_ERROR;
        }
    }

    if (texp->def.op.optinfo.objc > 0) {
        ret = RDB_cursor_seek(qrp->val.stored.curp, texp->def.op.optinfo.objc,
                fv, flags);
    } else {
        ret = RDB_cursor_first(qrp->val.stored.curp);
    }
    if (ret == DB_NOTFOUND) {
        qrp->endreached = RDB_TRUE;
        ret = RDB_OK;
    }

    RDB_free(fv);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
init_eval_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    qrp->matp = RDB_new_obj(ecp);
    if (qrp->matp == NULL)
        return RDB_ERROR;

    RDB_init_obj(qrp->matp);
    if (RDB_evaluate(exp, NULL, NULL, NULL, ecp, txp, qrp->matp) != RDB_OK)
        goto error;

    qrp->endreached = RDB_FALSE;

    return RDB_init_stored_qresult(qrp, qrp->matp, NULL, ecp, txp);

error:
    RDB_free(qrp->matp);
    qrp->matp = NULL;
    return RDB_ERROR;
}

/*
 * Initialize qresult from expression.
 */
static int
init_expr_qresult(RDB_qresult *qrp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    /* Set type so the type of tuple attributes is available */
    if (RDB_expr_type(exp, NULL, NULL, NULL, ecp, txp) == NULL) {
        return RDB_ERROR;
    }

    if (exp->kind == RDB_EX_OBJ) {
        return init_qresult(qrp, &exp->def.obj, ecp, txp);
    }
    if (exp->kind == RDB_EX_TBP) {
        /* Cannot create a cursor over a primary index */
        if (exp->def.tbref.indexp != NULL
                && exp->def.tbref.indexp->idxp != NULL)
            return init_index_qresult(qrp, exp->def.tbref.tbp,
                    exp->def.tbref.indexp, ecp, txp);
        return init_qresult(qrp, exp->def.tbref.tbp, ecp, txp);
    }
    if (exp->kind == RDB_EX_VAR && txp != NULL) {
        RDB_object *tbp = RDB_get_table(exp->def.varname, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;        
        return init_qresult(qrp, tbp, ecp, txp);
    }
    if (exp->kind != RDB_EX_RO_OP) {
        return init_eval_qresult(qrp, exp, ecp, txp);
    }

    /* Transform UPDATE */
    if (strcmp(exp->def.op.name, "update") == 0) {
        if (RDB_convert_update(exp, NULL, NULL, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }

    qrp->endreached = RDB_FALSE;
    qrp->matp = NULL;

    if (strcmp(exp->def.op.name, "where") == 0
            && (exp->def.op.optinfo.objc > 0
                || exp->def.op.optinfo.stopexp != NULL)) {
        /* Check for index */
        RDB_tbindex *indexp;
        if (exp->def.op.args.firstp->kind == RDB_EX_TBP) {
            indexp = exp->def.op.args.firstp->def.tbref.indexp;
        } else if (exp->def.op.args.firstp->kind == RDB_EX_RO_OP
                && strcmp (exp->def.op.args.firstp->def.op.name, "project") == 0
                && exp->def.op.args.firstp->def.op.args.firstp->kind == RDB_EX_TBP) {
            indexp = exp->def.op.args.firstp->def.op.args.firstp->def.tbref.indexp;
        }
        return init_where_index_qresult(qrp, exp, indexp, ecp, txp);
    }

    if (strcmp(exp->def.op.name, "project") == 0) {
        RDB_expression *texp = exp->def.op.args.firstp;

        qrp->val.children.tpl_valid = RDB_FALSE;
        if (texp->kind == RDB_EX_TBP
                && texp->def.tbref.tbp->kind == RDB_OB_TABLE
                && texp->def.tbref.tbp->val.tbp->stp != NULL) {
            if (RDB_init_stored_qresult(qrp, texp->def.tbref.tbp, exp, ecp, txp)
                    != RDB_OK) {
                return RDB_ERROR;
            }
        } else {
            qrp->exp = exp;
            qrp->nested = RDB_TRUE;
            qrp->val.children.qrp = RDB_expr_qresult(exp->def.op.args.firstp, ecp, txp);
            if (qrp->val.children.qrp == NULL)
                return RDB_ERROR;
            qrp->val.children.qr2p = NULL;
        }
        return RDB_OK;        
    }
    if ((strcmp(exp->def.op.name, "where") == 0)
            || (strcmp(exp->def.op.name, "union") == 0)
            || (strcmp(exp->def.op.name, "d_union") == 0)
            || (strcmp(exp->def.op.name, "minus") == 0)
            || (strcmp(exp->def.op.name, "semiminus") == 0)
            || (strcmp(exp->def.op.name, "intersect") == 0)
            || (strcmp(exp->def.op.name, "semijoin") == 0)
            || (strcmp(exp->def.op.name, "extend") == 0)
            || (strcmp(exp->def.op.name, "rename") == 0)
            || (strcmp(exp->def.op.name, "wrap") == 0)
            || (strcmp(exp->def.op.name, "unwrap") == 0)
            || (strcmp(exp->def.op.name, "ungroup") == 0)) {
        qrp->exp = exp;
        qrp->nested = RDB_TRUE;
        qrp->val.children.tpl_valid = RDB_FALSE;
        qrp->val.children.qrp = RDB_expr_qresult(exp->def.op.args.firstp, ecp, txp);
        if (qrp->val.children.qrp == NULL)
            return RDB_ERROR;
        qrp->val.children.qr2p = NULL;
        return RDB_OK;        
    }
    if (strcmp(exp->def.op.name, "group") == 0) {
        return group_qresult(qrp, exp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "join") == 0) {
        return RDB_join_qresult(qrp, exp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "summarize") == 0) {
        return summarize_qresult(qrp, exp, ecp, txp);
    }    
    if (strcmp(exp->def.op.name, "divide") == 0) {
        return sdivide_qresult(qrp, exp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "tclose") == 0) {
        return RDB_tclose_qresult(qrp, exp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "relation") == 0) {
        qrp->nested = RDB_FALSE;
        qrp->exp = exp;
        qrp->val.stored.curp = NULL;

        /* Start with first argument */
        qrp->val.next_exp = exp->def.op.args.firstp;
        return RDB_OK;
    }
    return init_eval_qresult(qrp, exp, ecp, txp);
}

/*
 * Create qresult from expression
 */
RDB_qresult *
RDB_expr_qresult(RDB_expression *exp, RDB_exec_context *ecp,
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

RDB_qresult *
RDB_index_qresult(RDB_object *tbp, struct RDB_tbindex *indexp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_qresult *qrp = RDB_alloc(sizeof (RDB_qresult), ecp);
    if (qrp == NULL) {
        return NULL;
    }
    if (init_index_qresult(qrp, tbp, indexp, ecp, txp) != RDB_OK) {
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

    if (RDB_TB_CHECK & tbp->val.tbp->flags) {
        if (RDB_check_table(tbp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }

    if (RDB_table_is_persistent(tbp) && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (tbp->val.tbp->exp == NULL) {
        return RDB_init_stored_qresult(qrp, tbp, NULL, ecp, txp);
    }
    return init_expr_qresult(qrp, tbp->val.tbp->exp, ecp, txp);
}

/*
 * Check if a qresult based on *exp may return duplicates and store the result in *resp.
 */
static int
expr_dups(RDB_expression *exp, RDB_exec_context *ecp, RDB_bool *resp)
{
    if (exp->kind == RDB_EX_OBJ) {
        if (exp->def.obj.kind != RDB_OB_TABLE
                || exp->def.obj.val.tbp->exp != NULL) {
            RDB_raise_invalid_argument("expression required", ecp);
            return RDB_ERROR;
        }
        *resp = RDB_FALSE;
        return RDB_OK;
    }
    if (exp->kind == RDB_EX_TBP) {
        exp = exp->def.tbref.tbp->val.tbp->exp;
        if (exp == NULL) {
            *resp = RDB_FALSE;
            return RDB_OK;
        }
    }

    if (strcmp(exp->def.op.name, "relation") == 0) {
        /* A tuple may appear twice among the arguments */
        *resp = RDB_TRUE;
        return RDB_OK;
    }

    if (strcmp(exp->def.op.name, "where") == 0
            || strcmp(exp->def.op.name, "minus") == 0
            || strcmp(exp->def.op.name, "semiminus") == 0
            || strcmp(exp->def.op.name, "intersect") == 0
            || strcmp(exp->def.op.name, "semijoin") == 0
            || strcmp(exp->def.op.name, "extend") == 0
            || strcmp(exp->def.op.name, "rename") == 0
            || strcmp(exp->def.op.name, "extend") == 0
            || strcmp(exp->def.op.name, "rename") == 0
            || strcmp(exp->def.op.name, "wrap") == 0
            || strcmp(exp->def.op.name, "unwrap") == 0
            || strcmp(exp->def.op.name, "ungroup") == 0
            || strcmp(exp->def.op.name, "divide") == 0) {
        return expr_dups(exp->def.op.args.firstp, ecp, resp);
    } else if (strcmp(exp->def.op.name, "union") == 0) {
        *resp = RDB_TRUE;
        return RDB_OK;
    } else if (strcmp(exp->def.op.name, "d_union") == 0) {
        *resp = RDB_FALSE;
        return RDB_OK;
    } else if (strcmp(exp->def.op.name, "join") == 0) {
        if (expr_dups(exp->def.op.args.firstp, ecp, resp) != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;
        if (expr_dups(exp->def.op.args.firstp->nextp, ecp, resp) != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;
    } else if (strcmp(exp->def.op.name, "project") == 0) {
        int keyc, newkeyc;
        RDB_string_vec *keyv;
        RDB_bool freekey;

        if (expr_dups(exp->def.op.args.firstp, ecp, resp) != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;

        keyc = RDB_infer_keys(exp->def.op.args.firstp, NULL, NULL, NULL,
                ecp, NULL, &keyv, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;

        newkeyc = RDB_check_project_keyloss(exp, keyc, keyv, NULL, ecp);
        if (newkeyc == RDB_ERROR) {
            if (freekey) {
                RDB_free_keys(keyc, keyv);
            }
            return RDB_ERROR;
        }
        *resp = (RDB_bool) (newkeyc == 0);
        if (freekey) {
            RDB_free_keys(keyc, keyv);
        }
        return RDB_OK;
    }
    *resp = RDB_FALSE;
    return RDB_OK;
}

/*
 * Check if the qresult may return duplicates and store the result in *resp.
 */
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
RDB_duprem(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_transaction *txp)
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
        RDB_type *reltyp = RDB_expr_type(qrp->exp, NULL, NULL, NULL, ecp, txp);
        if (reltyp == NULL)
            return RDB_ERROR;
        reltyp = RDB_dup_nonscalar_type(reltyp, ecp);
        if (reltyp == NULL)
            return RDB_ERROR;

        /* Create materialized (all-key) table */
        qrp->matp = RDB_new_obj(ecp);
        if (qrp->matp == NULL) {
            RDB_del_nonscalar_type(reltyp, ecp);
            return RDB_ERROR;
        }            
        ret = RDB_init_table_i(qrp->matp, NULL, RDB_FALSE, reltyp, 0, NULL,
                0, NULL, RDB_TRUE, NULL, ecp);
        if (ret != RDB_OK) {
            RDB_del_nonscalar_type(reltyp, ecp);
            RDB_free_obj(qrp->matp, ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

RDB_qresult *
RDB_table_qresult(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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

RDB_qresult *
RDB_table_iterator(RDB_object *tbp,
                   int seqitc, const RDB_seq_item seqitv[],
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_qresult *qrp;
    RDB_expression *texp;

    for (i = 0; i < seqitc; i++) {
        RDB_type *attrtyp = RDB_type_attr_type(RDB_obj_type(tbp),
                seqitv[i].attrname);
        if (attrtyp == NULL) {
            RDB_raise_invalid_argument("attribute not found", ecp);
            return NULL;
        }
        if (!RDB_type_is_ordered(attrtyp)) {
            RDB_raise_invalid_argument("attribute type is not ordered", ecp);
            return NULL;
        }
    }

    if (RDB_TB_CHECK & tbp->val.tbp->flags) {
        if (RDB_check_table(tbp, ecp, txp) != RDB_OK)
            return NULL;
    }

    texp = RDB_optimize(tbp, seqitc, seqitv, ecp, txp);
    if (texp == NULL)
        return NULL;

    if (seqitc > 0) {
        RDB_tbindex *indexp = RDB_expr_sortindex(texp);
        if (indexp == NULL || !RDB_index_sorts(indexp, seqitc, seqitv)) {
            /* Create sorter */
            if (RDB_sorter(texp, &qrp, ecp, txp, seqitc, seqitv) != RDB_OK)
                return NULL;
        }
    } else {
        qrp = RDB_expr_qresult(texp, ecp, txp);
        if (qrp == NULL) {
            return NULL;
        }
        /* Add duplicate remover, if necessary */
        if (RDB_duprem(qrp, ecp, txp) != RDB_OK) {
            RDB_del_qresult(qrp, ecp, txp);
            return NULL;
        }
    }
    qrp->opt_exp = texp;
    return qrp;
}

/*
 * Creates a qresult which sorts a table.
 */
int
RDB_sorter(RDB_expression *texp, RDB_qresult **qrpp, RDB_exec_context *ecp,
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
    qrp->val.stored.tbp = NULL;
    qrp->endreached = RDB_FALSE;

    for (i = 0; i < seqitc; i++) {
        key.strv[i] = seqitv[i].attrname;
        ascv[i] = seqitv[i].asc;
    }

    /*
     * Create a sorted RDB_table
     */

    typ = RDB_expr_type(texp, NULL, NULL, NULL, ecp, txp);
    if (typ == NULL)
        goto error;
    typ = RDB_dup_nonscalar_type(typ, ecp);
    if (typ == NULL)
        goto error;

    qrp->matp = RDB_new_rtable(NULL, RDB_FALSE,
            typ, 1, &key, 0, NULL, RDB_TRUE, ecp);
    if (qrp->matp == NULL) {
        RDB_del_nonscalar_type(typ, ecp);
        goto error;
    }

    if (RDB_create_stored_table(qrp->matp,
            txp!= NULL ? txp->dbp->dbrootp->envp : NULL, ascv, ecp, txp) != RDB_OK)
        goto error;

    /*
     * Copy tuples into the newly created RDB_table
     */

    tmpqrp = RDB_expr_qresult(texp, ecp, txp);
    if (tmpqrp == NULL)
        goto error;

    /*
     * The duplicate elimination can produce a error message
     * "Duplicate data items are not supported with sorted data"
     * in the error log. This is expected behaviour.
     */
    RDB_init_obj(&tpl);
    while ((ret = RDB_next_tuple(tmpqrp, &tpl, ecp, txp)) == RDB_OK) {
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

    ret = RDB_del_qresult(tmpqrp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_init_stored_qresult(qrp, qrp->matp, NULL, ecp, txp);
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

static int
next_ungroup_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object *attrtbp;
    RDB_hashtable_iter hiter;
    char *attrname = RDB_obj_string(&qrp->exp->def.op.args.firstp->nextp->def.obj);

    /* If no tuple has been read, read first tuple */
    if (!qrp->val.children.tpl_valid) {
        RDB_init_obj(&qrp->val.children.tpl);
        ret = RDB_next_tuple(qrp->val.children.qrp, &qrp->val.children.tpl,
                ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        qrp->val.children.tpl_valid = RDB_TRUE;
    }

    /* If there is no 2nd qresult, open it from relation-valued attribute */
    if (qrp->val.children.qr2p == NULL) {
        attrtbp = RDB_tuple_get(&qrp->val.children.tpl, attrname);

        qrp->val.children.qr2p = RDB_table_qresult(attrtbp, ecp, txp);
        if (qrp->val.children.qr2p == NULL)
            return RDB_ERROR;
    }

    /* Read next element of relation-valued attribute */
    ret = RDB_next_tuple(qrp->val.children.qr2p, tplp, ecp, txp);
    while (ret == RDB_ERROR
            && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        /* Destroy qresult over relation-valued attribute */
        ret = RDB_del_qresult(qrp->val.children.qr2p, ecp, txp);
        qrp->val.children.qr2p = NULL;
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* Read next tuple */
        ret = RDB_next_tuple(qrp->val.children.qrp, &qrp->val.children.tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        attrtbp = RDB_tuple_get(&qrp->val.children.tpl, attrname);

        /* Create qresult over relation-valued attribute */
        qrp->val.children.qr2p = RDB_table_qresult(attrtbp, ecp, txp);
        if (qrp->val.children.qr2p == NULL) {
            return RDB_ERROR;
        }

        ret = RDB_next_tuple(qrp->val.children.qr2p, tplp, ecp, txp);
    }
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    /* Merge tuples, skipping the relation-valued attribute */
    RDB_init_hashtable_iter(&hiter,
            (RDB_hashtable *) &qrp->val.children.tpl.val.tpl_tab);
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
find_str_exp(RDB_expression *exp, const char *str)
{
    int i = 0;
    do {
        if (strcmp(RDB_obj_string(&exp->def.obj), str) == 0)
            return i;
        exp = exp->nextp;
        ++i;
    } while (exp != NULL);
    return -1;
}

static int
next_unwrap(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_hashtable_iter it;
    tuple_entry *entryp;
    RDB_expression *argp;
    RDB_expression *exp = qrp->exp;

    RDB_init_obj(&tpl);
    ret = RDB_next_tuple(qrp->val.children.qrp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    argp = exp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        char *attrname = RDB_obj_string(&argp->def.obj);
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

        ret = RDB_copy_tuple(tplp, wtplp, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        argp = argp->nextp;
    }
    
    /* Copy remaining attributes */
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&tpl.val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        /* Copy attribute if it does not appear in attrv */
        if (find_str_exp(exp->def.op.args.firstp->nextp, entryp->key) == -1) {
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

int
RDB_seek_index_qresult(RDB_qresult *qrp, struct RDB_tbindex *indexp,
        const RDB_object *tplp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_field *fv = RDB_alloc(sizeof (RDB_field) * indexp->attrc, ecp);
    if (fv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < indexp->attrc; i++) {
        RDB_object *attrobjp = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
        /* !! attrobjp->typ == NULL */
        attrobjp->store_typ = attrobjp->typ;
        ret = RDB_obj_to_field(&fv[i], attrobjp, ecp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_cursor_seek(qrp->val.stored.curp, indexp->attrc, fv, RDB_REC_RANGE);
    if (ret == RDB_OK) {
        qrp->endreached = RDB_FALSE;
    } else {
        if (ret == DB_NOTFOUND) {
            qrp->endreached = RDB_TRUE;
            ret = RDB_OK;
        } else {
            RDB_handle_errcode(ret, ecp, txp);
        }
    }

cleanup:
    RDB_free(fv);

    return ret;
}

/*
 * Read a tuple from a table using the unique index given by indexp,
 * using the values given by objpv as a key.
 * Read only the attributes in tpltyp.
 */
int
RDB_get_by_uindex(RDB_object *tbp, RDB_object *objpv[], RDB_tbindex *indexp,
        RDB_type *tpltyp, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *tplp)
{
    RDB_field *fv;
    RDB_field *resfv = NULL;
    int i;
    int ret;
    int keylen = indexp->attrc;
    int resfc = tpltyp->def.tuple.attrc - keylen;

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
        ret = RDB_obj_to_field(&fv[i], objpv[i], ecp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    /*
     * Set 'no' fields of resfv and Read fields
     */
    if (indexp->idxp == NULL) {
        int rfi = 0;

        for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
            int fno = *RDB_field_no(tbp->val.tbp->stp, tpltyp->def.tuple.attrv[i].name);

            if (fno >= keylen)
                resfv[rfi++].no = fno;
        }
        ret = RDB_get_fields(tbp->val.tbp->stp->recmapp, fv, resfc,
                             RDB_table_is_persistent(tbp) ? txp->txid : NULL, resfv);
    } else {
        int rfi = 0;

        for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
            int j;
            int fno = *RDB_field_no(tbp->val.tbp->stp, tpltyp->def.tuple.attrv[i].name);

            /* Search field number in index */
            for (j = 0; j < keylen && indexp->idxp->fieldv[j] != fno; j++);

            /* If not found, so the field must be read from the DB */
            if (j >= keylen)
                resfv[rfi++].no = fno;
        }
        ret = RDB_index_get_fields(indexp->idxp, fv, resfc,
                RDB_table_is_persistent(tbp) ? txp->txid : NULL, resfv);
    }

    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
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
    for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
        int rfi; /* Index in resv, -1 if key attr */
        char *attrname = tpltyp->def.tuple.attrv[i].name;
        RDB_int fno = *RDB_field_no(tbp->val.tbp->stp, attrname);

        /* Search field number in resfv */
        rfi = 0;
        while (rfi < tpltyp->def.tuple.attrc - keylen
                && resfv[rfi].no != fno)
            rfi++;
        if (rfi >= tpltyp->def.tuple.attrc - keylen)
            rfi = -1;

        if (rfi != -1) {
            /* non-key attribute */
            RDB_object val;

            RDB_init_obj(&val);
            ret = RDB_irep_to_obj(&val, tpltyp->def.tuple.attrv[i].typ,
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
next_project(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object *valp;
    RDB_expression *argp;

    /* Get tuple */
    if (!qrp->nested) {
        if (tplp != NULL) {
            ret = RDB_get_by_cursor(qrp->val.stored.tbp, qrp->val.stored.curp,
                    RDB_expr_type(qrp->exp, NULL, NULL, NULL, ecp, txp)->def.basetyp,
                    tplp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        }
        ret = RDB_cursor_next(qrp->val.stored.curp, 0);
        if (ret == DB_NOTFOUND)
            qrp->endreached = RDB_TRUE;
        else if (ret != RDB_OK)
            return RDB_ERROR;
    } else {
        RDB_object tpl;

        RDB_init_obj(&tpl);

        ret = RDB_next_tuple(qrp->val.children.qrp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }

        if (tplp != NULL) {
            /* Copy attributes into new tuple */
            argp = qrp->exp->def.op.args.firstp->nextp;
            while (argp != NULL) {
                char *attrname = RDB_obj_string(&argp->def.obj);

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
    RDB_tbindex *indexp;
    RDB_bool rtup;
    RDB_type *tpltyp;
    RDB_type *reltyp = NULL;
    RDB_bool dup = RDB_FALSE;

    if (qrp->exp->def.op.args.firstp->kind == RDB_EX_TBP) {
        indexp = qrp->exp->def.op.args.firstp->def.tbref.indexp;
        tpltyp = qrp->val.stored.tbp->typ->def.basetyp;
    } else {
        indexp = qrp->exp->def.op.args.firstp->def.op.args.firstp->def.tbref.indexp;
        reltyp = RDB_expr_type(qrp->exp->def.op.args.firstp, NULL, NULL,
                NULL, ecp, txp);
        if (reltyp == NULL)
            return RDB_ERROR;
        tpltyp = reltyp->def.basetyp;
    }

    if (qrp->val.stored.curp == NULL) {
        ret = RDB_get_by_uindex(qrp->val.stored.tbp,
                qrp->exp->def.op.optinfo.objpv, indexp,
                tpltyp, ecp, txp, tplp);
        if (ret != RDB_OK) {
            goto error;
        }

        qrp->endreached = RDB_TRUE;
        return RDB_OK;
    }

    if (qrp->exp->def.op.optinfo.objc == indexp->attrc
            && qrp->exp->def.op.optinfo.all_eq)
        dup = RDB_TRUE;

    do {
        ret = RDB_next_stored_tuple(qrp, qrp->val.stored.tbp, tplp,
                qrp->exp->def.op.optinfo.asc, dup, tpltyp, ecp, txp);
        if (ret != RDB_OK)
            goto error;
        if (qrp->exp->def.op.optinfo.all_eq)
            rtup = RDB_TRUE;
        else {
            if (qrp->exp->def.op.optinfo.stopexp != NULL) {
                ret = RDB_evaluate_bool(qrp->exp->def.op.optinfo.stopexp,
                        &RDB_tpl_get, tplp, NULL, ecp, txp, &rtup);
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
            ret = RDB_evaluate_bool(qrp->exp->def.op.args.firstp->nextp, &RDB_tpl_get,
                    tplp, NULL, ecp, txp, &rtup);
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
    int wrapc = (RDB_expr_list_length(&exp->def.op.args) - 1) / 2;

    RDB_init_obj(&tpl);

    /* Wrap attributes */
    argp = exp->def.op.args.firstp->nextp;
    for (i = 0; i < wrapc; i++) {
        char *wrattrname;
        RDB_object *attrp;
        char *attrname = RDB_obj_string(&argp->nextp->def.obj);
        RDB_int len = RDB_array_length(&argp->def.obj, ecp);
        if (len == RDB_ERROR) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        for (j = 0; j < len; j++) {
            objp = RDB_array_get(&argp->def.obj,
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
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&tplp->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        int i;
        /* char *attrname = RDB_obj_string(&exp->def.op.argv[2 + 2 * i]->var.obj); */

        i = 0;
        ret = RDB_INT_MAX;
        argp = exp->def.op.args.firstp->nextp;
        while (i < wrapc && ret == RDB_INT_MAX) {
            ret = find_str(&argp->def.obj, entryp->key,
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
next_wrap(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    ret = RDB_next_tuple(qrp->val.children.qrp, &tpl, ecp, txp);
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

        if (qrp->val.children.qrp != NULL)
            ret = RDB_del_qresult(qrp->val.children.qrp, ecp, txp);
        else
            ret = RDB_OK;
        if (qrp->val.children.qr2p != NULL) {
            ret2 = RDB_del_qresult(qrp->val.children.qr2p, ecp, txp);
            if (ret2 != RDB_OK)
                ret = ret2;
        }
        if (qrp->val.children.tpl_valid)
            RDB_destroy_obj(&qrp->val.children.tpl, ecp);
    } else if (qrp->val.stored.curp != NULL) {
        ret = RDB_destroy_cursor(qrp->val.stored.curp);
        if (ret != RDB_OK) {
            RDB_handle_errcode(ret, ecp, txp);
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
RDB_sdivide_preserves(RDB_expression *exp, const RDB_object *tplp,
        RDB_qresult *qr3p, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_bool *resultp)
{
    int ret;
    int i;
    RDB_object tpl2;
    RDB_qresult qr;
    RDB_type *tb1tuptyp;
    RDB_bool matchall = RDB_TRUE;
    RDB_type *tb1typ = RDB_expr_type(exp->def.op.args.firstp,
            NULL, NULL, NULL, ecp, txp);
    if (tb1typ == NULL)
        return RDB_ERROR;
    if (tb1typ->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("argument #1 of DIVIDE must be a relation",
                ecp);
        return RDB_ERROR;
    }
    tb1tuptyp = tb1typ->def.basetyp;

    /*
     * Join this tuple with all tuples from table 2 and set matchall to RDB_FALSE
     * if not all the result tuples are an element of table 3
     */

    if (init_expr_qresult(&qr, exp->def.op.args.firstp->nextp, ecp, txp) != RDB_OK) {
        return RDB_ERROR;
    }

    for (;;) {
        RDB_bool b;
        RDB_bool match = RDB_TRUE;

        RDB_init_obj(&tpl2);

        ret = RDB_next_tuple(&qr, &tpl2, ecp, txp);
        if (ret != RDB_OK)
            break;

        /* Join *tplp and tpl2 into tpl2 */
        for (i = 0; i < tb1tuptyp->def.tuple.attrc; i++) {
             RDB_object *objp = RDB_tuple_get(tplp,
                     tb1tuptyp->def.tuple.attrv[i].name);
             RDB_object *dstobjp = RDB_tuple_get(&tpl2,
                     tb1tuptyp->def.tuple.attrv[i].name);

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
                         tb1tuptyp->def.tuple.attrv[i].name, objp, ecp);
                 if (ret != RDB_OK) {
                     destroy_qresult(&qr, ecp, txp);
                     RDB_destroy_obj(&tpl2, ecp);
                     return RDB_ERROR;
                 }
             }
        }
        if (!match)
            continue;

        ret = RDB_expr_matching_tuple(exp->def.op.args.firstp->nextp->nextp,
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
next_sdivide(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    do {
        ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        ret = RDB_sdivide_preserves(qrp->exp, tplp, qrp->val.children.qr2p,
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
        ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            break;
        ret = RDB_evaluate_bool(qrp->exp->def.op.args.firstp->nextp,
                &RDB_tpl_get, tplp, NULL, ecp, txp, &expres);
        if (ret != RDB_OK)
            break;
    } while (!expres);

    return ret;
}

static int
next_rename(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);
    if (RDB_next_tuple(qrp->val.children.qrp, &tpl, ecp, txp) != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    ret = RDB_rename_tuple_ex(tplp, &tpl, qrp->exp, ecp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static int
next_semiminus_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    do {
        ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        ret = RDB_expr_matching_tuple(qrp->exp->def.op.args.firstp->nextp,
                tplp, ecp, txp, &b);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    } while (b);
    return RDB_OK;
}

static int
next_union(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (!qrp->val.children.qrp->endreached) {
        ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
        if (ret == RDB_OK || RDB_obj_type(RDB_get_err(ecp))
                    != &RDB_NOT_FOUND_ERROR)
            return ret;
    }
    RDB_clear_err(ecp);

    /* Switch to second table */
    if (qrp->val.children.qr2p == NULL) {
        qrp->val.children.qr2p = RDB_expr_qresult(
                qrp->exp->def.op.args.firstp->nextp, ecp, txp);
        if (qrp->val.children.qr2p == NULL)
            return RDB_ERROR;
    }
    return RDB_next_tuple(qrp->val.children.qr2p, tplp, ecp, txp);
}

static int
next_d_union(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool match;

    if (!qrp->val.children.qrp->endreached) {
        ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
        if (ret == RDB_OK || RDB_obj_type(RDB_get_err(ecp))
                    != &RDB_NOT_FOUND_ERROR)
            return ret;
    }
    RDB_clear_err(ecp);

    /* Switch to second table */
    if (qrp->val.children.qr2p == NULL) {
        qrp->val.children.qr2p = RDB_expr_qresult(
                qrp->exp->def.op.args.firstp->nextp, ecp, txp);
        if (qrp->val.children.qr2p == NULL)
            return RDB_ERROR;
    }

    if (RDB_next_tuple(qrp->val.children.qr2p, tplp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    /* Check if tuple is in the 1st argument */
    if (RDB_expr_matching_tuple(qrp->exp->def.op.args.firstp, tplp, ecp, txp,
            &match) != RDB_OK) {
        return RDB_ERROR;
    }
    if (match) {
        RDB_raise_element_exists("duplicate tuple in d_union", ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
next_semijoin(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_bool b;
    do {
        ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        ret = RDB_expr_matching_tuple(qrp->exp->def.op.args.firstp->nextp,
                tplp, ecp, txp, &b);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
    } while (!b);
    return RDB_OK;
}

static int
next_extend(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object obj;
    RDB_expression *argp;
    
    ret = RDB_next_tuple(qrp->val.children.qrp, tplp, ecp, txp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    argp = qrp->exp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        RDB_init_obj(&obj);
        if (RDB_evaluate(argp, &RDB_tpl_get, tplp, NULL, ecp,
                txp, &obj) != RDB_OK) {
            RDB_destroy_obj(&obj, ecp);
            return RDB_ERROR;
        }
        ret = RDB_tuple_set(tplp, RDB_obj_string(
                &argp->nextp->def.obj),
                &obj, ecp);
        RDB_destroy_obj(&obj, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        argp = argp->nextp->nextp;
    }
    return RDB_OK;
}

static int
next_relation(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    /* endreached is meaningless, next_exp tells us if we're done */
    if (qrp->val.next_exp == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    /* Evaluate argument and go to the next */
    ret = RDB_evaluate(qrp->val.next_exp, NULL, NULL, NULL, ecp, txp, tplp);
    qrp->val.next_exp = qrp->val.next_exp->nextp;
    return ret;
}

int
RDB_next_tuple(RDB_qresult *qrp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
	int ret;

    if (qrp->endreached) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    if (qrp->exp == NULL) {
        RDB_type *tpltyp;
        if (qrp->val.stored.tbp == NULL) {
            /* It's a sorter */
           return RDB_next_stored_tuple(qrp, qrp->matp, tplp, RDB_TRUE, RDB_FALSE,
                    qrp->matp->typ->def.basetyp, ecp, txp);
	    }
        tpltyp = qrp->val.stored.tbp->typ->kind == RDB_TP_RELATION ?
                qrp->val.stored.tbp->typ->def.basetyp
                : RDB_obj_impl_type(qrp->val.stored.tbp)->def.scalar.arep->def.basetyp;
        return RDB_next_stored_tuple(qrp, qrp->val.stored.tbp, tplp, RDB_TRUE,
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

        if (strcmp(qrp->exp->def.op.name, "where") == 0) {
            if (qrp->nested) {
                if (next_where_tuple(qrp, tplp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            } else {
                if (next_where_index(qrp, tplp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }                
        } else if (strcmp(qrp->exp->def.op.name, "project") == 0) {
            if (next_project(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "rename") == 0) {
            if (next_rename(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "join") == 0) {
            if (RDB_next_join(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if ((strcmp(qrp->exp->def.op.name, "minus") == 0)
                || (strcmp(qrp->exp->def.op.name, "semiminus") == 0)) {
            if (next_semiminus_tuple(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "union") == 0) {
            if (next_union(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "d_union") == 0) {
            if (next_d_union(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if ((strcmp(qrp->exp->def.op.name, "intersect") == 0)
                || (strcmp(qrp->exp->def.op.name, "semijoin") == 0)) {
            if (next_semijoin(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "extend") == 0) {
            if (next_extend(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "wrap") == 0) {
            if (next_wrap(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "unwrap") == 0) {
            if (next_unwrap(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "divide") == 0) {
            if (next_sdivide(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "tclose") == 0) {
            if (RDB_next_tclose_tuple(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "ungroup") == 0) {
            if (next_ungroup_tuple(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else if (strcmp(qrp->exp->def.op.name, "relation") == 0) {
            if (next_relation(qrp, tplp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        } else {
            RDB_raise_internal(qrp->exp->def.op.name, ecp);
            return RDB_ERROR;
        }

        /* Check for duplicate, if necessary */
        if (qrp->matp != NULL && strcmp(qrp->exp->def.op.name, "tclose") != 0) {
            ret = RDB_insert_real(qrp->matp, tplp, ecp, txp);
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
RDB_reset_qresult(RDB_qresult *qrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    /*
     * If it is a qresult over a RELATION expression, we don't have to do anything
     */
    if (qrp->exp != NULL && qrp->exp->kind == RDB_EX_RO_OP
            && strcmp(qrp->exp->def.op.name, "relation") == 0) {
        return RDB_OK;
    }

    if (qrp->nested) {
        ret = RDB_reset_qresult(qrp->val.children.qrp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        if (qrp->val.children.qr2p != NULL) {
            ret = RDB_reset_qresult(qrp->val.children.qr2p, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        }
        if (qrp->val.children.tpl_valid) {
            RDB_destroy_obj(&qrp->val.children.tpl, ecp);
            qrp->val.children.tpl_valid = RDB_FALSE;
        }
    } else {
        if (qrp->val.stored.curp != NULL) {
            /* Reset cursor */
            ret = RDB_cursor_first(qrp->val.stored.curp);
            if (ret == DB_NOTFOUND) {
                qrp->endreached = RDB_TRUE;
                ret = RDB_OK;
            } else {
                qrp->endreached = RDB_FALSE;
            }
            if (ret != RDB_OK) {
                RDB_handle_errcode(ret, ecp, txp);
                return RDB_ERROR;
            }
        } else {
            /* Unique index qresult w/o cursor */
            qrp->endreached = RDB_FALSE;
        }
    }
    if (qrp->exp != NULL && qrp->matp != NULL) {
        /* Clear materialized result */
        if (RDB_delete_real(qrp->matp, NULL, NULL, NULL, ecp, txp)
                == (RDB_int) RDB_ERROR)
            return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_del_qresult(RDB_qresult *qrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret = destroy_qresult(qrp, ecp, txp);

    RDB_free(qrp);

    return ret;
}

int
RDB_del_table_iterator(RDB_qresult *qrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *texp = qrp->opt_exp;
    int ret = RDB_del_qresult(qrp, ecp, txp);
    RDB_del_expr(texp, ecp);
    return ret;
}
