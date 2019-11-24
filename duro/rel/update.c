/*
 * Copyright (C) 2003-2008, 2012-2013, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "update.h"
#include "typeimpl.h"
#include "qresult.h"
#include "insert.h"
#include "internal.h"
#include "stable.h"
#include "sqlgen.h"
#include "delete.h"
#include <obj/objinternal.h>
#include <gen/strfns.h>

#ifdef POSTGRESQL
#include <pgrec/pgenv.h>
#endif

#include <string.h>
#include <stdio.h>

static RDB_bool
is_keyattr(const char *attrname, RDB_object *tbp, RDB_exec_context *ecp)
{
    int i, j;
    int keyc;
    RDB_string_vec *keyv;
    
    keyc = RDB_table_keys(tbp, ecp, &keyv);
    for (i = 0; i < keyc; i++)
        for (j = 0; j < keyv[i].strc; j++)
            if (strcmp(attrname, keyv[i].strv[j]) == 0)
                return RDB_TRUE;
    return RDB_FALSE;
}

static int
upd_to_vals(int updc, const RDB_attr_update updv[],
            RDB_object *tplp, RDB_object *valv,
            RDB_getobjfn *getfn, void *getarg,
            RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    struct RDB_tuple_and_getfn tg;

    tg.tplp = tplp;
    tg.getfn = getfn;
    tg.getarg = getarg;

    for (i = 0; i < updc; i++) {
        if (RDB_evaluate(updv[i].exp, &RDB_get_from_tuple_or_fn, &tg, NULL,
                ecp, txp, &valv[i]) != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static RDB_int
update_stored_complex(RDB_object *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    int ret;
    int i;
    RDB_bool b;
    struct RDB_tuple_and_getfn tg;
    RDB_object tmptb;
    RDB_type *tmptbtyp;
    RDB_object copytb;
    RDB_type *tpltyp = tbp->typ->def.basetyp;
    RDB_cursor *curp = NULL;
    RDB_object *valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    if (valv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);
    RDB_init_obj(&tpl);
    RDB_init_obj(&tmptb);

    if (!RDB_table_is_persistent(tbp)) {
        /* Make copy of the table so it can be restored if the update fails */
        RDB_type *copytbtyp;

        RDB_init_obj(&copytb);
        copytbtyp = RDB_dup_nonscalar_type(tbp->typ, ecp);
        if (copytbtyp == NULL) {
            rcount = (RDB_int) RDB_ERROR;
            goto cleanup;
        }
        if (RDB_init_table_from_type(&copytb, NULL, copytbtyp,
                1, tbp->val.tbp->keyv, 0, NULL, ecp) != RDB_OK) {
            RDB_del_nonscalar_type(copytbtyp, ecp);
            rcount = (RDB_int) RDB_ERROR;
            goto cleanup;
        }
        if (RDB_move_tuples(&copytb, tbp, RDB_DISTINCT, ecp, NULL) == (RDB_int) RDB_ERROR) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    /*
     * Iterate over the records and insert the updated records into
     * a temporary table. For the temporary table, only one key is needed.
     */
    tmptbtyp = RDB_dup_nonscalar_type(tbp->typ, ecp);
    if (tmptbtyp == NULL) {
        rcount = (RDB_int) RDB_ERROR;
        goto cleanup;
    }

    ret = RDB_init_table_from_type(&tmptb, NULL, tmptbtyp,
            1, tbp->val.tbp->keyv, 0, NULL, ecp);
    if (ret != RDB_OK) {
        RDB_del_nonscalar_type(tmptbtyp, ecp);
        rcount = (RDB_int) RDB_ERROR;
        goto cleanup;
    }

    curp = RDB_recmap_cursor(tbp->val.tbp->stp->recmapp, RDB_TRUE,
            RDB_table_is_persistent(tbp) ? txp->tx : NULL, ecp);
    if (curp == NULL) {
        rcount = (RDB_int) RDB_ERROR;
        goto cleanup;
    }
    ret = RDB_cursor_first(curp, ecp);
    rcount = 0;
    while (ret == RDB_OK) {
        if (RDB_get_by_cursor(tbp, curp, tpltyp, &tpl, ecp, txp) != RDB_OK) {
            rcount = (RDB_int) RDB_ERROR;
            goto cleanup;
        }

        /* Evaluate condition */
        if (condp == NULL)
            b = RDB_TRUE;
        else {
            tg.tplp = &tpl;
            tg.getfn = getfn;
            tg.getarg = getarg;

            ret = RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL, ecp,
                    RDB_table_is_persistent(tbp) ? txp : NULL, &b);
            if (ret != RDB_OK) {
                rcount = (RDB_int) RDB_ERROR;
                return ret;
            }
        }
        if (b) {
            ret = upd_to_vals(updc, updv, &tpl, valv, getfn, getarg,
                    ecp, RDB_table_is_persistent(tbp) ? txp : NULL);
            if (ret != RDB_OK) {
                rcount = (RDB_int) RDB_ERROR;
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Update tuple */
                ret = RDB_tuple_set(&tpl, updv[i].name, &valv[i], ecp);
                if (ret != RDB_OK) {
                    rcount = (RDB_int) RDB_ERROR;
                    goto cleanup;
                }
            }

            /* Insert tuple into temporary table */
            ret = RDB_insert_nonvirtual(&tmptb, &tpl, ecp,
                    RDB_table_is_persistent(tbp) ? txp : NULL);
            /*
             * If the elements already exists, more than one tuple are combined into one,
             * and that's OK
             */
            if (ret != RDB_OK
                    && RDB_obj_type(RDB_get_err(ecp)) != &RDB_ELEMENT_EXISTS_ERROR) {
                rcount = (RDB_int) RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        ret = RDB_cursor_next(curp, 0, ecp);
    };

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_handle_err(ecp,
                RDB_table_is_persistent(tbp) ? txp : NULL);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Delete the updated records from the original table.
     */

    /* Reset cursor */
    ret = RDB_cursor_first(curp, ecp);
    
    RDB_cmp_ecp = ecp;
    while (ret == RDB_OK) {
        if (RDB_get_by_cursor(tbp, curp, tpltyp, &tpl, ecp, txp) != RDB_OK) {
            rcount = (RDB_int) RDB_ERROR;
            goto cleanup;
        }

        /* Evaluate condition */
        if (condp == NULL) {
            b = RDB_TRUE;
        } else {
            tg.tplp = &tpl;
            tg.getfn = getfn;
            tg.getarg = getarg;

            ret = RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL, ecp,
                    RDB_table_is_persistent(tbp) ? txp : NULL, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        if (b) {
            /* Delete tuple */
            ret = RDB_cursor_delete(curp, ecp);
            if (ret != RDB_OK) {
                RDB_handle_err(ecp,
                        RDB_table_is_persistent(tbp) ? txp : NULL);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        ret = RDB_cursor_next(curp, 0, ecp);
    };

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_handle_err(ecp,
                RDB_table_is_persistent(tbp) ? txp : NULL);
        rcount = RDB_ERROR;
        goto cleanup;
    }
    
    ret = RDB_destroy_cursor(curp, ecp);
    curp = NULL;
    if (ret != RDB_OK) {
        RDB_handle_err(ecp,
                RDB_table_is_persistent(tbp) ? txp : NULL);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Insert the records from the temporary table into the original table.
     */
    if (RDB_move_tuples(tbp, &tmptb, RDB_DISTINCT, ecp,
            RDB_table_is_persistent(tbp) ? txp : NULL) == (RDB_int) RDB_ERROR) {
        rcount = RDB_ERROR;
        if (!RDB_table_is_persistent(tbp)) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_KEY_VIOLATION_ERROR)
                goto cleanup;
            if (RDB_delete_nonvirtual(tbp, NULL, NULL, NULL, ecp, NULL) == (RDB_int) RDB_ERROR)
                goto cleanup;
            RDB_move_tuples(tbp, &copytb, RDB_DISTINCT, ecp, NULL);

            /* Restore error */
            RDB_raise_key_violation("", ecp);
        }
    }

cleanup:
    RDB_destroy_obj(&tmptb, ecp);
    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp, ecp);
        if (ret != RDB_OK) {
            RDB_handle_err(ecp,
                    RDB_table_is_persistent(tbp) ? txp : NULL);
            rcount = RDB_ERROR;
        }
    }
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    RDB_free(valv);

    if (RDB_destroy_obj(&tpl, ecp) != RDB_OK) {
        rcount = RDB_ERROR;
    }

    if (!RDB_table_is_persistent(tbp)) {
        RDB_destroy_obj(&copytb, ecp);
    }
    return rcount;
}

static RDB_int
update_stored_simple(RDB_object *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    int ret;
    int i;
    void *datap;
    size_t len;
    RDB_bool b;
    struct RDB_tuple_and_getfn tg;
    RDB_type *tpltyp = tbp->typ->def.basetyp;
    RDB_cursor *curp = NULL;
    RDB_object *valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    RDB_field *fieldv = RDB_alloc(sizeof(RDB_field) * updc, ecp);

    if (valv == NULL || fieldv == NULL) {
        RDB_free(valv);
        RDB_free(fieldv);
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);
    RDB_init_obj(&tpl);

    /*
     * Iterator over the records and update them if the select expression
     * evaluates to true.
     */
    curp = RDB_recmap_cursor(tbp->val.tbp->stp->recmapp, RDB_TRUE,
            RDB_table_is_persistent(tbp) ? txp->tx : NULL, ecp);
    if (curp == NULL) {
        RDB_handle_err(ecp, RDB_table_is_persistent(tbp) ? txp : NULL);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    rcount = 0;
    ret = RDB_cursor_first(curp, ecp);
    while (ret == RDB_OK) {
        /* Read tuple */
        for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
            RDB_object val;

            ret = RDB_cursor_get(curp,
                    *RDB_field_no(tbp->val.tbp->stp, tpltyp->def.tuple.attrv[i].name),
                    &datap, &len, ecp);
            if (ret != RDB_OK) {
                RDB_handle_err(ecp, RDB_table_is_persistent(tbp) ? txp : NULL);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            RDB_init_obj(&val);
            if (RDB_irep_to_obj(&val, tpltyp->def.tuple.attrv[i].typ,
                              datap, len, ecp) != RDB_OK) {
                RDB_destroy_obj(&val, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            ret = RDB_tuple_set(&tpl, tpltyp->def.tuple.attrv[i].name, &val,
                    ecp);
            RDB_destroy_obj(&val, ecp);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        /* Evaluate condition */
        if (condp != NULL) {
            tg.tplp = &tpl;
            tg.getfn = getfn;
            tg.getarg = getarg;
            ret = RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL, ecp,
                    RDB_table_is_persistent(tbp) ? txp : NULL, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        } else {
            b = RDB_TRUE;
        }

        if (b) {
            /* Perform update */
            if (upd_to_vals(updc, updv, &tpl, valv, getfn, getarg,
                    ecp, RDB_table_is_persistent(tbp) ? txp : NULL) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Get field number from map */
                fieldv[i].no = *RDB_field_no(tbp->val.tbp->stp, updv[i].name);

                /* Set type - needed for tuple and array attributes */
                valv[i].store_typ = RDB_type_attr_type(RDB_obj_type(tbp),
                            updv[i].name);

                /* Get data */
                if (RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
                    ret = RDB_ERROR;
                    goto cleanup;
                }
            }
            ret = RDB_cursor_set(curp, updc, fieldv, ecp);
            if (ret != RDB_OK) {
                RDB_handle_err(ecp, RDB_table_is_persistent(tbp) ? txp : NULL);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        ret = RDB_cursor_next(curp, 0, ecp);
    };

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_handle_err(ecp, RDB_table_is_persistent(tbp) ? txp : NULL);
        rcount = RDB_ERROR;
     }

cleanup:
    RDB_free(fieldv);

    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp, ecp);
        if (ret != RDB_OK) {
            RDB_handle_err(ecp, RDB_table_is_persistent(tbp) ? txp : NULL);
            rcount = RDB_ERROR;
        }
    }
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    RDB_free(valv);

    ret = RDB_destroy_obj(&tpl, ecp);
    if (ret != RDB_OK) {
        RDB_handle_err(ecp, RDB_table_is_persistent(tbp) ? txp : NULL);
        rcount = RDB_ERROR;
    }
    return rcount;
}

static RDB_int
update_where_uindex(RDB_expression *texp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    int ret;
    int i;
    struct RDB_tuple_and_getfn tg;
    RDB_object tpl;
    RDB_bool b;
    int objc;
    RDB_field *fvv;
    RDB_object *valv;
    RDB_field *fieldv;
    RDB_expression *refexp;

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }
    objc = refexp->def.tbref.indexp->attrc;

    if (refexp->def.tbref.tbp->val.tbp->stp == NULL) {
        /*
         * The stored table may have been created by another process,
         * so try to open it
         */
        if (RDB_provide_stored_table(refexp->def.tbref.tbp,
                RDB_FALSE, ecp, txp) != RDB_OK) {
            return (RDB_int) RDB_ERROR;
        }

        if (refexp->def.tbref.tbp->val.tbp->stp == NULL) {
            return 0;
        }
    }
    fvv = RDB_alloc(sizeof(RDB_field) * objc, ecp);
    valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    fieldv = RDB_alloc(sizeof(RDB_field) * updc, ecp);

    if (fvv == NULL || valv == NULL || fieldv == NULL) {
        RDB_free(fvv);
        RDB_free(valv);
        RDB_free(fieldv);
        rcount = (RDB_int) RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    /* Convert to a field value */
    for (i = 0; i < objc; i++) {
        if (RDB_obj_to_field(&fvv[i], texp->def.op.optinfo.objpv[i], ecp)
                != RDB_OK) {
            rcount = (RDB_int) RDB_ERROR;
            goto cleanup;
        }
    }

    /* Read tuple */
    RDB_init_obj(&tpl);
    ret = RDB_get_by_uindex(refexp->def.tbref.tbp,
            texp->def.op.optinfo.objpv, refexp->def.tbref.indexp,
            refexp->def.tbref.tbp->typ->def.basetyp, ecp, txp, &tpl);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            rcount = 0;
        } else {
            rcount = (RDB_int) RDB_ERROR;
        }
        goto cleanup;
    }

    if (condp != NULL) {
        /*
         * Check condition
         */
        tg.tplp = &tpl;
        tg.getfn = getfn;
        tg.getarg = getarg;

        if (RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL,
                ecp, txp, &b) != RDB_OK) {
            rcount = (RDB_int) RDB_ERROR;
            goto cleanup;
        }

        if (!b) {
            rcount = 0;
            goto cleanup;
        }
    }

    ret = upd_to_vals(updc, updv, &tpl, valv, getfn, getarg, ecp, txp);
    if (ret != RDB_OK) {
        rcount = (RDB_int) RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < updc; i++) {
        fieldv[i].no = *RDB_field_no(
                 refexp->def.tbref.tbp->val.tbp->stp, updv[i].name);
         
        /* Set type - needed for tuple and array attributes */
        valv[i].store_typ = RDB_type_attr_type(
                    RDB_obj_type(refexp->def.tbref.tbp), updv[i].name);
        if (RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
            rcount = (RDB_int) RDB_ERROR;
            goto cleanup;
        }
    }

    RDB_cmp_ecp = ecp;
    if (refexp->def.tbref.indexp->idxp == NULL) {
        ret = RDB_update_rec(refexp->def.tbref.tbp->val.tbp->stp->recmapp,
                fvv, updc, fieldv,
                RDB_table_is_persistent(refexp->def.tbref.tbp) ?
                        txp->tx : NULL,
                        ecp);
    } else {
        /* Set primary key fields update */
        int keyfieldc = RDB_pkey_len(refexp->def.tbref.tbp);
        RDB_type *tptyp = RDB_base_type(RDB_obj_type(refexp->def.tbref.tbp));
        RDB_field *keyv = RDB_alloc(sizeof(RDB_field) * keyfieldc, ecp);
        if (keyv == NULL) {
            rcount = (RDB_int) RDB_ERROR;
            goto cleanup;
        }

        for (i = 0; i < tptyp->def.tuple.attrc; i++) {
            RDB_object *valp;
            RDB_int fno = *RDB_field_no(refexp->def.tbref.tbp->val.tbp->stp,
                    tptyp->def.tuple.attrv[i].name);
            if (fno < keyfieldc) {
                valp = RDB_tuple_get(&tpl, tptyp->def.tuple.attrv[i].name);
                valp->store_typ = tptyp->def.tuple.attrv[i].typ;
                ret = RDB_obj_to_field(&keyv[fno], valp, ecp);
                if (ret != RDB_OK) {
                    RDB_free(keyv);
                    rcount = (RDB_int) RDB_ERROR;
                    goto cleanup;
                }
            }
        }

        ret = RDB_update_rec(refexp->def.tbref.tbp->val.tbp->stp->recmapp,
                keyv, updc, fieldv,
                RDB_table_is_persistent(refexp->def.tbref.tbp) ?
                        txp->tx : NULL,
                        ecp);
        RDB_free(keyv);
    }
    if (ret != RDB_OK) {
        RDB_handle_err(ecp, txp);
        rcount = (RDB_int) RDB_ERROR;
    } else {
       rcount = 1;
    }

cleanup:
    for (i = 0; i < updc; i++)
        RDB_destroy_obj(&valv[i], ecp);
    RDB_destroy_obj(&tpl, ecp);
    RDB_free(valv);
    RDB_free(fieldv);
    RDB_free(fvv);

    return rcount;
}

static RDB_int
update_where_index_simple(RDB_expression *texp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    int ret;
    int i;
    int flags;
    RDB_expression *refexp;
    int objc;
    RDB_field *fv;
    RDB_object *valv;
    RDB_field *fieldv;
    RDB_cursor *curp = NULL;

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }
    objc = refexp->def.tbref.indexp->attrc;

    fv = RDB_alloc(sizeof(RDB_field) * objc, ecp);
    valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    fieldv = RDB_alloc(sizeof(RDB_field) * updc, ecp);

    if (fv == NULL || valv == NULL || fieldv == NULL) {
        RDB_free(fv);
        RDB_free(valv);
        RDB_free(fieldv);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    curp = RDB_index_cursor(refexp->def.tbref.indexp->idxp, RDB_TRUE,
            RDB_table_is_persistent(refexp->def.tbref.tbp) ? txp->tx : NULL, ecp);
    if (curp == NULL) {
        RDB_handle_err(ecp, txp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (texp->def.op.optinfo.objc > 0) {
        /* Convert to a field value */
        for (i = 0; i < texp->def.op.optinfo.objc; i++) {
            if (RDB_obj_to_field(&fv[i], texp->def.op.optinfo.objpv[i],
                    ecp) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (texp->def.op.optinfo.objc != refexp->def.tbref.indexp->attrc
                || !texp->def.op.optinfo.all_eq)
            flags = RDB_REC_RANGE;
        else
            flags = 0;

        ret = RDB_cursor_seek(curp, texp->def.op.optinfo.objc, fv, flags, ecp);
        if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            rcount = 0;
            goto cleanup;
        }
    } else {
        ret = RDB_cursor_first(curp, ecp);
        if (ret != RDB_OK) {
            RDB_handle_err(ecp, txp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    rcount = 0;
    RDB_cmp_ecp = ecp;
    RDB_init_obj(&tpl);
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;
        struct RDB_tuple_and_getfn tg;

        /* Read tuple */
        ret = RDB_get_by_cursor(refexp->def.tbref.tbp, curp,
                refexp->def.tbref.tbp->typ->def.basetyp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }

        tg.tplp = &tpl;
        tg.getfn = getfn;
        tg.getarg = getarg;

        if (texp->def.op.optinfo.stopexp != NULL) {
            ret = RDB_evaluate_bool(texp->def.op.optinfo.stopexp,
                    &RDB_get_from_tuple_or_fn, &tg, NULL, ecp, txp, &b);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (!b) {
                ret = RDB_ERROR;
                RDB_raise_not_found("", ecp);
                break;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL, ecp, txp, &upd)
                    != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(texp->def.op.args.firstp->nextp, &RDB_get_from_tuple_or_fn, &tg,
                NULL, ecp, txp, &b) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
        upd = (RDB_bool) (upd && b);

        if (upd) {
            ret = upd_to_vals(updc, updv, &tpl, valv, getfn, getarg, ecp, txp);
            if (ret != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }

            for (i = 0; i < updc; i++) {
                fieldv[i].no = *RDB_field_no(
                        refexp->def.tbref.tbp->val.tbp->stp, updv[i].name);
                 
                /* Set type - needed for tuple and array attributes */
                valv[i].store_typ = RDB_type_attr_type(
                            RDB_obj_type(refexp->def.tbref.tbp), updv[i].name);
                if (RDB_obj_to_field(&fieldv[i], &valv[i], ecp) != RDB_OK) {
                    rcount = RDB_ERROR;
                    goto cleanup;
                }
            }

            RDB_cmp_ecp = ecp;
            ret = RDB_cursor_set(curp, updc, fieldv, ecp); /* update */
            if (ret != RDB_OK) {
                RDB_handle_err(ecp, txp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        if (texp->def.op.optinfo.objc == refexp->def.tbref.indexp->attrc
                && texp->def.op.optinfo.all_eq) {
            flags = RDB_REC_DUP;
        } else {
            flags = 0;
        }

        ret = RDB_cursor_next(curp, flags, ecp);
    } while (ret == RDB_OK);

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_handle_err(ecp, txp);
        rcount = RDB_ERROR;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);

    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp, ecp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                RDB_handle_err(ecp, txp);
                rcount = RDB_ERROR;
            }
        }
    }

    for (i = 0; i < updc; i++) {
        ret = RDB_destroy_obj(&valv[i], ecp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                RDB_handle_err(ecp, txp);
                rcount = RDB_ERROR;
            }
        }
    }
    RDB_free(valv);
    RDB_free(fieldv);
    RDB_free(fv);

    return rcount;
}

static RDB_int
update_where_index_complex(RDB_expression *texp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_object tpl;
    struct RDB_tuple_and_getfn tg;
    int ret;
    int i;
    int flags;
    RDB_expression *refexp;
    int objc;
    RDB_object tmptb;
    RDB_type *tmptbtyp;
    RDB_cursor *curp = NULL;
    RDB_field *fv;
    RDB_object *valv = RDB_alloc(sizeof(RDB_object) * updc, ecp);
    RDB_field *fieldv = RDB_alloc(sizeof(RDB_field) * updc, ecp);

    if (valv == NULL || fieldv == NULL) {
        RDB_free(valv);
        RDB_free(fieldv);
        return RDB_ERROR;
    }

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }
    objc = refexp->def.tbref.indexp->attrc;

    fv = RDB_alloc(sizeof(RDB_field) * objc, ecp);
    if (fv == NULL) {
        RDB_free(valv);
        RDB_free(fieldv);
        return RDB_ERROR;
    }

    for (i = 0; i < updc; i++)
        RDB_init_obj(&valv[i]);

    RDB_init_obj(&tmptb);

    /*
     * Iterate over the records and insert the updated records into
     * a temporary table. For the temporary table, only one key is needed.
     */

    tmptbtyp = RDB_dup_nonscalar_type(refexp->def.tbref.tbp->typ, ecp);
    if (tmptbtyp == NULL) {
        rcount = RDB_ERROR;
        goto cleanup;
    }
    ret = RDB_init_table_from_type(&tmptb, NULL, tmptbtyp, 1,
            refexp->def.tbref.tbp->val.tbp->keyv, 0, NULL, ecp);
    if (ret != RDB_OK) {
        RDB_del_nonscalar_type(tmptbtyp, ecp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    curp = RDB_index_cursor(refexp->def.tbref.indexp->idxp, RDB_TRUE,
            RDB_table_is_persistent(refexp->def.tbref.tbp) ? txp->tx : NULL, ecp);
    if (ret != RDB_OK) {
        RDB_handle_err(ecp, txp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    if (texp->def.op.optinfo.objc > 0) {
        /* Convert to a field value */
        for (i = 0; i < texp->def.op.optinfo.objc; i++) {
            if (RDB_obj_to_field(&fv[i], texp->def.op.optinfo.objpv[i], ecp)
                    != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (texp->def.op.optinfo.objc != refexp->def.tbref.indexp->attrc
                || !texp->def.op.optinfo.all_eq)
            flags = RDB_REC_RANGE;
        else
            flags = 0;

        /* Set cursor position */
        ret = RDB_cursor_seek(curp, texp->def.op.optinfo.objc, fv, flags, ecp);
        if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            rcount = RDB_OK;
            goto cleanup;
        }
        if (ret != RDB_OK) {
            RDB_handle_err(ecp, txp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
    } else {
        ret = RDB_cursor_first(curp, ecp);
        if (ret != RDB_OK) {
            RDB_handle_err(ecp, txp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    rcount = 0;
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        RDB_init_obj(&tpl);
        if (RDB_get_by_cursor(refexp->def.tbref.tbp, curp,
                refexp->def.tbref.tbp->typ->def.basetyp, &tpl,
                ecp, txp) != RDB_OK) {
            rcount = RDB_ERROR;
            RDB_destroy_obj(&tpl, ecp);
            goto cleanup;
        }

        tg.tplp = &tpl;
        tg.getfn = getfn;
        tg.getarg = getarg;

        if (texp->def.op.optinfo.stopexp != NULL) {
            if (RDB_evaluate_bool(texp->def.op.optinfo.stopexp,
                    &RDB_get_from_tuple_or_fn, &tg, NULL, ecp, txp, &b) != RDB_OK) {
                rcount = RDB_ERROR;
                RDB_destroy_obj(&tpl, ecp);
                goto cleanup;
            }
            if (!b) {
                ret = RDB_ERROR;
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_not_found("", ecp);
                break;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL,
                    ecp, txp, &upd) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(texp->def.op.args.firstp->nextp,
                &RDB_get_from_tuple_or_fn, &tg, NULL, ecp, txp, &b) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        upd = (RDB_bool) (upd && b);

        if (upd) {
            if (upd_to_vals(updc, updv, &tpl, valv, getfn, getarg, ecp, txp) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            for (i = 0; i < updc; i++) {
                /* Update tuple */
                if (RDB_tuple_set(&tpl, updv[i].name, &valv[i], ecp) != RDB_OK) {
                    RDB_destroy_obj(&tpl, ecp);
                    rcount = RDB_ERROR;
                    goto cleanup;
                }
            }
            
            /* Insert tuple into temporary table */
            if (RDB_insert(&tmptb, &tpl, ecp, txp) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }
        RDB_destroy_obj(&tpl, ecp);
        if (texp->def.op.optinfo.objc == refexp->def.tbref.indexp->attrc
                && texp->def.op.optinfo.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;

        ret = RDB_cursor_next(curp, flags, ecp);
    } while (ret == RDB_OK);

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_handle_err(ecp, txp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Delete the updated records from the original table.
     */

    /* Reset cursor */
    if (texp->def.op.optinfo.objc > 0) {
        ret = RDB_cursor_seek(curp, texp->def.op.optinfo.objc, fv, flags, ecp);
    } else {
        ret = RDB_cursor_first(curp, ecp);
    }
    if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        ret = RDB_OK;
        goto cleanup;
    }
    if (ret != RDB_OK) {
        RDB_handle_err(ecp, txp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    RDB_cmp_ecp = ecp;
    do {
        RDB_bool upd = RDB_TRUE;
        RDB_bool b;

        /* Read tuple */
        RDB_init_obj(&tpl);
        ret = RDB_get_by_cursor(refexp->def.tbref.tbp, curp,
                RDB_obj_type(refexp->def.tbref.tbp)->def.basetyp, &tpl,
                ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }

        tg.tplp = &tpl;
        tg.getfn = getfn;
        tg.getarg = getarg;

        if (texp->def.op.optinfo.stopexp != NULL) {
            if (RDB_evaluate_bool(texp->def.op.optinfo.stopexp,
                    &RDB_get_from_tuple_or_fn, &tg, NULL, ecp, txp, &b) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_not_found("", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
            if (!b) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_not_found("", ecp);
                ret = RDB_ERROR;
                break;
            }
        }

        if (condp != NULL) {
            /*
             * Check condition
             */
            if (RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL, ecp, txp, &upd)
                    != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (RDB_evaluate_bool(texp->def.op.args.firstp->nextp,
                &RDB_get_from_tuple_or_fn, &tg, NULL, ecp, txp, &b) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }
        RDB_destroy_obj(&tpl, ecp);
        upd = (RDB_bool) (upd && b);

        if (upd) {
            ret = RDB_cursor_delete(curp, ecp);
            if (ret != RDB_OK) {
                RDB_handle_err(ecp, txp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }
        if (texp->def.op.optinfo.objc == refexp->def.tbref.indexp->attrc
                && texp->def.op.optinfo.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;

        ret = RDB_cursor_next(curp, flags, ecp);
    } while (ret == RDB_OK);

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_handle_err(ecp, txp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    /*
     * Insert the records from the temporary table into the original table.
     */

    if (RDB_move_tuples(refexp->def.tbref.tbp, &tmptb, RDB_DISTINCT, ecp, txp)
             == (RDB_int) RDB_ERROR) {
        rcount = RDB_ERROR;
    }

cleanup:
    RDB_destroy_obj(&tmptb, ecp);

    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp, ecp);
        if (ret != RDB_OK) {
            if (rcount != RDB_ERROR) {
                RDB_handle_err(ecp, txp);
                rcount = RDB_ERROR;
            }
        }
    }

    for (i = 0; i < updc; i++) {
        RDB_destroy_obj(&valv[i], ecp);
    }
    RDB_free(valv);
    RDB_free(fieldv);
    RDB_free(fv);

    return rcount;
}

static RDB_bool
update_refers(RDB_object *tbp, int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp)
{
    /* Check if one of the expressions refers to the table itself */
    int i;
    for (i = 0; i < updc; i++) {
        if (RDB_expr_refers(updv[i].exp, tbp))
            return RDB_TRUE;
    }

    return RDB_FALSE;
}

#ifdef POSTGRESQL
static RDB_bool
is_key_to_const(RDB_object *tbp, int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp)
{
    int i;

    /* Check if a key attribute is updated */
    for (i = 0; i < updc; i++) {
        if (is_keyattr(updv[i].name, tbp, ecp)
                && RDB_expr_is_const(updv[i].exp)) {
            return RDB_TRUE;
        }
    }
    return RDB_FALSE;
}
#endif

static RDB_bool
upd_complex(RDB_object *tbp, int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp)
{
    int i;

    /* Check if a key attribute is updated */
    for (i = 0; i < updc; i++) {
        if (is_keyattr(updv[i].name, tbp, ecp)) {
            return RDB_TRUE;
        }
    }

    return update_refers(tbp, updc, updv, ecp);
}

RDB_attr_update *upd_replace_varnames(int updc, const RDB_attr_update updv[],
        RDB_getobjfn *getfn, void *getarg, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    RDB_attr_update *newupdv = RDB_alloc(sizeof(RDB_attr_update) * updc, ecp);
    if (newupdv == NULL)
        return NULL;

    for (i = 0; i < updc; i++) {
        newupdv[i].exp = NULL;
    }

    for (i = 0; i < updc; i++) {
        newupdv[i].name = updv[i].name;
        newupdv[i].exp = RDB_expr_resolve_varnames(updv[i].exp, getfn, getarg, ecp, txp);
        if (newupdv[i].exp == NULL)
            goto error;
    }
    return newupdv;

error:
    for (i = 0; i < updc; i++) {
        if (newupdv[i].exp != NULL) {
            RDB_del_expr(newupdv[i].exp, ecp);
        }
    }
    RDB_free(newupdv);
    return NULL;
}

#ifdef POSTGRESQL
static void
free_updv(int updc, RDB_attr_update updv[], RDB_exec_context *ecp) {
    int i;
    for (i = 0; i < updc; i++) {
        RDB_del_expr(updv[i].exp, ecp);
    }
    RDB_free(updv);
}

static RDB_int
sql_update(RDB_object *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object command;
    RDB_object sqlexp;
    RDB_int ret;
    int i;

    RDB_init_obj(&command);
    RDB_init_obj(&sqlexp);
    if (RDB_string_to_obj(&command, "UPDATE \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, RDB_table_name(tbp), ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, "\" SET ", ecp) != RDB_OK)
        goto error;
    for (i = 0; i < updc; i++) {
        if (RDB_append_char(&command, '"', ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, updv[i].name, ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, "\" = ", ecp) != RDB_OK)
            goto error;
        if (RDB_expr_to_sql(&sqlexp, updv[i].exp, RDB_db_env(RDB_tx_db(txp)), ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, RDB_obj_string(&sqlexp), ecp) != RDB_OK)
            goto error;
        if (i < updc - 1) {
            if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                goto error;
        }
    }

    if (condp != NULL) {
        if (RDB_append_string(&command, " WHERE ", ecp) != RDB_OK)
            goto error;
        if (RDB_expr_to_sql(&sqlexp, condp, RDB_db_env(RDB_tx_db(txp)), ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, RDB_obj_string(&sqlexp), ecp) != RDB_OK)
            goto error;
    }
    ret = RDB_update_pg_sql(RDB_db_env(RDB_tx_db(txp)), RDB_obj_string(&command),
            txp->tx, ecp);
    RDB_destroy_obj(&sqlexp, ecp);
    RDB_destroy_obj(&command, ecp);
    return ret;

error:
    RDB_destroy_obj(&sqlexp, ecp);
    RDB_destroy_obj(&command, ecp);
    return (RDB_int) RDB_ERROR;
}

static RDB_bool
updv_sql_convertible(int updc, const RDB_attr_update updv[],
        RDB_gettypefn *getfnp, void *getarg)
{
    int i;
    for (i = 0; i < updc; i++) {
        if (!RDB_nontable_sql_convertible(updv[i].exp, getfnp, getarg))
            return RDB_FALSE;
    }
    return RDB_TRUE;
}
#endif

RDB_int
RDB_update_nonvirtual(RDB_object *tbp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (updc == 0)
        return (RDB_int) 0;

    if (tbp->val.tbp->stp == NULL) {
        if (RDB_provide_stored_table(tbp, RDB_FALSE, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }

        if (tbp->val.tbp->stp == NULL)
            return RDB_OK;
    }

#ifdef POSTGRESQL
    if (RDB_table_is_persistent(tbp) && txp != NULL && RDB_env_queries(txp->envp)
            && !update_refers(tbp, updc, updv, ecp)
            && !is_key_to_const(tbp, updc, updv, ecp)) {
        RDB_int cnt;
        RDB_expression *repcondp = NULL;
        RDB_attr_update *repupdv = upd_replace_varnames(updc, updv,
                getfn, getarg, ecp, txp);
        if (repupdv == NULL)
            return RDB_ERROR;

        if (condp != NULL) {
            repcondp = RDB_expr_resolve_varnames(condp, getfn, getarg, ecp, txp);
            if (repcondp == NULL) {
                free_updv(updc, repupdv, ecp);
                return (RDB_int) RDB_ERROR;
            }
        }
        if ((condp == NULL || RDB_nontable_sql_convertible(repcondp,
                &RDB_get_tuple_attr_type, tbp->typ->def.basetyp))
                && updv_sql_convertible(updc, repupdv,
                        &RDB_get_tuple_attr_type, tbp->typ->def.basetyp)) {
            cnt = sql_update(tbp, repcondp, updc, repupdv, ecp, txp);
            if (repcondp != NULL)
                RDB_del_expr(repcondp, ecp);
            free_updv(updc, repupdv, ecp);
            return cnt;
        }
        if (repcondp != NULL)
            RDB_del_expr(repcondp, ecp);
        free_updv(updc, repupdv, ecp);
    }
#endif

    if (upd_complex(tbp, updc, updv, ecp)
            || (condp != NULL && RDB_expr_refers(condp, tbp))) {
        /*
         * If e.g. a key is updated, the simple update method cannot be used
         */
        return update_stored_complex(tbp, condp, updc, updv, getfn, getarg,
                ecp, txp);
    }
    return update_stored_simple(tbp, condp, updc, updv, getfn, getarg,
            ecp, txp);
}

static RDB_bool
upd_refers_index(int updc, const RDB_attr_update updv[], const RDB_tbindex *idxp)
{
    int i, j;
    for (i = 0; i < updc; i++) {
        for (j = 0; j < idxp->attrc; j++) {
            if (strcmp(updv[i].name, idxp->attrv[j].attrname) == 0) {
                return RDB_TRUE;
            }
        }
    }
    return RDB_FALSE;
}

RDB_int
RDB_update_where_index(RDB_expression *texp, RDB_expression *condp,
        int updc, const RDB_attr_update updv[],
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *refexp;

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }
    
    if (refexp->def.tbref.tbp->val.tbp->stp == NULL) {
        if (RDB_provide_stored_table(refexp->def.tbref.tbp,
                RDB_FALSE, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }

        if (refexp->def.tbref.tbp->val.tbp->stp == NULL) {
            return 0;
        }
    }

    if (refexp->def.tbref.indexp->unique) {
        return update_where_uindex(texp, condp, updc, updv, getfn, getarg,
                ecp, txp);
    }
    if (txp == NULL) {
        RDB_raise_internal("no transaction", ecp);
        return RDB_ERROR;
    }

    if (upd_complex(refexp->def.tbref.tbp, updc, updv, ecp)
        || RDB_expr_refers(texp->def.op.args.firstp->nextp, refexp->def.tbref.tbp)
        || (condp != NULL && RDB_expr_refers(condp, refexp->def.tbref.tbp))
        || upd_refers_index(updc, updv, refexp->def.tbref.indexp)) {
        return update_where_index_complex(texp, condp, updc, updv,
                getfn, getarg, ecp, txp);
    }
    return update_where_index_simple(texp, condp, updc, updv, getfn, getarg,
            ecp, txp);
}
