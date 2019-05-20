/*
 * Copyright (C) 2004-2007, 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "delete.h"
#include "typeimpl.h"
#include "internal.h"
#include "stable.h"
#include "qresult.h"
#include "sqlgen.h"
#include <obj/objinternal.h>
#include <gen/strfns.h>

#ifdef POSTGRESQL
#include <pgrec/pgenv.h>
#endif

#include <string.h>
#include <stdio.h>

static RDB_int
delete_by_uindex(RDB_object *tbp, RDB_object *objpv[], RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    RDB_field *fv;
    int i;
    int ret;
    int keylen = indexp->attrc;

    fv = RDB_alloc(sizeof (RDB_field) * keylen, ecp);
    if (fv == NULL) {
        RDB_raise_no_memory(ecp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < keylen; i++) {
        ret = RDB_obj_to_field(&fv[i], objpv[i], ecp);
        if (ret != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    RDB_cmp_ecp = ecp;
    if (indexp->idxp == NULL) {
        ret = RDB_delete_rec(tbp->val.tbp->stp->recmapp, keylen, fv,
                RDB_table_is_persistent(tbp) ? txp->tx : NULL, ecp);
    } else {
        ret = RDB_index_delete_rec(indexp->idxp, fv,
                RDB_table_is_persistent(tbp) ? txp->tx : NULL, ecp);
    }
    if (ret == RDB_OK) {
        rcount = 1;
    } else {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            rcount = 0;
        } else {
            RDB_handle_err(ecp, txp);
            rcount = RDB_ERROR;
        }
    }

cleanup:
    RDB_free(fv);
    return rcount;
}

#ifdef POSTGRESQL
static RDB_int
sql_delete(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object command;
    RDB_object where;
    RDB_int ret;
    RDB_environment *envp = RDB_db_env(RDB_tx_db(txp));

    RDB_init_obj(&command);
    RDB_init_obj(&where);
    if (RDB_string_to_obj(&command, "DELETE FROM \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, RDB_table_name(tbp), ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(&command, '"', ecp) != RDB_OK)
        goto error;
    if (condp != NULL) {
        if (RDB_append_string(&command, " WHERE ", ecp) != RDB_OK)
            goto error;
        if (RDB_expr_to_sql(&where, condp, envp, ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, RDB_obj_string(&where), ecp) != RDB_OK)
            goto error;
    }
    ret = RDB_update_pg_sql(envp, RDB_obj_string(&command),
            txp->tx, ecp);
    RDB_destroy_obj(&where, ecp);
    RDB_destroy_obj(&command, ecp);
    return ret;

error:
    RDB_destroy_obj(&where, ecp);
    RDB_destroy_obj(&command, ecp);
    return (RDB_int) RDB_ERROR;
}
#endif

RDB_int
RDB_delete_nonvirtual(RDB_object *tbp, RDB_expression *condp,
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    int ret;
    int i;
    RDB_cursor *curp;
    RDB_object tpl;
    void *datap;
    size_t len;
    RDB_bool b;
    RDB_type *tpltyp = tbp->typ->def.basetyp;

    if (tbp->val.tbp->stp == NULL) {
        /*
         * The stored table may have been created by another process,
         * so try to open it
         */
        if (RDB_provide_stored_table(tbp, RDB_FALSE, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }

        if (tbp->val.tbp->stp == NULL) {
            /* Physical table representation still not there, so table is empty */
            return 0;
        }
    }

#ifdef POSTGRESQL
    if (RDB_table_is_persistent(tbp) && txp != NULL && RDB_env_queries(txp->envp)) {
        RDB_int cnt;
        RDB_expression *repcondp = NULL;

        if (condp != NULL) {
            repcondp = RDB_expr_resolve_varnames(condp, getfn, getarg, ecp, txp);
            if (repcondp == NULL)
                return (RDB_int) RDB_ERROR;
        }
        if (condp == NULL || RDB_nontable_sql_convertible(repcondp,
                &RDB_get_tuple_attr_type, tpltyp)) {
            cnt = sql_delete(tbp, repcondp, ecp, txp);
            if (repcondp != NULL)
                RDB_del_expr(repcondp, ecp);
            return cnt;
        }
        if (repcondp != NULL)
            RDB_del_expr(repcondp, ecp);
    }
#endif

    curp = RDB_recmap_cursor(tbp->val.tbp->stp->recmapp, RDB_TRUE,
            RDB_table_is_persistent(tbp) ? txp->tx : NULL, ecp);
    if (curp == NULL) {
        RDB_handle_err(ecp, txp);
        return RDB_ERROR;
    }
    ret = RDB_cursor_first(curp, ecp);
    
    RDB_cmp_ecp = ecp;
    rcount = 0;
    while (ret == RDB_OK) {
        RDB_init_obj(&tpl);
        if (condp != NULL) {
            struct RDB_tuple_and_getfn tg;

            for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
                RDB_object val;

                ret = RDB_cursor_get(curp,
                        *RDB_field_no(tbp->val.tbp->stp, tpltyp->def.tuple.attrv[i].name),
                        &datap, &len, ecp);
                if (ret != RDB_OK) {
                   RDB_destroy_obj(&tpl, ecp);
                   RDB_handle_err(ecp, txp);
                   goto error;
                }
                RDB_init_obj(&val);
                ret = RDB_irep_to_obj(&val, tpltyp->def.tuple.attrv[i].typ,
                                 datap, len, ecp);
                if (ret != RDB_OK) {
                   RDB_destroy_obj(&val, ecp);
                   RDB_destroy_obj(&tpl, ecp);
                   goto error;
                }
                ret = RDB_tuple_set(&tpl, tpltyp->def.tuple.attrv[i].name,
                        &val, ecp);
                RDB_destroy_obj(&val, ecp);
                if (ret != RDB_OK) {
                   RDB_destroy_obj(&tpl, ecp);
                   goto error;
                }
            }

            tg.tplp = &tpl;
            tg.getfn = getfn;
            tg.getarg = getarg;

            if (RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL,
                    ecp, txp, &b) != RDB_OK)
                 goto error;
        } else {
            b = RDB_TRUE;
        }
        if (b) {
            ret = RDB_cursor_delete(curp, ecp);
            if (ret != RDB_OK) {
                RDB_handle_err(ecp, txp);
                RDB_destroy_obj(&tpl, ecp);
                goto error;
            }
            rcount++;
        }
        RDB_destroy_obj(&tpl, ecp);
        ret = RDB_cursor_next(curp, 0, ecp);
    };
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_handle_err(ecp, txp);
        goto error;
    }

    if (RDB_destroy_cursor(curp, ecp) != RDB_OK) {
        RDB_handle_err(ecp, txp);
        curp = NULL;
        goto error;
    }
    return rcount;

error:
    if (curp != NULL)
        RDB_destroy_cursor(curp, ecp);
    return RDB_ERROR;
}

static RDB_int
delete_where_uindex(RDB_expression *texp, RDB_expression *condp,
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    int ret;
    RDB_expression *refexp;

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }

    if (condp != NULL) {
        RDB_object tpl;
        RDB_bool b;
        struct RDB_tuple_and_getfn tg;

        RDB_init_obj(&tpl);

        /*
         * Read tuple and check condition
         */
        if (RDB_get_by_uindex(refexp->def.tbref.tbp,
                texp->def.op.optinfo.objpv, refexp->def.tbref.indexp,
                refexp->def.tbref.tbp->typ->def.basetyp, ecp, txp, &tpl)
                    != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            rcount = RDB_ERROR;
            goto cleanup;
        }

        tg.tplp = &tpl;
        tg.getfn = getfn;
        tg.getarg = getarg;

        ret = RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg,
                NULL, ecp, txp, &b);
        RDB_destroy_obj(&tpl, ecp);
        if (ret != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }

        if (!b)
            return 0;
    }

    rcount = delete_by_uindex(refexp->def.tbref.tbp,
            texp->def.op.optinfo.objpv, refexp->def.tbref.indexp,
            ecp, txp);

cleanup:
    return rcount;
}

static RDB_int
delete_where_nuindex(RDB_expression *texp, RDB_expression *condp,
        RDB_getobjfn *getfn, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int rcount;
    int ret;
    int i;
    int flags;
    RDB_cursor *curp = NULL;
    RDB_field *fv = NULL;
    RDB_expression *refexp;
    RDB_tbindex *indexp;
    int keylen;

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
        refexp = texp->def.op.args.firstp;
    } else {
        /* child is projection */
        refexp = texp->def.op.args.firstp->def.op.args.firstp;
    }

    indexp = refexp->def.tbref.indexp;
    keylen = indexp->attrc;

    curp = RDB_index_cursor(indexp->idxp, RDB_TRUE,
            RDB_table_is_persistent(refexp->def.tbref.tbp) ?
            txp->tx : NULL, ecp);
    if (curp == NULL) {
        RDB_handle_err(ecp, txp);
        return RDB_ERROR;
    }

    fv = RDB_alloc(sizeof (RDB_field) * keylen, ecp);
    if (fv == NULL) {
        RDB_raise_no_memory(ecp);
        rcount = RDB_ERROR;
        goto cleanup;
    }

    for (i = 0; i < keylen; i++) {
        if (RDB_obj_to_field(&fv[i], texp->def.op.optinfo.objpv[i], ecp)
                != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    if (texp->def.op.optinfo.objc != indexp->attrc
            || !texp->def.op.optinfo.all_eq)
        flags = RDB_REC_RANGE;
    else
        flags = 0;

    ret = RDB_cursor_seek(curp, texp->def.op.optinfo.objc, fv, flags, ecp);
    if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        rcount = 0;
        goto cleanup;
    }

    RDB_cmp_ecp = ecp;
    rcount = 0;
    do {
        RDB_bool del = RDB_TRUE;
        RDB_bool b;
        struct RDB_tuple_and_getfn tg;

        RDB_object tpl;

        RDB_init_obj(&tpl);

        /*
         * Read tuple and check condition
         */
        ret = RDB_get_by_cursor(refexp->def.tbref.tbp,
                curp, refexp->def.tbref.tbp->typ->def.basetyp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
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
                RDB_destroy_obj(&tpl, ecp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            if (!b) {
                RDB_destroy_obj(&tpl, ecp);
                goto cleanup;
            }
        }
        if (condp != NULL) {
            ret = RDB_evaluate_bool(condp, &RDB_get_from_tuple_or_fn, &tg, NULL,
                    ecp, txp, &del);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }
        ret = RDB_evaluate_bool(texp->def.op.args.firstp->nextp,
                &RDB_get_from_tuple_or_fn, &tg, NULL, ecp, txp, &b);
        RDB_destroy_obj(&tpl, ecp);
        if (ret != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
        del = (RDB_bool) (del && b);

        if (del) {
            ret = RDB_cursor_delete(curp, ecp);
            if (ret != RDB_OK) {
                RDB_handle_err(ecp, txp);
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount++;
        }

        if (texp->def.op.optinfo.objc == indexp->attrc
                && texp->def.op.optinfo.all_eq)
            flags = RDB_REC_DUP;
        else
            flags = 0;
        ret = RDB_cursor_next(curp, flags, ecp);
    } while (ret == RDB_OK);

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_handle_err(ecp, txp);
        rcount = RDB_ERROR;
    }

cleanup:
    if (curp != NULL) {
        ret = RDB_destroy_cursor(curp, ecp);
        if (ret != RDB_OK && rcount != RDB_ERROR) {
            RDB_handle_err(ecp, txp);
            rcount = RDB_ERROR;
        }
    }
    RDB_free(fv);
    return rcount;
}

RDB_int
RDB_delete_where_index(RDB_expression *texp, RDB_expression *condp,
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

    if (refexp->def.tbref.indexp->unique) {
        return delete_where_uindex(texp, condp, getfn, getarg, ecp, txp);
    }
    return delete_where_nuindex(texp, condp, getfn, getarg, ecp, txp);
}

static int
sql_delete_tuple(RDB_object *tbp, RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_type *tuptyp = tbp->typ->def.basetyp;
    int attrcount = tuptyp->def.tuple.attrc;
    RDB_field *fvp = RDB_alloc(sizeof(RDB_field) * attrcount, ecp);
    if (fvp == NULL) {
        return (RDB_int) RDB_ERROR;
    }

    for (i = 0; i < attrcount; i++) {
        int *fnop = RDB_field_no(tbp->val.tbp->stp, tuptyp->def.tuple.attrv[i].name);
        RDB_object *valp = RDB_tuple_get(tplp, tuptyp->def.tuple.attrv[i].name);
        RDB_type *attrtyp = tuptyp->def.tuple.attrv[i].typ;

        /* If there is no value, check if there is a default */
        if (valp == NULL) {
            RDB_raise_invalid_argument("missing value", ecp);
            goto error;
        }

        /* Typecheck */
        if (!RDB_obj_matches_type(valp, attrtyp)) {
            RDB_raise_type_mismatch(
                    "tuple attribute type does not match table attribute type",
                    ecp);
            goto error;
        }

        /* Set type information for storage */
        valp->store_typ = attrtyp;

        if (RDB_obj_to_field(&fvp[*fnop], valp, ecp) != RDB_OK) {
            goto error;
        }
    }

    ret = RDB_delete_rec(tbp->val.tbp->stp->recmapp, attrcount, fvp, txp->tx, ecp);
    RDB_free(fvp);
    return ret;

error:
    RDB_free(fvp);
    return RDB_ERROR;
}

RDB_int
RDB_delete_nonvirtual_tuple(RDB_object *tbp, RDB_object *tplp, int flags, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_tbindex *indexp;
    RDB_type *valtyp;
    RDB_type *attrtyp;
    RDB_bool contains;
    RDB_object **objpv;

    if (tbp->val.tbp->stp == NULL) {
        if (RDB_provide_stored_table(tbp, RDB_FALSE, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }

        if (tbp->val.tbp->stp == NULL) {
            /* No stored table, so table is empty */
            if ((RDB_INCLUDED & flags) != 0) {
                RDB_raise_not_found("tuple not found", ecp);
                return (RDB_int) RDB_ERROR;
            }
            return (RDB_int) 0;
        }
    }

    if (RDB_table_is_persistent(tbp) && txp != NULL && RDB_env_queries(txp->envp)) {
        ret = sql_delete_tuple(tbp, tplp, ecp, txp);
        if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            return 0;
        }
        return ret == RDB_ERROR ? (RDB_int) RDB_ERROR : 1;
    }

    /* Check if the table contains the tuple */
    if (RDB_table_contains(tbp, tplp, ecp, txp, &contains) != RDB_OK)
        return (RDB_int) RDB_ERROR;
    if (!contains) {
        if ((RDB_INCLUDED & flags) != 0) {
            RDB_raise_not_found("tuple not found", ecp);
            return (RDB_int) RDB_ERROR;
        }
        return (RDB_int) 0;
    }

    /* Get primary index */
    for (i = 0;
            i < tbp->val.tbp->stp->indexc && tbp->val.tbp->stp->indexv[i].idxp != NULL;
            i++);
    if (i == tbp->val.tbp->stp->indexc) {
        RDB_raise_internal("primary index not found", ecp);
        return (RDB_int) RDB_ERROR;
    }
    indexp = &tbp->val.tbp->stp->indexv[i];

    /* Get attribute values of the primary index */
    objpv = RDB_alloc(sizeof (RDB_object *) * indexp->attrc, ecp);
    if (objpv == NULL)
        return (RDB_int) RDB_ERROR;
    for (i = 0; i < indexp->attrc; i++) {
        objpv[i] = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
        if (objpv[i] == NULL) {
            RDB_raise_invalid_argument("missing attribute", ecp);
            goto error;
        }

        attrtyp = RDB_type_attr_type(RDB_obj_type(tbp),
                indexp->attrv[i].attrname);
        valtyp = RDB_obj_type(objpv[i]);

        /* Typecheck */
        if (valtyp == NULL) {
            if (objpv[i]->kind != RDB_OB_TUPLE && objpv[i]->kind != RDB_OB_ARRAY) {
                RDB_raise_invalid_argument("missing type information", ecp);
                goto error;
            }
        } else {
            if (!RDB_type_equals(valtyp, attrtyp)) {
                RDB_raise_type_mismatch(
                        "tuple attribute type does not match table attribute type",
                        ecp);
                goto error;
            }
        }

        objpv[i]->store_typ = attrtyp;
    }

    /* Delete using primary index */
    ret = delete_by_uindex(tbp, objpv, indexp, ecp, txp);
    if (ret == (RDB_int) RDB_ERROR)
        goto error;
    RDB_free(objpv);
    return ret;

error:
    RDB_free(objpv);
    return (RDB_int) RDB_ERROR;
}
