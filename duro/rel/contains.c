/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <string.h>

static RDB_expression *
attr_eq(RDB_attr *attrp, const RDB_object *tup)
{
    return RDB_eq(RDB_expr_attr(attrp->name),
                  RDB_obj_const(RDB_tuple_get(tup, attrp->name)));
}

static int
project_contains(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    RDB_bool result;
    int ret;
    
    if (tuptyp->var.tuple.attrc > 0) {
        RDB_expression *condp;
        RDB_table *seltbp;
        int i, ret;

        /* create where-condition */        
        condp = attr_eq(&tuptyp->var.tuple.attrv[0], tup);
        
        for (i = 1; i < tuptyp->var.tuple.attrc; i++) {
            condp = RDB_and(condp, attr_eq(&tuptyp->var.tuple.attrv[0],
                    tup));
        }
        if (condp == NULL)
            return RDB_NO_MEMORY;

        /* create selection table */
        ret = RDB_select(tbp, condp, &seltbp);
        if (ret != RDB_OK) {
            RDB_drop_expr(condp);
            return ret;
        }

        /* check if selection is empty */
        ret = RDB_table_is_empty(seltbp, txp, &result);
        _RDB_drop_table(seltbp, txp, RDB_FALSE);
        if (ret != RDB_OK)
            return ret;
        return result ? RDB_NOT_FOUND : RDB_OK;
    } else {
        /* projection with no attributes */
    
        ret = RDB_table_is_empty(tbp->var.project.tbp, txp, &result);
        if (ret != RDB_OK)
            return ret;
        return result ? RDB_NOT_FOUND : RDB_OK;
    }
}

int
_RDB_find_rename_to(int renc, RDB_renaming renv[], const char *name)
{
    int i;

    for (i = 0; i < renc && strcmp(renv[i].to, name) != 0; i++);
    if (i >= renc)
        return -1; /* not found */
    /* found */
    return i;
}

static int    
rename_contains(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    RDB_object tpl;
    int i;
    int ret;
    RDB_type *tuptyp = tbp->typ->var.basetyp;

    RDB_init_obj(&tpl);
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        /* has the attribute been renamed? */
        char *attrname = tuptyp->var.tuple.attrv[i].name;
        int ai = _RDB_find_rename_to(tbp->var.rename.renc, tbp->var.rename.renv,
                attrname);

        if (ai >= 0) { /* Yes, entry found */
            ret = RDB_tuple_set(&tpl, tbp->var.rename.renv[ai].from,
                        RDB_tuple_get(tup, tbp->var.rename.renv[ai].to));
        } else {
            ret = RDB_tuple_set(&tpl, attrname, RDB_tuple_get(tup, attrname));
        }
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret)) {
                RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                RDB_rollback_all(txp);
            }
            goto error;
        }
    }
    ret = RDB_table_contains(tbp->var.rename.tbp, &tpl, txp);

error:
    RDB_destroy_obj(&tpl);
    return ret;
}

static int    
wrap_contains(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl;
    char **attrv = malloc(sizeof(char *) * tbp->var.wrap.wrapc);

    if (attrv == NULL)
        return RDB_NO_MEMORY;

    /*
     * Create unwrapped tuple
     */

    for (i = 0; i < tbp->var.wrap.wrapc; i++)
        attrv[i] = tbp->var.wrap.wrapv[i].attrname;

    RDB_init_obj(&tpl);
    ret = RDB_unwrap_tuple(tup, tbp->var.wrap.wrapc, attrv, &tpl);
    free(attrv);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }

    /*
     * Check if unwrapped tuple is in base table
     */

    ret = RDB_table_contains(tbp->var.wrap.tbp, &tpl, txp);
    RDB_destroy_obj(&tpl);
    return ret;
}    

static int    
unwrap_contains(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    int ret;
    int i, j;
    RDB_object tpl;
    RDB_wrapping *wrapv = malloc(sizeof(RDB_wrapping) * tbp->var.unwrap.attrc);
    
    if (wrapv == NULL)
        return RDB_NO_MEMORY;

    /*
     * Create wrapped tuple
     */

    for (i = 0; i < tbp->var.unwrap.attrc; i++) {
        RDB_type *tuptyp = _RDB_tuple_type_attr(tbp->var.unwrap.tbp->typ->var.basetyp,
                tbp->var.unwrap.attrv[i])->typ;
        wrapv[i].attrc = tuptyp->var.tuple.attrc;
        wrapv[i].attrv = malloc(sizeof(char *) * tuptyp->var.tuple.attrc);
        if (wrapv[i].attrv == NULL)
            return RDB_NO_MEMORY;
        for (j = 0; j < wrapv[i].attrc; j++)
            wrapv[i].attrv[j] = tuptyp->var.tuple.attrv[j].name;

        wrapv[i].attrname = tbp->var.unwrap.attrv[i];
        
    }

    RDB_init_obj(&tpl);
    ret = RDB_wrap_tuple(tup, tbp->var.unwrap.attrc, wrapv, &tpl);
    if (ret != RDB_OK)
        goto cleanup;

    /*
     * Check if wrapped tuple is in base table
     */

    ret = RDB_table_contains(tbp->var.unwrap.tbp, &tpl, txp);

cleanup:
    for (i = 0; i < tbp->var.unwrap.attrc; i++)
        free(wrapv[i].attrv);
    free(wrapv);

    RDB_destroy_obj(&tpl);
    return ret;
}    

static int    
sdivide_contains(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    int ret;

    ret = RDB_table_contains(tbp->var.sdivide.tb1p, tplp, txp);
    if (ret != RDB_OK)
        return ret;

    return _RDB_sdivide_preserves(tbp, tplp, NULL, txp);
}

int
RDB_table_contains(RDB_table *tbp, const RDB_object *tup, RDB_transaction *txp)
{
    int ret;
    RDB_bool b;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;
    
    switch (tbp->kind) {
        case RDB_TB_STORED:
            {
                int i;
                RDB_type *tuptyp = tbp->typ->var.basetyp;
                int attrcount = tuptyp->var.tuple.attrc;
                RDB_field *fvp;

                fvp = malloc(sizeof(RDB_field) * attrcount);
                if (fvp == NULL)
                    return RDB_NO_MEMORY;
                for (i = 0; i < attrcount; i++) {
                    RDB_object *valp;
                    int fno = *(int*)RDB_hashmap_get(&tbp->var.stored.attrmap,
                            tuptyp->var.tuple.attrv[i].name, NULL);

                    valp = RDB_tuple_get(tup, tuptyp->var.tuple.attrv[i].name);
                    if (valp == NULL) {
                        free(fvp);
                        return RDB_INVALID_ARGUMENT;
                    }
                    if (valp->typ != NULL && !RDB_type_equals (RDB_obj_type(valp),
                            tuptyp->var.tuple.attrv[i].typ)) {
                        free(fvp);
                        return RDB_TYPE_MISMATCH;
                    }

                    /* Set type - needed for tuples */
                    if (valp->typ == NULL && valp->kind == RDB_OB_TUPLE)
                        _RDB_set_tuple_type(valp, tuptyp->var.tuple.attrv[i].typ);
                    _RDB_obj_to_field(&fvp[fno], valp);
                }
                ret = RDB_contains_rec(tbp->var.stored.recmapp, fvp, txp->txid);
                free(fvp);
                if (RDB_is_syserr(ret)) {
                    RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                    RDB_rollback_all(txp);
                }
                return ret;
            }
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            ret = RDB_evaluate_bool(tbp->var.select.exprp, tup, txp, &b);
            if (ret != RDB_OK)
                return ret;
            if (!b)
                return RDB_NOT_FOUND;
            return RDB_table_contains(tbp->var.select.tbp, tup, txp);
        case RDB_TB_UNION:
            ret = RDB_table_contains(tbp->var._union.tbp1, tup, txp);
            if (ret == RDB_OK)
                return RDB_OK;
            return RDB_table_contains(tbp->var._union.tbp2, tup, txp);            
        case RDB_TB_MINUS:
            ret = RDB_table_contains(tbp->var.minus.tbp1, tup, txp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_table_contains(tbp->var.minus.tbp2, tup, txp);
            if (ret == RDB_OK)
                return RDB_NOT_FOUND;
            if (ret == RDB_NOT_FOUND)
                return RDB_OK;
            return ret;
        case RDB_TB_INTERSECT:
            ret = RDB_table_contains(tbp->var.intersect.tbp1, tup, txp);
            if (ret != RDB_OK)
                return ret;
            return RDB_table_contains(tbp->var.intersect.tbp2, tup, txp);
        case RDB_TB_JOIN:
            ret = RDB_table_contains(tbp->var.join.tbp1, tup, txp);
            if (ret != RDB_OK)
                return ret;
            return RDB_table_contains(tbp->var.join.tbp2, tup, txp);
        case RDB_TB_EXTEND:
            return RDB_table_contains(tbp->var.extend.tbp, tup, txp);
        case RDB_TB_PROJECT:
            return project_contains(tbp, tup, txp);
        case RDB_TB_SUMMARIZE:
            {
                int ret2;
            
                RDB_qresult *qrp;
                ret = _RDB_table_qresult(tbp, txp, &qrp);
                if (ret != RDB_OK)
                    return ret;
                ret = _RDB_qresult_contains(qrp, tup, txp);
                ret2 = _RDB_drop_qresult(qrp, txp);
                if (ret == RDB_OK)
                    ret =  ret2;
                if (RDB_is_syserr(ret)) {
                    RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
                    RDB_rollback_all(txp);
                }
                return ret;
            }
        case RDB_TB_RENAME:
            return rename_contains(tbp, tup, txp);
        case RDB_TB_WRAP:
            return wrap_contains(tbp, tup, txp);
        case RDB_TB_UNWRAP:
            return unwrap_contains(tbp, tup, txp);;
        case RDB_TB_SDIVIDE:
            return sdivide_contains(tbp, tup, txp);
    }
    /* should never be reached */
    abort();
}
