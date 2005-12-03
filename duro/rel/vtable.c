/*
 * $Id$
 *
 * Copyright (C) 2004-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/*
 * Functions for virtual tables
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

static RDB_string_vec *
dup_keys(int keyc, RDB_string_vec *keyv) {
    RDB_string_vec *newkeyv;
    int i;

    newkeyv = malloc(keyc * sizeof(RDB_string_vec));
    if (newkeyv == NULL) {
        return NULL;
    }
    for (i = 0; i < keyc; i++)
        newkeyv[i].strv = NULL;
    for (i = 0; i < keyc; i++) {
        newkeyv[i].strc = keyv[i].strc;
        newkeyv[i].strv = RDB_dup_strvec(
                keyv[i].strc, keyv[i].strv);
        if (newkeyv[i].strv == NULL) {
            goto error;
        }
    }
    return newkeyv;
error:
    /* Free keys */
    for (i = 0; i < keyc; i++) {
        if (newkeyv[i].strv != NULL)
            RDB_free_strvec(newkeyv[i].strc, newkeyv[i].strv);
    }
    return NULL;
}

static RDB_string_vec *
dup_rename_keys(int keyc, RDB_string_vec *keyv, int renc,
        const RDB_renaming renv[])
{
    RDB_string_vec *newkeyv;
    int i, j;

    newkeyv = malloc(keyc * sizeof(RDB_string_vec));
    if (newkeyv == NULL) {
        return NULL;
    }
    for (i = 0; i < keyc; i++)
        newkeyv[i].strv = NULL;
    for (i = 0; i < keyc; i++) {
        newkeyv[i].strc = keyv[i].strc;
        newkeyv[i].strv = malloc(sizeof (RDB_attr) * keyv[i].strc);
        if (newkeyv[i].strv == NULL) {
            goto error;
        }
        for (j = 0; j < keyv[i].strc; j++)
            newkeyv[i].strv[j] = NULL;
        for (j = 0; j < keyv[i].strc; j++) {
            /* Has the attribute been renamed? */
            int ai = _RDB_find_rename_from(renc, renv, keyv[i].strv[j]);
            if (ai >= 0) /* Yes */
                newkeyv[i].strv[j] = RDB_dup_str(renv[ai].to);
            else
                newkeyv[i].strv[j] = RDB_dup_str(keyv[i].strv[j]);
            if (newkeyv[i].strv[j] == NULL)
                goto error;
        }
    }
    return newkeyv;

error:
    /* Free keys */
    for (i = 0; i < keyc; i++) {
        if (newkeyv[i].strv != NULL)
            RDB_free_strvec(newkeyv[i].strc, newkeyv[i].strv);
    }
    return NULL;
}

RDB_table *
_RDB_select(RDB_table *tbp, RDB_expression *condp, RDB_exec_context *ecp)
{
    RDB_table *newtbp;

    /* Allocate RDB_table structure */
    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    /* Create type */
    newtbp->typ = _RDB_dup_nonscalar_type(tbp->typ, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_SELECT;
    newtbp->var.select.tbp = tbp;
    newtbp->var.select.exp = condp;
    newtbp->var.select.objpc = 0;

    return newtbp;
}

RDB_table *
RDB_select(RDB_table *tbp, RDB_expression *condp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return NULL;
    }

    /*
     * Check if condition is of type BOOLEAN
     */
    
    ret = _RDB_check_expr_type(condp, tbp->typ->var.basetyp, &RDB_BOOLEAN,
            ecp, txp);
    if (ret != RDB_OK)
        return NULL;

    return _RDB_select(tbp, condp, ecp);
}

static RDB_string_vec *
all_key(RDB_table *tbp)
{
    int attrc;
    int i;
    RDB_string_vec *keyv = malloc(sizeof (RDB_string_vec));

    if (keyv == NULL)
        return NULL;

    attrc = keyv[0].strc =
            tbp->typ->var.basetyp->var.tuple.attrc;
    keyv[0].strv = malloc(sizeof(char *) * attrc);
    if (keyv[0].strv == NULL) {
        free(keyv);
        return NULL;
    }
    for (i = 0; i < attrc; i++)
        keyv[0].strv[i] = NULL;
    for (i = 0; i < attrc; i++) {
        keyv[0].strv[i] = RDB_dup_str(
                tbp->typ->var.basetyp->var.tuple.attrv[i].name);
        if (keyv[0].strv[i] == NULL) {
            goto error;
        }
    }

    return keyv;
error:
    RDB_free_strvec(keyv[0].strc, keyv[0].strv);
    free(keyv);
    return NULL;
}

RDB_table *
RDB_union(RDB_table *tb1p, RDB_table *tb2p, RDB_exec_context *ecp)
{
    RDB_table *newtbp;

    if (!RDB_type_equals(tb1p->typ, tb2p->typ)) {
        RDB_raise_type_mismatch("UNION tables must match", ecp);
        return NULL;
    }

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL) {
        return NULL;
    }

    /* Create type */
    newtbp->typ = _RDB_dup_nonscalar_type(tb1p->typ, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtbp->kind = RDB_TB_UNION;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var._union.tb1p = tb1p;
    newtbp->var._union.tb2p = tb2p;

    return newtbp;
}

RDB_table *
RDB_minus(RDB_table *tb1p, RDB_table *tb2p, RDB_exec_context *ecp)
{
    if (!RDB_type_equals(tb1p->typ, tb2p->typ)) {
        RDB_raise_type_mismatch("MINUS tables must match", ecp);
        return NULL;
    }
    return RDB_semiminus(tb1p, tb2p, ecp);
}

RDB_table *
RDB_semiminus(RDB_table *tb1p, RDB_table *tb2p, RDB_exec_context *ecp)
{
    RDB_table *newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL) {
        return NULL;
    }

    /* Create type */
    newtbp->typ = _RDB_dup_nonscalar_type(tb1p->typ, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtbp->kind = RDB_TB_SEMIMINUS;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.semiminus.tb1p = tb1p;
    newtbp->var.semiminus.tb2p = tb2p;

    return newtbp;
}

RDB_table *
RDB_intersect(RDB_table *tb1p, RDB_table *tb2p, RDB_exec_context *ecp)
{
    RDB_table *newtbp;

    if (!RDB_type_equals(tb1p->typ, tb2p->typ)) {
        RDB_raise_type_mismatch("INTERSECT tables must match", ecp);
        return NULL;
    }

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    /* Create type */
    newtbp->typ = _RDB_dup_nonscalar_type(tb1p->typ, ecp);
    if (newtbp->typ == NULL) {
        return NULL;
    }

    newtbp->kind = RDB_TB_INTERSECT;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.intersect.tb1p = tb1p;
    newtbp->var.intersect.tb2p = tb2p;
    newtbp->name = NULL;

    return newtbp;
}

RDB_table *
RDB_join(RDB_table *tb1p, RDB_table *tb2p, RDB_exec_context *ecp)
{
    RDB_table *newtbp;

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL) {
        return NULL;
    }

    newtbp->kind = RDB_TB_JOIN;
    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.join.tb1p = tb1p;
    newtbp->var.join.tb2p = tb2p;

    newtbp->typ = RDB_join_relation_types(tb1p->typ, tb2p->typ, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }
    return newtbp;
}

RDB_table *
RDB_extend(RDB_table *tbp, int attrc, const RDB_virtual_attr attrv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    for (i = 0; i < attrc; i++) {
        if (!_RDB_legal_name(attrv[i].name)) {
            RDB_raise_invalid_argument("invalid attribute name", ecp);
            return NULL;
        }
    }
    return _RDB_extend(tbp, attrc, attrv, ecp, txp);
}

RDB_table *
_RDB_extend(RDB_table *tbp, int attrc, const RDB_virtual_attr attrv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_table *newtbp = NULL;
    RDB_attr *attrdefv = NULL;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return NULL;
    }

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_EXTEND;
    newtbp->typ = NULL;
    newtbp->var.extend.tbp = tbp;
    newtbp->var.extend.attrc = attrc;
    newtbp->var.extend.attrv = malloc(sizeof(RDB_virtual_attr) * attrc);
    if (newtbp->var.extend.attrv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    attrdefv = malloc(sizeof(RDB_attr) * attrc);
    if (attrdefv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    for (i = 0; i < attrc; i++) {
        attrdefv[i].name = NULL;
        attrdefv[i].typ = NULL;
    }
    for (i = 0; i < attrc; i++) {
        newtbp->var.extend.attrv[i].name = RDB_dup_str(attrv[i].name);
        if (newtbp->var.extend.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtbp->var.extend.attrv[i].exp = attrv[i].exp;
        attrdefv[i].name = RDB_dup_str(attrv[i].name);
        if (attrdefv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        attrdefv[i].typ = RDB_expr_type(attrv[i].exp, tbp->typ->var.basetyp,
                ecp, txp);
        if (attrdefv[i].typ == NULL)
            goto error;
    }
    newtbp->typ = RDB_extend_relation_type(tbp->typ, attrc, attrdefv, ecp);
    if (newtbp->typ == NULL)
        goto error;

    for (i = 0; i < attrc; i++) {
        free(attrdefv[i].name);
        if (!RDB_type_is_scalar(attrdefv[i].typ)) {
            RDB_drop_type(attrdefv[i].typ, ecp, NULL);
        }
    }
    free(attrdefv);
    return newtbp;

error:
    if (newtbp->typ != NULL)
        RDB_drop_type(newtbp->typ, ecp, NULL);
    free(newtbp);
    if (attrdefv != NULL) {
        for (i = 0; i < attrc; i++) {
            free(attrdefv[i].name);
            if (attrdefv[i].typ != NULL
                    && !RDB_type_is_scalar(attrdefv[i].typ)) {
                RDB_drop_type(attrdefv[i].typ, ecp, NULL);
            }
        }
        free(attrdefv);
    }
    return NULL;
}

static int
check_keyloss(RDB_table *tbp, int attrc, RDB_attr attrv[], RDB_bool presv[])
{
    int i, j, k;
    int count = 0;

    for (i = 0; i < tbp->keyc; i++) {
        for (j = 0; j < tbp->keyv[i].strc; j++) {
            /* Search for key attribute in attrv */
            for (k = 0;
                 (k < attrc) && (strcmp(tbp->keyv[i].strv[j], attrv[k].name) != 0);
                 k++);
            /* If not found, exit loop */
            if (k >= attrc)
                break;
        }
        /* If the loop didn't terminate prematurely, the key is preserved */
        presv[i] = (RDB_bool) (j >= tbp->keyv[i].strc);
        if (presv[i])
            count++;
    }
    return count;
}

RDB_table *
RDB_project(RDB_table *tbp, int attrc, char *attrv[], RDB_exec_context *ecp)
{
    RDB_table *newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_PROJECT;
    newtbp->var.project.tbp = tbp;
    newtbp->var.project.indexp = NULL;
    newtbp->keyv = NULL;

    /* Create type */
    newtbp->typ = RDB_project_relation_type(tbp->typ, attrc, attrv, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    return newtbp;
}

RDB_table *
RDB_remove(RDB_table *tbp, int attrc, char *attrv[], RDB_exec_context *ecp)
{
    int ret;
    int i, j;
    char **resattrv;
    RDB_table *newtbp = NULL;
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int baseattrc = tuptyp->var.tuple.attrc;

    if (attrc > baseattrc) {
        RDB_raise_invalid_argument("invalid projection attributes", ecp);
        return NULL;
    }

    /* Allocate vector of remaining attributes */
    resattrv = malloc((baseattrc - attrc) * sizeof (char *));
    if (resattrv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    /* Get the table attributes which are not in attrv */
    for (i = 0, j = 0; i < baseattrc && j < baseattrc - attrc; i++) {
        if (RDB_find_str(attrc, attrv, tuptyp->var.tuple.attrv[i].name) == -1) {
            if (j == baseattrc - attrc) {
                /* Not-existing attribute in attrv */
                RDB_raise_attribute_not_found(tuptyp->var.tuple.attrv[i].name,
                        ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
            resattrv[j++] = tuptyp->var.tuple.attrv[i].name;
        }
    }

    newtbp = RDB_project(tbp, baseattrc - attrc, resattrv, ecp);

cleanup:
    free(resattrv);
    return newtbp;
}

RDB_table *
RDB_summarize(RDB_table *tb1p, RDB_table *tb2p, int addc,
        const RDB_summarize_add addv[], RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_table *newtbp;
    int i, ai;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return NULL;
    }

    /* Additional (internal) attribute for each AVG */
    int avgc;
    char **avgv;

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_SUMMARIZE;
    newtbp->var.summarize.tb1p = tb1p;
    newtbp->var.summarize.tb2p = tb2p;
    newtbp->typ = NULL;

    newtbp->var.summarize.addc = addc;
    newtbp->var.summarize.addv = malloc(sizeof(RDB_summarize_add) * addc);
    if (newtbp->var.summarize.addv == NULL) {
        free(newtbp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    avgc = 0;
    for (i = 0; i < addc; i++) {
        newtbp->var.summarize.addv[i].name = NULL;
        if (addv[i].op == RDB_AVG)
            avgc++;
        newtbp->var.summarize.addv[i].exp = NULL;
    }
    avgv = malloc(avgc * sizeof(char *));
    for (i = 0; i < avgc; i++)
        avgv[i] = NULL;
    if (avgv == NULL) {
        free(newtbp->var.summarize.addv);
        free(newtbp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    ai = 0;
    for (i = 0; i < addc; i++) {
        switch (addv[i].op) {
            case RDB_COUNTD:
                RDB_raise_not_supported("COUNTD not supported", ecp);
                goto error;
            case RDB_SUMD:
                RDB_raise_not_supported("SUMD not supported", ecp);
                goto error;
            case RDB_AVGD:
                RDB_raise_not_supported("AVGD not supported", ecp);
                goto error;
            case RDB_AVG:
                avgv[ai] = malloc(strlen(addv[i].name) + 3);
                if (avgv[ai] == NULL) {
                    RDB_raise_no_memory(ecp);
                    goto error;
                }
                strcpy(avgv[ai], addv[i].name);
                strcat(avgv[ai], AVG_COUNT_SUFFIX);
                ai++;
                break;
            default: ;
        }
        newtbp->var.summarize.addv[i].op = addv[i].op;
        newtbp->var.summarize.addv[i].name = RDB_dup_str(addv[i].name);
        if (newtbp->var.summarize.addv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtbp->var.summarize.addv[i].exp = addv[i].exp;
    }

    newtbp->typ = RDB_summarize_type(tb1p->typ, tb2p->typ, addc, addv, avgc, avgv,
            ecp, txp);
    if (newtbp->typ == NULL)
        goto error;

    free(avgv);
    return newtbp;

error:
    for (i = 0; i < avgc; i++)
        free(avgv[i]);
    free(avgv);
    for (i = 0; i < addc; i++) {
        free(newtbp->var.summarize.addv[i].name);
    }
    free(newtbp);
    return NULL;
}

RDB_table *
RDB_rename(RDB_table *tbp, int renc, const RDB_renaming renv[],
           RDB_exec_context *ecp)
{
    RDB_table *newtbp;
    int i;

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_RENAME;
    newtbp->keyc = tbp->keyc;

    newtbp->typ = RDB_rename_relation_type(tbp->typ, renc, renv, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    newtbp->var.rename.renc = renc;
    newtbp->var.rename.renv = malloc(sizeof (RDB_renaming) * renc);
    if (newtbp->var.rename.renv == NULL) {
        RDB_drop_type(newtbp->typ, ecp, NULL);
        free(newtbp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < renc; i++) {
        newtbp->var.rename.renv[i].to = newtbp->var.rename.renv[i].from = NULL;
    }
    for (i = 0; i < renc; i++) {
        newtbp->var.rename.renv[i].to = RDB_dup_str(renv[i].to);
        if (newtbp->var.rename.renv[i].to == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtbp->var.rename.renv[i].from = RDB_dup_str(renv[i].from);
        if (newtbp->var.rename.renv[i].from == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }
    newtbp->var.rename.tbp = tbp;
    return newtbp;

error:
    for (i = 0; i < renc; i++) {
        free(newtbp->var.rename.renv[i].to);
        free(newtbp->var.rename.renv[i].from);
    }
    free(newtbp->var.rename.renv);
    free(newtbp);
    return NULL;
}

RDB_table *
RDB_wrap(RDB_table *tbp, int wrapc, const RDB_wrapping wrapv[],
         RDB_exec_context *ecp)
{
    RDB_table *newtbp;
    int i;

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_WRAP;
    newtbp->typ = RDB_wrap_relation_type(tbp->typ, wrapc, wrapv, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    newtbp->var.wrap.wrapc = wrapc;
    newtbp->var.wrap.wrapv = malloc(sizeof (RDB_wrapping) * wrapc);
    if (newtbp->var.wrap.wrapv == NULL) {
        RDB_drop_type(newtbp->typ, ecp, NULL);
        free(newtbp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < wrapc; i++) {
        newtbp->var.wrap.wrapv[i].attrname = NULL;
        newtbp->var.wrap.wrapv[i].attrv = NULL;
    }
    for (i = 0; i < wrapc; i++) {
        newtbp->var.wrap.wrapv[i].attrname = RDB_dup_str(wrapv[i].attrname);
        if (newtbp->var.wrap.wrapv[i].attrname == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtbp->var.wrap.wrapv[i].attrc = wrapv[i].attrc;
        newtbp->var.wrap.wrapv[i].attrv = RDB_dup_strvec(wrapv[i].attrc,
                wrapv[i].attrv);
        if (newtbp->var.wrap.wrapv[i].attrv == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }
    newtbp->var.wrap.tbp = tbp;

    return newtbp;

error:
    RDB_drop_type(newtbp->typ, ecp, NULL);
    for (i = 0; i < wrapc; i++) {
        free(newtbp->var.wrap.wrapv[i].attrname);
        if (newtbp->var.wrap.wrapv[i].attrv != NULL)
            RDB_free_strvec(newtbp->var.wrap.wrapv[i].attrc,
                    newtbp->var.wrap.wrapv[i].attrv);
    }
    free(newtbp->var.wrap.wrapv);
    free(newtbp);
    return NULL;
}

RDB_table *
RDB_unwrap(RDB_table *tbp, int attrc, char *attrv[], RDB_exec_context *ecp)
{
    RDB_table *newtbp;

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_UNWRAP;
    newtbp->typ = NULL;
    newtbp->typ = RDB_unwrap_relation_type(tbp->typ, attrc, attrv, ecp);
    if (newtbp->typ == NULL) {
        goto error;
    }

    newtbp->var.unwrap.attrc = attrc;
    newtbp->var.unwrap.attrv = RDB_dup_strvec(attrc, attrv);
    if (newtbp->var.unwrap.attrv == NULL) {
        RDB_drop_type(newtbp->typ, ecp, NULL);
        free(newtbp);
        RDB_raise_no_memory(ecp);
        goto error;
    }
    newtbp->var.unwrap.tbp = tbp;

    return newtbp;

error:
    if (newtbp->typ != NULL)
        RDB_drop_type(newtbp->typ, ecp, NULL);
    free(newtbp);
    return NULL;
}

RDB_table *
RDB_sdivide(RDB_table *tb1p, RDB_table *tb2p, RDB_table *tb3p,
        RDB_exec_context *ecp)
{
    RDB_type *typ;
    RDB_table *newtbp;

    /*
     * Table 1 JOIN table 2 must be of same type as table 3
     */
    typ = RDB_join_relation_types(tb1p->typ, tb2p->typ, ecp);
    if (typ == NULL) {
        return NULL;
    }

    if (!RDB_type_equals(typ, tb3p->typ)) {
        RDB_drop_type(typ, ecp, NULL);
        RDB_raise_not_supported(
            "argument #3 to SDIVIDE must be of the same type "
            "as the join of arguments #1 and #2", ecp);
        return NULL;
    }
    RDB_drop_type(typ, ecp, NULL);

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->typ = _RDB_dup_nonscalar_type(tb1p->typ, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->var.sdivide.tb1p = tb1p;
    newtbp->var.sdivide.tb2p = tb2p;
    newtbp->var.sdivide.tb3p = tb3p;
    newtbp->kind = RDB_TB_SDIVIDE;

    return newtbp;
}

RDB_table *
RDB_group(RDB_table *tbp, int attrc, char *attrv[], const char *gattr,
        RDB_exec_context *ecp)
{
    int i;
    RDB_table *newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_GROUP;

    newtbp->typ = RDB_group_type(tbp->typ, attrc, attrv, gattr, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    newtbp->var.group.tbp = tbp;
    newtbp->var.group.attrc = attrc;
    newtbp->var.group.attrv = malloc(sizeof (char *) * attrc);
    if (newtbp->var.group.attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        newtbp->var.group.attrv[i] = RDB_dup_str(attrv[i]);
        if (newtbp->var.group.attrv[i] == NULL) {
            RDB_raise_no_memory(ecp);
            return NULL;
        }
    }
    newtbp->var.group.gattr = RDB_dup_str(gattr);
    if (newtbp->var.group.gattr == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    return newtbp;
}

RDB_table *
RDB_ungroup(RDB_table *tbp, const char *attr, RDB_exec_context *ecp)
{
    RDB_table *newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_UNGROUP;

    newtbp->typ = RDB_ungroup_type(tbp->typ, attr, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    newtbp->var.ungroup.tbp = tbp;
    newtbp->var.ungroup.attr = RDB_dup_str(attr);
    if (newtbp->var.ungroup.attr == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    return newtbp;
}

static RDB_table *
dup_extend(RDB_table *tbp, RDB_exec_context *ecp)
{
    int i;
    int attrc = tbp->var.extend.attrc;
    RDB_table *newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_EXTEND;
    newtbp->var.extend.tbp = _RDB_dup_vtable(tbp->var.extend.tbp, ecp);
    if (newtbp->var.extend.tbp == NULL)
        goto error;
    newtbp->var.extend.attrc = attrc;
    newtbp->var.extend.attrv = malloc(sizeof(RDB_virtual_attr) * attrc);
    if (newtbp->var.extend.attrv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    for (i = 0; i < attrc; i++) {
        newtbp->var.extend.attrv[i].name = NULL;
        newtbp->var.extend.attrv[i].exp = NULL;
    }
    for (i = 0; i < attrc; i++) {
        newtbp->var.extend.attrv[i].name =
                RDB_dup_str(tbp->var.extend.attrv[i].name);
        if (newtbp->var.extend.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtbp->var.extend.attrv[i].exp =
                RDB_dup_expr(tbp->var.extend.attrv[i].exp, ecp);
        if (newtbp->var.extend.attrv[i].exp == NULL) {
            goto error;
        }
    }
    newtbp->typ = _RDB_dup_nonscalar_type(tbp->typ, ecp);
    if (newtbp->typ == NULL) {
        goto error;
    }
    return newtbp;

error:
    if (newtbp->var.extend.tbp != NULL
            && newtbp->var.extend.tbp->kind != RDB_TB_REAL) {
        RDB_drop_table(newtbp->var.extend.tbp, ecp, NULL);
    }
    if (newtbp->var.extend.attrv != NULL) {
        for (i = 0; i < attrc; i++) {
            free(newtbp->var.extend.attrv[i].name);
            if (newtbp->var.extend.attrv[i].exp != NULL)
                RDB_drop_expr(newtbp->var.extend.attrv[i].exp, ecp);
        }
    }
    free(newtbp);
    return NULL;
}

static RDB_table *
dup_summarize(RDB_table *tbp, RDB_exec_context *ecp)
{
    RDB_table *newtbp;
    int i;
    int addc = tbp->var.summarize.addc;

    newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_SUMMARIZE;
    newtbp->var.summarize.tb1p = _RDB_dup_vtable(tbp->var.summarize.tb1p, ecp);
    if (newtbp->var.summarize.tb1p == NULL) {
        free(newtbp);
        return NULL;
    }
    newtbp->var.summarize.tb2p = _RDB_dup_vtable(tbp->var.summarize.tb2p, ecp);
    if (newtbp->var.summarize.tb2p == NULL) {
        free(newtbp);
        return NULL;
    }
    newtbp->typ = _RDB_dup_nonscalar_type(tbp->typ, ecp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    newtbp->var.summarize.addc = addc;
    newtbp->var.summarize.addv = malloc(sizeof(RDB_summarize_add) * addc);
    if (newtbp->var.summarize.addv == NULL) {
        free(newtbp);
        return NULL;
    }
    for (i = 0; i < addc; i++) {
        newtbp->var.summarize.addv[i].op = tbp->var.summarize.addv[i].op;
        newtbp->var.summarize.addv[i].name = RDB_dup_str(
                tbp->var.summarize.addv[i].name);
        if (newtbp->var.summarize.addv[i].name == NULL) {
            goto error;
        }
        if (tbp->var.summarize.addv[i].op != RDB_COUNT
                && tbp->var.summarize.addv[i].op != RDB_COUNTD) {
            newtbp->var.summarize.addv[i].exp = RDB_dup_expr(
                    tbp->var.summarize.addv[i].exp, ecp);
            if (newtbp->var.summarize.addv[i].exp == NULL)
                goto error;
        } else {
            newtbp->var.summarize.addv[i].exp = NULL;
        }
    }

    return newtbp;

error:
    if (newtbp->typ != NULL)
        free(newtbp->typ);
    for (i = 0; i < addc; i++) {
        free(newtbp->var.summarize.addv[i].name);
        if (newtbp->var.summarize.addv[i].exp != NULL)
            RDB_drop_expr(newtbp->var.summarize.addv[i].exp, ecp);
    }
    free(newtbp);
    return NULL;
}

static RDB_table *
dup_select(RDB_table *tbp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_table *ntbp;
    RDB_table *ctbp = _RDB_dup_vtable(tbp->var.select.tbp, ecp);
    if (ctbp == NULL)
        return NULL;

    exp = RDB_dup_expr(tbp->var.select.exp, ecp);
    if (exp == NULL)
        return NULL;
    ntbp = _RDB_select(ctbp, exp, ecp);
    if (ntbp == NULL) {
        RDB_drop_expr(exp, ecp);
        return NULL;
    }

    if (tbp->var.select.tbp->kind == RDB_TB_PROJECT
            && tbp->var.select.tbp->var.project.indexp != NULL) {
        /* Selection uses index */
        ntbp->var.select.objpv = _RDB_index_objpv(
                tbp->var.select.tbp->var.project.indexp,
                exp, ntbp->typ, tbp->var.select.objpc, tbp->var.select.all_eq,
                tbp->var.select.asc);
        if (ntbp->var.select.objpv == NULL)
            return NULL;
        ntbp->var.select.objpc = tbp->var.select.objpc;
        ntbp->var.select.all_eq = tbp->var.select.all_eq;
        ntbp->var.select.asc = tbp->var.select.asc;
    }

    return ntbp;
}

RDB_table *
_RDB_dup_vtable(RDB_table *tbp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_table *tb1p;
    RDB_table *tb2p;
    RDB_table *tb3p;
    RDB_table *ntbp;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            if (tbp->name == NULL) {
                RDB_table *ntbp = RDB_create_table_from_type(NULL, RDB_FALSE,
                        tbp->typ, tbp->keyc, tbp->keyv, ecp, NULL);
                if (ntbp == NULL)
                    return NULL;
                ret = _RDB_move_tuples(ntbp, tbp, ecp, NULL);
                if (ret != RDB_OK)
                    return NULL;
                return ntbp;
            }
            return tbp;
        case RDB_TB_SELECT:
            return dup_select(tbp, ecp);
        case RDB_TB_UNION:
            tb1p = _RDB_dup_vtable(tbp->var._union.tb1p, ecp);
            if (tb1p == NULL)
                return NULL;
            tb2p = _RDB_dup_vtable(tbp->var._union.tb2p, ecp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            return RDB_union(tb1p, tb2p, ecp);
        case RDB_TB_SEMIMINUS:
            tb1p = _RDB_dup_vtable(tbp->var.semiminus.tb1p, ecp);
            if (tb1p == NULL)
                return NULL;
            tb2p = _RDB_dup_vtable(tbp->var.semiminus.tb2p, ecp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            return RDB_semiminus(tb1p, tb2p, ecp);
        case RDB_TB_INTERSECT:
            tb1p = _RDB_dup_vtable(tbp->var.intersect.tb1p, ecp);
            if (tb1p == NULL)
                return NULL;
            tb2p = _RDB_dup_vtable(tbp->var.intersect.tb2p, ecp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            return RDB_intersect(tb1p, tb2p, ecp);
        case RDB_TB_JOIN:
            tb1p = _RDB_dup_vtable(tbp->var.join.tb1p, ecp);
            if (tb1p == NULL)
                return NULL;
            tb2p = _RDB_dup_vtable(tbp->var.join.tb2p, ecp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            return RDB_join(tb1p, tb2p, ecp);
        case RDB_TB_EXTEND:
            return dup_extend(tbp, ecp);
        case RDB_TB_PROJECT:
        {
            char **attrnamev;
            int attrc = tbp->typ->var.basetyp->var.tuple.attrc;

            tb1p = _RDB_dup_vtable(tbp->var.project.tbp, ecp);
            if (tb1p == NULL)
                return NULL;
            attrnamev = malloc(attrc * sizeof(char *));
            if (attrnamev == NULL)
                return NULL;
            for (i = 0; i < attrc; i++) {
                attrnamev[i] = tbp->typ->var.basetyp->var.tuple.attrv[i].name;
            }
            ntbp = RDB_project(tb1p, attrc, attrnamev, ecp);
            free(attrnamev);
            if (ntbp == NULL)
                return NULL;
            ntbp->var.project.indexp = tbp->var.project.indexp;
            return ntbp;
        }
        case RDB_TB_SUMMARIZE:
            return dup_summarize(tbp, ecp);
        case RDB_TB_RENAME:
            tb1p = _RDB_dup_vtable(tbp->var.rename.tbp, ecp);
            if (tb1p == NULL)
                return NULL;
            return RDB_rename(tb1p, tbp->var.rename.renc, tbp->var.rename.renv,
                    ecp);
        case RDB_TB_WRAP:
            tb1p = _RDB_dup_vtable(tbp->var.wrap.tbp, ecp);
            if (tb1p == NULL)
                return NULL;
            tbp = RDB_wrap(tb1p, tbp->var.wrap.wrapc, tbp->var.wrap.wrapv,
                    ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;            
        case RDB_TB_UNWRAP:
            tb1p = _RDB_dup_vtable(tbp->var.unwrap.tbp, ecp);
            if (tb1p == NULL)
                return NULL;
            return RDB_unwrap(tb1p, tbp->var.unwrap.attrc, tbp->var.unwrap.attrv,
                    ecp);
        case RDB_TB_GROUP:
            tb1p = _RDB_dup_vtable(tbp->var.group.tbp, ecp);
            if (tb1p == NULL)
                return NULL;
            return RDB_group(tb1p, tbp->var.group.attrc, tbp->var.group.attrv,
                    tbp->var.group.gattr, ecp);
        case RDB_TB_UNGROUP:
            tb1p = _RDB_dup_vtable(tbp->var.ungroup.tbp, ecp);
            if (tb1p == NULL)
                return NULL;
            return RDB_ungroup(tb1p, tbp->var.ungroup.attr, ecp);
        case RDB_TB_SDIVIDE:
            tb1p = _RDB_dup_vtable(tbp->var.sdivide.tb1p, ecp);
            if (tb1p == NULL)
                return NULL;
            tb2p = _RDB_dup_vtable(tbp->var.sdivide.tb2p, ecp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            tb3p = _RDB_dup_vtable(tbp->var.sdivide.tb3p, ecp);
            if (tb3p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                 if (tb2p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb2p, ecp, NULL);
                return NULL;
            }
            return RDB_sdivide(tb1p, tb2p, tb3p, ecp);
    }
    /* Must never be reached */
    abort();
}

static RDB_bool
renamings_equals(int renc, RDB_renaming renv1[], RDB_renaming renv2[])
{
    int i, ai;

    for (i = 0; i < renc; i++) {
        ai = _RDB_find_rename_from(renc, renv2, renv1[i].from);
        if (ai == -1)
            return RDB_FALSE;
        if (strcmp(renv1[i].to, renv2[ai].to) != 0)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

RDB_bool
_RDB_table_def_equals(RDB_table *tb1p, RDB_table *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (tb1p == tb2p)
        return RDB_TRUE;

    if (tb1p->kind != tb2p->kind)
        return RDB_FALSE;

    switch (tb1p->kind) {
        case RDB_TB_REAL:
            return RDB_FALSE;
        case RDB_TB_SELECT:
        {
            RDB_bool res;
            int ret = _RDB_expr_equals(tb1p->var.select.exp,
                    tb2p->var.select.exp, ecp, txp, &res);
            if (ret != RDB_OK || !res)
                return RDB_FALSE;
            return (RDB_bool) _RDB_table_def_equals(tb1p->var.select.tbp,
                    tb2p->var.select.tbp, ecp, txp);
        }
        case RDB_TB_UNION:
            return (RDB_bool) (_RDB_table_def_equals(tb1p->var._union.tb1p,
                    tb2p->var._union.tb1p, ecp, txp)
                    && _RDB_table_def_equals(tb1p->var._union.tb2p,
                    tb2p->var._union.tb2p, ecp, txp));
        case RDB_TB_SEMIMINUS:
            return (RDB_bool) (_RDB_table_def_equals(tb1p->var.semiminus.tb1p,
                    tb2p->var.semiminus.tb1p, ecp, txp)
                    && _RDB_table_def_equals(tb1p->var.semiminus.tb2p,
                    tb2p->var.semiminus.tb2p, ecp, txp));
        case RDB_TB_INTERSECT:
            return (RDB_bool) (_RDB_table_def_equals(tb1p->var.intersect.tb1p,
                    tb2p->var.intersect.tb1p, ecp, txp)
                    && _RDB_table_def_equals(tb1p->var.intersect.tb2p,
                    tb2p->var.intersect.tb2p, ecp, txp));
        case RDB_TB_JOIN:
            return (RDB_bool) (_RDB_table_def_equals(tb1p->var.join.tb1p,
                    tb2p->var.join.tb1p, ecp, txp)
                    && _RDB_table_def_equals(tb1p->var.join.tb2p,
                    tb2p->var.join.tb2p, ecp, txp));
        case RDB_TB_SDIVIDE:
            return (RDB_bool) (_RDB_table_def_equals(tb1p->var.sdivide.tb1p,
                    tb2p->var.sdivide.tb1p, ecp, txp)
                    && _RDB_table_def_equals(tb1p->var.sdivide.tb2p,
                    tb2p->var.sdivide.tb2p, ecp, txp)
                    && _RDB_table_def_equals(tb1p->var.sdivide.tb2p,
                    tb2p->var.sdivide.tb3p, ecp, txp));
        case RDB_TB_PROJECT:
            return (RDB_bool) (RDB_type_equals(tb1p->typ, tb2p->typ)
                    && _RDB_table_def_equals(tb1p->var.project.tbp,
                            tb2p->var.project.tbp, ecp, txp));
        case RDB_TB_RENAME:
            return (RDB_bool) (_RDB_table_def_equals(tb1p->var.rename.tbp,
                            tb2p->var.rename.tbp, ecp, txp)
                    && tb1p->var.rename.renc == tb2p->var.rename.renc
                    && renamings_equals(tb1p->var.rename.renc,
                            tb1p->var.rename.renv, tb2p->var.rename.renv));
        case RDB_TB_EXTEND:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_WRAP:
        case RDB_TB_UNWRAP:
        case RDB_TB_GROUP:
            /* !! */
            return RDB_FALSE;
        case RDB_TB_UNGROUP:
            return (RDB_bool) (_RDB_table_def_equals(tb1p->var.ungroup.tbp,
                            tb2p->var.ungroup.tbp, ecp, txp)
                    && strcmp(tb1p->var.ungroup.attr, tb2p->var.ungroup.attr)
                            == 0);
    }
    /* Must never be reached */
    abort();
}

static int
infer_join_keys(RDB_table *tbp, RDB_exec_context *ecp)
{
    int i, j, k;
    int keyc1, keyc2;
    int newkeyc;
    RDB_string_vec *keyv1, *keyv2;
    RDB_string_vec *newkeyv;

    keyc1 = RDB_table_keys(tbp->var.join.tb1p, ecp, &keyv1);
    if (keyc1 < 0)
        return keyc1;
    keyc2 = RDB_table_keys(tbp->var.join.tb2p, ecp, &keyv2);
    if (keyc2 < 0)
        return keyc2;

    newkeyc = keyc1 * keyc2;
    newkeyv = malloc(sizeof (RDB_string_vec) * newkeyc);
    if (newkeyv == NULL)
        goto error;
    for (i = 0; i < keyc1; i++) {
        for (j = 0; j < keyc2; j++) {
            RDB_string_vec *attrsp = &newkeyv[i * keyc2 + j];

            attrsp->strc = keyv1[i].strc + keyv2[j].strc;
            attrsp->strv = malloc(sizeof(char *) * attrsp->strc);
            if (attrsp->strv == NULL)
                goto error;
            for (k = 0; k < attrsp->strc; k++)
                attrsp->strv[k] = NULL;
            for (k = 0; k < keyv1[i].strc; k++) {
                attrsp->strv[k] = RDB_dup_str(keyv1[i].strv[k]);
                if (attrsp->strv[k] == NULL)
                    goto error;
            }
            for (k = 0; k < keyv2[j].strc; k++) {
                attrsp->strv[keyv1[i].strc + k] =
                        RDB_dup_str(keyv2[j].strv[k]);
                if (attrsp->strv[keyv1[i].strc + k] == NULL)
                    goto error;
            }
        }
    }
    tbp->keyc = newkeyc;
    tbp->keyv = newkeyv;
    return RDB_OK;

error:
    if (newkeyv != NULL) {
        for (i = 0; i < newkeyc; i++) {
            if (newkeyv[i].strv != NULL)
                RDB_free_strvec(newkeyv[i].strc, newkeyv[i].strv);
        }
    }
    RDB_raise_no_memory(ecp);
    return RDB_ERROR;
}

static int
infer_project_keys(RDB_table *tbp, RDB_exec_context *ecp)
{
    int keyc;
    int newkeyc;
    RDB_string_vec *keyv;
    RDB_string_vec *newkeyv;
    RDB_bool *presv;
    
    keyc = RDB_table_keys(tbp->var.project.tbp, ecp, &keyv);
    if (keyc < 0)
        return keyc;

    presv = malloc(sizeof(RDB_bool) * keyc);
    if (presv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    newkeyc = check_keyloss(tbp->var.project.tbp,
            tbp->typ->var.basetyp->var.tuple.attrc,
            tbp->typ->var.basetyp->var.tuple.attrv, presv);
    tbp->var.project.keyloss = (RDB_bool) (newkeyc == 0);
    if (tbp->var.project.keyloss) {
        /* Table is all-key */
        newkeyc = 1;
        newkeyv = all_key(tbp);
        if (newkeyv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    } else {
        int i, j;

        /* Pick the keys which survived the projection */
        newkeyv = malloc(sizeof (RDB_string_vec) * newkeyc);
        if (newkeyv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        for (i = 0; i < newkeyc; i++) {
            newkeyv[i].strv = NULL;
        }

        for (j = i = 0; j < keyc; j++) {
            if (presv[j]) {
                newkeyv[i].strc = keyv[j].strc;
                newkeyv[i].strv = RDB_dup_strvec(keyv[j].strc,
                        keyv[j].strv);
                if (newkeyv[i].strv == NULL) {
                    RDB_raise_no_memory(ecp);
                    return RDB_ERROR;
                }
                i++;
            }
        }
    }
    free(presv);
    tbp->keyc = newkeyc;
    tbp->keyv = newkeyv;
    return RDB_OK;
}

static int
infer_group_keys(RDB_table *tbp, RDB_exec_context *ecp)
{
    int i, j;
    int newkeyc;
    RDB_string_vec *newkeyv;

    /*
     * Key consists of all attributes which are not grouped
     */    
    newkeyc = 1;
    newkeyv = malloc(sizeof(RDB_string_vec));
    if (newkeyv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    newkeyv[0].strc = tbp->typ->var.basetyp->var.tuple.attrc - 1;
    newkeyv[0].strv = malloc(sizeof (char *) * newkeyv[0].strc);
    if (newkeyv[0].strv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    j = 0;
    for (i = 0; i < tbp->typ->var.basetyp->var.tuple.attrc; i++) {
        if (strcmp(tbp->typ->var.basetyp->var.tuple.attrv[i].name,
                tbp->var.group.gattr) != 0) {
            newkeyv[0].strv[j] = RDB_dup_str(
                    tbp->typ->var.basetyp->var.tuple.attrv[i].name);
            if (newkeyv[0].strv[j] == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            j++;
        }
    }

    tbp->keyc = newkeyc;
    tbp->keyv = newkeyv;
    return RDB_OK;
}

int
_RDB_infer_keys(RDB_table *tbp, RDB_exec_context *ecp)
{
    int keyc;
    RDB_string_vec *keyv;
    RDB_string_vec *newkeyv;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            RDB_raise_invalid_argument("table is real", ecp);
            return RDB_ERROR;
        case RDB_TB_SELECT:
            /* Copy keys */
            keyc = RDB_table_keys(tbp->var.select.tbp, ecp, &keyv);
            if (keyc < 0)
                return RDB_ERROR;
            newkeyv = dup_keys(keyc, keyv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            tbp->keyc = keyc;
            tbp->keyv = newkeyv;
            return RDB_OK;
        case RDB_TB_UNION:
        case RDB_TB_WRAP:
        case RDB_TB_UNWRAP:
        case RDB_TB_UNGROUP:
            /*
             * Table is all-key
             * (to be improved in case of WRAP, UNWRAP, UNGROUP)
             */
            newkeyv = all_key(tbp);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            tbp->keyc = 1;
            tbp->keyv = newkeyv;
            return RDB_OK;
        case RDB_TB_SEMIMINUS:
            /* Copy keys */
            keyc = RDB_table_keys(tbp->var.semiminus.tb1p, ecp, &keyv);
            if (keyc < 0)
                return RDB_ERROR;
            newkeyv = dup_keys(keyc, keyv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            tbp->keyc = keyc;
            tbp->keyv = newkeyv;
            return RDB_OK;
        case RDB_TB_INTERSECT:
            /* Copy keys */
            keyc = RDB_table_keys(tbp->var.intersect.tb1p, ecp, &keyv);
            if (keyc < 0)
                return RDB_ERROR;
            newkeyv = dup_keys(keyc, keyv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            tbp->keyc = keyc;
            tbp->keyv = newkeyv;
            return RDB_OK;
        case RDB_TB_JOIN:
            return infer_join_keys(tbp, ecp);
        case RDB_TB_EXTEND:
            /* Copy keys */
            keyc = RDB_table_keys(tbp->var.extend.tbp, ecp, &keyv);
            if (keyc < 0)
                return RDB_ERROR;
            newkeyv = dup_keys(keyc, keyv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            tbp->keyc = keyc;
            tbp->keyv = newkeyv;
            return RDB_OK;
        case RDB_TB_PROJECT:
            return infer_project_keys(tbp, ecp);
        case RDB_TB_SUMMARIZE:
            keyc = RDB_table_keys(tbp->var.summarize.tb2p, ecp, &keyv);
            if (keyc < 0)
                return RDB_ERROR;
            newkeyv = dup_keys(keyc, keyv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            tbp->keyc = keyc;
            tbp->keyv = newkeyv;
            return RDB_OK;
        case RDB_TB_RENAME:
            keyc = RDB_table_keys(tbp->var.rename.tbp, ecp, &keyv);
            if (keyc < 0)
                return RDB_ERROR;
            newkeyv = dup_rename_keys(keyc, keyv, tbp->var.rename.renc,
                    tbp->var.rename.renv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            tbp->keyc = keyc;
            tbp->keyv = newkeyv;
            return RDB_OK;
        case RDB_TB_GROUP:
            return infer_group_keys(tbp, ecp);
        case RDB_TB_SDIVIDE:
            /* Copy keys */
            keyc = RDB_table_keys(tbp->var.sdivide.tb1p, ecp, &keyv);
            if (keyc < 0)
                return RDB_ERROR;
            newkeyv = dup_keys(keyc, keyv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            tbp->keyc = keyc;
            tbp->keyv = newkeyv;
            return RDB_OK;
    }
    /* Must never be reached */
    abort();
}

RDB_bool
_RDB_table_refers(RDB_table *srctbp, RDB_table *dsttbp)
{
    int i;

    switch (srctbp->kind) {
        case RDB_TB_REAL:
            return (RDB_bool) (srctbp == dsttbp);
        case RDB_TB_SELECT:
            if (_RDB_expr_refers(srctbp->var.select.exp, dsttbp))
                return RDB_TRUE;
            return _RDB_table_refers(srctbp->var.select.tbp, dsttbp);
        case RDB_TB_UNION:
            if (_RDB_table_refers(srctbp->var._union.tb1p, dsttbp))
                return RDB_TRUE;
            return _RDB_table_refers(srctbp->var._union.tb2p, dsttbp);
        case RDB_TB_SEMIMINUS:
            if (_RDB_table_refers(srctbp->var.semiminus.tb1p, dsttbp))
                return RDB_TRUE;
            return _RDB_table_refers(srctbp->var.semiminus.tb2p, dsttbp);
        case RDB_TB_INTERSECT:
            if (_RDB_table_refers(srctbp->var.intersect.tb1p, dsttbp))
                return RDB_TRUE;
            return _RDB_table_refers(srctbp->var.intersect.tb2p, dsttbp);
        case RDB_TB_JOIN:
            if (_RDB_table_refers(srctbp->var.join.tb1p, dsttbp))
                return RDB_TRUE;
            return _RDB_table_refers(srctbp->var.join.tb2p, dsttbp);
        case RDB_TB_EXTEND:
            for (i = 0; i < srctbp->var.extend.attrc; i++) {
                if (_RDB_expr_refers(srctbp->var.extend.attrv[i].exp, dsttbp))
                    return RDB_TRUE;
            }
            return _RDB_table_refers(srctbp->var.extend.tbp, dsttbp);
        case RDB_TB_PROJECT:
            return _RDB_table_refers(srctbp->var.project.tbp, dsttbp);
        case RDB_TB_SUMMARIZE:
            for (i = 0; i < srctbp->var.summarize.addc; i++) {
                if (_RDB_expr_refers(srctbp->var.summarize.addv[i].exp, dsttbp))
                    return RDB_TRUE;
            }
            if (_RDB_table_refers(srctbp->var.summarize.tb1p, dsttbp))
                return RDB_TRUE;
            return _RDB_table_refers(srctbp->var.summarize.tb2p, dsttbp);
        case RDB_TB_RENAME:
            return _RDB_table_refers(srctbp->var.rename.tbp, dsttbp);
        case RDB_TB_WRAP:
            return _RDB_table_refers(srctbp->var.wrap.tbp, dsttbp);
        case RDB_TB_UNWRAP:
            return _RDB_table_refers(srctbp->var.unwrap.tbp, dsttbp);
        case RDB_TB_GROUP:
            return _RDB_table_refers(srctbp->var.group.tbp, dsttbp);
        case RDB_TB_UNGROUP:
            return _RDB_table_refers(srctbp->var.ungroup.tbp, dsttbp);
        case RDB_TB_SDIVIDE:
            if (_RDB_table_refers(srctbp->var.sdivide.tb1p, dsttbp))
                return RDB_TRUE;
            if (_RDB_table_refers(srctbp->var.sdivide.tb2p, dsttbp))
                return RDB_TRUE;
            return _RDB_table_refers(srctbp->var.sdivide.tb3p, dsttbp);
    }
    /* Must never be reached */
    abort();
}

RDB_object **
_RDB_index_objpv(_RDB_tbindex *indexp, RDB_expression *exp, RDB_type *tbtyp,
        int objpc, RDB_bool all_eq, RDB_bool asc)
{
    int i;
    RDB_expression *nodep;
    RDB_expression *attrexp;

    RDB_object **objpv = malloc(sizeof (RDB_object *) * objpc);
    if (objpv == NULL)
        return NULL;
    for (i = 0; i < objpc; i++) {
        nodep = _RDB_attr_node(exp, indexp->attrv[i].attrname, "=");
        if (nodep == NULL && !all_eq) {
            if (asc) {
                nodep = _RDB_attr_node(exp,
                        indexp->attrv[i].attrname, ">=");
                if (nodep == NULL)
                    nodep = _RDB_attr_node(exp, indexp->attrv[i].attrname, ">");
            } else {
                nodep = _RDB_attr_node(exp,
                        indexp->attrv[i].attrname, "<=");
                if (nodep == NULL)
                    nodep = _RDB_attr_node(exp, indexp->attrv[i].attrname, "<");
            }
        }
        attrexp = nodep;
        if (attrexp->kind == RDB_EX_RO_OP
                && strcmp (attrexp->var.op.name, "AND") == 0)
            attrexp = attrexp->var.op.argv[1];
        if (attrexp->var.op.argv[1]->var.obj.typ == NULL
                && (attrexp->var.op.argv[1]->var.obj.kind == RDB_OB_TUPLE
                || attrexp->var.op.argv[1]->var.obj.kind == RDB_OB_ARRAY))
            attrexp->var.op.argv[1]->var.obj.typ =
                    RDB_type_attr_type(tbtyp, indexp->attrv[i].attrname);

        objpv[i] = &attrexp->var.op.argv[1]->var.obj;       
    }
    return objpv;
}
