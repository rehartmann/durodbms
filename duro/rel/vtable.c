/*
 * $Id$
 *
 * Copyright (C) 2004-2006 René Hartmann.
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

RDB_object *
RDB_expr_to_vtable(RDB_expression *exp, RDB_exec_context *ecp,
    RDB_transaction *txp)
{
	/* Check exp ... */

    return _RDB_expr_to_vtable(exp, ecp, txp);
}

RDB_object *
_RDB_expr_to_vtable(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *newtbp = _RDB_new_obj(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->kind = RDB_OB_TABLE;

    /* Create type */
    newtbp->typ = RDB_expr_type(exp, NULL, ecp, txp);
    if (newtbp->typ == NULL) {
        free(newtbp);
        return NULL;
    }

    newtbp->var.tb.is_user = RDB_TRUE;
    newtbp->var.tb.is_persistent = RDB_FALSE;
    newtbp->var.tb.keyv = NULL;
    newtbp->var.tb.exp = exp;
    newtbp->var.tb.stp = NULL;
    newtbp->var.tb.name = NULL;

    return newtbp;
}

#ifdef NIX
static RDB_string_vec *
dup_keys(int keyc, RDB_string_vec *keyv)
{
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

RDB_object *
RDB_project(RDB_object *tbp, int attrc, char *attrv[], RDB_exec_context *ecp)
{
    RDB_object *newtbp = _RDB_new_table(ecp);
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

RDB_object *
RDB_remove(RDB_object *tbp, int attrc, char *attrv[], RDB_exec_context *ecp)
{
    int ret;
    int i, j;
    char **resattrv;
    RDB_object *newtbp = NULL;
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

RDB_object *
RDB_summarize(RDB_object *tb1p, RDB_object *tb2p, int addc,
        const RDB_summarize_add addv[], RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *newtbp;
    int i, ai;

    /* Additional (internal) attribute for each AVG */
    int avgc;
    char **avgv;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return NULL;
    }

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

RDB_object *
RDB_rename(RDB_object *tbp, int renc, const RDB_renaming renv[],
           RDB_exec_context *ecp)
{
    RDB_object *newtbp;
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

RDB_object *
RDB_wrap(RDB_object *tbp, int wrapc, const RDB_wrapping wrapv[],
         RDB_exec_context *ecp)
{
    RDB_object *newtbp;
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

RDB_object *
RDB_unwrap(RDB_object *tbp, int attrc, char *attrv[], RDB_exec_context *ecp)
{
    RDB_object *newtbp;

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

RDB_object *
RDB_sdivide(RDB_object *tb1p, RDB_object *tb2p, RDB_object *tb3p,
        RDB_exec_context *ecp)
{
    RDB_type *typ;
    RDB_object *newtbp;

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

RDB_object *
RDB_group(RDB_object *tbp, int attrc, char *attrv[], const char *gattr,
        RDB_exec_context *ecp)
{
    int i;
    RDB_object *newtbp = _RDB_new_table(ecp);
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

RDB_object *
RDB_ungroup(RDB_object *tbp, const char *attr, RDB_exec_context *ecp)
{
    RDB_object *newtbp = _RDB_new_table(ecp);
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

static RDB_object *
dup_extend(RDB_object *tbp, RDB_exec_context *ecp)
{
    int i;
    int attrc = tbp->var.extend.attrc;
    RDB_object *newtbp = _RDB_new_table(ecp);
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

RDB_object *
_RDB_dup_vtable(RDB_object *tbp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_object *tb1p;
    RDB_object *tb2p;
    RDB_object *tb3p;
    RDB_object *ntbp;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            if (tbp->name == NULL) {
                RDB_object *ntbp = RDB_create_table_from_type(NULL, RDB_FALSE,
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
        case RDB_TB_SEMIJOIN:
            tb1p = _RDB_dup_vtable(tbp->var.semijoin.tb1p, ecp);
            if (tb1p == NULL)
                return NULL;
            tb2p = _RDB_dup_vtable(tbp->var.semijoin.tb2p, ecp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            return RDB_semijoin(tb1p, tb2p, ecp);
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
#endif

RDB_bool
_RDB_table_def_equals(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
	int ret;
	RDB_bool res;
	
    if (tb1p == tb2p)
        return RDB_TRUE;

    if (tb1p->var.tb.exp == NULL || tb2p->var.tb.exp == NULL)
        return RDB_FALSE;

    ret = _RDB_expr_equals(tb1p->var.tb.exp,
                    tb2p->var.tb.exp, ecp, txp, &res);
    if (ret != RDB_OK)
       return RDB_FALSE;
    return res;
}

static int
check_keyloss(RDB_expression *exp, int attrc, RDB_attr attrv[], RDB_bool presv[],
        RDB_exec_context *ecp)
{
    int i, j, k;
    int count = 0;
    RDB_string_vec *keyv;
    int keyc = _RDB_infer_keys(exp, ecp, &keyv); /* !! */
    if (keyc == RDB_ERROR)
        return RDB_ERROR;

    for (i = 0; i < keyc; i++) {
        for (j = 0; j < keyv[i].strc; j++) {
            /* Search for key attribute in attrv */
            for (k = 0;
                 (k < attrc) && (strcmp(keyv[i].strv[j], attrv[k].name) != 0);
                 k++);
            /* If not found, exit loop */
            if (k >= attrc)
                break;
        }
        /* If the loop didn't terminate prematurely, the key is preserved */
        presv[i] = (RDB_bool) (j >= keyv[i].strc);
        if (presv[i])
            count++;
    }
    return count;
}

static RDB_string_vec *
all_key(RDB_expression *exp, RDB_exec_context *ecp)
{
    int attrc;
    int i;
    RDB_type *tbtyp = RDB_expr_type(exp, NULL, ecp, NULL);
    RDB_string_vec *keyv = malloc(sizeof (RDB_string_vec));
    if (keyv == NULL)
        return NULL;

    attrc = keyv[0].strc =
            tbtyp->var.basetyp->var.tuple.attrc;
    keyv[0].strv = malloc(sizeof(char *) * attrc);
    if (keyv[0].strv == NULL) {
        free(keyv);
        return NULL;
    }
    for (i = 0; i < attrc; i++)
        keyv[0].strv[i] = NULL;
    for (i = 0; i < attrc; i++) {
        keyv[0].strv[i] = RDB_dup_str(
                tbtyp->var.basetyp->var.tuple.attrv[i].name);
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

static int
infer_join_keys(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_string_vec **keyvp)
{
    int i, j, k;
    int keyc1, keyc2;
    int newkeyc;
    RDB_string_vec *keyv1, *keyv2;
    RDB_string_vec *newkeyv;

    keyc1 = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv1);
    if (keyc1 < 0)
        return keyc1;
    keyc2 = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv2);
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
    *keyvp = newkeyv;
    return newkeyc;

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
infer_project_keys(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_string_vec **keyvp)
{
    int keyc;
    int newkeyc;
    RDB_string_vec *keyv;
    RDB_string_vec *newkeyv;
    RDB_bool *presv;
    RDB_bool keyloss;
    RDB_type *tbtyp = RDB_expr_type(exp, NULL, ecp, NULL);
    
    keyc = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv);
    if (keyc < 0)
        return keyc;

    presv = malloc(sizeof(RDB_bool) * keyc);
    if (presv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    newkeyc = check_keyloss(exp->var.op.argv[0],
            tbtyp->var.basetyp->var.tuple.attrc,
            tbtyp->var.basetyp->var.tuple.attrv, presv, ecp);
    keyloss = (RDB_bool) (newkeyc == 0);
    if (keyloss) {
        /* Table is all-key */
        newkeyc = 1;
        newkeyv = all_key(exp, ecp);
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
    *keyvp = newkeyv;
    return keyc;
}

static int
infer_group_keys(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_string_vec **keyvp)
{
    int i, j;
    RDB_string_vec *newkeyv;
    RDB_type *tbtyp = RDB_expr_type(exp, NULL, ecp, NULL);

    /*
     * Key consists of all attributes which are not grouped
     */    
    newkeyv = malloc(sizeof(RDB_string_vec));
    if (newkeyv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    newkeyv[0].strc = tbtyp->var.basetyp->var.tuple.attrc - 1;
    newkeyv[0].strv = malloc(sizeof (char *) * newkeyv[0].strc);
    if (newkeyv[0].strv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    j = 0;
    for (i = 0; i < tbtyp->var.basetyp->var.tuple.attrc; i++) {
        if (strcmp(tbtyp->var.basetyp->var.tuple.attrv[i].name,
                RDB_obj_string(&exp->var.op.argv[1]->var.obj)) != 0) {
            newkeyv[0].strv[j] = RDB_dup_str(
                    tbtyp->var.basetyp->var.tuple.attrv[i].name);
            if (newkeyv[0].strv[j] == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            j++;
        }
    }

    *keyvp = newkeyv;
    return 1;
}

int
_RDB_infer_keys(RDB_expression *exp, RDB_exec_context *ecp,
       RDB_string_vec **keyvp)
{
    if ((strcmp(exp->var.op.name, "WHERE") == 0)
            || (strcmp(exp->var.op.name, "SEMIMINUS") == 0)
            || (strcmp(exp->var.op.name, "SEMIJOIN") == 0)
            || (strcmp(exp->var.op.name, "EXTEND") == 0)
            || (strcmp(exp->var.op.name, "SDIVIDE") == 0)) {
        return _RDB_infer_keys(exp->var.op.argv[0], ecp, keyvp);
    }
    if ((strcmp(exp->var.op.name, "UNION") == 0)
            || (strcmp(exp->var.op.name, "WRAP") == 0)
            || (strcmp(exp->var.op.name, "UNWRAP") == 0)
            || (strcmp(exp->var.op.name, "UNGROUP") == 0)) {
        *keyvp = all_key(exp, ecp);
        if (*keyvp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        return 1;
    }
    if (strcmp(exp->var.op.name, "JOIN") == 0) {
    	return infer_join_keys(exp, ecp, keyvp);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
    	return infer_project_keys(exp, ecp, keyvp);
    }
    if (strcmp(exp->var.op.name, "SUMMARIZE") == 0) {
        return _RDB_infer_keys(exp->var.op.argv[1], ecp, keyvp);
    }
    if (strcmp(exp->var.op.name, "RENAME") == 0) {
        int keyc = _RDB_infer_keys(exp->var.op.argv[0], ecp, keyvp);
        /* !!
            newkeyv = dup_rename_keys(keyc, keyv, tbp->var.rename.renc,
                    tbp->var.rename.renv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            *keyvp = newkeyv;
        */
        return keyc;
    }
    if (strcmp(exp->var.op.name, "GROUP") == 0) {
        return infer_group_keys(exp, ecp, keyvp);
    }
    /* Must never be reached */
    abort();
}

#ifdef NIX
int
_RDB_infer_keys(RDB_object *tbp, RDB_exec_context *ecp)
{
	int keyc;
    RDB_string_vec *keyv;
	RDB_expression *exp = tbp->var.tb.exp;
    if (exp == NULL) {
        RDB_raise_invalid_argument("table is real", ecp);
        return RDB_ERROR;
    }

	keyc = _RDB_infer_keys(tbp->var.tb.exp, ecp, &keyv);
	if (keyc == RDB_ERROR)
	    return RDB_ERROR;
	tbp->var.tb.keyc = keyc;
	tbp->var.tb.keyv = keyv;
	return RDB_OK;
}
#endif

RDB_bool
_RDB_table_refers(const RDB_object *srctbp, const RDB_object *dsttbp)
{
    if (srctbp == dsttbp)
        return RDB_TRUE;
	if (srctbp->var.tb.exp == NULL)
	    return RDB_FALSE;
	return _RDB_expr_refers(srctbp->var.tb.exp, dsttbp);
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
