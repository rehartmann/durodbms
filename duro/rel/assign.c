/*
 * $Id$
 *
 * Copyright (C) 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

static RDB_bool
exp_refers_target(const RDB_expression *exp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[])
{
    int i;

    for (i = 0; i < insc; i++) {
        if (_RDB_expr_refers(exp, insv[i].tbp))
            return RDB_TRUE;
    }
    for (i = 0; i < updc; i++) {
        if (_RDB_expr_refers(exp, updv[i].tbp))
            return RDB_TRUE;
    }
    for (i = 0; i < delc; i++) {
        if (_RDB_expr_refers(exp, delv[i].tbp))
            return RDB_TRUE;
    }
    /* !! copyv */
    return RDB_FALSE;
}

static RDB_table *
replace_targets_tb(RDB_table *tbp, int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[], RDB_transaction *);

static RDB_table *
replace_targets_extend(RDB_table *tbp, int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[], RDB_transaction *txp)
{
    int i;
    int attrc = tbp->var.extend.attrc;
    RDB_table *newtbp = _RDB_new_table();
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_EXTEND;
    newtbp->var.extend.tbp = replace_targets_tb(tbp->var.extend.tbp,
            insc, insv, updc, updv, delc, delv, copyc, copyv, txp);
    if (newtbp->var.extend.tbp == NULL)
        goto error;
    newtbp->var.extend.attrc = attrc;
    newtbp->var.extend.attrv = malloc(sizeof(RDB_virtual_attr) * attrc);
    if (newtbp->var.extend.attrv == NULL) {
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
            goto error;
        }
        newtbp->var.extend.attrv[i].exp =
                RDB_dup_expr(tbp->var.extend.attrv[i].exp);
        if (newtbp->var.extend.attrv[i].exp == NULL) {
            goto error;
        }
    }
    newtbp->typ = _RDB_dup_nonscalar_type(tbp->typ);
    if (newtbp->typ == NULL) {
        goto error;
    }
    return newtbp;

error:
    if (newtbp->var.extend.tbp != NULL
            && newtbp->var.extend.tbp->kind != RDB_TB_REAL) {
        RDB_drop_table(newtbp->var.extend.tbp, NULL);
    }
    if (newtbp->var.extend.attrv != NULL) {
        for (i = 0; i < attrc; i++) {
            free(newtbp->var.extend.attrv[i].name);
            if (newtbp->var.extend.attrv[i].exp != NULL)
                RDB_drop_expr(newtbp->var.extend.attrv[i].exp);
        }
    }
    free(newtbp);
    return NULL;
}

static RDB_table *
replace_targets_summarize(RDB_table *tbp, int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[], RDB_transaction *txp)
{
    RDB_table *newtbp;
    int i;
    int addc = tbp->var.summarize.addc;

    newtbp = _RDB_new_table();
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_SUMMARIZE;
    newtbp->var.summarize.tb1p = replace_targets_tb(tbp->var.summarize.tb1p,
            insc, insv, updc, updv, delc, delv, copyc, copyv, txp);
    if (newtbp->var.summarize.tb1p == NULL) {
        free(newtbp);
        return NULL;
    }
    newtbp->var.summarize.tb2p = replace_targets_tb(tbp->var.summarize.tb2p,
            insc, insv, updc, updv, delc, delv, copyc, copyv, txp);
    if (newtbp->var.summarize.tb2p == NULL) {
        free(newtbp);
        return NULL;
    }
    newtbp->typ = _RDB_dup_nonscalar_type(tbp->typ);
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
                    tbp->var.summarize.addv[i].exp);
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
            RDB_drop_expr(newtbp->var.summarize.addv[i].exp);
    }
    free(newtbp);
    return NULL;
}

static RDB_table *
replace_updattrs(RDB_table *tbp, int updc, RDB_attr_update updv[],
        RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_table *tb1p, *tb2p;
    RDB_table *tb3p = NULL;
    RDB_virtual_attr *vattrv;
    char **attrv = NULL;
    RDB_renaming *renv = NULL;

    /*
     * Add 'updated' attributes
     */
    vattrv = malloc(sizeof(RDB_virtual_attr) * updc);
    if (vattrv == NULL)
        return NULL;
    for (i = 0; i < updc; i++)
        vattrv[i].name = NULL;
    for (i = 0; i < updc; i++) {
        vattrv[i].name = malloc(strlen(updv[i].name) + 2);
        if (vattrv[i].name == NULL)
            goto cleanup;
        vattrv[i].name[0] = '$';
        strcpy(vattrv[i].name + 1, updv[i].name);
        vattrv[i].exp = updv[i].exp;
    }
    ret = _RDB_extend(tbp, updc, vattrv, txp, &tb1p);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    /*
     * Remove old attributes
     */
    attrv = malloc(sizeof(char *) * updc);
    if (attrv == NULL) {
        RDB_drop_table(tb1p, NULL);
        goto cleanup;
    }
    for (i = 0; i < updc; i++) {
        attrv[i] = updv[i].name;
    }
    ret = RDB_remove(tb1p, updc, attrv, &tb2p);
    if (ret != RDB_OK) {
        RDB_drop_table(tb1p, NULL);
        goto cleanup;
    }

    /*
     * Rename new attributes
     */
    renv = malloc(sizeof (RDB_renaming) * updc);
    if (renv == NULL) {
        RDB_drop_table(tb2p, NULL);
        goto cleanup;
    }
    for (i = 0; i < updc; i++) {
        renv[i].from = vattrv[i].name;
        renv[i].to = updv[i].name;
    }
    ret = RDB_rename(tb2p, updc, renv, &tb3p);
    if (ret != RDB_OK) {
        RDB_drop_table(tb2p, NULL);
        tb3p = NULL;
    }

cleanup:
    for (i = 0; i < updc; i++)
        free(vattrv[i].name);
    free(vattrv);
    free(attrv);
    free(renv);

    return tb3p;
}

static RDB_table *
replace_targets_real(RDB_table *tbp, int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[], RDB_transaction *txp)
{
    int i;
    int ret;

    for (i = 0; i < insc; i++) {
        if (insv[i].tbp == tbp) {
            RDB_table *tb1p, *tb2p;

            ret = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
                    0, NULL, NULL, &tb1p);
            if (ret != RDB_OK)
                return NULL;
            ret = RDB_insert(tb1p, insv[i].tplp, NULL);
            if (ret != RDB_OK)
                return NULL;
            ret = RDB_union(tbp, tb1p, &tb2p);
            if (ret != RDB_OK) {
                RDB_drop_table(tb1p, NULL);
                return NULL;
            }
            return tb2p;
        }
    }
    for (i = 0; i < updc; i++) {
        if (updv[i].tbp == tbp) {
            if (updv[i].condp == NULL) {
                return replace_updattrs(tbp, updv[i].updc, updv[i].updv, txp);
            } else {
                RDB_expression *ucondp, *nucondp;
                RDB_table *utbp, *nutbp;

                ucondp = RDB_dup_expr(updv[i].condp);
                if (ucondp == NULL)
                    return NULL;
                nucondp = RDB_ro_op_va("NOT", RDB_dup_expr(updv[i].condp),
                        (RDB_expression *) NULL);
                if (nucondp == NULL) {
                    RDB_drop_expr(ucondp);
                    return NULL;
                }
                ret = _RDB_select(tbp, ucondp, &utbp);
                if (ret != RDB_OK) {
                    RDB_drop_expr(ucondp);
                    RDB_drop_expr(nucondp);
                    return NULL;
                }

                ret = _RDB_select(tbp, nucondp, &nutbp);
                if (ret != RDB_OK) {
                    RDB_drop_expr(ucondp);
                    RDB_drop_table(utbp, NULL);
                    return NULL;
                }

                tbp = replace_updattrs(utbp, updv[i].updc, updv[i].updv, txp);
                if (tbp == NULL) {
                    RDB_drop_table(utbp, NULL);
                    RDB_drop_table(nutbp, NULL);
                    return NULL;
                }
                utbp = tbp;
                    
                ret = RDB_union(utbp, nutbp, &tbp);
                if (ret != RDB_OK) {
                    RDB_drop_table(utbp, NULL);
                    RDB_drop_table(nutbp, NULL);
                    return NULL;
                }
                    
                return tbp;
            }
        }
    }
    for (i = 0; i < delc; i++) {
        if (delv[i].tbp == tbp) {
            RDB_table *tb1p, *tb2p;
            RDB_expression *exp = NULL;
            
            if (delv[i].condp != NULL) {
                exp = RDB_dup_expr(delv[i].condp);
                if (exp == NULL)
                    return NULL;

                ret = _RDB_select(tbp, exp, &tb1p);
                if (ret != RDB_OK) {
                    RDB_drop_expr(exp);
                    return NULL;
                }
                ret = RDB_minus(tbp, tb1p, &tb2p);
                if (ret != RDB_OK) {
                    RDB_drop_table(tb1p, NULL);
                    return NULL;
                }
                return tb2p;
            }
            /* Expression is NULL - table will become empty */
            ret = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
                    0, NULL, NULL, &tb1p);
            if (ret != RDB_OK)
                return NULL;
            return tb1p;
        }
    }
    return tbp;
}

/*
 * Replace all child tables of tbp which are assignment targets
 * by the assignment sources.
 */
static RDB_table *
replace_targets_tb(RDB_table *tbp, int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[], RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_table *tb1p;
    RDB_table *tb2p;
    RDB_table *tb3p;
    RDB_table *ntbp;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            return replace_targets_real(tbp, insc, insv, updc, updv, delc, delv,
                    copyc, copyv, txp);
        case RDB_TB_SELECT:
        {
            RDB_expression *exp = RDB_dup_expr(tbp->var.select.exp);
            if (exp == NULL)
                return NULL;

            tb1p = replace_targets_tb(tbp->var.select.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL) {
                RDB_drop_expr(exp);
                return NULL;
            }

            ret = _RDB_select(tb1p, exp, &tbp);
            if (ret != RDB_OK) {
                if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, NULL);
                RDB_drop_expr(exp);
                return NULL;
            }
            return tbp;
        }
        case RDB_TB_UNION:
            tb1p = replace_targets_tb(tbp->var._union.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var._union.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb2p == NULL) {
                if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, NULL);
                return NULL;
            }
            ret = RDB_union(tb1p, tb2p, &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
        case RDB_TB_MINUS:
            tb1p = replace_targets_tb(tbp->var.minus.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var.minus.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, NULL);
                return NULL;
            }
            ret = RDB_minus(tb1p, tb2p, &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
        case RDB_TB_INTERSECT:
            tb1p = replace_targets_tb(tbp->var.intersect.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var.intersect.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, NULL);
                return NULL;
            }
            ret = RDB_intersect(tb1p, tb2p, &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
        case RDB_TB_JOIN:
            tb1p = replace_targets_tb(tbp->var.join.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var.join.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, NULL);
                return NULL;
            }
            ret = RDB_join(tb1p, tb2p, &ntbp);
            if (ret != RDB_OK)
                return NULL;
            return ntbp;
        case RDB_TB_EXTEND:
            return replace_targets_extend(tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
        case RDB_TB_PROJECT:
        {
            char **attrnamev;
            int attrc = tbp->typ->var.basetyp->var.tuple.attrc;

            tb1p = replace_targets_tb(tbp->var.project.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            attrnamev = malloc(attrc * sizeof(char *));
            if (attrnamev == NULL)
                return NULL;
            for (i = 0; i < attrc; i++) {
                attrnamev[i] = tbp->typ->var.basetyp->var.tuple.attrv[i].name;
            }
            ret = RDB_project(tb1p, attrc, attrnamev, &ntbp);
            free(attrnamev);
            if (ret != RDB_OK)
                return NULL;
            ntbp->var.project.indexp = tbp->var.project.indexp;
            return ntbp;
        }
        case RDB_TB_SUMMARIZE:
            return replace_targets_summarize(tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
        case RDB_TB_RENAME:
            tb1p = replace_targets_tb(tbp->var.rename.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            ret = RDB_rename(tb1p, tbp->var.rename.renc, tbp->var.rename.renv,
                    &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
        case RDB_TB_WRAP:
            tb1p = replace_targets_tb(tbp->var.wrap.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            ret = RDB_wrap(tb1p, tbp->var.wrap.wrapc, tbp->var.wrap.wrapv,
                    &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
        case RDB_TB_UNWRAP:
            tb1p = replace_targets_tb(tbp->var.unwrap.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            ret = RDB_unwrap(tb1p, tbp->var.unwrap.attrc, tbp->var.unwrap.attrv,
                    &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
        case RDB_TB_GROUP:
            tb1p = replace_targets_tb(tbp->var.group.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            ret = RDB_group(tb1p, tbp->var.group.attrc, tbp->var.group.attrv,
                    tbp->var.group.gattr, &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
        case RDB_TB_UNGROUP:
            tb1p = replace_targets_tb(tbp->var.ungroup.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            ret = RDB_ungroup(tb1p, tbp->var.ungroup.attr, &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
        case RDB_TB_SDIVIDE:
            tb1p = replace_targets_tb(tbp->var.sdivide.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var.sdivide.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, NULL);
                return NULL;
            }
            tb3p = replace_targets_tb(tbp->var.sdivide.tb3p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (tb3p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, NULL);
                 if (tb2p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, NULL);
                return NULL;
            }
            ret = RDB_sdivide(tb1p, tb2p, tb3p, &tbp);
            if (ret != RDB_OK)
                return NULL;
            return tbp;
    }

    /* Must never be reached */
    abort();
}

static RDB_expression *
replace_targets(RDB_expression *exp, int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[], RDB_transaction *txp)
{
    RDB_expression *newexp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = replace_targets(exp->var.op.argv[0], insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (newexp == NULL)
                return NULL;
            return RDB_tuple_attr(newexp, exp->var.op.name);
        case RDB_EX_GET_COMP:
            newexp = replace_targets(exp->var.op.argv[0], insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_comp(newexp, exp->var.op.name);
        case RDB_EX_RO_OP:
        {
            int i;
            RDB_expression **argexpv = (RDB_expression **)
                    malloc(sizeof (RDB_expression *) * exp->var.op.argc);

            if (argexpv == NULL)
                return NULL;

            for (i = 0; i < exp->var.op.argc; i++) {
                argexpv[i] = replace_targets(exp->var.op.argv[i], insc, insv,
                        updc, updv, delc, delv, copyc, copyv, txp);
                if (argexpv[i] == NULL)
                    return NULL;
            }
            newexp = RDB_ro_op(exp->var.op.name, exp->var.op.argc, argexpv);
            free(argexpv);
            return newexp;
        }
        case RDB_EX_AGGREGATE:
            newexp = replace_targets(exp->var.op.argv[0], insc, insv,
                    updc, updv, delc, delv, copyc, copyv, txp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_aggregate(newexp, exp->var.op.op,
                    exp->var.op.name);
        case RDB_EX_OBJ:
            if (exp->var.obj.kind == RDB_OB_TABLE) {
                RDB_table *newtbp = replace_targets_tb(exp->var.obj.var.tbp,
                        insc, insv, updc, updv, delc, delv, copyc, copyv, txp);
                if (newtbp == NULL)
                    return NULL;
                return RDB_table_to_expr(newtbp);
            }
            return RDB_obj_to_expr(&exp->var.obj);
        case RDB_EX_ATTR:
            return RDB_expr_attr(exp->var.attrname);
    }
    abort();
}

typedef struct insert_node {
    RDB_ma_insert ins;
    struct insert_node *nextp;
} insert_node;

static void
concat_inslists (insert_node **dstpp, insert_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        insert_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static int
check_extend_tuple(RDB_object *tplp, RDB_table *tbp, RDB_transaction *txp)
{
    int i;
    int ret;

    /* Check the additional attributes */
    for (i = 0; i < tbp->var.extend.attrc; i++) {
        RDB_virtual_attr *vattrp = &tbp->var.extend.attrv[i];
        RDB_object val;
        RDB_object *valp;
        RDB_bool iseq;
        
        valp = RDB_tuple_get(tplp, vattrp->name);
        if (valp == NULL) {
            return RDB_INVALID_ARGUMENT;
        }
        RDB_init_obj(&val);
        ret = RDB_evaluate(vattrp->exp, tplp, txp, &val);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&val);
            return ret;
        }
        ret = RDB_obj_equals(&val, valp, txp, &iseq);
        RDB_destroy_obj(&val);
        if (ret != RDB_OK)
            return ret;
        if (!iseq)
            return RDB_PREDICATE_VIOLATION;
    }
    return RDB_OK;
}

static insert_node *
new_insert_node(RDB_table *tbp, const RDB_object *tplp)
{
    int ret;

    insert_node *insnp = malloc(sizeof (insert_node));
    if (insnp == NULL)
        return NULL;
    insnp->ins.tplp = malloc(sizeof(RDB_object));
    if (insnp->ins.tplp == NULL) {
        free(insnp);
        return NULL;
    }
    RDB_init_obj(insnp->ins.tplp);
    ret = _RDB_copy_tuple(insnp->ins.tplp, tplp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(insnp->ins.tplp);
        free(insnp->ins.tplp);
        free(insnp);
        return NULL;
    }
    insnp->ins.tbp = tbp;
    insnp->nextp = NULL;
    return insnp;
}

static void
del_inslist(insert_node *insnp)
{
    insert_node *hinsnp;

    while (insnp != NULL) {
        hinsnp = insnp->nextp;

        RDB_destroy_obj(insnp->ins.tplp);
        free(insnp->ins.tplp);
        free(insnp);
        insnp = hinsnp;
    }    
}

static int
resolve_insert(const RDB_ma_insert *insp, insert_node **inslpp,
               RDB_transaction *txp)
{
    int ret, ret2;
    RDB_bool b;
    RDB_ma_insert ins;
    RDB_object tpl;

    switch (insp->tbp->kind) {
        case RDB_TB_REAL:
            *inslpp = new_insert_node(insp->tbp, insp->tplp);
            if (*inslpp == NULL)
                return RDB_NO_MEMORY;
            ret = RDB_OK;
            break;
        case RDB_TB_SELECT:
            ret = RDB_evaluate_bool(insp->tbp->var.select.exp, insp->tplp, txp, &b);
            if (ret != RDB_OK)
                break;
            if (!b) {
                ret = RDB_PREDICATE_VIOLATION;
                break;
            }
            ins.tbp = insp->tbp->var.select.tbp;
            ins.tplp = insp->tplp;
            ret = resolve_insert(&ins, inslpp, txp);
            break;
        case RDB_TB_PROJECT:
            ins.tbp = insp->tbp->var.project.tbp;
            ins.tplp = insp->tplp;
            ret = resolve_insert(&ins, inslpp, txp);
            break;
        case RDB_TB_INTERSECT:
            ret = RDB_table_contains(insp->tbp->var.intersect.tb1p, insp->tplp,
                    txp);
            if (ret != RDB_OK && ret != RDB_NOT_FOUND)
                return ret;
            ret2 = RDB_table_contains(insp->tbp->var.intersect.tb2p,
                    insp->tplp, txp);
            if (ret2 != RDB_OK && ret2 != RDB_NOT_FOUND)
                return ret2;

            /*
             * If both 'subtables' contain the tuple, the insert fails
             */
            if (ret == RDB_OK && ret2 == RDB_OK)
                return RDB_ELEMENT_EXISTS;

            /*
             * Insert the tuple into the table(s) which do not contain it
             */
            *inslpp = NULL;
            if (ret == RDB_NOT_FOUND) {
                ins.tbp = insp->tbp->var.intersect.tb1p;
                ins.tplp = insp->tplp;
                ret = resolve_insert(&ins, inslpp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            if (ret2 == RDB_NOT_FOUND) {
                insert_node *hinsnp;

                ins.tbp = insp->tbp->var.intersect.tb2p;
                ins.tplp = insp->tplp;
                ret = resolve_insert(&ins, &hinsnp, txp);
                if (ret != RDB_OK) {
                    if (*inslpp != NULL)
                        del_inslist(*inslpp);
                    return ret;
                }
                if (*inslpp == NULL) {
                    *inslpp = hinsnp;
                } else {
                    concat_inslists(inslpp, hinsnp);
                }
            }
            ret = RDB_OK;
            break;
        case RDB_TB_RENAME:
            RDB_init_obj(&tpl);
            ret = _RDB_invrename_tuple(insp->tplp,
                    insp->tbp->var.rename.renc, insp->tbp->var.rename.renv,
                    &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }
            ins.tbp = insp->tbp->var.wrap.tbp;
            ins.tplp = &tpl;
            ret = resolve_insert(&ins, inslpp, txp);
            RDB_destroy_obj(&tpl);
            break;
        case RDB_TB_EXTEND:
            ret = check_extend_tuple(insp->tplp, insp->tbp, txp);
            if (ret != RDB_OK)
                return ret;
            ins.tbp = insp->tbp->var.extend.tbp;
            ins.tplp = insp->tplp;
            ret = resolve_insert(&ins, inslpp, txp);
            break;
        case RDB_TB_WRAP:
            RDB_init_obj(&tpl);
            ret = _RDB_invwrap_tuple(insp->tplp, insp->tbp->var.wrap.wrapc,
                    insp->tbp->var.wrap.wrapv, &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }
            ins.tbp = insp->tbp->var.wrap.tbp;
            ins.tplp = &tpl;
            ret = resolve_insert(&ins, inslpp, txp);
            RDB_destroy_obj(&tpl);
            break;
        case RDB_TB_UNWRAP:
            RDB_init_obj(&tpl);
            ret = _RDB_invunwrap_tuple(insp->tplp,
                    insp->tbp->var.unwrap.attrc, insp->tbp->var.unwrap.attrv,
                    insp->tbp->var.unwrap.tbp->typ->var.basetyp, &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }
            ins.tbp = insp->tbp->var.wrap.tbp;
            ins.tplp = &tpl;
            ret = resolve_insert(&ins, inslpp, txp);
            RDB_destroy_obj(&tpl);
            break;
        default:
            ret = RDB_NOT_SUPPORTED;
    }
    return ret;
}

int
RDB_multi_assign(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_transaction *txp)
{
    int i, j;
    int ret;
    RDB_dbroot *dbrootp;

    /* List of inserts generated from inserts into virtual tables */
    insert_node *geninsnp = NULL;

    insert_node *insnp;

    if (copyc > 0)
        return RDB_NOT_SUPPORTED;

    /*
     * Check if conditions are of type BOOLEAN
     */
    /* ... */

    /*
     * Resolve virtual table assignment
     */
    for (i = 0; i < insc; i++) {
        if (insv[i].tbp->kind != RDB_TB_REAL) {
            ret = resolve_insert(&insv[i], &insnp, txp);
            if (ret != RDB_OK)
                goto cleanup;

            /* Add inserts to list */
            concat_inslists(&geninsnp, insnp);
        }
    }
    for (i = 0; i < updc; i++) {
        if (updv[i].tbp->kind != RDB_TB_REAL)
            return RDB_NOT_SUPPORTED;
    }
    for (i = 0; i < delc; i++) {
        if (delv[i].tbp->kind != RDB_TB_REAL)
            return RDB_NOT_SUPPORTED;
    }

    /*
     * Check if the same target is assigned twice
     */
    for (i = 0; i < insc; i++) {
        for (j = i + 1; j < insc; j++) {
            if (insv[i].tbp == insv[j].tbp)
                return RDB_INVALID_ARGUMENT;
        }
        for (j = 0; j < updc; j++) {
            if (insv[i].tbp == updv[j].tbp)
                return RDB_INVALID_ARGUMENT;
        }
        for (j = 0; j < delc; j++) {
            if (insv[i].tbp == delv[j].tbp)
                return RDB_NOT_SUPPORTED;
        }
    }
    for (i = 0; i < updc; i++) {
        for (j = i + 1; j < updc; j++) {
            if (updv[i].tbp == updv[j].tbp)
                return RDB_INVALID_ARGUMENT;
        }
        for (j = 0; j < delc; j++) {
            if (updv[i].tbp == delv[j].tbp)
                return RDB_INVALID_ARGUMENT;
        }
    }
    for (i = 0; i < delc; i++) {
        for (j = i + 1; j < delc; j++) {
            if (delv[i].tbp == delv[j].tbp)
                return RDB_INVALID_ARGUMENT;
        }
    }

    /*
     * Check for dependencies
     */

    /* ... */

    /*
     * Check constraints
     */
    if (txp != NULL) {
        RDB_constraint *constrp;

        dbrootp = RDB_tx_db(txp)->dbrootp;

        if (!dbrootp->constraints_read) {
            ret = _RDB_read_constraints(txp);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            dbrootp->constraints_read = RDB_TRUE;
        }

        constrp = dbrootp->first_constrp;
        while (constrp != NULL) {
            /*
             * Check if constraint refers to assignment target
             */
            if (exp_refers_target(constrp->exp, insc, insv, updc, updv,
                    delc, delv, copyc, copyv)) {
                RDB_bool b;
                /*
                 * Replace target tables
                 */
                RDB_expression *newexp = replace_targets(constrp->exp,
                        insc, insv, updc, updv, delc, delv, copyc, copyv, txp);
                if (newexp == NULL) {
                    ret = RDB_NO_MEMORY;
                    goto cleanup;
                }
                    
                /*
                 * Check constraint
                 */
                ret = RDB_evaluate_bool(newexp, NULL, txp, &b);
                RDB_drop_expr(newexp);
                if (ret != RDB_OK)
                    goto cleanup;
                if (!b) {
                    ret = RDB_PREDICATE_VIOLATION;
                    goto cleanup;
                }
            }
            constrp = constrp->nextp;
        }
    }

    /*
     * Execute assignments
     */
    for (i = 0; i < insc; i++) {
        if (insv[i].tbp->kind == RDB_TB_REAL) {
            ret = _RDB_insert_real(insv[i].tbp, insv[i].tplp, txp);
            if (ret != RDB_OK)
                goto cleanup;
        }
    }
    for (i = 0; i < updc; i++) {
        ret = _RDB_update_real(updv[i].tbp, updv[i].condp,
                 updv[i].updc, updv[i].updv, txp);
        if (ret != RDB_OK)
            goto cleanup;
    }
    for (i = 0; i < delc; i++) {
        ret = _RDB_delete_real(delv[i].tbp, delv[i].condp, txp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    insnp = geninsnp;
    while (insnp != NULL) {
        ret = _RDB_insert_real(insnp->ins.tbp, insnp->ins.tplp, txp);
        if (ret != RDB_OK)
            goto cleanup;
        insnp = insnp->nextp;
    }

    ret = RDB_OK;

cleanup:
    del_inslist(geninsnp);

    return ret;
}
