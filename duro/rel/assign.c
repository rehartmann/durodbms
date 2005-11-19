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
#include <assert.h>

static RDB_bool
expr_refers_target(const RDB_expression *exp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[])
{
    int i;

    for (i = 0; i < insc; i++) {
        if (_RDB_expr_table_depend(exp, insv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < updc; i++) {
        if (_RDB_expr_table_depend(exp, updv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < delc; i++) {
        if (_RDB_expr_table_depend(exp, delv[i].tbp))
            return RDB_TRUE;
    }

    for (i = 0; i < copyc; i++) {
        if (exp->kind == RDB_EX_OBJ) {
            RDB_table *tbp = RDB_obj_table(&exp->var.obj);
            if (tbp != NULL && _RDB_expr_table_depend(exp, tbp))
                return RDB_TRUE;
        }
    }

    return RDB_FALSE;
}

typedef struct insert_node {
    RDB_ma_insert ins;
    struct insert_node *nextp;
} insert_node;

typedef struct update_node {
    RDB_ma_update upd;
    struct update_node *nextp;
} update_node;

typedef struct delete_node {
    RDB_ma_delete del;
    struct delete_node *nextp;
} delete_node;

static RDB_table *
replace_targets_tb(RDB_table *tbp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *, RDB_transaction *);

static RDB_table *
replace_targets_extend(RDB_table *tbp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int attrc = tbp->var.extend.attrc;
    RDB_table *newtbp = _RDB_new_table(ecp);
    if (newtbp == NULL)
        return NULL;

    newtbp->is_user = RDB_TRUE;
    newtbp->is_persistent = RDB_FALSE;
    newtbp->kind = RDB_TB_EXTEND;
    newtbp->var.extend.tbp = replace_targets_tb(tbp->var.extend.tbp,
            insc, insv, updc, updv, delc, delv, copyc, copyv, ecp, txp);
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
replace_targets_summarize(RDB_table *tbp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
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
    newtbp->var.summarize.tb1p = replace_targets_tb(tbp->var.summarize.tb1p,
            insc, insv, updc, updv, delc, delv, copyc, copyv, ecp, txp);
    if (newtbp->var.summarize.tb1p == NULL) {
        free(newtbp);
        return NULL;
    }
    newtbp->var.summarize.tb2p = replace_targets_tb(tbp->var.summarize.tb2p,
            insc, insv, updc, updv, delc, delv, copyc, copyv, ecp, txp);
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
replace_updattrs(RDB_table *tbp, int updc, RDB_attr_update updv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
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
        vattrv[i].exp = RDB_dup_expr(updv[i].exp, ecp);
        if (vattrv[i].exp == NULL) {
            int j;

            for (j = 0; j < i; j++) {
                RDB_drop_expr(updv[i].exp, ecp);
            }
        }
    }
    tb1p = _RDB_extend(tbp, updc, vattrv, ecp, txp);
    if (tb1p == NULL) {
        for (i = 0; i < updc; i++) {
            RDB_drop_expr(vattrv[i].exp, ecp);
        }
        tb3p = NULL;
        goto cleanup;
    }

    /*
     * Remove old attributes
     */
    attrv = malloc(sizeof(char *) * updc);
    if (attrv == NULL) {
        RDB_drop_table(tb1p, ecp, NULL);
        goto cleanup;
    }
    for (i = 0; i < updc; i++) {
        attrv[i] = updv[i].name;
    }
    tb2p = RDB_remove(tb1p, updc, attrv, ecp);
    if (tb2p == NULL) {
        RDB_drop_table(tb1p, ecp, NULL);
        tb3p = NULL;
        goto cleanup;
    }

    /*
     * Rename new attributes
     */
    renv = malloc(sizeof (RDB_renaming) * updc);
    if (renv == NULL) {
        RDB_drop_table(tb2p, ecp, NULL);
        goto cleanup;
    }
    for (i = 0; i < updc; i++) {
        renv[i].from = vattrv[i].name;
        renv[i].to = updv[i].name;
    }
    tb3p = RDB_rename(tb2p, updc, renv, ecp);
    if (tb3p == NULL) {
        RDB_drop_table(tb2p, ecp, NULL);
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
replace_targets_real_ins(RDB_table *tbp, const RDB_ma_insert *insp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (insp->tbp == tbp) {
        RDB_table *tb1p, *tb2p;

        tb1p = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
                0, NULL, ecp, NULL);
        if (tb1p == NULL)
            return NULL;
        ret = RDB_insert(tb1p, insp->tplp, ecp, NULL);
        if (ret != RDB_OK)
            return NULL;
        tb2p = RDB_union(tbp, tb1p, ecp);
        if (tb2p == NULL) {
            RDB_drop_table(tb1p, ecp, NULL);
            return NULL;
        }
        return tb2p;
    }
    return tbp;
}

static RDB_table *
replace_targets_real_upd(RDB_table *tbp, const RDB_ma_update *updp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *cond2p = NULL;
    RDB_table *utbp = updp->tbp;

    if (utbp == tbp) {
        if (updp->condp == NULL && cond2p == NULL) {
            return replace_updattrs(tbp, updp->updc, updp->updv, ecp, txp);
        } else {
            RDB_expression *ucondp, *nucondp, *tcondp;
            RDB_table *utbp, *nutbp;

            /*
             * Create condition for the updated part
             */
            if (updp->condp != NULL) {
                ucondp = RDB_dup_expr(updp->condp, ecp);
                if (ucondp == NULL)
                    return NULL;
                if (cond2p != NULL) {
                    tcondp = RDB_dup_expr(cond2p, ecp);
                    if (tcondp == NULL) {
                        RDB_drop_expr(cond2p, ecp);
                        return NULL;
                    }
                    cond2p = tcondp;
                    tcondp = RDB_ro_op_va("AND", ecp, ucondp, cond2p,
                            (RDB_expression *) NULL);
                    if (tcondp == NULL) {
                        RDB_drop_expr(ucondp, ecp);
                        RDB_drop_expr(cond2p, ecp);
                        return NULL;
                    }
                    ucondp = tcondp;
                }
            } else {
                ucondp = RDB_dup_expr(cond2p, ecp);
                if (ucondp == NULL)
                    return NULL;
            }

            /*
             * Create condition for not updated part
             */
            tcondp = RDB_dup_expr(ucondp, ecp);
            if (tcondp == NULL) {
                RDB_drop_expr(ucondp, ecp);
                return NULL;
            }
            nucondp = RDB_ro_op_va("NOT", ecp, tcondp, (RDB_expression *) NULL);
            if (nucondp == NULL) {
                RDB_drop_expr(tcondp, ecp);
                return NULL;
            }

            /*
             * Create selections and merge them by 'union'
             */

            utbp = _RDB_select(tbp, ucondp, ecp);
            if (utbp == NULL) {
                RDB_drop_expr(ucondp, ecp);
                RDB_drop_expr(nucondp, ecp);
                return NULL;
            }

            nutbp = _RDB_select(tbp, nucondp, ecp);
            if (nutbp == NULL) {
                RDB_drop_expr(ucondp, ecp);
                RDB_drop_table(utbp, ecp, NULL);
                return NULL;
            }

            tbp = replace_updattrs(utbp, updp->updc, updp->updv, ecp, txp);
            if (tbp == NULL) {
                RDB_drop_table(utbp, ecp, NULL);
                RDB_drop_table(nutbp, ecp, NULL);
                return NULL;
            }
            utbp = tbp;
                
            tbp = RDB_union(utbp, nutbp, ecp);
            if (tbp == NULL) {
                RDB_drop_table(utbp, ecp, NULL);
                RDB_drop_table(nutbp, ecp, NULL);
                return NULL;
            }
        }
    }
    return tbp;
}

static RDB_table *
replace_targets_real(RDB_table *tbp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_table *htbp;

    for (i = 0; i < insc; i++) {
        if (insv[i].tbp->kind == RDB_TB_REAL) {
            htbp = replace_targets_real_ins(tbp, &insv[i], ecp, txp);
            if (htbp != tbp)
                return htbp;
        }
    }

    for (i = 0; i < updc; i++) {
        if (updv[i].tbp->kind == RDB_TB_REAL) {
            htbp = replace_targets_real_upd(tbp, &updv[i], ecp, txp);
            if (htbp != tbp)
                return htbp;
        }
    }

    for (i = 0; i < delc; i++) {
        if (delv[i].tbp == tbp) {
            RDB_table *tb1p, *tb2p;
            RDB_expression *exp = NULL;
            
            if (delv[i].condp != NULL) {
                exp = RDB_dup_expr(delv[i].condp, ecp);
                if (exp == NULL)
                    return NULL;

                tb1p = _RDB_select(tbp, exp, ecp);
                if (tb1p == NULL) {
                    RDB_drop_expr(exp, ecp);
                    return NULL;
                }
                tb2p = RDB_minus(tbp, tb1p, ecp);
                if (tb2p == NULL) {
                    RDB_drop_table(tb1p, ecp, NULL);
                    return NULL;
                }
                return tb2p;
            }
            /* Expression is NULL - table will become empty */
            tb1p = RDB_create_table_from_type(NULL, RDB_FALSE, tbp->typ,
                    0, NULL, ecp, NULL);
            if (tb1p == NULL)
                return NULL;
            return tb1p;
        }
    }

    for (i = 0; i < copyc; i++) {
        if (copyv[i].dstp->kind == RDB_OB_TABLE
                && copyv[i].dstp->var.tbp == tbp) {
            return copyv[i].srcp->var.tbp;
        }
    }

    return tbp;
}

/*
 * Replace all child tables of tbp which are assignment targets
 * by the assignment sources.
 */
static RDB_table *
replace_targets_tb(RDB_table *tbp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_table *tb1p;
    RDB_table *tb2p;
    RDB_table *tb3p;
    RDB_table *ntbp;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            return replace_targets_real(tbp, insc, insv, updc, updv,
                    delc, delv, copyc, copyv, ecp, txp);
        case RDB_TB_SELECT:
        {
            RDB_expression *exp = RDB_dup_expr(tbp->var.select.exp, ecp);
            if (exp == NULL)
                return NULL;

            tb1p = replace_targets_tb(tbp->var.select.tbp,
                    insc, insv, updc, updv, delc, delv,
                    copyc, copyv, ecp, txp);
            if (tb1p == NULL) {
                RDB_drop_expr(exp, ecp);
                return NULL;
            }

            tbp = _RDB_select(tb1p, exp, ecp);
            if (tbp == NULL) {
                if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                RDB_drop_expr(exp, ecp);
                return NULL;
            }
            return tbp;
        }
        case RDB_TB_UNION:
            tb1p = replace_targets_tb(tbp->var._union.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var._union.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb2p == NULL) {
                if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            tbp = RDB_union(tb1p, tb2p, ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;
        case RDB_TB_MINUS:
            tb1p = replace_targets_tb(tbp->var.minus.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var.minus.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            tbp = RDB_minus(tb1p, tb2p, ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;
        case RDB_TB_INTERSECT:
            tb1p = replace_targets_tb(tbp->var.intersect.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var.intersect.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            tbp = RDB_intersect(tb1p, tb2p, ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;
        case RDB_TB_JOIN:
            tb1p = replace_targets_tb(tbp->var.join.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var.join.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            ntbp = RDB_join(tb1p, tb2p, ecp);
            if (ntbp == NULL)
                return NULL;
            return ntbp;
        case RDB_TB_EXTEND:
            return replace_targets_extend(tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
        case RDB_TB_PROJECT:
        {
            char **attrnamev;
            int attrc = tbp->typ->var.basetyp->var.tuple.attrc;

            tb1p = replace_targets_tb(tbp->var.project.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
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
            return ntbp;
        }
        case RDB_TB_SUMMARIZE:
            return replace_targets_summarize(tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
        case RDB_TB_RENAME:
            tb1p = replace_targets_tb(tbp->var.rename.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            return RDB_rename(tb1p, tbp->var.rename.renc, tbp->var.rename.renv,
                    ecp);
        case RDB_TB_WRAP:
            tb1p = replace_targets_tb(tbp->var.wrap.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tbp = RDB_wrap(tb1p, tbp->var.wrap.wrapc, tbp->var.wrap.wrapv,
                    ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;
        case RDB_TB_UNWRAP:
            tb1p = replace_targets_tb(tbp->var.unwrap.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tbp = RDB_unwrap(tb1p, tbp->var.unwrap.attrc, tbp->var.unwrap.attrv,
                    ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;
        case RDB_TB_GROUP:
            tb1p = replace_targets_tb(tbp->var.group.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tbp = RDB_group(tb1p, tbp->var.group.attrc, tbp->var.group.attrv,
                    tbp->var.group.gattr, ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;
        case RDB_TB_UNGROUP:
            tb1p = replace_targets_tb(tbp->var.ungroup.tbp, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tbp = RDB_ungroup(tb1p, tbp->var.ungroup.attr, ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;
        case RDB_TB_SDIVIDE:
            tb1p = replace_targets_tb(tbp->var.sdivide.tb1p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb1p == NULL)
                return NULL;
            tb2p = replace_targets_tb(tbp->var.sdivide.tb2p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb2p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            tb3p = replace_targets_tb(tbp->var.sdivide.tb3p, insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (tb3p == NULL) {
                 if (tb1p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                 if (tb2p->kind != RDB_TB_REAL)
                    RDB_drop_table(tb1p, ecp, NULL);
                return NULL;
            }
            tbp = RDB_sdivide(tb1p, tb2p, tb3p, ecp);
            if (tbp == NULL)
                return NULL;
            return tbp;
    }

    /* Must never be reached */
    abort();
}

static RDB_expression *
replace_targets(RDB_expression *exp,
        int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *newexp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = replace_targets(exp->var.op.argv[0], insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (newexp == NULL)
                return NULL;
            return RDB_tuple_attr(newexp, exp->var.op.name, ecp);
        case RDB_EX_GET_COMP:
            newexp = replace_targets(exp->var.op.argv[0], insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_comp(newexp, exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            int i;
            RDB_expression **argexpv = (RDB_expression **)
                    malloc(sizeof (RDB_expression *) * exp->var.op.argc);

            if (argexpv == NULL)
                return NULL;

            for (i = 0; i < exp->var.op.argc; i++) {
                argexpv[i] = replace_targets(exp->var.op.argv[i],
                        insc, insv, updc, updv, delc, delv,
                        copyc, copyv, ecp, txp);
                if (argexpv[i] == NULL)
                    return NULL;
            }
            newexp = RDB_ro_op(exp->var.op.name, exp->var.op.argc, argexpv,
                    ecp);
            free(argexpv);
            return newexp;
        }
        case RDB_EX_AGGREGATE:
            newexp = replace_targets(exp->var.op.argv[0], insc, insv,
                    updc, updv, delc, delv, copyc, copyv, ecp, txp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_aggregate(newexp, exp->var.op.op,
                    exp->var.op.name, ecp);
        case RDB_EX_OBJ:
            if (exp->var.obj.kind == RDB_OB_TABLE) {
                RDB_table *newtbp = replace_targets_tb(exp->var.obj.var.tbp,
                        insc, insv, updc, updv, delc, delv,
                        copyc, copyv, ecp, txp);
                if (newtbp == NULL)
                    return NULL;
                return RDB_table_to_expr(newtbp, ecp);
            }
            return RDB_obj_to_expr(&exp->var.obj, ecp);
        case RDB_EX_ATTR:
            return RDB_expr_attr(exp->var.attrname, ecp);
    }
    abort();
}

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

static void
concat_updlists (update_node **dstpp, update_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        update_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static void
concat_dellists (delete_node **dstpp, delete_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        delete_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static int
check_extend_tuple(RDB_object *tplp, RDB_table *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp)
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
            RDB_raise_invalid_argument("invalid EXTEND attribute", ecp);
            return RDB_ERROR;
        }
        RDB_init_obj(&val);
        ret = RDB_evaluate(vattrp->exp, tplp, ecp, txp, &val);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&val, ecp);
            return ret;
        }
        ret = RDB_obj_equals(&val, valp, ecp, txp, &iseq);
        RDB_destroy_obj(&val, ecp);
        if (ret != RDB_OK)
            return ret;
        if (!iseq) {
            RDB_raise_predicate_violation("EXTEND predicate violation", ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static insert_node *
new_insert_node(RDB_table *tbp, const RDB_object *tplp, RDB_exec_context *ecp)
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
    ret = _RDB_copy_tuple(insnp->ins.tplp, tplp, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(insnp->ins.tplp, ecp);
        free(insnp->ins.tplp);
        free(insnp);
        return NULL;
    }
    insnp->ins.tbp = tbp;
    insnp->nextp = NULL;
    return insnp;
}

static void
del_inslist(insert_node *insnp, RDB_exec_context *ecp)
{
    insert_node *hinsnp;

    while (insnp != NULL) {
        hinsnp = insnp->nextp;

        if (insnp->ins.tplp != NULL) {
            RDB_destroy_obj(insnp->ins.tplp, ecp);
            free(insnp->ins.tplp);
        }
        free(insnp);
        insnp = hinsnp;
    }    
}

static void
del_updlist(update_node *updnp, RDB_exec_context *ecp)
{
    update_node *hupdnp;

    while (updnp != NULL) {
        hupdnp = updnp->nextp;

        if (updnp->upd.condp != NULL) {
            RDB_drop_expr(updnp->upd.condp, ecp);
        }
        free(updnp);
        updnp = hupdnp;
    }    
}

static void
del_dellist(delete_node *delnp, RDB_exec_context *ecp)
{
    delete_node *hdelnp;

    while (delnp != NULL) {
        hdelnp = delnp->nextp;

        if (delnp->del.condp != NULL) {
            RDB_drop_expr(delnp->del.condp, ecp);
        }
        free(delnp);
        delnp = hdelnp;
    }    
}

static int
resolve_insert(const RDB_ma_insert *insp, insert_node **inslpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_bool b, b2;
    RDB_ma_insert ins;
    RDB_object tpl;

    switch (insp->tbp->kind) {
        case RDB_TB_REAL:
            *inslpp = new_insert_node(insp->tbp, insp->tplp, ecp);
            if (*inslpp == NULL)
                return RDB_ERROR;
            return RDB_OK;
        case RDB_TB_SELECT:
            ret = RDB_evaluate_bool(insp->tbp->var.select.exp, insp->tplp,
                    ecp, txp, &b);
            if (ret != RDB_OK)
                return ret;
            if (!b) {
                RDB_raise_predicate_violation("SELECT predicate violation",
                        ecp);
                return RDB_ERROR;
            }
            ins.tbp = insp->tbp->var.select.tbp;
            ins.tplp = insp->tplp;
            return resolve_insert(&ins, inslpp, ecp, txp);
        case RDB_TB_PROJECT:
            ins.tbp = insp->tbp->var.project.tbp;
            ins.tplp = insp->tplp;
            return resolve_insert(&ins, inslpp, ecp, txp);
        case RDB_TB_INTERSECT:
            if (RDB_table_contains(insp->tbp->var.intersect.tb1p, insp->tplp,
                    ecp, txp, &b) != RDB_OK) {
                return RDB_ERROR;
            }
            if (RDB_table_contains(insp->tbp->var.intersect.tb2p,
                    insp->tplp, ecp, txp, &b2) != RDB_OK) {
                return RDB_ERROR;
            }

            /*
             * If both 'subtables' contain the tuple, the insert fails
             */
            if (b && b2) {
                RDB_raise_element_exists("tuple is already in table", ecp);
                return RDB_ERROR;
            }

            /*
             * Insert the tuple into the table(s) which do not contain it
             */
            *inslpp = NULL;
            if (!b) {
                ins.tbp = insp->tbp->var.intersect.tb1p;
                ins.tplp = insp->tplp;
                ret = resolve_insert(&ins, inslpp, ecp, txp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
            }
            if (!b2) {
                insert_node *hinsnp;

                ins.tbp = insp->tbp->var.intersect.tb2p;
                ins.tplp = insp->tplp;
                ret = resolve_insert(&ins, &hinsnp, ecp, txp);
                if (ret != RDB_OK) {
                    if (*inslpp != NULL)
                        del_inslist(*inslpp, ecp);
                    return RDB_ERROR;
                }
                if (*inslpp == NULL) {
                    *inslpp = hinsnp;
                } else {
                    concat_inslists(inslpp, hinsnp);
                }
            }
            return RDB_OK;
        case RDB_TB_JOIN:
            if (RDB_table_contains(insp->tbp->var.join.tb1p, insp->tplp,
                    ecp, txp, &b) != RDB_OK) {
                return RDB_ERROR;
            }
            if (RDB_table_contains(insp->tbp->var.join.tb2p, insp->tplp,
                    ecp, txp, &b2) != RDB_OK) {
                return RDB_ERROR;
            }

            /*
             * If both 'subtables' contain the tuple, the insert fails
             */
            if (b && b2) {
                RDB_raise_element_exists("tuple is already in table", ecp);
                return RDB_ERROR;
            }

            /*
             * Insert the tuple into the table(s) which do not contain it
             */
            *inslpp = NULL;
            if (!b) {
                ins.tbp = insp->tbp->var.join.tb1p;
                ins.tplp = insp->tplp;
                ret = resolve_insert(&ins, inslpp, ecp, txp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
            }
            if (!b2) {
                insert_node *hinsnp;

                ins.tbp = insp->tbp->var.join.tb2p;
                ins.tplp = insp->tplp;
                ret = resolve_insert(&ins, &hinsnp, ecp, txp);
                if (ret != RDB_OK) {
                    if (*inslpp != NULL)
                        del_inslist(*inslpp, ecp);
                    return RDB_ERROR;
                }
                if (*inslpp == NULL) {
                    *inslpp = hinsnp;
                } else {
                    concat_inslists(inslpp, hinsnp);
                }
            }
            return RDB_OK;
        case RDB_TB_RENAME:
            RDB_init_obj(&tpl);
            ret = _RDB_invrename_tuple(insp->tplp,
                    insp->tbp->var.rename.renc, insp->tbp->var.rename.renv,
                    ecp, &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return ret;
            }
            ins.tbp = insp->tbp->var.wrap.tbp;
            ins.tplp = &tpl;
            ret = resolve_insert(&ins, inslpp, ecp, txp);
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        case RDB_TB_EXTEND:
            ret = check_extend_tuple(insp->tplp, insp->tbp, ecp, txp);
            if (ret != RDB_OK)
                return ret;
            ins.tbp = insp->tbp->var.extend.tbp;
            ins.tplp = insp->tplp;
            return resolve_insert(&ins, inslpp, ecp, txp);
        case RDB_TB_WRAP:
            RDB_init_obj(&tpl);
            ret = _RDB_invwrap_tuple(insp->tplp, insp->tbp->var.wrap.wrapc,
                    insp->tbp->var.wrap.wrapv, ecp, &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return ret;
            }
            ins.tbp = insp->tbp->var.wrap.tbp;
            ins.tplp = &tpl;
            ret = resolve_insert(&ins, inslpp, ecp, txp);
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        case RDB_TB_UNWRAP:
            RDB_init_obj(&tpl);
            ret = _RDB_invunwrap_tuple(insp->tplp,
                    insp->tbp->var.unwrap.attrc, insp->tbp->var.unwrap.attrv,
                    insp->tbp->var.unwrap.tbp->typ->var.basetyp, ecp, &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return ret;
            }
            ins.tbp = insp->tbp->var.wrap.tbp;
            ins.tplp = &tpl;
            ret = resolve_insert(&ins, inslpp, ecp, txp);
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        case RDB_TB_UNION:
        case RDB_TB_MINUS:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_GROUP:
        case RDB_TB_UNGROUP:
        case RDB_TB_SDIVIDE:
            RDB_raise_not_supported(
                    "Insert is not supported for this virtual table", ecp);
            return RDB_ERROR;
    }
    abort();
}

static update_node *
new_update_node(const RDB_ma_update *updp, RDB_exec_context *ecp)
{
    update_node *nupdnp = malloc(sizeof (update_node));
    if (nupdnp == NULL)
        return NULL;
    if (updp->condp == NULL) {
        nupdnp->upd.condp = NULL;
    } else {
        nupdnp->upd.condp = RDB_dup_expr(updp->condp, ecp);
        if (nupdnp->upd.condp == NULL) {
            free(nupdnp);
            return NULL;
        }
    }
    nupdnp->upd.tbp = updp->tbp;
    nupdnp->upd.updc = updp->updc;
    nupdnp->upd.updv = updp->updv;
    nupdnp->nextp = NULL;
    return nupdnp;
}

static int
resolve_update(const RDB_ma_update *updp, update_node **updnpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_ma_update upd;
    update_node *updnp;
    int ret;

    switch (updp->tbp->kind) {
        case RDB_TB_REAL:
            *updnpp = new_update_node(updp, ecp);
            if (*updnpp == NULL)
                return RDB_ERROR;
            return RDB_OK;
        case RDB_TB_SELECT:
            upd.tbp = updp->tbp->var.select.tbp;
            upd.condp = updp->condp;
            upd.updc = updp->updc;
            upd.updv = updp->updv;
            ret = resolve_update(&upd, updnpp, ecp, txp);
            if (ret != RDB_OK)
                return ret;

            updnp = *updnpp;
            while (updnp != NULL) {
                if (updnp->upd.condp == NULL) {
                    updnp->upd.condp = RDB_dup_expr(updp->tbp->var.select.exp,
                            ecp);
                    if (updnp->upd.condp == NULL) {
                        del_updlist(*updnpp, ecp);
                        return RDB_ERROR;
                    }
                } else {
                    RDB_expression *ncondp;
                    RDB_expression *hcondp = RDB_dup_expr(
                            updp->tbp->var.select.exp, ecp);
                    if (hcondp == NULL) {
                        del_updlist(*updnpp, ecp);
                        return RDB_ERROR;
                    }
                    ncondp = RDB_ro_op_va("AND", ecp, hcondp, updnp->upd.condp,
                            (RDB_expression *) NULL);
                    if (ncondp == NULL) {
                        RDB_drop_expr(hcondp, ecp);
                        del_updlist(*updnpp, ecp);
                        return RDB_ERROR;
                    }                        
                    updnp->upd.condp = ncondp;
                }
                updnp = updnp->nextp;
            }
            return RDB_OK;
        case RDB_TB_PROJECT:
            upd.tbp = updp->tbp->var.project.tbp;
            upd.condp = updp->condp;
            upd.updc = updp->updc;
            upd.updv = updp->updv;
            return resolve_update(&upd, updnpp, ecp, txp);
        default: ;            
    }
    RDB_raise_not_supported(
            "Update is not supported for this virtual table", ecp);
    return RDB_ERROR;
}

static delete_node *
new_delete_node(const RDB_ma_delete *delp, RDB_exec_context *ecp)
{
    delete_node *ndelnp = malloc(sizeof (delete_node));
    if (ndelnp == NULL)
        return NULL;
    if (delp->condp == NULL) {
        ndelnp->del.condp = NULL;
    } else {
        ndelnp->del.condp = RDB_dup_expr(delp->condp, ecp);
        if (ndelnp->del.condp == NULL) {
            free(ndelnp);
            return NULL;
        }
    }
    ndelnp->del.tbp = delp->tbp;
    ndelnp->nextp = NULL;
    return ndelnp;
}

static int
resolve_delete(const RDB_ma_delete *delp, delete_node **delnpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_ma_delete del;
    delete_node *delnp;
    int ret;

    switch (delp->tbp->kind) {
        case RDB_TB_REAL:
            *delnpp = new_delete_node(delp, ecp);
            if (*delnpp == NULL)
                return RDB_ERROR;
            return RDB_OK;
        case RDB_TB_SELECT:
            del.tbp = delp->tbp->var.select.tbp;
            del.condp = delp->condp;
            ret = resolve_delete(&del, delnpp, ecp, txp);
            if (ret != RDB_OK)
                return ret;

            delnp = *delnpp;
            while (delnp != NULL) {
                if (delnp->del.condp == NULL) {
                    delnp->del.condp = RDB_dup_expr(delp->tbp->var.select.exp,
                            ecp);
                    if (delnp->del.condp == NULL) {
                        del_dellist(*delnpp, ecp);
                        return RDB_ERROR;
                    }
                } else {
                    RDB_expression *ncondp;
                    RDB_expression *hcondp = RDB_dup_expr(
                            delp->tbp->var.select.exp, ecp);
                    if (hcondp == NULL) {
                        del_dellist(*delnpp, ecp);
                        return RDB_ERROR;
                    }
                    ncondp = RDB_ro_op_va("AND", ecp, hcondp, delnp->del.condp,
                            (RDB_expression *) NULL);
                    if (ncondp == NULL) {
                        RDB_drop_expr(hcondp, ecp);
                        del_dellist(*delnpp, ecp);
                        return RDB_ERROR;
                    }                        
                    delnp->del.condp = ncondp;
                }
                delnp = delnp->nextp;
            }
            return RDB_OK;
        case RDB_TB_PROJECT:
            del.tbp = delp->tbp->var.project.tbp;
            del.condp = delp->condp;
            return resolve_delete(&del, delnpp, ecp, txp);
        case RDB_TB_MINUS:
            del.tbp = delp->tbp->var.minus.tb1p;
            del.condp = delp->condp;
            return resolve_delete(&del, delnpp, ecp, txp);
        case RDB_TB_RENAME:
            del.tbp = delp->tbp->var.rename.tbp;
            del.condp = delp->condp;
            ret = resolve_delete(&del, delnpp, ecp, txp);
            if (ret != RDB_OK)
                return ret;
            delnp = *delnpp;
            while (delnp != NULL) {
                if (delnp->del.condp != NULL) {
                    ret = _RDB_invrename_expr(delnp->del.condp,
                            delp->tbp->var.rename.renc,
                            delp->tbp->var.rename.renv, ecp);
                    if (ret != RDB_OK) {
                        del_dellist(*delnpp, ecp);
                        return RDB_ERROR;
                    }
                }
                delnp = delnp->nextp;
            }
            return RDB_OK;
        case RDB_TB_EXTEND:
            del.tbp = delp->tbp->var.extend.tbp;
            del.condp = delp->condp;
            ret = resolve_delete(&del, delnpp, ecp, txp);
            if (ret != RDB_OK)
                return ret;
            delnp = *delnpp;
            while (delnp != NULL) {
                if (delnp->del.condp != NULL) {
                    ret = _RDB_resolve_extend_expr(&delnp->del.condp,
                            delp->tbp->var.extend.attrc,
                            delp->tbp->var.extend.attrv, ecp);
                    if (ret != RDB_OK) {
                        del_dellist(*delnpp, ecp);
                        return ret;
                    }
                }
                delnp = delnp->nextp;
            }
            return RDB_OK;
        case RDB_TB_UNION:
        case RDB_TB_JOIN:
        case RDB_TB_INTERSECT:
        case RDB_TB_SUMMARIZE:
        case RDB_TB_WRAP:
        case RDB_TB_UNWRAP:
        case RDB_TB_GROUP:
        case RDB_TB_UNGROUP:
        case RDB_TB_SDIVIDE:
            RDB_raise_not_supported(
                    "Delete is not supported for this virtual table", ecp);
            return RDB_ERROR;
    }
    abort();
}

static int
do_update(const RDB_ma_update *updp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_table *tbp, *ntbp, *dtbp;
    RDB_expression *condp;

    if (updp->tbp->kind == RDB_TB_REAL && updp->condp == NULL) {
        return _RDB_update_real(updp->tbp, NULL, updp->updc, updp->updv,
                ecp, txp);
    }

    if (updp->condp != NULL) {
        tbp = _RDB_select(updp->tbp, updp->condp, ecp);
        if (tbp == NULL)
            return RDB_ERROR;
    } else {
        tbp = updp->tbp;
    }

    ret = _RDB_optimize(tbp, 0, NULL, ecp, txp, &ntbp);

    /* drop select */
    _RDB_free_table(tbp, ecp);

    if (ntbp->var.select.tbp->kind == RDB_TB_SELECT) {
        condp = ntbp->var.select.exp;
        dtbp = ntbp->var.select.tbp;
    } else {
        condp = NULL;
        dtbp = ntbp;
    }

    if (dtbp->var.select.tbp->var.project.indexp != NULL) {
        if (dtbp->var.select.tbp->var.project.indexp->idxp == NULL) {
            ret = _RDB_update_select_pindex(dtbp, condp,
                    updp->updc, updp->updv, ecp, txp);
        } else {
            ret = _RDB_update_select_index(dtbp, condp,
                    updp->updc, updp->updv, ecp, txp);
        }
    } else {
        ret = _RDB_update_real(ntbp->var.select.tbp->var.project.tbp,
                ntbp->var.select.exp, updp->updc, updp->updv, ecp, txp);
    }
    RDB_drop_table(ntbp, ecp, NULL);
    return ret;
}

static int
do_delete(const RDB_ma_delete *delp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_table *tbp, *ntbp, *dtbp;
    RDB_expression *condp;

    if (delp->tbp->kind == RDB_TB_REAL && delp->condp == NULL) {
        return _RDB_delete_real(delp->tbp, NULL, ecp, txp);
    }

    if (delp->condp != NULL) {
        tbp = _RDB_select(delp->tbp, delp->condp, ecp);
        if (tbp == NULL)
            return RDB_ERROR;
    } else {
        tbp = delp->tbp;
    }

    ret = _RDB_optimize(tbp, 0, NULL, ecp, txp, &ntbp);
    /* drop select */
    _RDB_free_table(tbp, ecp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    if (ntbp->var.select.tbp->kind == RDB_TB_SELECT) {
        condp = ntbp->var.select.exp;
        dtbp = ntbp->var.select.tbp;
    } else {
        condp = NULL;
        dtbp = ntbp;
    }

    if (dtbp->var.select.tbp->var.project.indexp != NULL) {
        if (dtbp->var.select.tbp->var.project.indexp->unique) {
            ret = _RDB_delete_select_uindex(dtbp, condp, ecp, txp);
        } else {
            ret = _RDB_delete_select_index(dtbp, condp, ecp, txp);
        }
    } else {
        ret = _RDB_delete_real(ntbp->var.select.tbp->var.project.tbp,
                ntbp->var.select.exp, ecp, txp);
    }
    RDB_drop_table(ntbp, ecp, NULL);
    return ret;
}

static int
copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *srctyp = RDB_obj_type(srcvalp);
    int ret = _RDB_copy_obj(dstvalp, srcvalp, ecp, txp);
    if (ret != RDB_OK)
        return ret;

    if (srctyp != NULL && RDB_type_is_scalar(srctyp))
        dstvalp->typ = srctyp;
    return RDB_OK;
} 

static int
resolve_inserts(int insc, const RDB_ma_insert *insv, RDB_ma_insert **ninsvp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int llen;

    /* list of generated inserts */
    insert_node *geninsnp = NULL;

    insert_node *insnp;

    for (i = 0; i < insc; i++) {
        if (insv[i].tbp->kind != RDB_TB_REAL) {
            ret = resolve_insert(&insv[i], &insnp, ecp, txp);
            if (ret != RDB_OK)
                goto cleanup;

            /* Add inserts to list */
            concat_inslists(&geninsnp, insnp);
        }
    }

    /*
     * If inserts have been generated, allocate new insert list
     * which consists of old and new inserts
     */

    llen = 0;
    insnp = geninsnp;
    while (insnp != NULL) {
        llen++;
        insnp = insnp->nextp;
    }

    if (llen > 0) {
        (*ninsvp) = malloc(sizeof (RDB_ma_insert) * (insc + llen));
        if (*ninsvp == NULL)
            goto cleanup;

        for (i = 0; i < insc; i++) {
            (*ninsvp)[i].tbp = insv[i].tbp;
            (*ninsvp)[i].tplp = insv[i].tplp;
        }

        insnp = geninsnp;
        while (insnp != NULL) {
            (*ninsvp)[i].tbp = insnp->ins.tbp;
            (*ninsvp)[i].tplp = insnp->ins.tplp;

            /* mark as copied */
            insnp->ins.tplp = NULL;

            insnp = insnp->nextp;
            i++;
        }
    } else {
        *ninsvp = (RDB_ma_insert *) insv;
    }
    ret = insc + llen;
    
cleanup:
    if (geninsnp == NULL)
        del_inslist(geninsnp, ecp);

    return ret;
}

static int
resolve_updates(int updc, const RDB_ma_update *updv, RDB_ma_update **nupdvp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int llen;

    /* list of generated updates */
    update_node *genupdnp = NULL;

    update_node *updnp;

    for (i = 0; i < updc; i++) {
        if (updv[i].tbp->kind != RDB_TB_REAL) {
            /* Convert virtual table updates to real table updates */
            ret = resolve_update(&updv[i], &updnp, ecp, txp);
            if (ret != RDB_OK)
                goto cleanup;

            /* Add updates to list */
            concat_updlists(&genupdnp, updnp);
        }
    }

    /*
     * If inserts have been generated, allocate new list
     * which consists of old and new updates
     */

    llen = 0;
    updnp = genupdnp;
    while (updnp != NULL) {
        llen++;
        updnp = updnp->nextp;
    }

    if (llen > 0) {
        (*nupdvp) = malloc(sizeof (RDB_ma_update) * (updc + llen));
        if (*nupdvp == NULL)
            goto cleanup;

        for (i = 0; i < updc; i++) {
            (*nupdvp)[i].tbp = updv[i].tbp;
            (*nupdvp)[i].condp = updv[i].condp;
            (*nupdvp)[i].updc = updv[i].updc;
            (*nupdvp)[i].updv = updv[i].updv;
        }

        updnp = genupdnp;
        while (updnp != NULL) {
            (*nupdvp)[i].tbp = updnp->upd.tbp;
            (*nupdvp)[i].condp = updnp->upd.condp;
            (*nupdvp)[i].updc = updnp->upd.updc;
            (*nupdvp)[i].updv = updnp->upd.updv;

            /* to prevent the expression from being dropped twice */
            updnp->upd.condp = NULL;

            updnp = updnp->nextp;
            i++;
        }
    } else {
        *nupdvp = (RDB_ma_update *) updv;
    }
    ret = updc + llen;
    
cleanup:
    if (genupdnp == NULL)
        del_updlist(genupdnp, ecp);

    return ret;
}

static int
resolve_deletes(int delc, const RDB_ma_delete *delv, RDB_ma_delete **ndelvp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int llen;

    /* list of generated deletes */
    delete_node *gendelnp = NULL;

    delete_node *delnp;

    for (i = 0; i < delc; i++) {
        if (delv[i].tbp->kind != RDB_TB_REAL) {
            /* Convert virtual table deletes to real table deletes */
            ret = resolve_delete(&delv[i], &delnp, ecp, txp);
            if (ret != RDB_OK)
                goto cleanup;

            /* Add deletes to list */
            concat_dellists(&gendelnp, delnp);
        }
    }

    /*
     * If inserts have been generated, allocate new list
     * which consists of old and new deletes
     */

    llen = 0;
    delnp = gendelnp;
    while (delnp != NULL) {
        llen++;
        delnp = delnp->nextp;
    }

    if (llen > 0) {
        (*ndelvp) = malloc(sizeof (RDB_ma_delete) * (delc + llen));
        if (*ndelvp == NULL)
            goto cleanup;

        for (i = 0; i < delc; i++) {
            (*ndelvp)[i].tbp = delv[i].tbp;
            (*ndelvp)[i].condp = delv[i].condp;
        }

        delnp = gendelnp;
        while (delnp != NULL) {
            (*ndelvp)[i].tbp = delnp->del.tbp;
            (*ndelvp)[i].condp = delnp->del.condp;

            /* to prevent the expression from being dropped twice */
            delnp->del.condp = NULL;

            delnp = delnp->nextp;
            i++;
        }
    } else {
        *ndelvp = (RDB_ma_delete *) delv;
    }
    ret = delc + llen;
    
cleanup:
    if (gendelnp == NULL)
        del_dellist(gendelnp, ecp);

    return ret;
}

int
RDB_multi_assign(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int ret;
    RDB_dbroot *dbrootp;
    int ninsc;
    int nupdc;
    int ndelc;
    RDB_transaction subtx;
    RDB_transaction *atxp = NULL;
    RDB_ma_insert *ninsv = NULL;
    RDB_ma_update *nupdv = NULL;
    RDB_ma_delete *ndelv = NULL;

    RDB_bool need_tx;

    /*
     * A running transaction is required for:
     * - updates
     * - deletes, except deletes of transient tables without a condition
     * - copying persistent tables
     * - inserts into persistent tables
     */
    if (updc > 0 || delc > 0) {
        need_tx = RDB_TRUE;
    } else {
        for (i = 0;
             i < copyc && (RDB_obj_table(copyv[i].dstp) == NULL
                           || !RDB_obj_table(copyv[i].dstp)->is_persistent);
             i++);
        need_tx = (RDB_bool) (i < copyc);
        if (!need_tx) {
            for (i = 0;
                 i < insc && !insv[i].tbp->is_persistent;
                 i++);
            need_tx = (RDB_bool) (i < insc);
        }
    }

    if (need_tx && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Check if conditions are of type BOOLEAN
     */

    for (i = 0; i < updc; i++) {
        if (updv[i].condp != NULL) {
            ret = _RDB_check_expr_type(updv[i].condp,
                    updv[i].tbp->typ->var.basetyp, &RDB_BOOLEAN, ecp, txp);
            if (ret != RDB_OK)
                return ret;
        }
    }

    for (i = 0; i < delc; i++) {
        if (delv[i].condp != NULL) {
            ret = _RDB_check_expr_type(delv[i].condp,
                    delv[i].tbp->typ->var.basetyp, &RDB_BOOLEAN, ecp, txp);
            if (ret != RDB_OK)
                return ret;
        }
    }

    /*
     * Check types of updated attributes
     */
    for (i = 0; i < updc; i++) {
        for (j = 0; j < updv[i].updc; j++) {
            RDB_attr *attrp = _RDB_tuple_type_attr(
                    updv[i].tbp->typ->var.basetyp, updv[i].updv[j].name);
            if (attrp == NULL) {
                RDB_raise_attribute_not_found(updv[i].updv[j].name, ecp);
                return RDB_ERROR;
            }

            if (RDB_type_is_scalar(attrp->typ)) {
                ret = _RDB_check_expr_type(updv[i].updv[j].exp,
                        updv[i].tbp->typ->var.basetyp, attrp->typ, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
        }
    }

    /*
     * Check types of copied values
     */
    for (i = 0; i < copyc; i++) {
        RDB_type *srctyp = RDB_obj_type(copyv[i].srcp);

        if (srctyp != NULL && RDB_type_is_scalar(srctyp)) {
            /* If destination carries a value, types must match */
            if (copyv[i].dstp->kind != RDB_OB_INITIAL
                    && (copyv[i].dstp->typ == NULL
                     || !RDB_type_equals(copyv[i].srcp->typ,
                                    copyv[i].dstp->typ))) {
                RDB_raise_type_mismatch("source does not match destination",
                        ecp);
                return RDB_ERROR;
            }
        }
    }

    /*
     * Resolve virtual table assignment
     */

    ninsc = resolve_inserts(insc, insv, &ninsv, ecp, txp);
    if (ninsc < 0) {
        ret = ninsc;
        ninsv = NULL;
        goto cleanup;
    }

    nupdc = resolve_updates(updc, updv, &nupdv, ecp, txp);
    if (nupdc < 0) {
        ret = nupdc;
        nupdv = NULL;
        goto cleanup;
    }

    ndelc = resolve_deletes(delc, delv, &ndelv, ecp, txp);
    if (ndelc < 0) {
        ret = ndelc;
        ndelv = NULL;
        goto cleanup;
    }

    /*
     * Check if the same target is assigned twice
     * or if a target is used a source later
     */
    for (i = 0; i < ninsc; i++) {
        for (j = i + 1; j < ninsc; j++) {
            if (ninsv[i].tbp == ninsv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < nupdc; j++) {
            if (ninsv[i].tbp == nupdv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < ndelc; j++) {
            if (ninsv[i].tbp == ndelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && insv[i].tbp == copyv[j].dstp->var.tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }

            /*
             * Check if a presviously modified table is source of a copy
             */
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && insv[i].tbp == copyv[j].srcp->var.tbp) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
            }
        }
    }
    for (i = 0; i < nupdc; i++) {
        for (j = i + 1; j < updc; j++) {
            if (nupdv[i].tbp == nupdv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < ndelc; j++) {
            if (updv[i].tbp == ndelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && updv[i].tbp == copyv[j].dstp->var.tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && updv[i].tbp == copyv[j].srcp->var.tbp) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
            }
        }
    }
    for (i = 0; i < ndelc; i++) {
        for (j = i + 1; j < ndelc; j++) {
            if (ndelv[i].tbp == ndelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && ndelv[i].tbp == copyv[j].dstp->var.tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && delv[i].tbp == copyv[j].srcp->var.tbp) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
            }
        }
    }

    for (i = 0; i < copyc; i++) {
        for (j = i + 1; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && copyv[i].dstp->kind == RDB_OB_TABLE
                    && copyv[i].dstp->var.tbp == copyv[j].dstp->var.tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && copyv[i].dstp->kind == RDB_OB_TABLE
                    && copyv[j].srcp->kind == RDB_OB_TABLE
                    && copyv[i].dstp->var.tbp == copyv[j].srcp->var.tbp)
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
        }
    }

    /*
     * Check constraints
     */
    if (txp != NULL) {
        RDB_constraint *constrp;

        dbrootp = RDB_tx_db(txp)->dbrootp;

        if (!dbrootp->constraints_read) {
            ret = _RDB_read_constraints(ecp, txp);
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
            /* resolved inserts/updates/... */
            if (expr_refers_target(constrp->exp, ninsc, ninsv, nupdc, nupdv,
                    ndelc, ndelv, copyc, copyv)) {
                RDB_bool b;

                /*
                 * Replace target tables
                 */
                RDB_expression *newexp = replace_targets(constrp->exp,
                        ninsc, ninsv, nupdc, nupdv, ndelc, ndelv,
                        copyc, copyv, ecp, txp);
                if (newexp == NULL) {
                    ret = RDB_ERROR;
                    goto cleanup;
                }
                    
                /*
                 * Check constraint
                 */
                ret = RDB_evaluate_bool(newexp, NULL, ecp, txp, &b);
                RDB_drop_expr(newexp, ecp);
                if (ret != RDB_OK)
                    goto cleanup;
                if (!b) {
                    RDB_raise_predicate_violation(constrp->name, ecp);
                    ret = RDB_ERROR;
                    goto cleanup;
                }
            }
            constrp = constrp->nextp;
        }
    }

    /*
     * Execute assignments
     */

    /* Start subtransaction, if there is more than one assignment */
    if (ninsc + nupdc + ndelc + copyc > 1) {
        ret = RDB_begin_tx(ecp, &subtx, RDB_tx_db(txp), txp);
        if (ret != RDB_OK)
            goto cleanup;
        atxp = &subtx;
    } else {
        atxp = txp;
    }

    for (i = 0; i < ninsc; i++) {
        if (ninsv[i].tbp->kind == RDB_TB_REAL) {
            ret = _RDB_insert_real(ninsv[i].tbp, ninsv[i].tplp, ecp, atxp);
            if (ret != RDB_OK)
                goto cleanup;
        }
    }
    for (i = 0; i < nupdc; i++) {
        if (nupdv[i].tbp->kind == RDB_TB_REAL) {
            ret = do_update(&nupdv[i], ecp, atxp);
            if (ret != RDB_OK)
                goto cleanup;
        }
    }
    for (i = 0; i < ndelc; i++) {
        if (ndelv[i].tbp->kind == RDB_TB_REAL) {
            ret = do_delete(&ndelv[i], ecp, atxp);
            if (ret != RDB_OK)
                goto cleanup;
        }
    }
    for (i = 0; i < copyc; i++) {
        if (copyv[i].dstp->kind == RDB_OB_TABLE
                && copyv[i].dstp->var.tbp->kind != RDB_TB_REAL) {
            RDB_raise_not_supported(
                    "Virtual table is copy destination", ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        ret = copy_obj(copyv[i].dstp, copyv[i].srcp, ecp, atxp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    /* Commit subtx, if it has been started */
    if (atxp == &subtx) {
        ret = RDB_commit(ecp, &subtx);
        atxp = NULL;
    } else {
        ret = RDB_OK;
    }

cleanup:
    /*
     * Free generated inserts, updates, deletes
     */
    if (ninsv != insv) {
        for (i = insc; i < ninsc; i++) {
            RDB_destroy_obj(ninsv[i].tplp, ecp);
            free(ninsv[i].tplp);
        }
        free(ninsv);
    }

    if (nupdv != updv) {
        for (i = updc; i < nupdc; i++) {
            if (nupdv[i].condp != NULL) {
                RDB_drop_expr(nupdv[i].condp, ecp);
            }
        }
        free(nupdv);
    }

    if (ndelv != delv) {
        for (i = delc; i < ndelc; i++) {
            if (ndelv[i].condp != NULL) {
                RDB_drop_expr(ndelv[i].condp, ecp);
            }
        }
        free(ndelv);
    }

    /* Abort subtx, if necessary */
    if (ret != RDB_OK) {
        if (atxp == &subtx) {
            RDB_rollback(ecp, &subtx);
        }
    }

    return ret;
}
