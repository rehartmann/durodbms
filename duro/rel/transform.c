/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>
#include <stdlib.h>
#include <string.h>

static void
del_keys(RDB_table *tbp)
{
    int i;

    if (tbp->keyv != NULL) {
        /* Delete candidate keys */
        for (i = 0; i < tbp->keyc; i++) {
            RDB_free_strvec(tbp->keyv[i].strc, tbp->keyv[i].strv);
        }
        free(tbp->keyv);
        tbp->keyv = NULL;
    }
}

static int
copy_type(RDB_table *dstp, const RDB_table *srcp)
{
    RDB_type *typ = _RDB_dup_nonscalar_type(srcp->typ);
    if (typ == NULL)
        return RDB_NO_MEMORY;

    RDB_drop_type(dstp->typ, NULL);
    dstp->typ = typ;

    return RDB_OK;
}

static int
transform_exp(RDB_expression *);

/* Try to eliminate NOT operator */
static int
eliminate_not(RDB_expression *exp)
{
    int ret;
    RDB_expression *hexp;

    switch (exp->var.op.arg1->kind) {
        case RDB_EX_AND:
            hexp = RDB_not(exp->var.op.arg1->var.op.arg2);
            if (hexp == NULL)
                return RDB_NO_MEMORY;
            exp->kind = RDB_EX_OR;
            exp->var.op.arg2 = hexp;
            exp->var.op.arg1->kind = RDB_EX_NOT;

            ret = eliminate_not(exp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
            return eliminate_not(exp->var.op.arg2);
        case RDB_EX_OR:
            hexp = RDB_not(exp->var.op.arg1->var.op.arg2);
            if (hexp == NULL)
                return RDB_NO_MEMORY;
            exp->kind = RDB_EX_AND;
            exp->var.op.arg2 = hexp;
            exp->var.op.arg1->kind = RDB_EX_NOT;

            ret = eliminate_not(exp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
            return eliminate_not(exp->var.op.arg2);
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
            hexp = exp->var.op.arg1;
            switch (exp->var.op.arg1->kind) {
                case RDB_EX_EQ:
                    exp->kind = RDB_EX_NEQ;
                    break;
                case RDB_EX_NEQ:
                    exp->kind = RDB_EX_EQ;
                    break;
                case RDB_EX_LT:
                    exp->kind = RDB_EX_GET;
                    break;
                case RDB_EX_GT:
                    exp->kind = RDB_EX_LET;
                    break;
                case RDB_EX_LET:
                    exp->kind = RDB_EX_GT;
                    break;
                case RDB_EX_GET:
                    exp->kind = RDB_EX_LT;
                    break;
                default: ; /* never reached */
            }
            exp->var.op.arg1 = hexp->var.op.arg1;
            exp->var.op.arg2 = hexp->var.op.arg2;
            free(hexp);
            ret = transform_exp(exp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
            return transform_exp(exp->var.op.arg2);
        case RDB_EX_NOT:
            hexp = exp->var.op.arg1;
            memcpy(exp, hexp->var.op.arg1, sizeof (RDB_expression));
            free(hexp->var.op.arg1);
            free(hexp);
            return transform_exp(exp);
        default:
            return transform_exp(exp->var.op.arg1);
    }
}

static int
transform_exp(RDB_expression *exp)
{
    int ret;

    switch (exp->kind) {
        case RDB_EX_NOT:
            return eliminate_not(exp);
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_AND:
        case RDB_EX_OR:
            ret = transform_exp(exp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
            return transform_exp(exp->var.op.arg2);
        default: ;
    }
    return RDB_OK;
}

static int
transform_select(RDB_table *tbp)
{
    int ret;
    RDB_expression *exp;
    RDB_table *chtbp = tbp->var.select.tbp;

    do {
        switch (chtbp->kind) {
            case RDB_TB_REAL:
                return transform_exp(tbp->var.select.exp);
            case RDB_TB_SELECT:
            case RDB_TB_SELECT_INDEX:
                /*
                 * Merge SELECT tables
                 */
                exp = RDB_and(tbp->var.select.exp,
                        chtbp->var.select.exp);
                if (exp == NULL)
                    return RDB_NO_MEMORY;

                tbp->var.select.exp = exp;
                tbp->var.select.tbp = chtbp->var.select.tbp;
                _RDB_free_table(chtbp);
                chtbp = tbp->var.select.tbp;
                break;
            case RDB_TB_MINUS:
            {
                RDB_table *htbp = chtbp->var.minus.tb1p;

                ret = transform_exp(tbp->var.select.exp);
                if (ret != RDB_OK)
                    return ret;
                exp = tbp->var.select.exp;

                ret = _RDB_transform(chtbp->var.minus.tb2p);
                if (ret != RDB_OK)
                    return ret;

                tbp->kind = RDB_TB_MINUS;
                tbp->keyc = chtbp->keyc;
                tbp->keyv = chtbp->keyv;
                tbp->var.minus.tb1p = chtbp;
                tbp->var.minus.tb2p = chtbp->var.minus.tb2p;

                chtbp->kind = RDB_TB_SELECT;
                chtbp->var.select.tbp = htbp;
                chtbp->var.select.exp = exp;

                /* Keys may have changed, so delete them */
                del_keys(chtbp);

                tbp = chtbp;
                chtbp = tbp->var.select.tbp;
                break;
            }
            case RDB_TB_UNION:
            {
                RDB_table *newtbp;
                RDB_expression *ex2p;
                RDB_table *htbp = chtbp->var._union.tb1p;

                ret = transform_exp(tbp->var.select.exp);
                if (ret != RDB_OK)
                    return ret;

                ex2p = RDB_dup_expr(tbp->var.select.exp);
                if (ex2p == NULL)
                    return RDB_NO_MEMORY;

                ret = RDB_select(chtbp->var._union.tb2p, ex2p, &newtbp);
                if (ret != RDB_OK) {
                    RDB_drop_expr(ex2p);
                    return ret;
                }

                exp = tbp->var.select.exp;

                tbp->kind = RDB_TB_UNION;
                tbp->var._union.tb1p = chtbp;
                tbp->var._union.tb2p = newtbp;
                chtbp->kind = RDB_TB_SELECT;
                chtbp->var.select.tbp = htbp;
                chtbp->var.select.exp = exp;

                ret = transform_select(newtbp);
                if (ret != RDB_OK)
                    return ret;

                /* Keys may have changed, so delete them */
                del_keys(chtbp);

                tbp = chtbp;
                chtbp = tbp->var.select.tbp;
                break;
            }
            case RDB_TB_INTERSECT:
            {
                RDB_table *newtbp;
                RDB_expression *ex2p;
                RDB_table *htbp = chtbp->var._union.tb1p;

                ret = transform_exp(tbp->var.select.exp);
                if (ret != RDB_OK)
                    return ret;

                ex2p = RDB_dup_expr(tbp->var.select.exp);
                if (ex2p == NULL)
                    return RDB_NO_MEMORY;

                ret = RDB_select(chtbp->var._union.tb2p, ex2p, &newtbp);
                if (ret != RDB_OK) {
                    RDB_drop_expr(ex2p);
                    return ret;
                }

                exp = tbp->var.select.exp;

                tbp->kind = RDB_TB_INTERSECT;
                tbp->var._union.tb1p = chtbp;
                tbp->var._union.tb2p = newtbp;
                chtbp->kind = RDB_TB_SELECT;
                chtbp->var.select.tbp = htbp;
                chtbp->var.select.exp = exp;

                ret = transform_select(newtbp);
                if (ret != RDB_OK)
                    return ret;

                /* Keys may have changed, so delete them */
                del_keys(chtbp);

                tbp = chtbp;
                chtbp = tbp->var.select.tbp;
                break;
            }
            case RDB_TB_EXTEND:
            {
                RDB_table *htbp = chtbp->var.extend.tbp;

                ret = transform_exp(tbp->var.select.exp);
                if (ret != RDB_OK)
                    return ret;
                exp = tbp->var.select.exp;
                ret = _RDB_resolve_extend_expr(&exp, chtbp->var.extend.attrc,
                        chtbp->var.extend.attrv);
                if (ret != RDB_OK)
                    return ret;

                tbp->kind = RDB_TB_EXTEND;
                tbp->var.extend.attrc = chtbp->var.extend.attrc;
                tbp->var.extend.attrv = chtbp->var.extend.attrv;

                chtbp->kind = RDB_TB_SELECT;
                chtbp->var.select.tbp = htbp;
                chtbp->var.select.exp = exp;

                ret = copy_type(chtbp, htbp);
                if (ret != RDB_OK)
                     return ret;

                /* Keys may have changed, so delete them */
                del_keys(chtbp);

                tbp = chtbp;
                chtbp = tbp->var.select.tbp;
                break;
            }
            case RDB_TB_RENAME:
            {
                RDB_table *htbp = chtbp->var.rename.tbp;

                ret = transform_exp(tbp->var.select.exp);
                if (ret != RDB_OK)
                    return ret;
                exp = tbp->var.select.exp;
                ret = _RDB_invrename_expr(exp, chtbp->var.rename.renc,
                        chtbp->var.rename.renv);
                if (ret != RDB_OK)
                    return ret;

                tbp->kind = RDB_TB_RENAME;
                tbp->var.rename.renc = chtbp->var.rename.renc;
                tbp->var.rename.renv = chtbp->var.rename.renv;

                chtbp->kind = RDB_TB_SELECT;
                chtbp->var.select.tbp = htbp;
                chtbp->var.select.exp = exp;

                ret = copy_type(chtbp, htbp);
                if (ret != RDB_OK)
                     return ret;

                /* Keys may have changed, so delete them */
                del_keys(chtbp);

                tbp = chtbp;
                chtbp = tbp->var.select.tbp;
                break;
            }
            case RDB_TB_JOIN:
            case RDB_TB_PROJECT:
            case RDB_TB_SUMMARIZE:
                ret = transform_exp(tbp->var.select.exp);
                if (ret != RDB_OK)
                    return ret;
                return _RDB_transform(chtbp);
            case RDB_TB_WRAP:
            case RDB_TB_UNWRAP:
            case RDB_TB_GROUP:
            case RDB_TB_UNGROUP:
            case RDB_TB_SDIVIDE:
                ret = transform_exp(tbp->var.select.exp);
                if (ret != RDB_OK)
                    return ret;
                return _RDB_transform(chtbp);
        }
    } while (tbp->kind == RDB_TB_SELECT);

    return RDB_OK;
}

static int
transform_project(RDB_table *tbp);

static int
swap_project_union(RDB_table *tbp, RDB_table *chtbp)
{
    int ret;
    int i;
    RDB_table *newtbp;
    RDB_type *newtyp;
    RDB_table *htbp = chtbp->var._union.tb1p;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    char **attrnamev = malloc(attrc * sizeof(char *));
    if (attrnamev == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < attrc; i++) {
        attrnamev[i] = tbp->typ->var.basetyp
                ->var.tuple.attrv[i].name;
    }

    /* Create new project table for child #2 */
    ret = RDB_project(chtbp->var._union.tb2p, attrc, attrnamev, &newtbp);
    if (ret != RDB_OK) {
        free(attrnamev);
        return ret;
    }

    /* Alter parent */
    tbp->kind = RDB_TB_UNION;
    tbp->var._union.tb1p = chtbp;
    tbp->var._union.tb2p = newtbp;

    /* Alter child #1 */
    chtbp->kind = RDB_TB_PROJECT;
    chtbp->var.project.tbp = htbp;
    ret = RDB_project_relation_type(chtbp->typ, attrc, attrnamev, &newtyp);
    free(attrnamev);
    if (ret != RDB_OK) {
        return ret;
    }
    RDB_drop_type(chtbp->typ, NULL);
    chtbp->typ = newtyp;

    ret = transform_project(newtbp);
    if (ret != RDB_OK)
        return ret;

    /* Infer keys for first child to set keyloss flag */
    return _RDB_infer_keys(chtbp);
}

static int
swap_project_rename(RDB_table *tbp, RDB_table *chtbp)
{
    int i, j;
    int ret;
    RDB_type *newtyp;
    RDB_renaming *renv;
    char **attrnamev;
    RDB_table *htbp = chtbp->var.rename.tbp;
    int nattrc = tbp->typ->var.basetyp->var.tuple.attrc;

    /*
     * Alter parent
     */
    renv = malloc(sizeof (RDB_renaming) * tbp->var.rename.renc);
    if (renv == NULL)
        return RDB_NO_MEMORY;
    tbp->kind = RDB_TB_RENAME;
    tbp->var.rename.tbp = chtbp;
    tbp->var.rename.renc = chtbp->var.rename.renc;
    tbp->var.rename.renv = renv;

    /* Take renamings whose dest appear in the parent */
    j = 0;
    for (i = 0; i < chtbp->var.rename.renc; i++) {
        if (_RDB_tuple_type_attr(tbp->typ->var.basetyp,
                chtbp->var.rename.renv[i].to) != NULL) {
            tbp->var.rename.renv[j].from =
                    RDB_dup_str(chtbp->var.rename.renv[i].from);
            tbp->var.rename.renv[j].to =
                    RDB_dup_str(chtbp->var.rename.renv[i].to);
            j++;
        }
    }

    /*
     * Alter child
     */
     
    /* Destroy renamings */
    for (i = 0; i < chtbp->var.rename.renc; i++) {
        free(chtbp->var.rename.renv[i].from);
        free(chtbp->var.rename.renv[i].to);
    }
    chtbp->kind = RDB_TB_PROJECT;
    chtbp->var.project.tbp = htbp;

    attrnamev = malloc(nattrc * sizeof(char *));
    if (attrnamev == NULL)
        return RDB_NO_MEMORY;
    for (i = 0; i < nattrc; i++) {
        attrnamev[i] = tbp->typ->var.basetyp->var.tuple.attrv[i].name;
        for (j = 0; j < tbp->var.rename.renc
                && strcmp(attrnamev[i], tbp->var.rename.renv[j].to) != 0; j++);
        if (j < tbp->var.rename.renc)
            attrnamev[j] = tbp->var.rename.renv[j].from;
    }
    ret = RDB_project_relation_type(htbp->typ, nattrc, attrnamev, &newtyp);
    free(attrnamev);
    if (ret != RDB_OK)
        return ret;
    RDB_drop_type(chtbp->typ, NULL);
    chtbp->typ = newtyp;

    return RDB_OK;
}

static int
transform_project(RDB_table *tbp)
{
    int ret;
    RDB_table *chtbp = tbp->var.project.tbp;

    do {
        switch (chtbp->kind) {
            case RDB_TB_REAL:
                return RDB_OK;
            case RDB_TB_PROJECT:
                /* Merge projects */
                tbp->var.project.tbp = chtbp->var.project.tbp;
                tbp->var.project.keyloss = (RDB_bool) (tbp->var.project.keyloss
                        || chtbp->var.project.keyloss);
                _RDB_free_table(chtbp);
                chtbp = tbp->var.project.tbp;
                break;
            case RDB_TB_UNION:
                ret = swap_project_union(tbp, chtbp);
                if (ret != RDB_OK)
                    return ret;
                tbp = chtbp;
                chtbp = tbp->var.project.tbp;
                break;
            case RDB_TB_RENAME:
                ret = swap_project_rename(tbp, chtbp);
                if (ret != RDB_OK)
                    return ret;
                tbp = chtbp;
                chtbp = tbp->var.project.tbp;
                break;
            case RDB_TB_SELECT:
            case RDB_TB_SELECT_INDEX:
            case RDB_TB_MINUS:
            case RDB_TB_INTERSECT:
            case RDB_TB_JOIN:
            case RDB_TB_EXTEND:
            case RDB_TB_SUMMARIZE:
            case RDB_TB_WRAP:
            case RDB_TB_UNWRAP:
            case RDB_TB_SDIVIDE:
            case RDB_TB_GROUP:
            case RDB_TB_UNGROUP:
                return _RDB_transform(chtbp);
        }
    } while (tbp->kind == RDB_TB_PROJECT);
    return RDB_OK;
}

int
_RDB_transform(RDB_table *tbp)
{
    int ret;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            break;
        case RDB_TB_MINUS:
            ret = _RDB_transform(tbp->var.minus.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_transform(tbp->var.minus.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNION:
            ret = _RDB_transform(tbp->var._union.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_transform(tbp->var._union.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_INTERSECT:
            ret = _RDB_transform(tbp->var.intersect.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_transform(tbp->var.intersect.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_INDEX:
            ret = transform_select(tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_JOIN:
            ret = _RDB_transform(tbp->var.join.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_transform(tbp->var.join.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_EXTEND:
            ret = _RDB_transform(tbp->var.extend.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            ret = transform_project(tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SUMMARIZE:
            ret = _RDB_transform(tbp->var.summarize.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_transform(tbp->var.summarize.tb2p);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_RENAME:
            ret = _RDB_transform(tbp->var.rename.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_WRAP:
            ret = _RDB_transform(tbp->var.wrap.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNWRAP:
            ret = _RDB_transform(tbp->var.unwrap.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_GROUP:
            ret = _RDB_transform(tbp->var.group.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNGROUP:
            ret = _RDB_transform(tbp->var.ungroup.tbp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SDIVIDE:
            ret = _RDB_transform(tbp->var.sdivide.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_transform(tbp->var.sdivide.tb2p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_transform(tbp->var.sdivide.tb3p);
            if (ret != RDB_OK)
                return ret;
            break;
    }

    return RDB_OK;
}
