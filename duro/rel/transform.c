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
            case RDB_TB_STORED:
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

int
_RDB_transform(RDB_table *tbp)
{
    int ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
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
            ret = _RDB_transform(tbp->var.project.tbp);
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
