/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include <stdlib.h>

static int
transform_select(RDB_table *tbp)
{
    int ret;
    int keyc;
    RDB_string_vec *keyv;
    RDB_expression *exp;
    RDB_table *chtbp = tbp->var.select.tbp;

    do {
        switch (chtbp->kind) {
            case RDB_TB_STORED:
                return RDB_OK;
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

                exp = tbp->var.select.exp;
                keyc = tbp->keyc;
                keyv = tbp->keyv;

                ret = _RDB_transform(chtbp->var.minus.tb2p);
                if (ret != RDB_OK)
                    return ret;

                tbp->kind = RDB_TB_MINUS;
                tbp->keyc = chtbp->keyc;
                tbp->keyv = chtbp->keyv;
                tbp->var.minus.tb1p = chtbp;
                tbp->var.minus.tb2p = chtbp->var.minus.tb2p;
                chtbp->kind = RDB_TB_SELECT;
                chtbp->keyc = keyc;
                chtbp->keyv = keyv;
                chtbp->var.select.tbp = htbp;
                chtbp->var.select.exp = exp;

                tbp = chtbp;
                chtbp = tbp->var.select.tbp;
                break;
            }
            case RDB_TB_UNION:
            {
                RDB_table *newtbp;
                RDB_table *htbp = tbp->var.select.tbp->var._union.tb1p;
                RDB_expression *ex2p = RDB_dup_expr(tbp->var.select.exp);

                if (ex2p == NULL)
                    return RDB_NO_MEMORY;

                ret = RDB_select(chtbp->var._union.tb2p, ex2p, &newtbp);
                if (ret != RDB_OK) {
                    RDB_drop_expr(ex2p);
                    return ret;
                }

                exp = tbp->var.select.exp;
                keyc = tbp->keyc;
                keyv = tbp->keyv;

                tbp->kind = RDB_TB_UNION;
                tbp->keyc = chtbp->keyc;
                tbp->keyv = chtbp->keyv;
                tbp->var._union.tb1p = chtbp;
                tbp->var._union.tb2p = newtbp;
                chtbp->kind = RDB_TB_SELECT;
                chtbp->keyc = keyc;
                chtbp->keyv = keyv;
                chtbp->var.select.tbp = htbp;
                chtbp->var.select.exp = exp;

                ret = transform_select(newtbp);
                if (ret != RDB_OK)
                    return ret;

                tbp = chtbp;
                chtbp = tbp->var.select.tbp;
                break;
            }
            case RDB_TB_INTERSECT:
            {
                RDB_table *newtbp;
                RDB_table *htbp = tbp->var.select.tbp->var._union.tb1p;
                RDB_expression *ex2p = RDB_dup_expr(tbp->var.select.exp);

                if (ex2p == NULL)
                    return RDB_NO_MEMORY;

                ret = RDB_select(chtbp->var._union.tb2p, ex2p, &newtbp);
                if (ret != RDB_OK) {
                    RDB_drop_expr(ex2p);
                    return ret;
                }

                exp = tbp->var.select.exp;
                keyc = tbp->keyc;
                keyv = tbp->keyv;

                tbp->kind = RDB_TB_INTERSECT;
                tbp->keyc = chtbp->keyc;
                tbp->keyv = chtbp->keyv;
                tbp->var._union.tb1p = chtbp;
                tbp->var._union.tb2p = newtbp;
                chtbp->kind = RDB_TB_SELECT;
                chtbp->keyc = keyc;
                chtbp->keyv = keyv;
                chtbp->var.select.tbp = htbp;
                chtbp->var.select.exp = exp;

                ret = transform_select(newtbp);
                if (ret != RDB_OK)
                    return ret;

                tbp = chtbp;
                chtbp = tbp->var.select.tbp;
                break;
            }
            case RDB_TB_JOIN:
            case RDB_TB_EXTEND:
            case RDB_TB_PROJECT:
            case RDB_TB_SUMMARIZE:
            case RDB_TB_RENAME:
            case RDB_TB_WRAP:
            case RDB_TB_UNWRAP:
            case RDB_TB_GROUP:
            case RDB_TB_UNGROUP:
            case RDB_TB_SDIVIDE:
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
