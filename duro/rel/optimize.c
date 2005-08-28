/*
 * $Id$
 *
 * Copyright (C) 2004, 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

static RDB_bool is_and(RDB_expression *exp) {
    return (RDB_bool) exp->kind == RDB_EX_RO_OP
            && strcmp (exp->var.op.name, "AND") == 0;
}

static void
unbalance_and(RDB_expression *exp)
{
    RDB_expression *axp;

    if (!is_and(exp))
        return;

    if (is_and(exp->var.op.argv[0]))
        unbalance_and(exp->var.op.argv[0]);
        
    if (is_and(exp->var.op.argv[1])) {
        unbalance_and(exp->var.op.argv[1]);
        if (is_and(exp->var.op.argv[0])) {
            RDB_expression *ax2p;

            /* Find leftmost factor */
            axp = exp->var.op.argv[0];
            while (is_and(axp->var.op.argv[0]))
                axp = axp->var.op.argv[0];

            /* Swap leftmost factor and right child */
            ax2p = exp->var.op.argv[1];
            exp->var.op.argv[1] = axp->var.op.argv[0];
            axp->var.op.argv[0] = ax2p;
        } else {
            /* Swap children */
            axp = exp->var.op.argv[0];
            exp->var.op.argv[0] = exp->var.op.argv[1];
            exp->var.op.argv[1] = axp;
        }
    }
}

/*
 * Check if the expression covers all index attributes.
 */
static int
expr_covers_index(RDB_expression *exp, _RDB_tbindex *indexp)
{
    int i;

    for (i = 0; i < indexp->attrc
            && _RDB_attr_node(exp, indexp->attrv[i].attrname, "=") != NULL;
            i++);
    return i;
}

/*
 * Check if attrv covers all index attributes.
 */
static int
attrv_covers_index(int attrc, char *attrv[], _RDB_tbindex *indexp)
{
    int i;

    /* Check if all index attributes appear in attrv */
    for (i = 0; i < indexp->attrc; i++) {
        if (RDB_find_str(attrc, attrv, indexp->attrv[i].attrname) == -1)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/* !! return error code? */
static void
move_node(RDB_table *tbp, RDB_expression **dstpp, RDB_expression *nodep,
        RDB_transaction *txp)
{
    RDB_expression *prevp;

    /*
     * Move node
     */

    /* Get previous node */
    if (nodep == tbp->var.select.exp) {
        prevp = NULL;
    } else {
        prevp = tbp->var.select.exp;
        while (prevp->var.op.argv[0] != nodep)
            prevp = prevp->var.op.argv[0];
    }

    if (!is_and(nodep)) {
        if (*dstpp == NULL)
            *dstpp = nodep;
        else
            *dstpp = RDB_ro_op_va("AND", *dstpp, nodep, (RDB_expression *) NULL);
        if (prevp == NULL) {
            tbp->var.select.exp = NULL;
        } else {
            if (prevp == tbp->var.select.exp) {
                tbp->var.select.exp = prevp->var.op.argv[1];
            } else {
                RDB_expression *pprevp = tbp->var.select.exp;

                while (pprevp->var.op.argv[0] != prevp)
                    pprevp = pprevp->var.op.argv[0];
                pprevp->var.op.argv[0] = prevp->var.op.argv[1];
            }
            free(prevp->var.op.name);
            free(prevp->var.op.argv);
            free(prevp);
        }
    } else {
        if (*dstpp == NULL)
            *dstpp = nodep->var.op.argv[1];
        else
            *dstpp = RDB_ro_op_va("AND", *dstpp, nodep->var.op.argv[1],
                    (RDB_expression *) NULL);
        if (prevp == NULL)
            tbp->var.select.exp = nodep->var.op.argv[0];
        else
            prevp->var.op.argv[0] = nodep->var.op.argv[0];
        free(nodep->var.op.name);
        free(nodep->var.op.argv);
        free(nodep);
    }
}

/*
 * Split a selection into two: one that uses the index specified by indexp
 * (the child) and one which does not (the parent)
 * If the parent condition simple becomes true, simply convert
 * the selection into a selection which uses the index.
 */
static int
split_by_index(RDB_table *tbp, _RDB_tbindex *indexp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_expression *nodep;
    RDB_expression *ixexp = NULL;
    RDB_expression *stopexp = NULL;
    RDB_bool asc = RDB_TRUE;
    RDB_bool all_eq = RDB_TRUE;
    int objpc = 0;
    RDB_object **objpv;

    for (i = 0; i < indexp->attrc && all_eq; i++) {
        RDB_expression *attrexp;

        if (indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp)) {
            nodep = _RDB_attr_node(tbp->var.select.exp,
                    indexp->attrv[i].attrname, "=");
            if (nodep == NULL) {
                nodep = _RDB_attr_node(tbp->var.select.exp,
                        indexp->attrv[i].attrname, ">=");
                if (nodep == NULL) {
                    nodep = _RDB_attr_node(tbp->var.select.exp,
                            indexp->attrv[i].attrname, ">");
                    if (nodep == NULL) {
                        nodep = _RDB_attr_node(tbp->var.select.exp,
                                indexp->attrv[i].attrname, "<=");
                        if (nodep == NULL) {
                            nodep = _RDB_attr_node(tbp->var.select.exp,
                                    indexp->attrv[i].attrname, "<");
                            if (nodep == NULL)
                                break;
                        }
                    }
                }
                if (strcmp(nodep->var.op.name, ">=") == 0
                        || strcmp(nodep->var.op.name, ">") == 0) {
                    stopexp = _RDB_attr_node(tbp->var.select.exp,
                            indexp->attrv[i].attrname, "<=");
                    if (stopexp == NULL) {
                        stopexp = _RDB_attr_node(tbp->var.select.exp,
                                indexp->attrv[i].attrname, "<");
                    }
                    if (stopexp != NULL) {
                        attrexp = stopexp;
                        if (is_and(attrexp))
                            attrexp = attrexp->var.op.argv[1];
                        move_node(tbp, &ixexp, stopexp, txp);
                        stopexp = attrexp;
                    }
                }
                all_eq = RDB_FALSE;
            }
        } else {
            nodep = _RDB_attr_node(tbp->var.select.exp,
                    indexp->attrv[i].attrname, "=");
        }
        attrexp = nodep;
        if (is_and(attrexp))
            attrexp = attrexp->var.op.argv[1];

        objpc++;

        if (indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp)) {
            if (strcmp(attrexp->var.op.name, "=") == 0
                    || strcmp(attrexp->var.op.name, ">=") == 0
                    || strcmp(attrexp->var.op.name, ">") == 0)
                asc = indexp->attrv[i].asc;
            else
                asc = (RDB_bool) !indexp->attrv[i].asc;
        }
        
        move_node(tbp, &ixexp, nodep, txp);
    }

    objpv = _RDB_index_objpv(indexp, ixexp, tbp->typ,
            objpc, all_eq, asc);
    if (objpv == NULL)
        return RDB_NO_MEMORY;

    if (tbp->var.select.exp != NULL) {
        RDB_table *sitbp;

        /*
         * Split table into two
         */
        if (ixexp == NULL) {
            ixexp = RDB_bool_to_expr(RDB_TRUE);
            if (ixexp == NULL)
                return RDB_NO_MEMORY;
        }
        ret = RDB_select(tbp->var.select.tbp, ixexp, txp, &sitbp);
        if (ret != RDB_OK)
            return ret;

        sitbp->var.select.objpv = objpv;
        sitbp->var.select.objpc = objpc;
        sitbp->var.select.asc = asc;
        sitbp->var.select.all_eq = all_eq;
        sitbp->var.select.stopexp = stopexp;
        tbp->var.select.tbp = sitbp;
    } else {
        /*
         * Convert table to index select
         */
        tbp->var.select.objpv = objpv;
        tbp->var.select.objpc = objpc;
        tbp->var.select.asc = asc;
        tbp->var.select.all_eq = all_eq;
        tbp->var.select.exp = ixexp;
        tbp->var.select.stopexp = stopexp;
    }
    return RDB_OK;
}

static unsigned
table_cost(RDB_table *tbp)
{
    _RDB_tbindex *indexp;

    if (tbp->stp != NULL)
        return tbp->stp->est_cardinality;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            return 0;
        case RDB_TB_MINUS:
            return table_cost(tbp->var.minus.tb1p); /* !! */
        case RDB_TB_UNION:
            return table_cost(tbp->var._union.tb1p)
                + table_cost(tbp->var._union.tb2p);
        case RDB_TB_INTERSECT:
            return table_cost(tbp->var.intersect.tb1p);
        case RDB_TB_SELECT:
            if (tbp->var.select.objpc == 0)
                return table_cost(tbp->var.select.tbp);
            indexp = tbp->var.select.tbp->var.project.indexp;
            if (indexp->idxp == NULL)
                return 1;
            if (indexp->unique)
                return 2;
            if (!RDB_index_is_ordered(indexp->idxp))
                return 3;
            return 4; /* !! */
        case RDB_TB_JOIN:
            if (tbp->var.join.tb2p->kind != RDB_TB_PROJECT
                    || tbp->var.join.tb2p->var.project.indexp == NULL)
                return table_cost(tbp->var.join.tb1p)
                        * table_cost(tbp->var.join.tb2p);
            indexp = tbp->var.join.tb2p->var.project.indexp;
            if (indexp->idxp == NULL)
                return table_cost(tbp->var.join.tb1p);
            if (indexp->unique)
                return table_cost(tbp->var.join.tb1p) * 2;
            if (!RDB_index_is_ordered(indexp->idxp))
                return table_cost(tbp->var.join.tb1p) * 3;
            return table_cost(tbp->var.join.tb1p) * 4;
        case RDB_TB_EXTEND:
            return table_cost (tbp->var.extend.tbp);
        case RDB_TB_PROJECT:
            return table_cost(tbp->var.project.tbp);
        case RDB_TB_SUMMARIZE:
            return table_cost(tbp->var.summarize.tb1p);
        case RDB_TB_RENAME:
            return table_cost(tbp->var.rename.tbp);
        case RDB_TB_WRAP:
            return table_cost(tbp->var.wrap.tbp);
        case RDB_TB_UNWRAP:
            return table_cost(tbp->var.unwrap.tbp);
        case RDB_TB_GROUP:
            return table_cost(tbp->var.group.tbp);
        case RDB_TB_UNGROUP:
            return table_cost(tbp->var.ungroup.tbp);
        case RDB_TB_SDIVIDE:
            return table_cost(tbp->var.sdivide.tb1p)
                    * table_cost(tbp->var.sdivide.tb2p); /* !! */
    }
    abort();
}

static int
mutate(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp);

/*
 * Add "null project" parent
 */
static RDB_table *
null_project(RDB_table *tbp)
{
    int ret;
    RDB_table *ptbp = _RDB_new_table();
    if (tbp == NULL)
        return NULL;

    ptbp->is_user = RDB_TRUE;
    ptbp->is_persistent = RDB_FALSE;
    ptbp->kind = RDB_TB_PROJECT;
    ptbp->keyv = NULL;
    ptbp->var.project.tbp = tbp;
    ptbp->var.project.indexp = NULL;
    ptbp->var.project.keyloss = RDB_FALSE;

    /* Create type */
    ret = RDB_create_relation_type(
            tbp->typ->var.basetyp->var.tuple.attrc,
            tbp->typ->var.basetyp->var.tuple.attrv, &ptbp->typ);
    if (ret != RDB_OK) {
        free(ptbp);
        return NULL;
    }
    return ptbp;
}

static int
mutate_select(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_table *ctbp = tbp->var.select.tbp;

    if (ctbp->kind == RDB_TB_PROJECT
            && ctbp->var.project.tbp->kind == RDB_TB_REAL) {
        /* Convert condition into 'unbalanced' form */
        unbalance_and(tbp->var.select.exp);
    }

    int tbc = mutate(ctbp, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;

    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;
        RDB_expression *exp = RDB_dup_expr(tbp->var.select.exp);
        if (exp == NULL)
            return RDB_NO_MEMORY;

        ret = RDB_select(tbpv[i], exp, txp, &ntbp);
        if (ret != RDB_OK)
            return ret;
        if (tbpv[i]->kind == RDB_TB_PROJECT
                && tbpv[i]->var.project.indexp != NULL)
        {
            _RDB_tbindex *indexp = tbpv[i]->var.project.indexp;

            if ((indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp))
                    || expr_covers_index(tbp->var.select.exp, indexp)
                            == indexp->attrc) {
                ret = split_by_index(ntbp, indexp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
        }
        tbpv[i] = ntbp;
    }
    return tbc;
}

static int
mutate_union(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc1, tbc2;
    int i;
    int ret;

    tbc1 = mutate(tbp->var._union.tb1p, tbpv, cap, txp);
    if (tbc1 < 0)
        return tbc1;
    for (i = 0; i < tbc1; i++) {
        RDB_table *ntbp;
        RDB_table *otbp = _RDB_dup_vtable(tbp->var._union.tb2p);
        if (otbp == NULL)
            return RDB_NO_MEMORY;

        ret = RDB_union(tbpv[i], otbp, &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    tbc2 = mutate(tbp->var._union.tb2p, &tbpv[tbc1], cap - tbc1, txp);
    if (tbc2 < 0)
        return tbc2;
    for (i = tbc1; i < tbc1 + tbc2; i++) {
        RDB_table *ntbp;
        RDB_table *otbp = _RDB_dup_vtable(tbp->var._union.tb1p);
        if (otbp == NULL)
            return RDB_NO_MEMORY;

        ret = RDB_union(otbp, tbpv[i], &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc1 + tbc2;
}

static int
mutate_minus(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i;
    int ret;

    tbc = mutate(tbp->var.minus.tb1p, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;
    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;
        RDB_table *otbp = _RDB_dup_vtable(tbp->var.minus.tb2p);
        if (otbp == NULL)
            return RDB_NO_MEMORY;

        ret = RDB_minus(tbpv[i], otbp, &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc;
}

static int
mutate_intersect(RDB_table *tbp, RDB_table **tbpv, int cap,
        RDB_transaction *txp)
{
    int tbc1, tbc2;
    int i;
    int ret;

    tbc1 = mutate(tbp->var.intersect.tb1p, tbpv, cap, txp);
    if (tbc1 < 0)
        return tbc1;
    for (i = 0; i < tbc1; i++) {
        RDB_table *ntbp;
        RDB_table *otbp = _RDB_dup_vtable(tbp->var.intersect.tb2p);
        if (otbp == NULL)
            return RDB_NO_MEMORY;

        ret = RDB_intersect(tbpv[i], otbp, &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    tbc2 = mutate(tbp->var.intersect.tb2p, &tbpv[tbc1], cap - tbc1, txp);
    if (tbc2 < 0)
        return tbc2;
    for (i = tbc1; i < tbc1 + tbc2; i++) {
        RDB_table *ntbp;
        RDB_table *otbp = _RDB_dup_vtable(tbp->var.intersect.tb1p);
        if (otbp == NULL)
            return RDB_NO_MEMORY;

        ret = RDB_intersect(tbpv[i], otbp, &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc1 + tbc2;
}

static int
mutate_rename(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i;
    int ret;

    tbc = mutate(tbp->var.rename.tbp, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;
    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;

        ret = RDB_rename(tbpv[i], tbp->var.rename.renc, tbp->var.rename.renv,
                &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc;
}

static int
mutate_extend(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i, j;
    int ret;
    RDB_virtual_attr *extv;

    tbc = mutate(tbp->var.extend.tbp, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;

    extv = malloc(sizeof (RDB_virtual_attr)
            * tbp->var.extend.attrc);
    if (extv == NULL)
        return RDB_NO_MEMORY;
    for (i = 0; i < tbp->var.extend.attrc; i++)
        extv[i].name = tbp->var.extend.attrv[i].name;

    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;

        for (j = 0; j < tbp->var.extend.attrc; j++) {
            extv[j].exp = RDB_dup_expr(tbp->var.extend.attrv[j].exp);
            if (extv[j].exp == NULL)
                return RDB_NO_MEMORY;
        }
        ret = _RDB_extend(tbpv[i], tbp->var.extend.attrc, extv, txp, &ntbp);
        if (ret != RDB_OK) {
            for (j = 0; j < tbp->var.extend.attrc; j++)
                RDB_drop_expr(extv[j].exp);
            free(extv);
            return ret;
        }
        tbpv[i] = ntbp;
    }
    free(extv);
    return tbc;
}

static int
mutate_summarize(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i, j;
    int ret;
    RDB_summarize_add *addv;

    tbc = mutate(tbp->var.summarize.tb1p, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;

    addv = malloc(sizeof (RDB_summarize_add) * tbp->var.summarize.addc);
    if (addv == NULL)
        return RDB_NO_MEMORY;
    for (i = 0; i < tbp->var.summarize.addc; i++) {
        addv[i].op = tbp->var.summarize.addv[i].op;
        addv[i].name = tbp->var.summarize.addv[i].name;
    }

    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;
        RDB_table *otbp = _RDB_dup_vtable(tbp->var.summarize.tb2p);
        if (otbp == NULL) {
            free(addv);
            return RDB_NO_MEMORY;
        }

        for (j = 0; j < tbp->var.summarize.addc; j++) {
            if (tbp->var.summarize.addv[j].exp != NULL) {
                addv[j].exp = RDB_dup_expr(tbp->var.summarize.addv[j].exp);
                if (addv[j].exp == NULL)
                    return RDB_NO_MEMORY;
            } else {
                addv[j].exp = NULL;
            }
        }

        ret = RDB_summarize(tbpv[i], otbp, tbp->var.summarize.addc, addv,
                txp, &ntbp);
        if (ret != RDB_OK) {
            for (j = 0; j < tbp->var.summarize.addc; j++)
                RDB_drop_expr(addv[j].exp);
            free(addv);
            return ret;
        }
        tbpv[i] = ntbp;
    }
    free(addv);
    return tbc;
}

static int
mutate_project(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i;
    int ret;
    char **namev;

    if (tbp->var.project.tbp->kind == RDB_TB_REAL
            && tbp->var.project.tbp->stp != NULL
            && tbp->var.project.tbp->stp->indexc > 0) {
        tbc = tbp->var.project.tbp->stp->indexc;
        for (i = 0; i < tbc; i++) {
            _RDB_tbindex *indexp = &tbp->var.project.tbp->stp->indexv[i];
            RDB_table *ptbp = _RDB_dup_vtable(tbp);
            if (ptbp == NULL)
                return RDB_NO_MEMORY;
            ptbp->var.project.indexp = indexp;
            tbpv[i] = ptbp;
        }
    } else {            
        tbc = mutate(tbp->var.project.tbp, tbpv, cap, txp);
        if (tbc < 0)
            return tbc;

        namev = malloc(sizeof (char *) * tbp->typ->var.basetyp->var.tuple.attrc);
        if (namev == NULL)
            return RDB_NO_MEMORY;
        for (i = 0; i < tbp->typ->var.basetyp->var.tuple.attrc; i++)
            namev[i] = tbp->typ->var.basetyp->var.tuple.attrv[i].name;

        for (i = 0; i < tbc; i++) {
            RDB_table *ntbp;

            ret = RDB_project(tbpv[i], tbp->typ->var.basetyp->var.tuple.attrc,
                    namev, &ntbp);
            if (ret != RDB_OK) {
                free(namev);
                return ret;
            }
            tbpv[i] = ntbp;
        }
        free(namev);
    }
    return tbc;
}

static int
common_attrs(RDB_type *tpltyp1, RDB_type *tpltyp2, char **cmattrv)
{
    int i, j;
    int cattrc = 0;

    for (i = 0; i < tpltyp1->var.tuple.attrc; i++) {
        for (j = 0;
             j < tpltyp2->var.tuple.attrc
                     && strcmp(tpltyp1->var.tuple.attrv[i].name,
                     tpltyp2->var.tuple.attrv[j].name) != 0;
             j++);
        if (j < tpltyp2->var.tuple.attrc)
            cmattrv[cattrc++] = tpltyp1->var.tuple.attrv[i].name;
    }
    return cattrc;
}

static int
mutate_join(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc1, tbc2, tbc;
    int i;
    int ret;
    int cmattrc;
    char **cmattrv;

    cmattrv = malloc(sizeof(char *)
            * tbp->var.join.tb1p->typ->var.basetyp->var.tuple.attrc);
    if (cmattrv == NULL)
        return RDB_NO_MEMORY;
    cmattrc = common_attrs(tbp->var.join.tb1p->typ->var.basetyp,
            tbp->var.join.tb2p->typ->var.basetyp, cmattrv);

    tbc1 = mutate(tbp->var.join.tb1p, tbpv, cap, txp);
    if (tbc1 < 0) {
        free(cmattrv);
        return tbc1;
    }
    tbc = 0;
    for (i = 0; i < tbc1; i++) {
        RDB_table *ntbp;

        /*
         * Take project with index only if the index covers the
         * common attributes
         */
        if (tbpv[i]->kind != RDB_TB_PROJECT
                || tbpv[i]->var.project.indexp == NULL
                || attrv_covers_index(cmattrc, cmattrv,
                        tbpv[i]->var.project.indexp)) {
            RDB_table *otbp = _RDB_dup_vtable(tbp->var.join.tb2p);
            if (otbp == NULL) {
                free(cmattrv);
                return RDB_NO_MEMORY;
            }

            ret = RDB_join(otbp, tbpv[i], &ntbp);
            if (ret != RDB_OK) {
                free(cmattrv);
                return ret;
            }
            tbpv[tbc++] = ntbp;
        }
    }
    tbc1 = tbc;
    tbc2 = mutate(tbp->var.join.tb2p, &tbpv[tbc1], cap - tbc1, txp);
    if (tbc2 < 0) {
        free(cmattrv);
        return tbc2;
    }
    tbc = 0;
    for (i = tbc1; i < tbc1 + tbc2; i++) {
        RDB_table *ntbp;

        if (tbpv[i]->kind != RDB_TB_PROJECT
                || tbpv[i]->var.project.indexp == NULL
                || attrv_covers_index(cmattrc, cmattrv,
                        tbpv[i]->var.project.indexp)) {
            RDB_table *otbp = _RDB_dup_vtable(tbp->var.join.tb1p);
            if (otbp == NULL) {
                free(cmattrv);
                return RDB_NO_MEMORY;
            }
            ret = RDB_join(otbp, tbpv[i], &ntbp);
            if (ret != RDB_OK) {
                free(cmattrv);
                return ret;
            }
            tbpv[tbc1 + tbc++] = ntbp;
        }
    }
    tbc = tbc1 + tbc;

    free(cmattrv);
    return tbc;
}

static int
mutate_wrap(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i;
    int ret;

    tbc = mutate(tbp->var.wrap.tbp, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;
    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;

        ret = RDB_wrap(tbpv[i], tbp->var.wrap.wrapc, tbp->var.wrap.wrapv,
                &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc;
}

static int
mutate_unwrap(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i;
    int ret;

    tbc = mutate(tbp->var.unwrap.tbp, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;
    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;

        ret = RDB_unwrap(tbpv[i], tbp->var.unwrap.attrc, tbp->var.unwrap.attrv,
                &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc;
}

static int
mutate_group(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i;
    int ret;

    tbc = mutate(tbp->var.group.tbp, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;
    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;

        ret = RDB_group(tbpv[i], tbp->var.group.attrc, tbp->var.group.attrv,
                tbp->var.group.gattr, &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc;
}

static int
mutate_ungroup(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc;
    int i;
    int ret;

    tbc = mutate(tbp->var.ungroup.tbp, tbpv, cap, txp);
    if (tbc < 0)
        return tbc;
    for (i = 0; i < tbc; i++) {
        RDB_table *ntbp;

        ret = RDB_ungroup(tbpv[i], tbp->var.ungroup.attr, &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc;
}

static int
mutate_sdivide(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    int tbc1, tbc2;
    int i;
    int ret;

    tbc1 = mutate(tbp->var.sdivide.tb1p, tbpv, cap, txp);
    if (tbc1 < 0)
        return tbc1;
    for (i = 0; i < tbc1; i++) {
        RDB_table *ntbp;
        RDB_table *otb1p = _RDB_dup_vtable(tbp->var.sdivide.tb2p);
        RDB_table *otb2p = _RDB_dup_vtable(tbp->var.sdivide.tb3p);
        if (otb1p == NULL || otb2p == NULL)
            return RDB_NO_MEMORY;

        ret = RDB_sdivide(tbpv[i], otb1p, otb2p, &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    tbc2 = mutate(tbp->var.sdivide.tb2p, &tbpv[tbc1], cap - tbc1, txp);
    if (tbc2 < 0)
        return tbc2;
    for (i = tbc1; i < tbc1 + tbc2; i++) {
        RDB_table *ntbp;
        RDB_table *otb1p = _RDB_dup_vtable(tbp->var.sdivide.tb1p);
        RDB_table *otb2p = _RDB_dup_vtable(tbp->var.sdivide.tb3p);
        if (otb1p == NULL || otb2p == NULL)
            return RDB_NO_MEMORY;

        ret = RDB_sdivide(otb1p, tbpv[i], otb2p, &ntbp);
        if (ret != RDB_OK)
            return ret;
        tbpv[i] = ntbp;
    }
    return tbc1 + tbc2;
}

static int
mutate(RDB_table *tbp, RDB_table **tbpv, int cap, RDB_transaction *txp)
{
    switch (tbp->kind) {
        case RDB_TB_REAL:
            return 0;
        case RDB_TB_UNION:
            return mutate_union(tbp, tbpv, cap, txp);
        case RDB_TB_MINUS:
            return mutate_minus(tbp, tbpv, cap, txp);
        case RDB_TB_INTERSECT:
            return mutate_intersect(tbp, tbpv, cap, txp);
        case RDB_TB_SELECT:
            /* Select over index is not further optimized */
            if (tbp->var.select.tbp->kind != RDB_TB_PROJECT
                    || tbp->var.select.tbp->var.project.indexp == NULL)
                return mutate_select(tbp, tbpv, cap, txp);
            return 0;
        case RDB_TB_JOIN:
            return mutate_join(tbp, tbpv, cap, txp);
        case RDB_TB_EXTEND:
            return mutate_extend(tbp, tbpv, cap, txp);
        case RDB_TB_PROJECT:
            return mutate_project(tbp, tbpv, cap, txp);
        case RDB_TB_SUMMARIZE:
            return mutate_summarize(tbp, tbpv, cap, txp);
        case RDB_TB_RENAME:
            return mutate_rename(tbp, tbpv, cap, txp);
        case RDB_TB_WRAP:
            return mutate_wrap(tbp, tbpv, cap, txp);
        case RDB_TB_UNWRAP:
            return mutate_unwrap(tbp, tbpv, cap, txp);
        case RDB_TB_GROUP:
            return mutate_group(tbp, tbpv, cap, txp);
        case RDB_TB_UNGROUP:
            return mutate_ungroup(tbp, tbpv, cap, txp);
        case RDB_TB_SDIVIDE:
            return mutate_sdivide(tbp, tbpv, cap, txp);
    }
    return 0;
}

enum {
    tbpv_cap = 256
};

RDB_bool
_RDB_index_sorts(struct _RDB_tbindex *indexp, int seqitc, const RDB_seq_item seqitv[])
{
    int i;
/*
    if (indexp->unique)
        return RDB_TRUE;
*/
    if (indexp->idxp == NULL || !RDB_index_is_ordered(indexp->idxp)
            || indexp->attrc < seqitc)
        return RDB_FALSE;

    for (i = 0; i < seqitc; i++) {
        if (strcmp(indexp->attrv[i].attrname, seqitv[i].attrname) != 0
                || indexp->attrv[i].asc != seqitv[i].asc)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/*
 * Estimate cost for reading all tuples of the table in the order
 * specified by seqitc/seqitv.
 */
static unsigned
sorted_table_cost(RDB_table *tbp, int seqitc,
        const RDB_seq_item seqitv[])
{
    int cost = table_cost(tbp);

    /* Check if the index must be sorted */
    if (seqitc > 0) {
        _RDB_tbindex *indexp = _RDB_sortindex(tbp);
        if (indexp == NULL || !_RDB_index_sorts(indexp, seqitc, seqitv))
        {
            int scost = (((double) cost) /* !! * log10(cost) */ / 7);

            if (scost == 0)
                scost = 1;
            cost += scost;
        }
    }

    return cost;
}

static int
add_project(RDB_table *tbp)
{
    RDB_table *ptbp;
    int ret;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            break;
        case RDB_TB_MINUS:
            if (tbp->var.minus.tb1p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.minus.tb1p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.minus.tb1p = ptbp;
            } else {
                ret = add_project(tbp->var.minus.tb1p);
                if (ret != RDB_OK)
                    return ret;
            }
            if (tbp->var.minus.tb2p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.minus.tb2p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.minus.tb2p = ptbp;
            } else {
                ret = add_project(tbp->var.minus.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_UNION:
            if (tbp->var._union.tb1p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var._union.tb1p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var._union.tb1p = ptbp;
            } else {
                ret = add_project(tbp->var._union.tb1p);
                if (ret != RDB_OK)
                    return ret;
            }
            if (tbp->var._union.tb2p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var._union.tb2p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var._union.tb2p = ptbp;
            } else {
                ret = add_project(tbp->var._union.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_INTERSECT:
            if (tbp->var.intersect.tb1p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.intersect.tb1p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.intersect.tb1p = ptbp;
            } else {
                ret = add_project(tbp->var.intersect.tb1p);
                if (ret != RDB_OK)
                    return ret;
            }
            if (tbp->var.intersect.tb2p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.intersect.tb2p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.intersect.tb2p = ptbp;
            } else {
                ret = add_project(tbp->var.intersect.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_SELECT:
            if (tbp->var.select.tbp->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.select.tbp);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.select.tbp = ptbp;
            } else {
                ret = add_project(tbp->var.select.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_JOIN:
            if (tbp->var.join.tb1p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.join.tb1p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.join.tb1p = ptbp;
            } else {
                ret = add_project(tbp->var.join.tb1p);
                if (ret != RDB_OK)
                    return ret;
            }
            if (tbp->var.join.tb2p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.join.tb2p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.join.tb2p = ptbp;
            } else {
                ret = add_project(tbp->var.join.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_EXTEND:
            if (tbp->var.extend.tbp->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.extend.tbp);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.extend.tbp = ptbp;
            } else {
                ret = add_project(tbp->var.extend.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_PROJECT:
            if (tbp->var.project.tbp->kind != RDB_TB_REAL) {
                ret = add_project(tbp->var.project.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_SUMMARIZE:
            if (tbp->var.summarize.tb1p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.summarize.tb1p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.summarize.tb1p = ptbp;
            } else {
                ret = add_project(tbp->var.summarize.tb1p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_RENAME:
            if (tbp->var.rename.tbp->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.rename.tbp);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.rename.tbp = ptbp;
            } else {
                ret = add_project(tbp->var.rename.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_WRAP:
            if (tbp->var.wrap.tbp->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.wrap.tbp);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.wrap.tbp = ptbp;
            } else {
                ret = add_project(tbp->var.wrap.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_UNWRAP:
            if (tbp->var.unwrap.tbp->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.unwrap.tbp);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.unwrap.tbp = ptbp;
            } else {
                ret = add_project(tbp->var.unwrap.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_GROUP:
            if (tbp->var.group.tbp->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.group.tbp);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.group.tbp = ptbp;
            } else {
                ret = add_project(tbp->var.group.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_UNGROUP:
            if (tbp->var.ungroup.tbp->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.ungroup.tbp);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.ungroup.tbp = ptbp;
            } else {
                ret = add_project(tbp->var.ungroup.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_SDIVIDE:
            if (tbp->var.sdivide.tb1p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.sdivide.tb1p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.sdivide.tb1p = ptbp;
            } else {
                ret = add_project(tbp->var.sdivide.tb1p);
                if (ret != RDB_OK)
                    return ret;
            }
            if (tbp->var.sdivide.tb2p->kind == RDB_TB_REAL) {
                ptbp = null_project(tbp->var.sdivide.tb2p);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                tbp->var.sdivide.tb2p = ptbp;
            } else {
                ret = add_project(tbp->var.sdivide.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
    }
    return RDB_OK;
}

static void
table_to_empty(RDB_table *tbp) 
{
    /* !! other kinds of tables */
    if (tbp->kind == RDB_TB_SELECT) {
        RDB_drop_expr(tbp->var.select.exp);
        if (RDB_table_name(tbp->var.select.tbp) == NULL) {
            RDB_drop_table(tbp->var.select.tbp, NULL);
        }
        tbp->kind = RDB_TB_REAL;
    }
}

static int
replace_empty(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    /* Check if there is a constraint that says the table is empty */
    if (RDB_table_name(tbp) == NULL
            && RDB_hashtable_get(&txp->dbp->dbrootp->empty_tbtab, tbp, txp)
                    != NULL) {
        table_to_empty(tbp);
    }

    switch (tbp->kind) {
        case RDB_TB_REAL:
            break;
        case RDB_TB_MINUS:
            ret = replace_empty(tbp->var.minus.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = replace_empty(tbp->var.minus.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNION:
            ret = replace_empty(tbp->var._union.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = replace_empty(tbp->var._union.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_INTERSECT:
            ret = replace_empty(tbp->var.intersect.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = replace_empty(tbp->var.intersect.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SELECT:
            ret = replace_empty(tbp->var.select.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_JOIN:
            ret = replace_empty(tbp->var.join.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = replace_empty(tbp->var.join.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_EXTEND:
            ret = replace_empty(tbp->var.extend.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            ret = replace_empty(tbp->var.project.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SUMMARIZE:
            ret = replace_empty(tbp->var.summarize.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = replace_empty(tbp->var.summarize.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_RENAME:
            ret = replace_empty(tbp->var.rename.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_WRAP:
            ret = replace_empty(tbp->var.wrap.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNWRAP:
            ret = replace_empty(tbp->var.unwrap.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_GROUP:
            ret = replace_empty(tbp->var.group.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNGROUP:
            ret = replace_empty(tbp->var.ungroup.tbp, txp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SDIVIDE:
            ret = replace_empty(tbp->var.sdivide.tb1p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = replace_empty(tbp->var.sdivide.tb2p, txp);
            if (ret != RDB_OK)
                return ret;
            ret = replace_empty(tbp->var.sdivide.tb3p, txp);
            if (ret != RDB_OK)
                return ret;
            break;
    }

    return RDB_OK;
}

int
_RDB_optimize(RDB_table *tbp, int seqitc, const RDB_seq_item seqitv[],
        RDB_transaction *txp, RDB_table **ntbpp)
{
    int ret;
    int i;

    if (tbp->kind == RDB_TB_REAL) {
        if (seqitc > 0 && tbp->stp != NULL) {
            /*
             * Check if an index can be used for sorting
             */

            for (i = 0; i < tbp->stp->indexc
                    && !_RDB_index_sorts(&tbp->stp->indexv[i],
                            seqitc, seqitv);
                    i++);
            /* If yes, create projection */
            if (i < tbp->stp->indexc) {
                RDB_table *ptbp = null_project(tbp);
                if (ptbp == NULL)
                    return RDB_NO_MEMORY;
                ptbp->var.project.indexp = &tbp->stp->indexv[i];
                tbp = ptbp;
            }
        }
        *ntbpp = tbp;
    } else {
        RDB_table *ntbp;
        unsigned obestcost, bestcost;
        int bestn;
        int tbc;
        RDB_table *tbpv[tbpv_cap];

        /*
         * Make a copy of the table, so it can be transformed freely
         */
        ntbp = _RDB_dup_vtable(tbp);
        if (ntbp == NULL)
            return RDB_NO_MEMORY;

        /*
         * Algebraic optimization
         */

        ret = _RDB_transform(ntbp);
        if (ret != RDB_OK)
            return ret;

        /*
         * Replace tables which are declared to be empty
         * by a constraint
         */
        if (RDB_tx_db(txp) != NULL) {
            ret = replace_empty(ntbp, txp);
            if (ret != RDB_OK)
                return ret;
        }

        /*
         * Add a project table above real tables
         * to prepare for indexes
         */
        
        ret = add_project(ntbp);
        if (ret != RDB_OK)
            return ret;

        /*
         * Try to find cheapest table
         */

        bestcost = sorted_table_cost(ntbp, seqitc, seqitv);
        do {
            obestcost = bestcost;

            tbc = mutate(ntbp, tbpv, tbpv_cap, txp);
            if (tbc < 0)
                return tbc;

            bestn = -1;

            for (i = 0; i < tbc; i++) {
                int cost = sorted_table_cost(tbpv[i], seqitc, seqitv);

                if (cost < bestcost) {
                    bestcost = cost;
                    bestn = i;
                }
            }
            if (bestn == -1) {
                for (i = 0; i < tbc; i++)
                    RDB_drop_table(tbpv[i], txp);
            } else {
                RDB_drop_table(ntbp, txp);
                ntbp = tbpv[bestn];
                for (i = 0; i < tbc; i++) {
                    if (i != bestn)
                        RDB_drop_table(tbpv[i], txp);
                }
            }
        } while (bestcost < obestcost);
        *ntbpp = ntbp;
    }

    return RDB_OK;
}
