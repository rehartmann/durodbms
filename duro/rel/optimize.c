/*
 * Copyright (C) 2004-2009, 2011-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "optimize.h"
#include "transform.h"
#include "internal.h"
#include "stable.h"
#include "tostr.h"
#include <obj/objinternal.h>
#include <gen/strfns.h>

#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <stdio.h>

RDB_bool
RDB_index_sorts(struct RDB_tbindex *indexp, int seqitc,
        const RDB_seq_item seqitv[])
{
    int i;

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

enum {
    tbpv_cap = 256
};

static RDB_bool
is_and(const RDB_expression *exp)
{
    return RDB_expr_is_binop(exp, "and");
}

static int
alter_op(RDB_expression *exp, const char *name, RDB_exec_context *ecp)
{
    char *newname;

    newname = RDB_realloc(exp->def.op.name, strlen(name) + 1, ecp);
    if (newname == NULL) {
        return RDB_ERROR;
    }
    strcpy(newname, name);
    exp->def.op.name = newname;

    return RDB_OK;
}

/**
 * Remove the (only) child of exp and turn the grandchildren of exp into children
 */
static int
eliminate_child (RDB_expression *exp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *hexp = exp->def.op.args.firstp;
    if (alter_op(exp, name, ecp) != RDB_OK)
        return RDB_ERROR;

    exp->def.op.args.firstp = hexp->def.op.args.firstp;
    RDB_free(hexp->def.op.name);
    RDB_free(hexp);
    return RDB_transform(exp->def.op.args.firstp, NULL, NULL, ecp, txp);
}

/* Try to eliminate NOT operator */
static int
eliminate_not(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *hexp;

    if (exp->kind != RDB_EX_RO_OP)
        return RDB_OK;

    if (strcmp(exp->def.op.name, "not") != 0) {
        RDB_expression *argp = exp->def.op.args.firstp;

        while (argp != NULL) {
            if (eliminate_not(argp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
        }
        return RDB_OK;
    }

    if (exp->def.op.args.firstp->kind != RDB_EX_RO_OP)
        return RDB_OK;

    if (strcmp(exp->def.op.args.firstp->def.op.name, "and") == 0) {
        hexp = RDB_ro_op("not", ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        hexp->nextp = NULL;
        RDB_add_arg(hexp, exp->def.op.args.firstp->def.op.args.firstp->nextp);
        ret = alter_op(exp, "or", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->nextp = hexp;

        ret = alter_op(exp->def.op.args.firstp, "not", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->def.op.args.firstp->nextp = NULL;

        ret = eliminate_not(exp->def.op.args.firstp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
        return eliminate_not(exp->def.op.args.firstp->nextp, ecp, txp);
    }
    if (strcmp(exp->def.op.args.firstp->def.op.name, "or") == 0) {
        hexp = RDB_ro_op("not", ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        hexp->nextp = NULL;
        RDB_add_arg(hexp, exp->def.op.args.firstp->def.op.args.firstp->nextp);
        ret = alter_op(exp, "and", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->nextp = hexp;

        ret = alter_op(exp->def.op.args.firstp, "not", ecp);
        if (ret != RDB_OK)
            return ret;
        exp->def.op.args.firstp->def.op.args.firstp->nextp = NULL;

        ret = eliminate_not(exp->def.op.args.firstp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
        return eliminate_not(exp->def.op.args.firstp->nextp, ecp, txp);
    }
    if (strcmp(exp->def.op.args.firstp->def.op.name, "=") == 0)
        return eliminate_child(exp, "<>", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, "<>") == 0)
        return eliminate_child(exp, "=", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, "<") == 0)
        return eliminate_child(exp, ">=", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, ">") == 0)
        return eliminate_child(exp, "<=", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, "<=") == 0)
        return eliminate_child(exp, ">", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, ">=") == 0)
        return eliminate_child(exp, "<", ecp, txp);
    if (strcmp(exp->def.op.args.firstp->def.op.name, "not") == 0) {
        hexp = exp->def.op.args.firstp;
        memcpy(exp, hexp->def.op.args.firstp, sizeof (RDB_expression));
        RDB_free(hexp->def.op.args.firstp->def.op.name);
        RDB_free(hexp->def.op.args.firstp);
        RDB_free(hexp->def.op.name);
        RDB_free(hexp);
        return eliminate_not(exp->def.op.args.firstp, ecp, txp);;
    }

    return eliminate_not(exp->def.op.args.firstp, ecp, txp);
}

static void
unbalance_and(RDB_expression *exp)
{
    RDB_expression *axp;

    if (!is_and(exp))
        return;

    if (is_and(exp->def.op.args.firstp))
        unbalance_and(exp->def.op.args.firstp);

    if (is_and(exp->def.op.args.firstp->nextp)) {
        unbalance_and(exp->def.op.args.firstp->nextp);
        if (is_and(exp->def.op.args.firstp)) {
            RDB_expression *ax2p;

            /* Find leftmost factor */
            axp = exp->def.op.args.firstp;
            while (is_and(axp->def.op.args.firstp))
                axp = axp->def.op.args.firstp;

            /* Swap leftmost factor and right child */
            ax2p = exp->def.op.args.firstp->nextp;
            ax2p = axp->nextp;
            axp->nextp = NULL;
            exp->def.op.args.firstp->nextp = axp->def.op.args.firstp;
            axp->def.op.args.firstp = ax2p;
        } else {
            /* Swap children */
            axp = exp->def.op.args.firstp;
            exp->def.op.args.firstp = axp->nextp;
            axp->nextp = exp->def.op.args.firstp->nextp;
            exp->def.op.args.firstp->nextp = axp;
        }
    }
}

/*
 * Check if the expression covers all index attributes.
 */
static RDB_bool
expr_covers_index(RDB_expression *exp, RDB_tbindex *indexp)
{
    int i;

    for (i = 0; i < indexp->attrc; i++) {
        if (RDB_attr_node(exp, indexp->attrv[i].attrname, "=") == NULL)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/*
 * Return the number of attributes covered by the index
 */
static int
table_index_attrs(const RDB_type *reltyp, RDB_tbindex *indexp)
{
    int i;

    /* Check if all index attributes appear in attrv */
    for (i = 0; i < indexp->attrc; i++) {
        if (RDB_tuple_type_attr(reltyp->def.basetyp,
                indexp->attrv[i].attrname) == NULL)
            break;
    }
    return i;
}

/*
 * Check if *reltyp covers all index attributes.
 */
static RDB_bool
table_covers_index(const RDB_type *reltyp, RDB_tbindex *indexp)
{
    return (RDB_bool) table_index_attrs(reltyp, indexp) == indexp->attrc;
}

/*
 * Check if *reltyp covers all index attributes, renamed by *renexp.
 */
static RDB_bool
table_covers_index_rename(const RDB_type *reltyp, RDB_tbindex *indexp,
        const RDB_expression *renexp)
{
    int i;
    char *attrnamp;

    for (i = 0; i < indexp->attrc; i++) {
        attrnamp = RDB_rename_attr(indexp->attrv[i].attrname, renexp);

        /* Attribute is not renamed - use original attribute name */
        if (attrnamp == NULL)
            attrnamp = indexp->attrv[i].attrname;

        if (RDB_tuple_type_attr(reltyp->def.basetyp, attrnamp) == NULL)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/**
 * Move node *nodep, which belongs to WHERE expression *texp, to **dstpp.
 * As a result the second argument of *texp may be NULL.
 */
static int
move_node(RDB_expression *texp, RDB_expression **dstpp, RDB_expression *nodep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *parentp;

    /* Get parent node */
    if (nodep == texp->def.op.args.firstp->nextp) {
        parentp = NULL;
    } else {
        parentp = texp->def.op.args.firstp->nextp;
        while (parentp->def.op.args.firstp != nodep
                && parentp->def.op.args.firstp->nextp != nodep) {
            parentp = parentp->def.op.args.firstp;
            if (parentp->kind != RDB_EX_RO_OP) {
                RDB_raise_internal("subexpression not found", ecp);
                return RDB_ERROR;
            }
        }
    }

    /* Remove *nodep from source */
    if (parentp == NULL) {
        texp->def.op.args.firstp->nextp = NULL;
    } else {
        if (parentp == texp->def.op.args.firstp->nextp) {
            if (nodep == parentp->def.op.args.firstp) {
                texp->def.op.args.firstp->nextp = parentp->def.op.args.firstp->nextp;
            } else {
                texp->def.op.args.firstp->nextp = parentp->def.op.args.firstp;
            }
            texp->def.op.args.firstp->nextp->nextp = NULL;
        } else {
            RDB_expression *pparentp = texp->def.op.args.firstp->nextp;

            while (pparentp->def.op.args.firstp != parentp) {
                pparentp = pparentp->def.op.args.firstp;
                if (parentp->kind != RDB_EX_RO_OP) {
                    RDB_raise_internal("subexpression not found", ecp);
                    return RDB_ERROR;
                }
            }
            if (nodep == parentp->def.op.args.firstp) {
                parentp->def.op.args.firstp->nextp->nextp = parentp->nextp;
                pparentp->def.op.args.firstp = parentp->def.op.args.firstp->nextp;
            } else {
                parentp->def.op.args.firstp->nextp = parentp->nextp;
                pparentp->def.op.args.firstp = parentp->def.op.args.firstp;
            }
        }
        RDB_free(parentp->def.op.name);
        RDB_free(parentp);
    }

    if (*dstpp == NULL)
        *dstpp = nodep;
    else {
        RDB_expression *exp = RDB_ro_op("and", ecp);
        if (exp == NULL)
            return RDB_ERROR;
        RDB_add_arg(exp, *dstpp);
        RDB_add_arg(exp, nodep);
        *dstpp = exp;
    }
    return RDB_OK;
}

static int
index_attr_idx(RDB_tbindex *indexp, const char *attrname)
{
    int i;
    for (i = 0;
            i < indexp->attrc && strcmp(attrname, indexp->attrv[i].attrname) != 0;
            i++);
    return i < indexp->attrc ? i : -1;
}

static RDB_expression *
index_like(RDB_expression *condexp, RDB_tbindex *indexp)
{
    if (condexp->kind != RDB_EX_RO_OP)
        return NULL;
    if (strcmp (condexp->def.op.name, "like") == 0) {
        /*
         * Check if expression is of the form attr = <string literal>
         * where attr is the first index attribute
         */
        if (condexp->def.op.args.firstp != NULL
                && condexp->def.op.args.firstp->kind == RDB_EX_VAR
                && index_attr_idx(indexp,
                        condexp->def.op.args.firstp->def.varname) >= 0
                && condexp->def.op.args.firstp->nextp != NULL
                && condexp->def.op.args.firstp->nextp->kind == RDB_EX_OBJ) {
            const char *pattern = RDB_obj_string(
                    &condexp->def.op.args.firstp->nextp->def.obj);
            if (pattern[0] != '\0' && pattern[0] != '*'
                    && pattern[0] != '?') {
                return condexp;
            }
        }
    } else if (strcmp (condexp->def.op.name, "and") == 0
            && condexp->def.op.args.firstp != NULL
            && condexp->def.op.args.firstp->nextp != NULL) {
        RDB_expression *likexp = index_like(condexp->def.op.args.firstp,
                indexp);
        if (likexp != NULL)
            return likexp;
        return index_like(condexp->def.op.args.firstp->nextp,
                indexp);
    }
    return NULL;
}

/*
 * Return the position of the first '?' or '*' in pattern.
 */
static int
like_first_meta(const char *pattern, RDB_exec_context *ecp)
{
    wchar_t wch;
    int nb;
    int offs = 0;

    for (;;) {
        nb = mbtowc(&wch, pattern + offs, MB_CUR_MAX);
        if (nb == -1) {
            RDB_raise_invalid_argument("invalid pattern", ecp);
            return RDB_ERROR;
        }
        if (wch == (wchar_t) 0 || wch == (wchar_t) '*'
                || wch == (wchar_t) '?') {
            return offs;
        }
        offs += nb;
    }

    // Should never be reached
    RDB_raise_internal("like_first_meta()", ecp);
    return RDB_ERROR;
}

static RDB_expression *
var_str_op(const char *varname, const char *str, const char *opname,
        RDB_exec_context *ecp)
{
    RDB_expression *varexp;
    RDB_expression *strexp;
    RDB_expression *opexp = RDB_ro_op(opname, ecp);
    if (opexp == NULL)
        return NULL;
    varexp = RDB_var_ref(varname, ecp);
    if (varexp == NULL)
        goto error;
    RDB_add_arg(opexp, varexp);

    strexp = RDB_string_to_expr(str, ecp);
    if (strexp == NULL)
        goto error;
    RDB_add_arg(opexp, strexp);

    return opexp;

error:
    RDB_del_expr(opexp, ecp);
    return NULL;
}



/* Convert a T WHERE C into T WHERE (attr > <start>) AND C */
static int
add_like_start_stop(RDB_expression *texp, RDB_expression *likexp,
        RDB_exec_context *ecp)
{
    RDB_object startvalobj;
    RDB_expression *andexp;
    RDB_expression *cmpexp = NULL;
    const char *pattern = RDB_obj_string(RDB_expr_obj(
            likexp->def.op.args.firstp->nextp));
    int n = like_first_meta(pattern, ecp);
    if (n == RDB_ERROR)
        return RDB_ERROR;

    RDB_init_obj(&startvalobj);
    if (RDB_string_n_to_obj(&startvalobj,
            pattern, n, ecp) != RDB_OK)
        goto error;

    cmpexp = var_str_op(RDB_expr_var_name(likexp->def.op.args.firstp),
            RDB_obj_string(&startvalobj), "starts_with", ecp);
    if (cmpexp == NULL)
        goto error;

    andexp = RDB_ro_op("and", ecp);
    if (andexp == NULL) {
        goto error;
    }
    RDB_add_arg(andexp, cmpexp);

    cmpexp->nextp = texp->def.op.args.firstp->nextp;
    texp->def.op.args.firstp->nextp = andexp;
    andexp->nextp = NULL;

    RDB_destroy_obj(&startvalobj, ecp);
    return RDB_OK;

error:
     if (cmpexp != NULL)
         RDB_del_expr(cmpexp, ecp);
     RDB_destroy_obj(&startvalobj, ecp);
     return RDB_ERROR;
}

/**
 * Split a WHERE expression into two: one that uses the index specified
 * by indexp (the child) and one which does not (the parent).
 * If the parent condition becomes TRUE, simply convert
 * the selection into a selection which uses the index.
 */
static int
split_by_index(RDB_expression *texp, RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *ixexp = NULL;
    RDB_expression *stopexp = NULL;
    RDB_bool all_eq = RDB_TRUE;
    int objpc = 0;
    RDB_object **objpv;
    RDB_expression *likexp;

    /* If there is a LIKE xx* or LIKE xx?, add a >= 'xx' and < 'xy' */

    likexp = index_like(texp->def.op.args.firstp->nextp, indexp);
    if (likexp != NULL) {
        if (add_like_start_stop(texp, likexp, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    /*
     * Move index-related subexpressions to ixexp.
     * Move subexpressions that are part of the stop expression to stopexp first.
     */
    for (i = 0; i < indexp->attrc && all_eq; i++) {
        RDB_expression *startexp;

        if (indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp)) {
            RDB_expression *stpexp = NULL;

            /* Indexes with inverse order are not supported yet */
            if (!indexp->attrv[i].asc)
                break;

            startexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                    indexp->attrv[i].attrname, "=");
            if (startexp == NULL) {
                all_eq = RDB_FALSE;
                startexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                        indexp->attrv[i].attrname, "starts_with");
                if (startexp == NULL) {
                    startexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                            indexp->attrv[i].attrname, ">=");
                    if (startexp == NULL) {
                        startexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                                indexp->attrv[i].attrname, ">");
                    }

                    /* Get stop expression */
                    stpexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                            indexp->attrv[i].attrname, "<=");
                    if (stpexp == NULL) {
                        stpexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                                indexp->attrv[i].attrname, "<");
                    }

                    if (startexp != NULL) {
                        if (move_node(texp, &ixexp, startexp, ecp, txp) != RDB_OK)
                            return RDB_ERROR;
                        objpc++;
                    }
                    if (stpexp != NULL) {
                        if (move_node(texp, &stopexp, stpexp, ecp, txp) != RDB_OK)
                            return RDB_ERROR;
                    }
                    break;
                }
            }
            stpexp = startexp;
            if (stpexp != NULL) {
                if (move_node(texp, &stopexp, stpexp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }
        } else {
            startexp = RDB_attr_node(texp->def.op.args.firstp->nextp,
                    indexp->attrv[i].attrname, "=");
            if (startexp != NULL) {
                if (move_node(texp, &ixexp, startexp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }
        }

        objpc++;
    }

    /*
     * Add stopexp to ixexp
     * (Note that stopexp is not managed)
     */
    if (stopexp != NULL) {
        if (ixexp == NULL) {
            ixexp = stopexp;
        } else {
            RDB_expression *nixexp = RDB_ro_op("and", ecp);
            if (nixexp == NULL)
                return RDB_ERROR;
            RDB_add_arg(nixexp, ixexp);
            RDB_add_arg(nixexp, stopexp);
            ixexp = nixexp;
        }
    }

    if (objpc > 0) {
        if (texp->def.op.args.firstp->kind == RDB_EX_TBP) {
            objpv = RDB_index_objpv(indexp, ixexp, texp->def.op.args.firstp->def.tbref.tbp->typ,
                    objpc, RDB_TRUE, ecp);
        } else {
            objpv = RDB_index_objpv(indexp, ixexp,
                    texp->def.op.args.firstp->def.op.args.firstp->def.tbref.tbp->typ,
                    objpc, RDB_TRUE, ecp);
        }
        if (objpv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    if (texp->def.op.args.firstp->nextp != NULL) {
        RDB_expression *sitexp, *arg2p;

        /*
         * Split table into two
         */
        if (ixexp == NULL) {
            ixexp = RDB_bool_to_expr(RDB_TRUE, ecp);
            if (ixexp == NULL)
                return RDB_ERROR;
        }

        sitexp = RDB_ro_op("where", ecp);
        if (sitexp == NULL)
            return RDB_ERROR;
        arg2p = texp->def.op.args.firstp->nextp;
        RDB_add_arg(sitexp, texp->def.op.args.firstp);
        RDB_add_arg(sitexp, ixexp);

        if (sitexp->def.op.optinfo.objc != 0) {
            RDB_raise_internal("sitexp->def.op.optinfo.objc != 0", ecp);
            return RDB_ERROR;
        }
        sitexp->def.op.optinfo.objc = objpc;
        sitexp->def.op.optinfo.objpv = objpv;
        sitexp->def.op.optinfo.objv = NULL;
        sitexp->def.op.optinfo.asc = RDB_TRUE;
        sitexp->def.op.optinfo.all_eq = all_eq;
        sitexp->def.op.optinfo.stopexp = stopexp;

        texp->def.op.args.firstp = sitexp;
        sitexp->nextp = arg2p;
    } else {
        /*
         * Convert table to index select
         */
        texp->def.op.args.firstp->nextp = ixexp;
        ixexp->nextp = NULL;
        if (texp->def.op.optinfo.objc != 0) {
            RDB_raise_internal("texp->def.op.optinfo.objc != 0", ecp);
            return RDB_ERROR;
        }

        texp->def.op.optinfo.objc = objpc;
        texp->def.op.optinfo.objpv = objpv;
        texp->def.op.optinfo.objv = NULL;
        texp->def.op.optinfo.asc = RDB_TRUE;
        texp->def.op.optinfo.all_eq = all_eq;
        texp->def.op.optinfo.stopexp = stopexp;
    }
    texp->def.op.args.lastp = texp->def.op.args.firstp->nextp;

    return RDB_OK;
}

/*
 * Check if the expression is a table or a projection/rename over a table
 */
static RDB_bool
table_can_use_index(const RDB_expression *exp, RDB_bool *childp)
{
    if (exp->kind == RDB_EX_TBP) {
        /*
         * Assume that virtual tables have been resolved, so this must be
         * a real (stored) table
         */
        if (childp != NULL)
            *childp = RDB_FALSE;
        return RDB_TRUE;
    }
    if ((RDB_expr_is_op(exp, "project") || RDB_expr_is_op(exp, "rename"))
            && exp->def.op.args.firstp->kind == RDB_EX_TBP) {
        if (childp != NULL)
            *childp = RDB_TRUE;
        return RDB_TRUE;
    }
    return RDB_FALSE;
}

static unsigned
table_cost(const RDB_expression *);

static unsigned
table_est_cardinality(const RDB_expression *exp)
{
    switch(exp->kind) {
    case RDB_EX_TBP:
        return exp->def.tbref.tbp->val.tbp->stp != NULL ?
                exp->def.tbref.tbp->val.tbp->stp->est_cardinality : 0;
    case RDB_EX_OBJ:
        if (exp->def.obj.kind != RDB_OB_TABLE)
            return 1;
        return exp->def.obj.val.tbp->stp != NULL ?
                exp->def.obj.val.tbp->stp->est_cardinality : 0;
    case RDB_EX_VAR:
        return 1;
    case RDB_EX_RO_OP:
        break;
    }

    if (RDB_expr_is_binop(exp, "where")) {
        /*
         * Keeping track of the selectivity is not supported yet,
         * so simply divide the estimated cost by half
         */
        return (table_cost(exp) + 1) / 2;
    }
    return table_cost(exp);
}

static unsigned
table_cost(const RDB_expression *exp)
{
    RDB_tbindex *indexp;
    RDB_bool child;

    if (exp->kind != RDB_EX_RO_OP)
        return table_est_cardinality(exp);

    if (RDB_expr_is_binop(exp, "semiminus")
            || RDB_expr_is_binop(exp, "minus")
            || RDB_expr_is_binop(exp, "semijoin")
            || RDB_expr_is_binop(exp, "intersect")) {
        if (table_can_use_index(exp->def.op.args.firstp->nextp, &child)) {
            if (child) {
                indexp = exp->def.op.args.firstp->nextp->def.op.args.firstp->def.tbref.indexp;
            } else {
                indexp = exp->def.op.args.firstp->nextp->def.tbref.indexp;
            }
            if (indexp != NULL) {
                if (indexp->unique)
                    return table_cost(exp->def.op.args.firstp);
                else
                    return table_cost(exp->def.op.args.firstp) * 2;
            }
        }
        /* No index used, so the 2nd table has to be searched sequentially */
        return table_cost(exp->def.op.args.firstp)
                + table_est_cardinality(exp->def.op.args.firstp)
                        * table_cost(exp->def.op.args.firstp->nextp);
    }

    if (RDB_expr_is_binop(exp, "union"))
        return table_cost(exp->def.op.args.firstp)
                + table_cost(exp->def.op.args.firstp->nextp);

    if (RDB_expr_is_binop(exp, "where")) {
        if (exp->def.op.optinfo.objc == 0 && exp->def.op.optinfo.stopexp == NULL)
            return table_cost(exp->def.op.args.firstp);
        if (exp->def.op.args.firstp->kind == RDB_EX_TBP) {
            indexp = exp->def.op.args.firstp->def.tbref.indexp;
        } else {
            indexp = exp->def.op.args.firstp->def.op.args.firstp->def.tbref.indexp;
        }
        if (indexp == NULL) {
            return table_cost(exp->def.op.args.firstp);
        }
        if (indexp->idxp == NULL)
            return 1;
        if (indexp->unique)
            return 2;
        if (!RDB_index_is_ordered(indexp->idxp))
            return 3;
        return 4;
    }
    if (RDB_expr_is_binop(exp, "join")) {
        if (exp->def.op.args.firstp->nextp->kind == RDB_EX_TBP
                && exp->def.op.args.firstp->nextp->def.tbref.indexp != NULL) {
            indexp = exp->def.op.args.firstp->nextp->def.tbref.indexp;
            if (indexp->idxp == NULL)
                return table_cost(exp->def.op.args.firstp);
            if (indexp->unique)
                return table_cost(exp->def.op.args.firstp) * 2;
            if (!RDB_index_is_ordered(indexp->idxp))
                return table_cost(exp->def.op.args.firstp) * 3;
            return table_cost(exp->def.op.args.firstp) * 4;
        }
        return table_cost(exp->def.op.args.firstp)
                * table_cost(exp->def.op.args.firstp->nextp);
    }
    if (strcmp(exp->def.op.name, "extend") == 0
             || strcmp(exp->def.op.name, "project") == 0
             || strcmp(exp->def.op.name, "remove") == 0
             || strcmp(exp->def.op.name, "summarize") == 0
             || strcmp(exp->def.op.name, "rename") == 0
             || strcmp(exp->def.op.name, "wrap") == 0
             || strcmp(exp->def.op.name, "unwrap") == 0
             || strcmp(exp->def.op.name, "group") == 0
             || strcmp(exp->def.op.name, "ungroup") == 0)
        return table_cost(exp->def.op.args.firstp);
    if (strcmp(exp->def.op.name, "divide") == 0) {
        return table_cost(exp->def.op.args.firstp)
                * table_cost(exp->def.op.args.firstp->nextp); /* !! */
    }
    if (strcmp(exp->def.op.name, "is_empty") == 0
            || strcmp(exp->def.op.name, "count") == 0)
        return table_cost(exp->def.op.args.firstp);

    /* Other operator */
    if (exp->def.op.args.firstp != NULL)
        return table_cost(exp->def.op.args.firstp);

    return 0;
}

static int
mutate(RDB_expression *exp, RDB_expression **tbpv, int cap, RDB_expression *,
        RDB_exec_context *, RDB_transaction *);

static int
mutate_where(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int tbc;
    RDB_expression *chexp = texp->def.op.args.firstp;
    RDB_expression *condp = texp->def.op.args.firstp->nextp;

    if (chexp->kind == RDB_EX_TBP
        || (RDB_expr_is_op(chexp, "project")
            && chexp->def.op.args.firstp->kind == RDB_EX_TBP)) {
        if (eliminate_not(condp, ecp, txp) != RDB_OK)
            return RDB_ERROR;

        /* Convert condition into 'unbalanced' form */
        unbalance_and(condp);
    }

    tbc = mutate(chexp, tbpv, cap, empty_exp, ecp, txp);
    if (tbc < 0)
        return tbc;

    for (i = 0; i < tbc; i++) {
        RDB_expression *nexp;
        RDB_expression *exp = RDB_dup_expr(condp, ecp);
        if (exp == NULL)
            return RDB_ERROR;

        nexp = RDB_ro_op("where", ecp);
        if (nexp == NULL) {
            RDB_del_expr(exp, ecp);
            return RDB_ERROR;
        }
        RDB_add_arg(nexp, tbpv[i]);
        RDB_add_arg(nexp, exp);

        /*
         * If the child table is a stored table or a
         * projection of a stored table, try to use an index
         */
        if (tbpv[i]->kind == RDB_EX_TBP
                && tbpv[i]->def.tbref.indexp != NULL)
        {
            RDB_tbindex *indexp = tbpv[i]->def.tbref.indexp;
            if ((indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp))
                    || expr_covers_index(exp, indexp)) {
                if (split_by_index(nexp, indexp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }
        } else if (RDB_expr_is_op(tbpv[i], "project")
                && tbpv[i]->def.op.args.firstp->kind == RDB_EX_TBP
                && tbpv[i]->def.op.args.firstp->def.tbref.indexp != NULL) {
            RDB_tbindex *indexp = tbpv[i]->def.op.args.firstp->def.tbref.indexp;
            if ((indexp->idxp != NULL && RDB_index_is_ordered(indexp->idxp))
                    || expr_covers_index(exp, indexp)) {
                if (split_by_index(nexp, indexp, ecp, txp) != RDB_OK)
                    return RDB_ERROR;
            }
        }
        tbpv[i] = nexp;
    }
    return tbc;
}

static int
mutate_unary(RDB_expression *exp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int tbc = mutate(exp->def.op.args.firstp, tbpv, cap, empty_exp, ecp, txp);
    if (tbc <= 0)
        return tbc;

    for (i = 0; i < tbc; i++) {
        RDB_expression *nexp = RDB_ro_op(exp->def.op.name, ecp);
        if (nexp == NULL)
            return RDB_ERROR;
        RDB_add_arg(nexp, tbpv[i]);
        tbpv[i] = nexp;
    }
    return tbc;
}

static RDB_expression *
dup_expr_deep(const RDB_expression *, RDB_exec_context *,
        RDB_transaction *);

static RDB_expression *
dup_ro_op_expr_deep(const RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *argp, *nargp;

    RDB_expression *newexp = RDB_ro_op(exp->def.op.name, ecp);
    if (newexp == NULL)
        return NULL;

    if (strcmp(exp->def.op.name, ".") == 0
            && RDB_expr_list_length(&exp->def.op.args) == 2) {
        /* Special treatment for '.' operator - do not try to resolve attribute name */
        nargp = dup_expr_deep(exp->def.op.args.firstp, ecp, txp);
        if (nargp == NULL) {
            RDB_del_expr(newexp, ecp);
            return NULL;
        }
        RDB_add_arg(newexp, nargp);

        nargp = RDB_dup_expr(exp->def.op.args.firstp->nextp, ecp);
        if (nargp == NULL) {
            RDB_del_expr(newexp, ecp);
            return NULL;
        }
        RDB_add_arg(newexp, nargp);
    } else if (strcmp(exp->def.op.name, "extend") == 0
            || strcmp(exp->def.op.name, "update") == 0
           || RDB_expr_is_binop(exp, "where")) {
        /* In the case of extend, update, and where, only do deep copy on 1st argument */
        argp = exp->def.op.args.firstp;
        if (argp != NULL) {
            nargp = dup_expr_deep(argp, ecp, txp);
            if (nargp == NULL) {
                RDB_del_expr(newexp, ecp);
                return NULL;
            }
            RDB_add_arg(newexp, nargp);
            argp = argp->nextp;
            while (argp != NULL) {
                nargp = RDB_dup_expr(argp, ecp);
                if (nargp == NULL) {
                    RDB_del_expr(newexp, ecp);
                    return NULL;
                }
                RDB_add_arg(newexp, nargp);
                argp = argp->nextp;
            }
        }
    } else if (strcmp(exp->def.op.name, "summarize") == 0) {
        /* In the case of summarize, only do deep copy on first 2 arguments */
        argp = exp->def.op.args.firstp;
        if (argp != NULL) {
            nargp = dup_expr_deep(argp, ecp, txp);
            if (nargp == NULL) {
                RDB_del_expr(newexp, ecp);
                return NULL;
            }
            RDB_add_arg(newexp, nargp);
            argp = argp->nextp;
            if (argp != NULL) {
                nargp = dup_expr_deep(argp, ecp, txp);
                if (nargp == NULL) {
                    RDB_del_expr(newexp, ecp);
                    return NULL;
                }
                RDB_add_arg(newexp, nargp);
                argp = argp->nextp;
                while (argp != NULL) {
                    nargp = RDB_dup_expr(argp, ecp);
                    if (nargp == NULL) {
                        RDB_del_expr(newexp, ecp);
                        return NULL;
                    }
                    RDB_add_arg(newexp, nargp);
                    argp = argp->nextp;
                }
            }
        }
    } else {
        argp = exp->def.op.args.firstp;
        while (argp != NULL) {
            nargp = dup_expr_deep(argp, ecp, txp);
            if (nargp == NULL) {
                RDB_del_expr(newexp, ecp);
                return NULL;
            }
            RDB_add_arg(newexp, nargp);
            argp = argp->nextp;
        }
    }
    return newexp;
}

/*
 * Copy expression, resolving table names and making a copy
 * of virtual tables (recursively)
 */
static RDB_expression *
dup_expr_deep(const RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *newexp;

    switch (exp->kind) {
    case RDB_EX_RO_OP:
        newexp = dup_ro_op_expr_deep(exp, ecp, txp);
        break;
    case RDB_EX_OBJ:
        newexp = RDB_obj_to_expr(&exp->def.obj, ecp);
        break;
    case RDB_EX_TBP:
        if (RDB_TB_CHECK & exp->def.tbref.tbp->val.tbp->flags) {
            if (RDB_check_table(exp->def.tbref.tbp, ecp, txp) != RDB_OK)
                return NULL;
        }

        if (RDB_table_is_real(exp->def.tbref.tbp)) {
            newexp = RDB_table_ref(exp->def.tbref.tbp, ecp);
        } else {
            newexp = dup_expr_deep(RDB_vtable_expr(exp->def.tbref.tbp),
                    ecp, txp);
        }
        break;
    case RDB_EX_VAR:
        /*
         * Resolve table name.
         * If the name refers to a virtual table, convert it
         * to its defining expression
         */
        newexp = NULL;
        if (txp != NULL
                && (exp->typ == NULL || RDB_type_is_relation(exp->typ))) {
            RDB_object *tbp = RDB_get_table(RDB_expr_var_name(exp),
                    ecp, txp);
            if (tbp != NULL) {
                if (RDB_table_is_real(tbp)) {
                    newexp = RDB_table_ref(tbp, ecp);
                } else {
                    newexp = dup_expr_deep(RDB_vtable_expr(tbp), ecp, txp);
                }
                if (newexp == NULL)
                    return NULL;
            } else {
                if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NAME_ERROR) {
                    /* Ignore error */
                } else {
                    return NULL;
                }
            }
        }
        if (newexp == NULL)
            newexp = RDB_var_ref(RDB_expr_var_name(exp), ecp);
        break;
    }
    if (newexp == NULL)
        return NULL;
        
    if (exp->typ != NULL) {
        newexp->typ = RDB_dup_nonscalar_type(exp->typ, ecp);
        if (newexp->typ == NULL) {
            RDB_del_expr(newexp, ecp);
            return NULL;
        }
    }
    return newexp;
}

/*
 * Call mutate for the nargc first arguments
 */
static int
mutate_vt(RDB_expression *texp, int nargc, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j, k;
    RDB_expression *argp;
    int ntbc;
    int otbc = 0;
    int argc = RDB_expr_list_length(&texp->def.op.args);

    argp = texp->def.op.args.firstp;
    for (j = 0; j < nargc; j++) {
        ntbc = mutate(argp, &tbpv[otbc], cap - otbc, empty_exp, ecp, txp);
        if (ntbc < 0)
            return ntbc;
        for (i = otbc; i < otbc + ntbc; i++) {
            RDB_expression *argp2;
            RDB_expression *nexp = RDB_ro_op(texp->def.op.name, ecp);
            if (nexp == NULL)
                return RDB_ERROR;

            argp2 = texp->def.op.args.firstp;
            for (k = 0; k < argc; k++) {
                if (k == j) {
                    RDB_add_arg(nexp, tbpv[i]);
                } else {
                    RDB_expression *otexp = RDB_dup_expr(argp2, ecp);
                    if (otexp == NULL) {
                        RDB_del_expr(nexp, ecp);
                        return RDB_ERROR;
                    }
                    RDB_add_arg(nexp, otexp);
                }
                argp2 = argp2->nextp;
            }

            /* If the resulting expression is known to be empty, replace it */
            if (empty_exp != NULL) {
                RDB_bool iseq;
                if (RDB_expr_equals(nexp, empty_exp, ecp, txp, &iseq) != RDB_OK) {
                    RDB_del_expr(nexp, ecp);
                    return RDB_ERROR;
                }
                if (iseq) {
                    if (RDB_expr_to_empty_table(nexp, ecp, txp) != RDB_OK) {
                        RDB_del_expr(nexp, ecp);
                        return RDB_ERROR;
                    }
                }
            }
            tbpv[i] = nexp;
        }
        otbc += ntbc;
        argp = argp->nextp;
    }
    return otbc;
}

static int
mutate_full_vt(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    return mutate_vt(texp, RDB_expr_list_length(&texp->def.op.args), tbpv,
            cap, empty_exp, ecp, txp);
}

/*
 * Check if *exp is a subset of *ex2p.
 * If the check says *exp is also empty set *resultp to RDB_TRUE.
 * Otherwise, set *resultp to RDB_FALSE. (Note that doesn't necessarily
 * mean that *exp is nonempty)
 */
static int
expr_is_subset(RDB_expression *exp, RDB_expression *ex2p,
        RDB_bool *resultp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Check for equality */
    if (RDB_expr_equals(exp, ex2p, ecp, txp, resultp) != RDB_OK)
        return RDB_ERROR;
    if (*resultp)
        return RDB_OK;

    if (RDB_expr_is_binop(exp, "minus") && RDB_expr_is_binop(ex2p, "minus")) {
        /*
         * Handle that case that both expressions are a MINUS operator invocation
         * If exp #1 is of form T1 MINUS T2
         * and exp #2 is T3 MINUS T4 then exp #1 is a subset of exp #2
         * if T1 is a subset of T3 and T4 is a subset of T2
         */
        RDB_bool r1, r2;
        if (expr_is_subset(exp->def.op.args.firstp,
                ex2p->def.op.args.firstp, &r1, ecp, txp) != RDB_OK)
            return RDB_ERROR;
        if (expr_is_subset(ex2p->def.op.args.firstp->nextp,
                exp->def.op.args.firstp->nextp, &r2, ecp, txp) != RDB_OK)
            return RDB_ERROR;
        *resultp = (RDB_bool) (r1 && r2);
    } else if (RDB_expr_is_op(exp, "project")
            && RDB_expr_is_op(ex2p, "project")) {
        /*
         * If both expressions are projections they are subsets if they are
         * of the same type and their arg #1 are subsets
         */
        RDB_type *typ1, *typ2;
        typ1 = RDB_expr_type(exp, NULL, NULL, NULL, ecp, txp);
        if (typ1 == NULL)
            return RDB_ERROR;
        typ2 = RDB_expr_type(ex2p, NULL, NULL, NULL, ecp, txp);
        if (typ2 == NULL)
            return RDB_ERROR;
        if (RDB_type_equals(typ1, typ2)) {
            return expr_is_subset(exp->def.op.args.firstp,
                    ex2p->def.op.args.firstp, resultp, ecp, txp);
        }
    } else if (RDB_expr_is_op(exp, "where")) {
        return expr_is_subset(exp->def.op.args.firstp,
                ex2p, resultp, ecp, txp);
    }
    return RDB_OK;
}

/*
 * If *exp or a subexpression of *exp is a subset of *empty_exp,
 * it must be empty, so replace it with an empty relation.
 * *empty_exp must not be NULL.
 */
static int
replace_empty(RDB_expression *exp, RDB_expression *empty_exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_bool issubset;
    /* If *exp is a subset of *empty_exp it must be empty too */
    if (expr_is_subset(exp, empty_exp, &issubset, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (issubset) {
        return RDB_expr_to_empty_table(exp, ecp, txp);
    }
    if (exp->kind == RDB_EX_RO_OP) {
        RDB_expression *argp = exp->def.op.args.firstp;

        while (argp != NULL) {
            if (replace_empty(argp, empty_exp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
    	}
    }
    return RDB_OK;
}

/*
 * (T1 UNION T2) [SEMI]MINUS T3 -> (T1 [SEMI]MINUS T3) UNION (T2 [SEMI]MINUS T3)
 * Returns the result.
 * Useful when there is a constraint of the form IS_EMPTY(T1 SEMIMINUS T2)
 * so one of the children can be optimized away.
 * empty_exp points to an expression declared to be empty
 * by a constraint.
 */
static RDB_expression *
transform_semi_minus_union1(RDB_expression *texp, RDB_expression *empty_exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *ex1p, *ex2p, *ex3p, *ex4p;
    RDB_expression *resexp;

    ex1p = RDB_dup_expr(texp->def.op.args.firstp->def.op.args.firstp, ecp);
    if (ex1p == NULL) {
        return NULL;
    }

    ex2p = RDB_dup_expr(texp->def.op.args.firstp->nextp, ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex1p, ecp);
        return NULL;
    }

    ex3p = RDB_ro_op(texp->def.op.name, ecp);
    if (ex3p == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex2p, ecp);
        return NULL;
    }
    /* Make *ex3p T1 [SEMI]MINUS T3 */
    RDB_add_arg(ex3p, ex1p);
    RDB_add_arg(ex3p, ex2p);

    ex1p = RDB_dup_expr(texp->def.op.args.firstp->def.op.args.firstp->nextp, ecp);
    if (ex1p == NULL) {
        RDB_del_expr(ex3p, ecp);
        return NULL;
    }

    ex2p = RDB_dup_expr(texp->def.op.args.firstp->nextp, ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex3p, ecp);
        return NULL;
    }

    ex4p = RDB_ro_op(texp->def.op.name, ecp);
    if (ex4p == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex2p, ecp);
        RDB_del_expr(ex3p, ecp);
        return NULL;
    }
    /* Make *ex4p T2 [SEMI]MINUS T3 */
    RDB_add_arg(ex4p, ex1p);
    RDB_add_arg(ex4p, ex2p);

    resexp = RDB_ro_op("union", ecp);
    if (resexp == NULL) {
        RDB_del_expr(ex3p, ecp);
        RDB_del_expr(ex4p, ecp);
        return NULL;
    }
    RDB_add_arg(resexp, ex3p);
    RDB_add_arg(resexp, ex4p);

    ret = replace_empty(resexp, empty_exp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_del_expr(resexp, ecp);
        return NULL;
    }
    return resexp;
}

/*
 * T1 [SEMI]MINUS (T2 UNION T3) -> (T1 [SEMI]MINUS T2) [SEMI]MINUS T3
 * Returns the result.
 * Useful when there is a constraint of the form IS_EMPTY(T1 SEMIMINUS T2)
 * to optimize the constraint checking of an insert into T2.
 */
static RDB_expression *
transform_semi_minus_union2(RDB_expression *texp, RDB_expression *empty_exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *ex1p, *ex2p, *ex3p;

    ex1p = RDB_dup_expr(texp->def.op.args.firstp, ecp);
    if (ex1p == NULL)
        return NULL;
    ex2p = RDB_dup_expr(texp->def.op.args.firstp->nextp->def.op.args.firstp,
            ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex1p, ecp);
        return NULL;
    }
    ex3p = RDB_ro_op(texp->def.op.name, ecp);
    if (ex3p == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex2p, ecp);
        return NULL;
    }
    RDB_add_arg(ex3p, ex1p);
    RDB_add_arg(ex3p, ex2p);

    ex2p = RDB_dup_expr(
            texp->def.op.args.firstp->nextp->def.op.args.firstp->nextp,
            ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex3p, ecp);
        return NULL;
    }

    ex1p = RDB_ro_op(texp->def.op.name, ecp);
    if (ex1p == NULL) {
        RDB_del_expr(ex3p, ecp);
        RDB_del_expr(ex2p, ecp);
        return NULL;
    }

    RDB_add_arg(ex1p, ex3p);
    RDB_add_arg(ex1p, ex2p);

    ret = replace_empty(ex1p, empty_exp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_del_expr(ex1p, ecp);
        return NULL;
    }
    return ex1p;
}

/*
 * Transform *texp of form T1 MINUS (T3 UNION T4) UNION T2 MINUS (T3 UNION T4)
 * further to ((T1 MINUS T3) MINUS T4) UNION ((T2 MINUS T3) MINUS T4)
 */
static RDB_expression *
transform_semi_minus_union3(RDB_expression *texp, RDB_expression *empty_exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *ex1p, *ex2p, *rexp;

    ex1p = transform_semi_minus_union2(texp->def.op.args.firstp,
            empty_exp, ecp, txp);
    if (ex1p == NULL)
        return NULL;
    ex2p = transform_semi_minus_union2(texp->def.op.args.firstp->nextp,
            empty_exp, ecp, txp);
    if (ex2p == NULL) {
        RDB_del_expr(ex1p, ecp);
        return NULL;
    }
    rexp = RDB_ro_op("union", ecp);
    if (rexp == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex2p, ecp);
        return NULL;
    }
    RDB_add_arg(rexp, ex1p);
    RDB_add_arg(rexp, ex2p);
    if (replace_empty(ex1p, empty_exp, ecp, txp) != RDB_OK) {
        RDB_del_expr(rexp, ecp);
        return NULL;
    }
    return rexp;
}

static RDB_expression *
op_from_copy(const char *opname, RDB_expression *ex1p, RDB_expression *ex2p,
        RDB_exec_context *ecp)
{
    RDB_expression *ex1cp;
    RDB_expression *ex2cp = NULL;
    RDB_expression *rexp;

    ex1cp = RDB_dup_expr(ex1p, ecp);
    if (ex1cp == NULL)
        goto error;
    if (ex2p != NULL) {
        ex2cp = RDB_dup_expr(ex2p, ecp);
        if (ex2cp == NULL)
            goto error;
    }

    rexp = RDB_ro_op(opname, ecp);
    if (rexp == NULL)
        goto error;
    RDB_add_arg(rexp, ex1cp);
    if (ex2p != NULL)
        RDB_add_arg(rexp, ex2cp);
    return rexp;

error:
    if (ex1cp == NULL)
        RDB_del_expr(ex1cp, ecp);
    if (ex2cp == NULL)
        RDB_del_expr(ex2cp, ecp);
    return NULL;
}

/*
 * T1 MINUS (T2 WHERE C) -> (T1 MINUS T2) UNION (T2 WHERE NOT C INTERSECT T1)
 */
static RDB_expression *
transform_minus_where(RDB_expression *texp, RDB_expression *empty_exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *rexp;
    RDB_expression *ex1p = NULL;
    RDB_expression *ex2p = NULL;
    RDB_expression *ex3p = NULL;
    RDB_expression *ex4p;
    RDB_expression *condp = NULL;

    ex1p = op_from_copy("minus", texp->def.op.args.firstp,
            texp->def.op.args.firstp->nextp->def.op.args.firstp, ecp);
    if (ex1p == NULL)
        return NULL;

    condp = op_from_copy("not", texp->def.op.args.firstp->nextp->def.op.args.firstp->nextp,
            NULL, ecp);
    if (condp == NULL)
        goto error;

    ex2p = RDB_dup_expr(texp->def.op.args.firstp->nextp->def.op.args.firstp, ecp);
    if (ex2p == NULL)
        goto error;

    ex3p = RDB_ro_op("where", ecp);
    if (ex3p == NULL)
        goto error;
    RDB_add_arg(ex3p, ex2p);
    RDB_add_arg(ex3p, condp);
    condp = NULL;

    ex2p = RDB_dup_expr(texp->def.op.args.firstp, ecp);
    if (ex2p == NULL)
        goto error;

    ex4p = RDB_ro_op("intersect", ecp);
    if (ex4p == NULL)
        goto error;
    RDB_add_arg(ex4p, ex3p);
    RDB_add_arg(ex4p, ex2p);

    ex2p = ex4p;
    ex3p = NULL;

    rexp = RDB_ro_op("union", ecp);
    if (rexp == NULL)
        goto error;
    RDB_add_arg(rexp, ex1p);
    RDB_add_arg(rexp, ex2p);
    if (replace_empty(ex1p, empty_exp, ecp, txp) != RDB_OK) {
        RDB_del_expr(rexp, ecp);
        goto error;
    }

    return rexp;

error:
    if (ex1p != NULL)
        RDB_del_expr(ex1p, ecp);
    if (ex2p != NULL)
        RDB_del_expr(ex2p, ecp);
    if (ex3p != NULL)
        RDB_del_expr(ex3p, ecp);
    if (condp != NULL)
        RDB_del_expr(condp, ecp);
    return NULL;
}

/*
 * If the second table is a stored table, try to find unique indexes that
 * cover the first table.
 * Otherwise simply mutate the child tables.
 */
static int
mutate_matching_index(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int tbc;
    RDB_bool child;

    /*
     * If the second table is a stored table or a projection over a stored table
     * (assuming virtual tables have been resolved), try to use an index
     */
    if (table_can_use_index(texp->def.op.args.firstp->nextp, &child)) {
        RDB_type *tb1typ;
        RDB_object *tbp = child ?
                texp->def.op.args.firstp->nextp->def.op.args.firstp->def.tbref.tbp
                : texp->def.op.args.firstp->nextp->def.tbref.tbp;

        /* Use unique indexes which cover the attributes of the first argument */
        RDB_stored_table *stbp = tbp->val.tbp->stp;
        if (stbp == NULL)
            return RDB_OK;

        tb1typ = RDB_expr_type(texp->def.op.args.firstp, NULL, NULL, NULL, ecp, txp);
        if (tb1typ == NULL)
            return RDB_ERROR;
        tbc = 0;
        /* Add a copy to tbpv for each index */
        for (i = 0; i < stbp->indexc && tbc < cap; i++) {
            if (table_covers_index(tb1typ, &stbp->indexv[i])) {
                tbpv[tbc] = op_from_copy(texp->def.op.name, texp->def.op.args.firstp,
                        texp->def.op.args.firstp->nextp, ecp);
                if (tbpv[tbc] == NULL)
                    return RDB_ERROR;
                /* Set index for second argument */
                if (child) {
                    /* Check if index covers type of table #2 */
                    RDB_type *tb2typ = RDB_expr_type(texp->def.op.args.firstp, NULL, NULL, NULL, ecp, txp);
                    if (tb2typ == NULL)
                        return RDB_ERROR;
                    if (table_covers_index(tb1typ, &stbp->indexv[i])) {
                        tbpv[tbc]->def.op.args.firstp->nextp->def.op.args.firstp
                                ->def.tbref.indexp = &stbp->indexv[i];
                        tbc++;
                    }
                } else {
                    tbpv[tbc]->def.op.args.firstp->nextp->def.tbref.indexp =
                            &stbp->indexv[i];
                    tbc++;
                }
            }
        }

        if (tbc < cap) {
            /* Vary first argument */
            int ntbc = mutate_vt(texp, 1, tbpv + tbc, cap - tbc, empty_exp, ecp, txp);
            if (ntbc < 0)
                return RDB_ERROR;
            tbc += ntbc;
        }
    } else {
        tbc = mutate_vt(texp, 2, tbpv, cap, empty_exp, ecp, txp);
        if (tbc < 0)
            return RDB_ERROR;
    }

    return tbc;
}

/*
 * empty_exp may be NULL.
 */
static int
mutate_semi_minus(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc = mutate_matching_index(texp, tbpv, cap, empty_exp, ecp, txp);
    if (tbc < 0)
        return tbc;

    if (empty_exp == NULL)
        return tbc;

    if (tbc < cap && RDB_expr_is_binop(texp->def.op.args.firstp, "union")) {
        tbpv[tbc] = transform_semi_minus_union1(texp, empty_exp, ecp, txp);
        if (tbpv[tbc] == NULL)
            return RDB_ERROR;
        tbc++;
        if (tbc < cap
                && RDB_expr_is_binop(tbpv[tbc - 1], "union")
                && RDB_expr_is_binop(tbpv[tbc - 1]->def.op.args.firstp, "minus")
                && RDB_expr_is_binop(tbpv[tbc - 1]->def.op.args.firstp
                        ->def.op.args.firstp->nextp,
                        "union")
                && RDB_expr_is_binop(tbpv[tbc - 1]->def.op.args.firstp->nextp, "minus")
                && RDB_expr_is_binop(tbpv[tbc - 1]->def.op.args.firstp->nextp
                        ->def.op.args.firstp->nextp,
                        "union"))
        {
            /*
             * Expression was of form (T1 UNION T2) MINUS (T3 UNION T4)
             * and tbpv[tbc - 1] is now
             * (T1 MINUS (T3 UNION T4)) UNION (T2 MINUS (T3 UNION T4))
             * Transform expression further to
             * ((T1 MINUS T3) MINUS T4) UNION ((T2 MINUS T3) MINUS T4)
             */
            tbpv[tbc] = transform_semi_minus_union3(tbpv[tbc - 1], empty_exp, ecp, txp);
            if (tbpv[tbc] == NULL) {
                RDB_del_expr(tbpv[tbc - 1], ecp);
                return RDB_ERROR;
            }
            tbc++;
        }
    }
    if (tbc < cap && RDB_expr_is_binop(texp->def.op.args.firstp->nextp, "union")) {
        tbpv[tbc] = transform_semi_minus_union2(texp, empty_exp, ecp, txp);
        if (tbpv[tbc] == NULL)
            return RDB_ERROR;
        tbc++;
    }
    if (tbc < cap && strcmp(texp->def.op.name, "minus") == 0
            && RDB_expr_is_binop(texp->def.op.args.firstp->nextp, "where")) {
        tbpv[tbc] = transform_minus_where(texp, empty_exp, ecp, txp);
        if (tbpv[tbc] == NULL)
            return RDB_ERROR;
        tbc++;
    }
    return tbc;
}

static int
index_joins(RDB_expression *otexp, RDB_expression *itexp, 
        RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc;
    int i;
    RDB_object *tbp;
    RDB_type *ottyp = RDB_expr_type(otexp, NULL, NULL, NULL, ecp, txp);
    if (ottyp == NULL)
        return RDB_ERROR;

    if (itexp->kind == RDB_EX_TBP) {
        tbp = itexp->def.tbref.tbp;
    } else {
        tbp = itexp->def.op.args.firstp->def.tbref.tbp;
    }

    if (!RDB_table_is_stored(tbp))
        return 0;

    if (tbp->kind == RDB_OB_TABLE && tbp->val.tbp->stp == NULL) {
        if (RDB_provide_stored_table(tbp, RDB_TRUE, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }

    /*
     * Use indexes of table #2 which cover table #1
     */

    tbc = 0;
    for (i = 0; i < tbp->val.tbp->stp->indexc && tbc < cap; i++) {
        RDB_bool useindex;

        if (RDB_expr_is_op(itexp, "rename")) {
            useindex = table_covers_index_rename(ottyp, &tbp->val.tbp->stp->indexv[i],
                    itexp);
        } else {
            useindex = table_covers_index(ottyp, &tbp->val.tbp->stp->indexv[i]);
        }

        if (useindex) {
            RDB_expression *arg1p, *arg2p;
            RDB_expression * ntexp = RDB_ro_op("join", ecp);
            if (ntexp == NULL) {
                return RDB_ERROR;
            }

            arg1p = RDB_dup_expr(otexp, ecp);
            if (arg1p == NULL) {
                RDB_del_expr(ntexp, ecp);
                return RDB_ERROR;
            }
            RDB_add_arg(ntexp, arg1p);

            arg2p = RDB_dup_expr(itexp, ecp);
            if (arg2p == NULL) {
                RDB_del_expr(ntexp, ecp);
                return RDB_ERROR;
            }
            RDB_add_arg(ntexp, arg2p);

            if (itexp->kind == RDB_EX_TBP) {
                itexp->def.tbref.indexp = &tbp->val.tbp->stp->indexv[i];
            } else {
                itexp->def.op.args.firstp->def.tbref.indexp
                        = &tbp->val.tbp->stp->indexv[i];
            }

            tbpv[tbc++] = ntexp;
        }
    }

    return tbc;
}

static int
mutate_join(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int tbc = 0;

    if (texp->def.op.args.firstp->kind != RDB_EX_TBP
            && texp->def.op.args.firstp->nextp->kind != RDB_EX_TBP) {
        return mutate_full_vt(texp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (texp->def.op.args.firstp->nextp->kind == RDB_EX_TBP
            || (RDB_expr_is_op(texp->def.op.args.firstp->nextp, "rename")
               && texp->def.op.args.firstp->nextp->def.op.args.firstp->kind == RDB_EX_TBP)) {
        /* Arg #2 is stored table or rename over stored table */
        tbc = index_joins(texp->def.op.args.firstp, texp->def.op.args.firstp->nextp,
                tbpv, cap, ecp, txp);
        if (tbc == RDB_ERROR)
            return RDB_ERROR;
    }

    if (texp->def.op.args.firstp->kind == RDB_EX_TBP
            || (RDB_expr_is_op(texp->def.op.args.firstp, "rename")
               && texp->def.op.args.firstp->def.op.args.firstp->kind == RDB_EX_TBP)) {
        /*
         * Arg #1 is stored table or rename over stored table,
         * reverse order of arguments
         */
        int ret = index_joins(texp->def.op.args.firstp->nextp, texp->def.op.args.firstp,
                tbpv + tbc, cap - tbc, ecp, txp);
        if (ret == RDB_ERROR)
            return RDB_ERROR;
        tbc += ret;
    }
    return tbc;
}

static int
mutate_tbref(RDB_expression *texp, RDB_expression **tbpv, int cap,
        RDB_exec_context *ecp)
{
    if (texp->def.tbref.tbp->kind == RDB_OB_TABLE
            && texp->def.tbref.tbp->val.tbp->stp != NULL
            && texp->def.tbref.tbp->val.tbp->stp->indexc > 0) {
        int i;
        int tbc = texp->def.tbref.tbp->val.tbp->stp->indexc;
        if (tbc > cap)
            tbc = cap;

        /* For each index, generate an expression that uses the index */
        for (i = 0; i < tbc; i++) {
            RDB_tbindex *indexp = &texp->def.tbref.tbp->val.tbp->stp->indexv[i];
            RDB_expression *tiexp = RDB_table_ref(texp->def.tbref.tbp, ecp);
            if (tiexp == NULL)
                return RDB_ERROR;
            tiexp->def.tbref.indexp = indexp;
            tbpv[i] = tiexp;
        }
        return tbc;
    } else {
        return 0;
    }
}

/*
 * Create equivalents of *exp and store them in *tbpv.
 */
static int
mutate(RDB_expression *exp, RDB_expression **tbpv, int cap,
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (exp->kind == RDB_EX_TBP) {
        return mutate_tbref(exp, tbpv, cap, ecp);
    }

    if (exp->kind != RDB_EX_RO_OP)
        return 0;

    if (RDB_expr_is_binop(exp, "where")) {
        return mutate_where(exp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (RDB_expr_is_binop(exp, "join")) {
        return mutate_join(exp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (RDB_expr_is_binop(exp, "minus")
            || RDB_expr_is_binop(exp, "semiminus")) {
        return mutate_semi_minus(exp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (RDB_expr_is_binop(exp, "intersect")
            || RDB_expr_is_binop(exp, "semijoin")) {
        return mutate_matching_index(exp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (RDB_expr_is_binop(exp, "union")) {
        return mutate_full_vt(exp, tbpv, cap, empty_exp, ecp, txp);
    }

    if (strcmp(exp->def.op.name, "extend") == 0
            || strcmp(exp->def.op.name, "project") == 0
            || strcmp(exp->def.op.name, "remove") == 0
            || strcmp(exp->def.op.name, "summarize") == 0
            || strcmp(exp->def.op.name, "wrap") == 0
            || strcmp(exp->def.op.name, "unwrap") == 0
            || strcmp(exp->def.op.name, "group") == 0
            || strcmp(exp->def.op.name, "ungroup") == 0) {
        return mutate_vt(exp, 1, tbpv, cap, empty_exp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "rename") == 0
            && exp->def.op.args.firstp != NULL) {
        if (exp->def.op.args.firstp->nextp == NULL) {
            /* If the rename is a no-op, take the argument */
            return mutate(exp->def.op.args.firstp, tbpv, cap, empty_exp, ecp, txp);
        }
        return mutate_vt(exp, 1, tbpv, cap, empty_exp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "divide") == 0) {
        return mutate_vt(exp, 2, tbpv, cap, empty_exp, ecp, txp);
    }
    if (exp->def.op.args.firstp != NULL
            && exp->def.op.args.firstp->nextp == NULL) {
        return mutate_unary(exp, tbpv, cap, empty_exp, ecp, txp);
    }
    return 0;
}

/*
 * Estimate cost for reading all tuples of the table in the order
 * specified by seqitc/seqitv.
 */
static unsigned
sorted_table_cost(RDB_expression *texp, int seqitc,
        const RDB_seq_item seqitv[])
{
    int cost = table_cost(texp);

    /* Check if the result must be sorted */
    if (seqitc > 0) {
        RDB_tbindex *indexp = RDB_expr_sortindex(texp);
        if (indexp == NULL || !RDB_index_sorts(indexp, seqitc, seqitv))
        {
            int scost = (((double) cost) /* !! * log10(cost) */ / 7);

            if (scost == 0)
                scost = 1;
            cost += scost;
        }
    }

    return cost;
}

RDB_expression *
RDB_optimize(RDB_object *tbp, int seqitc, const RDB_seq_item seqitv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *nexp;

    if (tbp->kind != RDB_OB_TABLE) {
        RDB_raise_invalid_argument("not a table", ecp);
        return NULL;
    }

    if (RDB_TB_CHECK & tbp->val.tbp->flags) {
        if (RDB_check_table(tbp, ecp, txp) != RDB_OK)
            return NULL;
    }

    if (tbp->val.tbp->exp == NULL) {
        /* It's a real table - no optimization possible */
        if (seqitc > 0 && tbp->val.tbp->stp != NULL) {
            /*
             * Check if an index can be used for sorting
             */

            for (i = 0; i < tbp->val.tbp->stp->indexc
                    && !RDB_index_sorts(&tbp->val.tbp->stp->indexv[i],
                            seqitc, seqitv);
                    i++);
            /* If yes, create reference */
            if (i < tbp->val.tbp->stp->indexc) {
                nexp = RDB_table_ref(tbp, ecp);
                if (nexp == NULL)
                    return NULL;
                nexp->def.tbref.indexp = &tbp->val.tbp->stp->indexv[i];
                return nexp;
            }
        }
        return RDB_table_ref(tbp, ecp);
    }

    /* Set expression types so it is known which names cannot refer to tables */
    if (RDB_expr_type(tbp->val.tbp->exp, NULL, NULL, NULL, ecp, txp) == NULL)
        return NULL;
    return RDB_optimize_expr(tbp->val.tbp->exp, seqitc, seqitv, NULL,
            ecp, txp);
}

static void
trace_plan_cost(RDB_expression *exp, int cost, const char *txt,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_database *dbp = txp != NULL ? RDB_tx_db(txp) : NULL;
    /* dbp could be NULL, e.g. when RDB_get_dbs() is executed */
    if (dbp != NULL && RDB_env_trace(RDB_db_env(dbp)) > 0) {
        /*
         * Write expression (with index info) and cost (if not -1) to stderr
         */
        RDB_object strobj;

        RDB_init_obj(&strobj);
        if (RDB_expr_to_str(&strobj, exp, ecp, txp, RDB_SHOW_INDEX) != RDB_OK) {
            RDB_destroy_obj(&strobj, ecp);
            return;
        }

        fprintf(stderr, "%s: %s", txt, RDB_obj_string(&strobj));
        if (cost != -1)
            fprintf(stderr, ", cost: %d\n", cost);
        else
            fputs("\n", stderr);
        RDB_destroy_obj(&strobj, ecp);
    }
}

/*
 * Return an optimized version of exp or exp itself, if a cheaper
 * version could not be found
 */
static RDB_expression *
mutate_select(RDB_expression *exp, int seqitc, const RDB_seq_item seqitv[],
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    unsigned obestcost, bestcost;
    int bestn;
    int tbc;
    RDB_expression *texpv[tbpv_cap];
    RDB_expression *bestexp = exp;

    bestcost = sorted_table_cost(exp, seqitc, seqitv);

    trace_plan_cost(exp, bestcost, "transformed plan", ecp, txp);

    /* Perform rounds of optimization until there is no improvement */
    do {
        obestcost = bestcost;

        trace_plan_cost(bestexp, obestcost, "original plan", ecp, txp);

        tbc = mutate(bestexp, texpv, tbpv_cap, empty_exp, ecp, txp);
        if (tbc < 0)
            return NULL;

        bestn = -1;

        for (i = 0; i < tbc; i++) {
            int cost = sorted_table_cost(texpv[i], seqitc, seqitv);

            trace_plan_cost(texpv[i], cost, "alternative plan", ecp, txp);

            if (cost < bestcost) {
                bestcost = cost;
                bestn = i;
            }
        }
        if (bestn == -1) {
            for (i = 0; i < tbc; i++) {
                RDB_del_expr(texpv[i], ecp);
            }
        } else {
            if (bestexp != exp) {
                RDB_del_expr(bestexp, ecp);
            }
            bestexp = texpv[bestn];
            for (i = 0; i < tbc; i++) {
                if (i != bestn) {
                    RDB_del_expr(texpv[i], ecp);
                }
            }
        }
    } while (bestcost < obestcost);
    trace_plan_cost(bestexp, bestcost, "winning plan", ecp, txp);
    bestexp->optimized = RDB_TRUE;
    return bestexp;
}

RDB_expression *
RDB_optimize_expr(RDB_expression *texp, int seqitc, const RDB_seq_item seqitv[],
        RDB_expression *empty_exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *nexp;
    RDB_expression *optexp;
    RDB_database *dbp = NULL;

    trace_plan_cost(texp, -1, "plan before transformation", ecp, txp);

    /*
     * Make a deep copy of the table, so it can be transformed freely
     */
    nexp = dup_expr_deep(texp, ecp, txp);
    if (nexp == NULL)
        return NULL;

    /*
     * Algebraic optimization
     */
    if (RDB_transform(nexp, NULL, NULL, ecp, txp) != RDB_OK)
        return NULL;

    if (txp != NULL)
        dbp = RDB_tx_db(txp);

    /*
     * Replace tables which are declared to be empty
     * by a constraint
     */
    if (dbp != NULL) {
        if (empty_exp != NULL) {
            if (replace_empty(nexp, empty_exp, ecp, txp) != RDB_OK)
                return NULL;
        }
    }

    if (dbp != NULL && RDB_env_queries(RDB_db_env(dbp))) {
        /* Optimization has to be performed by the storage engine */
        nexp->optimized = RDB_TRUE;
        optexp = nexp;
    } else {
        /*
         * Try to find cheapest table
         */

        optexp = mutate_select(nexp, seqitc, seqitv, empty_exp, ecp, txp);

        /* If a better expression has been found, destroy the original one */
        if (optexp != nexp) {
            RDB_del_expr(nexp, ecp);
        }
    }

    return optexp;
}
