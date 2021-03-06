/*
 * Functions for query transformation
 *
 * Copyright (C) 2004-2008, 2011-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "transform.h"
#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>
#include <obj/objinternal.h>

#include <stdlib.h>
#include <string.h>

/*
 * Check if *exp1 == NOT(*exp2) or *exp2 == NOT(*exp1)
 */
int
RDB_exprs_compl(const RDB_expression *ex1p, const RDB_expression *ex2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    *resp = RDB_FALSE;

    if (ex1p->kind != RDB_EX_RO_OP || ex2p->kind != RDB_EX_RO_OP) {
        return RDB_OK;
    }

    if (strcmp(ex2p->def.op.name, "not") == 0) {
        if (RDB_expr_equals(ex1p, ex2p->def.op.args.firstp, ecp, txp, resp)
                != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;            
    }

    if (strcmp(ex1p->def.op.name, "not") == 0) {
        if (RDB_expr_equals(ex1p->def.op.args.firstp, ex2p, ecp, txp, resp)
                != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

RDB_bool
RDB_expr_is_binop(const RDB_expression *exp, const char *name)
{
    return RDB_expr_is_op(exp, name)
            && exp->def.op.args.firstp != NULL
            && exp->def.op.args.firstp->nextp != NULL
            && exp->def.op.args.firstp->nextp->nextp == NULL;
}

static int
transform_project(RDB_expression *, RDB_gettypefn *, void *,
        RDB_exec_context *, RDB_transaction *);

static int
transform_union(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_bool compl, teq;

    if (strcmp(exp->def.op.name, "union") != 0) {
        RDB_raise_invalid_argument("union expected", ecp);
        return RDB_ERROR;
    }
    if (RDB_expr_is_op(exp->def.op.args.firstp, "project")
            && RDB_expr_is_op(exp->def.op.args.firstp->nextp, "project") == 0) {
        RDB_expression *wex1p = exp->def.op.args.firstp->def.op.args.firstp;
        RDB_expression *wex2p = exp->def.op.args.firstp->nextp->def.op.args.firstp;
        if (RDB_expr_is_binop(wex1p, "where")
                && RDB_expr_is_binop(wex2p, "where") == 0) {
            if (RDB_expr_equals(wex1p->def.op.args.firstp,
                    wex2p->def.op.args.firstp,
                    ecp, txp, &teq) != RDB_OK)
                return RDB_ERROR;
            if (RDB_exprs_compl(wex1p->def.op.args.firstp->nextp,
                    wex2p->def.op.args.firstp->nextp,
                    ecp, txp, &compl) != RDB_OK)
                return RDB_ERROR;

            if (teq && compl) {
                RDB_expression *pexp;
                /*
                 * Replace (T WHERE C) { ... } UNION (T WHERE NOT C) { ... }
                 * by T { ... }
                 */

                if (RDB_del_expr(exp->def.op.args.firstp->nextp, ecp) != RDB_OK)
                    return RDB_ERROR;

                RDB_free(exp->def.op.name);
                pexp = exp->def.op.args.firstp;
                exp->def.op.name = pexp->def.op.name;

                exp->def.op.args.firstp = wex1p->def.op.args.firstp;
                exp->def.op.args.lastp = pexp->def.op.args.lastp;
                if (RDB_del_expr(wex1p->def.op.args.firstp->nextp, ecp) != RDB_OK)
                    return RDB_ERROR;
                wex1p->def.op.args.firstp->nextp = pexp->def.op.args.firstp->nextp;

                RDB_free(pexp);
                RDB_free(wex1p->def.op.name);

                return transform_project(exp, getfnp, arg, ecp, txp);
            }
        }
    } else if (RDB_expr_is_binop(exp->def.op.args.firstp, "where")
            && RDB_expr_is_binop(exp->def.op.args.firstp->nextp, "where")) {
        RDB_bool res;
        RDB_expression *grandchp = exp->def.op.args.firstp->def.op.args.firstp;
        if (RDB_expr_equals(grandchp,
                exp->def.op.args.firstp->nextp->def.op.args.firstp, ecp, txp, &res)
                != RDB_OK) {
            return RDB_ERROR;
        }
        if (res) {
            if (RDB_exprs_compl(exp->def.op.args.firstp->def.op.args.firstp->nextp,
                    exp->def.op.args.firstp->nextp->def.op.args.firstp->nextp,
                    ecp, txp, &res) != RDB_OK) {
                return RDB_ERROR;
            }
            if (res) {
                /*
                 * Replace (T WHERE C) UNION (T WHERE NOT C)
                 * by RENAME T {}
                 */
                char *name = RDB_realloc(exp->def.op.name, sizeof("rename"), ecp);
                if (name == NULL) {
                    return RDB_ERROR;
                }
                strcpy(name, "rename");
                exp->def.op.name = name;
                RDB_del_expr(exp->def.op.args.firstp->nextp, ecp);
                RDB_del_expr(exp->def.op.args.firstp->def.op.args.firstp->nextp, ecp);
                exp->def.op.args.firstp->def.op.args.firstp = NULL;
                RDB_del_expr(exp->def.op.args.firstp, ecp);
                grandchp->nextp = NULL;
                exp->def.op.args.firstp = grandchp;
            }
        }
    }
    return RDB_OK;
}

/*
 * Convert comparisons of the form <constant> <comp op> <var>
 * are converted to <var> <comp op> <constant>
 */
static void
normalize_comparisons(RDB_expression *exp)
{
    if (exp->kind != RDB_EX_RO_OP)
        return;

    if (exp->def.op.args.firstp != NULL
            && exp->def.op.args.firstp->nextp != NULL
            && exp->def.op.args.firstp->nextp->nextp == NULL
            && (strcmp(exp->def.op.name, "=") == 0
                || strcmp(exp->def.op.name, "<=") == 0
                || strcmp(exp->def.op.name, "<") == 0
                || strcmp(exp->def.op.name, ">=") == 0
                || strcmp(exp->def.op.name, ">") == 0)) {
        if (exp->def.op.args.firstp->kind == RDB_EX_OBJ
                && exp->def.op.args.firstp->nextp->kind == RDB_EX_VAR) {
            RDB_expression *hexp;

            /* Modify operator */
            if (strcmp(exp->def.op.name, "<=") == 0)
                strcpy(exp->def.op.name, ">=");
            else if (strcmp(exp->def.op.name, "<") == 0)
                strcpy(exp->def.op.name, ">");
            else if (strcmp(exp->def.op.name, ">=") == 0)
                strcpy(exp->def.op.name, "<=");
            else if (strcmp(exp->def.op.name, ">") == 0)
                strcpy(exp->def.op.name, "<");

            /* Swap arguments */
            hexp = exp->def.op.args.firstp;
            exp->def.op.args.firstp = hexp->nextp;
            exp->def.op.args.firstp->nextp = hexp;
            hexp->nextp = NULL;
        }
    } else {
        RDB_expression *childp = exp->def.op.args.firstp;
        while (childp != NULL) {
            normalize_comparisons(childp);
            childp = childp->nextp;
        }
    }
}

static int
transform_where(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    char *hname;
    RDB_expression *hexp;

    normalize_comparisons(exp->def.op.args.firstp->nextp);

    if (RDB_transform(exp->def.op.args.firstp, getfnp, arg, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    for(;;) {
        RDB_expression *chexp = exp->def.op.args.firstp;
        if (chexp->kind != RDB_EX_RO_OP)
            return RDB_OK;

        if (strcmp(chexp->def.op.name, "where") == 0) {
            /*
             * Merge WHERE expressions
             */
            RDB_expression *condp = RDB_ro_op("and", ecp);
            if (condp == NULL)
                return RDB_ERROR;
            RDB_add_arg(condp, exp->def.op.args.firstp->nextp);
            RDB_add_arg(condp, chexp->def.op.args.firstp->nextp);

            chexp->def.op.args.firstp->nextp = condp;
            condp->nextp = NULL;
            exp->def.op.args.firstp = chexp->def.op.args.firstp;
            exp->def.op.args.lastp = condp;

            if (RDB_destroy_expr(chexp, ecp) != RDB_OK)
                return RDB_ERROR;
            RDB_free(chexp);
        } else if (strcmp(chexp->def.op.name, "minus") == 0
                || strcmp(chexp->def.op.name, "semiminus") == 0
                || strcmp(chexp->def.op.name, "semijoin") == 0) {
            /*
             * WHERE(SEMIMINUS( -> SEMIMINUS(WHERE(
             * Same for MINUS, SEMIJOIN
             */

            if (RDB_transform(chexp->def.op.args.firstp->nextp, getfnp, arg,
                    ecp, txp) != RDB_OK)
                return RDB_ERROR;

            hname = exp->def.op.name;
            exp->def.op.name = chexp->def.op.name;
            chexp->def.op.name = hname;

            hexp = chexp->def.op.args.firstp->nextp;
            chexp->def.op.args.firstp->nextp = exp->def.op.args.firstp->nextp;
            exp->def.op.args.firstp->nextp = hexp;
            chexp->def.op.args.lastp = chexp->def.op.args.firstp->nextp;
            exp->def.op.args.lastp = exp->def.op.args.firstp->nextp;

            exp = chexp;
        } else if (strcmp(chexp->def.op.name, "union") == 0) {
            /*
             * WHERE(UNION( -> UNION(WHERE(
             */
            RDB_expression *wexp;
            RDB_expression *condp = RDB_dup_expr(exp->def.op.args.firstp->nextp,
                    ecp);
            if (condp == NULL)
                return RDB_ERROR;

            wexp = RDB_ro_op("where", ecp);
            if (wexp == NULL) {
                RDB_del_expr(condp, ecp);
                return RDB_ERROR;
            }
            RDB_add_arg(wexp, exp->def.op.args.firstp->def.op.args.firstp->nextp);
            RDB_add_arg(wexp, condp);
            wexp->nextp = NULL;

            /* Swap operator names */
            hname = chexp->def.op.name;
            chexp->def.op.name = exp->def.op.name;
            exp->def.op.name = hname;

            chexp->def.op.args.firstp->nextp = exp->def.op.args.firstp->nextp;
            exp->def.op.args.firstp->nextp = wexp;
            chexp->def.op.args.lastp = chexp->def.op.args.firstp->nextp;
            exp->def.op.args.lastp = wexp;

            return RDB_transform(chexp, getfnp, arg, ecp, txp);
        } else if (strcmp(chexp->def.op.name, "extend") == 0) {
            if (exp->typ != NULL) {
                RDB_del_nonscalar_type(exp->typ, ecp);
                exp->typ = NULL;
            }
            if (chexp->typ != NULL) {
                RDB_del_nonscalar_type(chexp->typ, ecp);
                chexp->typ = NULL;
            }

            /*
             * WHERE(EXTEND( -> EXTEND(WHERE(
             */
            if (RDB_resolve_exprnames(&exp->def.op.args.firstp->nextp,
                    chexp->def.op.args.firstp->nextp, ecp) != RDB_OK)
                return RDB_ERROR;

            hname = chexp->def.op.name;
            chexp->def.op.name = exp->def.op.name;
            exp->def.op.name = hname;

            hexp = exp->def.op.args.firstp->nextp;
            exp->def.op.args.firstp->nextp = chexp->def.op.args.firstp->nextp;
            chexp->def.op.args.firstp->nextp = hexp;
            RDB_expr_list_set_lastp(&exp->def.op.args);
            RDB_expr_list_set_lastp(&chexp->def.op.args);
            
            exp = chexp;
        } else if (strcmp(chexp->def.op.name, "rename") == 0) {
            RDB_expression *argp;

            /*
             * WHERE(RENAME( -> RENAME(WHERE(
             * Only if the where condition does not contain
             * variables which have been "renamed away".
             */

            argp = chexp->def.op.args.firstp->nextp;
            while (argp != NULL) {
                if (RDB_expr_refers_var(exp->def.op.args.firstp->nextp,
                        RDB_obj_string(RDB_expr_obj(argp))))
                    return RDB_OK;
                argp = argp->nextp->nextp;
            }

            if (exp->typ != NULL) {
                RDB_del_nonscalar_type(exp->typ, ecp);
                exp->typ = NULL;
            }

            if (RDB_invrename_expr(chexp->nextp, chexp, ecp)
                    != RDB_OK)
                return RDB_ERROR;

            if (chexp->typ != NULL) {
                RDB_del_nonscalar_type(chexp->typ, ecp);
                chexp->typ = NULL;
            }

            hname = chexp->def.op.name;
            chexp->def.op.name = exp->def.op.name;
            exp->def.op.name = hname;
            
            hexp = exp->def.op.args.firstp->nextp;
            exp->def.op.args.firstp->nextp = chexp->def.op.args.firstp->nextp;
            chexp->def.op.args.firstp->nextp = hexp;
            RDB_expr_list_set_lastp(&exp->def.op.args);
            RDB_expr_list_set_lastp(&chexp->def.op.args);

            exp = chexp;
        } else {
            return RDB_transform(chexp, getfnp, arg, ecp, txp);
        }
    }

    /* Should never be reached */
    RDB_raise_internal("transform_where()", ecp);
    return RDB_ERROR;
}

static int
swap_project_union(RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *nexp, *nargp;
    RDB_expression *argp;
    char *hname;
    RDB_expression *chexp = exp->def.op.args.firstp;

    if (strcmp(exp->def.op.name, "project") != 0
        || strcmp(chexp->def.op.name, "union") != 0) {
        RDB_raise_invalid_argument("project(union expected", ecp);
        return RDB_ERROR;
    }

    /*
     * Create new project table for child #2
     */
    nexp = RDB_ro_op("project", ecp);
    if (nexp == NULL) {
        return RDB_ERROR;
    }
    RDB_add_arg(nexp, chexp->def.op.args.firstp->nextp);

    argp = exp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        nargp = RDB_dup_expr(argp, ecp);
        if (nargp == NULL) {
            RDB_del_expr(nexp, ecp);
            return RDB_ERROR;
        }
        RDB_add_arg(nexp, nargp);
        argp = argp->nextp;
    }

    if (exp->typ != NULL) {
        RDB_del_nonscalar_type(exp->typ, ecp);
        exp->typ = NULL;
    }
    if (chexp->typ != NULL) {
        RDB_del_nonscalar_type(chexp->typ, ecp);
        chexp->typ = NULL;
    }

    /*
     * Swap parent and child
     */

    hname = exp->def.op.name;
    exp->def.op.name = chexp->def.op.name;
    chexp->def.op.name = hname;

    chexp->def.op.args.firstp->nextp = exp->def.op.args.firstp->nextp;

    exp->def.op.args.firstp = exp->def.op.args.lastp = NULL;
    RDB_add_arg(exp, chexp);
    RDB_add_arg(exp, nexp);

    return RDB_OK;
}

static RDB_expression *
proj_attr(const RDB_expression *exp, const char *attrname)
{
    RDB_expression *argp = exp->def.op.args.firstp->nextp;

    while (argp != NULL) {
        if (strcmp(RDB_obj_string(&argp->def.obj), attrname) == 0)
            return argp;
        argp = argp->nextp;
    }
    return NULL;
}

/**
 * Transform PROJECT(RENAME) to RENAME(PROJECT) or PROJECT
 */
static int
swap_project_rename(RDB_expression *texp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    char *opname;
    RDB_expression *argp, *cargp;
    RDB_expression *hexp;
    RDB_expression *chtexp = texp->def.op.args.firstp;

    /*
     * Alter parent by removing renamings which become obsolete
     * because of the projection
     */

    /* Remove renamings whose dest does not appear in the parent */
    argp = chtexp->def.op.args.firstp;
    while (argp->nextp != NULL) {
        if (proj_attr(texp, RDB_obj_string(&argp->nextp->nextp->def.obj))
                == NULL) {
            hexp = argp->nextp->nextp->nextp;
            RDB_del_expr(argp->nextp->nextp, ecp);
            RDB_del_expr(argp->nextp, ecp);
            argp->nextp = hexp;
        } else {
            argp = argp->nextp->nextp;
        }
    }
    if (chtexp->def.op.args.firstp->nextp == NULL) {
        /*
         * Remove child
         */
        chtexp->def.op.args.firstp->nextp = texp->def.op.args.firstp->nextp;
        chtexp->def.op.args.lastp = texp->def.op.args.lastp;
        texp->def.op.args.firstp = chtexp->def.op.args.firstp;
        if (RDB_destroy_expr(chtexp, ecp) != RDB_OK)
            return RDB_ERROR;
        RDB_free(chtexp);

        return transform_project(texp, getfnp, arg, ecp, txp);
    }

    /*
     * Swap parent and child
     */

    /* Change project attributes */
    argp = texp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        cargp = chtexp->def.op.args.firstp->nextp;
        while (cargp != NULL
                && strcmp(RDB_obj_string(&cargp->nextp->def.obj),
                        RDB_obj_string(&argp->def.obj)) != 0) {
            cargp = cargp->nextp->nextp;
        }
        if (cargp != NULL) {
            if (RDB_string_to_obj(&argp->def.obj,
                    RDB_obj_string(&cargp->def.obj), ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
        argp = argp->nextp;
    }

    if (texp->typ != NULL) {
        RDB_del_nonscalar_type(texp->typ, ecp);
        texp->typ = NULL;
    }
    if (chtexp->typ != NULL) {
        RDB_del_nonscalar_type(chtexp->typ, ecp);
        chtexp->typ = NULL;
    }

    opname = texp->def.op.name;
    texp->def.op.name = chtexp->def.op.name;
    chtexp->def.op.name = opname;

    hexp = texp->def.op.args.firstp->nextp;
    texp->def.op.args.firstp->nextp = chtexp->def.op.args.firstp->nextp;
    chtexp->def.op.args.firstp->nextp = hexp;
    RDB_expr_list_set_lastp(&texp->def.op.args);
    RDB_expr_list_set_lastp(&chtexp->def.op.args);

    return RDB_OK;
}

/**
 * Transform PROJECT(EXTEND) to EXTEND(PROJECT) or PROJECT
 */
static int
transform_project_extend(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int expc;
    RDB_expression **expv = NULL;
    RDB_expression *argp, *hexp;
    RDB_expression *chexp = exp->def.op.args.firstp;
    int chargc = RDB_expr_list_length(&chexp->def.op.args);

    /*
     * Remove EXTEND-Attributes which are 'projected away'
     */

    expv = RDB_alloc(sizeof (RDB_expression *) * chargc, ecp);
    if (expv == NULL) {
        return RDB_ERROR;
    }

    expv[0] = chexp->def.op.args.firstp;
    expc = 1;
    argp = chexp->def.op.args.firstp;
    while (argp->nextp != NULL) {
        if (proj_attr(exp, RDB_obj_string(&argp->nextp->nextp->def.obj)) != NULL) {
            expv[expc] = argp->nextp;
            expv[expc + 1] = argp->nextp->nextp;
            expc += 2;
            argp = argp->nextp->nextp;
        } else {
            hexp = argp->nextp->nextp->nextp;
            RDB_del_expr(argp->nextp->nextp, ecp);
            RDB_del_expr(argp->nextp, ecp);
            argp->nextp = hexp;
        }
    }

    if (expc == 1) {
        /* Extend became empty - remove */
        expv[0]->nextp = exp->def.op.args.firstp->nextp;
        exp->def.op.args.firstp = expv[0];
        RDB_free(expv);
        if (RDB_destroy_expr(chexp, ecp) != RDB_OK)
            return RDB_ERROR;
        RDB_free(chexp);
        return transform_project(exp, getfnp, arg, ecp, txp);
    }
    chexp->def.op.args.firstp = chexp->def.op.args.lastp = NULL;
    for (i = 0; i < expc; i++)
        RDB_add_arg(chexp, expv[i]);
    RDB_free(expv);
    if (chexp->typ != NULL) {
        RDB_del_nonscalar_type(chexp->typ, ecp);
        chexp->typ = NULL;
    }

    return RDB_transform(chexp, getfnp, arg, ecp, txp);
}

/**
 * Transform project(WHERE) to WHERE(project) or project(WHERE(project))
 */
static int
swap_project_where(RDB_expression *exp, RDB_expression *chexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int attrc;
    char **attrv;
    RDB_expression *argp;
    RDB_type *chtyp = RDB_expr_type(chexp, NULL, NULL, NULL, ecp, txp);
    if (chtyp == NULL)
        return RDB_ERROR;

    attrv = RDB_alloc(sizeof(char *) * chtyp->def.basetyp->def.tuple.attrc, ecp);
    if (attrv == NULL) {
        return RDB_ERROR;
    }

    /*
     * Get attributes from parent
     */
    attrc = RDB_expr_list_length(&exp->def.op.args) - 1;
    argp = exp->def.op.args.firstp->nextp;
    for (i = 0; i < attrc; i++) {
        attrv[i] = RDB_obj_string(&argp->def.obj);
        argp = argp->nextp;
    }

    /*
     * Add attributes from child which are not attributes of the parent,
     * but are referred to by the where condition
     */
    for (i = 0; i < chtyp->def.basetyp->def.tuple.attrc; i++) {
        char *attrname = chtyp->def.basetyp->def.tuple.attrv[i].name;

        if (proj_attr(exp, attrname) == NULL
                && RDB_expr_refers_var(chexp, attrname)) {
            attrv[attrc++] = attrname;
        }
    }

    /*
     * If project and WHERE cannot be swapped because there are attributes
     * which are needed by the WHERE condition add a project grandchild
     * if it has any effect
     */
    if (attrc > RDB_expr_list_length(&exp->def.op.args) - 1) {
        if (attrc > chtyp->def.basetyp->def.tuple.attrc) {
            RDB_expression *argp;
            RDB_expression *nexp = RDB_ro_op("project", ecp);
            if (nexp == NULL)
                return RDB_ERROR;
            nexp->nextp = chexp->def.op.args.firstp->nextp;

            RDB_add_arg(nexp, chexp->def.op.args.firstp);
            for (i = 0; i < attrc; i++) {
                argp = RDB_string_to_expr(attrv[i], ecp);
                if (argp == NULL) {
                    RDB_del_expr(nexp, ecp);
                    RDB_free(attrv);
                    return RDB_ERROR;
                }
                RDB_add_arg(nexp, argp);
            }
            chexp->def.op.args.firstp = nexp;
        }
    } else {
        char *hname;
        RDB_expression *hexp;

        if (exp->typ != NULL) {
            RDB_del_nonscalar_type(exp->typ, ecp);
            exp->typ = NULL;
        }
        RDB_del_nonscalar_type(chexp->typ, ecp);
        chexp->typ = NULL;

        /*
         * Swap WHERE and project
         */
        hname = exp->def.op.name;
        exp->def.op.name = chexp->def.op.name;
        chexp->def.op.name = hname;

        hexp = exp->def.op.args.firstp->nextp;
        exp->def.op.args.firstp->nextp = chexp->def.op.args.firstp->nextp;
        chexp->def.op.args.firstp->nextp = hexp;

        RDB_expr_list_set_lastp(&exp->def.op.args);
        RDB_expr_list_set_lastp(&chexp->def.op.args);
    }
    RDB_free(attrv);

    return RDB_OK;
}

static int
transform_project(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *oexp = exp;
    RDB_expression *chexp, *argp;

    do {
        chexp = exp->def.op.args.firstp;
        if (chexp->kind != RDB_EX_RO_OP) {
            break;
        }

        if (strcmp(chexp->def.op.name, "project") == 0) {
            /*
             * Merge nested projection by eliminating the child
             */
            argp = chexp->def.op.args.firstp->nextp;
            while (argp != NULL) {
                RDB_expression *nextp = argp->nextp;
                if (RDB_del_expr(argp, ecp) != RDB_OK)
                    return RDB_ERROR;
                argp = nextp;
            }
            exp->def.op.args.firstp = chexp->def.op.args.firstp;
            exp->def.op.args.firstp->nextp = chexp->nextp;
            if (RDB_destroy_expr(chexp, ecp) != RDB_OK)
                return RDB_ERROR;
            RDB_free(chexp);
        } else if (strcmp(chexp->def.op.name, "union") == 0) {
            if (swap_project_union(exp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            if (RDB_transform(exp, getfnp, arg, ecp, txp)
                    != RDB_OK)
                return RDB_ERROR;
            exp = chexp;
        } else if (strcmp(chexp->def.op.name, "where") == 0) {
            if (swap_project_where(exp, chexp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            if (strcmp(chexp->def.op.name, "where") == 0) {
                return transform_where(chexp, getfnp, arg, ecp, txp);
            }
            exp = chexp;
        } else if (strcmp(chexp->def.op.name, "rename") == 0) {
            if (swap_project_rename(exp, getfnp, arg, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            exp = exp->def.op.args.firstp;
        } else if (strcmp(chexp->def.op.name, "extend") == 0) {
            return transform_project_extend(exp, getfnp, arg, ecp, txp);
        } else {
            return RDB_transform(chexp, getfnp, arg, ecp, txp);
        }
    } while (RDB_expr_is_op(exp, "project"));
    if (exp->kind == RDB_EX_RO_OP) {
        argp = exp->def.op.args.firstp;
        while (argp != NULL) {
            if (RDB_transform(argp, getfnp, arg, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
        }
    }
    if (oexp->kind == RDB_EX_RO_OP && strcmp(oexp->def.op.name, "project") != 0) {
        oexp->transformed = RDB_FALSE;
        return RDB_transform(oexp, getfnp, arg, ecp, txp);
    }
    return RDB_OK;
}

/**
 * Convert REMOVE into PROJECT
 */
int
RDB_remove_to_project(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression **argv = NULL;
    RDB_expression *argp, *nargp, *lastnargp;
    RDB_expr_list oargs;
    int attrc;
    int i, ai;
    char *opname;
    RDB_type *chtyp;
    RDB_type *tpltyp;

    if (RDB_expr_op_is_noarg(exp)) {
        RDB_raise_invalid_argument("invalid number of REMOVE arguments", ecp);
        return RDB_ERROR;
    }

    argp = exp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        if (argp->kind != RDB_EX_OBJ || argp->def.obj.typ != &RDB_STRING) {
            RDB_raise_type_mismatch("STRING argument required for REMOVE", ecp);
            return RDB_ERROR;
        }
        argp = argp->nextp;
    }                

    chtyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, NULL, ecp, txp);
    if (chtyp == NULL)
        return RDB_ERROR;
    if (chtyp->kind == RDB_TP_RELATION) {
        tpltyp = chtyp->def.basetyp;
    } else if (chtyp->kind == RDB_TP_TUPLE) {
        tpltyp = chtyp;
    } else {
        RDB_raise_type_mismatch("invalid type of REMOVE argument", ecp);
        return RDB_ERROR;
    }

    ai = 1;
    attrc = tpltyp->def.tuple.attrc - RDB_expr_list_length(&exp->def.op.args) + 1;
    if (attrc < 0) {
        RDB_raise_invalid_argument("invalid number of REMOVE arguments", ecp);
        goto error;
    }

    /* Old arguments 1 .. n */
    oargs.firstp = exp->def.op.args.firstp->nextp;
    oargs.lastp = exp->def.op.args.lastp;

    /* Initialize new argument list with arg #1 */
    nargp = lastnargp = exp->def.op.args.firstp;
    nargp->nextp = NULL;

    for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
        argp = oargs.firstp;
        while (argp != NULL
                && strcmp(RDB_obj_string(&argp->def.obj),
                        tpltyp->def.tuple.attrv[i].name) != 0)
            argp = argp->nextp;
        if (argp == NULL) {
            /*
             * Attribute not in list of removed attributes,
             * so it becomes a PROJECT argument
             */
            if (ai >= attrc + 1) {
                /*
                 * Number of PROJECT arguments would become greater than
                 * the number of attributes - this indicates that something
                 * must be wrong with the arguments
                 */
                RDB_raise_invalid_argument("invalid REMOVE arguments", ecp);
                goto error;
            }

            argp = RDB_string_to_expr(tpltyp->def.tuple.attrv[i].name,
                    ecp);
            if (argp == NULL)
                goto error;
            lastnargp->nextp = argp;
            argp->nextp = NULL;
            lastnargp = argp;
            ai++;
        }
    }
    if (ai != attrc + 1) {
        RDB_raise_invalid_argument("invalid REMOVE arguments", ecp);
        goto error;
    }

    opname = RDB_realloc(exp->def.op.name, 8, ecp);
    if (opname == NULL) {
        goto error;
    }
    strcpy(opname, "project");
    exp->def.op.name = opname;

    RDB_destroy_expr_list(&oargs, ecp);
    exp->def.op.args.firstp = nargp;
    exp->def.op.args.lastp = lastnargp;

    return RDB_OK;

error:
    if (argv != NULL) {
        for (i = 1; i < ai; i++) {
            RDB_del_expr(argv[i], ecp);
        }
    }
    return RDB_ERROR;
}

/**
 * Transform REMOVE
 */
static int
transform_remove(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (RDB_remove_to_project(exp, getfnp, arg, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    return transform_project(exp, getfnp, arg, ecp, txp);
}

static int
transform_update(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (RDB_convert_update(exp, getfnp, arg, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    return RDB_transform(exp->def.op.args.firstp, getfnp, arg, ecp, txp);
}

/**
 * Convert UPDATE into RENAME(PROJECT(EXTEND(
 */
int
RDB_convert_update(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object nattrname;
    RDB_expression *pexp, *xexp;
    RDB_expression *argp = exp->def.op.args.firstp->nextp;

    strcpy(exp->def.op.name, "rename");

    pexp = RDB_ro_op("remove", ecp);
    if (pexp == NULL)
        return RDB_ERROR;
    pexp->nextp = NULL;

    xexp = RDB_ro_op("extend", ecp);
    if (xexp == NULL)
        return RDB_ERROR;
    RDB_add_arg(pexp, xexp);
    RDB_add_arg(xexp, exp->def.op.args.firstp);

    exp->def.op.args.firstp = exp->def.op.args.lastp = pexp;

    RDB_init_obj(&nattrname);
    while (argp != NULL) {
        RDB_expression *nargp, *hexp;

        if (argp->nextp == NULL) {
            RDB_raise_invalid_argument("UPDATE: invalid number of arguments",
                    ecp);
            goto error;
        }
        nargp = argp->nextp->nextp;

        if (argp->kind != RDB_EX_OBJ) {
            RDB_raise_invalid_argument("UPDATE argument must be of type string",
                    ecp);
            goto error;
        }

        /* Build attribute name prefixed by $ */
        if (RDB_string_to_obj(&nattrname, "$", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&nattrname, RDB_obj_string(RDB_expr_obj(argp)),
                ecp) != RDB_OK)
            goto error;

        /* Append EXTEND args */
        RDB_add_arg(xexp, argp->nextp);
        hexp = RDB_obj_to_expr(&nattrname, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(xexp, hexp);

        /* Append REMOVE args */
        hexp = RDB_obj_to_expr(RDB_expr_obj(argp), ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(pexp, hexp);

        /* Append RENAME args */
        hexp = RDB_obj_to_expr(&nattrname, ecp);
        if (hexp == NULL)
            goto error;
        RDB_add_arg(exp, hexp);
        RDB_add_arg(exp, argp);

        argp = nargp;
    }
    RDB_destroy_obj(&nattrname, ecp);
    return RDB_remove_to_project(pexp, getfnp, arg, ecp, txp);

error:
    RDB_destroy_obj(&nattrname, ecp);
        return RDB_ERROR;
}

static int
transform_is_empty(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *chexp = exp->def.op.args.firstp;

    /* Add projection, if not already present */
    if (!RDB_expr_is_op(chexp, "project")
            || chexp->def.op.args.firstp->nextp != NULL) {
        RDB_expression *pexp = RDB_ro_op("project", ecp);
        if (pexp == NULL)
            return RDB_ERROR;

        RDB_add_arg(pexp, chexp);

        pexp->nextp = NULL;
        exp->def.op.args.firstp = exp->def.op.args.lastp = pexp;

        return transform_project(pexp, getfnp, arg, ecp, txp);
    }

    return RDB_OK;
}

static int
transform_children(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp = exp->def.op.args.firstp;
    while (argp != NULL) {
        if (RDB_transform(argp, getfnp, arg, ecp, txp) != RDB_OK)
            return RDB_ERROR;
        argp = argp->nextp;
    }
    return RDB_OK;
}

/**
 * Perform algebraic optimization.
 * Eliminating NOT in WHERE expressions is not performed here,
 * because it conflicts with optimizing certain UNIONs.
 */
int
RDB_transform(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (exp->transformed) {
        return RDB_OK;
    }
    exp->transformed = RDB_TRUE;

    if (exp->kind != RDB_EX_RO_OP)
        return RDB_OK;

    if (transform_children(exp, getfnp, arg, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (strcmp(exp->def.op.name, "update") == 0) {
        return transform_update(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "union") == 0) {
        return transform_union(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "where") == 0) {
        return transform_where(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "project") == 0) {
        return transform_project(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "remove") == 0) {
        return transform_remove(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "is_empty") == 0) {
        return transform_is_empty(exp, getfnp, arg, ecp, txp);
    }
    return RDB_OK;
} /* RDB_transform */

/*
 * Convert the expression into a reference to an empty table
 */
int
RDB_expr_to_empty_table(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *typ = RDB_expr_type_tpltyp(exp, NULL, NULL, NULL, NULL,
            ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    /*
     * exp->typ will be consumed by RDB_init_table_from_type(),
     * so prevent it from being destroyed.
     */
    exp->typ = NULL;

    if (RDB_drop_expr_children(exp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_destroy_expr(exp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    exp->kind = RDB_EX_OBJ;
    RDB_init_obj(&exp->def.obj);
    return RDB_init_table_from_type(&exp->def.obj, NULL, typ, 0, NULL,
            0, NULL, ecp);
}

/*
 * Convert variable names referring to tables
 * to table references if possible
 */
int
RDB_expr_resolve_tbnames(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *argp;

    switch (exp->kind) {
    case RDB_EX_RO_OP:
        argp = exp->def.op.args.firstp;
        while (argp != NULL) {
            if (RDB_expr_resolve_tbnames(argp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
        }
        return RDB_OK;
    case RDB_EX_OBJ:
    case RDB_EX_TBP:
        return RDB_OK;
    case RDB_EX_VAR:
        if (exp->typ == NULL || RDB_type_is_relation(exp->typ)) {
            RDB_object *tbp = RDB_get_table(exp->def.varname, ecp, txp);
            if (tbp == NULL) {
                RDB_clear_err(ecp);
                return RDB_OK;
            }

            /* Transform into table ref */
            RDB_free(exp->def.varname);
            exp->kind = RDB_EX_TBP;
            exp->def.tbref.tbp = tbp;
            exp->def.tbref.indexp = NULL;
        }
    }
    return RDB_OK;
}
