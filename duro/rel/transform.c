/*
 * $Id$
 *
 * Copyright (C) 2004-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "transform.h"
#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>

#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int
exprs_compl(const RDB_expression *ex1p, const RDB_expression *ex2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    *resp = RDB_FALSE;

    if (strcmp(ex2p->var.op.name, "NOT") == 0) {
        if (_RDB_expr_equals(ex1p, ex2p->var.op.args.firstp, ecp, txp, resp)
                != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;            
    }

    if (strcmp(ex1p->var.op.name, "NOT") == 0) {
        if (_RDB_expr_equals(ex1p->var.op.args.firstp, ex2p, ecp, txp, resp)
                != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
transform_project(RDB_expression *, RDB_exec_context *, RDB_transaction *);

static int
transform_union(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_bool compl, teq;

    if (exp->var.op.args.firstp->kind == RDB_EX_RO_OP
            && strcmp(exp->var.op.args.firstp->var.op.name, "PROJECT") == 0
            && exp->var.op.args.firstp->nextp->kind == RDB_EX_RO_OP
            && strcmp(exp->var.op.args.firstp->nextp->var.op.name, "PROJECT") == 0) {
        RDB_expression *wex1p = exp->var.op.args.firstp->var.op.args.firstp;
        RDB_expression *wex2p = exp->var.op.args.firstp->nextp->var.op.args.firstp;
        if (wex1p->kind == RDB_EX_RO_OP
                && strcmp(wex1p->var.op.name, "WHERE") == 0
                && wex2p->kind == RDB_EX_RO_OP
                && strcmp(wex2p->var.op.name, "WHERE") == 0) {
            if (_RDB_expr_equals(wex1p->var.op.args.firstp,
                    wex2p->var.op.args.firstp,
                    ecp, txp, &teq) != RDB_OK)
                return RDB_ERROR;
            if (exprs_compl(wex1p->var.op.args.firstp->nextp,
                    wex2p->var.op.args.firstp->nextp,
                    ecp, txp, &compl) != RDB_OK)
                return RDB_ERROR;

            if (teq && compl) {
                RDB_expression *pexp;
                /*
                 * Replace (T WHERE C) { ... } UNION (T WHERE NOT C) { ... }
                 * by T { ... }
                 */

                if (RDB_drop_expr(exp->var.op.args.firstp->nextp, ecp) != RDB_OK)
                    return RDB_ERROR;

                free(exp->var.op.name);
                pexp = exp->var.op.args.firstp;
                exp->var.op.name = pexp->var.op.name;

                exp->var.op.args.firstp = wex1p->var.op.args.firstp;
                exp->var.op.args.lastp = pexp->var.op.args.lastp;
                if (RDB_drop_expr(wex1p->var.op.args.firstp->nextp, ecp) != RDB_OK)
                    return RDB_ERROR;
                wex1p->var.op.args.firstp->nextp = pexp->var.op.args.firstp->nextp;

                free(pexp);
                free(wex1p->var.op.name);

                return transform_project(exp, ecp, txp);
            }
        }
    }
    return RDB_OK;
}

static int
transform_where(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *hname;
    RDB_expression *hexp;

    if (_RDB_transform(exp->var.op.args.firstp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    for(;;) {
        RDB_expression *chexp = exp->var.op.args.firstp;
        if (chexp->kind != RDB_EX_RO_OP)
            return RDB_OK;

        if (strcmp(chexp->var.op.name, "WHERE") == 0) {
            /*
             * Merge WHERE expressions
             */
            RDB_expression *condp = RDB_ro_op("AND", ecp);
            if (condp == NULL)
                return RDB_ERROR;
            RDB_add_arg(condp, exp->var.op.args.firstp->nextp);
            RDB_add_arg(condp, chexp->var.op.args.firstp->nextp);

            chexp->var.op.args.firstp->nextp = condp;
            condp->nextp = NULL;
            exp->var.op.args.firstp = chexp->var.op.args.firstp; 
            exp->var.op.args.lastp = condp; 

            if (_RDB_destroy_expr(chexp, ecp) != RDB_OK)
                return RDB_ERROR;
            free(chexp);
        } else if (strcmp(chexp->var.op.name, "MINUS") == 0
                || strcmp(chexp->var.op.name, "SEMIMINUS") == 0
                || strcmp(chexp->var.op.name, "SEMIJOIN") == 0) {
            /*
             * WHERE(SEMIMINUS( -> SEMIMINUS(WHERE(
             * Same for MINUS, SEMIJOIN
             */

            if (_RDB_transform(chexp->var.op.args.firstp->nextp, ecp, txp)
                    != RDB_OK)
                return RDB_ERROR;

            hname = exp->var.op.name;
            exp->var.op.name = chexp->var.op.name;
            chexp->var.op.name = hname;

            hexp = chexp->var.op.args.firstp->nextp;
            chexp->var.op.args.firstp->nextp = exp->var.op.args.firstp->nextp;
            exp->var.op.args.firstp->nextp = hexp;
            chexp->var.op.args.lastp = chexp->var.op.args.firstp->nextp;
            exp->var.op.args.lastp = exp->var.op.args.firstp->nextp;

            exp = chexp;
        } else if (strcmp(chexp->var.op.name, "UNION") == 0) {
            /*
             * WHERE(UNION( -> UNION(WHERE(
             */
            RDB_expression *wexp;
            RDB_expression *condp = RDB_dup_expr(exp->var.op.args.firstp->nextp,
                    ecp);
            if (condp == NULL)
                return RDB_ERROR;

            wexp = RDB_ro_op("WHERE", ecp);
            if (wexp == NULL) {
                RDB_drop_expr(condp, ecp);
                return RDB_ERROR;
            }
            RDB_add_arg(wexp, exp->var.op.args.firstp->var.op.args.firstp->nextp);
            RDB_add_arg(wexp, condp);
            wexp->nextp = NULL;

            /* Swap operator names */
            hname = chexp->var.op.name;
            chexp->var.op.name = exp->var.op.name;
            exp->var.op.name = hname;

            chexp->var.op.args.firstp->nextp = exp->var.op.args.firstp->nextp;
            exp->var.op.args.firstp->nextp = wexp;
            chexp->var.op.args.lastp = chexp->var.op.args.firstp->nextp;
            exp->var.op.args.lastp = wexp;

            return _RDB_transform(chexp, ecp, txp);
        } else if (strcmp(chexp->var.op.name, "EXTEND") == 0) {
            if (exp->typ != NULL) {
                RDB_drop_type(exp->typ, ecp, NULL);
                exp->typ = NULL;
            }
            if (chexp->typ != NULL) {
                RDB_drop_type(chexp->typ, ecp, NULL);
                chexp->typ = NULL;
            }

            /*
             * WHERE(EXTEND( -> EXTEND(WHERE(
             */
            if (_RDB_resolve_extend_expr(&exp->var.op.args.firstp->nextp,
                    chexp, ecp) != RDB_OK)
                return RDB_ERROR;

            hname = chexp->var.op.name;
            chexp->var.op.name = exp->var.op.name;
            exp->var.op.name = hname;

            hexp = exp->var.op.args.firstp->nextp;
            exp->var.op.args.firstp->nextp = chexp->var.op.args.firstp->nextp;
            chexp->var.op.args.firstp->nextp = hexp;
            _RDB_expr_list_set_lastp(&exp->var.op.args);
            _RDB_expr_list_set_lastp(&chexp->var.op.args);
            
            exp = chexp;
        } else if (strcmp(chexp->var.op.name, "RENAME") == 0) {
            RDB_expression *argp;

            /*
             * WHERE(RENAME( -> RENAME(WHERE(
             * Only if the where condition does not contain
             * variables which have been "renamed away".
             */

            argp = chexp->var.op.args.firstp->nextp;
            while (argp != NULL) {
                if (_RDB_expr_refers_var(exp->var.op.args.firstp->nextp,
                        RDB_obj_string(RDB_expr_obj(argp))))
                    return RDB_OK;
                argp = argp->nextp->nextp;
            }

            if (exp->typ != NULL) {
                RDB_drop_type(exp->typ, ecp, NULL);
                exp->typ = NULL;
            }

            if (_RDB_invrename_expr(chexp->nextp, chexp, ecp)
                    != RDB_OK)
                return RDB_ERROR;

            if (chexp->typ != NULL) {
                RDB_drop_type(chexp->typ, ecp, NULL);
                chexp->typ = NULL;
            }

            hname = chexp->var.op.name;
            chexp->var.op.name = exp->var.op.name;
            exp->var.op.name = hname;
            
            hexp = exp->var.op.args.firstp->nextp;
            exp->var.op.args.firstp->nextp = chexp->var.op.args.firstp->nextp;
            chexp->var.op.args.firstp->nextp = hexp;
            _RDB_expr_list_set_lastp(&exp->var.op.args);
            _RDB_expr_list_set_lastp(&chexp->var.op.args);

            exp = chexp;
        } else {
            return _RDB_transform(chexp, ecp, txp);
        }
    }
}

static int
swap_project_union(RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *nexp, *nargp;
    RDB_expression *argp;
    char *hname;
    RDB_expression *chexp = exp->var.op.args.firstp;

    assert(strcmp(exp->var.op.name, "PROJECT") == 0);
    assert(strcmp(chexp->var.op.name, "UNION") == 0);

    /*
     * Create new project table for child #2
     */
    nexp = RDB_ro_op("PROJECT", ecp);
    if (nexp == NULL) {
        return RDB_ERROR;
    }
    RDB_add_arg(nexp, chexp->var.op.args.firstp->nextp);

    argp = exp->var.op.args.firstp->nextp;
    while (argp != NULL) {
        nargp = RDB_dup_expr(argp, ecp);
        if (nargp == NULL) {
            RDB_drop_expr(nexp, ecp);
            return RDB_ERROR;
        }
        RDB_add_arg(nexp, nargp);
        argp = argp->nextp;
    }

    if (exp->typ != NULL) {
        RDB_drop_type(exp->typ, ecp, NULL);
        exp->typ = NULL;
    }
    if (chexp->typ != NULL) {
        RDB_drop_type(chexp->typ, ecp, NULL);
        chexp->typ = NULL;
    }

    /*
     * Swap parent and child
     */

    hname = exp->var.op.name;
    exp->var.op.name = chexp->var.op.name;
    chexp->var.op.name = hname;

    chexp->var.op.args.firstp->nextp = exp->var.op.args.firstp->nextp;

    exp->var.op.args.firstp = exp->var.op.args.lastp = NULL;
    RDB_add_arg(exp, chexp);
    RDB_add_arg(exp, nexp);

    return RDB_OK;
}

static RDB_expression *
proj_attr(const RDB_expression *exp, const char *attrname)
{
    RDB_expression *argp = exp->var.op.args.firstp->nextp;

    while (argp != NULL) {
        if (strcmp(RDB_obj_string(&argp->var.obj), attrname) == 0)
            return argp;
        argp = argp->nextp;
    }
    return NULL;
}

/**
 * Transform PROJECT(RENAME) to RENAME(PROJECT) or PROJECT
 */
static int
swap_project_rename(RDB_expression *texp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char *opname;
    RDB_expression *argp, *cargp;
    RDB_expression *hexp;
    RDB_expression *chtexp = texp->var.op.args.firstp;

    /*
     * Alter parent by removing renamings which become obsolete
     * because of the projection
     */

    /* Remove renamings whose dest does not appear in the parent */
    argp = chtexp->var.op.args.firstp;
    while (argp->nextp != NULL) {
        if (proj_attr(texp, RDB_obj_string(&argp->nextp->nextp->var.obj))
                == NULL) {
            hexp = argp->nextp->nextp->nextp;
            RDB_drop_expr(argp->nextp, ecp);
            RDB_drop_expr(argp->nextp->nextp, ecp);
            argp->nextp = hexp;
        } else {
            argp = argp->nextp->nextp;
        }
    }
    if (chtexp->var.op.args.firstp->nextp == NULL) {
        /*
         * Remove child
         */
        chtexp->var.op.args.firstp->nextp = texp->var.op.args.firstp->nextp;
        chtexp->var.op.args.lastp = texp->var.op.args.lastp;
        texp->var.op.args.firstp = chtexp->var.op.args.firstp;
        if (_RDB_destroy_expr(chtexp, ecp) != RDB_OK)
            return RDB_ERROR;
        free(chtexp);

        return transform_project(texp, ecp, txp);
    }

    /*
     * Swap parent and child
     */

    /* Change project attributes */
    argp = texp->var.op.args.firstp->nextp;
    while (argp != NULL) {
        cargp = chtexp->var.op.args.firstp->nextp;
        while (cargp != NULL
                && strcmp(RDB_obj_string(&cargp->nextp->var.obj),
                        RDB_obj_string(&argp->var.obj)) != 0) {
            cargp = cargp->nextp->nextp;
        }
        if (cargp != NULL) {
            if (RDB_string_to_obj(&argp->var.obj,
                    RDB_obj_string(&cargp->var.obj), ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
        argp = argp->nextp;
    }

    if (texp->typ != NULL) {
        RDB_drop_type(texp->typ, ecp, NULL);
        texp->typ = NULL;
    }
    if (chtexp->typ != NULL) {
        RDB_drop_type(chtexp->typ, ecp, NULL);
        chtexp->typ = NULL;
    }

    opname = texp->var.op.name;
    texp->var.op.name = chtexp->var.op.name;
    chtexp->var.op.name = opname;

    hexp = texp->var.op.args.firstp->nextp;
    texp->var.op.args.firstp->nextp = chtexp->var.op.args.firstp->nextp;
    chtexp->var.op.args.firstp->nextp = hexp;
    _RDB_expr_list_set_lastp(&texp->var.op.args);
    _RDB_expr_list_set_lastp(&chtexp->var.op.args);

    return RDB_OK;
}

/**
 * Transform PROJECT(EXTEND) to EXTEND(PROJECT) or PROJECT
 */
static int
transform_project_extend(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int expc;
    RDB_expression **expv;
    RDB_expression *argp, *hexp;
    RDB_expression *chexp = exp->var.op.args.firstp;
    int chargc = RDB_expr_list_length(&chexp->var.op.args);

    /*
     * Remove EXTEND-Attributes which are 'projected away'
     */

    expv = malloc(sizeof (RDB_expression *) * chargc);
    if (expv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    expv[0] = chexp->var.op.args.firstp;
    expc = 1;
    argp = chexp->var.op.args.firstp;
    while (argp->nextp != NULL) {
        if (proj_attr(exp, RDB_obj_string(&argp->nextp->nextp->var.obj)) != NULL) {
            expv[expc] = argp->nextp;
            expv[expc + 1] = argp->nextp->nextp;
            expc += 2;
            argp = argp->nextp->nextp;
        } else {
            hexp = argp->nextp->nextp->nextp;
            RDB_drop_expr(argp->nextp->nextp, ecp);
            RDB_drop_expr(argp->nextp, ecp);
            argp->nextp = hexp;
        }
    }

    if (expc == 1) {
        /* Extend became empty - remove */
        expv[0]->nextp = exp->var.op.args.firstp->nextp;
        exp->var.op.args.firstp = expv[0];
        free(expv);
        if (_RDB_destroy_expr(chexp, ecp) != RDB_OK)
            return RDB_ERROR;
        free(chexp);
        return transform_project(exp, ecp, txp);
    }
    chexp->var.op.args.firstp = chexp->var.op.args.lastp = NULL;
    for (i = 0; i < expc; i++)
        RDB_add_arg(chexp, expv[i]);
    if (chexp->typ != NULL) {
        RDB_drop_type(chexp->typ, ecp, NULL);
        chexp->typ = NULL;
    }

    return _RDB_transform(chexp, ecp, txp);
}

/**
 * Transform PROJECT(WHERE) to WHERE(PROJECT) or PROJECT(WHERE(PROJECT))
 */
static int
swap_project_where(RDB_expression *exp, RDB_expression *chexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int attrc;
    char **attrv;
    RDB_expression *argp;
    RDB_type *chtyp = RDB_expr_type(chexp, NULL, NULL, ecp, txp);
    if (chtyp == NULL)
        return RDB_ERROR;

    attrv = malloc(sizeof(char *) * chtyp->var.basetyp->var.tuple.attrc);
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /*
     * Get attributes from parent
     */
    attrc = RDB_expr_list_length(&exp->var.op.args) - 1;
    argp = exp->var.op.args.firstp->nextp;
    for (i = 0; i < attrc; i++) {
        attrv[i] = RDB_obj_string(&argp->var.obj);
        argp = argp->nextp;
    }

    /*
     * Add attributes from child which are not attributes of the parent,
     * but are referred to by the select condition
     */
    for (i = 0; i < chtyp->var.basetyp->var.tuple.attrc; i++) {
        char *attrname = chtyp->var.basetyp->var.tuple.attrv[i].name;

        if (proj_attr(exp, attrname) == NULL
                && _RDB_expr_refers_var(chexp, attrname)) {
            attrv[attrc++] = attrname;
        }
    }

    if (attrc > RDB_expr_list_length(&exp->var.op.args) - 1) {
        /*
         * Add project
         */
        RDB_expression *argp;
        RDB_expression *nexp = RDB_ro_op("PROJECT", ecp);
        if (nexp == NULL)
            return RDB_ERROR;
        nexp->nextp = chexp->var.op.args.firstp->nextp;

        RDB_add_arg(nexp, chexp->var.op.args.firstp);
        for (i = 0; i < attrc; i++) {
            argp = RDB_string_to_expr(attrv[i], ecp);
            if (argp == NULL) {
                RDB_drop_expr(nexp, ecp);
                free(attrv);
                return RDB_ERROR;
            }
            RDB_add_arg(nexp, argp);
        }
        chexp->var.op.args.firstp = nexp;
    } else {
        char *hname;
        RDB_expression *hexp;

        if (exp->typ != NULL) {
            RDB_drop_type(exp->typ, ecp, NULL);
            exp->typ = NULL;
        }
        RDB_drop_type(chexp->typ, ecp, NULL);
        chexp->typ = NULL;

        /*
         * Swap WHERE and project
         */
        hname = exp->var.op.name;
        exp->var.op.name = chexp->var.op.name;
        chexp->var.op.name = hname;

        hexp = exp->var.op.args.firstp->nextp;
        exp->var.op.args.firstp->nextp = chexp->var.op.args.firstp->nextp;
        chexp->var.op.args.firstp->nextp = hexp;

        _RDB_expr_list_set_lastp(&exp->var.op.args);
        _RDB_expr_list_set_lastp(&chexp->var.op.args);
    }
    free(attrv);

    return RDB_OK;
}

static int
transform_project(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *chexp, *argp;

    do {
        chexp = exp->var.op.args.firstp;
        if (chexp->kind != RDB_EX_RO_OP)
            return RDB_OK;

        if (strcmp(chexp->var.op.name, "PROJECT") == 0) {
            /*
             * Merge projects by eliminating the child
             */
            argp = chexp->var.op.args.firstp->nextp;
            while (argp != NULL) {
                RDB_expression *nextp = argp->nextp;
                if (RDB_drop_expr(argp, ecp) != RDB_OK)
                    return RDB_ERROR;
                argp = nextp;
            }
            exp->var.op.args.firstp = chexp->var.op.args.firstp;
            exp->var.op.args.firstp->nextp = chexp->nextp;
            if (_RDB_destroy_expr(chexp, ecp) != RDB_OK)
                return RDB_ERROR;
            free(chexp);
        } else if (strcmp(chexp->var.op.name, "UNION") == 0) {
            if (swap_project_union(exp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            if (transform_project(exp->var.op.args.firstp->nextp, ecp, txp)
                    != RDB_OK)
                return RDB_ERROR;
            exp = chexp;
        } else if (strcmp(chexp->var.op.name, "WHERE") == 0) {
            if (swap_project_where(exp, chexp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            if (strcmp(chexp->var.op.name, "WHERE") == 0) {
                return transform_where(chexp, ecp, txp);
            }
            exp = chexp;
        } else if (strcmp(chexp->var.op.name, "RENAME") == 0) {
            if (swap_project_rename(exp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            exp = exp->var.op.args.firstp;
            assert(exp->kind <= RDB_EX_RO_OP);
        } else if (strcmp(chexp->var.op.name, "EXTEND") == 0) {
            return transform_project_extend(exp, ecp, txp);
        } else {
            return _RDB_transform(chexp, ecp, txp);
        }
    } while (exp->kind == RDB_EX_RO_OP
            && strcmp(exp->var.op.name, "PROJECT") == 0);
    assert(exp->kind <= RDB_EX_RO_OP);
    if (exp->kind == RDB_EX_RO_OP) {
        argp = exp->var.op.args.firstp;
        while (argp != NULL) {
            if (_RDB_transform(argp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
        }
    }
    return RDB_OK;
}

/**
 * Convert REMOVE into PROJECT
 */
int
_RDB_remove_to_project(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression **argv;
    RDB_expression *argp, *nargp, *lastnargp;
    RDB_expr_list oargs;
    int attrc;
    int i, ai;
    char *opname;
    RDB_type *chtyp;
    RDB_type *tpltyp;

    if (exp->var.op.args.firstp == NULL) {
        RDB_raise_invalid_argument("invalid number of REMOVE arguments", ecp);
        return RDB_ERROR;
    }

    argp = exp->var.op.args.firstp->nextp;
    while (argp != NULL) {
        if (argp->kind != RDB_EX_OBJ || argp->var.obj.typ != &RDB_STRING) {
            RDB_raise_type_mismatch("STRING argument required for REMOVE", ecp);
            return RDB_ERROR;
        }
        argp = argp->nextp;
    }                

    chtyp = RDB_expr_type(exp->var.op.args.firstp, NULL, NULL, ecp, txp);
    if (chtyp == NULL)
        return RDB_ERROR;
    if (chtyp->kind == RDB_TP_RELATION) {
        tpltyp = chtyp->var.basetyp;
    } else if (chtyp->kind == RDB_TP_TUPLE) {
        tpltyp = chtyp;
    } else {
        RDB_raise_type_mismatch("invalid type of REMOVE argument", ecp);
        return RDB_ERROR;
    }

    ai = 1;
    attrc = tpltyp->var.tuple.attrc - RDB_expr_list_length(&exp->var.op.args) + 1;
    if (attrc < 0) {
        RDB_raise_invalid_argument("invalid number of REMOVE arguments", ecp);
        goto error;
    }

    /* Old attributes 1 .. n */
    oargs.firstp = exp->var.op.args.firstp->nextp;
    oargs.lastp = exp->var.op.args.lastp;

    nargp = lastnargp = exp->var.op.args.firstp;
    nargp->nextp = NULL;

    for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
        argp = oargs.firstp;
        while (argp != NULL
                && strcmp(RDB_obj_string(&argp->var.obj),
                        tpltyp->var.tuple.attrv[i].name) != 0)
            argp = argp->nextp;
        if (argp == NULL) {
            /* Not found - take attribute */

            if (ai >= attrc + 1) {
                RDB_raise_invalid_argument("invalid REMOVE arguments", ecp);
                goto error;
            }

            argp = RDB_string_to_expr(tpltyp->var.tuple.attrv[i].name,
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

    opname = realloc(exp->var.op.name, 8);
    if (opname == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    strcpy(opname, "PROJECT");
    exp->var.op.name = opname;

    RDB_destroy_expr_list(&oargs, ecp);
    exp->var.op.args.firstp = nargp;
    exp->var.op.args.lastp = lastnargp;

    return RDB_OK;

error:
    if (argv != NULL) {
        for (i = 1; i < ai; i++) {
            RDB_drop_expr(argv[i], ecp);
        }
    }
    return RDB_ERROR;
}

/**
 * Transform REMOVE
 */
static int
transform_remove(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (_RDB_remove_to_project(exp, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    return transform_project(exp, ecp, txp);
}

/**
 * Transform UPDATE into RENAME(PROJECT(EXTEND(
 */
static int
transform_update(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object nattrname;
    RDB_expression *pexp, *xexp;
    RDB_expression *argp = exp->var.op.args.firstp->nextp;

    strcpy(exp->var.op.name, "RENAME");

    pexp = RDB_ro_op("REMOVE", ecp);
    if (pexp == NULL)
        return RDB_ERROR;
    pexp->nextp = NULL;

    xexp = RDB_ro_op("EXTEND", ecp);
    if (xexp == NULL)
        return RDB_ERROR;
    RDB_add_arg(pexp, xexp);
    RDB_add_arg(xexp, exp->var.op.args.firstp);

    exp->var.op.args.firstp = exp->var.op.args.lastp = pexp;

    RDB_init_obj(&nattrname);
    while (argp != NULL) {
        RDB_expression *nargp, *hexp;

        if (argp->nextp == NULL) {
            RDB_raise_invalid_argument("UPDATE: invalid number of arguments",
                    ecp);
            return RDB_ERROR;
        }
        nargp = argp->nextp->nextp;

        if (argp->kind != RDB_EX_OBJ) {
            RDB_raise_invalid_argument("UPDATE argument must be STRING",
                    ecp);
            return RDB_ERROR;
        }

        /* Build attribute name prefixed by $ */
        if (RDB_string_to_obj(&nattrname, "$", ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(&nattrname, RDB_obj_string(RDB_expr_obj(argp)),
                ecp) != RDB_OK)
            return RDB_ERROR;

        /* Append EXTEND args */
        RDB_add_arg(xexp, argp->nextp);
        hexp = RDB_obj_to_expr(&nattrname, ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        RDB_add_arg(xexp, hexp);

        /* Append REMOVE args */
        hexp = RDB_obj_to_expr(RDB_expr_obj(argp), ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        RDB_add_arg(pexp, hexp);

        /* Append RENAME args */
        hexp = RDB_obj_to_expr(&nattrname, ecp);
        if (hexp == NULL)
            return RDB_ERROR;
        RDB_add_arg(exp, hexp);
        RDB_add_arg(exp, argp);

        argp = nargp;
    }
    RDB_destroy_obj(&nattrname, ecp);

    return transform_remove(pexp, ecp, txp);
}

static int
transform_is_empty(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *chexp = exp->var.op.args.firstp;

    if (_RDB_transform(chexp, ecp, txp) != RDB_OK)
       return RDB_ERROR;

    /* Add projection, if not already present */
    if (chexp->kind != RDB_EX_RO_OP || strcmp(chexp->var.op.name, "PROJECT") != 0
            || chexp->var.op.args.firstp->nextp != NULL) {
        RDB_expression *pexp = RDB_ro_op("PROJECT", ecp);
        if (pexp == NULL)
            return RDB_ERROR;

        RDB_add_arg(pexp, chexp);

        pexp->nextp = NULL;
        exp->var.op.args.firstp = exp->var.op.args.lastp = pexp;

        return transform_project(pexp, ecp, txp);
    }

    return RDB_OK;
}

/**
 * Perform algebraic optimization.
 * Eliminating NOT in WHERE expressions is not performed here,
 * because it conflicts with optimizing certain UNIONs.
 */
int
_RDB_transform(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *argp;

    if (exp->kind != RDB_EX_RO_OP)
        return RDB_OK;

    argp = exp->var.op.args.firstp;
    while (argp != NULL) {
        if (_RDB_transform(argp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
        argp = argp->nextp;
    }
    if (strcmp(exp->var.op.name, "UPDATE") == 0) {
        return transform_update(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "UNION") == 0) {
        return transform_union(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "WHERE") == 0) {
        return transform_where(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        return transform_project(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "REMOVE") == 0) {
        return transform_remove(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "IS_EMPTY") == 0) {
        return transform_is_empty(exp, ecp, txp);
    }
    return RDB_OK;
}
