/*
 * $Id$
 *
 * Copyright (C) 2004-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

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
        if (_RDB_expr_equals(ex1p, ex2p->var.op.argv[0], ecp, txp, resp)
                != RDB_OK)
            return RDB_ERROR;
        if (*resp)
            return RDB_OK;            
    }

    if (strcmp(ex1p->var.op.name, "NOT") == 0) {
        if (_RDB_expr_equals(ex1p->var.op.argv[0], ex2p, ecp, txp, resp)
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
    RDB_bool merge;

    if (exp->var.op.argv[0]->kind == RDB_EX_RO_OP
            && strcmp(exp->var.op.argv[0]->var.op.name, "PROJECT") == 0
            && exp->var.op.argv[1]->kind == RDB_EX_RO_OP
            && strcmp(exp->var.op.argv[1]->var.op.name, "PROJECT") == 0) {
        RDB_expression *wex1p = exp->var.op.argv[0]->var.op.argv[0];
        RDB_expression *wex2p = exp->var.op.argv[1]->var.op.argv[0];
        if (wex1p->kind == RDB_EX_RO_OP
                && strcmp(wex1p->var.op.name, "WHERE") == 0
                && wex2p->kind == RDB_EX_RO_OP
                && strcmp(wex2p->var.op.name, "WHERE") == 0) {
            if (exprs_compl(wex1p->var.op.argv[1], wex2p->var.op.argv[1],
                    ecp, txp, &merge) != RDB_OK)
                return RDB_ERROR;
            if (merge) {
                RDB_expression *pexp;
                /*
                 * Replace (T WHERE C) { ... } UNION (T WHERE NOT C) { ... }
                 * by T { ... }
                 */

                if (RDB_drop_expr(exp->var.op.argv[1], ecp) != RDB_OK)
                    return RDB_ERROR;

                free(exp->var.op.name);
                pexp = exp->var.op.argv[0];
                exp->var.op.name = pexp->var.op.name;
                exp->var.op.argc = pexp->var.op.argc;
                free(exp->var.op.argv);
                exp->var.op.argv = pexp->var.op.argv;
                exp->var.op.argv[0] = wex1p->var.op.argv[0];

                free(pexp);
                if (RDB_drop_expr(wex1p->var.op.argv[1], ecp) != RDB_OK)
                    return RDB_ERROR;
                free(wex1p->var.op.name);
                free(wex1p->var.op.argv);

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
    int hargc;
    RDB_expression **hargv;
    RDB_expression *hexp;

    if (_RDB_transform(exp->var.op.argv[0], ecp, txp) != RDB_OK)
        return RDB_ERROR;

    for(;;) {
        RDB_expression *chexp = exp->var.op.argv[0];
        if (chexp->kind != RDB_EX_RO_OP)
            return RDB_OK;

        if (strcmp(chexp->var.op.name, "WHERE") == 0) {
            /*
             * Merge WHERE expressions
             */
            RDB_expression *condp = RDB_ro_op("AND", 2, ecp);
            if (condp == NULL)
                return RDB_ERROR;
            RDB_add_arg(condp, exp->var.op.argv[1]);
            RDB_add_arg(condp, chexp->var.op.argv[1]);

            exp->var.op.argv[0] = chexp->var.op.argv[0];
            exp->var.op.argv[1] = condp;

            free(chexp->var.op.name);
            free(chexp->var.op.argv);
            free(chexp);
        } else if (strcmp(chexp->var.op.name, "MINUS") == 0
                || strcmp(chexp->var.op.name, "SEMIMINUS") == 0
                || strcmp(chexp->var.op.name, "SEMIJOIN") == 0) {
            /*
             * WHERE(SEMIMINUS( -> SEMIMINUS(WHERE(
             * Same for MINUS, SEMIJOIN
             */
            if (_RDB_transform(chexp->var.op.argv[1], ecp, txp) != RDB_OK)
                return RDB_ERROR;

            hname = exp->var.op.name;
            exp->var.op.name = chexp->var.op.name;
            chexp->var.op.name = hname;

            hexp = chexp->var.op.argv[1];
            chexp->var.op.argv[1] = exp->var.op.argv[1];
            exp->var.op.argv[1] = hexp;

            exp = chexp;
        } else if (strcmp(chexp->var.op.name, "UNION") == 0) {
            /*
             * WHERE(UNION( -> UNION(WHERE(
             */
            RDB_expression *wexp;
            RDB_expression *condp = RDB_dup_expr(exp->var.op.argv[1], ecp);
            if (condp == NULL)
                return RDB_ERROR;

            wexp = RDB_ro_op("WHERE", 2, ecp);
            if (wexp == NULL) {
                RDB_drop_expr(condp, ecp);
                return RDB_ERROR;
            }
            RDB_add_arg(wexp, exp->var.op.argv[0]->var.op.argv[1]);
            RDB_add_arg(wexp, condp);

            /* Swap operator names */
            hname = chexp->var.op.name;
            chexp->var.op.name = exp->var.op.name;
            exp->var.op.name = hname;

            chexp->var.op.argv[1] = exp->var.op.argv[1];
            exp->var.op.argv[1] = wexp;

            return _RDB_transform(chexp, ecp, txp);
        } else if (strcmp(chexp->var.op.name, "EXTEND") == 0) {
            /*
             * WHERE(EXTEND( -> EXTEND(WHERE(
             */
            if (_RDB_resolve_extend_expr(&exp->var.op.argv[1], chexp, ecp)
                    != RDB_OK)
                return RDB_ERROR;

            hname = chexp->var.op.name;
            chexp->var.op.name = exp->var.op.name;
            exp->var.op.name = hname;
            
            hargv = chexp->var.op.argv;
            hargc = chexp->var.op.argc;
            chexp->var.op.argv = exp->var.op.argv;
            chexp->var.op.argc = exp->var.op.argc;
            exp->var.op.argv = hargv;
            exp->var.op.argc = hargc;

            hexp = chexp->var.op.argv[0];
            chexp->var.op.argv[0] = exp->var.op.argv[0];
            exp->var.op.argv[0] = hexp;
            
            exp = chexp;
        } else if (strcmp(chexp->var.op.name, "RENAME") == 0) {
            /*
             * WHERE(RENAME( -> RENAME(WHERE(
             * Only if the variables in the where condition 
             * all appear in the RENAME table
             */
            if (RDB_expr_type(exp, NULL, ecp, txp) == NULL) {
                RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
                if (errtyp != &RDB_ATTRIBUTE_NOT_FOUND_ERROR)
                    return RDB_ERROR;
                RDB_clear_err(ecp);
                return RDB_OK;
            }
            if (_RDB_invrename_expr(exp->var.op.argv[1], chexp, ecp) != RDB_OK)
                return RDB_ERROR;

            hname = chexp->var.op.name;
            chexp->var.op.name = exp->var.op.name;
            exp->var.op.name = hname;
            
            hargv = chexp->var.op.argv;
            hargc = chexp->var.op.argc;
            chexp->var.op.argv = exp->var.op.argv;
            chexp->var.op.argc = exp->var.op.argc;
            exp->var.op.argv = hargv;
            exp->var.op.argc = hargc;

            hexp = chexp->var.op.argv[0];
            chexp->var.op.argv[0] = exp->var.op.argv[0];
            exp->var.op.argv[0] = hexp;
            
            exp = chexp;
        } else {
            return _RDB_transform(chexp, ecp, txp);
        }
    }
}

static int
swap_project_union(RDB_expression *exp, RDB_expression *chexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *nexp, *nargp;
    RDB_expression **hargv;
    char *hname;

    /*
     * Create new project table for child #2
     */
    nexp = RDB_ro_op("PROJECT", exp->var.op.argc, ecp);
    if (nexp == NULL) {
        return RDB_ERROR;
    }

    RDB_add_arg(nexp, chexp->var.op.argv[1]);

    for (i = 1; i < exp->var.op.argc; i++) {
        nargp = RDB_dup_expr(exp->var.op.argv[i], ecp);
        if (nargp == NULL) {
            RDB_drop_expr(nexp, ecp);
            return RDB_ERROR;
        }
        RDB_add_arg(nexp, nargp);
    }

    /*
     * Swap parent and child
     */

    hname = exp->var.op.name;
    exp->var.op.name = chexp->var.op.name;
    chexp->var.op.name = hname;

    hargv = chexp->var.op.argv;
    chexp->var.op.argc = exp->var.op.argc;
    chexp->var.op.argv = exp->var.op.argv;
    exp->var.op.argc = 2;
    exp->var.op.argv = hargv;

    /*
     * Add new child
     */

    chexp->var.op.argv[0] = exp->var.op.argv[0];
    exp->var.op.argv[0] = chexp;
    exp->var.op.argv[1] = nexp;

    return RDB_OK;
}

static RDB_expression *
proj_attr(const RDB_expression *exp, const char *attrname)
{
    int i;
    
    for (i = 1; i < exp->var.op.argc; i++) {
        if (strcmp(RDB_obj_string(&exp->var.op.argv[i]->var.obj),
                   attrname) == 0)
            return exp->var.op.argv[i];
    }
    return NULL;
}

/* Transforms PROJECT(RENAME) to RENAME(PROJECT) or PROJECT */
static int
swap_project_rename(RDB_expression *texp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int si, di;
    int i, j;
    int argc;
    RDB_expression **argv;
    char *opname;
    RDB_expression *chtexp = texp->var.op.argv[0];

    /*
     * Alter parent by removing renamings which become obsolete
     * because of the projection
     */

    /* Remove renamings whose dest does not appear in the parent */
    di = 1;
    for (si = 1; si + 1 < chtexp->var.op.argc; si += 2) {
        if (proj_attr(texp, RDB_obj_string(&chtexp->var.op.argv[si + 1]->var.obj))
                == NULL) {
            RDB_drop_expr(chtexp->var.op.argv[si], ecp);
            RDB_drop_expr(chtexp->var.op.argv[si + 1], ecp);
        } else {
            if (di < si) {
                chtexp->var.op.argv[di] = chtexp->var.op.argv[si];
                chtexp->var.op.argv[di + 1] = chtexp->var.op.argv[si + 1];
            }
            di += 2;
        }
    }
    if (di < si) {
        chtexp->var.op.argc = di;
        if (di > 1) {
            chtexp->var.op.argv = realloc(chtexp->var.op.argv,
                    di * sizeof(RDB_expression *));
        } else {
            /*
             * Remove child
             */
            texp->var.op.argv[0] = chtexp->var.op.argv[0];
            free(chtexp->var.op.name);
            for (i = 1; i < chtexp->var.op.argc; i++)
                RDB_drop_expr(chtexp->var.op.argv[i], ecp);
            free(chtexp->var.op.argv);
            free(chtexp);

            return transform_project(texp, ecp, txp);
        }
    }

    /*
     * Swap parent and child
     */

    /* Change project attributes */
    for (i = 1; i < texp->var.op.argc; i++) {
        j = 1;
        while (j + 1 < chtexp->var.op.argc
                && strcmp(RDB_obj_string(&chtexp->var.op.argv[j + 1]->var.obj),
                        RDB_obj_string(&texp->var.op.argv[i]->var.obj)) != 0) {
            j += 2;
        }
        if (j + 1 < chtexp->var.op.argc) {
            if (RDB_string_to_obj(&texp->var.op.argv[i]->var.obj,
                    RDB_obj_string(&chtexp->var.op.argv[j]->var.obj),
                    ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }

    argc = texp->var.op.argc;
    argv = texp->var.op.argv;
    opname = texp->var.op.name;

    texp->var.op.argc = chtexp->var.op.argc;
    texp->var.op.argv = chtexp->var.op.argv;
    texp->var.op.name = chtexp->var.op.name;
    chtexp->var.op.argc = argc;
    chtexp->var.op.argv = argv;
    chtexp->var.op.name = opname;

    chtexp->var.op.argv[0] = texp->var.op.argv[0];
    texp->var.op.argv[0] = chtexp;

    return RDB_OK;
}

/* Transforms PROJECT(EXTEND) to EXTEND(PROJECT) or PROJECT */
static int
transform_project_extend(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int expc;
    RDB_expression **expv;
    RDB_expression *chexp = exp->var.op.argv[0];

    /*
     * Remove EXTEND-Attributes which are 'projected away'
     */

    expv = malloc(sizeof (RDB_expression *) * chexp->var.op.argc);
    if (expv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    expv[0] = chexp->var.op.argv[0];
    expc = 1;
    for (i = 1; i < chexp->var.op.argc; i += 2) {
        if (proj_attr(exp, RDB_obj_string(&chexp->var.op.argv[i + 1]->var.obj))
                != NULL) {
            expv[expc] = chexp->var.op.argv[i];
            expv[expc + 1] = chexp->var.op.argv[i + 1];
            expc += 2;
        } else {
            RDB_drop_expr(chexp->var.op.argv[i], ecp);
            RDB_drop_expr(chexp->var.op.argv[i + 1], ecp);
        }
    }

    free(chexp->var.op.argv);

    if (expc == 1) {
        /* Extend became empty - remove */
        exp->var.op.argv[0] = expv[0];
        free(expv);
        free(chexp->var.op.name);
        free(chexp);
        return transform_project(exp, ecp, txp);
    }
    chexp->var.op.argv = expv;
    chexp->var.op.argc = expc;

    return _RDB_transform(chexp, ecp, txp);
}

/*
 * Transforms PROJECT(WHERE) to WHERE(PROJECT) or PROJECT(WHERE(PROJECT))
 */
static int
swap_project_where(RDB_expression *exp, RDB_expression *chexp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int attrc;
    char **attrv;
    RDB_type *chtyp = RDB_expr_type(chexp, NULL, ecp, txp);
    if (chtyp == NULL)
        return RDB_ERROR;

    attrv = malloc(sizeof(char *) * chtyp->var.basetyp->var.tuple.attrc);
    if (attrv == NULL) {
        RDB_drop_type(chtyp, ecp, NULL);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /*
     * Get attributes from parent
     */
    attrc = exp->var.op.argc - 1;
    for (i = 0; i < attrc; i++) {
        attrv[i] = RDB_obj_string(&exp->var.op.argv[i + 1]->var.obj);
    }

    /*
     * Add attributes from child which are not attributes of the parent,
     * but are referred to by the select condition
     */
    for (i = 0; i < chtyp->var.basetyp->var.tuple.attrc; i++) {
        char *attrname = chtyp->var.basetyp->var.tuple.attrv[i].name;

        if (proj_attr(exp, attrname) == NULL
                && _RDB_expr_refers_attr(chexp, attrname)) {
            attrv[attrc++] = attrname;
        }
    }
    if (attrc > exp->var.op.argc - 1) {
        /*
         * Add project
         */
        RDB_expression *argp;
        RDB_expression *nexp = RDB_ro_op("PROJECT", attrc + 1, ecp);
        if (nexp == NULL)
            return RDB_ERROR;

        RDB_add_arg(nexp, chexp->var.op.argv[0]);
        for (i = 0; i < attrc; i++) {
            argp = RDB_string_to_expr(attrv[i], ecp);
            if (argp == NULL) {
                RDB_drop_expr(nexp, ecp);
                free(attrv);
                return RDB_ERROR;
            }
            RDB_add_arg(nexp, argp);
        }
        free(attrv);
        chexp->var.op.argv[0] = nexp;
    } else {
        char *hname;
        int hargc;
        RDB_expression **hargv;

        free(attrv);

        /*
         * Swap WHERE and project
         */
        hname = exp->var.op.name;
        exp->var.op.name = chexp->var.op.name;
        chexp->var.op.name = hname;

        hargc = exp->var.op.argc;
        hargv = exp->var.op.argv;
        exp->var.op.argc = chexp->var.op.argc;
        exp->var.op.argv = chexp->var.op.argv;
        chexp->var.op.argc = hargc;
        chexp->var.op.argv = hargv;
         
        chexp->var.op.argv[0] = exp->var.op.argv[0];
        exp->var.op.argv[0] = chexp;        
    }

    return RDB_OK;
}

static int
transform_project(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *chexp;
    int i;

    do {
        chexp = exp->var.op.argv[0];
/*
        if (_RDB_transform(chexp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
*/

        if (chexp->kind != RDB_EX_RO_OP)
            return RDB_OK;
        if (strcmp(chexp->var.op.name, "PROJECT") == 0) {
            /* Merge projects by eliminating the child */
            exp->var.op.argv[0] = chexp->var.op.argv[0];
            for (i = 1; i < chexp->var.op.argc; i++) {
                if (RDB_drop_expr(chexp->var.op.argv[i], ecp) != RDB_OK)
                    return RDB_ERROR;
            }
            free(chexp->var.op.name);
            free(chexp->var.op.argv);
            free(chexp);
        } else if (strcmp(chexp->var.op.name, "UNION") == 0) {
            if (swap_project_union(exp, chexp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
            if (transform_project(exp->var.op.argv[1], ecp, txp) != RDB_OK)
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
            exp = chexp;
        } else if (strcmp(chexp->var.op.name, "EXTEND") == 0) {
            return transform_project_extend(exp, ecp, txp);
        } else {
            return _RDB_transform(chexp, ecp, txp);
        }
    } while (exp->kind == RDB_EX_RO_OP
            && strcmp(exp->var.op.name, "PROJECT") == 0);
    return _RDB_transform(chexp, ecp, txp);
}

/*
 * Transform REMOVE into PROJECT
 */
static int
transform_remove(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression **argv;
    int attrc;
    int i;
    RDB_expression *attrexp;
    char *opname;
    RDB_type *typ = RDB_expr_type(exp, NULL, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    for (i = 1; i < exp->var.op.argc; i++) {
        RDB_drop_expr(exp->var.op.argv[i], ecp);
    }
    exp->var.op.argc = 1; /* for the error case */
    attrc = typ->var.basetyp->var.tuple.attrc;
    argv = realloc(exp->var.op.argv, sizeof(RDB_expression *) * (attrc + 1));
    if (argv == NULL) {
        RDB_drop_type(typ, ecp, NULL);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    opname = realloc(exp->var.op.name, 8);
    if (opname == NULL) {
        RDB_drop_type(typ, ecp, NULL);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    strcpy(opname, "PROJECT");
    exp->var.op.name = opname;

    for (i = 0; i < attrc; i++) {
        attrexp = RDB_string_to_expr(typ->var.basetyp->var.tuple.attrv[i].name,
                ecp);
        if (attrexp == NULL) {
            RDB_drop_type(typ, ecp, NULL);
            return RDB_ERROR;
        }
        argv[i + 1] = attrexp;
    }

    exp->var.op.argc = attrc + 1;
    exp->var.op.argv = argv;

    RDB_drop_type(typ, ecp, NULL);
    return transform_project(exp, ecp, txp);
}

static int
transform_is_empty(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
   RDB_expression *chexp = exp->var.op.argv[0];
    
   if (_RDB_transform(chexp, ecp, txp) != RDB_OK)
       return RDB_ERROR;

    /* Add projection, if not already present */
    if (chexp->kind != RDB_EX_RO_OP || strcmp(chexp->var.op.name, "PROJECT") != 0
            || chexp->var.op.argc > 1) {
        RDB_expression *pexp = RDB_ro_op("PROJECT", 1, ecp);
        if (pexp == NULL)
            return RDB_ERROR;

        RDB_add_arg(pexp, chexp);

        exp->var.op.argv[0] = pexp;

        return transform_project(pexp, ecp, txp);
    }

    return RDB_OK;
}

/*
 * Performs algebraic optimization.
 * Eliminating NOT in WHERE expressions is not performed here,
 * because it conflicts with optimizing certain UNIONs.
 */
int
_RDB_transform(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    if (exp->kind != RDB_EX_RO_OP)
        return RDB_OK;

    for (i = 0; i < exp->var.op.argc; i++) {
        if (_RDB_transform(exp->var.op.argv[i], ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }
    if (strcmp(exp->var.op.name, "UNION") == 0) {
        return transform_union(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        return transform_project(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "REMOVE") == 0) {
        return transform_remove(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "WHERE") == 0) {
        return transform_where(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "IS_EMPTY") == 0) {
        return transform_is_empty(exp, ecp, txp);
    }

    return RDB_OK;
}
