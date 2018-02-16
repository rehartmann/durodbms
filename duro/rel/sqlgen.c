/*
 * SQL generation functions
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "sqlgen.h"
#include "rdb.h"
#include <obj/objinternal.h>
#include <obj/type.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static RDB_bool
scalar_sql_convertible(RDB_expression *);

static RDB_bool
explist_user_types(RDB_expr_list *explistp)
{
    RDB_exec_context ec;
    RDB_expression *exp;

    RDB_init_exec_context(&ec);
    exp = explistp->firstp;
    while (exp != NULL) {
        RDB_type *typ = RDB_expr_type(exp, NULL, NULL, NULL, &ec, NULL);
        if (typ == NULL) {
            RDB_destroy_exec_context(&ec);
            return RDB_FALSE;
        }
        if (RDB_type_is_scalar(typ) && !typ->def.scalar.builtin) {
            RDB_destroy_exec_context(&ec);
            return RDB_FALSE;
        }
        exp = exp->nextp;
    }
    RDB_destroy_exec_context(&ec);
    return RDB_TRUE;
}

static RDB_bool
explist_scalar_sql_convertible(RDB_expr_list *explistp)
{
    RDB_expression *exp;

    exp = explistp->firstp;
    while (exp != NULL) {
        if (!scalar_sql_convertible(exp))
            return RDB_FALSE;
        exp = exp->nextp;
    }
    return RDB_TRUE;
}

static RDB_bool
scalar_sql_convertible(RDB_expression *exp)
{
    RDB_type *typ;

    switch (exp->kind) {
    case RDB_EX_VAR:
        return RDB_TRUE;
    case RDB_EX_OBJ:
        typ = RDB_obj_type(RDB_expr_obj(exp));
        return (RDB_bool) (typ == &RDB_STRING || typ == &RDB_INTEGER
                || typ == &RDB_BOOLEAN || typ == &RDB_FLOAT);
    case RDB_EX_RO_OP:
        if (strcmp(exp->def.op.name, "=") == 0
                || strcmp(exp->def.op.name, "<>") == 0) {
            return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 2
                    && explist_scalar_sql_convertible(&exp->def.op.args));
        }
        if (strcmp(exp->def.op.name, ">") == 0
                || strcmp(exp->def.op.name, "<") == 0
                || strcmp(exp->def.op.name, ">=") == 0
                || strcmp(exp->def.op.name, "<=") == 0
                || strcmp(exp->def.op.name, "+") == 0
                || strcmp(exp->def.op.name, "*") == 0
                || strcmp(exp->def.op.name, "/") == 0
                || strcmp(exp->def.op.name, "%") == 0
                || strcmp(exp->def.op.name, "power") == 0
                || strcmp(exp->def.op.name, "atan2") == 0
                || strcmp(exp->def.op.name, "||") == 0) {
            return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 2
                    && explist_user_types(&exp->def.op.args)
                    && explist_scalar_sql_convertible(&exp->def.op.args));
        }
        if (strcmp(exp->def.op.name, "not") == 0
                || strcmp(exp->def.op.name, "abs") == 0
                || strcmp(exp->def.op.name, "sqrt") == 0
                || strcmp(exp->def.op.name, "sin") == 0
                || strcmp(exp->def.op.name, "cos") == 0
                || strcmp(exp->def.op.name, "atan") == 0
                || strcmp(exp->def.op.name, "log") == 0
                || strcmp(exp->def.op.name, "ln") == 0
                || strcmp(exp->def.op.name, "power") == 0
                || strcmp(exp->def.op.name, "exp") == 0
                || strcmp(exp->def.op.name, "strlen") == 0
                || strcmp(exp->def.op.name, "strlen_b") == 0
                || strcmp(exp->def.op.name, "cast_as_integer") == 0
                || strcmp(exp->def.op.name, "cast_as_float") == 0
                || strcmp(exp->def.op.name, "cast_as_string") == 0)
        {
            return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 1
                    && explist_user_types(&exp->def.op.args)
                    && scalar_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0)));
        }
        if (strcmp(exp->def.op.name, "substr") == 0) {
            return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 3
                    && explist_user_types(&exp->def.op.args)
                    && scalar_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0)));
        }
        if (strcmp(exp->def.op.name, "-") == 0) {
            RDB_int len = RDB_expr_list_length(&exp->def.op.args);
            return (RDB_bool) ((len == 1 || len == 2)
                    && explist_user_types(&exp->def.op.args)
                    && explist_scalar_sql_convertible(&exp->def.op.args));
        }
        return RDB_FALSE;
    default: ;
    }
    return RDB_FALSE;
}

RDB_bool
RDB_sql_convertible(RDB_expression *exp)
{
    if (exp->kind == RDB_EX_VAR
            || (exp->kind == RDB_EX_TBP
                && RDB_table_is_persistent(exp->def.tbref.tbp)))
        return RDB_TRUE;
    if (exp->kind != RDB_EX_RO_OP)
        return RDB_FALSE;
    if (strcmp(exp->def.op.name, "project") == 0) {
        if (RDB_expr_list_length(&exp->def.op.args) < 2)
            return RDB_FALSE;
        return (RDB_bool) RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0));
    }
    if (strcmp(exp->def.op.name, "rename") == 0) {
        if (RDB_expr_list_length(&exp->def.op.args) < 3)
            return RDB_FALSE;
        return (RDB_bool) RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0));
    }
    if (strcmp(exp->def.op.name, "where") == 0) {
        return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 2
                && RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0))
                && scalar_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 1)));
    }
    if (strcmp(exp->def.op.name, "join") == 0
            || strcmp(exp->def.op.name, "semijoin") == 0
            || strcmp(exp->def.op.name, "union") == 0
            || strcmp(exp->def.op.name, "intersect") == 0
            || strcmp(exp->def.op.name, "minus") == 0) {
        return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 2
                && RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0))
                && RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 1)));
    }
    if (strcmp(exp->def.op.name, "extend") == 0) {
        RDB_expression *argexp;

        if (RDB_expr_list_length(&exp->def.op.args) == 0
                || !RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0)))
            return RDB_FALSE;
        argexp = exp->def.op.args.firstp->nextp;
        while (argexp != NULL) {
            RDB_object *objp;
            if (!scalar_sql_convertible(argexp))
                return RDB_FALSE;
            argexp = argexp->nextp;
            if (argexp == NULL)
                return RDB_FALSE;
            objp = RDB_expr_obj(argexp);
            if (objp == NULL || RDB_obj_type(objp) != &RDB_STRING)
                return RDB_FALSE;
            argexp = argexp->nextp;
        }
        return RDB_TRUE;
    }
    return RDB_FALSE;
}

static int
expr_to_sql(RDB_object *, RDB_expression *, RDB_exec_context *);

static int
append_sub_sql(RDB_object *sql, const char *te, RDB_exec_context *ecp)
{
    static unsigned int aliasno = 0;
    char aliasbuf[18];
    /*
     * Append a table expression, putting it into parentheses if it's
     * already a SELECT
     */
    if (strstr(te, "SELECT ") == te) {
        if (RDB_append_char(sql, '(', ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        if (RDB_append_string(sql, te, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        sprintf(aliasbuf, ") AS p%u", aliasno++);
        if (RDB_append_string(sql, aliasbuf, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    } else {
        if (RDB_append_string(sql, te, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
append_sub_expr(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object se;

    RDB_init_obj(&se);
    if (expr_to_sql(&se, exp, ecp) != RDB_OK)
        goto error;
    if (append_sub_sql(sql, RDB_obj_string(&se), ecp) != RDB_OK)
        goto error;
    RDB_destroy_obj(&se, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&se, ecp);
    return RDB_ERROR;
}

static int
project_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    int i;
    int argcount = (int) RDB_expr_list_length(&exp->def.op.args);

    if (RDB_string_to_obj(sql, "SELECT ", ecp) != RDB_OK)
        return RDB_ERROR;
    for (i = 1; i < argcount; i++) {
        RDB_object *attrobjp = RDB_expr_obj(RDB_expr_list_get(&exp->def.op.args, i));
        if (attrobjp == NULL || RDB_obj_type(attrobjp) != &RDB_STRING) {
            RDB_raise_invalid_argument("invalid project argument", ecp);
            return RDB_ERROR;
        }
        if (RDB_append_string(sql, "d_", ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(sql, RDB_obj_string(attrobjp), ecp) != RDB_OK)
            return RDB_ERROR;
        if (i < argcount - 1) {
            if (RDB_append_char(sql, ',', ecp) != RDB_OK)
                return RDB_ERROR;
        }
    }
    if (RDB_append_string(sql, " FROM ", ecp) != RDB_OK)
        return RDB_ERROR;
    if (append_sub_expr(sql, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static char *
find_renaming(RDB_expr_list *explistp, const char *attrname)
{
    RDB_expression *exp = explistp->firstp->nextp;
    while (exp != NULL) {
        RDB_object *fromobjp, *toobjp;
        RDB_expression *toexp = exp->nextp;
        if (toexp == NULL) {
            return NULL;
        }
        fromobjp = RDB_expr_obj(exp);
        toobjp = RDB_expr_obj(toexp);
        if (fromobjp != NULL && toobjp != NULL
                && RDB_obj_type(fromobjp) == &RDB_STRING
                && RDB_obj_type(toobjp) == &RDB_STRING
                && strcmp(RDB_obj_string(fromobjp), attrname) == 0) {
            return RDB_obj_string(toobjp);
        }
        exp = toexp->nextp;
    }
    return NULL;
}

static int
rename_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    int i;
    RDB_attr *attrs;
    int attrc;
    RDB_type *typ = RDB_expr_type(exp->def.op.args.firstp, NULL, NULL, NULL, ecp, NULL);
    if (typ == NULL) {
        RDB_raise_invalid_argument("missing type", ecp);
        return RDB_ERROR;
    }

    attrs = RDB_type_attrs(typ, &attrc);
    if (attrs == NULL) {
        if (typ == NULL) {
            RDB_raise_invalid_argument("invalid 1st argument to rename", ecp);
            return RDB_ERROR;
        }
    }

    if (RDB_string_to_obj(sql, "SELECT ", ecp) != RDB_OK)
        return RDB_ERROR;
    for (i = 0; i < attrc; i++) {
        char *toname;

        if (RDB_append_string(sql, "d_", ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(sql, attrs[i].name, ecp) != RDB_OK)
            return RDB_ERROR;
        toname = find_renaming(&exp->def.op.args, attrs[i].name);
        if (toname != NULL) {
            if (RDB_append_string(sql, " AS d_", ecp) != RDB_OK)
                return RDB_ERROR;
            if (RDB_append_string(sql, toname, ecp) != RDB_OK)
                return RDB_ERROR;
        }
        if (i < attrc - 1) {
            if (RDB_append_char(sql, ',', ecp) != RDB_OK)
                return RDB_ERROR;
        }
    }
    if (RDB_append_string(sql, " FROM ", ecp) != RDB_OK)
        return RDB_ERROR;
    if (append_sub_expr(sql, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
add_select_attrs(RDB_object *sql, RDB_expression *exp,
        RDB_exec_context *ecp)
{
    RDB_attr *attrs;
    int attrc;
    int i;
    RDB_type *typ = RDB_expr_type(exp, NULL, NULL, NULL, ecp, NULL);
    if (typ == NULL) {
        return RDB_ERROR;
    }
    attrs = RDB_type_attrs(typ, &attrc);
    if (attrs == NULL) {
        if (typ == NULL) {
            RDB_raise_invalid_argument("invalid 1st argument to rename", ecp);
            return RDB_ERROR;
        }
    }
    for (i = 0; i < attrc; i++) {
        if (RDB_append_string(sql, "d_", ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(sql, attrs[i].name, ecp) != RDB_OK)
            return RDB_ERROR;
        if (i < attrc - 1) {
            if (RDB_append_char(sql, ',', ecp) != RDB_OK)
                return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
extend_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_expression *argexp;
    RDB_object e;

    if (RDB_string_to_obj(sql, "SELECT ", ecp) != RDB_OK)
        return RDB_ERROR;
    if (add_select_attrs(sql, exp->def.op.args.firstp, ecp) != RDB_OK)
        return RDB_ERROR;
    argexp = exp->def.op.args.firstp->nextp;
    RDB_init_obj(&e);
    while (argexp != NULL) {
        if (RDB_append_char(sql, ',', ecp) != RDB_OK)
            return RDB_ERROR;

        if (expr_to_sql(&e, argexp, ecp) != RDB_OK) {
            RDB_destroy_obj(&e, ecp);
            return RDB_ERROR;
        }
        if (RDB_append_string(sql, RDB_obj_string(&e), ecp) != RDB_OK) {
            RDB_destroy_obj(&e, ecp);
            return RDB_ERROR;
        }

        if (RDB_append_string(sql, " AS d_", ecp) != RDB_OK)
            return RDB_ERROR;

        argexp = argexp->nextp;
        if (RDB_append_string(sql, RDB_obj_string(RDB_expr_obj(argexp)), ecp) != RDB_OK)
            return RDB_ERROR;

        argexp = argexp->nextp;
    }
    RDB_destroy_obj(&e, ecp);

    if (RDB_append_string(sql, " FROM ", ecp) != RDB_OK)
        return RDB_ERROR;
    if (append_sub_expr(sql, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
where_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object e;

    if (RDB_string_to_obj(sql, "SELECT * FROM ", ecp) != RDB_OK)
        return RDB_ERROR;
    if (append_sub_expr(sql, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(sql, " WHERE ", ecp) != RDB_OK)
        return RDB_ERROR;

    RDB_init_obj(&e);
    if (expr_to_sql(&e, RDB_expr_list_get(&exp->def.op.args, 1), ecp) != RDB_OK) {
        RDB_destroy_obj(&e, ecp);
        return RDB_ERROR;
    }
    if (RDB_append_string(sql, RDB_obj_string(&e), ecp) != RDB_OK) {
        RDB_destroy_obj(&e, ecp);
        return RDB_ERROR;
    }
    RDB_destroy_obj(&e, ecp);
    return RDB_OK;
}

static int
join_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    if (RDB_string_to_obj(sql, "SELECT ", ecp) != RDB_OK)
        return RDB_ERROR;
    if (strcmp(exp->def.op.name, "semijoin") == 0) {
        RDB_attr *attrs;
        int attrc;
        int i;
        RDB_type *typ = RDB_expr_type(exp->def.op.args.firstp, NULL, NULL, NULL,
                ecp, NULL);
        if (typ == NULL) {
            return RDB_ERROR;
        }
        attrs = RDB_type_attrs(typ, &attrc);
        if (attrs == NULL) {
            RDB_raise_internal("Unable to get relation attributes", ecp);
            return RDB_ERROR;
        }
        for (i = 0; i < attrc; i++) {
            if (RDB_append_string(sql, "d_", ecp) != RDB_OK)
                return RDB_ERROR;
            if (RDB_append_string(sql, attrs[i].name, ecp) != RDB_OK)
                return RDB_ERROR;
            if (i < attrc - 1) {
                if (RDB_append_char(sql, ',', ecp) != RDB_OK)
                    return RDB_ERROR;
            }
        }
    } else {
        if (RDB_append_char(sql, '*', ecp) != RDB_OK)
            return RDB_ERROR;
    }
    if (RDB_append_string(sql, " FROM ", ecp) != RDB_OK)
        return RDB_ERROR;
    if (append_sub_expr(sql, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(sql, " NATURAL JOIN ", ecp) != RDB_OK)
        return RDB_ERROR;
    return append_sub_expr(sql, RDB_expr_list_get(&exp->def.op.args, 1), ecp);
}

static int
combine_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object se;

    RDB_init_obj(&se);
    if (expr_to_sql(&se, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK)
        goto error;
    if (RDB_string_to_obj(sql, "", ecp) != RDB_OK)
        goto error;
    if (strstr(RDB_obj_string(&se), "SELECT ") != RDB_obj_string(&se)) {
        if (RDB_append_string(sql, "SELECT ", ecp) != RDB_OK)
            goto error;
        if (add_select_attrs(sql, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(sql, " FROM ", ecp) != RDB_OK)
            goto error;
    }
    if (RDB_append_string(sql, RDB_obj_string(&se), ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(sql, ' ', ecp) != RDB_OK)
        goto error;
    if (strcmp(exp->def.op.name, "minus") == 0) {
        if (RDB_append_string(sql, "EXCEPT", ecp) != RDB_OK)
            goto error;
    } else {
        if (RDB_append_string(sql, exp->def.op.name, ecp) != RDB_OK)
            goto error;
    }
    if (RDB_append_char(sql, ' ', ecp) != RDB_OK)
        goto error;
    if (expr_to_sql(&se, RDB_expr_list_get(&exp->def.op.args, 1), ecp) != RDB_OK)
        goto error;
    if (strstr(RDB_obj_string(&se), "SELECT ") != RDB_obj_string(&se)) {
        if (RDB_append_string(sql, "SELECT ", ecp) != RDB_OK)
            goto error;
        if (add_select_attrs(sql, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(sql, " FROM ", ecp) != RDB_OK)
            goto error;
    }
    if (RDB_append_string(sql, RDB_obj_string(&se), ecp) != RDB_OK)
        goto error;
    RDB_destroy_obj(&se, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&se, ecp);
    return RDB_ERROR;
}

static int
infix_binop_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object arg;

    RDB_init_obj(&arg);
    if (RDB_string_to_obj(sql, "(", ecp) != RDB_OK)
        goto error;
    if (expr_to_sql(&arg, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, RDB_obj_string(&arg), ecp) != RDB_OK)
        goto error;

    if (RDB_append_char(sql, ' ', ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, exp->def.op.name, ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(sql, ' ', ecp) != RDB_OK)
        goto error;

    if (expr_to_sql(&arg, RDB_expr_list_get(&exp->def.op.args, 1), ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, RDB_obj_string(&arg), ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(sql, ')', ecp) != RDB_OK)
        goto error;

    RDB_destroy_obj(&arg, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&arg, ecp);
    return RDB_ERROR;
}

static int
unop_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object arg;

    RDB_init_obj(&arg);
    if (RDB_string_to_obj(sql, "(", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, exp->def.op.name, ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(sql, ' ', ecp) != RDB_OK)
        goto error;
    if (expr_to_sql(&arg, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, RDB_obj_string(&arg), ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(sql, ')', ecp) != RDB_OK)
        goto error;

    RDB_destroy_obj(&arg, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&arg, ecp);
    return RDB_ERROR;
}

static int
substr_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object arg;
    RDB_expression *argexp;

    RDB_init_obj(&arg);
    if (RDB_string_to_obj(sql, "substring(", ecp) != RDB_OK)
        goto error;
    argexp = exp->def.op.args.firstp;
    if (expr_to_sql(&arg, argexp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, RDB_obj_string(&arg), ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, " from 1+", ecp) != RDB_OK)
        goto error;
    argexp = argexp->nextp;
    if (expr_to_sql(&arg, argexp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, RDB_obj_string(&arg), ecp) != RDB_OK)
        goto error;
    argexp = argexp->nextp;
    if (RDB_append_string(sql, " for ", ecp) != RDB_OK)
        goto error;
    if (expr_to_sql(&arg, argexp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, RDB_obj_string(&arg), ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(sql, ')', ecp) != RDB_OK)
        goto error;

    RDB_destroy_obj(&arg, ecp);
    return RDB_OK;

    error:
    RDB_destroy_obj(&arg, ecp);
    return RDB_ERROR;
}

static int
op_inv_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object arg;
    RDB_expression *argexp;

    RDB_init_obj(&arg);
    if (strcmp(exp->def.op.name, "strlen") == 0) {
        if (RDB_string_to_obj(sql, "char_length", ecp) != RDB_OK)
            goto error;
    } else if (strcmp(exp->def.op.name, "strlen_b") == 0) {
        if (RDB_string_to_obj(sql, "octet_length", ecp) != RDB_OK)
            goto error;
     } else {
        if (RDB_string_to_obj(sql, exp->def.op.name, ecp) != RDB_OK)
            goto error;
    }
    if (RDB_append_string(sql, "(", ecp) != RDB_OK)
        goto error;
    argexp = exp->def.op.args.firstp;
    while (argexp != NULL) {
        if (expr_to_sql(&arg, argexp, ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(sql, RDB_obj_string(&arg), ecp) != RDB_OK)
            goto error;
        if (argexp->nextp != NULL) {
            if (RDB_append_char(sql, ',', ecp) != RDB_OK)
                goto error;
        }
        argexp = argexp->nextp;
    }
    if (RDB_append_char(sql, ')', ecp) != RDB_OK)
        goto error;

    RDB_destroy_obj(&arg, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&arg, ecp);
    return RDB_ERROR;
}

static int
cast_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object arg;
    const char *type = exp->def.op.name + 8;

    RDB_init_obj(&arg);
    if (strcmp(type, "float") == 0) {
        type = "double precision";
    } else if (strcmp(type, "string") == 0) {
        type = "text";
    }

    if (RDB_string_to_obj(sql, "cast(", ecp) != RDB_OK)
        goto error;
    if (expr_to_sql(&arg, exp->def.op.args.firstp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, RDB_obj_string(&arg), ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, " AS ", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(sql, type, ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(sql, ')', ecp) != RDB_OK)
        goto error;

    RDB_destroy_obj(&arg, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&arg, ecp);
    return RDB_ERROR;
}

static int
obj_to_sql(RDB_object *sql, RDB_object *srcp, RDB_exec_context *ecp)
{
    RDB_object str;
    int is_string = RDB_obj_type(srcp) == &RDB_STRING;
    int is_float = RDB_obj_type(srcp) == &RDB_FLOAT;

    RDB_init_obj(&str);
    if (RDB_obj_to_string(&str, srcp, ecp) != RDB_OK)
        goto error;

    if (RDB_string_to_obj(sql, is_string ? "'" : "", ecp) != RDB_OK)
        return RDB_ERROR;
    if (is_float) {
        if (RDB_append_string(sql, "CAST (", ecp) != RDB_OK)
            return RDB_ERROR;
    }
    if (RDB_append_string(sql, RDB_obj_string(&str), ecp) != RDB_OK)
        return RDB_ERROR;
    if (is_string) {
        if (RDB_append_char(sql, '\'', ecp) != RDB_OK)
            return RDB_ERROR;
    }
    if (is_float) {
        if (RDB_append_string(sql, " AS DOUBLE PRECISION)", ecp) != RDB_OK)
            return RDB_ERROR;
    }
    RDB_destroy_obj(&str, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&str, ecp);
    return RDB_ERROR;
}

static int
expr_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    switch (exp->kind)
    {
    case RDB_EX_VAR:
        if (RDB_string_to_obj(sql, "d_", ecp) != RDB_OK)
            return RDB_ERROR;
        return RDB_append_string(sql, exp->def.varname, ecp);
    case RDB_EX_TBP:
        return RDB_string_to_obj(sql, RDB_table_name(exp->def.tbref.tbp), ecp);
    case RDB_EX_RO_OP:
        if (strcmp(exp->def.op.name, "project") == 0) {
            return project_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "rename") == 0) {
            return rename_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "where") == 0) {
            return where_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "join") == 0
                || strcmp(exp->def.op.name, "semijoin") == 0) {
            return join_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "union") == 0
                || strcmp(exp->def.op.name, "intersect") == 0
                || strcmp(exp->def.op.name, "minus") == 0) {
            return combine_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "extend") == 0) {
            return extend_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "=") == 0
                || strcmp(exp->def.op.name, "<>") == 0
                || strcmp(exp->def.op.name, ">") == 0
                || strcmp(exp->def.op.name, "<") == 0
                || strcmp(exp->def.op.name, ">=") == 0
                || strcmp(exp->def.op.name, "<=") == 0
                || strcmp(exp->def.op.name, "and") == 0
                || strcmp(exp->def.op.name, "or") == 0
                || strcmp(exp->def.op.name, "+") == 0
                || strcmp(exp->def.op.name, "/") == 0
                || strcmp(exp->def.op.name, "%") == 0
                || strcmp(exp->def.op.name, "*") == 0
                || strcmp(exp->def.op.name, "||") == 0) {
            return infix_binop_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "not") == 0) {
            return unop_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "-") == 0) {
            RDB_int len = RDB_expr_list_length(&exp->def.op.args);
            if (len == 1)
                return unop_to_sql(sql, exp, ecp);
            if (len == 2)
                return infix_binop_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "abs") == 0
                || strcmp(exp->def.op.name, "sqrt") == 0
                || strcmp(exp->def.op.name, "sin") == 0
                || strcmp(exp->def.op.name, "cos") == 0
                || strcmp(exp->def.op.name, "atan") == 0
                || strcmp(exp->def.op.name, "atan2") == 0
                || strcmp(exp->def.op.name, "log") == 0
                || strcmp(exp->def.op.name, "ln") == 0
                || strcmp(exp->def.op.name, "power") == 0
                || strcmp(exp->def.op.name, "exp") == 0
                || strcmp(exp->def.op.name, "strlen") == 0) {
            return op_inv_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "cast_as_integer") == 0
                || strcmp(exp->def.op.name, "cast_as_float") == 0
                || strcmp(exp->def.op.name, "cast_as_string") == 0) {
            return cast_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "substr") == 0) {
            return substr_to_sql(sql, exp, ecp);
        }
        RDB_raise_invalid_argument(exp->def.op.name, ecp);
        return RDB_ERROR;
    case RDB_EX_OBJ:
        return obj_to_sql(sql, RDB_expr_obj(exp), ecp);
    default: ;
    }
    RDB_raise_invalid_argument("", ecp);
    return RDB_ERROR;
}

int
RDB_expr_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object te;
    RDB_init_obj(&te);

    if (expr_to_sql(&te, exp, ecp) != RDB_OK)
        goto error;

    /*
     * If the generated SQL is already a complete SELECT ... FROM ...,
     * copy it to the result, otherwise prepend 'SELECT * FROM '.
     */
    if (strstr(RDB_obj_string(&te), "SELECT ") == RDB_obj_string(&te)) {
        /* Add DISTINCT */
        if (RDB_string_to_obj(sql, "SELECT DISTINCT ", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(sql, RDB_obj_string(&te) + 7, ecp) != RDB_OK)
            goto error;
    } else {
        if (RDB_string_to_obj(sql, "SELECT DISTINCT * FROM ", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(sql, RDB_obj_string(&te), ecp) != RDB_OK)
            goto error;
    }

    RDB_destroy_obj(&te, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&te, ecp);
    return RDB_ERROR;
}
