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
                )
        {
            return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 1
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
        RDB_expression *tbexp = RDB_expr_list_get(&exp->def.op.args, 0);
        return tbexp != NULL && RDB_sql_convertible(tbexp)
                && RDB_expr_list_length(&exp->def.op.args) >= 2;
    }
    if (strcmp(exp->def.op.name, "where") == 0) {
        return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 2
                && RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0))
                && scalar_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 1)));
    }
    if (strcmp(exp->def.op.name, "join") == 0) {
        return (RDB_bool) (RDB_expr_list_length(&exp->def.op.args) == 2
                && RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 0))
                && RDB_sql_convertible(RDB_expr_list_get(&exp->def.op.args, 1)));
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
    if (RDB_string_to_obj(sql, "SELECT * FROM ", ecp) != RDB_OK)
        return RDB_ERROR;
    if (append_sub_expr(sql, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(sql, " NATURAL JOIN ", ecp) != RDB_OK)
        return RDB_ERROR;
    return append_sub_expr(sql, RDB_expr_list_get(&exp->def.op.args, 1), ecp);
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
op_inv_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_object arg;
    RDB_expression *argexp;

    RDB_init_obj(&arg);
    if (strcmp(exp->def.op.name, "strlen") == 0) {
        if (RDB_string_to_obj(sql, "char_length", ecp) != RDB_OK)
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
obj_to_sql(RDB_object *sql, RDB_object *srcp, RDB_exec_context *ecp)
{
    RDB_object str;
    int is_string = RDB_obj_type(srcp) == &RDB_STRING;

    RDB_init_obj(&str);
    if (RDB_obj_to_string(&str, srcp, ecp) != RDB_OK)
        goto error;

    if (RDB_string_to_obj(sql, is_string ? "'" : "", ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(sql, RDB_obj_string(&str), ecp) != RDB_OK)
        return RDB_ERROR;
    if (is_string) {
        if (RDB_append_char(sql, '\'', ecp) != RDB_OK)
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
        if (strcmp(exp->def.op.name, "where") == 0) {
            return where_to_sql(sql, exp, ecp);
        }
        if (strcmp(exp->def.op.name, "join") == 0) {
            return join_to_sql(sql, exp, ecp);
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
        RDB_raise_invalid_argument(exp->def.op.name, ecp);
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
