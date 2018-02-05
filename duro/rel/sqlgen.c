/*
 * SQL generation functions
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "sqlgen.h"
#include "rdb.h"
#include <obj/objinternal.h>
#include <stdlib.h>
#include <string.h>

RDB_bool
RDB_sql_convertible(RDB_expression *exp) {
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
    return RDB_FALSE;
}

static int
expr_to_sql_te(RDB_object *, RDB_expression *, RDB_exec_context *);

static int
append_te(RDB_object *sql, const char *te, RDB_exec_context *ecp)
{
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
        if (RDB_append_char(sql, '(', ecp) != RDB_OK) {
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
project_to_sql(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    int i;
    RDB_object te;
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

    RDB_init_obj(&te);
    if (expr_to_sql_te(&te, RDB_expr_list_get(&exp->def.op.args, 0), ecp) != RDB_OK) {
        RDB_destroy_obj(&te, ecp);
        return RDB_ERROR;
    }
    if (append_te(sql, RDB_obj_string(&te), ecp) != RDB_OK) {
        RDB_destroy_obj(&te, ecp);
        return RDB_ERROR;
    }
    RDB_destroy_obj(&te, ecp);
    return RDB_OK;
}

static int
expr_to_sql_te(RDB_object *sql, RDB_expression *exp, RDB_exec_context *ecp)
{
    switch (exp->kind)
    {
    case RDB_EX_VAR:
        return RDB_string_to_obj(sql, exp->def.varname, ecp);
    case RDB_EX_TBP:
        return RDB_string_to_obj(sql, RDB_table_name(exp->def.tbref.tbp), ecp);
    case RDB_EX_RO_OP:
        if (strcmp(exp->def.op.name, "project") == 0) {
            return project_to_sql(sql, exp, ecp);
        }
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

    if (expr_to_sql_te(&te, exp, ecp) != RDB_OK)
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
