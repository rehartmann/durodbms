/*
 * $Id$
 *
 * Copyright (C) 2004-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "transform.h"
#include "internal.h"
#include "catalog.h"
#include <gen/strfns.h>

#include <string.h>

RDB_bool
RDB_expr_is_const(const RDB_expression *exp)
{
    int i;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            return RDB_TRUE;
        case RDB_EX_RO_OP:
            for (i = 0; i < exp->var.op.argc; i++) {
                if (!RDB_expr_is_const(exp->var.op.argv[i]))
                    return RDB_FALSE;
            }
            return RDB_TRUE;
        case RDB_EX_GET_COMP:
            return RDB_expr_is_const(exp->var.op.argv[0]);
        default: ;
    }
    return RDB_FALSE;
}

static RDB_type *
wrap_type(const RDB_expression *exp, RDB_type **argtv,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int wrapc;
    RDB_wrapping *wrapv;
    RDB_type *typ = NULL;

    if (exp->var.op.argc < 1 || exp->var.op.argc %2 != 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    wrapc = (exp->var.op.argc - 1) / 2;
    if (wrapc > 0) {
        wrapv = malloc(sizeof(RDB_wrapping) * wrapc);
        if (wrapv == NULL) {
            RDB_raise_no_memory(ecp);
            return NULL;
        }
    }
    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrv = NULL;
    }

    for (i = 0; i < wrapc; i++) {
        RDB_object *objp;

        wrapv[i].attrc =  RDB_array_length(
                &exp->var.op.argv[i * 2 + 1]->var.obj, ecp);
        wrapv[i].attrv = malloc(sizeof (char *) * wrapv[i].attrc);
        for (j = 0; j < wrapv[i].attrc; j++) {
            objp = RDB_array_get(&exp->var.op.argv[i * 2 + 1]->var.obj,
                    (RDB_int) j, ecp);
            if (objp == NULL) {
                goto cleanup;
            }
            wrapv[i].attrv[j] = RDB_obj_string(objp);
        }        
        wrapv[i].attrname = RDB_obj_string(
                &exp->var.op.argv[i * 2 + 2]->var.obj);
    }

    if (argtv[0] == NULL) {
        RDB_raise_invalid_argument("Invalid WRAP argument", ecp);
        goto cleanup;
    }

    if (argtv[0]->kind == RDB_TP_RELATION) {
        typ = RDB_wrap_relation_type(argtv[0], wrapc, wrapv, ecp);
    } else if (argtv[0]->kind == RDB_TP_TUPLE) {
        typ = RDB_wrap_tuple_type(argtv[0], wrapc, wrapv, ecp);
    } else {
        RDB_raise_type_mismatch("First argument to WRAP must be non-scalar",
                ecp);
    }        

cleanup:
    for (i = 0; i < wrapc; i++) {
        free(wrapv[i].attrv);
    }
    if (wrapc > 0)
        free(wrapv);
    return typ;
}

static RDB_expression *
expr_resolve_attrs(const RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp)
{
    RDB_object *objp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            return RDB_tuple_attr(
                    expr_resolve_attrs(exp->var.op.argv[0], tplp, ecp),
                            exp->var.op.name, ecp);
        case RDB_EX_GET_COMP:
            return RDB_expr_comp(
                    expr_resolve_attrs(exp->var.op.argv[0], tplp, ecp),
                            exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            int i;
            RDB_expression *newexp = RDB_ro_op(exp->var.op.name,
                    exp->var.op.argc, ecp);
            if (newexp == NULL)
                return NULL;

            for (i = 0; i < exp->var.op.argc; i++) {
                RDB_expression *argexp = expr_resolve_attrs(
                        exp->var.op.argv[i], tplp, ecp);
                if (argexp == NULL) {
                    RDB_drop_expr(newexp, ecp);
                    return NULL;
                }
                RDB_add_arg(newexp, argexp);
            }
            return newexp;
        }
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj, ecp);
        case RDB_EX_TBP:
            return RDB_table_ref(exp->var.tbref.tbp, ecp);
        case RDB_EX_VAR:
            objp = RDB_tuple_get(tplp, exp->var.varname);
            if (objp != NULL)
                return RDB_obj_to_expr(objp, ecp);
            return RDB_var_ref(exp->var.varname, ecp);
    }
    abort();
}

static RDB_type *
where_type(const RDB_expression *exp, const RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *condtyp;
    RDB_type *jtyp;
    RDB_type *reltyp;

    if (exp->var.op.argc != 2) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    reltyp = RDB_expr_type(exp->var.op.argv[0], tpltyp, ecp, txp);
    if (reltyp == NULL)
        return NULL;

    if (reltyp->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("WHERE requires relation argument", ecp);
        return NULL;
    }

    if (tpltyp != NULL) {
        jtyp = RDB_join_tuple_types(tpltyp, reltyp->var.basetyp, ecp);
        if (jtyp == NULL) {
            return NULL;
        }
    }

    condtyp = RDB_expr_type(exp->var.op.argv[1],
            tpltyp == NULL ? reltyp->var.basetyp : jtyp, ecp, txp);
    if (tpltyp != NULL)
        RDB_drop_type(jtyp, ecp, NULL);
    if (condtyp == NULL) {
        return NULL;
    }

    if (condtyp != &RDB_BOOLEAN) {
        RDB_raise_type_mismatch("WHERE requires BOOLEAN argument", ecp);
        return NULL;
    }
    return RDB_dup_nonscalar_type(reltyp, ecp);
}

static RDB_type *
extend_type(const RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    RDB_attr *attrv;
    RDB_type *tpltyp, *typ;
    int attrc;
    RDB_type *arg1typ;

    if (exp->var.op.argc < 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    attrc = (exp->var.op.argc - 1) / 2;
    arg1typ = RDB_expr_type(exp->var.op.argv[0], NULL, ecp, txp);
    if (arg1typ == NULL)
        return NULL;

    if (arg1typ->kind != RDB_TP_RELATION && arg1typ->kind != RDB_TP_TUPLE) {
        RDB_raise_invalid_argument(
                "Invalid first argument to EXTEND", ecp);
        return NULL;
    }

    if (arg1typ->kind == RDB_TP_RELATION)
        tpltyp = arg1typ->var.basetyp;
    else
        tpltyp = arg1typ;

    attrv = malloc(sizeof (RDB_attr) * attrc);
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        attrv[i].typ = RDB_expr_type(exp->var.op.argv[1 + i * 2], tpltyp,
                ecp, txp);
        if (attrv[i].typ == NULL) {
            int j;

            free(attrv);
            for (j = 0; i < j; j++) {
                if (!RDB_type_is_scalar(attrv[j].typ)) {
                    RDB_drop_type(attrv[j].typ, ecp, NULL);
                }
            }
            return NULL;
        }
        if (exp->var.op.argv[2 + i * 2]->kind != RDB_EX_OBJ ||
                exp->var.op.argv[2 + i * 2]->var.obj.typ != &RDB_STRING) {
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        attrv[i].name = RDB_obj_string(&exp->var.op.argv[2 + i * 2]->var.obj);
    }
    switch (arg1typ->kind) {
        case RDB_TP_RELATION:
            typ = RDB_extend_relation_type(arg1typ, attrc, attrv, ecp);
            break;
        case RDB_TP_TUPLE:
            typ = RDB_extend_tuple_type(arg1typ, attrc, attrv, ecp);
            break;
        default:
            RDB_raise_type_mismatch(
                    "EXTEND requires tuple or relation argument", ecp);
            typ = NULL;
    }
    free(attrv);
    return typ;
}

static RDB_bool
expr_is_string(const RDB_expression *exp)
{
    return exp->kind == RDB_EX_OBJ && exp->var.obj.typ == &RDB_STRING;
}

static RDB_type *
project_type(const RDB_expression *exp, RDB_type *argtv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    char **attrv;
    RDB_type *typ;

    if (exp->var.op.argc == 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    if (argtv[0] == NULL) {
        RDB_raise_invalid_argument("Invalid PROJECT argument", ecp);
        return NULL;
    }

    for (i = 1; i < exp->var.op.argc; i++) {
        if (!expr_is_string(exp->var.op.argv[i])) {
            RDB_raise_type_mismatch("PROJECT requires STRING arguments",
                    ecp);
            return NULL;
        }
    }
    attrv = malloc(sizeof (char *) * (exp->var.op.argc - 1));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 1; i < exp->var.op.argc; i++) {
        attrv[i - 1] = RDB_obj_string(&exp->var.op.argv[i]->var.obj);
    }
    if (argtv[0]->kind == RDB_TP_RELATION) {
        typ = RDB_project_relation_type(argtv[0], exp->var.op.argc - 1, attrv,
               ecp);
    } else if (argtv[0]->kind == RDB_TP_TUPLE) {
        typ = RDB_project_tuple_type(argtv[0], exp->var.op.argc - 1, attrv,
               ecp);
    } else {
        RDB_raise_invalid_argument("invalid PROJECT argument", ecp);
        typ = NULL;
    }
    free(attrv);
    return typ;
}

static RDB_type *
unwrap_type(const RDB_expression *exp, RDB_type *argtv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    char **attrv;
    int attrc;
    int i;
    RDB_type *typ;

    if (exp->var.op.argc == 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    if (argtv[0] == NULL) {
        RDB_raise_invalid_argument("Invalid UNWRAP argument", ecp);
        return NULL;
    }
    
    attrc = exp->var.op.argc - 1;

    for (i = 1; i < exp->var.op.argc; i++) {
        if (!expr_is_string(exp->var.op.argv[i])) {
            RDB_raise_type_mismatch(
                    "UNWRAP argument must be STRING", ecp);
            return NULL;
        }
    }

    attrv = malloc(attrc * sizeof (char *));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        attrv[i] = RDB_obj_string(&exp->var.op.argv[i + 1]->var.obj);
    }

    if (argtv[0]->kind == RDB_TP_RELATION) {
        typ = RDB_unwrap_relation_type(argtv[0], attrc, attrv, ecp);
    } else if (argtv[0]->kind == RDB_TP_TUPLE) {
        typ = RDB_unwrap_tuple_type(argtv[0], attrc, attrv, ecp);
    } else {
        RDB_raise_invalid_argument("invalid UNWRAP argument", ecp);
        typ = NULL;
    }

    free(attrv);
    return typ;
}

static RDB_type *
group_type(const RDB_expression *exp, RDB_type *argtv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    char **attrv;
    int i;
    RDB_type *typ;

    if (exp->var.op.argc < 2) {
        RDB_raise_invalid_argument("invalid GROUP argument", ecp);
        return NULL;
    }

    if (argtv[0] == NULL || argtv[0]->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("Invalid GROUP argument", ecp);
        return NULL;
    }

    for (i = 1; i < exp->var.op.argc; i++) {
        if (!expr_is_string(exp->var.op.argv[i])) {
            RDB_raise_type_mismatch("STRING attribute required", ecp);
            return NULL;
        }
    }

    if (exp->var.op.argc > 2) {
        attrv = malloc(sizeof (char *) * (exp->var.op.argc - 2));
        if (attrv == NULL) {
            RDB_raise_no_memory(ecp);
            return NULL;
        }
    } else {
        attrv = NULL;
    }

    for (i = 1; i < exp->var.op.argc - 1; i++) {
        attrv[i - 1] = RDB_obj_string(&exp->var.op.argv[i]->var.obj);
    }
    typ = RDB_group_type(argtv[0], exp->var.op.argc - 2, attrv,
            RDB_obj_string(&exp->var.op.argv[exp->var.op.argc - 1]->var.obj),
            ecp);
    free(attrv);
    return typ;
}

static RDB_type *
rename_type(const RDB_expression *exp, RDB_type **argtv,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int renc;
    int i;
    RDB_type *typ;
    RDB_renaming *renv = NULL;

    if (exp->var.op.argc == 0 || exp->var.op.argc % 2 != 1) {
        RDB_raise_invalid_argument("invalid number of RENAME arguments", ecp);
        return NULL;
    }

    renc = (exp->var.op.argc - 1) / 2;
    if (renc > 0) {
        renv = malloc(sizeof (RDB_renaming) * renc);
        if (renv == NULL) {
            RDB_raise_no_memory(ecp);
            return NULL;
        }
    }

    for (i = 0; i < renc; i++) {
        if (exp->var.op.argv[1 + i * 2]->kind != RDB_EX_OBJ ||
                exp->var.op.argv[1 + i * 2]->var.obj.typ != &RDB_STRING) {
            free(renv);
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        if (exp->var.op.argv[2 + i * 2]->kind != RDB_EX_OBJ ||
                exp->var.op.argv[2 + i * 2]->var.obj.typ != &RDB_STRING) {
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        renv[i].from = RDB_obj_string(&exp->var.op.argv[1 + i * 2]->var.obj);
        renv[i].to = RDB_obj_string(&exp->var.op.argv[2 + i * 2]->var.obj);
    }

    switch (argtv[0]->kind) {
        case RDB_TP_RELATION:
            typ = RDB_rename_relation_type(argtv[0], renc, renv, ecp);
            break;
        case RDB_TP_TUPLE:
            typ = RDB_rename_tuple_type(argtv[0], renc, renv, ecp);
            break;
        default:
            RDB_raise_invalid_argument(
                    "relation or tuple argument required", ecp);
            typ = NULL;
    }
    free(renv);
    return typ;
}

static RDB_type *
expr_op_type(RDB_expression *exp, const RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_type *typ;
    RDB_ro_op_desc *op;
    RDB_type **argtv = NULL;
    int argc = exp->var.op.argc;

    /*
     * WHERE, EXTEND and SUMMARIZE require special treatment
     */
    if (strcmp(exp->var.op.name, "WHERE") == 0) {
        return where_type(exp, tpltyp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "EXTEND") == 0) {
        return extend_type(exp, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "SUMMARIZE") == 0) {
        return RDB_summarize_type(argc, exp->var.op.argv, 0, NULL, ecp, txp);
    }

    if (strcmp(exp->var.op.name, "REMOVE") == 0) {
        if (_RDB_remove_to_project(exp, ecp, txp) != RDB_OK)
            goto error;
    }

    /*
     * Get argument types
     */
    argtv = malloc(sizeof (RDB_type *) * argc);
    if (argtv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < argc; i++) {
        /*
         * The expression may not have a type (e.g. if it's an array),
         * so RDB_expr_type can return NULL without raising an error.
         */
        argtv[i] = RDB_expr_type(exp->var.op.argv[i], tpltyp, ecp, txp);
        if (argtv[i] == NULL && RDB_get_err(ecp) != NULL)
            goto error;
    }

    /* Aggregate operators */
    if (strcmp(exp->var.op.name, "COUNT") == 0
            && argc == 1) {
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("WHERE requires relation argument", ecp);
            goto error;
        }
        typ = &RDB_INTEGER;
    } else if (strcmp(exp->var.op.name, "AVG") == 0) {
        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }
        if (exp->var.op.argv[1]->kind != RDB_EX_VAR) {
            RDB_raise_invalid_argument("invalid aggregate", ecp);
            goto error;
        }
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("aggregate requires relation argument",
                    ecp);
            goto error;
        }
        if (argtv[1] != &RDB_INTEGER && argtv[1] != &RDB_FLOAT
                && argtv[1] != &RDB_DOUBLE) {
            RDB_raise_type_mismatch("invalid attribute type", ecp);
            goto error;
        }
        typ = &RDB_DOUBLE;
    } else if (strcmp(exp->var.op.name, "SUM") == 0
            || strcmp(exp->var.op.name, "MAX") == 0
            || strcmp(exp->var.op.name, "MIN") == 0) {
        if (exp->var.op.argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }
        if (exp->var.op.argv[1]->kind != RDB_EX_VAR) {
            RDB_raise_invalid_argument("invalid aggregate", ecp);
            goto error;
        }
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("aggregate requires relation argument",
                    ecp);
            goto error;
        }
        if (argtv[1] != &RDB_INTEGER && argtv[1] != &RDB_FLOAT
                && argtv[1] != &RDB_DOUBLE) {
            RDB_raise_type_mismatch("invalid attribute type", ecp);
            goto error;
        }
        typ = argtv[1];
    } else if (strcmp(exp->var.op.name, "ANY") == 0
            || strcmp(exp->var.op.name, "ALL") == 0) {
        if (exp->var.op.argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }

        if (argtv[1] != &RDB_BOOLEAN) {
            RDB_raise_type_mismatch("invalid attribute type", ecp);
            goto error;
        }
        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->var.op.name, "RENAME") == 0) {
        typ = rename_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->var.op.name, "WRAP") == 0) {
        typ = wrap_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        typ = project_type(exp, argtv, ecp, txp);
    } else if (exp->var.op.argc == 2
            && (argtv[0] == NULL || !RDB_type_is_scalar(argtv[0]))
            && (argtv[1] == NULL || !RDB_type_is_scalar(argtv[1]))
            && (strcmp(exp->var.op.name, "=") == 0
                    || strcmp(exp->var.op.name, "<>") == 0)) {
        /*
         * Handle nonscalar comparison
         */
        if (argtv[0] != NULL && argtv[1] != NULL
                && !RDB_type_equals(argtv[0], argtv[1])) {
            RDB_raise_type_mismatch("", ecp);
            goto error;
        }
        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->var.op.name, "IF") == 0) {
        /*
         * Handle IF-THEN-ELSE
         */
        if (exp->var.op.argc != 3) {
            RDB_raise_invalid_argument("invalid number of arguments", ecp);
            goto error;
        }

        if (argtv[0] != &RDB_BOOLEAN || !RDB_type_equals(argtv[1], argtv[2])) {
            RDB_raise_type_mismatch("IF requires BOOLEAN arguments", ecp);
            goto error;
        }
        typ = RDB_dup_nonscalar_type(argtv[1], ecp);
    /*
     * Handle built-in scalar operators with relational arguments
     */
    } else if (strcmp(exp->var.op.name, "IS_EMPTY") == 0) {
        if (exp->var.op.argc != 1) {
            RDB_raise_invalid_argument("invalid number of arguments", ecp);
            goto error;
        }

        if (argtv[0] == NULL || argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("IS_EMPTY requires table argument", ecp);
            goto error;
        }

        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->var.op.name, "IN") == 0
                    || strcmp(exp->var.op.name, "SUBSET_OF") == 0) {
        if (exp->var.op.argc != 2) {
            RDB_raise_invalid_argument("invalid number of arguments", ecp);
            goto error;
        }

        if (argtv[1] == NULL || argtv[1]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("table argument required", ecp);
            goto error;
        }

        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->var.op.name, "UNWRAP") == 0) {
        typ = unwrap_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->var.op.name, "GROUP") == 0) {
        typ = group_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->var.op.name, "UNGROUP") == 0) {
        if (exp->var.op.argc != 2 || argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid UNGROUP", ecp);
            goto error;
        }        
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("UNGROUP requires table argument", ecp);
            goto error;
        }
        if (exp->var.op.argv[1]->kind != RDB_EX_OBJ
                || exp->var.op.argv[1]->var.obj.typ != &RDB_STRING) {
            RDB_raise_type_mismatch("invalid UNGROUP argument", ecp);
            goto error;
        }

        typ = RDB_ungroup_type(argtv[0],
                RDB_obj_string(&exp->var.op.argv[1]->var.obj), ecp);
    } else if (strcmp(exp->var.op.name, "JOIN") == 0) {
        if (exp->var.op.argc != 2 || argtv[0] == NULL
                || argtv[1] == NULL) {
            RDB_raise_invalid_argument("invalid JOIN", ecp);
            goto error;
        }        
            
        if (argtv[0]->kind != RDB_TP_RELATION
                    || argtv[1]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("table argument required", ecp);
            goto error;
        }
        typ = RDB_join_relation_types(argtv[0], argtv[1], ecp);
    } else if (strcmp(exp->var.op.name, "UNION") == 0
                || strcmp(exp->var.op.name, "MINUS") == 0
                || strcmp(exp->var.op.name, "SEMIMINUS") == 0
                || strcmp(exp->var.op.name, "INTERSECT") == 0
                || strcmp(exp->var.op.name, "SEMIJOIN") == 0) {
        if (exp->var.op.argc != 2 || argtv[0] == NULL
                || argtv[1] == NULL) {
            RDB_raise_invalid_argument("invalid relational operator invocation",
                    ecp);
            goto error;
        }        

        if (argtv[0]->kind != RDB_TP_RELATION
                    || argtv[1]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("table argument required", ecp);
            goto error;
        }

        typ = RDB_dup_nonscalar_type(argtv[0], ecp);
        if (typ == NULL)
            goto error;
        free(argtv);
        return typ;
    } else if (strcmp(exp->var.op.name, "DIVIDE") == 0) {
        if (exp->var.op.argc != 3 || argtv[0] == NULL
                || argtv[1] == NULL || argtv[2] == NULL) {
            RDB_raise_invalid_argument("invalid DIVIDE",
                    ecp);
            goto error;
        }        

        if (argtv[0]->kind != RDB_TP_RELATION
                    || argtv[1]->kind != RDB_TP_RELATION
                    || argtv[2]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("table argument required", ecp);
            goto error;
        }

        typ = RDB_dup_nonscalar_type(argtv[0], ecp);
        if (typ == NULL)
            goto error;
        free(argtv);
        return typ;
    } else if (strcmp(exp->var.op.name, "TO_TUPLE") == 0) {
        if (exp->var.op.argc != 1 || argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid TO_TUPLE",
                    ecp);
            goto error;
        }        

        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("table argument required", ecp);
            goto error;
        }

        typ = RDB_dup_nonscalar_type(argtv[0]->var.basetyp, ecp);
    } else {
        for (i = 0; i < exp->var.op.argc; i++) {
            if (argtv[i] == NULL) {
                RDB_raise_operator_not_found("", ecp);
                goto error;
            }
        }

        ret = _RDB_get_ro_op(exp->var.op.name, exp->var.op.argc,
            argtv, ecp, txp, &op);
        if (ret != RDB_OK)
            goto error;
        typ = RDB_dup_nonscalar_type(op->rtyp, ecp);
    }

    free(argtv);
    return typ;

error:
    free(argtv);
    return NULL;
}

/*
 * Returns the type of an expression.
 */
RDB_type *
RDB_expr_type(RDB_expression *exp, const RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_attr *attrp;
    RDB_type *typ;

    if (exp->typ != NULL)
        return exp->typ;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            /* Get type from RDB_object */
            typ = RDB_obj_type(&exp->var.obj);
            if (typ != NULL)
                return typ;

            /* No type available - generate type from tuple */
            if (exp->var.obj.kind == RDB_OB_TUPLE) {
                exp->typ = _RDB_tuple_type(&exp->var.obj, ecp);
            }
            return exp->typ;
        case RDB_EX_TBP:
            return RDB_obj_type(exp->var.tbref.tbp);
        case RDB_EX_VAR:
            if (tpltyp != NULL) {
                /* Get type from tuple type */
                attrp = _RDB_tuple_type_attr(tpltyp, exp->var.varname);
                if (attrp != NULL) {
                    exp->typ = RDB_dup_nonscalar_type(attrp->typ, ecp);
                    return exp->typ;
                }
            }
            if (txp != NULL) {
                RDB_object *tbp = RDB_get_table(exp->var.varname, ecp, txp);
                if (tbp != NULL) {
                    /* Transform into table ref */
                    free(exp->var.varname);
                    exp->kind = RDB_EX_TBP;
                    exp->var.tbref.tbp = tbp;
                    exp->var.tbref.indexp = NULL;
                    return RDB_obj_type(exp->var.tbref.tbp);
                }
            }
            RDB_raise_attribute_not_found(exp->var.varname, ecp);
            return NULL;
        case RDB_EX_TUPLE_ATTR:
            typ = RDB_expr_type(exp->var.op.argv[0], tpltyp, ecp, txp);
            if (typ == NULL)
                return NULL;
            typ = RDB_type_attr_type(typ, exp->var.op.name);
            if (typ == NULL) {
                RDB_raise_attribute_not_found(exp->var.varname, ecp);
                return NULL;
            }
            return typ;
        case RDB_EX_GET_COMP:
            typ = RDB_expr_type(exp->var.op.argv[0], tpltyp, ecp, txp);
            if (typ == NULL)
                return typ;
            attrp = _RDB_get_icomp(typ, exp->var.op.name);
            if (attrp == NULL) {
                RDB_raise_invalid_argument("component not found", ecp);
                return NULL;
            }
            return attrp->typ;
        case RDB_EX_RO_OP:
            exp->typ = expr_op_type(exp, tpltyp, ecp, txp);
            return exp->typ;
    }
    abort();
}

int
_RDB_check_expr_type(RDB_expression *exp, const RDB_type *tuptyp,
        const RDB_type *checktyp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ = RDB_expr_type(exp, tuptyp, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    if (!RDB_type_equals(typ, checktyp)) {
        RDB_raise_type_mismatch("", ecp);
        return RDB_ERROR;
    }        
    return RDB_OK;
}

int
_RDB_expr_equals(const RDB_expression *ex1p, const RDB_expression *ex2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    int i;

    if (ex1p->kind != ex2p->kind) {
        *resp = RDB_FALSE;
        return RDB_OK;
    }

    switch (ex1p->kind) {
        case RDB_EX_OBJ:
            return RDB_obj_equals(&ex1p->var.obj, &ex2p->var.obj, ecp, txp,
                    resp);
        case RDB_EX_TBP:
            if (ex1p->var.tbref.tbp->var.tb.is_persistent
                    || ex2p->var.tbref.tbp->var.tb.is_persistent) {
                *resp = (RDB_bool) (ex1p->var.tbref.tbp == ex2p->var.tbref.tbp);
                return RDB_OK;
            }
            return RDB_obj_equals(ex1p->var.tbref.tbp, ex2p->var.tbref.tbp, ecp, txp,
                    resp);
        case RDB_EX_VAR:
            *resp = (RDB_bool)
                    (strcmp (ex1p->var.varname, ex2p->var.varname) == 0);
            break;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            ret = _RDB_expr_equals(ex1p->var.op.argv[0], ex2p->var.op.argv[0],
                    ecp, txp, resp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            *resp = (RDB_bool)
                    (strcmp (ex1p->var.op.name, ex2p->var.op.name) == 0);
            break;
        case RDB_EX_RO_OP:
            if (ex1p->var.op.argc != ex2p->var.op.argc
                    || strcmp(ex1p->var.op.name, ex2p->var.op.name) != 0) {
                *resp = RDB_FALSE;
                return RDB_OK;
            }
            for (i = 0; i < ex1p->var.op.argc; i++) {
                ret = _RDB_expr_equals(ex1p->var.op.argv[i],
                        ex2p->var.op.argv[i], ecp, txp, resp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
                if (!*resp)
                    return RDB_OK;
            }
            *resp = RDB_TRUE;
            break;
    }
    return RDB_OK;
}

static RDB_expression *
new_expr(RDB_exec_context *ecp) {
    RDB_expression *exp = malloc(sizeof (RDB_expression));

    if (exp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    exp->typ = NULL;
	return exp;
}

RDB_expression *
RDB_bool_to_expr(RDB_bool v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_BOOLEAN;
    exp->var.obj.kind = RDB_OB_BOOL;
    exp->var.obj.var.bool_val = v;

    return exp;
}

RDB_expression *
RDB_int_to_expr(RDB_int v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_INTEGER;
    exp->var.obj.kind = RDB_OB_INT;
    exp->var.obj.var.int_val = v;

    return exp;
}

RDB_expression *
RDB_float_to_expr(RDB_float v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_FLOAT;
    exp->var.obj.kind = RDB_OB_FLOAT;
    exp->var.obj.var.float_val = v;

    return exp;
}

RDB_expression *
RDB_double_to_expr(RDB_double v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_DOUBLE;
    exp->var.obj.kind = RDB_OB_DOUBLE;
    exp->var.obj.var.double_val = v;

    return exp;
}

RDB_expression *
RDB_string_to_expr(const char *v, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);    
    if (exp == NULL) {
        return NULL;
    }
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_STRING;
    exp->var.obj.kind = RDB_OB_BIN;
    exp->var.obj.var.bin.datap = RDB_dup_str(v);
    if (exp->var.obj.var.bin.datap == NULL) {
        free(exp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    exp->var.obj.var.bin.len = strlen(v) + 1;

    return exp;
}

RDB_expression *
RDB_obj_to_expr(const RDB_object *objp, RDB_exec_context *ecp)
{
    int ret;
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_OBJ;
    RDB_init_obj(&exp->var.obj);
    if (objp != NULL) {
        ret = RDB_copy_obj(&exp->var.obj, objp, ecp);
        if (ret != RDB_OK) {
            free(exp);
            return NULL;
        }
    }
    return exp;
}

RDB_expression *
RDB_table_ref(RDB_object *tbp, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->kind = RDB_EX_TBP;
    exp->var.tbref.tbp = tbp;
    exp->var.tbref.indexp = NULL;
    return exp;
}

RDB_expression *
RDB_var_ref(const char *attrname, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->kind = RDB_EX_VAR;
    exp->var.varname = RDB_dup_str(attrname);
    if (exp->var.varname == NULL) {
        RDB_raise_no_memory(ecp);
        free(exp);
        return NULL;
    }
    
    return exp;
}

RDB_expression *
_RDB_create_unexpr(RDB_expression *arg, enum _RDB_expr_kind kind,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;

    if (arg == NULL)
        return NULL;

    exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->var.op.argv = malloc(sizeof (RDB_expression *));
    if (exp->var.op.argv == NULL) {
        RDB_raise_no_memory(ecp);
        free(exp);
        return NULL;
    }
        
    exp->kind = kind;
    exp->var.op.argv[0] = arg;

    return exp;
}

RDB_expression *
_RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2,
        enum _RDB_expr_kind kind, RDB_exec_context *ecp)
{
    RDB_expression *exp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->var.op.argv = malloc(sizeof (RDB_expression *) * 2);
    if (exp->var.op.argv == NULL) {
        RDB_raise_no_memory(ecp);
        free(exp);
        return NULL;
    }
        
    exp->kind = kind;
    exp->var.op.argv[0] = arg1;
    exp->var.op.argv[1] = arg2;

    return exp;
}

RDB_expression *
RDB_ro_op(const char *opname, int argc, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    int i;

    exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->kind = RDB_EX_RO_OP;
    
    exp->var.op.name = RDB_dup_str(opname);
    if (exp->var.op.name == NULL) {
        free(exp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    exp->var.op.argv = malloc(sizeof(RDB_expression *) * argc);
    if (exp->var.op.argv == NULL) {
        free(exp);
        free(exp->var.op.name);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    exp->var.op.argc = argc;
    for (i = 0; i < argc; i++)
       exp->var.op.argv[i] = NULL;

    exp->var.op.optinfo.objpc = 0;

    return exp;
}

enum {
    EXPV_LEN = 64
};

void
RDB_add_arg(RDB_expression *exp, RDB_expression *argp)
{
	int i;
	
    for (i = 0; exp->var.op.argv[i] != NULL; i++);

    exp->var.op.argv[i] = argp;
}

RDB_expression *
RDB_eq(RDB_expression *arg1, RDB_expression *arg2, RDB_exec_context *ecp)
{
    RDB_expression *exp = RDB_ro_op("=", 2, ecp);
    if (exp == NULL)
        return NULL;

    RDB_add_arg(exp, arg1);
    RDB_add_arg(exp, arg2);
    return exp;
}

RDB_expression *
RDB_tuple_attr(RDB_expression *arg, const char *attrname,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;

    exp = _RDB_create_unexpr(arg, RDB_EX_TUPLE_ATTR, ecp);
    if (exp == NULL)
        return NULL;

    exp->var.op.name = RDB_dup_str(attrname);
    if (exp->var.op.name == NULL) {
        RDB_raise_no_memory(ecp);
        RDB_drop_expr(exp, ecp);
        return NULL;
    }
    return exp;
}

RDB_expression *
RDB_expr_comp(RDB_expression *arg, const char *compname,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;

    exp = _RDB_create_unexpr(arg, RDB_EX_GET_COMP, ecp);
    if (exp == NULL)
        return NULL;

    exp->var.op.name = RDB_dup_str(compname);
    if (exp->var.op.name == NULL) {
        RDB_drop_expr(exp, ecp);
        return NULL;
    }
    return exp;
}

static int
drop_children(RDB_expression *exp, RDB_exec_context *ecp)
{
    int i;
    
    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            if (RDB_drop_expr(exp->var.op.argv[0], ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        case RDB_EX_RO_OP:
            for (i = 0; i < exp->var.op.argc; i++) {
                if (exp->var.op.argv[i] != NULL) {
                    if (RDB_drop_expr(exp->var.op.argv[i], ecp) != RDB_OK)
                        return RDB_ERROR;
                }
            }
            break;
        default: ;
    }
    return RDB_OK;
}

/*
 * Destroy the expression but not the children and don't
 * free the memory.
 */
int
_RDB_destroy_expr(RDB_expression *exp, RDB_exec_context *ecp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            /* The expression takes responsibility for non-scalar types */
            if (exp->var.obj.typ != NULL
                    && !RDB_type_is_scalar(exp->var.obj.typ)
                    && exp->var.obj.kind != RDB_OB_TABLE)
                RDB_drop_type(exp->var.obj.typ, ecp, NULL);
            if (RDB_destroy_obj(&exp->var.obj, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        case RDB_EX_TBP:
            break;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            free(exp->var.op.name);
            free(exp->var.op.argv);
            break;
        case RDB_EX_RO_OP:
            free(exp->var.op.name);
            free(exp->var.op.argv);
            if (exp->var.op.optinfo.objpc > 0)
                free(exp->var.op.optinfo.objpv);
            break;
        case RDB_EX_VAR:
            free(exp->var.varname);
            break;
    }
    if (exp->typ != NULL && !RDB_type_is_scalar(exp->typ))
        return RDB_drop_type(exp->typ, ecp, NULL);
    return RDB_OK;
}

/* Destroy the expression and all subexpressions */
int
RDB_drop_expr(RDB_expression *exp, RDB_exec_context *ecp)
{
    int ret;

    if (drop_children(exp, ecp) != RDB_OK)
        return RDB_ERROR;
    ret = _RDB_destroy_expr(exp, ecp);
    free(exp);
    return ret;
}

/*
 * Convert an expression to a virtual table.
 */
static int
evaluate_vt(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_expression *nexp = tplp != NULL ? expr_resolve_attrs(exp, tplp, ecp)
            : RDB_dup_expr(exp, ecp);
    if (nexp == NULL)
        return RDB_ERROR;

    if (_RDB_vtexp_to_obj(nexp, ecp, txp, retvalp) != RDB_OK) {
        RDB_drop_expr(nexp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static RDB_object *
process_aggr_args(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *tbp;
    RDB_expression *texp;

    if (exp->var.op.argc != 2) {
        RDB_raise_invalid_argument("invalid number of aggregate arguments",
                ecp);
        return NULL;
    }

    if (exp->var.op.argv[1]->kind != RDB_EX_VAR) {
        RDB_raise_invalid_argument("invalid aggregate argument #2",
                ecp);
        return NULL;
    }

    texp = RDB_dup_expr(exp->var.op.argv[0], ecp);
    if (texp == NULL)
        return NULL;

    tbp = RDB_expr_to_vtable(texp, ecp, txp);
    if (tbp == NULL) {
        RDB_drop_expr(texp, ecp);
        return NULL;
    }
    return tbp;
}

static int
evaluate_ro_op(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    int i;
    RDB_object *tbp;
    RDB_object **valpv;
    RDB_object *valv = NULL;
    int argc = exp->var.op.argc;

    /*
     * Special treatment of relational operators
     */

    if (strcmp(exp->var.op.name, "EXTEND") == 0) {
        RDB_type *typ = RDB_expr_type(exp->var.op.argv[0], NULL, ecp, txp);
        if (typ == NULL)
            return RDB_ERROR;
        
        if (typ->kind == RDB_TP_RELATION) {
            return evaluate_vt(exp, tplp, ecp, txp, valp);
        } else if (typ->kind == RDB_TP_TUPLE) {
            int attrc = (exp->var.op.argc - 1) / 2;
            RDB_virtual_attr *attrv;

            attrv = malloc(sizeof (RDB_virtual_attr) * attrc);
            if (attrv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }

            for (i = 0; i < attrc; i++) {
                attrv[i].exp = exp->var.op.argv[1 + i * 2];
                
                if (!expr_is_string(exp->var.op.argv[2 + i * 2])) {
                    RDB_raise_type_mismatch("attribute argument must be string",
                            ecp);
                    return RDB_ERROR;
                }
                attrv[i].name = RDB_obj_string(
                        &exp->var.op.argv[2 + i * 2]->var.obj);
            }
            
            ret = RDB_evaluate(exp->var.op.argv[0], tplp, ecp, txp, valp);
            if (ret != RDB_OK) {
                free(attrv);
                return RDB_ERROR;
            }
            
            ret = RDB_extend_tuple(valp, attrc, attrv, ecp, txp);
            free(attrv);
            return ret;
        }
        RDB_raise_invalid_argument("invalid extend argument", ecp);
        return RDB_ERROR;
    }

    if (strcmp(exp->var.op.name, "WHERE") == 0
            || strcmp(exp->var.op.name, "SUMMARIZE") == 0
            || strcmp(exp->var.op.name, "MINUS") == 0
            || strcmp(exp->var.op.name, "SEMIMINUS") == 0
            || strcmp(exp->var.op.name, "INTERSECT") == 0
            || strcmp(exp->var.op.name, "SEMIJOIN") == 0
            || strcmp(exp->var.op.name, "UNION") == 0
            || strcmp(exp->var.op.name, "DIVIDE") == 0
            || strcmp(exp->var.op.name, "GROUP") == 0
            || strcmp(exp->var.op.name, "UNGROUP") == 0) {
        return evaluate_vt(exp, tplp, ecp, txp, valp);
    }

    if (strcmp(exp->var.op.name, "SUM") == 0) {
        tbp = process_aggr_args(exp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        ret = RDB_sum(tbp, exp->var.op.argv[1]->var.varname, ecp, txp, valp);
        RDB_drop_table(tbp, ecp, NULL);
        return ret;
    }
    if (strcmp(exp->var.op.name, "AVG") ==  0) {
        RDB_double res;

        tbp = process_aggr_args(exp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        ret = RDB_avg(tbp, exp->var.op.argv[1]->var.varname, ecp, txp, &res);
        RDB_drop_table(tbp, ecp, NULL);
        if (ret == RDB_OK) {
            RDB_double_to_obj(valp, res);
        }
        return ret;
    }
    if (strcmp(exp->var.op.name, "MIN") ==  0) {
        tbp = process_aggr_args(exp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        ret = RDB_min(tbp, exp->var.op.argv[1]->var.varname, ecp, txp, valp);
        RDB_drop_table(tbp, ecp, NULL);
        return ret;
    }
    if (strcmp(exp->var.op.name, "MAX") ==  0) {
        tbp = process_aggr_args(exp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        ret = RDB_max(tbp, exp->var.op.argv[1]->var.varname, ecp, txp, valp);
        RDB_drop_table(tbp, ecp, NULL);
        return ret;
    }
    if (strcmp(exp->var.op.name, "ALL") ==  0) {
        RDB_bool res;

        tbp = process_aggr_args(exp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        ret = RDB_all(tbp, exp->var.op.argv[1]->var.varname, ecp, txp, &res);
        RDB_drop_table(tbp, ecp, NULL);
        if (ret == RDB_OK) {
            RDB_bool_to_obj(valp, res);
        }
        return ret;
    }
    if (strcmp(exp->var.op.name, "ANY") ==  0) {
        RDB_bool res;

        tbp = process_aggr_args(exp, ecp, txp);
        if (tbp == NULL)
            return RDB_ERROR;
        ret = RDB_any(tbp, exp->var.op.argv[1]->var.varname, ecp, txp, &res);
        RDB_drop_table(tbp, ecp, NULL);
        if (ret == RDB_OK) {
            RDB_bool_to_obj(valp, res);
        }
        return ret;
    }
    valpv = malloc(argc * sizeof (RDB_object *));
    if (valpv == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }
    valv = malloc(argc * sizeof (RDB_object));
    if (valv == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }
    for (i = 0; i < argc; i++)
        valpv[i] = NULL;
    for (i = 0; i < argc; i++) {
        switch (exp->var.op.argv[i]->kind) {
            case RDB_EX_OBJ:
                valpv[i] = &exp->var.op.argv[i]->var.obj;
                break;
            case RDB_EX_TBP:
                valpv[i] = exp->var.op.argv[i]->var.tbref.tbp;
                break;
            default:
                valpv[i] = &valv[i];
                RDB_init_obj(&valv[i]);
                ret = RDB_evaluate(exp->var.op.argv[i], tplp, ecp, txp, &valv[i]);
                if (ret != RDB_OK)
                    goto cleanup;
                break;
        }
    }
    ret = RDB_call_ro_op(exp->var.op.name, argc, valpv, ecp, txp, valp);

cleanup:
    if (valv != NULL) {
        for (i = 0; i < argc; i++) {
            if (valpv[i] != NULL && exp->var.op.argv[i]->kind != RDB_EX_OBJ
                    && exp->var.op.argv[i]->kind != RDB_EX_TBP) {
                RDB_destroy_obj(&valv[i], ecp);
            }
        }
        free(valv);
    }
    free(valpv);
    return ret;
}

int
RDB_evaluate(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *valp)
{
    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        {
            int ret;
            RDB_object tpl;
            RDB_object *attrp;

            RDB_init_obj(&tpl);
            ret = RDB_evaluate(exp->var.op.argv[0], tplp, ecp, txp, &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return ret;
            }
            if (tpl.kind != RDB_OB_TUPLE) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_type_mismatch("", ecp);
                return RDB_ERROR;
            }
                
            attrp = RDB_tuple_get(&tpl, exp->var.op.name);
            if (attrp == NULL) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_attribute_not_found(exp->var.op.name, ecp);
                return RDB_ERROR;
            }
            ret = RDB_copy_obj(valp, attrp, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return ret;
            }
            return RDB_destroy_obj(&tpl, ecp);
        }
        case RDB_EX_GET_COMP:
        {
            int ret;
            RDB_object obj;

            RDB_init_obj(&obj);
            ret = RDB_evaluate(exp->var.op.argv[0], tplp, ecp, txp, &obj);
            if (ret != RDB_OK) {
                 RDB_destroy_obj(&obj, ecp);
                 return ret;
            }
            ret = RDB_obj_comp(&obj, exp->var.op.name, valp, ecp, txp);
            RDB_destroy_obj(&obj, ecp);
            return ret;
        }
        case RDB_EX_RO_OP:
            return evaluate_ro_op(exp, tplp, ecp, txp, valp);
        case RDB_EX_VAR:
            /* Try to get tuple attribute */
            if (tplp != NULL) {
                RDB_object *srcp = RDB_tuple_get(tplp, exp->var.varname);
                if (srcp != NULL)
                    return RDB_copy_obj(valp, srcp, ecp);
            }

            /* Try to get table */
            if (txp != NULL) {
                RDB_object *srcp = RDB_get_table(exp->var.varname, ecp, txp);
                
                if (srcp != NULL)
                    return _RDB_copy_obj(valp, srcp, ecp, txp);
            }
            RDB_raise_attribute_not_found(exp->var.varname, ecp);
            return RDB_ERROR;
        case RDB_EX_OBJ:
            return RDB_copy_obj(valp, &exp->var.obj, ecp);
        case RDB_EX_TBP:
            return _RDB_copy_obj(valp, exp->var.tbref.tbp, ecp, txp);
    }
    /* Should never be reached */
    abort();
}

int
RDB_evaluate_bool(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_object val;

    RDB_init_obj(&val);
    ret = RDB_evaluate(exp, tplp, ecp, txp, &val);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val, ecp);
        return ret;
    }
    if (RDB_obj_type(&val) != &RDB_BOOLEAN) {
        RDB_destroy_obj(&val, ecp);
        RDB_raise_type_mismatch("expression type must be BOOLEAN", ecp);
        return RDB_ERROR;
    }

    *resp = val.var.bool_val;
    RDB_destroy_obj(&val, ecp);
    return RDB_OK;
}

RDB_expression *
RDB_dup_expr(const RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_expression *newexp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = RDB_dup_expr(exp->var.op.argv[0], ecp);
            if (newexp == NULL)
                return NULL;
            return RDB_tuple_attr(newexp, exp->var.op.name, ecp);
        case RDB_EX_GET_COMP:
            newexp = RDB_dup_expr(exp->var.op.argv[0], ecp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_comp(newexp, exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            int i;
            RDB_expression *argp;

            newexp = RDB_ro_op(exp->var.op.name, exp->var.op.argc, ecp);
            if (newexp == NULL)
                return NULL;
            for (i = 0; i < exp->var.op.argc; i++) {
                argp = RDB_dup_expr(exp->var.op.argv[i], ecp);
                if (argp == NULL)
                    return NULL;
                RDB_add_arg(newexp, argp);
            }
            return newexp;
        }
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj, ecp);
        case RDB_EX_TBP:
            return RDB_table_ref(exp->var.tbref.tbp, ecp);
        case RDB_EX_VAR:
            return RDB_var_ref(exp->var.varname, ecp);
    }
    abort();
}

RDB_bool
_RDB_expr_expr_depend(const RDB_expression *ex1p, const RDB_expression *ex2p)
{
    switch (ex1p->kind) {
        case RDB_EX_OBJ:
            return RDB_FALSE;
        case RDB_EX_TBP:
            return _RDB_expr_table_depend(ex2p, ex1p->var.tbref.tbp);
        case RDB_EX_VAR:
            return RDB_FALSE;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_expr_expr_depend(ex1p->var.op.argv[0], ex2p);
        case RDB_EX_RO_OP:
        {
            int i;

            for (i = 0; i < ex1p->var.op.argc; i++) {
                if (_RDB_expr_expr_depend(ex1p->var.op.argv[i], ex2p))
                    return RDB_TRUE;
            }
            return RDB_FALSE;
        }
    }
    /* Should never be reached */
    abort();
}

RDB_bool
_RDB_expr_refers(const RDB_expression *exp, const RDB_object *tbp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            return RDB_FALSE;
        case RDB_EX_TBP:
            return _RDB_table_refers(exp->var.tbref.tbp, tbp);
        case RDB_EX_VAR:
            return RDB_FALSE;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_expr_refers(exp->var.op.argv[0], tbp);
        case RDB_EX_RO_OP:
        {
            int i;

            for (i = 0; i < exp->var.op.argc; i++)
                if (_RDB_expr_refers(exp->var.op.argv[i], tbp))
                    return RDB_TRUE;
            
            return RDB_FALSE;
        }
    }
    /* Should never be reached */
    abort();
}

RDB_bool
_RDB_expr_refers_attr(const RDB_expression *exp, const char *attrname)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_FALSE;
        case RDB_EX_VAR:
            return (RDB_bool) (strcmp(exp->var.varname, attrname) == 0);
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_expr_refers_attr(exp->var.op.argv[0], attrname);
        case RDB_EX_RO_OP:
        {
            int i;

            for (i = 0; i < exp->var.op.argc; i++) {
                if (_RDB_expr_refers_attr(exp->var.op.argv[i], attrname))
                    return RDB_TRUE;
            }            
            return RDB_FALSE;
        }
    }
    /* Should never be reached */
    abort();
}

/*
 * Check if there is some table which both exp and tbp depend on
 */
RDB_bool
_RDB_expr_table_depend(const RDB_expression *exp, const RDB_object *tbp)
{
    if (tbp->var.tb.exp == NULL)
        return _RDB_expr_refers(exp, tbp);
    return _RDB_expr_expr_depend(tbp->var.tb.exp, exp);
}

RDB_object *
RDB_expr_obj(RDB_expression *exp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            return &exp->var.obj;
        case RDB_EX_TBP:
            return exp->var.tbref.tbp;
        default: ;
    }
    return NULL;
}

int
_RDB_invrename_expr(RDB_expression *exp, RDB_expression *texp,
        RDB_exec_context *ecp)
{
    int ret;
    int i;
    int renc;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_invrename_expr(exp->var.op.argv[0], texp, ecp);
        case RDB_EX_RO_OP:
            for (i = 0; i < exp->var.op.argc; i++) {
                ret = _RDB_invrename_expr(exp->var.op.argv[i], texp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
            /* Search attribute name in dest attrs */
            renc = (texp->var.op.argc - 1) / 2;
            for (i = 0;
                    i < renc && strcmp(
                            RDB_obj_string(&texp->var.op.argv[2 + i * 2]->var.obj),
                            exp->var.varname) != 0;
                    i++);

            /* If found, replace it */
            if (i < renc) {
                char *from = RDB_obj_string(&texp->var.op.argv[1 + i * 2]->var.obj);
                char *name = realloc(exp->var.varname, strlen(from) + 1);
                if (name == NULL) {
                    RDB_raise_no_memory(ecp);
                    return RDB_ERROR;
                }

                strcpy(name, from);
                exp->var.varname = name;
            }
            return RDB_OK;
    }
    abort();
}

int
_RDB_resolve_extend_expr(RDB_expression **expp, RDB_expression *texp,
        RDB_exec_context *ecp)
{
    int i;
    int attrc;

    switch ((*expp)->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_resolve_extend_expr(&(*expp)->var.op.argv[0],
                    texp, ecp);
        case RDB_EX_RO_OP:
            for (i = 0; i < (*expp)->var.op.argc; i++) {
                if (_RDB_resolve_extend_expr(&(*expp)->var.op.argv[i],
                        texp, ecp) != RDB_OK)
                    return RDB_ERROR;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
            attrc = (texp->var.op.argc - 1) / 2;
        
            /* Search attribute name in attrv[].name */
            for (i = 0;
                    i < attrc && strcmp(
                            RDB_obj_string(&texp->var.op.argv[2 + i * 2]->var.obj),
                            (*expp)->var.varname) != 0;
                    i++);

            /* If found, replace attribute by expression */
            if (i < attrc) {
                RDB_expression *exp = RDB_dup_expr(texp->var.op.argv[1 + i * 2], ecp);
                if (exp == NULL) {
                    return RDB_ERROR;
                }

                RDB_drop_expr(*expp, ecp);
                *expp = exp;
            }
            return RDB_OK;
    }
    abort();
}

static RDB_bool
expr_var(RDB_expression *exp, const char *attrname, char *opname)
{
    if (exp->kind == RDB_EX_RO_OP && strcmp(exp->var.op.name, opname) == 0) {
        if (exp->var.op.argv[0]->kind == RDB_EX_VAR
                && strcmp(exp->var.op.argv[0]->var.varname, attrname) == 0
                && exp->var.op.argv[1]->kind == RDB_EX_OBJ)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

RDB_expression *
_RDB_attr_node(RDB_expression *exp, const char *attrname, char *opname)
{
    while (exp->kind == RDB_EX_RO_OP
            && strcmp (exp->var.op.name, "AND") == 0) {
        if (expr_var(exp->var.op.argv[1], attrname, opname))
            return exp;
        exp = exp->var.op.argv[0];
    }
    if (expr_var(exp, attrname, opname))
        return exp;
    return NULL;
}

/*
 * Convert the expression into a reference to an empty table
 */
int
_RDB_expr_to_empty_table(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *typ = RDB_expr_type(exp, NULL, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    /*
     * exp->typ will be consumed by RDB_init_table_from_type(),
     * so prevent it from being destroyed.
     */
    exp->typ = NULL;

    if (drop_children(exp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_destroy_expr(exp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    exp->kind = RDB_EX_OBJ;
    RDB_init_obj(&exp->var.obj);
    return RDB_init_table_from_type(&exp->var.obj, NULL, typ, 0, NULL, ecp);
}
