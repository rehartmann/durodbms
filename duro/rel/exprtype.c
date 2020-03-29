/*
 * Copyright (C) 2012, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "transform.h"
#include "internal.h"
#include <gen/strfns.h>
#include <obj/objinternal.h>

#include <string.h>

struct chained_type_getters {
    void *getarg1;
    RDB_gettypefn *getfn1p;
    void *getarg2;
    RDB_gettypefn *getfn2p;
};

RDB_type *
RDB_get_tuple_attr_type(const char *attrname, void *arg)
{
    RDB_attr *attrp = RDB_tuple_type_attr(arg, attrname);
    return attrp != NULL ? attrp->typ : NULL;
}

static RDB_type *
get_type_from_chained_getters(const char *attrname, void *arg) {
    RDB_type *typ;
    struct chained_type_getters *mapp = arg;
    if (mapp->getfn1p != NULL) {
        typ = (*mapp->getfn1p) (attrname, mapp->getarg1);
    }
    if (typ != NULL || mapp->getfn2p == NULL)
        return typ;

    return (*mapp->getfn2p) (attrname, mapp->getarg2);
}

static RDB_type *
wrap_type(const RDB_expression *exp, RDB_type **argtv,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int wrapc;
    RDB_wrapping *wrapv;
    RDB_expression *argp;
    RDB_type *typ = NULL;
    int argc = RDB_expr_list_length(&exp->def.op.args);

    if (argc < 1 || argc % 2 != 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    wrapc = (argc - 1) / 2;
    if (wrapc > 0) {
        wrapv = RDB_alloc(sizeof(RDB_wrapping) * wrapc, ecp);
        if (wrapv == NULL) {
            return NULL;
        }
    }
    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrv = NULL;
    }

    argp = exp->def.op.args.firstp->nextp;
    for (i = 0; i < wrapc; i++) {
        RDB_object *objp;

        wrapv[i].attrc =  RDB_array_length(&argp->def.obj, ecp);
        wrapv[i].attrv = RDB_alloc(sizeof (char *) * wrapv[i].attrc, ecp);
        if (wrapv[i].attrv == NULL)
            return NULL;
        for (j = 0; j < wrapv[i].attrc; j++) {
            objp = RDB_array_get(&argp->def.obj, (RDB_int) j, ecp);
            if (objp == NULL) {
                goto cleanup;
            }
            wrapv[i].attrv[j] = RDB_obj_string(objp);
        }
        argp = argp->nextp;
        wrapv[i].attrname = RDB_obj_string(&argp->def.obj);
        argp = argp->nextp;
    }

    if (argtv[0] == NULL) {
        RDB_raise_invalid_argument("Invalid WRAP argument", ecp);
        goto cleanup;
    }

    if (RDB_type_is_relation(argtv[0])) {
        typ = RDB_wrap_relation_type(argtv[0], wrapc, wrapv, ecp);
    } else if (RDB_type_is_tuple(argtv[0])) {
        typ = RDB_wrap_tuple_type(argtv[0], wrapc, wrapv, ecp);
    } else {
        RDB_raise_type_mismatch("First argument to WRAP must be non-scalar",
                ecp);
    }

cleanup:
    for (i = 0; i < wrapc; i++) {
        RDB_free(wrapv[i].attrv);
    }
    if (wrapc > 0)
        RDB_free(wrapv);
    return typ;
}

static RDB_type *
where_type(const RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *condtyp;
    RDB_type *reltyp;

    /*
     * Check argument types
     */

    if (RDB_expr_list_length(&exp->def.op.args) != 2) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    reltyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, envp, ecp, txp);
    if (reltyp == NULL)
        return NULL;

    if (reltyp->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("WHERE requires relation argument", ecp);
        return NULL;
    }
    if (getfnp != NULL) {
        struct chained_type_getters tgetters;
        tgetters.getarg1 = arg;
        tgetters.getfn1p = getfnp;
        tgetters.getarg2 = reltyp->def.basetyp;
        tgetters.getfn2p = RDB_get_tuple_attr_type;
        condtyp = RDB_expr_type(exp->def.op.args.lastp, get_type_from_chained_getters,
                &tgetters, envp, ecp, txp);
    } else {
        condtyp = RDB_expr_type(exp->def.op.args.lastp, RDB_get_tuple_attr_type,
                reltyp->def.basetyp, envp, ecp, txp);
    }
    if (condtyp == NULL) {
        return NULL;
    }
    if (condtyp != &RDB_BOOLEAN) {
        RDB_raise_type_mismatch("WHERE requires BOOLEAN argument", ecp);
        return NULL;
    }

    /*
     * Return copy of type
     */
    return RDB_dup_nonscalar_type(reltyp, ecp);
}

/**
 * Return the type of the expression. Use *tpltyp to resolve refs to variables.
 * If the type is non-scalar, it is managed by the expression.
 */
RDB_type *
RDB_expr_type_tpltyp(RDB_expression *exp, const RDB_type *tpltyp,
        RDB_gettypefn *getfnp, void *getarg,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    struct chained_type_getters chgetters;

    chgetters.getfn1p = tpltyp != NULL ? &RDB_get_tuple_attr_type : NULL;
    chgetters.getarg1 = (void *) tpltyp;
    chgetters.getfn2p = getfnp;
    chgetters.getarg2 = getarg;

    return RDB_expr_type(exp, get_type_from_chained_getters, &chgetters,
            envp, ecp, txp);
}

static RDB_type *
extend_type(const RDB_expression *exp, RDB_gettypefn *getfnp, void *getarg,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_attr *attrv;
    RDB_type *tpltyp, *typ;
    int attrc;
    RDB_type *arg1typ;
    RDB_expression *argp;
    int argc = RDB_expr_list_length(&exp->def.op.args);

    if (argc < 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    attrc = (argc - 1) / 2;
    arg1typ = RDB_expr_type(exp->def.op.args.firstp, getfnp, getarg, envp, ecp, txp);
    if (arg1typ == NULL)
        return NULL;

    if (arg1typ->kind != RDB_TP_RELATION && arg1typ->kind != RDB_TP_TUPLE) {
        RDB_raise_invalid_argument(
                "Invalid first argument to EXTEND", ecp);
        return NULL;
    }

    if (arg1typ->kind == RDB_TP_RELATION)
        tpltyp = arg1typ->def.basetyp;
    else
        tpltyp = arg1typ;

    attrv = RDB_alloc(sizeof (RDB_attr) * attrc, ecp);
    if (attrv == NULL) {
        return NULL;
    }
    argp = exp->def.op.args.firstp->nextp;
    for (i = 0; i < attrc; i++) {
        attrv[i].typ = RDB_expr_type_tpltyp(argp, tpltyp, getfnp, getarg, envp, ecp, txp);
        if (attrv[i].typ == NULL)
            attrv[i].typ = RDB_expr_type(argp, getfnp, getarg, envp, ecp, txp);
        if (attrv[i].typ == NULL) {
            int j;

            RDB_free(attrv);
            for (j = 0; i < j; j++) {
                if (!RDB_type_is_scalar(attrv[j].typ)) {
                    RDB_del_nonscalar_type(attrv[j].typ, ecp);
                }
            }
            return NULL;
        }
        argp = argp->nextp;
        if (argp->kind != RDB_EX_OBJ ||
                argp->def.obj.typ != &RDB_STRING) {
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        attrv[i].name = RDB_obj_string(&argp->def.obj);
        argp = argp->nextp;
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
    RDB_free(attrv);
    return typ;
}

static RDB_type *
project_type(const RDB_expression *exp, RDB_type *argtv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    const char **attrv;
    RDB_type *typ;
    RDB_expression *argp;
    int argc = RDB_expr_list_length(&exp->def.op.args);

    if (argc == 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    if (argtv[0] == NULL) {
        RDB_raise_invalid_argument("Invalid PROJECT argument", ecp);
        return NULL;
    }

    argp = exp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        if (!RDB_expr_is_string(argp)) {
            RDB_raise_type_mismatch("PROJECT requires string arguments",
                    ecp);
            return NULL;
        }
        argp = argp->nextp;
    }
    attrv = RDB_alloc(sizeof (char *) * (argc - 1), ecp);
    if (attrv == NULL) {
        return NULL;
    }
    argp = exp->def.op.args.firstp->nextp;
    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(&argp->def.obj);
        argp = argp->nextp;
    }
    if (argtv[0]->kind == RDB_TP_RELATION) {
        typ = RDB_project_relation_type(argtv[0], argc - 1, attrv,
               ecp);
    } else if (argtv[0]->kind == RDB_TP_TUPLE) {
        typ = RDB_project_tuple_type(argtv[0], argc - 1, attrv,
               ecp);
    } else {
        RDB_raise_invalid_argument("invalid PROJECT argument", ecp);
        typ = NULL;
    }
    RDB_free(attrv);
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
    RDB_expression *argp;
    int argc = RDB_expr_list_length(&exp->def.op.args);

    if (argc == 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    if (argtv[0] == NULL) {
        RDB_raise_invalid_argument("Invalid UNWRAP argument", ecp);
        return NULL;
    }

    attrc = argc - 1;

    argp = exp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        if (!RDB_expr_is_string(argp)) {
            RDB_raise_type_mismatch(
                    "UNWRAP argument must be of type string", ecp);
            return NULL;
        }
        argp = argp->nextp;
    }

    attrv = RDB_alloc(attrc * sizeof (char *), ecp);
    if (attrv == NULL) {
        return NULL;
    }
    argp = exp->def.op.args.firstp->nextp;
    i = 0;
    while (argp != NULL) {
        if (!RDB_expr_is_string(argp)) {
            RDB_raise_invalid_argument("string expression expected by UNWRAP",
                    ecp);
            return NULL;
        }
        attrv[i++] = RDB_obj_string(&argp->def.obj);
        argp = argp->nextp;
    }

    if (argtv[0]->kind == RDB_TP_RELATION) {
        typ = RDB_unwrap_relation_type(argtv[0], attrc, attrv, ecp);
    } else if (argtv[0]->kind == RDB_TP_TUPLE) {
        typ = RDB_unwrap_tuple_type(argtv[0], attrc, attrv, ecp);
    } else {
        RDB_raise_invalid_argument("invalid UNWRAP argument", ecp);
        typ = NULL;
    }

    RDB_free(attrv);
    return typ;
}

static RDB_type *
group_type(const RDB_expression *exp, RDB_type *argtv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    char **attrv;
    int i;
    RDB_type *typ;
    RDB_expression *argp;
    int argc = RDB_expr_list_length(&exp->def.op.args);

    if (argc < 2) {
        RDB_raise_invalid_argument("invalid GROUP argument", ecp);
        return NULL;
    }

    if (argtv[0] == NULL || argtv[0]->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("Invalid GROUP argument", ecp);
        return NULL;
    }

    argp = exp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        if (!RDB_expr_is_string(argp)) {
            RDB_raise_type_mismatch("STRING attribute required", ecp);
            return NULL;
        }
        argp = argp->nextp;
    }

    if (argc > 2) {
        attrv = RDB_alloc(sizeof (char *) * (argc - 2), ecp);
        if (attrv == NULL) {
            return NULL;
        }
    } else {
        attrv = NULL;
    }

    argp = exp->def.op.args.firstp->nextp;
    for (i = 1; i < argc - 1; i++) {
        attrv[i - 1] = RDB_obj_string(&argp->def.obj);
        argp = argp->nextp;
    }
    typ = RDB_group_type(argtv[0], argc - 2, attrv,
            RDB_obj_string(&exp->def.op.args.lastp->def.obj), ecp);
    RDB_free(attrv);
    return typ;
}

static RDB_type *
tuple_type(const RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int attrc;
    RDB_attr *attrv;
    RDB_expression *argp;
    RDB_object *attrobjp;
    RDB_type *typ = NULL;
    int argc = RDB_expr_list_length(&exp->def.op.args);

    if ((argc % 2) == 1) {
        RDB_raise_invalid_argument("invalid number of TUPLE arguments", ecp);
        return NULL;
    }
    attrc = argc / 2;

    attrv = RDB_alloc(sizeof(RDB_attr) * attrc, ecp);
    if (attrv == NULL)
        return NULL;

    for (i = 0; i < attrc; i++) {
        attrv[i].typ = NULL;
    }

    argp = exp->def.op.args.firstp;
    for (i = 0; i < attrc; i++) {
        attrobjp = RDB_expr_obj(argp);
        if (attrobjp == NULL || RDB_obj_type(attrobjp) != &RDB_STRING) {
            RDB_raise_invalid_argument("invalid TUPLE argument", ecp);
            goto cleanup;
        }

        argp = argp->nextp;

        attrv[i].name = RDB_obj_string(attrobjp);
        attrv[i].typ = RDB_expr_type(argp, getfnp, arg, envp, ecp, txp);
        if (attrv[i].typ == NULL)
            goto cleanup;

        argp = argp->nextp;
    }

    typ = RDB_new_tuple_type(attrc, attrv, ecp);

cleanup:
    RDB_free(attrv);
    return typ;
}

static RDB_type *
array_type(const RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *rtyp = NULL;
    RDB_type *basetyp;

    if (exp->def.op.args.firstp == NULL) {
        RDB_raise_internal("argument required for ARRAY", ecp);
        goto error;
    }
    basetyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, envp, ecp, txp);
    if (basetyp == NULL) {
        RDB_raise_invalid_argument("type required for ARRAY argument", ecp);
        goto error;
    }
    basetyp = RDB_dup_nonscalar_type(basetyp, ecp);
    if (basetyp == NULL)
        return NULL;

    rtyp = RDB_new_array_type(basetyp, ecp);
    if (rtyp == NULL) {
        if (!RDB_type_is_scalar(basetyp))
            RDB_del_nonscalar_type(basetyp, ecp);
        return NULL;
    }

    return rtyp;

error:
    return NULL;
}

/*
 * Return type of relation expression *exp and tuple types in argtv.
 * The type is not consumed.
 */
static RDB_type *
relation_type(const RDB_expression *exp, RDB_type **argtv,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *rtyp;
    RDB_type *tpltyp;
    RDB_type *basetyp;

    /*
     * When there is no argument, the type of RELATION cannot be inferred.
     */
    if (exp->def.op.args.firstp == NULL) {
        RDB_raise_not_found("cannot determine RELATION type", ecp);
        return NULL;
    }
    tpltyp = argtv[0];
    if (tpltyp == NULL) {
        /*
         * If the type of the first arg hasn't been passed in argtv[0],
         * get type of first expression
         */
        tpltyp = RDB_expr_type(exp->def.op.args.firstp, NULL, NULL, envp, ecp, txp);
        if (tpltyp == NULL)
            return NULL;
    }
    if (tpltyp->kind != RDB_TP_TUPLE) {
        RDB_raise_type_mismatch("tuple argument required for RELATION", ecp);
        return NULL;
    }

    /*
     * Create relation type. The base type must be duplicated.
     */
    basetyp = RDB_dup_nonscalar_type(tpltyp, ecp);
    if (basetyp == NULL)
        return NULL;
    rtyp = RDB_new_relation_type_from_base(basetyp, ecp);
    if (rtyp == NULL) {
        RDB_del_nonscalar_type(basetyp, ecp);
        return NULL;
    }
    return rtyp;
}

static RDB_type *
rename_type(const RDB_expression *exp, RDB_type **argtv,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int renc;
    int i;
    RDB_type *typ;
    RDB_expression *argp;
    RDB_renaming *renv = NULL;
    int argc = RDB_expr_list_length(&exp->def.op.args);

    if (argc == 0 || argc % 2 != 1) {
        RDB_raise_invalid_argument("invalid number of RENAME arguments", ecp);
        return NULL;
    }

    renc = (argc - 1) / 2;
    if (renc > 0) {
        renv = RDB_alloc(sizeof (RDB_renaming) * renc, ecp);
        if (renv == NULL) {
            return NULL;
        }
    }

    argp = exp->def.op.args.firstp->nextp;
    for (i = 0; i < renc; i++) {
        if (argp->kind != RDB_EX_OBJ || argp->def.obj.typ != &RDB_STRING) {
            RDB_free(renv);
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        if (argp->nextp->kind != RDB_EX_OBJ ||
                argp->nextp->def.obj.typ != &RDB_STRING) {
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        renv[i].from = RDB_obj_string(&argp->def.obj);
        renv[i].to = RDB_obj_string(&argp->nextp->def.obj);
        argp = argp->nextp->nextp;
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
    RDB_free(renv);
    return typ;
}

static RDB_type *
var_type(const char *varname, RDB_gettypefn *getfnp, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ;
    RDB_object *errp;

    if (getfnp != NULL) {
        RDB_clear_err(ecp);
        typ = (*getfnp) (varname, getarg);
        if (typ != NULL) {
            return RDB_dup_nonscalar_type(typ, ecp);
        }
    }
    if (txp != NULL) {
        RDB_object *tbp = RDB_get_table(varname, ecp, txp);
        if (tbp != NULL) {
            return RDB_dup_nonscalar_type(RDB_obj_type(tbp), ecp);
        }
    }

    /*
     * Handle error - if no error or NOT_FOUND_ERROR has been raised raise NAME_ERROR
     */
    errp = RDB_get_err(ecp);
    if (errp == NULL || RDB_obj_type(errp) == &RDB_NOT_FOUND_ERROR) {
        RDB_raise_name(varname, ecp);
    }
    return NULL;
}

static RDB_type *
expr_op_type(RDB_expression *exp, RDB_gettypefn *getfnp, void *getarg,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_type *typ;
    RDB_operator *op;
    RDB_expression *argp;
    int argc;
    RDB_type **argtv = NULL;

    if (exp->def.op.name == NULL) {
        RDB_type *optyp = RDB_expr_type(exp->def.op.op, getfnp, getarg, envp, ecp, txp);
        if (optyp == NULL) {
            return NULL;
        }
        if (!RDB_type_is_operator(optyp)) {
            RDB_raise_invalid_argument("not an operator", ecp);
        }
        return optyp->def.op.rtyp;
    }

    /* Transform UPDATE */
    if (strcmp(exp->def.op.name, "update") == 0) {
        if (RDB_convert_update(exp, getfnp, getarg, ecp, txp) != RDB_OK)
            return NULL;
    }

    /*
     * WHERE, EXTEND, etc. require special treatment
     */
    if (strcmp(exp->def.op.name, "where") == 0) {
        return where_type(exp, getfnp, getarg, envp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "extend") == 0) {
        return extend_type(exp, getfnp, getarg, envp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "summarize") == 0) {
        return RDB_summarize_type(&exp->def.op.args, getfnp, getarg,
                ecp, txp);
    }
    if (strcmp(exp->def.op.name, "tuple") == 0) {
        return tuple_type(exp, getfnp, getarg, envp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "array") == 0) {
        return array_type(exp, getfnp, getarg, envp, ecp, txp);
    }

    if (strcmp(exp->def.op.name, "remove") == 0) {
        if (RDB_remove_to_project(exp, getfnp, getarg, ecp, txp) != RDB_OK)
            goto error;
    }

    argc = RDB_expr_list_length(&exp->def.op.args);

    /* Aggregate operators */
    if (strcmp(exp->def.op.name, "sum") == 0
            || strcmp(exp->def.op.name, "min") == 0
            || strcmp(exp->def.op.name, "max") == 0
            || strcmp(exp->def.op.name, "avg") == 0) {
        RDB_type *argtyp;
        RDB_type *attrtyp;

        if (argc != 1 && argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }

        argtyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, getarg, envp, ecp, txp);
        if (argtyp == NULL)
            goto error;
        if (argtyp->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("aggregate requires relation argument",
                    ecp);
            goto error;
        }
        if (argc == 1) {
            if (argtyp->def.basetyp->def.tuple.attrc != 1) {
                RDB_raise_invalid_argument("second argument required", ecp);
                goto error;
            }
            attrtyp = argtyp->def.basetyp->def.tuple.attrv[0].typ;
        } else {
            attrtyp = RDB_expr_type(exp->def.op.args.firstp->nextp, RDB_get_tuple_attr_type,
                    argtyp->def.basetyp, envp, ecp, txp);
            if (attrtyp == NULL)
                goto error;
        }
        if (attrtyp != &RDB_INTEGER && attrtyp != &RDB_FLOAT) {
            RDB_raise_type_mismatch("numeric type required", ecp);
            goto error;
        }
        return strcmp(exp->def.op.name, "avg") == 0 ? &RDB_FLOAT : attrtyp;
    } else if (strcmp(exp->def.op.name, "any") == 0
            || strcmp(exp->def.op.name, "all") == 0) {
        RDB_type *argtyp;
        RDB_type *attrtyp;

        if (argc != 1 && argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }

        argtyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, getarg, envp, ecp, txp);
        if (argtyp == NULL)
            goto error;
        if (argtyp->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("aggregate requires relation argument",
                    ecp);
            goto error;
        }
        if (argc == 1) {
            if (argtyp->def.basetyp->def.tuple.attrc != 1) {
                RDB_raise_invalid_argument("second argument required", ecp);
                goto error;
            }
            attrtyp = argtyp->def.basetyp->def.tuple.attrv[0].typ;
        } else {
            attrtyp = RDB_expr_type(exp->def.op.args.firstp->nextp, RDB_get_tuple_attr_type,
                    argtyp->def.basetyp, envp, ecp, txp);
            if (attrtyp == NULL)
                goto error;
        }
        if (attrtyp != &RDB_BOOLEAN) {
            RDB_raise_type_mismatch("boolean required", ecp);
            goto error;
        }
        return &RDB_BOOLEAN;
    }

    if (strcmp(exp->def.op.name, ".") == 0 && argc == 2) {
        const char *attrname;
        typ = RDB_expr_type(exp->def.op.args.firstp, getfnp, getarg,
                envp, ecp, txp);
        if (typ == NULL) {
            RDB_exec_context ec;
            RDB_object varnameobj;

            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NAME_ERROR)
                return NULL;

            /* Interpret as qualified variable name */
            RDB_init_obj(&varnameobj);
            RDB_init_exec_context(&ec);
            if (RDB_expr_attr_qid(exp, &varnameobj, &ec) != RDB_OK) {
                RDB_destroy_exec_context(&ec);
                RDB_destroy_obj(&varnameobj, ecp);
                return NULL;
            }
            RDB_destroy_exec_context(&ec);
            typ = var_type(RDB_obj_string(&varnameobj), getfnp, getarg, ecp, txp);
            RDB_destroy_obj(&varnameobj, ecp);
            return typ;
        }

        attrname = RDB_expr_var_name(exp->def.op.args.firstp->nextp);
        if (attrname == NULL) {
            RDB_raise_invalid_argument("invalid '.' expression", ecp);
            return NULL;
        }
        if (RDB_type_is_scalar(typ)) {
            RDB_attr *attrp = RDB_prop_attr(typ, attrname);
            if (attrp == NULL) {
                RDB_raise_name(attrname, ecp);
                return NULL;
            }
            return RDB_dup_nonscalar_type(attrp->typ, ecp);
        }
        if (!RDB_type_is_tuple(typ)) {
            RDB_raise_invalid_argument("scalar or tuple required", ecp);
            return NULL;
        }
        typ = RDB_type_attr_type(typ, attrname);
        if (typ == NULL) {
            RDB_raise_name(attrname, ecp);
            return NULL;
        }
        return RDB_dup_nonscalar_type(typ, ecp);
    }

    /*
     * Get argument types
     */
    if (argc > 0) {
        argtv = RDB_alloc(sizeof (RDB_type *) * argc, ecp);
        if (argtv == NULL) {
            return NULL;
        }
    }

    argp = exp->def.op.args.firstp;
    for (i = 0; i < argc; i++) {
        /*
         * The expression may not have a type (e.g. if it's an array).
         * In this case RDB_NOT_FOUND is raised, which is caught.
         */
        argtv[i] = RDB_expr_type(argp, getfnp, getarg, envp, ecp, txp);
        if (argtv[i] == NULL) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                goto error;
            RDB_clear_err(ecp);
        }
        argp = argp->nextp;
    }

    if (strcmp(exp->def.op.name, "count") == 0
            && argc == 1) {
        if (argc == 1 && argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("count requires relation argument", ecp);
            goto error;
        }
        typ = &RDB_INTEGER;
    } else if (strcmp(exp->def.op.name, "relation") == 0) {
        typ = relation_type(exp, argtv, envp, ecp, txp);
    } else if (strcmp(exp->def.op.name, "rename") == 0) {
        typ = rename_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "wrap") == 0) {
        typ = wrap_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "project") == 0) {
        typ = project_type(exp, argtv, ecp, txp);
    } else if (argc == 2
            && (argtv[0] == NULL || !RDB_type_is_scalar(argtv[0]))
            && (argtv[1] == NULL || !RDB_type_is_scalar(argtv[1]))
            && (strcmp(exp->def.op.name, "=") == 0
                    || strcmp(exp->def.op.name, "<>") == 0)) {
        /*
         * Handle nonscalar comparison
         */
        if (argtv[0] != NULL && argtv[1] != NULL
                && !RDB_type_equals(argtv[0], argtv[1])) {
            RDB_raise_type_mismatch("", ecp);
            goto error;
        }
        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->def.op.name, "if") == 0) {
        /*
         * Handle IF-THEN-ELSE
         */
        if (argc != 3) {
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
    } else if (strcmp(exp->def.op.name, "is_empty") == 0) {
        if (argc != 1) {
            RDB_raise_invalid_argument("invalid number of arguments", ecp);
            goto error;
        }

        if (argtv[0] == NULL || argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("is_empty requires relation argument", ecp);
            goto error;
        }

        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->def.op.name, "in") == 0
                    || strcmp(exp->def.op.name, "subset_of") == 0) {
        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of arguments", ecp);
            goto error;
        }

        if (argtv[1] == NULL || argtv[1]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("relation argument required", ecp);
            goto error;
        }

        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->def.op.name, "unwrap") == 0) {
        typ = unwrap_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "group") == 0) {
        typ = group_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "ungroup") == 0) {
        if (argc != 2 || argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid UNGROUP", ecp);
            goto error;
        }
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("UNGROUP requires relation argument", ecp);
            goto error;
        }
        if (exp->def.op.args.firstp->nextp->kind != RDB_EX_OBJ
                || exp->def.op.args.firstp->nextp->def.obj.typ != &RDB_STRING) {
            RDB_raise_type_mismatch("invalid UNGROUP argument", ecp);
            goto error;
        }

        typ = RDB_ungroup_type(argtv[0],
                RDB_obj_string(&exp->def.op.args.firstp->nextp->def.obj), ecp);
    } else if (strcmp(exp->def.op.name, "join") == 0) {
        if (argc != 2 || argtv[0] == NULL
                || argtv[1] == NULL) {
            RDB_raise_invalid_argument("invalid JOIN", ecp);
            goto error;
        }

        if (!RDB_type_is_relation(argtv[0])
                || !RDB_type_is_relation(argtv[1])) {
            RDB_raise_type_mismatch("relation argument required", ecp);
            goto error;
        }
        typ = RDB_join_relation_types(argtv[0], argtv[1], ecp);
    } else if (strcmp(exp->def.op.name, "union") == 0) {
        if (argc != 2 || argtv[0] == NULL
                || argtv[1] == NULL) {
            RDB_raise_invalid_argument("invalid relational operator invocation",
                    ecp);
            goto error;
        }

        if ((RDB_type_is_relation(argtv[0]) && RDB_type_is_relation(argtv[1]))) {
            if (!RDB_type_equals(argtv[0], argtv[1])) {
                RDB_raise_type_mismatch("argument types do not match", ecp);
                goto error;
            }
            typ = RDB_dup_nonscalar_type(argtv[0], ecp);
        } else if ((RDB_type_is_tuple(argtv[0]) && RDB_type_is_tuple(argtv[1]))) {
            typ = RDB_union_tuple_types(argtv[0], argtv[1], ecp);
        } else {
            RDB_raise_type_mismatch("table or tuple argument required", ecp);
            goto error;
        }
        if (typ == NULL)
            goto error;
        if (argc > 0)
            RDB_free(argtv);
        return typ;
    } else if (strcmp(exp->def.op.name, "d_union") == 0
                || strcmp(exp->def.op.name, "minus") == 0
                || strcmp(exp->def.op.name, "semiminus") == 0
                || strcmp(exp->def.op.name, "intersect") == 0
                || strcmp(exp->def.op.name, "semijoin") == 0) {
        if (argc != 2 || argtv[0] == NULL
                || argtv[1] == NULL) {
            RDB_raise_invalid_argument("invalid relational operator invocation",
                    ecp);
            goto error;
        }

        if (strcmp(exp->def.op.name, "d_union") == 0
                || strcmp(exp->def.op.name, "minus") == 0
                || strcmp(exp->def.op.name, "intersect") == 0) {
            if (!RDB_type_equals(argtv[0], argtv[1])) {
                RDB_raise_type_mismatch("argument types do not match", ecp);
                goto error;
            }
        }

        if (argtv[0]->kind != RDB_TP_RELATION
                    || argtv[1]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("relation argument required", ecp);
            goto error;
        }

        typ = RDB_dup_nonscalar_type(argtv[0], ecp);
        if (typ == NULL)
            goto error;
        RDB_free(argtv);
        return typ;
    } else if (strcmp(exp->def.op.name, "divide") == 0) {
        if (argc != 3 || argtv[0] == NULL
                || argtv[1] == NULL || argtv[2] == NULL) {
            RDB_raise_invalid_argument("invalid DIVIDE",
                    ecp);
            goto error;
        }

        if (argtv[0]->kind != RDB_TP_RELATION
                    || argtv[1]->kind != RDB_TP_RELATION
                    || argtv[2]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("relation argument required", ecp);
            goto error;
        }

        typ = RDB_dup_nonscalar_type(argtv[0], ecp);
        if (typ == NULL)
            goto error;
        RDB_free(argtv);
        return typ;
    } else if (strcmp(exp->def.op.name, "to_tuple") == 0) {
        if (argc != 1 || argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid to_tuple",
                    ecp);
            goto error;
        }

        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("relation argument required", ecp);
            goto error;
        }

        typ = RDB_dup_nonscalar_type(argtv[0]->def.basetyp, ecp);
    } else if (strcmp(exp->def.op.name, "length") == 0
            && argc == 1
            && (argtv[0] == NULL || argtv[0]->kind == RDB_TP_ARRAY)) {
        /* Array length operator */
        typ = &RDB_INTEGER;
    } else if (strcmp(exp->def.op.name, "index_of") == 0
            && argc == 2
            && (argtv[0] == NULL || argtv[0]->kind == RDB_TP_ARRAY)) {
        /* Array index operator */
        typ = &RDB_INTEGER;
    } else if (strcmp(exp->def.op.name, "[]") == 0
            && argc == 2 && argtv[0] != NULL) {
    	if (argtv[0]->kind == RDB_TP_ARRAY) {
            /* Array subscript operator */
            typ = RDB_dup_nonscalar_type(argtv[0]->def.basetyp, ecp);
    	} else if (argtv[0]->kind == RDB_TP_TUPLE) {
    		RDB_object obj;

    		RDB_init_obj(&obj);
    		if (RDB_evaluate(exp->def.op.args.firstp->nextp, NULL, NULL,
    		        envp, ecp, txp, &obj) == RDB_ERROR) {
    			RDB_destroy_obj(&obj, ecp);
    			goto error;
    		}
    		if (RDB_obj_type(&obj) != &RDB_STRING) {
    			RDB_destroy_obj(&obj, ecp);
    			RDB_raise_type_mismatch("subscript argument must be string", ecp);
    			goto error;
    		}

            typ = RDB_type_attr_type(argtv[0], RDB_obj_string(&obj));
            if (typ == NULL) {
            	RDB_raise_not_found(RDB_obj_string(&obj), ecp);
    			RDB_destroy_obj(&obj, ecp);
            	goto error;
            }
			RDB_destroy_obj(&obj, ecp);
    	}
    } else if (strcmp(exp->def.op.name, "tclose") == 0 && argc == 1) {
        if (argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid TCLOSE invocation", ecp);
            goto error;
        }
        if (!RDB_type_is_relation(argtv[0])) {
            RDB_raise_type_mismatch("relation argument required", ecp);
            goto error;
        }
        if (argtv[0]->def.basetyp->def.tuple.attrc != 2) {
            RDB_raise_invalid_argument("TCLOSE argument must have 2 attributes", ecp);
            goto error;
        }
        if (!RDB_type_equals(argtv[0]->def.basetyp->def.tuple.attrv[0].typ,
                argtv[0]->def.basetyp->def.tuple.attrv[1].typ)) {
            RDB_raise_type_mismatch("TCLOSE argument attributes not of same type", ecp);
            goto error;
        }
        typ = RDB_dup_nonscalar_type(argtv[0], ecp);
    } else {
        for (i = 0; i < argc; i++) {
            if (argtv[i] == NULL) {
                RDB_raise_operator_not_found(exp->def.op.name, ecp);
                goto error;
            }
        }

        op = RDB_get_ro_op(exp->def.op.name, argc, argtv, envp, ecp, txp);
        if (op == NULL)
            goto error;
        if (op->rtyp == NULL) {
            RDB_raise_invalid_argument(exp->def.op.name, ecp);
            goto error;
        }
        typ = RDB_dup_nonscalar_type(op->rtyp, ecp);
        if (typ == NULL)
            goto error;
    }

    if (argc > 0)
        RDB_free(argtv);
    return typ;

error:
    if (argc > 0)
        RDB_free(argtv);
    return NULL;
}

static RDB_type *
aggr_type(const RDB_expression *exp, const RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (exp->kind != RDB_EX_RO_OP) {
        RDB_raise_invalid_argument("invalid summarize argument", ecp);
        return NULL;
    }

    if (strcmp(exp->def.op.name, "count") == 0) {
        return &RDB_INTEGER;
    } else if (strcmp(exp->def.op.name, "avg") == 0) {
        return &RDB_FLOAT;
    } else if (strcmp(exp->def.op.name, "sum") == 0
            || strcmp(exp->def.op.name, "max") == 0
            || strcmp(exp->def.op.name, "min") == 0) {
        if (exp->def.op.args.firstp == NULL
                || exp->def.op.args.firstp->nextp != NULL) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments", ecp);
            return NULL;
        }
        return RDB_expr_type_tpltyp(exp->def.op.args.firstp, tpltyp, NULL, NULL,
                NULL, ecp, txp);
    } else if (strcmp(exp->def.op.name, "any") == 0
            || strcmp(exp->def.op.name, "all") == 0) {
        return &RDB_BOOLEAN;
    }
    RDB_raise_operator_not_found(exp->def.op.name, ecp);
    return NULL;
}

struct gettypeinfo {
    RDB_getobjfn *getfnp;
    void *arg;
};

static RDB_type *
getobjtype(const char *name, void *arg)
{
    RDB_object *objp;
    struct gettypeinfo *infop = (struct gettypeinfo *) arg;
    if (infop == NULL)
        return NULL;
    objp = (*infop->getfnp)(name, infop->arg);
    if (objp == NULL)
        return NULL;
    return RDB_obj_type(objp);
}

int
RDB_check_expr_type(RDB_expression *exp, const RDB_type *tuptyp,
        const RDB_type *checktyp,
        RDB_getobjfn *getfnp, void *getarg,
        RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ;
    struct gettypeinfo gtinfo;

    if (getfnp != NULL) {
        gtinfo.getfnp = getfnp;
        gtinfo.arg = getarg;
    }
    typ = RDB_expr_type_tpltyp(exp, tuptyp,
            getfnp != NULL ? &getobjtype : NULL,
            &gtinfo,
            envp, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    if (!RDB_type_equals(typ, checktyp)) {
        RDB_raise_type_mismatch("", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

RDB_type *
RDB_summarize_type(RDB_expr_list *expsp,
        RDB_gettypefn *getfnp, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_type *newtyp;
    int addc, attrc;
    RDB_expression *argp;
    RDB_attr *attrv = NULL;
    RDB_type *tb1typ = NULL;
    RDB_type *tb2typ = NULL;
    int expc = RDB_expr_list_length(expsp);

    if (expc < 2 || (expc % 2) != 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    addc = (expc - 2) / 2;
    attrc = addc;

    tb1typ = RDB_expr_type(expsp->firstp, getfnp, getarg, NULL, ecp, txp);
    if (tb1typ == NULL)
        return NULL;
    tb2typ = RDB_expr_type(expsp->firstp->nextp, getfnp, getarg, NULL, ecp, txp);
    if (tb2typ == NULL)
        goto error;

    if (tb2typ->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("relation type required", ecp);
        goto error;
    }

    attrv = RDB_alloc(sizeof (RDB_attr) * attrc, ecp);
    if (attrv == NULL) {
        goto error;
    }

    argp = expsp->firstp->nextp->nextp;
    for (i = 0; i < addc; i++) {
        attrv[i].typ = aggr_type(argp, tb1typ->def.basetyp,
                ecp, txp);
        if (attrv[i].typ == NULL)
            goto error;
        if (argp->nextp->kind != RDB_EX_OBJ) {
            RDB_raise_invalid_argument("invalid SUMMARIZE argument", ecp);
            goto error;
        }
        attrv[i].name = RDB_obj_string(&argp->nextp->def.obj);
        if (attrv[i].name == NULL) {
            RDB_raise_invalid_argument("invalid SUMMARIZE argument", ecp);
            goto error;
        }
        argp = argp->nextp->nextp;
    }

    newtyp = RDB_extend_relation_type(tb2typ, attrc, attrv, ecp);
    if (newtyp == NULL) {
        goto error;
    }

    RDB_free(attrv);
    return newtyp;

error:
    RDB_free(attrv);
    return NULL;
}

/** @addtogroup expr
 * @{
 */

/**
 * Get the type of an expression. The type is managed by the expression.
 * After RDB_expr_type() has been called once, future calls will return the same type.
 *
 * If txp is NULL, envp will be used to look up operators from the cache.
 * If txp is not NULL, envp is ignored.
 *
 * @returns The type of the expression, or NULL on failure.
 */
RDB_type *
RDB_expr_type(RDB_expression *exp, RDB_gettypefn *getfnp, void *getarg,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ;

    if (exp->typ != NULL)
        return exp->typ;

    switch (exp->kind) {
    case RDB_EX_OBJ:
        /* Get type from RDB_object */
        typ = RDB_obj_type(&exp->def.obj);
        if (typ != NULL)
            return typ;

        /* No type available - generate type from tuple */
        if (exp->def.obj.kind == RDB_OB_TUPLE) {
            exp->typ = RDB_tuple_type(&exp->def.obj, ecp);
            if (exp->typ == NULL)
                return NULL;
        }
        if (exp->typ == NULL) {
            RDB_raise_not_found("missing type information", ecp);
            return NULL;
        }
        return exp->typ;
    case RDB_EX_TBP:
        return RDB_obj_type(exp->def.tbref.tbp);
    case RDB_EX_VAR:
        exp->typ = var_type(RDB_expr_var_name(exp), getfnp, getarg, ecp, txp);
        return exp->typ;
    case RDB_EX_RO_OP:
        exp->typ = expr_op_type(exp, getfnp, getarg, envp, ecp, txp);
        return exp->typ;
    }
    abort();
} /* RDB_expr_type */

/*@}*/
