/*
 * $Id$
 *
 *  Created on: 31.01.2012
 *
 */

#include "rdb.h"
#include "transform.h"
#include "internal.h"
#include <gen/strfns.h>

#include <string.h>
#include <assert.h>

struct chained_type_map {
    void *arg1;
    RDB_gettypefn *getfn1p;
    void *arg2;
    RDB_gettypefn *getfn2p;
};

static RDB_type *
get_tuple_attr_type(const char *attrname, void *arg)
{
    RDB_attr *attrp = _RDB_tuple_type_attr(arg, attrname);
    return attrp != NULL ? attrp->typ : NULL;
}

static RDB_type *
get_chained_map_type(const char *attrname, void *arg) {
    struct chained_type_map *mapp = arg;
    RDB_type *typ = (*mapp->getfn1p) (attrname, mapp->arg1);
    if (typ != NULL)
        return typ;

    return (*mapp->getfn2p) (attrname, mapp->arg2);
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
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *condtyp;
    RDB_type *reltyp;

    if (RDB_expr_list_length(&exp->def.op.args) != 2) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    reltyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, ecp, txp);
    if (reltyp == NULL)
        return NULL;

    if (reltyp->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("WHERE requires relation argument", ecp);
        return NULL;
    }
    if (arg != NULL) {
        struct chained_type_map tmap;
        tmap.arg1 = arg;
        tmap.getfn1p = getfnp;
        tmap.arg2 = reltyp->def.basetyp;
        tmap.getfn2p = get_tuple_attr_type;
        condtyp = RDB_expr_type(exp->def.op.args.lastp, get_chained_map_type,
                &tmap, ecp, txp);
    } else {
        condtyp = RDB_expr_type(exp->def.op.args.lastp, get_tuple_attr_type,
                reltyp->def.basetyp, ecp, txp);
    }
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
extend_type(const RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
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
    arg1typ = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, ecp, txp);
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
        attrv[i].typ = _RDB_expr_type(argp, tpltyp, ecp, txp);
        if (attrv[i].typ == NULL) {
            int j;

            RDB_free(attrv);
            for (j = 0; i < j; j++) {
                if (!RDB_type_is_scalar(attrv[j].typ)) {
                    RDB_drop_type(attrv[j].typ, ecp, NULL);
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
    char **attrv;
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
        if (!_RDB_expr_is_string(argp)) {
            RDB_raise_type_mismatch("PROJECT requires STRING arguments",
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
        if (!_RDB_expr_is_string(argp)) {
            RDB_raise_type_mismatch(
                    "UNWRAP argument must be STRING", ecp);
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
        assert(_RDB_expr_is_string(argp));
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
        if (!_RDB_expr_is_string(argp)) {
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
        RDB_exec_context *ecp, RDB_transaction *txp)
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
        attrv[i].typ = RDB_expr_type(argp, getfnp, arg, ecp, txp);
        if (attrv[i].typ == NULL)
            goto cleanup;

        argp = argp->nextp;
    }

    typ = RDB_create_tuple_type(attrc, attrv, ecp);

cleanup:
    RDB_free(attrv);
    return typ;
}

static RDB_type *
array_type(const RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *rtyp = NULL;
    RDB_type *basetyp;

    if (exp->def.op.args.firstp == NULL) {
        RDB_raise_internal("argument required for ARRAY", ecp);
        goto error;
    }
    basetyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, ecp, txp);
    if (basetyp == NULL) {
        RDB_raise_invalid_argument("type required for ARRAY argument", ecp);
        goto error;
    }
    basetyp = RDB_dup_nonscalar_type(basetyp, ecp);
    if (basetyp == NULL)
        return NULL;

    rtyp = RDB_create_array_type(basetyp, ecp);
    if (rtyp == NULL) {
        if (!RDB_type_is_scalar(basetyp))
            RDB_drop_type(basetyp, ecp, NULL);
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
        RDB_exec_context *ecp, RDB_transaction *txp)
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
        tpltyp = RDB_expr_type(exp->def.op.args.firstp, NULL, NULL, ecp, txp);
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
    rtyp = RDB_create_relation_type_from_base(basetyp, ecp);
    if (rtyp == NULL) {
        RDB_drop_type(basetyp, ecp, NULL);
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
expr_op_type(RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_type *typ;
    RDB_operator *op;
    RDB_expression *argp;
    int argc;
    RDB_type **argtv = NULL;

    if (_RDB_transform(exp, getfnp, arg, ecp, txp) != RDB_OK)
        return NULL;

    /*
     * WHERE, EXTEND, etc. require special treatment
     */
    if (strcmp(exp->def.op.name, "WHERE") == 0) {
        return where_type(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "EXTEND") == 0) {
        return extend_type(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "SUMMARIZE") == 0) {
        return RDB_summarize_type(&exp->def.op.args, 0, NULL, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "TUPLE") == 0) {
        return tuple_type(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "ARRAY") == 0) {
        return array_type(exp, getfnp, arg, ecp, txp);
    }

    if (strcmp(exp->def.op.name, "REMOVE") == 0) {
        if (_RDB_remove_to_project(exp, getfnp, arg, ecp, txp) != RDB_OK)
            goto error;
    }

    argc = RDB_expr_list_length(&exp->def.op.args);

    /* Aggregate operators with attribute argument */
    if (strcmp(exp->def.op.name, "AVG") == 0) {
        RDB_type *argtyp;
        RDB_type *attrtyp;

        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }

        if (exp->def.op.args.firstp->nextp->kind != RDB_EX_VAR) {
            RDB_raise_invalid_argument("invalid aggregate", ecp);
            goto error;
        }
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("aggregate requires relation argument",
                    ecp);
            goto error;
        }

        argtyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, ecp, txp);
        attrtyp = RDB_type_attr_type(argtyp,
                exp->def.op.args.firstp->nextp->def.varname);
        if (attrtyp != &RDB_INTEGER && attrtyp != &RDB_FLOAT
                && attrtyp != &RDB_FLOAT) {
            RDB_raise_type_mismatch("invalid attribute type", ecp);
            goto error;
        }
        return &RDB_FLOAT;
    } else if (strcmp(exp->def.op.name, "SUM") == 0
            || strcmp(exp->def.op.name, "MAX") == 0
            || strcmp(exp->def.op.name, "MIN") == 0) {
        RDB_type *argtyp;
        RDB_type *attrtyp;

        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }
        if (exp->def.op.args.firstp->nextp->kind != RDB_EX_VAR) {
            RDB_raise_invalid_argument("invalid aggregate", ecp);
            goto error;
        }

        argtyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, ecp, txp);
        attrtyp = RDB_type_attr_type(argtyp,
                exp->def.op.args.firstp->nextp->def.varname);
        if (argtyp->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("aggregate requires relation argument",
                    ecp);
            goto error;
        }
        if (attrtyp != &RDB_INTEGER && attrtyp != &RDB_FLOAT
                && attrtyp != &RDB_FLOAT) {
            RDB_raise_type_mismatch("invalid attribute type", ecp);
            goto error;
        }
        return attrtyp;
    } else if (strcmp(exp->def.op.name, "ANY") == 0
            || strcmp(exp->def.op.name, "ALL") == 0) {
        RDB_type *argtyp;
        RDB_type *attrtyp;

        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }

        argtyp = RDB_expr_type(exp->def.op.args.firstp, getfnp, arg, ecp, txp);
        attrtyp = RDB_type_attr_type(argtyp,
                exp->def.op.args.firstp->nextp->def.varname);

        if (attrtyp != &RDB_BOOLEAN) {
            RDB_raise_type_mismatch("invalid attribute type", ecp);
            goto error;
        }
        return &RDB_BOOLEAN;
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
        argtv[i] = RDB_expr_type(argp, getfnp, arg, ecp, txp);
        if (argtv[i] == NULL) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                goto error;
            RDB_clear_err(ecp);
        }
        argp = argp->nextp;
    }

    if (strcmp(exp->def.op.name, "COUNT") == 0
            && argc == 1) {
        if (argc == 1 && argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("COUNT requires relation argument", ecp);
            goto error;
        }
        typ = &RDB_INTEGER;
    } else if (strcmp(exp->def.op.name, "RELATION") == 0) {
        typ = relation_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "RENAME") == 0) {
        typ = rename_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "WRAP") == 0) {
        typ = wrap_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "PROJECT") == 0) {
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
    } else if (strcmp(exp->def.op.name, "IF") == 0) {
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
    } else if (strcmp(exp->def.op.name, "IS_EMPTY") == 0) {
        if (argc != 1) {
            RDB_raise_invalid_argument("invalid number of arguments", ecp);
            goto error;
        }

        if (argtv[0] == NULL || argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("IS_EMPTY requires table argument", ecp);
            goto error;
        }

        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->def.op.name, "IN") == 0
                    || strcmp(exp->def.op.name, "SUBSET_OF") == 0) {
        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of arguments", ecp);
            goto error;
        }

        if (argtv[1] == NULL || argtv[1]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("table argument required", ecp);
            goto error;
        }

        typ = &RDB_BOOLEAN;
    } else if (strcmp(exp->def.op.name, "UNWRAP") == 0) {
        typ = unwrap_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "GROUP") == 0) {
        typ = group_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->def.op.name, "UNGROUP") == 0) {
        if (argc != 2 || argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid UNGROUP", ecp);
            goto error;
        }
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("UNGROUP requires table argument", ecp);
            goto error;
        }
        if (exp->def.op.args.firstp->nextp->kind != RDB_EX_OBJ
                || exp->def.op.args.firstp->nextp->def.obj.typ != &RDB_STRING) {
            RDB_raise_type_mismatch("invalid UNGROUP argument", ecp);
            goto error;
        }

        typ = RDB_ungroup_type(argtv[0],
                RDB_obj_string(&exp->def.op.args.firstp->nextp->def.obj), ecp);
    } else if (strcmp(exp->def.op.name, "JOIN") == 0) {
        if (argc != 2 || argtv[0] == NULL
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
    } else if (strcmp(exp->def.op.name, "UNION") == 0) {
        if (argc != 2 || argtv[0] == NULL
                || argtv[1] == NULL) {
            RDB_raise_invalid_argument("invalid relational operator invocation",
                    ecp);
            goto error;
        }

        if ((RDB_type_is_relation(argtv[0]) && RDB_type_is_relation(argtv[1]))) {
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
    } else if (strcmp(exp->def.op.name, "MINUS") == 0
                || strcmp(exp->def.op.name, "SEMIMINUS") == 0
                || strcmp(exp->def.op.name, "INTERSECT") == 0
                || strcmp(exp->def.op.name, "SEMIJOIN") == 0) {
        if (argc != 2 || argtv[0] == NULL
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
        RDB_free(argtv);
        return typ;
    } else if (strcmp(exp->def.op.name, "DIVIDE") == 0) {
        if (argc != 3 || argtv[0] == NULL
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
        RDB_free(argtv);
        return typ;
    } else if (strcmp(exp->def.op.name, "TO_TUPLE") == 0) {
        if (argc != 1 || argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid TO_TUPLE",
                    ecp);
            goto error;
        }

        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("table argument required", ecp);
            goto error;
        }

        typ = RDB_dup_nonscalar_type(argtv[0]->def.basetyp, ecp);
    } else if (strcmp(exp->def.op.name, "LENGTH") == 0
            && argc == 1
            && (argtv[0] == NULL || argtv[0]->kind == RDB_TP_ARRAY)) {
        typ = &RDB_INTEGER;
    } else if (strcmp(exp->def.op.name, "[]") == 0
            && argc == 2
            && (argtv[0] == NULL || argtv[0]->kind == RDB_TP_ARRAY)) {
        typ = RDB_dup_nonscalar_type(argtv[0]->def.basetyp, ecp);
    } else {
        for (i = 0; i < argc; i++) {
            if (argtv[i] == NULL) {
                RDB_raise_operator_not_found(exp->def.op.name, ecp);
                goto error;
            }
        }

        op = _RDB_get_ro_op(exp->def.op.name, argc, argtv, NULL, ecp, txp);
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
var_expr_type(RDB_expression *exp, RDB_gettypefn *getfnp, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ;
    RDB_object *errp;

    if (getfnp != NULL) {
        typ = (*getfnp) (RDB_expr_var_name(exp), getarg);
        if (typ != NULL) {
            exp->typ = RDB_dup_nonscalar_type(typ, ecp);
            return exp->typ;
        }
    }
    if (txp != NULL) {
        RDB_object *tbp = RDB_get_table(exp->def.varname, ecp, txp);
        if (tbp != NULL) {
            exp->typ = RDB_dup_nonscalar_type(RDB_obj_type(tbp), ecp);
            return exp->typ;
        }
    }

    /*
     * Handle error - if no error or NOT_FOUND_ERROR has been raised raise NAME_ERROR
     */
    errp = RDB_get_err(ecp);
    if (errp == NULL || RDB_obj_type(errp) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        RDB_raise_name(exp->def.varname, ecp);
    }
    return NULL;
}

/** @addtogroup expr
 * @{
 */

/**
 * Get the type of an expression. The type is managed by the expression.
 * After RDB_expr_type() has been called once, future calls will return the same type.
 *
 * @returns The type of the expression, or NULL on failure.
 */
RDB_type *
RDB_expr_type(RDB_expression *exp, RDB_gettypefn *getfnp, void *getarg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_attr *attrp;
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
                exp->typ = _RDB_tuple_type(&exp->def.obj, ecp);
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
            return var_expr_type(exp, getfnp, getarg, ecp, txp);
        case RDB_EX_TUPLE_ATTR:
            typ = RDB_expr_type(exp->def.op.args.firstp, getfnp, getarg, ecp, txp);
            if (typ == NULL)
                return NULL;
            typ = RDB_type_attr_type(typ, exp->def.op.name);
            if (typ == NULL) {
                RDB_raise_name(exp->def.varname, ecp);
                return NULL;
            }
            return typ;
        case RDB_EX_GET_COMP:
            typ = RDB_expr_type(exp->def.op.args.firstp, getfnp, getarg, ecp, txp);
            if (typ == NULL)
                return typ;
            attrp = _RDB_get_icomp(typ, exp->def.op.name);
            if (attrp == NULL) {
                RDB_raise_invalid_argument("component not found", ecp);
                return NULL;
            }
            return attrp->typ;
        case RDB_EX_RO_OP:
            exp->typ = expr_op_type(exp, getfnp, getarg, ecp, txp);
            return exp->typ;
    }
    abort();
} /* RDB_expr_type */

/*@}*/

/**
 * Return the type of the expression. Use *tpltyp to resolve refs to variables.
 * If the type is non-scalar, it is managed by the expression.
 */
RDB_type *
_RDB_expr_type(RDB_expression *exp, const RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    return RDB_expr_type(exp, &get_tuple_attr_type, (RDB_type *) tpltyp,
            ecp, txp);
}
