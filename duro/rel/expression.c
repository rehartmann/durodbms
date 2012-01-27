/*
 * $Id$
 *
 * Copyright (C) 2004-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "transform.h"
#include "internal.h"
#include "catalog.h"
#include <gen/strfns.h>

#include <string.h>
#include <assert.h>

struct chained_type_map {
    void *arg1;
    RDB_gettypefn *getfn1p;
    void *arg2;
    RDB_gettypefn *getfn2p;
};

/** @defgroup expr Expression functions
 * @{
 */

/**
 * RDB_expr_is_const returns if the expression is a constant expression.

@returns

RDB_TRUE if the expression is a constant expression, RDB_FALSE otherwise.
 */
RDB_bool
RDB_expr_is_const(const RDB_expression *exp)
{
    RDB_expression *argp;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            return RDB_TRUE;
        case RDB_EX_RO_OP:
            argp = exp->var.op.args.firstp;
            while (argp != NULL) {
                if (!RDB_expr_is_const(argp))
                    return RDB_FALSE;
                argp = argp->nextp;
            }
            return RDB_TRUE;
        case RDB_EX_GET_COMP:
            return RDB_expr_is_const(exp->var.op.args.firstp);
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
    RDB_expression *argp;
    RDB_type *typ = NULL;
    int argc = RDB_expr_list_length(&exp->var.op.args);

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

    argp = exp->var.op.args.firstp->nextp;
    for (i = 0; i < wrapc; i++) {
        RDB_object *objp;

        wrapv[i].attrc =  RDB_array_length(&argp->var.obj, ecp);
        wrapv[i].attrv = RDB_alloc(sizeof (char *) * wrapv[i].attrc, ecp);
        if (wrapv[i].attrv == NULL)
            return NULL;
        for (j = 0; j < wrapv[i].attrc; j++) {
            objp = RDB_array_get(&argp->var.obj, (RDB_int) j, ecp);
            if (objp == NULL) {
                goto cleanup;
            }
            wrapv[i].attrv[j] = RDB_obj_string(objp);
        }
        argp = argp->nextp;
        wrapv[i].attrname = RDB_obj_string(&argp->var.obj);
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

/**
 * Replace all occurrences of variable names by expressions
 * Replcement is performed in-place if possible.
 */
int
RDB_expr_resolve_varname_expr(RDB_expression **expp, const char *varname,
        RDB_expression *texp, RDB_exec_context *ecp)
{
    RDB_expression *argp;

    switch ((*expp)->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return RDB_expr_resolve_varname_expr(&(*expp)->var.op.args.firstp,
                    varname, texp, ecp);
        case RDB_EX_RO_OP:
            argp = (*expp)->var.op.args.firstp;
            (*expp)->var.op.args.firstp = NULL;
            while (argp != NULL) {
                RDB_expression *nextp = argp->nextp;
                if (RDB_expr_resolve_varname_expr(&argp, varname, texp, ecp) != RDB_OK)
                    return RDB_ERROR;
                RDB_add_arg(*expp, argp);
                argp = nextp;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
            if (strcmp(varname, RDB_expr_var_name(*expp)) == 0) {
                RDB_expression *exp = RDB_dup_expr(texp, ecp);
                if (exp == NULL) {
                    return RDB_ERROR;
                }

                exp->nextp = (*expp)->nextp;
                RDB_drop_expr(*expp, ecp);
                *expp = exp;
            }
            return RDB_OK;
    }
    /* Never reached */
    abort();
}

/*
 * Return a new expression in which all variable names for which
 * *<var>getfnp</var>() or RDB_get_table() return an RDB_object
 * have been replaced by this object
 */
RDB_expression *
RDB_expr_resolve_varnames(RDB_expression *exp, RDB_getobjfn *getfnp,
        void *getdata, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *objp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            return RDB_tuple_attr(
                    RDB_expr_resolve_varnames(exp->var.op.args.firstp, getfnp, getdata, ecp, txp),
                            exp->var.op.name, ecp);
        case RDB_EX_GET_COMP:
            return RDB_expr_comp(
                    RDB_expr_resolve_varnames(exp->var.op.args.firstp, getfnp, getdata, ecp, txp),
                            exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp;
            RDB_expression *newexp;

            if (strcmp(exp->var.op.name, "RELATION") == 0) {
                int ret;

                newexp = RDB_obj_to_expr(NULL, ecp);
                if (newexp == NULL)
                    return NULL;

                /*
                 * Evaluate selector and store result
                 * !! becomes obsolete if RELATION is supported as virtual relvar
                 */
                ret = RDB_evaluate(exp, getfnp, getdata, ecp, txp, &newexp->var.obj);
                if (ret != RDB_OK) {
                    RDB_drop_expr(newexp, ecp);
                    return NULL;
                }
                return newexp;
            }

            newexp = RDB_ro_op(exp->var.op.name, ecp);
            if (newexp == NULL)
                return NULL;

            /* Perform resolve on all arguments */
            argp = exp->var.op.args.firstp;
            while (argp != NULL) {
                RDB_expression *argexp = RDB_expr_resolve_varnames(
                        argp, getfnp, getdata, ecp, txp);
                if (argexp == NULL) {
                    RDB_drop_expr(newexp, ecp);
                    return NULL;
                }
                RDB_add_arg(newexp, argexp);
                argp = argp->nextp;
            }

            return newexp;
        }
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj, ecp);
        case RDB_EX_TBP:
            return RDB_table_ref(exp->var.tbref.tbp, ecp);
        case RDB_EX_VAR:
            if (getfnp != (RDB_getobjfn *) NULL) {
                objp = (*getfnp)(exp->var.varname, getdata);
                if (objp != NULL) {
                    if (objp->kind == RDB_OB_TABLE && objp->var.tb.exp != NULL) {
                        return RDB_expr_resolve_varnames(objp->var.tb.exp,
                                getfnp, getdata, ecp, txp);
                    }
                    if (objp->kind == RDB_OB_TABLE)
                        return RDB_table_ref(objp, ecp);
                    return RDB_obj_to_expr(objp, ecp);
                }
            }
            if (txp != NULL) {
                objp = RDB_get_table(exp->var.varname, ecp, txp);
                if (objp != NULL) {
                    return RDB_table_ref(objp, ecp);
                }
                RDB_clear_err(ecp);
            }
                
            return RDB_var_ref(exp->var.varname, ecp);
    }
    abort();
} /* RDB_expr_resolve_varnames */

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
where_type(const RDB_expression *exp, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *condtyp;
    RDB_type *reltyp;

    if (RDB_expr_list_length(&exp->var.op.args) != 2) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    reltyp = RDB_expr_type(exp->var.op.args.firstp, getfnp, arg, ecp, txp);
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
        tmap.arg2 = reltyp->var.basetyp;
        tmap.getfn2p = get_tuple_attr_type;
        condtyp = RDB_expr_type(exp->var.op.args.lastp, get_chained_map_type,
                &tmap, ecp, txp);        
    } else {
        condtyp = RDB_expr_type(exp->var.op.args.lastp, get_tuple_attr_type,
                reltyp->var.basetyp, ecp, txp);
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
    int argc = RDB_expr_list_length(&exp->var.op.args);

    if (argc < 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    attrc = (argc - 1) / 2;
    arg1typ = RDB_expr_type(exp->var.op.args.firstp, getfnp, arg, ecp, txp);
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

    attrv = RDB_alloc(sizeof (RDB_attr) * attrc, ecp);
    if (attrv == NULL) {
        return NULL;
    }
    argp = exp->var.op.args.firstp->nextp;
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
                argp->var.obj.typ != &RDB_STRING) {
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        attrv[i].name = RDB_obj_string(&argp->var.obj);
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
    int argc = RDB_expr_list_length(&exp->var.op.args);

    if (argc == 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    if (argtv[0] == NULL) {
        RDB_raise_invalid_argument("Invalid PROJECT argument", ecp);
        return NULL;
    }

    argp = exp->var.op.args.firstp->nextp;
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
    argp = exp->var.op.args.firstp->nextp;
    for (i = 0; i < argc - 1; i++) {    
        attrv[i] = RDB_obj_string(&argp->var.obj);
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
    int argc = RDB_expr_list_length(&exp->var.op.args);

    if (argc == 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    if (argtv[0] == NULL) {
        RDB_raise_invalid_argument("Invalid UNWRAP argument", ecp);
        return NULL;
    }
    
    attrc = argc - 1;

    argp = exp->var.op.args.firstp->nextp;
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
    argp = exp->var.op.args.firstp->nextp;
    i = 0;
    while (argp != NULL) {
        assert(_RDB_expr_is_string(argp));
        attrv[i++] = RDB_obj_string(&argp->var.obj);
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
    int argc = RDB_expr_list_length(&exp->var.op.args);

    if (argc < 2) {
        RDB_raise_invalid_argument("invalid GROUP argument", ecp);
        return NULL;
    }

    if (argtv[0] == NULL || argtv[0]->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("Invalid GROUP argument", ecp);
        return NULL;
    }

    argp = exp->var.op.args.firstp->nextp;
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

    argp = exp->var.op.args.firstp->nextp;
    for (i = 1; i < argc - 1; i++) {
        attrv[i - 1] = RDB_obj_string(&argp->var.obj);
        argp = argp->nextp;
    }
    typ = RDB_group_type(argtv[0], argc - 2, attrv,
            RDB_obj_string(&exp->var.op.args.lastp->var.obj), ecp);
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
    int argc = RDB_expr_list_length(&exp->var.op.args);

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

    argp = exp->var.op.args.firstp;
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

    if (exp->var.op.args.firstp == NULL) {
        RDB_raise_not_supported("argument required for ARRAY", ecp);
        goto error;
    }
    basetyp = RDB_expr_type(exp->var.op.args.firstp, getfnp, arg, ecp, txp);
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

    if (exp->var.op.args.firstp == NULL) {
        RDB_raise_invalid_argument("argument required for RELATION", ecp);
        return NULL;
    }
    tpltyp = argtv[0];
    if (tpltyp == NULL) {
        /*
         * If the type of the first arg hasn't been passed in argtv[0],
         * get type of first expression
         */
        tpltyp = RDB_expr_type(exp->var.op.args.firstp, NULL, NULL, ecp, txp);
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
    int argc = RDB_expr_list_length(&exp->var.op.args);

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

    argp = exp->var.op.args.firstp->nextp;
    for (i = 0; i < renc; i++) {
        if (argp->kind != RDB_EX_OBJ || argp->var.obj.typ != &RDB_STRING) {
            RDB_free(renv);
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        if (argp->nextp->kind != RDB_EX_OBJ ||
                argp->nextp->var.obj.typ != &RDB_STRING) {
            RDB_raise_invalid_argument("STRING argument required", ecp);
            return NULL;
        }
        renv[i].from = RDB_obj_string(&argp->var.obj);
        renv[i].to = RDB_obj_string(&argp->nextp->var.obj);
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
    if (strcmp(exp->var.op.name, "WHERE") == 0) {
        return where_type(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "EXTEND") == 0) {
        return extend_type(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "SUMMARIZE") == 0) {
        return RDB_summarize_type(&exp->var.op.args, 0, NULL, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "TUPLE") == 0) {
        return tuple_type(exp, getfnp, arg, ecp, txp);
    }
    if (strcmp(exp->var.op.name, "ARRAY") == 0) {
        return array_type(exp, getfnp, arg, ecp, txp);
    }

    if (strcmp(exp->var.op.name, "REMOVE") == 0) {
        if (_RDB_remove_to_project(exp, getfnp, arg, ecp, txp) != RDB_OK)
            goto error;
    }

    argc = RDB_expr_list_length(&exp->var.op.args);

    /* Aggregate operators with attribute argument */
    if (strcmp(exp->var.op.name, "AVG") == 0) {
        RDB_type *argtyp;
        RDB_type *attrtyp;

        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }

        if (exp->var.op.args.firstp->nextp->kind != RDB_EX_VAR) {
            RDB_raise_invalid_argument("invalid aggregate", ecp);
            goto error;
        }
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_invalid_argument("aggregate requires relation argument",
                    ecp);
            goto error;
        }

        argtyp = RDB_expr_type(exp->var.op.args.firstp, getfnp, arg, ecp, txp);
        attrtyp = RDB_type_attr_type(argtyp,
                exp->var.op.args.firstp->nextp->var.varname);
        if (attrtyp != &RDB_INTEGER && attrtyp != &RDB_FLOAT
                && attrtyp != &RDB_FLOAT) {
            RDB_raise_type_mismatch("invalid attribute type", ecp);
            goto error;
        }
        return &RDB_FLOAT;
    } else if (strcmp(exp->var.op.name, "SUM") == 0
            || strcmp(exp->var.op.name, "MAX") == 0
            || strcmp(exp->var.op.name, "MIN") == 0) {
        RDB_type *argtyp;
        RDB_type *attrtyp;

        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }
        if (exp->var.op.args.firstp->nextp->kind != RDB_EX_VAR) {
            RDB_raise_invalid_argument("invalid aggregate", ecp);
            goto error;
        }

        argtyp = RDB_expr_type(exp->var.op.args.firstp, getfnp, arg, ecp, txp);
        attrtyp = RDB_type_attr_type(argtyp,
                exp->var.op.args.firstp->nextp->var.varname);
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
    } else if (strcmp(exp->var.op.name, "ANY") == 0
            || strcmp(exp->var.op.name, "ALL") == 0) {
        RDB_type *argtyp;
        RDB_type *attrtyp;

        if (argc != 2) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments",
                    ecp);
            goto error;
        }

        argtyp = RDB_expr_type(exp->var.op.args.firstp, getfnp, arg, ecp, txp);
        attrtyp = RDB_type_attr_type(argtyp,
                exp->var.op.args.firstp->nextp->var.varname);

        if (attrtyp != &RDB_BOOLEAN) {
            RDB_raise_type_mismatch("invalid attribute type", ecp);
            goto error;
        }
        return &RDB_BOOLEAN;
    }

    /*
     * Get argument types
     */
    argtv = RDB_alloc(sizeof (RDB_type *) * argc, ecp);
    if (argtv == NULL) {
        return NULL;
    }

    argp = exp->var.op.args.firstp;
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

    if (strcmp(exp->var.op.name, "COUNT") == 0
            && argc == 1) {
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("COUNT requires relation argument", ecp);
            goto error;
        }
        typ = &RDB_INTEGER;
    } else if (strcmp(exp->var.op.name, "RELATION") == 0) {
        typ = relation_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->var.op.name, "RENAME") == 0) {
        typ = rename_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->var.op.name, "WRAP") == 0) {
        typ = wrap_type(exp, argtv, ecp, txp);
    } else if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        typ = project_type(exp, argtv, ecp, txp);
    } else if (argc == 2
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
    } else if (strcmp(exp->var.op.name, "IS_EMPTY") == 0) {
        if (argc != 1) {
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
        if (argc != 2) {
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
        if (argc != 2 || argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid UNGROUP", ecp);
            goto error;
        }        
        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("UNGROUP requires table argument", ecp);
            goto error;
        }
        if (exp->var.op.args.firstp->nextp->kind != RDB_EX_OBJ
                || exp->var.op.args.firstp->nextp->var.obj.typ != &RDB_STRING) {
            RDB_raise_type_mismatch("invalid UNGROUP argument", ecp);
            goto error;
        }

        typ = RDB_ungroup_type(argtv[0],
                RDB_obj_string(&exp->var.op.args.firstp->nextp->var.obj), ecp);
    } else if (strcmp(exp->var.op.name, "JOIN") == 0) {
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
    } else if (strcmp(exp->var.op.name, "UNION") == 0
                || strcmp(exp->var.op.name, "MINUS") == 0
                || strcmp(exp->var.op.name, "SEMIMINUS") == 0
                || strcmp(exp->var.op.name, "INTERSECT") == 0
                || strcmp(exp->var.op.name, "SEMIJOIN") == 0) {
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
    } else if (strcmp(exp->var.op.name, "DIVIDE") == 0) {
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
    } else if (strcmp(exp->var.op.name, "TO_TUPLE") == 0) {
        if (argc != 1 || argtv[0] == NULL) {
            RDB_raise_invalid_argument("invalid TO_TUPLE",
                    ecp);
            goto error;
        }        

        if (argtv[0]->kind != RDB_TP_RELATION) {
            RDB_raise_type_mismatch("table argument required", ecp);
            goto error;
        }

        typ = RDB_dup_nonscalar_type(argtv[0]->var.basetyp, ecp);
    } else if (strcmp(exp->var.op.name, "LENGTH") == 0
            && argc == 1
            && (argtv[0] == NULL || argtv[0]->kind == RDB_TP_ARRAY)) {
        typ = &RDB_INTEGER;
    } else if (strcmp(exp->var.op.name, "[]") == 0
            && argc == 2
            && (argtv[0] == NULL || argtv[0]->kind == RDB_TP_ARRAY)) {
        typ = RDB_dup_nonscalar_type(argtv[0]->var.basetyp, ecp);
    } else {
        for (i = 0; i < argc; i++) {
            if (argtv[i] == NULL) {
                RDB_raise_operator_not_found(exp->var.op.name, ecp);
                goto error;
            }
        }

        op = _RDB_get_ro_op(exp->var.op.name, argc, argtv, ecp, txp);
        if (op == NULL)
            goto error;
        if (op->rtyp == NULL) {
            RDB_raise_invalid_argument(exp->var.op.name, ecp);
            goto error;
        }
        typ = RDB_dup_nonscalar_type(op->rtyp, ecp);
        if (typ == NULL)
            goto error;
    }

    RDB_free(argtv);
    return typ;

error:
    RDB_free(argtv);
    return NULL;
}

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
            typ = RDB_obj_type(&exp->var.obj);
            if (typ != NULL)
                return typ;

            /* No type available - generate type from tuple */
            if (exp->var.obj.kind == RDB_OB_TUPLE) {
                exp->typ = _RDB_tuple_type(&exp->var.obj, ecp);
                if (exp->typ == NULL)
                    return NULL;
            }
            if (exp->typ == NULL) {
                RDB_raise_not_found("missing type information", ecp);
                return NULL;
            }
            return exp->typ;
        case RDB_EX_TBP:
            return RDB_obj_type(exp->var.tbref.tbp);
        case RDB_EX_VAR:
            if (getfnp != NULL) {
                typ = (*getfnp) (RDB_expr_var_name(exp), getarg);
                if (typ != NULL) {
                    exp->typ = RDB_dup_nonscalar_type(typ, ecp);
                    return exp->typ;
                }
            }
            if (txp != NULL) {
                RDB_object *tbp = RDB_get_table(exp->var.varname, ecp, txp);
                if (tbp != NULL) {
                    exp->typ = RDB_dup_nonscalar_type(RDB_obj_type(tbp), ecp);
                    return exp->typ;
                }
            }
            RDB_raise_name(exp->var.varname, ecp);
            return NULL;
        case RDB_EX_TUPLE_ATTR:
            typ = RDB_expr_type(exp->var.op.args.firstp, getfnp, getarg, ecp, txp);
            if (typ == NULL)
                return NULL;
            typ = RDB_type_attr_type(typ, exp->var.op.name);
            if (typ == NULL) {
                RDB_raise_name(exp->var.varname, ecp);
                return NULL;
            }
            return typ;
        case RDB_EX_GET_COMP:
            typ = RDB_expr_type(exp->var.op.args.firstp, getfnp, getarg, ecp, txp);
            if (typ == NULL)
                return typ;
            attrp = _RDB_get_icomp(typ, exp->var.op.name);
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

/**
 * Return operator name for read-only operator expressions.
 * 
 * @returns The operator name if the expression is a read-only operator
 * invocation, or NULL if it is not.
 */
const char *
RDB_expr_op_name(const RDB_expression *expr)
{
    return expr->kind == RDB_EX_RO_OP ? expr->var.op.name : NULL;
}

/**
 * Return variable name for expressions referring to variables.
 * 
 * @returns The variable name if the expression is a read-only variable
 * reference, or NULL if it is not.
 */
const char *
RDB_expr_var_name(const RDB_expression *expr)
{
    return expr->kind == RDB_EX_VAR ? expr->var.varname : NULL;
}

static RDB_expression *
new_expr(RDB_exec_context *ecp) {
    RDB_expression *exp = RDB_alloc(sizeof (RDB_expression), ecp);
    if (exp == NULL) {
        return NULL;
    }
    exp->typ = NULL;
    exp->transformed = RDB_FALSE;
	return exp;
}

/**
 * RDB_bool_to_expr creates a constant expression of type BOOLEAN.

@returns    A pointer to the newly created expression, of NULL if the creation
failed.
 */
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

/**
 * RDB_int_to_expr creates a constant expression of type INTEGER.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
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

/**
 * RDB_float_to_expr creates a constant expression of type FLOAT.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
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

/**
 * RDB_string_to_expr creates a constant expression of type STRING.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
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
        RDB_free(exp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    exp->var.obj.var.bin.len = strlen(v) + 1;

    return exp;
}

/**
 * RDB_obj_to_expr creates a constant expression from a RDB_object.

Passing an <var>objp</var> of NULL is equivalent to passing
a pointer to a RDB_object which has been newly initialized
using RDB_init_obj().

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_obj_to_expr(const RDB_object *objp, RDB_exec_context *ecp)
{
    RDB_expression *exp = new_expr(ecp);
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_OBJ;
    RDB_init_obj(&exp->var.obj);
    if (objp != NULL) {        
        if (RDB_copy_obj(&exp->var.obj, objp, ecp) != RDB_OK) {
            RDB_free(exp);
            return NULL;
        }
        if (objp->typ != NULL) {
            exp->var.obj.typ = RDB_dup_nonscalar_type(objp->typ, ecp);
            if (exp->var.obj.typ == NULL)
                return NULL;
        }
    }
    return exp;
}

/**
 * RDB_table_ref creates an expression which refers to a table.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
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

/**
 * RDB_var_ref creates an expression that refers to a variable.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
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
        RDB_free(exp);
        return NULL;
    }
    
    return exp;
}

/**
 * RDB_ro_op creates an expression which represents the invocation
of a readonly operator.

Use RDB_add_arg() to add arguments.

@returns

On success, a pointer to the newly created expression is returned.
If the expression could not be created due to insufficient memory,
NULL is returned and an error is left in *<var>ecp</var>.
 */
RDB_expression *
RDB_ro_op(const char *opname, RDB_exec_context *ecp)
{
    RDB_expression *exp;

    exp = new_expr(ecp);
    if (exp == NULL) {
        return NULL;
    }

    exp->kind = RDB_EX_RO_OP;
    
    exp->var.op.name = RDB_dup_str(opname);
    if (exp->var.op.name == NULL) {
        RDB_free(exp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    exp->var.op.args.firstp = exp->var.op.args.lastp = NULL;

    exp->var.op.optinfo.objpc = 0;

    return exp;
}

/**
 * RDB_add_arg adds the child expression *<var>argp</var>
to the expression *<var>exp</var>.
*<var>exp</var> must represent a read-only operator invocation.
RDB_ro_op() should be used to create
such an expression.

To obtain a valid expression representing a read-only
operator invocation, RDB_add_arg must be called once
for each argument of the operator.
 */
void
RDB_add_arg(RDB_expression *exp, RDB_expression *argp)
{
    RDB_expr_list_append(&exp->var.op.args, argp);
    exp->transformed = RDB_FALSE;
}

/**
 * RDB_eq creates an expression that represents an "is equal" operator.
If one of the arguments is NULL, NULL is returned.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
RDB_expression *
RDB_eq(RDB_expression *arg1, RDB_expression *arg2, RDB_exec_context *ecp)
{
    RDB_expression *exp = RDB_ro_op("=", ecp);
    if (exp == NULL)
        return NULL;

    RDB_add_arg(exp, arg1);
    RDB_add_arg(exp, arg2);
    return exp;
}

/**
 * RDB_tuple_attr creates an expression that represents a tuple attribute
extraction.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
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

/**
 * RDB_expr_comp creates an expression which evaluates to a
possible representation component.

@returns

A pointer to the newly created expression, of NULL if the creation
failed.
 */
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
    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            if (RDB_drop_expr(exp->var.op.args.firstp, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        case RDB_EX_RO_OP:
            if (RDB_destroy_expr_list(&exp->var.op.args, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        default: ;
    }
    return RDB_OK;
}

/**
 * RDB_drop_expr destroys the expression specified to by <var>exp</var>
(including all subexpressions) and frees all resources associated with it.

@returns

RDB_OK on success, RDB_ERROR on failure.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_drop_expr(RDB_expression *exp, RDB_exec_context *ecp)
{
    int ret;

    if (drop_children(exp, ecp) != RDB_OK)
        return RDB_ERROR;
    ret = _RDB_destroy_expr(exp, ecp);
    RDB_free(exp);
    return ret;
}

/**
 * Create a copy of an expression.
 */
RDB_expression *
RDB_dup_expr(const RDB_expression *exp, RDB_exec_context *ecp)
{
    RDB_expression *newexp;
    RDB_expression *argp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = RDB_dup_expr(exp->var.op.args.firstp, ecp);
            if (newexp == NULL)
                return NULL;
            return RDB_tuple_attr(newexp, exp->var.op.name, ecp);
        case RDB_EX_GET_COMP:
            newexp = RDB_dup_expr(exp->var.op.args.firstp, ecp);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_comp(newexp, exp->var.op.name, ecp);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argdp;

            newexp = RDB_ro_op(exp->var.op.name, ecp);
            if (newexp == NULL)
                return NULL;

            /*
             * Duplicate child expressions and append them to new expression
             */
            argp = exp->var.op.args.firstp;
            while (argp != NULL) {
                argdp = RDB_dup_expr(argp, ecp);
                if (argdp == NULL)
                    return NULL;
                RDB_add_arg(newexp, argdp);
                argp = argp->nextp;
            }

            /*
             * If the expression is RELATION {}, duplicate the type if available
             * because otherwise it would be impossible to determine the type
             * of the copy.
             */
            if (exp->var.op.args.firstp == NULL && exp->typ != NULL
                    && RDB_type_is_relation(exp->typ)
                    && strcmp(exp->var.op.name, "RELATION") == 0) {
                newexp->typ = RDB_dup_nonscalar_type(exp->typ, ecp);
                if (newexp->typ == NULL) {
                    RDB_drop_expr(newexp, ecp);
                    return NULL;
                }
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

/**
 * RDB_expr_obj returns a pointer to RDB_object embedded in an expression.

@returns

A pointer to the embedded RDB_object or NULL if the expression does
not represent a RDB_object.
 */
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

/**
 * Set the type of expression *<var>exp</var> to *<var>typ</var>.
 * Only used for special purposes, such as specifying the type of relation selectors.
 */
void
RDB_set_expr_type(RDB_expression *exp, RDB_type *typ)
{
    exp->typ = typ;
}

/*@}*/

RDB_bool
_RDB_expr_is_string(const RDB_expression *exp)
{
    return exp->kind == RDB_EX_OBJ && exp->var.obj.typ == &RDB_STRING;
}

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

/**
 * Initialize empty expression list.
 */
void
RDB_init_expr_list(RDB_expr_list *explistp)
{
    explistp->firstp = explistp->lastp = NULL;
}

/**
 * Drop all expressions in the list.
 */
int
RDB_destroy_expr_list(RDB_expr_list *explistp, RDB_exec_context *ecp)
{
    int ret = RDB_OK;
    RDB_expression *nexp;
    RDB_expression *exp = explistp->firstp;
    while (exp != NULL) {
        nexp = exp->nextp;
        if (RDB_drop_expr(exp, ecp) != RDB_OK)
            ret = RDB_ERROR;
        exp = nexp;
    }
    return ret;
}

RDB_int
RDB_expr_list_length(const RDB_expr_list *explistp)
{
    int len = 0;
    RDB_expression *exp = explistp->firstp;
    while (exp != NULL) {
        ++len;
        exp = exp->nextp;
    }
    return (RDB_int) len;
}

void
RDB_expr_list_append(RDB_expr_list *explistp, RDB_expression *exp)
{
    exp->nextp = NULL;
    if (explistp->firstp == NULL) {
        /* It's the first argument */
        explistp->firstp = explistp->lastp = exp;
    } else {
        explistp->lastp->nextp = exp;
        explistp->lastp = exp;
    }
}

void
_RDB_expr_list_set_lastp(RDB_expr_list *explistp)
{
    RDB_expression *exp = explistp->firstp;
    if (exp == NULL) {
        explistp->lastp = NULL;
        return;
    }
    while (exp->nextp != NULL)
        exp = exp->nextp;
    explistp->lastp = exp;
}

void
RDB_join_expr_lists(RDB_expr_list *explist1p, RDB_expr_list *explist2p)
{
    if (explist1p->firstp == NULL) {
        explist1p->firstp = explist2p->firstp;
    } else {
        explist1p->lastp->nextp = explist2p->firstp;
    }
    explist1p->lastp = explist2p->lastp;
    explist2p->firstp = explist2p->lastp = NULL;
}

/**
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
            RDB_free(exp->var.op.name);
            break;
        case RDB_EX_RO_OP:
            RDB_free(exp->var.op.name);
            if (exp->var.op.optinfo.objpc > 0)
                RDB_free(exp->var.op.optinfo.objpv);
            break;
        case RDB_EX_VAR:
            RDB_free(exp->var.varname);
            break;
    }
    if (exp->typ != NULL && !RDB_type_is_scalar(exp->typ))
        return RDB_drop_type(exp->typ, ecp, NULL);
    return RDB_OK;
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
            return _RDB_expr_expr_depend(ex1p->var.op.args.firstp, ex2p);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp = ex1p->var.op.args.firstp;
            while (argp != NULL) {
                if (_RDB_expr_expr_depend(argp, ex2p))
                    return RDB_TRUE;
                argp = argp->nextp;
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
            return _RDB_expr_refers(exp->var.op.args.firstp, tbp);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp = exp->var.op.args.firstp;
            while (argp != NULL) {
                if (_RDB_expr_refers(argp, tbp))
                    return RDB_TRUE;
                argp = argp->nextp;
            }
            
            return RDB_FALSE;
        }
    }
    /* Should never be reached */
    abort();
}

RDB_bool
_RDB_expr_refers_var(const RDB_expression *exp, const char *attrname)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_FALSE;
        case RDB_EX_VAR:
            return (RDB_bool) (strcmp(exp->var.varname, attrname) == 0);
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_expr_refers_var(exp->var.op.args.firstp, attrname);
        case RDB_EX_RO_OP:
        {
            RDB_expression *argp = exp->var.op.args.firstp;
            while (argp != NULL) {
                if (_RDB_expr_refers_var(argp, attrname))
                    return RDB_TRUE;
                argp = argp->nextp;
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
_RDB_tpl_get(const char *name, void *arg)
{
    return RDB_tuple_get((RDB_object *) arg, name);
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

    exp->kind = kind;
    exp->var.op.args.firstp = exp->var.op.args.lastp = arg;
    arg->nextp = NULL;

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
        
    exp->kind = kind;
    exp->var.op.args.firstp = arg1;
    exp->var.op.args.lastp = arg2;
    arg1->nextp = arg2;
    arg2->nextp = NULL;

    return exp;
}

int
_RDB_check_expr_type(RDB_expression *exp, const RDB_type *tuptyp,
        const RDB_type *checktyp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ = _RDB_expr_type(exp, tuptyp, ecp, txp);
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
    RDB_expression *arg1p, *arg2p;

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
            ret = _RDB_expr_equals(ex1p->var.op.args.firstp,
                    ex2p->var.op.args.firstp, ecp, txp, resp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            *resp = (RDB_bool)
                    (strcmp (ex1p->var.op.name, ex2p->var.op.name) == 0);
            break;
        case RDB_EX_RO_OP:
            if (RDB_expr_list_length(&ex1p->var.op.args)
                    != RDB_expr_list_length(&ex2p->var.op.args)
                    || strcmp(ex1p->var.op.name, ex2p->var.op.name) != 0) {
                *resp = RDB_FALSE;
                return RDB_OK;
            }
            arg1p = ex1p->var.op.args.firstp;
            arg2p = ex2p->var.op.args.firstp;
            while (arg1p != NULL) {
                ret = _RDB_expr_equals(arg1p, arg2p, ecp, txp, resp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
                if (!*resp)
                    return RDB_OK;
                arg1p = arg1p->nextp;
                arg2p = arg2p->nextp;
            }
            *resp = RDB_TRUE;
            break;
    }
    return RDB_OK;
}

int
_RDB_invrename_expr(RDB_expression *exp, RDB_expression *texp,
        RDB_exec_context *ecp)
{
    RDB_expression *argp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_invrename_expr(exp->var.op.args.firstp, texp, ecp);
        case RDB_EX_RO_OP:
            argp = exp->var.op.args.firstp;
            while (argp != NULL) {
                if (_RDB_invrename_expr(argp, texp, ecp) != RDB_OK)
                    return RDB_ERROR;
                argp = argp->nextp;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
            /* Search attribute name in dest attrs */
            argp = texp->var.op.args.firstp->nextp;
            while (argp != NULL) {
                RDB_expression *toargp = argp->nextp;
                if (strcmp(RDB_obj_string(&toargp->var.obj),
                        exp->var.varname) == 0)
                    break;
                argp = toargp->nextp;
            }

            /* If found, replace it */
            if (argp != NULL) {
                char *from = RDB_obj_string(&argp->var.obj);
                char *name = RDB_realloc(exp->var.varname, strlen(from) + 1, ecp);
                if (name == NULL) {
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
_RDB_resolve_exprnames(RDB_expression **expp, RDB_expression *texp,
        RDB_exec_context *ecp)
{
    RDB_expression *argp;

    switch ((*expp)->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_resolve_exprnames(&(*expp)->var.op.args.firstp,
                    texp, ecp);
        case RDB_EX_RO_OP:
            argp = (*expp)->var.op.args.firstp;
            (*expp)->var.op.args.firstp = NULL;
            while (argp != NULL) {
                RDB_expression *nextp = argp->nextp;
                if (_RDB_resolve_exprnames(&argp, texp, ecp) != RDB_OK)
                    return RDB_ERROR;
                RDB_add_arg(*expp, argp);
                argp = nextp;
            }
            return RDB_OK;
        case RDB_EX_OBJ:
        case RDB_EX_TBP:
            return RDB_OK;
        case RDB_EX_VAR:
            /* Search attribute name in attrs */
            argp = texp;
            while (argp != NULL) {
                if (strcmp(RDB_obj_string(&argp->nextp->var.obj),
                        (*expp)->var.varname) == 0)
                    break;
                argp = argp->nextp->nextp;
            }

            if (argp != NULL) {
                RDB_expression *exp = RDB_dup_expr(argp, ecp);
                if (exp == NULL) {
                    return RDB_ERROR;
                }

                exp->nextp = (*expp)->nextp;
                RDB_drop_expr(*expp, ecp);
                *expp = exp;
            }
            return RDB_OK;
    }
    abort();
}

/**
 * Returns if *exp is an invocation of operator *opname whose first
 * argument is a reference to *attrname.
 */
static RDB_bool
expr_var(RDB_expression *exp, const char *attrname, char *opname)
{
    if (exp->kind == RDB_EX_RO_OP && strcmp(exp->var.op.name, opname) == 0) {
        if (exp->var.op.args.firstp->kind == RDB_EX_VAR
                && strcmp(exp->var.op.args.firstp->var.varname, attrname) == 0
                && exp->var.op.args.firstp->nextp->kind == RDB_EX_OBJ)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

/**
 * Find term which is an invocation of operator *opname whose first
 * argument is a reference to *attrname.
 */
RDB_expression *
_RDB_attr_node(RDB_expression *exp, const char *attrname, char *opname)
{
    while (exp->kind == RDB_EX_RO_OP
            && strcmp (exp->var.op.name, "AND") == 0) {
        if (expr_var(exp->var.op.args.firstp->nextp, attrname, opname))
            return exp;
        exp = exp->var.op.args.firstp;
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
    RDB_type *typ = _RDB_expr_type(exp, NULL, ecp, txp);
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
