/*
 * $Id$
 *
 *  Created on: 01.01.2012
 */

#include "rdb.h"
#include "internal.h"
#include "transform.h"
#include "tostr.h"
#include <string.h>

/*
 * Evaluate an expression by converting it to a virtual table.
 */
static int
evaluate_vt(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    /*
     * Resolve variable names, if any
     */
    RDB_expression *nexp = getfnp != NULL
            ? RDB_expr_resolve_varnames(exp, getfnp, getdata, ecp, txp)
            : RDB_dup_expr(exp, ecp);
    if (nexp == NULL)
        return RDB_ERROR;

    if (RDB_vtexp_to_obj(nexp, ecp, txp, retvalp) != RDB_OK) {
        RDB_del_expr(nexp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/*
 *  Evaluate IF expression. There must be 3 arguments.
 */
static int
evaluate_if(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    RDB_object arg1;

    RDB_init_obj(&arg1);

    if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp, ecp, txp, &arg1)
            != RDB_OK) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (RDB_obj_type(&arg1) != &RDB_BOOLEAN) {
        RDB_raise_type_mismatch("BOOLEAN required", ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (RDB_obj_bool(&arg1)) {
        ret = RDB_evaluate(exp->def.op.args.firstp->nextp, getfnp, getdata,
                envp, ecp, txp, valp);
    } else {
        ret = RDB_evaluate(exp->def.op.args.firstp->nextp->nextp, getfnp, getdata,
                envp, ecp, txp, valp);
    }

cleanup:
    RDB_destroy_obj(&arg1, ecp);
    return ret;
}

struct get_type_info {
    RDB_getobjfn *getfnp;
    void *getdata;
};

static RDB_type *
get_type(const char *name, void *arg)
{
    struct get_type_info *infop = arg;
    RDB_object *objp = (*infop->getfnp) (name, infop->getdata);
    if (objp == NULL)
        return NULL;
    return objp->typ;
}

static int
evaluate_tuple_extend(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *valp)
{
    RDB_expression *argp;
    RDB_virtual_attr *attrv;
    int i;
    int ret;
    int attrc = (RDB_expr_list_length(&exp->def.op.args) - 1) / 2;

    attrv = RDB_alloc(sizeof (RDB_virtual_attr) * attrc, ecp);
    if (attrv == NULL) {
        return RDB_ERROR;
    }

    argp = exp->def.op.args.firstp->nextp;
    for (i = 0; i < attrc; i++) {
        attrv[i].exp = argp;

        if (!RDB_expr_is_string(argp->nextp)) {
            RDB_raise_type_mismatch("attribute argument must be string",
                    ecp);
            return RDB_ERROR;
        }
        attrv[i].name = RDB_obj_string(&argp->nextp->def.obj);
        argp = argp->nextp->nextp;
    }

    if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp, ecp,
            txp, valp) != RDB_OK) {
        RDB_free(attrv);
        return RDB_ERROR;
    }

    ret = RDB_extend_tuple(valp, attrc, attrv, ecp, txp);
    RDB_free(attrv);
    return ret;
}

static int
evaluate_ro_op(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *valp)
{
    int ret;
    int i;
    RDB_expression *argp;
    RDB_object tb;
    RDB_object **valpv;
    RDB_object *valv = NULL;
    int argc;
    struct get_type_info gtinfo;

    if (getfnp != NULL) {
        gtinfo.getdata = getdata;
        gtinfo.getfnp = getfnp;
    }

    if (RDB_transform(exp, getfnp != NULL ? get_type : NULL,
            getfnp != NULL ? &gtinfo : NULL, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    /*
     * Certain operators cannot be evaluated by evaluating the arguments
     * and calling an operator function, so they get special treatment
     */

    if (strcmp(exp->def.op.name, "extend") == 0) {
        RDB_type *typ = RDB_expr_type(exp->def.op.args.firstp, NULL, NULL,
                ecp, txp);
        if (typ == NULL)
            return RDB_ERROR;

        if (typ->kind == RDB_TP_RELATION) {
            return evaluate_vt(exp, getfnp, getdata, ecp, txp, valp);
        } else if (typ->kind == RDB_TP_TUPLE) {
            return evaluate_tuple_extend(exp, getfnp, getdata, envp, ecp, txp, valp);
        }
    }

    if (strcmp(exp->def.op.name, "where") == 0
            || strcmp(exp->def.op.name, "summarize") == 0) {
        return evaluate_vt(exp, getfnp, getdata, ecp, txp, valp);
    }

    argc = RDB_expr_list_length(&exp->def.op.args);

    /*
     * Handle aggregate functions except COUNT
     * First check if there are 2 arguments and the second is a variable name
     */
    if (argc == 2 && exp->def.op.args.firstp->nextp->kind == RDB_EX_VAR) {
        if (strcmp(exp->def.op.name, "sum") == 0) {
            RDB_init_obj(&tb);

            if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp,
                    ecp, txp, &tb) != RDB_OK)
                return RDB_ERROR;
            ret = RDB_sum(&tb, exp->def.op.args.firstp->nextp->def.varname, ecp,
                    txp, valp);
            RDB_destroy_obj(&tb, ecp);
            return ret;
        }
        if (strcmp(exp->def.op.name, "avg") ==  0) {
            RDB_float res;

            RDB_init_obj(&tb);
            if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp, ecp, txp, &tb) != RDB_OK)
                return RDB_ERROR;
            ret = RDB_avg(&tb, exp->def.op.args.firstp->nextp->def.varname, ecp, txp, &res);
            RDB_destroy_obj(&tb, ecp);
            if (ret == RDB_OK) {
                RDB_float_to_obj(valp, res);
            }
            return ret;
        }
        if (strcmp(exp->def.op.name, "min") ==  0) {
            RDB_init_obj(&tb);
            if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp,
                    ecp, txp, &tb) != RDB_OK)
                return RDB_ERROR;
            ret = RDB_min(&tb, exp->def.op.args.firstp->nextp->def.varname, ecp, txp, valp);
            RDB_destroy_obj(&tb, ecp);
            return ret;
        }
        if (strcmp(exp->def.op.name, "max") ==  0) {
            RDB_init_obj(&tb);
            if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp, ecp,
                    txp, &tb) != RDB_OK)
                return RDB_ERROR;
            ret = RDB_max(&tb, exp->def.op.args.firstp->nextp->def.varname, ecp, txp, valp);
            RDB_destroy_obj(&tb, ecp);
            return ret;
        }
        if (strcmp(exp->def.op.name, "all") ==  0) {
            RDB_bool res;

            RDB_init_obj(&tb);
            if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp, ecp,
                    txp, &tb) != RDB_OK)
                return RDB_ERROR;
            ret = RDB_all(&tb, exp->def.op.args.firstp->nextp->def.varname, ecp, txp, &res);
            RDB_destroy_obj(&tb, ecp);
            if (ret == RDB_OK) {
                RDB_bool_to_obj(valp, res);
            }
            return ret;
        }
        if (strcmp(exp->def.op.name, "any") ==  0) {
            RDB_bool res;

            RDB_init_obj(&tb);
            if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp,
                    ecp, txp, &tb) != RDB_OK)
                return RDB_ERROR;
            ret = RDB_any(&tb, exp->def.op.args.firstp->nextp->def.varname, ecp, txp, &res);
            RDB_destroy_obj(&tb, ecp);
            if (ret == RDB_OK) {
                RDB_bool_to_obj(valp, res);
            }
            return ret;
        }
    }

    if (argc == 3 && strcmp(exp->def.op.name, "if") == 0) {
        return evaluate_if(exp, getfnp, getdata, envp, ecp, txp, valp);
    }

    valpv = RDB_alloc(argc * sizeof (RDB_object *), ecp);
    if (valpv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    valv = RDB_alloc(argc * sizeof (RDB_object), ecp);
    if (valv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    for (i = 0; i < argc; i++)
        valpv[i] = NULL;

    /*
     * Get pointers to argument values, trying to avoid copying of values.
     */

    if (strcmp(exp->def.op.name, "ungroup") == 0) {
        int i = 2;
        i = i*i;
    }

    argp = exp->def.op.args.firstp;
    for (i = 0; i < argc; i++) {
        switch (argp->kind) {
            case RDB_EX_OBJ:
                valpv[i] = &argp->def.obj;
                break;
            case RDB_EX_TBP:
                valpv[i] = argp->def.tbref.tbp;
                break;
            case RDB_EX_VAR:
                if (getfnp != NULL) {
                    valpv[i] = (*getfnp)(argp->def.varname, getdata);
                }
                if (valpv[i] == NULL && txp != NULL) {
                    /* Try to get table */
                    valpv[i] = RDB_get_table(argp->def.varname, ecp, txp);
                }
                if (valpv[i] == NULL) {
                    RDB_raise_name(argp->def.varname, ecp);
                    ret = RDB_ERROR;
                    goto cleanup;
                }
                break;
            default:
                valpv[i] = &valv[i];
                RDB_init_obj(&valv[i]);
                ret = RDB_evaluate(argp, getfnp, getdata, envp, ecp, txp, &valv[i]);
                if (ret != RDB_OK)
                    goto cleanup;
                break;
        }

        /*
         * Obtain argument type if not already available
         */
        if (valpv[i]->typ == NULL) {
            valpv[i]->typ = RDB_expr_type(argp, getfnp != NULL ? get_type : NULL,
                    getfnp != NULL ? &gtinfo : NULL, ecp, txp);
            if (valpv[i]->typ == NULL) {
                ret = RDB_ERROR;
                goto cleanup;
            }
        }

        argp = argp->nextp;
    }

    /*
     * Check ARRAY arguments
     */
    if (strcmp(exp->def.op.name, "array") == 0) {
        RDB_type *typ;

        if (exp->typ != NULL) {
            /* Expression type is available - get array element type */
            typ = RDB_base_type(exp->typ);
            if (typ == NULL) {
                RDB_raise_internal("Obtaining ARRAY type failed", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        } else {
            /* No expression type - get type of argument #1 */
            if (argc < 1) {
                RDB_raise_internal("argument required for ARRAY", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
            typ = valpv[0]->typ;
        }

        /* Check argument types */
        for (i = 0; i < argc; i++) {
            if (!RDB_type_equals(valpv[i]->typ, typ)) {
                RDB_raise_type_mismatch("ARRAY type mismatch", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }
    }

    /* Handle RELATION with type */
    if (strcmp(exp->def.op.name, "relation") == 0 && exp->typ != NULL) {
        /* Relation type has been specified - use it for creating the table */
        RDB_type *typ = RDB_dup_nonscalar_type(exp->typ, ecp);
        if (typ == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        ret = RDB_op_type_relation(argc, valpv, typ, ecp, txp, valp);
        goto cleanup;
    }

    ret = RDB_call_ro_op_by_name_e(exp->def.op.name, argc, valpv, envp, ecp,
            txp, valp);

cleanup:
    if (valv != NULL) {
        argp = exp->def.op.args.firstp;
        for (i = 0; i < argc; i++) {
            if (valpv[i] != NULL && argp->kind != RDB_EX_OBJ
                    && argp->kind != RDB_EX_TBP && argp->kind != RDB_EX_VAR) {
                RDB_destroy_obj(&valv[i], ecp);
                /*
                 * Don't have to drop valpv[i]->typ
                 * because it's managed by the argument expression
                 */
            }
            argp = argp->nextp;
        }
        RDB_free(valv);
    }
    RDB_free(valpv);
    return ret;
}

/** @addtogroup expr
 * @{
 */

/**
 * Evaluate *exp and store the result in *valp.
 * If *exp is the invocation of a relational operator,
 * *valp will become a transient virtual table.
 */
int
RDB_evaluate(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *valp)
{
    if (txp != NULL)
        envp = txp->envp;
    if (envp != NULL && RDB_env_trace(envp) > 0) {
        fputs("Evaluating ", stderr);
        if (RDB_print_expr(exp, stderr, ecp) == RDB_ERROR)
            return RDB_ERROR;
        fputs("\n", stderr);
    }

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        {
            int ret;
            RDB_object tpl;
            RDB_object *attrp;

            RDB_init_obj(&tpl);
            ret = RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp, ecp,
                    txp, &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
            if (tpl.kind != RDB_OB_TUPLE) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_type_mismatch("", ecp);
                return RDB_ERROR;
            }

            attrp = RDB_tuple_get(&tpl, exp->def.op.name);
            if (attrp == NULL) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_name(exp->def.op.name, ecp);
                return RDB_ERROR;
            }
            ret = RDB_copy_obj(valp, attrp, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
            return RDB_destroy_obj(&tpl, ecp);
        }
        case RDB_EX_GET_COMP:
        {
            int ret;
            RDB_object obj;

            RDB_init_obj(&obj);
            if (RDB_evaluate(exp->def.op.args.firstp, getfnp, getdata, envp, ecp,
                    txp, &obj) != RDB_OK) {
                 RDB_destroy_obj(&obj, ecp);
                 return RDB_ERROR;
            }
            ret = RDB_obj_comp(&obj, exp->def.op.name, valp, envp, ecp, txp);
            RDB_destroy_obj(&obj, ecp);
            return ret;
        }
        case RDB_EX_RO_OP:
            return evaluate_ro_op(exp, getfnp, getdata, envp, ecp, txp, valp);
        case RDB_EX_VAR:
            /* Try to resolve variable via getfnp */
            if (getfnp != NULL) {
                RDB_object *srcp = (*getfnp)(exp->def.varname, getdata);
                if (srcp != NULL)
                    return RDB_copy_obj(valp, srcp, ecp);
            }

            /* Try to get table */
            if (txp != NULL) {
                RDB_object *srcp = RDB_get_table(exp->def.varname, ecp, txp);

                if (srcp != NULL)
                    return RDB_copy_obj_data(valp, srcp, ecp, txp);
            }
            RDB_raise_name(exp->def.varname, ecp);
            return RDB_ERROR;
        case RDB_EX_OBJ:
            return RDB_copy_obj(valp, &exp->def.obj, ecp);
        case RDB_EX_TBP:
            return RDB_copy_obj_data(valp, exp->def.tbref.tbp, ecp, txp);
    }
    /* Should never be reached */
    abort();
} /* RDB_evaluate */

int
RDB_evaluate_bool(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_bool *resp)
{
    int ret;
    RDB_object val;

    RDB_init_obj(&val);
    ret = RDB_evaluate(exp, getfnp, getdata, envp, ecp, txp, &val);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val, ecp);
        return ret;
    }
    if (RDB_obj_type(&val) != &RDB_BOOLEAN) {
        RDB_destroy_obj(&val, ecp);
        RDB_raise_type_mismatch("expression type must be BOOLEAN", ecp);
        return RDB_ERROR;
    }

    *resp = val.val.bool_val;
    RDB_destroy_obj(&val, ecp);
    return RDB_OK;
}

/*@}*/
