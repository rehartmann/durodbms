/*
 * $Id$
 *
 *  Created on: 01.01.2012
 */

#include "rdb.h"
#include "internal.h"
#include "transform.h"
#include <string.h>

/*
 * Convert an expression to a virtual table.
 */
static int
evaluate_vt(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_expression *nexp = getfnp != NULL
            ? RDB_expr_resolve_varnames(exp, getfnp, getdata, ecp, txp)
            : RDB_dup_expr(exp, ecp);
    if (nexp == NULL)
        return RDB_ERROR;

    if (_RDB_vtexp_to_obj(nexp, ecp, txp, retvalp) != RDB_OK) {
        RDB_drop_expr(nexp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
process_aggr_args(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *tbp)
{
    if (RDB_expr_list_length(&exp->var.op.args) != 2) {
        RDB_raise_invalid_argument("invalid number of aggregate arguments",
                ecp);
        return RDB_ERROR;
    }

    if (exp->var.op.args.firstp->nextp->kind != RDB_EX_VAR) {
        RDB_raise_invalid_argument("invalid aggregate argument #2", ecp);
        return RDB_ERROR;
    }

    if (RDB_evaluate(exp->var.op.args.firstp, getfnp, getdata, ecp, txp, tbp)
            != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

static int
evaluate_if(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    RDB_object arg1;

    RDB_init_obj(&arg1);

    /* Caller must ensure that there are 3 args */

    if (RDB_evaluate(exp->var.op.args.firstp, getfnp, getdata, ecp, txp, &arg1)
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
        ret = RDB_evaluate(exp->var.op.args.firstp->nextp, getfnp, getdata,
                ecp, txp, valp);
    } else {
        ret = RDB_evaluate(exp->var.op.args.firstp->nextp->nextp, getfnp, getdata,
                ecp, txp, valp);
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
evaluate_ro_op(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    int i;
    RDB_expression *argp;
    RDB_object tb;
    RDB_object **valpv;
    RDB_object *valv = NULL;
    int argc = RDB_expr_list_length(&exp->var.op.args);
    struct get_type_info gtinfo;

    if (getfnp != NULL) {
        gtinfo.getdata = getdata;
        gtinfo.getfnp = getfnp;
    }

    if (_RDB_transform(exp, getfnp != NULL ? get_type : NULL,
            getfnp != NULL ? &gtinfo : NULL, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    /*
     * Certain operators require special treatment
     */

    if (strcmp(exp->var.op.name, "EXTEND") == 0) {
        RDB_type *typ = RDB_expr_type(exp->var.op.args.firstp, NULL, NULL,
                ecp, txp);
        if (typ == NULL)
            return RDB_ERROR;

        if (typ->kind == RDB_TP_RELATION) {
            return evaluate_vt(exp, getfnp, getdata, ecp, txp, valp);
        } else if (typ->kind == RDB_TP_TUPLE) {
            int attrc = (argc - 1) / 2;
            RDB_virtual_attr *attrv;

            attrv = RDB_alloc(sizeof (RDB_virtual_attr) * attrc, ecp);
            if (attrv == NULL) {
                return RDB_ERROR;
            }

            argp = exp->var.op.args.firstp->nextp;
            for (i = 0; i < attrc; i++) {
                attrv[i].exp = argp;

                if (!_RDB_expr_is_string(argp->nextp)) {
                    RDB_raise_type_mismatch("attribute argument must be string",
                            ecp);
                    return RDB_ERROR;
                }
                attrv[i].name = RDB_obj_string(&argp->nextp->var.obj);
                argp = argp->nextp->nextp;
            }

            if (RDB_evaluate(exp->var.op.args.firstp, getfnp, getdata, ecp,
                    txp, valp) != RDB_OK) {
                RDB_free(attrv);
                return RDB_ERROR;
            }

            ret = RDB_extend_tuple(valp, attrc, attrv, ecp, txp);
            RDB_free(attrv);
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
        return evaluate_vt(exp, getfnp, getdata, ecp, txp, valp);
    }

    if (strcmp(exp->var.op.name, "SUM") == 0) {
        RDB_init_obj(&tb);

        if (process_aggr_args(exp, getfnp, getdata, ecp, txp, &tb) != RDB_OK)
            return RDB_ERROR;
        ret = RDB_sum(&tb, exp->var.op.args.firstp->nextp->var.varname, ecp,
                txp, valp);
        RDB_destroy_obj(&tb, ecp);
        return ret;
    }
    if (strcmp(exp->var.op.name, "AVG") ==  0) {
        RDB_float res;

        RDB_init_obj(&tb);
        if (process_aggr_args(exp, getfnp, getdata, ecp, txp, &tb) != RDB_OK)
            return RDB_ERROR;
        ret = RDB_avg(&tb, exp->var.op.args.firstp->nextp->var.varname, ecp, txp, &res);
        RDB_destroy_obj(&tb, ecp);
        if (ret == RDB_OK) {
            RDB_float_to_obj(valp, res);
        }
        return ret;
    }
    if (strcmp(exp->var.op.name, "MIN") ==  0) {
        RDB_init_obj(&tb);
        if (process_aggr_args(exp, getfnp, getdata, ecp, txp, &tb) != RDB_OK)
            return RDB_ERROR;
        ret = RDB_min(&tb, exp->var.op.args.firstp->nextp->var.varname, ecp, txp, valp);
        RDB_destroy_obj(&tb, ecp);
        return ret;
    }
    if (strcmp(exp->var.op.name, "MAX") ==  0) {
        RDB_init_obj(&tb);
        if (process_aggr_args(exp, getfnp, getdata, ecp, txp, &tb) != RDB_OK)
            return RDB_ERROR;
        ret = RDB_max(&tb, exp->var.op.args.firstp->nextp->var.varname, ecp, txp, valp);
        RDB_destroy_obj(&tb, ecp);
        return ret;
    }
    if (strcmp(exp->var.op.name, "ALL") ==  0) {
        RDB_bool res;

        RDB_init_obj(&tb);
        if (process_aggr_args(exp, getfnp, getdata, ecp, txp, &tb) != RDB_OK)
            return RDB_ERROR;
        ret = RDB_all(&tb, exp->var.op.args.firstp->nextp->var.varname, ecp, txp, &res);
        RDB_destroy_obj(&tb, ecp);
        if (ret == RDB_OK) {
            RDB_bool_to_obj(valp, res);
        }
        return ret;
    }
    if (strcmp(exp->var.op.name, "ANY") ==  0) {
        RDB_bool res;

        RDB_init_obj(&tb);
        if (process_aggr_args(exp, getfnp, getdata, ecp, txp, &tb) != RDB_OK)
            return RDB_ERROR;
        ret = RDB_any(&tb, exp->var.op.args.firstp->nextp->var.varname, ecp, txp, &res);
        RDB_destroy_obj(&tb, ecp);
        if (ret == RDB_OK) {
            RDB_bool_to_obj(valp, res);
        }
        return ret;
    }

    /* If needs special treatment because of lazy evaluation */
    if (strcmp(exp->var.op.name, "IF") == 0) {
        int len = RDB_expr_list_length(&exp->var.op.args);
        if (len == 3)
            return evaluate_if(exp, getfnp, getdata, ecp, txp, valp);
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
    argp = exp->var.op.args.firstp;
    for (i = 0; i < argc; i++) {
        switch (argp->kind) {
            case RDB_EX_OBJ:
                valpv[i] = &argp->var.obj;
                break;
            case RDB_EX_TBP:
                valpv[i] = argp->var.tbref.tbp;
                break;
            case RDB_EX_VAR:
                if (getfnp != NULL) {
                    valpv[i] = (*getfnp)(argp->var.varname, getdata);
                }
                if (valpv[i] == NULL && txp != NULL) {
                    /* Try to get table */
                    valpv[i] = RDB_get_table(argp->var.varname, ecp, txp);
                }
                if (valpv[i] == NULL) {
                    RDB_raise_name(argp->var.varname, ecp);
                    ret = RDB_ERROR;
                    goto cleanup;
                }
                break;
            default:
                valpv[i] = &valv[i];
                RDB_init_obj(&valv[i]);
                ret = RDB_evaluate(argp, getfnp, getdata, ecp, txp, &valv[i]);
                if (ret != RDB_OK)
                    goto cleanup;
                break;
        }

        /*
         * Obtain argument type if not already available (except for RELATION)
         */
        if (valpv[i]->typ == NULL && strcmp(exp->var.op.name, "RELATION") != 0) {
            valpv[i]->typ = RDB_expr_type(argp, NULL, NULL, ecp, txp);
            if (valpv[i]->typ == NULL) {
                ret = RDB_ERROR;
                goto cleanup;
            }
        }

        argp = argp->nextp;
    }

    if (strcmp(exp->var.op.name, "RELATION") == 0 && exp->typ != NULL) {
        /* Relation type has been specified - use it for creating the table */
        RDB_type *typ = RDB_dup_nonscalar_type(exp->typ, ecp);
        if (typ == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        ret = _RDB_op_type_relation(argc, valpv, typ, ecp, txp, valp);
    } else {
        ret = RDB_call_ro_op_by_name(exp->var.op.name, argc, valpv, ecp, txp, valp);
    }

cleanup:
    if (valv != NULL) {
        argp = exp->var.op.args.firstp;
        for (i = 0; i < argc; i++) {
            if (valpv[i] != NULL && argp->kind != RDB_EX_OBJ
                    && argp->kind != RDB_EX_TBP && argp->kind != RDB_EX_VAR) {
                RDB_destroy_obj(&valv[i], ecp);
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
 * If *exp defines a virtual table, *valp will become a transient virtual table.
 */
int
RDB_evaluate(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *valp)
{
    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        {
            int ret;
            RDB_object tpl;
            RDB_object *attrp;

            RDB_init_obj(&tpl);
            ret = RDB_evaluate(exp->var.op.args.firstp, getfnp, getdata, ecp,
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

            attrp = RDB_tuple_get(&tpl, exp->var.op.name);
            if (attrp == NULL) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_name(exp->var.op.name, ecp);
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
            if (RDB_evaluate(exp->var.op.args.firstp, getfnp, getdata, ecp,
                    txp, &obj) != RDB_OK) {
                 RDB_destroy_obj(&obj, ecp);
                 return RDB_ERROR;
            }
            ret = RDB_obj_comp(&obj, exp->var.op.name, valp, ecp, txp);
            RDB_destroy_obj(&obj, ecp);
            return ret;
        }
        case RDB_EX_RO_OP:
            return evaluate_ro_op(exp, getfnp, getdata, ecp, txp, valp);
        case RDB_EX_VAR:
            /* Try to resolve variable via getfnp */
            if (getfnp != NULL) {
                RDB_object *srcp = (*getfnp)(exp->var.varname, getdata);
                if (srcp != NULL)
                    return RDB_copy_obj(valp, srcp, ecp);
            }

            /* Try to get table */
            if (txp != NULL) {
                RDB_object *srcp = RDB_get_table(exp->var.varname, ecp, txp);

                if (srcp != NULL)
                    return _RDB_copy_obj(valp, srcp, ecp, txp);
            }
            RDB_raise_name(exp->var.varname, ecp);
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
RDB_evaluate_bool(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_object val;

    RDB_init_obj(&val);
    ret = RDB_evaluate(exp, getfnp, getdata, ecp, txp, &val);
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

/*@}*/
