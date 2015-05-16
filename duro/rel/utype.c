/*
 * Functions for user-defined types.
 *
 * Copyright (C) 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "serialize.h"
#include "cat_type.h"
#include "cat_op.h"
#include "internal.h"
#include <obj/objinternal.h>

#include <string.h>

typedef struct get_comp_type_data {
    const char *typename;
    int repc;
    const RDB_possrep *repv;
} get_comp_type_data;

static RDB_type *
getcomptype(const char *compname, void *arg)
{
    int i, j;
    const char *repcompname;
    get_comp_type_data *getcomptypedatap = (get_comp_type_data *) arg;

    /* Find component and return type */
    for (i = 0; i < getcomptypedatap->repc; i++) {
        for (j = 0; j < getcomptypedatap->repv[i].compc; j++) {
            repcompname = getcomptypedatap->repv[i].compv[j].name;
            if (repcompname == NULL)
                repcompname = getcomptypedatap->typename;
            if (strcmp(repcompname, compname) == 0)
                return getcomptypedatap->repv[i].compv[j].typ;
        }
    }
    return NULL;
}

static int
check_init(RDB_expression *initexp, int repc, const RDB_possrep repv[], const char *typename,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    RDB_type *argtyp;
    RDB_expr_list *arglistp;
    RDB_expression *argp;
    RDB_object selnameobj;

    /* Check if initexp is an operator invocation */
    if (initexp->kind != RDB_EX_RO_OP) {
        RDB_raise_invalid_argument("invalid INIT expression: operator invocation required", ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&selnameobj);

    /* Check if the operator name matches a selector name */
    for (i = 0; i < repc; i++) {
        if (repv[i].name != NULL) {
            if (RDB_possrep_to_selector(&selnameobj, repv[i].name, typename, ecp) != RDB_OK)
                goto error;
            if (strcmp(RDB_obj_string(&selnameobj), initexp->def.op.name) == 0)
                break;
        } else {
            /* No possrep name, use type name if only one possrep */
            if (repc == 1 && strcmp(typename, initexp->def.op.name) == 0)
                break;
        }
    }
    if (i >= repc) {
        RDB_raise_invalid_argument("invalid INIT expression: selector call required", ecp);
        goto error;
    }

    arglistp = RDB_expr_op_args(initexp);
    if (RDB_expr_list_length(arglistp) != repv[i].compc) {
        RDB_raise_invalid_argument("# of arguments does not match selector", ecp);
        goto error;
    }

    /* Check argument types against component */
    argp = arglistp->firstp;
    for (j = 0; j < repv[i].compc; j++) {
        argtyp = RDB_expr_type(argp, NULL, NULL, NULL, ecp, txp);
        if (argtyp == NULL)
            goto error;
        if (!RDB_type_equals(argtyp, repv[i].compv[j].typ)) {
            RDB_raise_type_mismatch("INIT expression argument type dot match selector", ecp);
            goto error;
        }
        argp = argp->nextp;
    }

    return RDB_destroy_obj(&selnameobj, ecp);

error:
    RDB_destroy_obj(&selnameobj, ecp);
    return RDB_ERROR;
}

/** @addtogroup type
 * @{
 */

/**
 *
Defines a type with the name <var>name</var> and
<var>repc</var> possible representations.
The individual possible representations are
described by the elements of <var>repv</var>.

If <var>constraintp</var> is not NULL, it specifies the type constraint.
When the constraint is evaluated, the value to check is made available
as an attribute with the same name as the type.

<var>initexp</var> specifies the initializer.
The expression must be of the type being defined.

<var>flags</var> can be 0 or RDB_TYPE_ORDERED.
If <var>flags</var> is RDB_TYPE_ORDERED, the type being defined is an ordered type.
If a type is ordered and has more than one possrep,
or if the possrep contains a component of a type that is not ordered,
a user-defined comparison operator must be provided.
(See RDB_implement_type()).

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>element_exists_error
<dd>There is already a type with name <var>name</var>.
<dt>type_mismatch_error
<dd><var>constraintp</var> is not NULL and not of type boolean.
<dd><var>constraintp</var> is not NULL and contains an operator
invocation where the argument type does not match the parameter type.
<dt>name_error
<dd><var>constraintp</var> is not NULL and contains a variable
reference that could not be resolved.
<dt>operator_not_found_error
<dd><var>constraintp</var> is not NULL and contains an invocation
of an operator that has not been defined.
<dt>invalid argument error
<dd><var>initexp</var> is not a valid selector invocation.
<dt>type_mismatch_error
<dd><var>initexp</var> contains an argument whose type does not match
the corresponing selector parameter.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_define_type(const char *name, int repc, const RDB_possrep repv[],
                RDB_expression *constraintp, RDB_expression *initexp,
                int flags, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object conval;
    RDB_object initval;
    RDB_object typedata;
    int i, j;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    /* Check if the constraint is of type boolean */
    if (constraintp != NULL) {
        RDB_type *constrtyp;
        get_comp_type_data getcomptypedata;

        getcomptypedata.typename = name;
        getcomptypedata.repc = repc;
        getcomptypedata.repv = repv;

        constrtyp = RDB_expr_type(constraintp,
                getcomptype, &getcomptypedata, NULL, ecp, txp);
        if (constrtyp == NULL)
            return RDB_ERROR;
        if (constrtyp != &RDB_BOOLEAN) {
            RDB_raise_type_mismatch("type constraint must be of type boolean",
                    ecp);
            return RDB_ERROR;
        }
    }

    /* Check if initexp is a selector invocation */
    if (check_init(initexp, repc, repv, name, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&conval);
    RDB_init_obj(&initval);
    RDB_init_obj(&typedata);

    if (RDB_binary_set(&typedata, 0, NULL, 0, ecp) != RDB_OK)
        goto error;

    /*
     * Insert tuple into sys_types
     */

    if (RDB_tuple_set_string(&tpl, "typename", name, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set(&tpl, "arep_type", &typedata, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set_int(&tpl, "arep_len", RDB_NOT_IMPLEMENTED, ecp)
            != RDB_OK)
        goto error;
    if (RDB_tuple_set_bool(&tpl, "sysimpl", RDB_FALSE, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set_bool(&tpl, "ordered",
            RDB_TYPE_ORDERED & flags ? RDB_TRUE : RDB_FALSE, ecp) != RDB_OK)
        goto error;

    /* Store constraint in tuple */
    if (RDB_expr_to_binobj(&conval, constraintp, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set(&tpl, "constraint", &conval, ecp) != RDB_OK)
        goto error;

    /* Store init expression in tuple */
    if (RDB_expr_to_binobj(&initval, initexp, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set(&tpl, "init", &initval, ecp) != RDB_OK)
        goto error;

    if (RDB_insert(txp->dbp->dbrootp->types_tbp, &tpl, ecp, txp) != RDB_OK)
        goto error;

    /*
     * Insert tuple into sys_possreps
     */

    for (i = 0; i < repc; i++) {
        char *prname = repv[i].name;

        if (prname == NULL) {
            char *dp;
            /* Possrep name may be NULL if there's only 1 possrep */
            if (repc > 1) {
                RDB_raise_invalid_argument("possrep name is NULL", ecp);
                goto error;
            }

            /* Make type name (without package) the possrep name */
            dp = strrchr(name, '.');
            if (dp == NULL) {
                prname = (char *) name;
            } else {
                prname = dp + 1;
            }
        }
        if (RDB_tuple_set_string(&tpl, "possrepname", prname, ecp) != RDB_OK)
            goto error;

        for (j = 0; j < repv[i].compc; j++) {
            char *cname = repv[i].compv[j].name;

            if (cname == NULL) {
                if (repv[i].compc > 1) {
                    RDB_raise_invalid_argument("component name is NULL", ecp);
                    goto error;
                }
                cname = prname;
            }
            if (RDB_tuple_set_int(&tpl, "compno", (RDB_int)j, ecp) != RDB_OK)
                goto error;
            if (RDB_tuple_set_string(&tpl, "compname", cname, ecp) != RDB_OK)
                goto error;

            if (RDB_type_to_binobj(&typedata, repv[i].compv[j].typ,
                    ecp) != RDB_OK)
                goto error;
            if (RDB_tuple_set(&tpl, "comptype", &typedata, ecp) != RDB_OK)
                goto error;

            if (RDB_insert(txp->dbp->dbrootp->possrepcomps_tbp, &tpl, ecp, txp)
                    != RDB_OK)
                goto error;
        }
    }

    RDB_destroy_obj(&typedata, ecp);
    RDB_destroy_obj(&conval, ecp);
    RDB_destroy_obj(&initval, ecp);
    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;

error:
    RDB_destroy_obj(&typedata, ecp);
    RDB_destroy_obj(&conval, ecp);
    RDB_destroy_obj(&initval, ecp);
    RDB_destroy_obj(&tpl, ecp);

    return RDB_ERROR;
}

int
RDB_del_type(RDB_type *typ, RDB_exec_context *ecp)
{
    int ret = RDB_OK;

    if (RDB_type_is_scalar(typ)) {
        RDB_free(typ->name);
        if (typ->def.scalar.repc > 0) {
            int i, j;

            for (i = 0; i < typ->def.scalar.repc; i++) {
                RDB_free(typ->def.scalar.repv[i].name);
                for (j = 0; j < typ->def.scalar.repv[i].compc; j++) {
                    RDB_free(typ->def.scalar.repv[i].compv[j].name);
                }
                RDB_free(typ->def.scalar.repv[i].compv);
            }
            RDB_free(typ->def.scalar.repv);
        }
        if (typ->def.scalar.arep != NULL
                && !RDB_type_is_scalar(typ->def.scalar.arep)) {
            ret = RDB_del_nonscalar_type(typ->def.scalar.arep, ecp);
        }
        if (typ->def.scalar.init_val_is_valid) {
            ret = RDB_destroy_obj(&typ->def.scalar.init_val, ecp);
        }
        if (typ->def.scalar.initexp != NULL) {
            ret = RDB_del_expr(typ->def.scalar.initexp, ecp);
        }
        RDB_free(typ);
    } else {
        ret = RDB_del_nonscalar_type(typ, ecp);
    }
    return ret;
}

/**
 * Deletes the user-defined type with name specified by <var>name</var>.

It is not possible to destroy built-in types.

If a selector operator is present, it will be deleted.

@returns

On success, RDB_OK is returned. Any other return value indicates an error.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>name_error
<dd>A type with name <var>name</var> was not found.
<dt>invalid_argument_error
<dd>The type is not user-defined.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_drop_type(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int cnt;
    RDB_type *typ;
    RDB_expression *wherep, *argp;
    RDB_type *ntp = NULL;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Check if the type is a built-in type
     */
    if (RDB_hashmap_get(&RDB_builtin_type_map, name) != NULL) {
        RDB_raise_invalid_argument("cannot drop a built-in type", ecp);
        return RDB_ERROR;
    }

    typ = RDB_get_type(name, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    /* Check if the type is still used by a table */
    if (RDB_cat_check_type_used(typ, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (typ->def.scalar.sysimpl) {
        /* Delete system-generated selector */

        RDB_object selnameobj;

        RDB_init_obj(&selnameobj);

        if (RDB_possrep_to_selector(&selnameobj, typ->def.scalar.repv[0].name,
                RDB_type_name(typ), ecp) != RDB_OK)
        {
            RDB_destroy_obj(&selnameobj, ecp);
            return RDB_ERROR;
        }

        if (RDB_drop_op(RDB_obj_string(&selnameobj), ecp, txp) != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR) {
                RDB_destroy_obj(&selnameobj, ecp);
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        }
        RDB_destroy_obj(&selnameobj, ecp);
    }

    if (RDB_type_is_ordered(typ)) {
        /* Delete comparison operators '<' etc. */
        if (RDB_del_cmp_op(&txp->dbp->dbrootp->ro_opmap, "<", typ, ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_del_cmp_op(&txp->dbp->dbrootp->ro_opmap, "<=", typ, ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_del_cmp_op(&txp->dbp->dbrootp->ro_opmap, ">", typ, ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_del_cmp_op(&txp->dbp->dbrootp->ro_opmap, ">=", typ, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    /* Delete type from database */
    wherep = RDB_ro_op("=", ecp);
    if (wherep == NULL) {
        return RDB_ERROR;
    }
    argp = RDB_var_ref("typename", ecp);
    if (argp == NULL) {
        RDB_del_expr(wherep, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);
    argp = RDB_string_to_expr(name, ecp);
    if (argp == NULL) {
        RDB_del_expr(wherep, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);

    cnt = RDB_delete(txp->dbp->dbrootp->types_tbp, wherep, ecp, txp);
    if (cnt == 0) {
        RDB_raise_name("type not found", ecp);
        return RDB_ERROR;
    }
    if (cnt == (RDB_int) RDB_ERROR) {
        RDB_del_expr(wherep, ecp);
        return RDB_ERROR;
    }
    cnt = RDB_delete(txp->dbp->dbrootp->possrepcomps_tbp, wherep, ecp,
            txp);
    RDB_del_expr(wherep, ecp);
    if (cnt == (RDB_int) RDB_ERROR) {
        return RDB_ERROR;
    }

    /*
     * Delete type in memory
     */

    /* Delete type from type map by putting a NULL pointer into it */
    if (RDB_hashmap_put(&txp->dbp->dbrootp->utypemap, name, ntp) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /*
     * Delete RDB_type struct last because name may be identical
     * to typ->name
     */
    if (RDB_del_type(typ, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}

static RDB_possrep *
RDB_get_possrep(const RDB_type *typ, const char *repname)
{
    int i;

    if (!RDB_type_is_scalar(typ))
        return NULL;
    for (i = 0; i < typ->def.scalar.repc
            && strcmp(typ->def.scalar.repv[i].name, repname) != 0;
            i++);
    if (i >= typ->def.scalar.repc)
        return NULL;
    return &typ->def.scalar.repv[i];
}

/**
 * Determines if the operator *<var>op</var> is a selector.
 */
RDB_bool
RDB_is_selector(const RDB_operator *op)
{
    if (op->rtyp == NULL)
        return RDB_FALSE; /* Not a read-only operator */

    /* Check if there is a possrep with the same name as the operator */
    return (RDB_bool) (RDB_get_possrep(op->rtyp, RDB_operator_name(op)) != NULL);
}

/**
 * Returns a pointer to a RDB_possrep structure representing the
 * possible representation of type <var>typ</var> containing
 * a component named <var>name</var>.
 * The structure is managed by the type.
 *
 * @returns A pointer to the RDB_possrep structure or NULL
 * if the component does not exist.
 */
RDB_possrep *
RDB_comp_possrep(const RDB_type *typ, const char *name)
{
    int i, j;

    for (i = 0; i < typ->def.scalar.repc; i++) {
        for (j = 0; j < typ->def.scalar.repv[i].compc; j++) {
            if (strcmp(typ->def.scalar.repv[i].compv[j].name, name) == 0) {
                return &typ->def.scalar.repv[i];
            }
        }
    }
    return NULL;
}

/**
Returns a pointer to RDB_type structure which
represents the type with the name <var>name</var>.

@returns

The pointer to the type on success, or NULL if an error occured.

@par Errors:

<dl>
<dt>type_not_found_error</dt>
<dd>A type with the name <var>name</var> could not be found.
</dd>
<dt></dt>
<dd>The type <var>name</var> is not a built-in type and <var>txp</var> is NULL.
</dd>
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
RDB_type *
RDB_get_type(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ;
    int ret;

    /*
     * search type in built-in type map
     */
    typ = RDB_hashmap_get(&RDB_builtin_type_map, name);
    if (typ != NULL) {
        return typ;
    }

    if (txp == NULL) {
        RDB_raise_type_not_found(name, ecp);
        return NULL;
    }

    /*
     * Search type in dbroot type map
     */
    typ = RDB_hashmap_get(&txp->dbp->dbrootp->utypemap, name);
    if (typ != NULL) {
        return typ;
    }

    /*
     * Search type in catalog
     */
    ret = RDB_cat_get_type(name, ecp, txp, &typ);
    if (ret != RDB_OK) {
        RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
        if (errtyp != NULL && errtyp == &RDB_NOT_FOUND_ERROR) {
            RDB_raise_type_not_found(name, ecp);
        }
        return NULL;
    }

    /*
     * Put type into type map
     */
    ret = RDB_hashmap_put(&txp->dbp->dbrootp->utypemap, name, typ);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    if (typ->ireplen != RDB_NOT_IMPLEMENTED) {
        /* Evaluate init expression */
        RDB_init_obj(&typ->def.scalar.init_val);
        if (RDB_evaluate(typ->def.scalar.initexp, NULL, NULL, NULL,
                ecp, txp, &typ->def.scalar.init_val) != RDB_OK) {
            RDB_destroy_obj(&typ->def.scalar.init_val, ecp);
            return NULL;
        }
        typ->def.scalar.init_val_is_valid = RDB_TRUE;

        /* Load selector, getters, and setters */
        if (RDB_load_type_ops(typ, ecp, txp) != RDB_OK)
            return NULL;

        if (RDB_type_is_ordered(typ)) {
            /*
             * Search for comparison function (after type was put into type map
             * so the type is available)
             */
            typ->compare_op = RDB_get_cmp_op(typ, ecp, txp);
            if (typ->compare_op == NULL) {
                if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR) {
                    return NULL;
                }
            } else {
                if (RDB_add_comparison_ops(typ, ecp, txp) != RDB_OK)
                    return NULL;
            }
        }
    }

    return typ;
}

/* @} */

typedef struct check_tc_data {
    RDB_object *objp;
    RDB_exec_context *ecp;
    RDB_transaction *txp;
    RDB_environment *envp;
    RDB_object comptpl; /* Stores the component values */
} check_tc_data;

static RDB_object *
getcomp(const char *name, void *arg)
{
    check_tc_data *checkdatap = (check_tc_data *) arg;

    RDB_object *compobjp = RDB_tuple_get(&checkdatap->comptpl, name);
    if (compobjp != NULL) {
        return compobjp;
    }

    /*
     * Get component value and set tuple attribute
     */
    if (RDB_tuple_set(&checkdatap->comptpl, name, NULL,
            checkdatap->ecp) != RDB_OK) {
        return NULL;
    }
    compobjp = RDB_tuple_get(&checkdatap->comptpl, name);

    if (RDB_obj_property(checkdatap->objp, name, compobjp,
            checkdatap->envp, checkdatap->ecp, checkdatap->txp) != RDB_OK) {
        return NULL;
    }
    return compobjp;
}

int
RDB_check_type_constraint(RDB_object *valp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (valp->typ->def.scalar.constraintp != NULL) {
        RDB_bool result;
        check_tc_data checkdata;

        checkdata.ecp = ecp;
        checkdata.objp = valp;
        checkdata.txp = txp;
        checkdata.envp = envp;
        RDB_init_obj(&checkdata.comptpl);

        ret = RDB_evaluate_bool(valp->typ->def.scalar.constraintp,
                &getcomp, &checkdata, envp, ecp, txp, &result);
        if (RDB_destroy_obj(&checkdata.comptpl, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        if (ret != RDB_OK) {
            return ret;
        }
        if (!result) {
            RDB_raise_type_constraint_violation(RDB_type_name(valp->typ), ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
load_getter(RDB_type *typ, const char *compname, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object opnameobj;
    RDB_int cnt;

    RDB_init_obj(&opnameobj);
    if (RDB_getter_name(typ, compname, &opnameobj, ecp) != RDB_OK)
        goto error;

    cnt = RDB_cat_load_ro_op(RDB_obj_string(&opnameobj), ecp, txp);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;
    if (cnt == 0) {
        RDB_raise_operator_not_found(RDB_obj_string(&opnameobj), ecp);
        goto error;
    }
    return RDB_destroy_obj(&opnameobj, ecp);

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
load_setter(RDB_type *typ, const char *compname, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object opnameobj;
    RDB_int cnt;

    RDB_init_obj(&opnameobj);
    if (RDB_setter_name(typ, compname, &opnameobj, ecp) != RDB_OK)
        goto error;

    cnt = RDB_cat_load_upd_op(RDB_obj_string(&opnameobj), ecp, txp);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;
    if (cnt == 0) {
        RDB_raise_operator_not_found(RDB_obj_string(&opnameobj), ecp);
        goto error;
    }
    return RDB_destroy_obj(&opnameobj, ecp);

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

/*
 * Load selector, getters and setters of user-defined type
 */
int
RDB_load_type_ops(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    for (i = 0; i < typ->def.scalar.repc; i++) {
        int j;

        if (RDB_cat_load_ro_op(typ->def.scalar.repv[i].name, ecp, txp)
                == (RDB_int) RDB_ERROR)
            return RDB_ERROR;

        if (!typ->def.scalar.sysimpl) {
            /* Load getters and setters */
            for (j = 0; j < typ->def.scalar.repv[i].compc; j++) {
                if (load_getter(typ, typ->def.scalar.repv[i].compv[j].name, ecp, txp)
                        != RDB_OK)
                    return RDB_ERROR;
                if (load_setter(typ, typ->def.scalar.repv[i].compv[j].name, ecp, txp)
                        != RDB_OK)
                    return RDB_ERROR;
            }
        }
    }
    return RDB_OK;
}

/*
 * Implements a system-generated selector
 */
int
RDB_sys_select(int argc, RDB_object *argv[],
        RDB_type *typ, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    /* If *retvalp carries a value, it must match the type */
    if (retvalp->kind != RDB_OB_INITIAL
            && (retvalp->typ == NULL
                || !RDB_type_equals(retvalp->typ, typ))) {
        RDB_raise_type_mismatch("invalid selector return type", ecp);
        return RDB_ERROR;
    }

    if (argc == 1) {
        /* Copy value */
        if (RDB_copy_obj_data(retvalp, argv[0], ecp, NULL) != RDB_OK)
            return RDB_ERROR;
    } else {
        /* Copy tuple attributes */
        int i;

        for (i = 0; i < argc; i++) {
            if (RDB_tuple_set(retvalp, typ->def.scalar.repv[0].compv[i].name,
                    argv[i], ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }
    retvalp->typ = typ;
    return RDB_OK;
}

/*
 * Implements a system-generated selector.
 * Signature is conformant to RDB_ro_op_func.
 */
int
RDB_op_sys_select(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_sys_select(argc, argv, op->rtyp, ecp, retvalp);
}

/* Invoke the comparison function of the argument type */
static int
sys_cmp(RDB_object *argv[], RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_int *resp)
{
    int ret;
    RDB_object cmpres;
    RDB_init_obj(&cmpres);
    ret = argv[0]->typ->compare_op->opfn.ro_fp(2, argv, argv[0]->typ->compare_op, ecp, txp,
            &cmpres);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&cmpres, ecp);
        return ret;
    }
    *resp = RDB_obj_int(&cmpres);
    RDB_destroy_obj(&cmpres, ecp);
    return RDB_OK;
}

/*
 * Implements the operator '<'.
 */
int
RDB_sys_lt(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int res;
    int ret = sys_cmp(argv, ecp, txp, &res);
    if (ret != RDB_OK)
        return ret;
    RDB_bool_to_obj(retvalp, (RDB_bool) (res < 0));
    return RDB_OK;
}

/**
 * Implements the operator '<='.
 */
int
RDB_sys_let(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int res;
    int ret = sys_cmp(argv, ecp, txp, &res);
    if (ret != RDB_OK)
        return ret;
    RDB_bool_to_obj(retvalp, (RDB_bool) (res <= 0));
    return RDB_OK;
}

/*
 * Implements the operator '<'.
 */
int
RDB_sys_gt(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int res;
    int ret = sys_cmp(argv, ecp, txp, &res);
    if (ret != RDB_OK)
        return ret;
    RDB_bool_to_obj(retvalp, (RDB_bool) (res > 0));
    return RDB_OK;
}

/*
 * Implements the operator '>='.
 */
int
RDB_sys_get(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int res;
    int ret = sys_cmp(argv, ecp, txp, &res);
    if (ret != RDB_OK)
        return ret;
    RDB_bool_to_obj(retvalp, (RDB_bool) (res >= 0));
    return RDB_OK;
}
