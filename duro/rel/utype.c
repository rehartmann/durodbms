/*
 * utype.c - functions for user-defined types
 *
 *  Created on: 28.10.2012
 *      Author: Rene Hartmann
 */

#include "rdb.h"
#include "internal.h"
#include "serialize.h"
#include "cat_type.h"
#include "cat_op.h"
#include "typeimpl.h"
#include <obj/tuple.h>
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

/** @addtogroup type
 * @{
 */

/**
 *
RDB_define_type defines a type with the name <var>name</var> and
<var>repc</var> possible representations.
The individual possible representations are
described by the elements of <var>repv</var>.

If <var>constraintp</var> is not NULL, it specifies the type constraint.
When the constraint is evaluated, the value to check is made available
as an attribute with the same name as the type.

<var>initexp</var> specifies the initializer.
The expression must be of the type being defined.

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
            (RDB_bool) RDB_TYPE_ORDERED & flags, ecp) != RDB_OK)
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
            /* Possrep name may be NULL if there's only 1 possrep */
            if (repc > 1) {
                RDB_raise_invalid_argument("possrep name is NULL", ecp);
                goto error;
            }
            /* Make type name the possrep name */
            prname = (char *) name;
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

static int
del_type(RDB_type *typ, RDB_exec_context *ecp)
{
    int ret = RDB_OK;

    if (RDB_type_is_scalar(typ)) {
        RDB_free(typ->name);
        if (typ->def.scalar.repc > 0) {
            int i, j;

            for (i = 0; i < typ->def.scalar.repc; i++) {
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
        RDB_free(typ);
    } else {
        ret = RDB_del_nonscalar_type(typ, ecp);
    }
    return ret;
}

/**
 * Delete the user-defined type with name specified by <var>name</var>.

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
        if (RDB_drop_op(typ->def.scalar.repv[0].name, ecp, txp) != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR) {
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        }
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
    if (cnt == (RDB_int) RDB_ERROR) {
        RDB_del_expr(wherep, ecp);
        return RDB_ERROR;
    }

    /*
     * Delete type in memory
     */

    /* Delete type from type map by putting a NULL pointer into it */
    if (RDB_hashmap_put(&txp->dbp->dbrootp->typemap, name, ntp) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /*
     * Delete RDB_type struct last because name may be identical
     * to typ->name
     */
    if (del_type(typ, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}

static int
create_selector(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int compc = typ->def.scalar.repv[0].compc;
    RDB_parameter *paramv = RDB_alloc(sizeof(RDB_parameter) * compc, ecp);
    if (paramv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < compc; i++)
        paramv[i].typ = typ->def.scalar.repv[0].compv[i].typ;
    ret = RDB_create_ro_op(typ->def.scalar.repv[0].name, compc, paramv, typ,
            "", "RDB_sys_select", typ->name,
            ecp, txp);
    RDB_free(paramv);
    return ret;
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
 * Check if the operator *<var>op</var> is a selector.
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
 * Return pointer to a RDB_possrep structure representing the
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

/** @defgroup typeimpl Type implementation functions
 * \#include <rel/typeimpl.h>
 * @{
 */

/**
 * RDB_implement_type implements the user-defined type with name
<var>name</var>. The type must have been defined previously using
RDB_define_type(). After RDB_implement_type was inkoved successfully,
this type may be used for local variables and table attributes.

If <var>arep</var> is not NULL, it must point to a type which is used
as the physical representation. The getter, setter, and selector operators
must be provided by the caller.

If <var>arep</var> is NULL and <var>areplen</var> is not RDB_SYS_REP,
<var>areplen</var> specifies the length, in bytes,
of the physical representation, which then is a fixed-length array of bytes.
The getter, setter, and selector operators must be provided by the caller.
RDB_irep_to_obj() can be used by the selector to assign a value to an RDB_object.
RDB_obj_irep() can be used by setters and getters to access the actual representation.

If <var>arep</var> is NULL and <var>areplen</var> is RDB_SYS_REP,
the getter and setter operators and the selector operator are provided by Duro.
In this case, the type must have exactly one possible representation.
If this representation has exactly one component, the type of this component will become
the physical representation. Otherwise the type will be represented by a tuple type with
one attribute for each component.

For user-provided setters, getters, and selectors,
the following conventions apply:

<dl>
<dt>Selectors
<dd>A selector is a read-only operator whose name is is the name of a possible
representation. It takes one argument for each component.
<dt>Getters
<dd>A getter is a read-only operator whose name consists of the
type and a component name, separated by RDB_GETTER_INFIX.
It takes one argument. The argument must be of the user-defined type in question.
The return type must be the component type.
<dt>Setters
<dd>A setter is an update operator whose name consists of the
type and a component name, separated by RDB_SETTER_INFIX.
It takes two arguments. The first argument is an update argument
and must be of the user-defined type in question.
The second argument is read-only and must be of the type of
the component.
</dl>

A user-defined comparison operator <code>cmp</code> returning an
<code>integer</code> may be supplied.
<code>cmp</code> must have two arguments, both of the user-defined type
for which the comparison is to be defined.

<code>cmp</code> must return -1, 0, or 1 if the first argument is lower than,
equal to, or greater than the second argument, respectively.

If <code>cmp</code> has been defined, it will be called by the built-in comparison
operators =, <>, <= etc.

@returns

On success, RDB_OK is returned. Any other return value indicates an error.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>not_found_error
<dd>The type has not been previously defined.
<dt>invalid_argument_error
<dd><var>arep</var> is NULL and <var>areplen</var> is RDB_SYS_REP,
and the type was defined with more than one possible representation.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_implement_type(const char *name, RDB_type *arep, RDB_int areplen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_attr_update upd[3];
    RDB_object typedata;
    int ret;
    int i;
    RDB_expression *exp, *argp;
    RDB_expression *wherep = NULL;
    RDB_type *typ = NULL;

    upd[0].exp = upd[1].exp = upd[2].exp = upd[3].exp = NULL;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    typ = RDB_get_type(name, ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    typ->def.scalar.sysimpl = (arep == NULL) && (areplen == RDB_SYS_REP);

    /* Load selector etc. to check if they have been provided */
    ret = RDB_load_type_ops(typ, ecp, txp);
    if (ret != RDB_OK)
       goto cleanup;

    if (typ->def.scalar.sysimpl) {
        /*
         * No actual rep given, so selector and getters/setters must be provided
         * by the system
         */
        int compc;

        /* # of possreps must be one */
        if (typ->def.scalar.repc != 1) {
            RDB_raise_invalid_argument("invalid # of possreps", ecp);
            return RDB_ERROR;
        }

        compc = typ->def.scalar.repv[0].compc;
        if (compc == 1) {
            arep = typ->def.scalar.repv[0].compv[0].typ;
        } else {
            /* More than one component, so internal rep is a tuple */
            arep = RDB_new_tuple_type(typ->def.scalar.repv[0].compc,
                    typ->def.scalar.repv[0].compv, ecp);
            if (arep == NULL)
                return RDB_ERROR;
        }

        typ->def.scalar.arep = arep;
        typ->ireplen = arep->ireplen;

        if (create_selector(typ, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    } else {
        typ->def.scalar.arep = arep;
        typ->ireplen = arep != NULL ? arep->ireplen : areplen;
    }

    /*
     * Update catalog
     */

    exp = RDB_var_ref("typename", ecp);
    if (exp == NULL) {
        return RDB_ERROR;
    }
    wherep = RDB_ro_op("=", ecp);
    if (wherep == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, exp);
    argp = RDB_string_to_expr(name, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);

    upd[0].name = "arep_len";
    upd[0].exp = RDB_int_to_expr(arep == NULL ? areplen : arep->ireplen, ecp);
    if (upd[0].exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    upd[1].name = "sysimpl";
    upd[1].exp = RDB_bool_to_expr(typ->def.scalar.sysimpl, ecp);
    if (upd[1].exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    if (arep != NULL) {
        RDB_init_obj(&typedata);
        ret = RDB_type_to_binobj(&typedata, arep, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typedata, ecp);
            goto cleanup;
        }

        upd[2].name = "arep_type";
        upd[2].exp = RDB_obj_to_expr(&typedata, ecp);
        RDB_destroy_obj(&typedata, ecp);
        if (upd[2].exp == NULL) {
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

    ret = RDB_update(txp->dbp->dbrootp->types_tbp, wherep,
            arep != NULL ? 3 : 2, upd, ecp, txp);
    if (ret != RDB_ERROR)
        ret = RDB_OK;

    RDB_init_obj(&typ->def.scalar.init_val);
    if (RDB_evaluate(typ->def.scalar.initexp, NULL, NULL, NULL, ecp, txp,
            &typ->def.scalar.init_val) != RDB_OK) {
        RDB_destroy_obj(&typ->def.scalar.init_val, ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }
    typ->def.scalar.init_val_is_valid = RDB_TRUE;

cleanup:
    for (i = 0; i < 3; i++) {
        if (upd[i].exp != NULL)
            RDB_del_expr(upd[i].exp, ecp);
    }
    if (wherep != NULL)
        RDB_del_expr(wherep, ecp);

    return ret;
}

/**
 * Check if the operator *<var>op</var> is a getter operator.
 */
RDB_bool
RDB_is_getter(const RDB_operator *op)
{
    /*
     * An operator is treated as a getter if it is read-only, has one argument
     * and the name contains the substring RDB_GETTER_INFIX.
     */
    if (op->rtyp == NULL && op->paramc != 1)
        return RDB_FALSE;

    return (RDB_bool) (strstr(op->name, RDB_GETTER_INFIX) != NULL);
}

/**
 * Check if the operator *<var>op</var> is a setter operator.
 */
RDB_bool
RDB_is_setter(const RDB_operator *op)
{
    /*
     * An operator is treated as a setter if it is an update operator,
     * has two arguments and the name contains the substring RDB_SETTER_INFIX.
     */
    if (op->rtyp != NULL && op->paramc != 2)
        return RDB_FALSE;

    return (RDB_bool) (strstr(op->name, RDB_SETTER_INFIX) != NULL);
}

/**
 * Delete selector, getter, and setter operators.
 */
int
RDB_drop_typeimpl_ops(const RDB_type *typ, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i, j;
    RDB_object opnameobj;

    if (!RDB_type_is_scalar(typ)) {
        RDB_raise_invalid_argument("type must be scalar", ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&opnameobj);
    for (i = 0; i < typ->def.scalar.repc; i++) {
        if (RDB_drop_op(typ->def.scalar.repv[i].name, ecp, txp) != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR)
                goto error;
            RDB_clear_err(ecp);
        }
        for (j = 0; j < typ->def.scalar.repv[i].compc; j++) {
            if (RDB_getter_name(typ, typ->def.scalar.repv[i].compv[j].name,
                    &opnameobj, ecp) != RDB_OK) {
                goto error;
            }
            if (RDB_drop_op(RDB_obj_string(&opnameobj), ecp, txp) != RDB_OK) {
                if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR)
                    goto error;
                RDB_clear_err(ecp);
            }

            if (RDB_setter_name(typ, typ->def.scalar.repv[i].compv[j].name,
                    &opnameobj, ecp) != RDB_OK) {
                goto error;
            }
            if (RDB_drop_op(RDB_obj_string(&opnameobj), ecp, txp) != RDB_OK) {
                if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR)
                    goto error;
                RDB_clear_err(ecp);
            }
        }
    }

    return RDB_destroy_obj(&opnameobj, ecp);

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

/* @} */

/**
Return a pointer to RDB_type structure which
represents the type with the name <var>name</var>.

@returns

The pointer to the type on success, or NULL if an error occured.

@par Errors:

<dl>
<dt>name_error</dt>
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
    RDB_type *typv[2];
    RDB_operator *cmpop;
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
        RDB_raise_name(name, ecp);
        return NULL;
    }

    /*
     * Search type in dbroot type map
     */
    typ = RDB_hashmap_get(&txp->dbp->dbrootp->typemap, name);
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
            RDB_raise_name(name, ecp);
        }
        return NULL;
    }

    /*
     * Put type into type map
     */
    ret = RDB_hashmap_put(&txp->dbp->dbrootp->typemap, name, typ);
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
    }

    /*
     * Search for comparison function (after type was put into type map
     * so the type is available)
     */
    typv[0] = typ;
    typv[1] = typ;
    cmpop = RDB_get_ro_op("cmp", 2, typv, NULL, ecp, txp);
    if (cmpop != NULL) {
        typ->compare_op = cmpop;
    } else {
        RDB_object *errp = RDB_get_err(ecp);
        if (errp != NULL
                && RDB_obj_type(errp) != &RDB_OPERATOR_NOT_FOUND_ERROR
                && RDB_obj_type(errp) != &RDB_TYPE_MISMATCH_ERROR) {
            return NULL;
        }
        RDB_clear_err(ecp);
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

int
RDB_getter_name(const RDB_type *typ, const char *compname,
        RDB_object *strobjp, RDB_exec_context *ecp)
{
    if (RDB_string_to_obj(strobjp, typ->name, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(strobjp, RDB_GETTER_INFIX, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(strobjp, compname, ecp);
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

int
RDB_setter_name(const RDB_type *typ, const char *compname,
        RDB_object *strobjp, RDB_exec_context *ecp)
{
    if (RDB_string_to_obj(strobjp, typ->name, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(strobjp, RDB_SETTER_INFIX, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(strobjp, compname, ecp);
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

/**
 * Implements a system-generated selector
 */
int
RDB_sys_select(int argc, RDB_object *argv[], const char *opname,
        RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_possrep *prp;

    /* Find possrep */
    prp = RDB_get_possrep(typ, opname);
    if (prp == NULL) {
        RDB_raise_invalid_argument("component name is NULL", ecp);
        return RDB_ERROR;
    }

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

/**
 * Implements a system-generated selector.
 * Signature is conformant to RDB_ro_op_func.
 */
int
RDB_op_sys_select(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_sys_select(argc, argv, op->name, op->rtyp, ecp, NULL, retvalp);
}
