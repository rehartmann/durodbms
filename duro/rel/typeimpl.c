/*
 * typeimpl.c
 *
 *  Created on: 02.12.2014
 *      Author: rene
 */

#include "typeimpl.h"
#include <obj/objinternal.h>
#include "serialize.h"
#include "internal.h"

#include <string.h>

/*
 * Turn a possrep name into selector by copying it,
 * prepending the module name if necessary
 */
int
RDB_possrep_to_selector(RDB_object *selnameobjp, const char *repname,
        const char *typename, RDB_exec_context *ecp)
{
    char *dp = strrchr(typename, '.');
    if (dp == NULL) {
        return RDB_string_to_obj(selnameobjp, repname, ecp);
    }
    if (RDB_string_n_to_obj(selnameobjp, typename, dp - typename + 1, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_append_string(selnameobjp, repname, ecp);
}

static int
create_selector(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_object nameobj;
    int compc = typ->def.scalar.repv[0].compc;
    RDB_parameter *paramv = RDB_alloc(sizeof(RDB_parameter) * compc, ecp);
    if (paramv == NULL) {
        return RDB_ERROR;
    }

    /* create_selector is only called when there is only one possrep, so take the first */
    for (i = 0; i < compc; i++)
        paramv[i].typ = typ->def.scalar.repv[0].compv[i].typ;

    RDB_init_obj(&nameobj);
    if (RDB_possrep_to_selector(&nameobj, typ->def.scalar.repv[0].name,
            RDB_type_name(typ), ecp) != RDB_OK) {
        RDB_destroy_obj(&nameobj, ecp);
        RDB_free(paramv);
        return RDB_ERROR;
    }

    ret = RDB_create_ro_op(RDB_obj_string(&nameobj), compc, paramv, typ,
                "", "RDB_op_sys_select", typ->name, ecp, txp);
    RDB_free(paramv);
    return ret;
}

static int
op_sys_cmp(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *resultp)
{
    int i;
    RDB_object arg1;
    RDB_object arg2;
    RDB_type *typ = RDB_obj_type(argv[0]);
    RDB_possrep *possrep = &typ->def.scalar.repv[0];

    /* Compare components of 1st possrep */
    for (i = 0; i < possrep->compc; i++) {
        RDB_object *cmpargv[2];

        RDB_init_obj(&arg1);
        RDB_init_obj(&arg2);

        /* How to get the component if there is no tx!? */
        if (RDB_obj_property (argv[0], possrep->compv[i].name,
                &arg1, NULL, ecp, txp) != RDB_OK)
            goto error;
        if (RDB_obj_property (argv[1], possrep->compv[i].name,
                &arg2, NULL, ecp, txp) != RDB_OK)
            goto error;
        cmpargv[0] = &arg1;
        cmpargv[1] = &arg2;

        if ((*possrep->compv[i].typ->compare_op->opfn.ro_fp) (2, cmpargv,
                possrep->compv[i].typ->compare_op, ecp, txp, resultp) != RDB_OK)
            goto error;

        RDB_destroy_obj(&arg1, ecp);
        RDB_destroy_obj(&arg2, ecp);

        if (RDB_obj_int(resultp) != 0)
            return RDB_OK;
    }

    return RDB_OK;

error:
    RDB_destroy_obj(&arg1, ecp);
    RDB_destroy_obj(&arg2, ecp);
    return RDB_ERROR;
}

/** @addtogroup type
 * @{
 *
 * @defgroup typeimpl Type implementation functions
 * \#include <rel/typeimpl.h>
 * @{
 */

/**
 * Implements the user-defined type with name
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
type and a component name, separated by RDB_GETTER_INFIX (<code>_get_</code>).
It takes one argument. The argument must be of the user-defined type in question.
The return type must be the component type.
<dt>Setters
<dd>A setter is an update operator whose name consists of the
type and a component name, separated by RDB_SETTER_INFIX (<code>_set_</code>).
It takes two arguments. The first argument is an update argument
and must be of the user-defined type in question.
The second argument is read-only and must be of the type of
the component.
</dl>

If the type is ordered, a user-defined comparison operator returning an
<code>integer</code> may be supplied.
The comparison operator must be a read-only operator whose name consists
of the type name and RDB_COMPARERE_SUFFIX (<code>_cmp</code>).
It takes two arguments, both of the user-defined type in question.

It must return a value lower than, equal to, or greater than zero
if the first argument is lower than, equal to, or greater than
the second argument, respectively.

If the operator has been defined, it will be called by the built-in comparison
operators =, <>, <= etc.

If the operator has not been provided for an ordered type that has
only one possible representation consisting of components of ordered types,
the built-in comparison operators =, <>, <= are implemented by DuroDBMS
based on that possible representation.
Otherwise, if the type is ordered but does not have only one possible
representation consisting of components of ordered types, the comparison
operator must be provided.

Invoking comparison operators on user-implemented types may fail when there
is no running transaction because a running transaction may be required to
access the user-defined property getter operator.

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

    if (RDB_type_is_ordered(typ)) {
        if (typ->compare_op == NULL) {
            /*
             * Search for comparison function
             */
            typ->compare_op = RDB_get_cmp_op(typ, ecp, txp);
            if (typ->compare_op == NULL)
                return RDB_ERROR;
        }

        if (RDB_add_comparison_ops(typ, ecp, txp) != RDB_OK)
            return RDB_ERROR;
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
 * Determines if the operator *<var>op</var> is a getter operator.
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
 * Determines if the operator *<var>op</var> is a setter operator.
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
 * Determines if the operator *<var>op</var> is a comparison operator
 * returning int.
 */
RDB_bool
RDB_is_comparer(const RDB_operator *op)
{
    /*
     * An operator is treated as a comparer if it is a readonly operator
     * returning integer, has two arguments and the name contains
     * the substring RDB_COMPARER_SUFFIX.
     */
    if (op->rtyp != &RDB_INTEGER && op->paramc != 2)
        return RDB_FALSE;

    return (RDB_bool) (strstr(op->name, RDB_COMPARER_SUFFIX) != NULL);
}

/**
 * Deletes selector, getter, and setter operators.
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

/** @} */

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

RDB_operator *
RDB_get_cmp_op(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_operator *cmpop;
    RDB_type *typv[2];
    RDB_object opnameobj;

    typv[0] = typ;
    typv[1] = typ;
    RDB_init_obj(&opnameobj);
    if (RDB_string_to_obj(&opnameobj, RDB_type_name(typ), ecp) != RDB_OK)
        return NULL;
    if (RDB_append_string(&opnameobj, RDB_COMPARER_SUFFIX, ecp) != RDB_OK)
        return NULL;
    cmpop = RDB_get_ro_op(RDB_obj_string(&opnameobj), 2, typv, NULL, ecp, txp);
    RDB_destroy_obj(&opnameobj, ecp);
    if (cmpop == NULL) {
        int i;
        static struct RDB_op_data sys_compare = {
            NULL,
            &RDB_INTEGER
        };
        RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
        if (errtyp != &RDB_OPERATOR_NOT_FOUND_ERROR
                && errtyp != &RDB_TYPE_MISMATCH_ERROR)
            return NULL;

        /* Use system-provided comparison if there is only 1 possrep */
        if (typ->def.scalar.repc != 1) {
            RDB_raise_operator_not_found(
                    "no comparison operator found and number of possreps is not 1",
                    ecp);
            return NULL;
        }

        /* Check if all component types are ordered */
        for (i = 0; i < typ->def.scalar.repv[0].compc; i++) {
            if (!RDB_type_is_ordered(typ->def.scalar.repv[0].compv[i].typ)) {
                RDB_raise_operator_not_found(
                        "no comparison operator found and component type is not an ordered type",
                        ecp);
                return NULL;
            }
        }

        if (sys_compare.name == NULL) {
            sys_compare.opfn.ro_fp = &op_sys_cmp;
            sys_compare.cleanup_fp = (RDB_op_cleanup_func *) NULL;
        }

        cmpop = &sys_compare;
    }
    return cmpop;
}

static int
add_ro_op(const char *name, int paramc, RDB_type *paramtv[],
        RDB_type *rtyp, RDB_ro_op_func *opfp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_operator *op = RDB_new_op_data(name, paramc, paramtv, rtyp, ecp);
    if (op == NULL)
        return RDB_ERROR;

    op->opfn.ro_fp = opfp;

    if (RDB_put_op(&txp->dbp->dbrootp->ro_opmap, op, ecp) != RDB_OK) {
        RDB_free_op_data(op, ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

/* Create operators '<' etc. but not persistently */
int
RDB_add_comparison_ops(RDB_type *typ, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *typev[2];

    typev[0] = typ;
    typev[1] = typ;

    if (add_ro_op("<", 2, typev, &RDB_BOOLEAN, RDB_sys_lt, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    if (add_ro_op("<=", 2, typev, &RDB_BOOLEAN, RDB_sys_let, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    if (add_ro_op(">", 2, typev, &RDB_BOOLEAN, RDB_sys_gt, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    if (add_ro_op(">=", 2, typev, &RDB_BOOLEAN, RDB_sys_get, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}
