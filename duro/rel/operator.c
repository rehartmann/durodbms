/*
 * $Id$
 *
 * Copyright (C) 2004-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include <gen/strfns.h>

#include <string.h>
#include <stdlib.h>

#include <assert.h>

static int
make_typesobj_from_params(int argc, RDB_parameter *paramv,
        RDB_exec_context *ecp, RDB_object *objp)
{
    int i;
    int ret;
    RDB_object typeobj;

    RDB_set_array_length(objp, argc, ecp);
    RDB_init_obj(&typeobj);
    for (i = 0; i < argc; i++) {
        ret = _RDB_type_to_binobj(&typeobj, paramv[i].typ, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typeobj, ecp);
            return ret;
        }
        ret = RDB_array_set(objp, (RDB_int) i, &typeobj, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typeobj, ecp);
            return ret;
        }
    }
    return RDB_destroy_obj(&typeobj, ecp);
}

/** @defgroup op Operator functions 
 * @{
 */

/**
 * RDB_create_ro_op creates a read-only operator with name <var>name</var>.
The argument types are specified by <var>argc</var> and <var>argtv</var>.

To execute the operator, Duro will execute the function specified by
<var>sym</var> from the library specified by <var>libname</var>.

The name of the library must be passed without the file extension.

This function must have the following signature:

@verbatim
int
<sym>(int argc, RDB_object *argv[], RDB_operator *op,
          RDB_exec_context *ecp, RDB_transaction *txp,
          RDB_object *retvalp)
@endverbatim

When the function is executed, the operator is passed through <var>op</var>
and the arguments are passed through <var>argc</var> and <var>argv</var>.

The function specified by <var>sym</var> must store the result at the
location specified by <var>retvalp</var> and return RDB_OK.
It can indicate an error condition by storing an error in *<var>ecp</var>
(see RDB_raise_err()) and returning RDB_ERROR.

The return type is passed through <var>rtyp</var>.

!!
If <var>iargp</var> is not NULL, it must point to a byte block
of length <var>iarglen</var> which will be passed to the function
specified by <var>sym</var>.
This can be used to pass code to an interpreter function.

Overloading operators is possible.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd>*<var>txp</var> is not a running transaction.
<dt>ELEMENT_EXIST_ERROR
<dd>A read-only operator with this name and signature does already exist.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_ro_op(const char *name, int paramc, RDB_parameter paramv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const void *iargp, size_t iarglen, 
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object rtypobj;
    RDB_object typesobj;
    RDB_type **paramtv;
    int ret;
    int i;

    paramtv = RDB_alloc(sizeof(RDB_type *) * paramc, ecp);
    if (paramtv == NULL) {
        return RDB_ERROR;
    }
    for (i = 0; i < paramc; i++) {
        paramtv[i] = paramv[i].typ;
    }

    /* Overloading built-in operators is not permitted */
    if (RDB_get_op(&_RDB_builtin_ro_op_map, name, paramc, paramtv, ecp) != NULL) {
        RDB_free(paramtv);
        RDB_raise_element_exists(name, ecp);
        return RDB_ERROR;
    }
    RDB_free(paramtv);

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Insert operator data into catalog
     */

    RDB_init_obj(&tpl);
    RDB_init_obj(&rtypobj);

    ret = RDB_tuple_set_string(&tpl, "NAME", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "LIB", libname, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "SYMBOL", symname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    if (iargp == NULL)
        iarglen = 0;

    RDB_init_obj(&iarg);
    ret = RDB_binary_set(&iarg, 0, iargp, iarglen, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&iarg, ecp);
        goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "IARG", &iarg, ecp);
    RDB_destroy_obj(&iarg, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized argument types */
    RDB_init_obj(&typesobj);
    ret = make_typesobj_from_params(paramc, paramv, ecp, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj, ecp);
        goto cleanup;
    }

    ret = RDB_tuple_set(&tpl, "ARGTYPES", &typesobj, ecp);
    RDB_destroy_obj(&typesobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = _RDB_type_to_binobj(&rtypobj, rtyp, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "RTYPE", &rtypobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->ro_ops_tbp, &tpl, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Check if it's a comparison operator */
    if (strcmp(name, "CMP") == 0 && paramc == 2
            && RDB_type_equals(paramv[0].typ, paramv[1].typ)
            && (paramv[0].typ->kind != RDB_TP_SCALAR /* !! */
                    || !paramv[0].typ->var.scalar.builtin)) {
        RDB_operator *cmpop;
        RDB_type *paramtv[2];

        paramtv[0] = paramv[0].typ;
        paramtv[1] = paramv[1].typ;

        cmpop = _RDB_get_ro_op(name, 2, paramtv, ecp, txp);
        if (cmpop == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        paramtv[0]->compare_op = cmpop;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&rtypobj, ecp);
    return ret;
}

/**
 * RDB_create_update_op creates an update operator with name <var>name</var>.
The argument types are specified by <var>argc</var> and <var>argtv</var>.

The argument <var>upd</var> specifies which of the arguments are updated.
If upd[<em>i</em>] is RDB_TRUE, this indicates that the <em>i</em>th argument
is updated by the operator.

To execute the operator, Duro will execute the function specified by
<var>sym</var> from the library specified by <var>libname</var>.

The name of the library must be passed without the file extension.

This function must have the following signature:

@verbatim
int
<sym>(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
@endverbatim

When the function is executed, the name of the operator is passed through <var>name</var>
and the arguments are passed through <var>argc</var> and <var>argv</var>.

On success, the function specified by <var>sym</var> must return RDB_OK.
It can indicate an error condition by leaving an error in *<var>ecp</var>
(see RDB_raise_err() and related functions)
and returning RDB_ERROR.

!!
If <var>iargp</var> is not NULL, it must point to a byte block
of length <var>iarglen</var> which will be passed to the function
specified by <var>sym</var>.
This can be used to pass code to an interpreter function.

Overloading operators is possible.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd>*<var>txp</var> is not a running transaction.
<dt>ELEMENT_EXIST_ERROR
<dd>An update operator with this name and signature does already exist.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_update_op(const char *name, int paramc, RDB_parameter paramv[],
                  const char *libname, const char *symname,
                  const void *iargp, size_t iarglen,
                  RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object updvobj;
    RDB_object updobj;
    RDB_object typesobj;
    int i;
    int ret;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Array types are not supported as argument types
     * !!
    for (i = 0; i < argc; i++) {
        if (paramv[i].typ->kind == RDB_TP_ARRAY) {
            RDB_raise_not_supported(
                    "array type not supported as argument type", ecp);
            return RDB_ERROR;
         }
    }
    */

    /*
     * Insert operator data into catalog
     */

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "NAME", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "LIB", libname, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "SYMBOL", symname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    if (iargp == NULL)
        iarglen = 0;

    RDB_init_obj(&iarg);
    ret = RDB_binary_set(&iarg, 0, iargp, iarglen, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&iarg, ecp);
        goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "IARG", &iarg, ecp);
    RDB_destroy_obj(&iarg, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    RDB_init_obj(&updvobj);
    RDB_init_obj(&updobj);
    RDB_set_array_length(&updvobj, (RDB_int) paramc, ecp);
    for (i = 0; i < paramc; i++) {
        RDB_bool_to_obj(&updobj, paramv[i].update);
        ret = RDB_array_set(&updvobj, (RDB_int) i, &updobj, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&updvobj, ecp);
            RDB_destroy_obj(&updobj, ecp);
            goto cleanup;
        }
    }
    ret = RDB_tuple_set(&tpl, "UPDV", &updvobj, ecp);
    RDB_destroy_obj(&updvobj, ecp);
    RDB_destroy_obj(&updobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized arg types */
    RDB_init_obj(&typesobj);
    ret = make_typesobj_from_params(paramc, paramv, ecp, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj, ecp);
        goto cleanup;
    }
    
    ret = RDB_tuple_set(&tpl, "ARGTYPES", &typesobj, ecp);
    RDB_destroy_obj(&typesobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->upd_ops_tbp, &tpl, ecp, txp);

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static RDB_bool
obj_is_scalar(const RDB_object *objp)
{
    return (RDB_bool) (objp->typ != NULL && RDB_type_is_scalar(objp->typ));
}

static RDB_bool
obj_is_table(const RDB_object *objp)
{
    /*
     * Check type first, as it could be a user-defined type
     * with a table representation
     */
    RDB_type *typ = RDB_obj_type(objp);
    if (typ == NULL)
        return (RDB_bool) (objp->kind == RDB_OB_TABLE);
    return (RDB_bool) (typ->kind == RDB_TP_RELATION);
}

static RDB_type **
valv_to_typev(int valc, RDB_object **valv, RDB_exec_context *ecp)
{
    int i;
    RDB_type **typv = RDB_alloc(sizeof (RDB_type *) * valc, ecp);
    if (typv == NULL)
        return NULL;

    for (i = 0; i < valc; i++) {
        typv[i] = RDB_obj_type(valv[i]);
        if (typv[i] == NULL) {
            RDB_raise_invalid_argument("cannot determine argument type", ecp);
            RDB_free(typv);
            return NULL;
        }
    }
    return typv;
}

/**
 * RDB_call_ro_op_by_name invokes the read-only operator with the name <var>name</var>,
passing the arguments in <var>argc</var> and <var>argv</var>.

The result will be stored at the location pointed to by
<var>retvalp</var>. 

The arguments must carry type information.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd><var>txp</var> is not NULL and *<var>txp</var> is not a running transaction.
<dt>OPERATOR_NOT_FOUND_ERROR
<dd>A read-only operator that matches the name and argument types could not be
found.
<dt>TYPE_MISMATCH_ERROR
<dd>A read-only operator that matches <var>name</var> could be found,
but it does not match the argument types.
</dl>

If <var>txp</var> is NULL, only built-in operators can be found.

If the user-supplied function which implements the function raises an
error, this error is returned in *ecp.

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_call_ro_op_by_name(const char *name, int argc, RDB_object *argv[],
               RDB_exec_context *ecp, RDB_transaction *txp,
               RDB_object *retvalp)
{
    RDB_operator *op;
    int ret;
    RDB_type **argtv;
    int i;

    /*
     * Handle RELATION, calling it directly,
     * bypassing getting types of all arguments
     */
    if (strcmp(name, "RELATION") == 0)
    	return _RDB_op_relation(argc, argv, NULL, ecp, txp, retvalp);

    /*
     * Handle nonscalar comparison
     */
    if (argc == 2 && !obj_is_scalar(argv[0]) && !obj_is_scalar(argv[1])) {
        if (strcmp(name, "=") == 0)
            return _RDB_obj_equals(2, argv, NULL /* !! */, ecp, txp, retvalp);
        if (strcmp(name, "<>") == 0) {
            ret = _RDB_obj_equals(2, argv, NULL /* !! */, ecp, txp, retvalp);
            if (ret != RDB_OK)
                return ret;
            retvalp->var.bool_val = (RDB_bool) !retvalp->var.bool_val;
            return RDB_OK;
        }
    }

    for (i = 0; i < argc; i++) {
        if (RDB_obj_type(argv[i]) == NULL) {
            RDB_raise_invalid_argument("cannot determine argument type", ecp);
            return RDB_ERROR;
        }
    }

    /*
     * Handle IF-THEN-ELSE
     */
    if (strcmp(name, "IF") == 0 && argc == 3) {
        if (argv[0]->typ != &RDB_BOOLEAN) {
            RDB_raise_type_mismatch("IF argument must be BOOLEAN", ecp);
            return RDB_ERROR;
        }
        if (argv[0]->var.bool_val) {
            ret = RDB_copy_obj(retvalp, argv[1], ecp);
        } else {
            ret = RDB_copy_obj(retvalp, argv[2], ecp);
        }
        return ret;
    }

    /*
     * Handle built-in operators with relational arguments
     */
    if (argc == 1 && obj_is_table(argv[0]))  {
        if (strcmp(name, "IS_EMPTY") == 0) {
            RDB_bool res;

            if (RDB_table_is_empty(argv[0], ecp, txp, &res) != RDB_OK)
                return RDB_ERROR;

            RDB_bool_to_obj(retvalp, res);
            return RDB_OK;
        }
        if (strcmp(name, "COUNT") == 0) {
            ret = RDB_cardinality(argv[0], ecp, txp);
            if (ret < 0)
                return RDB_ERROR;

            RDB_int_to_obj(retvalp, ret);
            return RDB_OK;
        }
    } else if (argc == 2 && obj_is_table(argv[1])) {
        if (strcmp(name, "IN") == 0) {
            RDB_bool b;

            ret = RDB_table_contains(argv[1], argv[0], ecp, txp, &b);
            if (ret != RDB_OK)
                return RDB_ERROR;

            RDB_bool_to_obj(retvalp, b);
            return RDB_OK;
        }
        if (strcmp(name, "SUBSET_OF") == 0) {
            RDB_bool res;

            ret = RDB_subset(argv[0], argv[1], ecp, txp, &res);
            if (ret != RDB_OK)
                return RDB_ERROR;
            RDB_bool_to_obj(retvalp, res);
            return RDB_OK;
        }
    }
    if (argc >= 1 && obj_is_table(argv[0]) && strcmp(name, "TO_TUPLE") == 0
            && argc == 1) {
        return RDB_extract_tuple(argv[0], ecp, txp, retvalp);
    }

    argtv = valv_to_typev(argc, argv, ecp);
    if (argtv == NULL) {
        return RDB_ERROR;
    }

    op = _RDB_get_ro_op(name, argc, argtv, ecp, txp);
    RDB_free(argtv);
    if (op == NULL) {
        goto error;
    }

    /* Set return type to make it available to the function */
    retvalp->typ = op->rtyp;

    ret = (*op->opfn.ro_fp)(argc, argv, op, ecp, txp, retvalp);
    if (ret != RDB_OK)
        goto error;

    /* Check type constraint if the operator is a selector */
    if (retvalp->typ != NULL && RDB_is_selector(op)) {
        if (_RDB_check_type_constraint(retvalp, ecp, txp) != RDB_OK) {
            /* Destroy illegal value */
            RDB_destroy_obj(retvalp, ecp);
            RDB_init_obj(retvalp);
            return RDB_ERROR;
        }
    }

    return RDB_OK;

error:
    return RDB_ERROR;
}

/**
 * Return the update operator with the name <var>name</var>
and the signature given by <var>argc</var> and <var>argtv</var>.

@returns the update operator, or NULL if an error occurred,
in which case *<var>ecp</var> carries the error information.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd>*<var>txp</var> is not a running transaction.
<dt>OPERATOR_NOT_FOUND_ERROR
<dd>An update operator that matches the name and arguments could not be
found.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */

RDB_operator *
RDB_get_update_op(const char *name, int argc, RDB_type *argtv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    /*
     * Try to get the operator from operator map
     */
    RDB_operator *op = RDB_get_op(&txp->dbp->dbrootp->upd_opmap, name, argc, argtv, ecp);
    if (op == NULL) {
        /*
         * The operator has not already been loaded into memory, so get it from the catalog
         */
        RDB_clear_err(ecp);
        op = _RDB_cat_get_upd_op(name, argc, argtv, ecp, txp);
        if (op == NULL) {
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                RDB_raise_operator_not_found(name, ecp);
            }
            return NULL;
        }
        if (RDB_put_op(&txp->dbp->dbrootp->upd_opmap, op, ecp) != RDB_OK) {
            RDB_free_op_data(op, ecp);
            return NULL;
        }
    }
    return op;
}

int
RDB_call_ro_op(RDB_operator *op, int argc, RDB_object *argv[],
                RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return (*op->opfn.ro_fp)(argc, argv, op, ecp, txp, retvalp);
}

/**
 * RDB_call_update_op_by_name invokes the update operator with the name <var>name</var>,
passing the arguments in <var>argc</var> and <var>argv</var>.

The arguments must carry type information.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd>*<var>txp</var> is not a running transaction.
<dt>OPERATOR_NOT_FOUND_ERROR
<dd>An update operator that matches the name and arguments could not be
found.
</dl>

If the user-supplied function which implements the operator raises an
error, this error is returned in *ecp.

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_call_update_op_by_name(const char *name, int argc, RDB_object *argv[],
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_operator *op;
    RDB_type **argtv;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    argtv = valv_to_typev(argc, argv, ecp);
    if (argtv == NULL) {
        return RDB_ERROR;
    }
    op = RDB_get_update_op(name, argc, argtv, ecp, txp);
    RDB_free(argtv);
    if (op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR) {
            RDB_raise_operator_not_found(name, ecp);
        }
        return RDB_ERROR;
    }

    return RDB_call_update_op(op, argc, argv, ecp, txp);
}

int
RDB_call_update_op(RDB_operator *op, int argc, RDB_object *argv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    return (*op->opfn.upd_fp)(argc, argv, op, ecp, txp);
}

/**
 * RDB_drop_op deletes the operator with the name <var>name</var>
from the database. This affects all overloaded versions.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd>*<var>txp</var> is not a running transaction.
<dt>OPERATOR_NOT_FOUND_ERROR
<dd>An operator with the specified name could not be found.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_drop_op(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp, *argp;
    RDB_object *vtbp;
    int ret;
    RDB_bool isempty;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Check if it's a read-only operator
     */
    exp = RDB_ro_op("WHERE", ecp);
    if (exp == NULL)
        return RDB_ERROR;
    argp = RDB_table_ref(txp->dbp->dbrootp->ro_ops_tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("NAME", ecp), RDB_string_to_expr(name, ecp),
            ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    
    vtbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    ret = RDB_table_is_empty(vtbp, ecp, txp, &isempty);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, ecp, txp);
        return ret;
    }
    ret = RDB_drop_table(vtbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    if (isempty) {
        /* It's an update operator */

        /* Delete all versions of update operator from operator map */
        if (RDB_del_ops(&txp->dbp->dbrootp->upd_opmap, name, ecp) != RDB_OK)
            return RDB_ERROR;

        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_var_ref("NAME", ecp), RDB_string_to_expr(name, ecp), ecp);
        if (exp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->upd_ops_tbp, exp, ecp, txp);
        RDB_drop_expr(exp, ecp);
        if (ret == 0) {
            RDB_raise_operator_not_found(name, ecp);
            return RDB_ERROR;
        }
        if (ret == RDB_ERROR) {
            return RDB_ERROR;
        }        
    } else {
        /* It's a read-only operator */

        /* Delete all versions of readonly operator from operator map */
        if (RDB_del_ops(&txp->dbp->dbrootp->ro_opmap, name, ecp) != RDB_OK)
            return RDB_ERROR;

        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_var_ref("NAME", ecp), RDB_string_to_expr(name, ecp),
               ecp);
        if (exp == NULL) {
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->ro_ops_tbp, exp, ecp, txp);
        if (ret == RDB_ERROR) {
            RDB_drop_expr(exp, ecp);
            RDB_errcode_to_error(ret, ecp, txp);
            return RDB_ERROR;
        }
    }

    return RDB_OK;
}

/**
 * Return the idx-th parameter of *<var>op</var>
 */
RDB_parameter *
RDB_get_parameter(const RDB_operator *op, int idx)
{
    if (idx >= op->paramc)
        return NULL;
    return &op->paramv[idx];
}

/**
 * Return the name of *<var>op</var>
 */
const char *
RDB_operator_name(const RDB_operator *op)
{
    return op->name;
}

/**
 * Return the return type of *<var>op</var> if it's a read-only operator.
 */
RDB_type *
RDB_return_type(const RDB_operator *op)
{
    return op->rtyp;
}

size_t
RDB_operator_iarglen(const RDB_operator *op)
{
    return op->iarg.var.bin.len;
}

void *
RDB_operator_iargp(const RDB_operator *op)
{
    return op->iarg.var.bin.datap;
}


void *
RDB_op_u_data(const RDB_operator *op)
{
    return op->u_data;
}

void
RDB_set_op_u_data(RDB_operator *op, void *u_data)
{
    op->u_data = u_data;
}

/**
 * Set function which is invoked when the *<var>op</var>
 * is deleted from memory
 */
void
RDB_set_op_cleanup_fn(RDB_operator *op,  RDB_op_cleanup_func *fp)
{
    op->cleanup_fp = fp;
}

/*@}*/

int
_RDB_eq_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val == argv[1]->var.bool_val));
    return RDB_OK;
}

int
_RDB_eq_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[0]->var.bin.len != argv[1]->var.bin.len)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else if (argv[0]->var.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->var.bin.datap,
            argv[1]->var.bin.datap, argv[0]->var.bin.len) == 0));
    return RDB_OK;
}

/* Default equality operator */
int
_RDB_obj_equals(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    RDB_bool res;
    RDB_type *arep = NULL;

    if (argv[0]->kind != argv[1]->kind) {
        RDB_raise_type_mismatch("", ecp);
        return RDB_ERROR;
    }

    /*
     * Check if there is a comparison function associated with the type
     */
    if (argv[0]->typ != NULL && RDB_type_is_scalar(argv[0]->typ)) {
        if (argv[0]->typ->compare_op != NULL) {
            arep = argv[0]->typ;
        } else if (argv[0]->typ->var.scalar.arep != NULL) {
            arep = argv[0]->typ->var.scalar.arep;
        }
    }

    /* If there is, call it */
    if (arep != NULL && arep->compare_op != NULL) {
        RDB_object retval;

        RDB_init_obj(&retval);
        ret = RDB_call_ro_op(arep->compare_op, 2, argv, ecp, txp, &retval);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&retval, ecp);
            return ret;
        }
        RDB_bool_to_obj(retvalp, (RDB_bool) RDB_obj_int(&retval) == 0);
        RDB_destroy_obj(&retval, ecp);
        return RDB_OK;
    }

    switch (argv[0]->kind) {
        case RDB_OB_INITIAL:
            RDB_raise_invalid_argument("invalid argument to equality", ecp);
            return RDB_ERROR;
        case RDB_OB_BOOL:
            return _RDB_eq_bool(2, argv, NULL, ecp, txp, retvalp);
        case RDB_OB_INT:
        case RDB_OB_FLOAT:
            /* Must not happen, because there must be a comparsion function */
            RDB_raise_internal("missing comparison function", ecp);
            return RDB_ERROR;
        case RDB_OB_BIN:
            return _RDB_eq_binary(2, argv, NULL, ecp, txp, retvalp);
        case RDB_OB_TUPLE:
            if (_RDB_tuple_equals(argv[0], argv[1], ecp, txp, &res) != RDB_OK)
                return RDB_ERROR;
            RDB_bool_to_obj(retvalp, res);
            break;
        case RDB_OB_TABLE:
            if (!RDB_type_equals(argv[0]->typ, argv[1]->typ)) {
                RDB_raise_type_mismatch("", ecp);
                return RDB_ERROR;
            }
            if (_RDB_table_equals(argv[0], argv[1], ecp, txp, &res) != RDB_OK)
                return RDB_ERROR;
            RDB_bool_to_obj(retvalp, res);
            break;
        case RDB_OB_ARRAY:
            ret = _RDB_array_equals(argv[0], argv[1], ecp, txp, &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            break;
    }
    return RDB_OK;
} 

int
_RDB_obj_not_equals(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int ret = _RDB_obj_equals(2, argv, NULL, ecp, txp, retvalp);
    if (ret != RDB_OK)
        return ret;
    retvalp->var.bool_val = (RDB_bool) !retvalp->var.bool_val;
    return RDB_OK;
}

RDB_operator *
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *errtyp;
    RDB_operator *op;
    RDB_bool typmismatch = RDB_FALSE;

    /* Lookup operator in built-in operator map */
    op = RDB_get_op(&_RDB_builtin_ro_op_map, name, argc, argtv, ecp);
    if (op != NULL)
        return op;

    errtyp = RDB_obj_type(RDB_get_err(ecp));
    if (errtyp != &RDB_OPERATOR_NOT_FOUND_ERROR
            && errtyp != &RDB_TYPE_MISMATCH_ERROR) {
        return NULL;
    }
    if (errtyp == &RDB_TYPE_MISMATCH_ERROR) {
        typmismatch = RDB_TRUE;
    }

    if (txp == NULL) {
        if (typmismatch) {
            RDB_raise_type_mismatch(name, ecp);
        } else {
            RDB_raise_operator_not_found(name, ecp);
        }
        return NULL;
    }
    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return NULL;
    }
     
    /* Lookup operator in dbroot map */
    op = RDB_get_op(&txp->dbp->dbrootp->ro_opmap, name, argc, argtv, ecp);
    if (op != NULL) {
        RDB_clear_err(ecp);
        return op;
    }

    errtyp = RDB_obj_type(RDB_get_err(ecp));        
    if (errtyp != &RDB_OPERATOR_NOT_FOUND_ERROR
            && errtyp != &RDB_TYPE_MISMATCH_ERROR) {
        return NULL;
    }
    if (errtyp == &RDB_TYPE_MISMATCH_ERROR) {
        typmismatch = RDB_TRUE;
    }
    RDB_clear_err(ecp);

    /*
     * Provide "=" and "<>" for user-defined types
     */
    if (argc == 2 && RDB_type_equals(argtv[0], argtv[1])) {
        int ret;

        if (strcmp(name, "=") == 0) {
            op = _RDB_new_operator(name, 2, argtv, &RDB_BOOLEAN, ecp);
            if (op == NULL) {
                return NULL;
            }
            op->opfn.ro_fp = &_RDB_obj_equals;
            ret = RDB_put_op(&txp->dbp->dbrootp->ro_opmap, op, ecp);
            if (ret != RDB_OK)
                return NULL;
            return op;
        }
        if (strcmp(name, "<>") == 0) {
            op = _RDB_new_operator(name, 2, argtv, &RDB_BOOLEAN, ecp);
            if (op == NULL) {
                return NULL;
            }
            op->opfn.ro_fp = &_RDB_obj_not_equals;
            ret = RDB_put_op(&txp->dbp->dbrootp->ro_opmap, op, ecp);
            if (ret != RDB_OK)
                return NULL;
            return op;
        }
    }
    RDB_clear_err(ecp);

    /*
     * Operator was not found in map, so read from catalog
     */
    op = _RDB_cat_get_ro_op(name, argc, argtv, ecp, txp);
    if (op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            if (typmismatch) {
                RDB_raise_type_mismatch(name, ecp);
            } else {
                RDB_raise_operator_not_found(name, ecp);
            }
        }
        return NULL;
    }

    /* Insert operator into dbroot map */
    if (RDB_put_op(&txp->dbp->dbrootp->ro_opmap, op, ecp) != RDB_OK) {
        RDB_free_op_data(op, ecp);
        return NULL;
    }

    return op;
}

int
_RDB_check_type_constraint(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (valp->typ->var.scalar.constraintp != NULL) {
        RDB_bool result;
        RDB_object tpl;

        RDB_init_obj(&tpl);

        /* Set tuple attribute */
        ret = RDB_tuple_set(&tpl, valp->typ->name, valp, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        ret = RDB_evaluate_bool(valp->typ->var.scalar.constraintp,
                &_RDB_tpl_get, &tpl, ecp, txp, &result);
        RDB_destroy_obj(&tpl, ecp);
        if (ret != RDB_OK) {
            return ret;
        }
        if (!result) {
            RDB_raise_type_constraint_violation("", ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

int
_RDB_add_selector(RDB_type *typ, RDB_exec_context *ecp)
{
    int i;
    RDB_operator *datap;
    int argc = typ->var.scalar.repv[0].compc;
    RDB_type **argtv = NULL;

    if (_RDB_init_builtin_ops(ecp) != RDB_OK)
        return RDB_ERROR;

    if (argc > 0) {
        argtv = RDB_alloc(sizeof(RDB_type *) * argc, ecp);
        if (argtv == NULL)
            return RDB_ERROR;
    }
    for (i = 0; i < argc; i++) {
        argtv[i] = typ->var.scalar.repv[0].compv[i].typ;
    }

    /*
     * Create RDB_operator and put it into read-only operator map
     */

    datap = _RDB_new_operator(typ->name, argc, argtv, typ, ecp);
    if (argtv != NULL)
        RDB_free(argtv);
    if (datap == NULL)
        goto error;
    datap->opfn.ro_fp = &_RDB_sys_select;

    if (RDB_put_op(&_RDB_builtin_ro_op_map, datap, ecp) != RDB_OK) {
        goto error;
    }
    return RDB_OK;

error:
    free(argtv);
    RDB_free_op_data(datap, ecp);
    return RDB_ERROR;
}
