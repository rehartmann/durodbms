/*
 * Copyright (C) 2004, 2013-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "cat_op.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <obj/operator.h>
#include <obj/builtinscops.h>
#include <obj/objinternal.h>

#include <string.h>
#include <stdlib.h>

static const char IS_PREFIX[] = "is_";
static size_t IS_PREFIX_LEN = sizeof(IS_PREFIX) - 1;

static const char TREAT_AS_PREFIX[] = "treat_as_";
static size_t TREAT_AS_PREFIX_LEN = sizeof(TREAT_AS_PREFIX) - 1;

/*
 * Convert paramv to an array of binary type representations
 */
static int
params_to_typesobj(int argc, RDB_parameter paramv[],
        RDB_exec_context *ecp, RDB_object *objp)
{
    int i;
    int ret;
    RDB_object typeobj;

    RDB_set_array_length(objp, argc, ecp);
    RDB_init_obj(&typeobj);
    for (i = 0; i < argc; i++) {
        ret = RDB_type_to_bin(&typeobj, paramv[i].typ, ecp);
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
 * Creates a read-only operator.

To execute the operator, DuroDBMS will execute the function specified by
<var>symname</var> from the library specified by <var>libname</var>.

The name of the library must be passed without the file extension.

The function must have the following signature:

@verbatim
int
<sym>(int argc, RDB_object *argv[], RDB_operator *op,
          RDB_exec_context *ecp, RDB_transaction *txp,
          RDB_object *retvalp)
@endverbatim

When the function is executed, the operator is passed through <var>op</var>
and the arguments are passed through <var>argc</var> and <var>argv</var>.

The function specified by <var>symname</var> must store the result at the
location specified by <var>retvalp</var> and return RDB_OK.
It can indicate an error condition by storing an error in *<var>ecp</var>
(see RDB_raise_err()) and returning RDB_ERROR.

The return type is passed through <var>rtyp</var>.

<var>sourcep</var> may be NULL or a pointer to the source code implementing
the operator.

Overloading operators is possible.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@param name the name of the operator.
@param paramc   the number of parameters.
@param paramv   the parameters. paramv[i].typ will become the type of the i-th
                parameter. The update field is ignored.
@param rtyp the return type.
@param libname  the name of a library containing the function which implements the
                operator.
@param symname  the name of the C function which implements the operator.
@param sourcep  a pointer to the source code, if the operator is executed by an interpreter.
@param ecp      a pointer to a RDB_exec_context used to return error information.
@param txp      the transaction which is used to write to the catalog.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>element_exists_error
<dd>A read-only operator with this name and signature does already exist.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_ro_op(const char *name, int paramc, RDB_parameter paramv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const char *sourcep,
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object rtypobj;
    RDB_object typesobj;
    RDB_object cretime;
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
    if (RDB_get_op(&RDB_builtin_ro_op_map, name, paramc, paramtv, ecp) != NULL) {
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
    RDB_init_obj(&cretime);

    ret = RDB_tuple_set_string(&tpl, "opname", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "lib", symname != NULL ? libname : "", ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "symbol", symname != NULL ? symname : "", ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "source", sourcep != NULL ? sourcep : "", ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_call_ro_op_by_name("now_utc", 0, NULL, ecp, txp, &cretime);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "creation_time", &cretime, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized argument types */
    RDB_init_obj(&typesobj);
    ret = params_to_typesobj(paramc, paramv, ecp, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj, ecp);
        goto cleanup;
    }

    ret = RDB_tuple_set(&tpl, "argtypes", &typesobj, ecp);
    RDB_destroy_obj(&typesobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_type_to_bin(&rtypobj, rtyp, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "rtype", &rtypobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->ro_ops_tbp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR
                || RDB_obj_type(RDB_get_err(ecp)) == &RDB_ELEMENT_EXISTS_ERROR) {
            RDB_raise_element_exists(name, ecp);
        }
        goto cleanup;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&rtypobj, ecp);
    RDB_destroy_obj(&cretime, ecp);
    return ret;
}

/**
 * RDB_create_update_op creates an update operator.

To execute the operator, DuroDBMS will execute the function specified by
<var>symname</var> from the library specified by <var>libname</var>.

The name of the library must be passed without the file extension.

This function must have the following signature:

@verbatim
int
<sym>(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
@endverbatim

When the function is executed, the name of the operator is passed through <var>name</var>
and the arguments are passed through <var>argc</var> and <var>argv</var>.

On success, the function specified by <var>symname</var> must return RDB_OK.
It can indicate an error condition by leaving an error in *<var>ecp</var>
(see RDB_raise_err() and related functions)
and returning RDB_ERROR.

<var>sourcep</var> may be NULL or a pointer to the source code implementing
the operator.

Overloading operators is possible.

@param name the name of the operator.
@param paramc   the number of parameters.
@param paramv   the parameters. paramv[i].typ will become the type of the i-th
                parameter. paramv[i].update specifies whether the argument is updated.
@param libname  the name of a library containing the function which implements the
                operator.
@param symname  the name of the C function which implements the operator.
@param sourcep  a pointer to the source code, if the operator is executed by an interpreter.
@param ecp      a pointer to a RDB_exec_context used to return error information.
@param txp      the transaction which is used to write to the catalog.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>element_exists_error
<dd>An update operator with this name and signature does already exist.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_update_op(const char *name, int paramc, RDB_parameter paramv[],
                  const char *libname, const char *symname,
                  const char *sourcep,
                  RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object updvobj;
    RDB_object updobj;
    RDB_object typesobj;
    RDB_object cretime;
    int i;
    int ret;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Insert operator data into catalog
     */

    RDB_init_obj(&cretime);

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "opname", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "lib", libname, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "symbol", symname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "source", sourcep != NULL ? sourcep : "", ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_call_ro_op_by_name("now_utc", 0, NULL, ecp, txp, &cretime);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "creation_time", &cretime, ecp);
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
    ret = RDB_tuple_set(&tpl, "updv", &updvobj, ecp);
    RDB_destroy_obj(&updvobj, ecp);
    RDB_destroy_obj(&updobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized arg types */
    RDB_init_obj(&typesobj);
    ret = params_to_typesobj(paramc, paramv, ecp, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj, ecp);
        goto cleanup;
    }
    
    ret = RDB_tuple_set(&tpl, "argtypes", &typesobj, ecp);
    RDB_destroy_obj(&typesobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->upd_ops_tbp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR
                || RDB_obj_type(RDB_get_err(ecp)) == &RDB_ELEMENT_EXISTS_ERROR) {
            RDB_raise_element_exists(name, ecp);
        }
        goto cleanup;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&cretime, ecp);
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

/**
 * RDB_call_ro_op_by_name invokes the read-only operator with the name <var>name</var>,
passing the arguments in <var>argc</var> and <var>argv</var>.

The result will be stored at the location pointed to by
<var>retvalp</var>. 

The arguments must carry type information.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> is not NULL and *<var>txp</var> is not a running transaction.
<dt>operator_not_found_error
<dd>A read-only operator that matches the name and argument types could not be
found.
<dt>type_mismatch_error
<dd>A read-only operator that matches <var>name</var> could be found,
but it does not match the argument types.
<dt>invalid_argument_error
<dd>An invalid argument was passed to the operator.
<dd>One or more of the arguments is a table that does not exist.
(e.g. after a rollback)
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
    return RDB_call_ro_op_by_name_e(name, argc, argv, NULL, ecp, txp, retvalp);
}

/**
 * Same as RDB_call_ro_op_by_name() but with an additional argument <var>envp</var>.
 * If <var>txp</var> is NULL and <var>envp</var> is not, <var>envp</var> is used
 * to look up read-only operators from memory.
 * If <var>txp</var> is not NULL, <var>envp</var> is ignored.
 */
int
RDB_call_ro_op_by_name_e(const char *name, int argc, RDB_object *argv[],
               RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp,
               RDB_object *retvalp)
{
    RDB_operator *op;
    int ret;
    int i;

    /*
     * Handle RELATION and other operators by calling them directly,
     * bypassing getting types of all arguments
     */

    if (strcmp(name, "relation") == 0)
    	return RDB_op_relation(argc, argv, NULL, ecp, txp, retvalp);

    /*
     * Handle nonscalar comparison
     */
    if (argc == 2 && !obj_is_scalar(argv[0]) && !obj_is_scalar(argv[1])) {
        if (strcmp(name, "=") == 0)
            return RDB_dfl_obj_equals(2, argv, NULL, ecp, txp, retvalp);
        if (strcmp(name, "<>") == 0) {
            ret = RDB_dfl_obj_equals(2, argv, NULL, ecp, txp, retvalp);
            if (ret != RDB_OK)
                return ret;
            retvalp->val.bool_val = (RDB_bool) !retvalp->val.bool_val;
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
    if (strcmp(name, "if") == 0 && argc == 3) {
        if (argv[0]->typ != &RDB_BOOLEAN) {
            RDB_raise_type_mismatch("IF argument must be boolean", ecp);
            return RDB_ERROR;
        }
        if (argv[0]->val.bool_val) {
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
        if (strcmp(name, "is_empty") == 0) {
            RDB_bool res;

            if (RDB_table_is_empty(argv[0], ecp, txp, &res) != RDB_OK)
                return RDB_ERROR;

            RDB_bool_to_obj(retvalp, res);
            return RDB_OK;
        }
        if (strcmp(name, "count") == 0) {
            ret = RDB_cardinality(argv[0], ecp, txp);
            if (ret < 0)
                return RDB_ERROR;

            RDB_int_to_obj(retvalp, ret);
            return RDB_OK;
        }
    }
    op = RDB_get_ro_op_by_args(name, argc, argv, envp, ecp, txp);
    if (op == NULL) {
        goto error;
    }

    if (op->opfn.ro_fp == NULL) {
        RDB_raise_operator_not_found("operator is not implemented", ecp);
        return RDB_ERROR;
    }

    /* Set return type to make it available to the function */
    retvalp->typ = op->rtyp;

    ret = (*op->opfn.ro_fp)(argc, argv, op, ecp, txp, retvalp);
    if (ret != RDB_OK)
        goto error;

    /* Check type constraint if the operator is a selector */
    if (retvalp->typ != NULL && RDB_is_selector(op)) {
        if (RDB_check_type_constraint(retvalp, envp, ecp, txp) != RDB_OK) {
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
A value of NULL in argtv matches any type.

If <var>txp</var> is NULL, <var>envp</var> is used to look up the
operator in memory. If <var>txp</var> is not NULL, <var>envp</var> is ignored.

@returns the update operator, or NULL if an error occurred,
in which case *<var>ecp</var> carries the error information.

@par Errors:

<dl>
<dt>invalid_argument_error
<dd>Both <var>txp</var> and <var>envp</var> are NULL.
<dt>operator_not_found_error
<dd>An update operator that matches the name and arguments could not be
found.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
RDB_operator *
RDB_get_update_op(const char *name, int argc, RDB_type *argtv[],
                RDB_environment *envp, RDB_exec_context *ecp,
                RDB_transaction *txp)
{
    RDB_bool type_mismatch = RDB_FALSE;
    RDB_dbroot *dbrootp;
    RDB_operator *op = RDB_get_op(&RDB_builtin_upd_op_map, name, argc, argtv,
            ecp);
    if (op != NULL)
        return op;
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_TYPE_MISMATCH_ERROR)
        type_mismatch = RDB_TRUE;

    if (txp != NULL) {
        dbrootp = RDB_tx_db(txp)->dbrootp;
    } else if (envp != NULL) {
        dbrootp = RDB_env_xdata(envp);
        /* Dbroot not initialized */
        if (dbrootp == NULL) {
            RDB_raise_operator_not_found(name, ecp);
            return NULL;
        }
    } else {
        RDB_raise_operator_not_found(name, ecp);
        return NULL;
    }

    /*
     * Try to get the operator from operator map
     */
    op = RDB_get_op(&dbrootp->upd_opmap, name, argc, argtv, ecp);
    if (op != NULL)
        return op;

    if (type_mismatch && RDB_obj_type(RDB_get_err(ecp))
            == &RDB_OPERATOR_NOT_FOUND_ERROR) {
        RDB_raise_type_mismatch("", ecp);
    }

    /*
     * The operator has not already been loaded into memory, so get it
     * from the catalog if a transaction is available
     */
    if (txp == NULL)
        return NULL;

    RDB_clear_err(ecp);
    if (RDB_cat_load_upd_op(name, ecp, txp) == (RDB_int) RDB_ERROR)
        return NULL;

    op = RDB_get_op(&dbrootp->upd_opmap, name, argc, argtv, ecp);
    if (op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_raise_operator_not_found(name, ecp);
        }
        return NULL;
    }
    if (RDB_env_trace(txp->envp) > 0) {
        fputs("Update operator ", stderr);
        fputs(name, stderr);
        fputs(" loaded from catalog\n", stderr);
    }
    return op;
}

/**
 * Like @ref RDB_get_update_op, but looks the operator up
 * by arguments instead of parameter types.
 */
RDB_operator *
RDB_get_update_op_by_args(const char *name, int argc, RDB_object *argv[],
                RDB_environment *envp, RDB_exec_context *ecp,
                RDB_transaction *txp)
{
    RDB_bool type_mismatch = RDB_FALSE;
    RDB_dbroot *dbrootp;
    RDB_operator *op = RDB_get_op_by_args(&RDB_builtin_upd_op_map, name, argc, argv,
            ecp);
    if (op != NULL)
        return op;
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_TYPE_MISMATCH_ERROR)
        type_mismatch = RDB_TRUE;

    if (txp != NULL) {
        dbrootp = RDB_tx_db(txp)->dbrootp;
    } else if (envp != NULL) {
        dbrootp = RDB_env_xdata(envp);
        /* Dbroot not initialized */
        if (dbrootp == NULL) {
            RDB_raise_operator_not_found(name, ecp);
            return NULL;
        }
    } else {
        RDB_raise_operator_not_found(name, ecp);
        return NULL;
    }

    /*
     * Try to get the operator from operator map
     */
    op = RDB_get_op_by_args(&dbrootp->upd_opmap, name, argc, argv, ecp);
    if (op != NULL)
        return op;

    if (type_mismatch && RDB_obj_type(RDB_get_err(ecp))
            == &RDB_OPERATOR_NOT_FOUND_ERROR) {
        RDB_raise_type_mismatch("", ecp);
    }

    /*
     * The operator has not already been loaded into memory, so get it
     * from the catalog if a transaction is available
     */
    if (txp == NULL)
        return NULL;

    RDB_clear_err(ecp);
    if (RDB_cat_load_upd_op(name, ecp, txp) == (RDB_int) RDB_ERROR)
        return NULL;

    op = RDB_get_op_by_args(&dbrootp->upd_opmap, name, argc, argv, ecp);
    if (op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_raise_operator_not_found(name, ecp);
        }
        return NULL;
    }
    if (RDB_env_trace(txp->envp) > 0) {
        fputs("Update operator ", stderr);
        fputs(name, stderr);
        fputs(" loaded from catalog\n", stderr);
    }
    return op;
}

/**
 * Invokes the update operator with the name <var>name</var>,
passing the arguments in <var>argc</var> and <var>argv</var>.

The arguments must carry type information.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>operator_not_found_error
<dd>An update operator that matches the name and arguments could not be
found.
<dt>invalid_argument_error
<dd>One or more of the arguments is a table that does not exist.
(e.g. after a rollback)
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
    RDB_operator *op = RDB_get_update_op_by_args(name, argc, argv, NULL, ecp, txp);
    if (op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR) {
            RDB_raise_operator_not_found(name, ecp);
        }
        return RDB_ERROR;
    }

    return RDB_call_update_op(op, argc, argv, ecp, txp);
}

/**
 * Calls the update operator *<var>op</var>,
passing the arguments in <var>argc</var> and <var>argv</var>.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

If the user-supplied function which implements the operator raises an
error, this error is returned in *ecp.
 */
int
RDB_call_update_op(RDB_operator *op, int argc, RDB_object *argv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    return (*op->opfn.upd_fp)(argc, argv, op, ecp, txp);
}

/**
 * RDB_drop_op deletes the operator with the name <var>name</var>.
 * This affects all overloaded versions.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>operator_not_found_error
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
    exp = RDB_ro_op("where", ecp);
    if (exp == NULL)
        return RDB_ERROR;
    argp = RDB_table_ref(txp->dbp->dbrootp->ro_ops_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_attr_eq_strval("opname", name, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    
    vtbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (vtbp == NULL) {
        RDB_del_expr(exp, ecp);
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
        exp = RDB_attr_eq_strval("opname", name, ecp);
        if (exp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->upd_ops_tbp, exp, ecp, txp);
        RDB_del_expr(exp, ecp);
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
        exp = RDB_attr_eq_strval("opname", name, ecp);
        if (exp == NULL) {
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->ro_ops_tbp, exp, ecp, txp);
        RDB_del_expr(exp, ecp);
        if (ret == RDB_ERROR) {
            RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }
    }

    return RDB_OK;
}

/*@}*/

/* Default equality operator */
int
RDB_dfl_obj_equals(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    RDB_bool res;
    RDB_type *impltyp1 = RDB_obj_impl_type(argv[0]);
    RDB_type *impltyp2 = RDB_obj_impl_type(argv[1]);
    RDB_type *arep = NULL;

    if (argv[0]->kind != argv[1]->kind) {
        RDB_bool_to_obj(retvalp, RDB_FALSE);
        return RDB_OK;
    }

    if (impltyp1 != NULL && impltyp2 != NULL
            && !RDB_type_equals(impltyp1, impltyp2)) {
        if (!RDB_type_is_scalar(impltyp1)) {
            RDB_raise_type_mismatch("", ecp);
            return RDB_ERROR;
        }
        RDB_bool_to_obj(retvalp, RDB_FALSE);
        return RDB_OK;
    }

    /*
     * Check if there is a comparison function associated with the type
     */
    if (impltyp1 != NULL && RDB_type_is_scalar(impltyp1)) {
        if (impltyp1->compare_op != NULL) {
            arep = impltyp1;
        } else if (impltyp1->def.scalar.arep != NULL) {
            arep = impltyp1->def.scalar.arep;
        }
    }

    /* If there is, call it */
    if (arep != NULL && arep->compare_op != NULL) {
        RDB_object retval;

        RDB_init_obj(&retval);
        ret = (*arep->compare_op->opfn.ro_fp)(2, argv, arep->compare_op, ecp, txp, &retval);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&retval, ecp);
            return ret;
        }
        RDB_bool_to_obj(retvalp, (RDB_bool) (RDB_obj_int(&retval) == 0));
        RDB_destroy_obj(&retval, ecp);
        return RDB_OK;
    }

    switch (argv[0]->kind) {
        case RDB_OB_INITIAL:
            RDB_raise_invalid_argument("invalid argument to equality", ecp);
            return RDB_ERROR;
        case RDB_OB_BOOL:
            return RDB_eq_bool(2, argv, NULL, ecp, txp, retvalp);
        case RDB_OB_INT:
        case RDB_OB_FLOAT:
        case RDB_OB_TIME:
            /* Must not happen, because there must be a comparison function */
            RDB_raise_internal("missing comparison function", ecp);
            return RDB_ERROR;
        case RDB_OB_BIN:
            return RDB_eq_binary(2, argv, NULL, ecp, txp, retvalp);
        case RDB_OB_TUPLE:
            if (RDB_tuple_equals(argv[0], argv[1], ecp, txp, &res) != RDB_OK)
                return RDB_ERROR;
            RDB_bool_to_obj(retvalp, res);
            break;
        case RDB_OB_TABLE:
            if (RDB_table_equals(argv[0], argv[1], ecp, txp, &res) != RDB_OK)
                return RDB_ERROR;
            RDB_bool_to_obj(retvalp, res);
            break;
        case RDB_OB_ARRAY:
            ret = RDB_array_equals(argv[0], argv[1], ecp, txp, &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            break;
    }
    return RDB_OK;
} 

int
RDB_obj_not_equals(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int ret = RDB_dfl_obj_equals(2, argv, NULL, ecp, txp, retvalp);
    if (ret != RDB_OK)
        return ret;
    retvalp->val.bool_val = (RDB_bool) !retvalp->val.bool_val;
    return RDB_OK;
}

static int
RDB_op_is_type(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_type *typ;
    if (argc != 1) {
        RDB_raise_invalid_argument("exactly one argument is required", ecp);
        return RDB_ERROR;
    }
    if (RDB_obj_type(argv[0]) == NULL) {
        RDB_raise_invalid_argument("argument type required", ecp);
        return RDB_ERROR;
    }

    /*
     * Raise error if the declared type and the target type do not have
     * a common subtype
     */
    typ = RDB_get_supertype_of_subtype(RDB_obj_type(argv[0]),
            RDB_operator_name(op) + IS_PREFIX_LEN);
    if (typ == NULL) {
        RDB_raise_type_mismatch(RDB_operator_name(op) + IS_PREFIX_LEN, ecp);
        return RDB_ERROR;
    }

    /* Check if the implemented type is subtype of the target type */
    RDB_bool_to_obj(retvalp, RDB_is_subtype(RDB_obj_impl_type(argv[0]), typ));
    return RDB_OK;
}

static int
RDB_op_treat_as_type(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argc != 1) {
        RDB_raise_invalid_argument("exactly one argument is required", ecp);
        return RDB_ERROR;
    }
    if (RDB_obj_type(argv[0]) == NULL) {
        RDB_raise_invalid_argument("argument type required", ecp);
        return RDB_ERROR;
    }
    if (!RDB_is_subtype(RDB_obj_impl_type(argv[0]), op->rtyp)) {
        RDB_raise_type_mismatch(RDB_type_name(op->rtyp), ecp);
        return RDB_ERROR;
    }
    if (RDB_copy_obj(retvalp, argv[0], ecp) != RDB_OK)
        return RDB_ERROR;
    RDB_obj_set_typeinfo(retvalp, op->rtyp);
    return RDB_OK;
}

static RDB_operator *
provide_eq_neq(const char *name, RDB_type *argtyp, RDB_dbroot *dbrootp,
        RDB_exec_context *ecp)
{
    RDB_operator *op;
    RDB_type *argtv[2];
    argtv[0] = argtv[1] = argtyp;

    if (strcmp(name, "=") == 0) {
        op = RDB_new_op_data(name, 2, argtv, &RDB_BOOLEAN, ecp);
        if (op == NULL) {
            return NULL;
        }
        op->opfn.ro_fp = &RDB_dfl_obj_equals;
        if (RDB_put_op(&dbrootp->ro_opmap, op, ecp) != RDB_OK)
            return NULL;
        return op;
    }
    op = RDB_new_op_data(name, 2, argtv, &RDB_BOOLEAN, ecp);
    if (op == NULL) {
        return NULL;
    }
    op->opfn.ro_fp = &RDB_obj_not_equals;
    if (RDB_put_op(&dbrootp->ro_opmap, op, ecp) != RDB_OK)
        return NULL;
    return op;
}

static RDB_operator *
provide_is_op(const char *name, RDB_dbroot *dbrootp,
        RDB_exec_context *ecp)
{
    RDB_operator *op = RDB_new_op_data(name, -1, NULL, &RDB_BOOLEAN, ecp);
    if (op == NULL) {
        return NULL;
    }
    op->opfn.ro_fp = &RDB_op_is_type;
    if (RDB_put_op(&dbrootp->ro_opmap, op, ecp) != RDB_OK)
        return NULL;
    return op;
}

static RDB_operator *
provide_treat_as_op(const char *name, RDB_dbroot *dbrootp, RDB_type *ttyp,
        RDB_exec_context *ecp)
{
    RDB_operator *op = RDB_new_op_data(name, -1, NULL, ttyp, ecp);
    if (op == NULL) {
        return NULL;
    }
    op->opfn.ro_fp = &RDB_op_treat_as_type;
    if (RDB_put_op(&dbrootp->ro_opmap, op, ecp) != RDB_OK)
        return NULL;
    return op;
}

/*
 * Get operator by name and types.
 * If txp is NULL and envp is not NULL
 * envp will be used to look up the operator in memory.
 * If txp is not NULL, envp is ignored.
 */
RDB_operator *
RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *errtyp;
    RDB_operator *op;
    RDB_dbroot *dbrootp;
    RDB_bool typmismatch = RDB_FALSE;

    /* Lookup operator in built-in operator map */
    op = RDB_get_op(&RDB_builtin_ro_op_map, name, argc, argtv, ecp);
    if (op != NULL)
        return op;

    /*
     * If search in builtin type map failed due to a type mismatch,
     * keep that info and use it later to raise type_mismatch_error
     * instead of operator_not_found_error.
     */
    errtyp = RDB_obj_type(RDB_get_err(ecp));
    if (errtyp != &RDB_OPERATOR_NOT_FOUND_ERROR
            && errtyp != &RDB_TYPE_MISMATCH_ERROR) {
        return NULL;
    }
    if (errtyp == &RDB_TYPE_MISMATCH_ERROR) {
        typmismatch = RDB_TRUE;
    }
    RDB_clear_err(ecp);

    if (txp != NULL) {
        if (!RDB_tx_is_running(txp)) {
            RDB_raise_no_running_tx(ecp);
            return NULL;
        }
        dbrootp = RDB_tx_db(txp)->dbrootp;
    } else if (envp != NULL) {
        dbrootp = RDB_env_xdata(envp);
        if (dbrootp == NULL) {
            RDB_raise_operator_not_found(name, ecp);
            return NULL;
        }
    } else {
        if (typmismatch) {
            RDB_raise_type_mismatch(name, ecp);
        } else {
            RDB_raise_operator_not_found(name, ecp);
        }
        return NULL;
    }

    /* Lookup operator in dbroot map */
    op = RDB_get_op(&dbrootp->ro_opmap, name, argc, argtv, ecp);
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

    /*
     * Provide "=" and "<>" for types with possreps
     */
    if (argc == 2 && RDB_type_is_scalar(argtv[0])
            && (!argtv[0]->def.scalar.builtin || argtv[0]->def.scalar.repc > 0)
            && RDB_share_subtype(argtv[0], argtv[1])
            && (strcmp(name, "=") == 0
                || strcmp(name, "<>") == 0)) {
        return provide_eq_neq(name, argtv[0], dbrootp, ecp);
    }

    if (strlen(name) > IS_PREFIX_LEN
            && strncmp(name, IS_PREFIX, IS_PREFIX_LEN) == 0
            && RDB_type_is_scalar(argtv[0])) {
        RDB_type *ttyp = RDB_get_supertype_of_subtype(argtv[0], name + IS_PREFIX_LEN);
        if (ttyp == NULL) {
            RDB_raise_type_mismatch(name + IS_PREFIX_LEN, ecp);
            return NULL;
        }
        return provide_is_op(name, dbrootp, ecp);
    }
    if (strlen(name) > TREAT_AS_PREFIX_LEN
            && strncmp(name, TREAT_AS_PREFIX, TREAT_AS_PREFIX_LEN) == 0
            && RDB_type_is_scalar(argtv[0])) {
        RDB_type *ttyp = RDB_get_supertype_of_subtype(argtv[0], name + TREAT_AS_PREFIX_LEN);
        if (ttyp == NULL) {
            RDB_raise_type_mismatch(name + TREAT_AS_PREFIX_LEN, ecp);
            return NULL;
        }
        return provide_treat_as_op(name, dbrootp, ttyp, ecp);
    }

    /*
     * Operator was not found in map, so read from catalog
     * if a transaction is available
     */
    if (txp == NULL)
        return NULL;

    RDB_clear_err(ecp);

    if (RDB_cat_load_ro_op(name, ecp, txp) == (RDB_int) RDB_ERROR) {
        return NULL;
    }
    op = RDB_get_op(&dbrootp->ro_opmap, name, argc, argtv, ecp);
    if (op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR) {
            if (typmismatch) {
                RDB_raise_type_mismatch(name, ecp);
            }
        }
        return NULL;
    }

    return op;
}

static int
check_return_type(RDB_operator *op, int argc, RDB_object *argv[],
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get operator by argument types to check if the return type matches */
    if (argc > 0) {
        int i;
        RDB_operator *top;
        RDB_type **argtv = RDB_alloc(sizeof (RDB_type *) * argc, ecp);
        if (argtv == NULL) {
            return RDB_ERROR;
        }
        for (i = 0; i < argc; i++) {
            argtv[i] = RDB_obj_type(argv[i]);
            if (argtv[i] == NULL) {
                RDB_raise_invalid_argument("missing type information",  ecp);
                RDB_free(argtv);
                return RDB_ERROR;
            }
        }
        top = RDB_get_ro_op(RDB_operator_name(op), argc, argtv, envp, ecp, txp);
        RDB_free(argtv);
        if (top == NULL)
            return RDB_ERROR;
        if (!RDB_type_equals(RDB_operator_type(op), RDB_operator_type(top))) {
            RDB_raise_type_mismatch("return type mismatch",  ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

/*
 * Get operator by name and values.
 * If txp is NULL and envp is not NULL
 * envp will be used to look up the operator in memory.
 * If txp is not NULL, envp is ignored.
 */
RDB_operator *
RDB_get_ro_op_by_args(const char *name, int argc, RDB_object *argv[],
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *errtyp;
    RDB_operator *op;
    RDB_dbroot *dbrootp;
    RDB_bool typmismatch = RDB_FALSE;

    /* Lookup operator in built-in operator map */
    op = RDB_get_op_by_args(&RDB_builtin_ro_op_map, name, argc, argv, ecp);
    if (op != NULL) {
        if (check_return_type(op, argc, argv, envp, ecp, txp) != RDB_OK)
            return NULL;
        return op;
    }

    /*
     * If search in builtin type map failed due to a type mismatch,
     * keep that info and use it later to raise type_mismatch_error
     * instead of operator_not_found_error.
     */
    errtyp = RDB_obj_type(RDB_get_err(ecp));
    if (errtyp != &RDB_OPERATOR_NOT_FOUND_ERROR
            && errtyp != &RDB_TYPE_MISMATCH_ERROR) {
        return NULL;
    }
    if (errtyp == &RDB_TYPE_MISMATCH_ERROR) {
        typmismatch = RDB_TRUE;
    }
    RDB_clear_err(ecp);

    if (txp != NULL) {
        if (!RDB_tx_is_running(txp)) {
            RDB_raise_no_running_tx(ecp);
            return NULL;
        }
        dbrootp = RDB_tx_db(txp)->dbrootp;
    } else if (envp != NULL) {
        dbrootp = RDB_env_xdata(envp);
        if (dbrootp == NULL) {
            RDB_raise_operator_not_found(name, ecp);
            return NULL;
        }
    } else {
        if (typmismatch) {
            RDB_raise_type_mismatch(name, ecp);
        } else {
            RDB_raise_operator_not_found(name, ecp);
        }
        return NULL;
    }

    /* Lookup operator in dbroot map */
    op = RDB_get_op_by_args(&dbrootp->ro_opmap, name, argc, argv, ecp);
    if (op != NULL) {
        RDB_clear_err(ecp);
        if (check_return_type(op, argc, argv, envp, ecp, txp) != RDB_OK)
            return NULL;
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

    /*
     * Provide "=" and "<>" for types with possreps
     */
    if (argc == 2) {
        RDB_type *arg1typ = RDB_obj_type(argv[0]);
        if (RDB_type_is_scalar(arg1typ)
                && (!arg1typ->def.scalar.builtin || arg1typ->def.scalar.repc > 0)
                && RDB_share_subtype(arg1typ, RDB_obj_type(argv[1]))) {
            if (strcmp(name, "=") == 0
                    || strcmp(name, "<>") == 0) {
                return provide_eq_neq(name, arg1typ, dbrootp, ecp);
            }
        }
    }

    if (argc == 1) {
        if (strlen(name) > IS_PREFIX_LEN
                && strncmp(name, IS_PREFIX, IS_PREFIX_LEN) == 0
                && RDB_type_is_scalar(RDB_obj_type(argv[0]))) {
            RDB_type *ttyp = RDB_get_supertype_of_subtype(RDB_obj_type(argv[0]), name + IS_PREFIX_LEN);
            if (ttyp == NULL) {
                RDB_raise_type_mismatch(name + IS_PREFIX_LEN, ecp);
                return NULL;
            }
            return provide_is_op(name, dbrootp, ecp);
        }
        if (strlen(name) > TREAT_AS_PREFIX_LEN
                && strncmp(name, TREAT_AS_PREFIX, TREAT_AS_PREFIX_LEN) == 0
                && RDB_type_is_scalar(RDB_obj_type(argv[0]))) {
            RDB_type *ttyp = RDB_get_supertype_of_subtype(RDB_obj_type(argv[0]), name + TREAT_AS_PREFIX_LEN);
            if (ttyp == NULL) {
                RDB_raise_type_mismatch(name + TREAT_AS_PREFIX_LEN, ecp);
                return NULL;
            }
            return provide_treat_as_op(name, dbrootp, ttyp, ecp);
        }
    }

    /*
     * Operator was not found in map, so read from catalog
     * if a transaction is available
     */
    if (txp == NULL)
        return NULL;

    RDB_clear_err(ecp);

    if (RDB_cat_load_ro_op(name, ecp, txp) == (RDB_int) RDB_ERROR) {
        return NULL;
    }
    op = RDB_get_op_by_args(&dbrootp->ro_opmap, name, argc, argv, ecp);
    if (op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR) {
            if (typmismatch) {
                RDB_raise_type_mismatch(name, ecp);
            }
        }
        return NULL;
    }

    if (check_return_type(op, argc, argv, envp, ecp, txp) != RDB_OK)
        return NULL;
    return op;
}

int
RDB_add_selector(RDB_type *typ, RDB_exec_context *ecp)
{
    int i;
    RDB_operator *datap;
    int argc = typ->def.scalar.repv[0].compc;
    RDB_type **argtv = NULL;

    if (argc > 0) {
        argtv = RDB_alloc(sizeof(RDB_type *) * argc, ecp);
        if (argtv == NULL)
            return RDB_ERROR;
    }
    for (i = 0; i < argc; i++) {
        argtv[i] = typ->def.scalar.repv[0].compv[i].typ;
    }

    /*
     * Create RDB_operator and put it into read-only operator map
     */

    datap = RDB_new_op_data(typ->name, argc, argtv, typ, ecp);
    if (argtv != NULL)
        RDB_free(argtv);
    if (datap == NULL)
        goto error;
    datap->opfn.ro_fp = &RDB_op_sys_select;

    if (RDB_put_op(&RDB_builtin_ro_op_map, datap, ecp) != RDB_OK) {
        goto error;
    }
    return RDB_OK;

error:
    free(argtv);
    RDB_free_op_data(datap, ecp);
    return RDB_ERROR;
}
