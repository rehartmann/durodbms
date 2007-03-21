/*
 * $Id$
 *
 * Copyright (C) 2004-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include <gen/strfns.h>

#include <string.h>
#include <stdlib.h>

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
<sym>(const char *name, int argc, RDB_object *argv[],
          const void *iargp, size_t iarglen, RDB_exec_context *ecp, RDB_transaction *txp,
          RDB_object *retvalp)
@endverbatim

When the function is executed, the name of the operator is passed through <var>name</var>
and the arguments are passed through <var>argc</var> and <var>argv</var>.

The function specified by <var>sym</var> must store the result at the
location specified by <var>retvalp</var> and return RDB_OK.
It can indicate an error condition by leaving an error in *<var>ecp</var>
(see RDB_raise_err())
and returning RDB_ERROR.

If <var>iargp</var> is not NULL, it must point to a byte block
of length <var>iarglen</var> which will be passed to the function
specified by <var>sym</var>.
This can be used to pass code to an interpreter function.

Overloading operators is possible.

Array-valued argument and return types are not supported.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_ELEMENT_EXIST_ERROR
<dd>A read-only operator with this name and signature does already exist.
<dt>RDB_NOT_SUPPORTED_ERROR
<dd>One of the argument types or the return type is an array type.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_ro_op(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const void *iargp, size_t iarglen, 
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object rtypobj;
    RDB_object typesobj;
    int ret;
    int i;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Array types are not supported as argument or return types
     */
    for (i = 0; i < argc; i++) {
        if (argtv[i]->kind == RDB_TP_ARRAY) {
            RDB_raise_not_supported(
                    "array type not supported as argument type", ecp);
            return RDB_ERROR;
        }
    }
    if (rtyp->kind == RDB_TP_ARRAY) {
        RDB_raise_not_supported("array type not supported as return type",
                ecp);
        return RDB_ERROR;
    }

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
    ret = _RDB_make_typesobj(argc, argtv, ecp, &typesobj);
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
    if (strcmp(name, "CMP") == 0 && argc == 2
            && argtv[0] == argtv[1]
            && (argtv[0]->kind != RDB_TP_SCALAR
                    || !argtv[0]->var.scalar.builtin)) {
        RDB_ro_op_desc *cmpop;

        ret = _RDB_get_ro_op(name, argc, argtv, ecp, txp, &cmpop);
        if (ret != RDB_OK)
            goto cleanup;
        argtv[0]->comparep = cmpop->funcp;
        argtv[0]->compare_iarglen = cmpop->iarg.var.bin.len;
        argtv[0]->compare_iargp = cmpop->iarg.var.bin.datap;
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
<sym>(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
@endverbatim

When the function is executed, the name of the operator is passed through <var>name</var>
and the arguments are passed through <var>argc</var> and <var>argv</var>.

On success, the function specified by <var>sym</var> must return RDB_OK.
It can indicate an error condition by leaving an error in *<var>ecp</var>
(see RDB_raise_err() and related functions)
and returning RDB_ERROR.

If <var>iargp</var> is not NULL, it must point to a byte block
of length <var>iarglen</var> which will be passed to the function
specified by <var>sym</var>.
This can be used to pass code to an interpreter function.

Overloading operators is possible.

Array-valued argument types are not supported.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_ELEMENT_EXIST_ERROR
<dd>An update operator with this name and signature does already exist.
<dt>RDB_NOT_SUPPORTED_ERROR
<dd>One of the argument types is an array type.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_update_op(const char *name, int argc, RDB_type *argtv[],
                  RDB_bool updv[], const char *libname, const char *symname,
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
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Array types are not supported as argument types
     */
    for (i = 0; i < argc; i++) {
        if (argtv[i]->kind == RDB_TP_ARRAY) {
            RDB_raise_not_supported(
                    "array type not supported as argument type", ecp);
            return RDB_ERROR;
         }
    }

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
    RDB_set_array_length(&updvobj, (RDB_int) argc, ecp);
    for (i = 0; i < argc; i++) {
        RDB_bool_to_obj(&updobj, updv[i]);
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
    ret = _RDB_make_typesobj(argc, argtv, ecp, &typesobj);
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
    RDB_type **typv = malloc(sizeof (RDB_type *) * valc);

    if (typv == NULL)
        return NULL;
    for (i = 0; i < valc; i++) {
        if (valv[i]->kind == RDB_OB_TUPLE)
            typv[i] = _RDB_tuple_type(valv[i], ecp);
        else
            typv[i] = RDB_obj_type(valv[i]);
    }
    return typv;
}

/**
 * RDB_call_ro_op invokes the read-only operator with the name <var>name</var>,
passing the arguments in <var>argc</var> and <var>argv</var>.

The result will be stored at the location pointed to by
<var>retvalp</var>. 

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>A read-only operator that matches the name and argument types could not be
found.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>A read-only operator that matches <var>name</var> could be found,
but it does not match the argument types.
</dl>

If the user-supplied function which implements the function raises an
error, this error is returned in *ecp.

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_call_ro_op(const char *name, int argc, RDB_object *argv[],
               RDB_exec_context *ecp, RDB_transaction *txp,
               RDB_object *retvalp)
{
    RDB_ro_op_desc *op;
    int ret;
    RDB_type **argtv;
    int i;

    /*
     * Handle nonscalar comparison
     */
    if (argc == 2 && !obj_is_scalar(argv[0]) && !obj_is_scalar(argv[1])) {
        if (strcmp(name, "=") == 0)
            return _RDB_obj_equals(name, 2, argv, NULL, 0, ecp, txp, retvalp);
        if (strcmp(name, "<>") == 0) {
            ret = _RDB_obj_equals(name, 2, argv, NULL, 0, ecp, txp, retvalp);
            if (ret != RDB_OK)
                return ret;
            retvalp->var.bool_val = (RDB_bool) !retvalp->var.bool_val;
            return RDB_OK;
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
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_get_ro_op(name, argc, argtv, ecp, txp, &op);
    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TUPLE)
            RDB_drop_type(argtv[i], ecp, NULL);
    }

    free(argtv);

    if (ret != RDB_OK) {
        goto error;
    }

    /* Set return type to make it available to the function */
    retvalp->typ = op->rtyp;

    ret = (*op->funcp)(name, argc, argv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, ecp, txp, retvalp);
    if (ret != RDB_OK)
        goto error;

    /* Check type constraint if the operator is a selector */
    if (retvalp->typ != NULL &&_RDB_get_possrep(retvalp->typ, name) != NULL) {
        ret = _RDB_check_type_constraint(retvalp, ecp, txp);
        if (ret != RDB_OK) {
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
 * RDB_call_update_op invokes the update operator with the name <var>name</var>,
passing the arguments in <var>argc</var> and <var>argv</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>An update operator that matches the name and arguments could not be
found.
</dl>

If the user-supplied function which implements the operator raises an
error, this error is returned in *ecp.

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_call_update_op(const char *name, int argc, RDB_object *argv[],
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_upd_op_data *op;
    RDB_type **argtv;
    int i;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    argtv = valv_to_typev(argc, argv, ecp);
    if (argtv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op = _RDB_get_upd_op(name, argc, argtv, ecp, txp);
    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TUPLE)
            RDB_drop_type(argtv[i], ecp, NULL);
    }
    free(argtv);
    if (op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            RDB_raise_operator_not_found(name, ecp);
        }
        return RDB_ERROR;
    }

    return (*op->funcp)(name, argc, argv, op->updv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, ecp, txp);
}

/**
 * RDB_drop_op deletes the operator with the name <var>name</var>
from the database. This affects all overloaded versions.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_NOT_FOUND_ERROR
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
        RDB_raise_invalid_tx(ecp);
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
        if (ret == RDB_ERROR) {
            return RDB_ERROR;
        }        
    } else {
        /* It's a read-only operator */
        RDB_ro_op_desc *oldop;
        RDB_ro_op_desc *op = NULL;

        /* Delete all versions of readonly operator from hashmap */
        oldop = RDB_hashmap_get(&txp->dbp->dbrootp->ro_opmap, name);
        if (oldop != NULL)
            _RDB_free_ro_ops(oldop, ecp);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->ro_opmap, name, op);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }

        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_var_ref("NAME", ecp), RDB_string_to_expr(name, ecp),
               ecp);
        if (exp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->ro_ops_tbp, exp, ecp, txp);
        if (ret == RDB_ERROR) {
            RDB_drop_expr(exp, ecp);
            _RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }
    }

    return RDB_OK;
}

/*@}*/

static void
free_ro_op(RDB_ro_op_desc *op, RDB_exec_context *ecp)
{
    int i;

    free(op->name);
    if (op->argtv != NULL) {
        for (i = 0; i < op->argc; i++) {
            if (RDB_type_name(op->argtv[i]) == NULL)
                RDB_drop_type(op->argtv[i], ecp, NULL);
        }
        free(op->argtv);
    }
    if (op->rtyp != NULL && RDB_type_name(op->rtyp) == NULL)
        RDB_drop_type(op->rtyp, ecp, NULL);
    if (op->modhdl != NULL) {
        /* Built-in operator */
        lt_dlclose(op->modhdl);
        RDB_destroy_obj(&op->iarg, ecp);
    }
    free(op);
}

void
_RDB_free_ro_ops(RDB_ro_op_desc *op, RDB_exec_context *ecp)
{
    do {
        RDB_ro_op_desc *nextop = op->nextp;
        free_ro_op(op, ecp);
        op = nextop;
    } while (op != NULL);
}

int
_RDB_eq_bool(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val == argv[1]->var.bool_val));
    return RDB_OK;
}

int
_RDB_eq_binary(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
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
_RDB_obj_equals(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
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
        if (argv[0]->typ->comparep != NULL) {
            arep = argv[0]->typ;
        } else if (argv[0]->typ->var.scalar.arep != NULL) {
            arep = argv[0]->typ->var.scalar.arep;
        }
    }

    /* If yes, call it */
    if (arep != NULL && arep->comparep != NULL) {
        RDB_object retval;

        RDB_init_obj(&retval);
        retval.typ = &RDB_INTEGER;
        ret = (*arep->comparep)("CMP", 2, argv, arep->compare_iargp,
                arep->compare_iarglen, ecp, txp, &retval);
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
            return _RDB_eq_bool("=", 2, argv, NULL, 0, ecp, txp, retvalp);
        case RDB_OB_INT:
        case RDB_OB_FLOAT:
        case RDB_OB_DOUBLE:
            /* Must not happen, because there must be a comparsion function */
            RDB_raise_internal("missing comparison function", ecp);
            return RDB_ERROR;
        case RDB_OB_BIN:
            return _RDB_eq_binary("=", 2, argv, NULL, 0, ecp, txp, retvalp);
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
_RDB_obj_not_equals(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret = _RDB_obj_equals("=", 2, argv, NULL, 0, ecp, txp, retvalp);
    if (ret != RDB_OK)
        return ret;
    retvalp->var.bool_val = (RDB_bool) !retvalp->var.bool_val;
    return RDB_OK;
}

RDB_ro_op_desc *
_RDB_new_ro_op(const char *name, int argc, RDB_type *rtyp,
        RDB_ro_op_func *funcp, RDB_exec_context *ecp)
{
    RDB_ro_op_desc *op = malloc(sizeof (RDB_ro_op_desc));
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    op->name = RDB_dup_str(name);
    if (op->name == NULL) {
        free(op);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    if (argc > 0) {
        op->argc = argc;
        op->argtv = malloc(sizeof (RDB_type *) * argc);
        if (op->argtv == NULL) {
            free(op->name);
            free(op);
            RDB_raise_no_memory(ecp);
            return NULL;
        }
    } else {
        op->argtv = NULL;
    }

    op->rtyp = rtyp;
    op->funcp = funcp;
    op->modhdl = NULL;    

    return op;
}

int
_RDB_put_ro_op(RDB_dbroot *dbrootp, RDB_ro_op_desc *op, RDB_exec_context *ecp)
{
    int ret;
    RDB_ro_op_desc *fop = RDB_hashmap_get(&dbrootp->ro_opmap, op->name);

    if (fop == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&dbrootp->ro_opmap, op->name, op);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, NULL);
            return RDB_ERROR;
        }
    } else {
        op->nextp = fop->nextp;
        fop->nextp = op;
    }
    return RDB_OK;
}

static RDB_ro_op_desc *
get_ro_op(RDB_hashmap *opmap, const char *name,
        int argc, RDB_type *argtv[], RDB_exec_context *ecp)
{
    RDB_ro_op_desc *rop;
    RDB_bool pm = RDB_FALSE;

    rop = RDB_hashmap_get(opmap, name);

    /* Search for an operator with same signature */
    while (rop != NULL) {
        if (rop->argtv != NULL && rop->argc == argc) {
            int i;

            pm = RDB_TRUE;
            for (i = 0; (i < argc)
                    && RDB_type_equals(rop->argtv[i], argtv[i]);
                 i++);
            if (i == argc) {
                /* Found */
                return rop;
            }
        }
        rop = rop->nextp;
    }

    if (pm) {
        RDB_raise_type_mismatch("", ecp);
    } else {
        RDB_raise_operator_not_found(name, ecp);
    }
    return NULL;
}

int
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_exec_context *ecp, RDB_transaction *txp,
               RDB_ro_op_desc **opp)
{
    int i;
    int ret;
    RDB_bool typnull = RDB_FALSE;
    RDB_bool typmismatch = RDB_FALSE;

    /* Check one of the types if NULL */
    for (i = 0; i < argc && !typnull; i++) {
        if (argtv[i] == NULL) {
            typnull = RDB_TRUE;
        }
    }

    if (!typnull) {
        RDB_type *errtyp;

        /* Lookup operator in built-in operator map */
        *opp = get_ro_op(&_RDB_builtin_ro_op_map, name, argc, argtv, ecp);
        if (*opp != NULL)
            return RDB_OK;

        errtyp = RDB_obj_type(RDB_get_err(ecp));        
        if (errtyp != &RDB_OPERATOR_NOT_FOUND_ERROR
                && errtyp != &RDB_TYPE_MISMATCH_ERROR) {
            return RDB_ERROR;
        }
        if (errtyp == &RDB_TYPE_MISMATCH_ERROR) {
            typmismatch = RDB_TRUE;
        }

        if (txp == NULL)
            return RDB_ERROR;

        /* Lookup operator in dbroot map */
        *opp = get_ro_op(&txp->dbp->dbrootp->ro_opmap, name, argc, argtv, ecp);
        if (*opp != NULL)
            return RDB_OK;

        errtyp = RDB_obj_type(RDB_get_err(ecp));        
        if (errtyp != &RDB_OPERATOR_NOT_FOUND_ERROR
                && errtyp != &RDB_TYPE_MISMATCH_ERROR) {
            return RDB_ERROR;
        }
        if (errtyp == &RDB_TYPE_MISMATCH_ERROR) {
            typmismatch = RDB_TRUE;
        }
        RDB_clear_err(ecp);
    }

    /*
     * Search for a generic operator in built-in type map
     */
    *opp = RDB_hashmap_get(&_RDB_builtin_ro_op_map, name);
    while ((*opp) != NULL) {
        if ((*opp)->argtv == NULL) {
            /* Generic operator found */
            return RDB_OK;
        }
        *opp = (*opp)->nextp;
    } 

    /*
     * Search for a generic operator
     */
    *opp = RDB_hashmap_get(&txp->dbp->dbrootp->ro_opmap, name);
    while ((*opp) != NULL) {
        if ((*opp)->argtv == NULL) {
            /* Generic operator found */
            return RDB_OK;
        }
        *opp = (*opp)->nextp;
    } 

    /*
     * If one of the argument types is NULL and a generic operator
     * was not found, return with failure
     */
    if (typnull) {
        RDB_raise_operator_not_found(name, ecp);
        return RDB_ERROR;
    }

    /*
     * Provide "=" and "<>" for user-defined types
     */
    if (argc == 2 && RDB_type_equals(argtv[0], argtv[1])) {
        RDB_ro_op_desc *op;
        int ret;

        if (strcmp(name, "=") == 0) {
            op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, &_RDB_obj_equals, ecp);
            if (op == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            op->argtv[0] = RDB_dup_nonscalar_type(argtv[0], ecp);
            op->argtv[1] = RDB_dup_nonscalar_type(argtv[1], ecp);
            ret = _RDB_put_ro_op(txp->dbp->dbrootp, op, ecp);
            if (ret != RDB_OK)
                return ret;
            *opp = op;
            return RDB_OK;
        }
        if (strcmp(name, "<>") == 0) {
            op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
            if (op == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            op->argtv[0] = RDB_dup_nonscalar_type(argtv[0], ecp);
            op->argtv[1] = RDB_dup_nonscalar_type(argtv[1], ecp);
            ret = _RDB_put_ro_op(txp->dbp->dbrootp, op, ecp);
            if (ret != RDB_OK)
                return ret;
            *opp = op;
            return RDB_OK;
        }
    }

    /*
     * Operator was not found in map, so read from catalog
     */
    if (_RDB_cat_get_ro_op(name, argc, argtv, ecp, txp, opp) != RDB_OK) {
        _RDB_cat_get_ro_op(name, argc, argtv, ecp, txp, opp);
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            if (typmismatch) {
                RDB_raise_type_mismatch("", ecp);
            } else {
                RDB_raise_operator_not_found(name, ecp);
            }
        }
        return RDB_ERROR;
    }

    /* Insert operator into dbroot map */
    ret = _RDB_put_ro_op(txp->dbp->dbrootp, *opp, ecp);
    if (ret != RDB_OK) {
        free_ro_op(*opp, ecp);
        return ret;
    }

    return RDB_OK;
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

static void
free_upd_op_data(RDB_upd_op_data *op, RDB_exec_context *ecp)
{
    free(op->updv);
    lt_dlclose(op->modhdl);
    RDB_destroy_obj(&op->iarg, ecp);
    free(op);
}

RDB_upd_op_data *
_RDB_get_upd_op(const char *name, int argc, RDB_type *argtv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    
    RDB_upd_op_data *op = RDB_get_op(&txp->dbp->dbrootp->upd_opmap, name, argc, argtv);
    if (op == NULL) {
        op = _RDB_cat_get_upd_op(name, argc, argtv, ecp, txp);
        if (op == NULL) {
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                RDB_clear_err(ecp);
                RDB_raise_operator_not_found(name, ecp);
            }
            return NULL;
        }
        ret = RDB_put_op(&txp->dbp->dbrootp->upd_opmap, name, argc, argtv, op, ecp);
        if (ret != RDB_OK) {
            free_upd_op_data(op, ecp);
            return NULL;
        }
    }
    return op;
}

int
_RDB_add_selector(RDB_type *typ, RDB_exec_context *ecp)
{
    int i;
    RDB_ro_op_desc *op;

    if (_RDB_init_builtin_ops(ecp) != RDB_OK)
        return RDB_ERROR;

    op = _RDB_new_ro_op(typ->name,
            typ->var.scalar.repv[0].compc, typ, &_RDB_sys_select, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    for (i = 0; i < typ->var.scalar.repv[0].compc; i++) {
        op->argtv[i] = typ->var.scalar.repv[0].compv[i].typ;
    }

    return _RDB_put_builtin_ro_op(op, ecp);
}
