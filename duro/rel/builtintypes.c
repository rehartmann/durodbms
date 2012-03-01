/*
 * $Id$
 *
 * Copyright (C) 2004 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "io.h"

#include <string.h>

/** @page builtin-types Built-in types
@section basic-types Basic data types

<table border="1" summary="Built-in basic data types">
<tr><th>Name<th>RDB_type variable<th>C type
<tr><td>boolean<td>RDB_BOOLEAN<td>RDB_bool
<tr><td>integer<td>RDB_INTEGER<td>RDB_int
<tr><td>float<td>RDB_FLOAT<td>RDB_float
<tr><td>string<td>RDB_STRING<td>char *
<tr><td>binary<td>RDB_BINARY<td>&nbsp;-
</table>

(Support for <code>binary</code> is incomplete.)

@section error-types Error types

Duro errors are scalar types.
They are shown below in Tutorial D notation.

<pre>
TYPE no_running_transaction_error POSSREP { };

TYPE invalid_argument_error POSSREP { msg string };

TYPE type_mismatch_error POSSREP { msg string };

TYPE not_found_error POSSREP { msg string };

TYPE operator_not_found_error POSSREP { msg string };

TYPE rdb_name_error POSSREP { msg string };

TYPE element_exists_error POSSREP { msg string };

TYPE type_constraint_violation_error POSSREP { msg string };

TYPE key_violation_error POSSREP { msg string };

TYPE predicate_violation_error POSSREP { msg string };

TYPE aggregate_undefined_error POSSREP { };

TYPE version_mismatch_error POSSREP { };

TYPE not_supported_error POSSREP { msg string };

TYPE syntax_error possrep { msg string };

TYPE in_use_error possrep { msg string };
</pre>

@subsection system-errors System errors

<pre>
TYPE no_memory_error POSSREP {  };
</pre>

Insufficient memory.

<pre>
TYPE system_error POSSREP { msg string };
</pre>

Unspecified system error.

<pre>
TYPE lock_not_granted_error POSSREP { };
</pre>

A lock was requested but could not be granted.

<pre>
TYPE deadlock_error POSSREP { };
</pre>

A deadlock condition was detected.

<pre>
TYPE resource_not_found_error POSSREP { msg string };
</pre>

A system resource, usually a file, could not be found.

<pre>
TYPE internal_error POSSREP { msg string };
</pre>

Internal error.

<pre>
TYPE fatal_error POSSREP { };
</pre>

Fatal error. This means that future calls to Duro functions
will most likely fail.

*/

RDB_type RDB_BOOLEAN;
RDB_type RDB_INTEGER;
RDB_type RDB_FLOAT;
RDB_type RDB_STRING;
RDB_type RDB_BINARY;

RDB_type RDB_NO_RUNNING_TX_ERROR;
RDB_type RDB_INVALID_ARGUMENT_ERROR;
RDB_type RDB_TYPE_MISMATCH_ERROR;
RDB_type RDB_NOT_FOUND_ERROR;
RDB_type RDB_OPERATOR_NOT_FOUND_ERROR;
RDB_type RDB_NAME_ERROR;
RDB_type RDB_ELEMENT_EXISTS_ERROR;
RDB_type RDB_TYPE_CONSTRAINT_VIOLATION_ERROR;
RDB_type RDB_KEY_VIOLATION_ERROR;
RDB_type RDB_PREDICATE_VIOLATION_ERROR;
RDB_type RDB_AGGREGATE_UNDEFINED_ERROR;
RDB_type RDB_VERSION_MISMATCH_ERROR;
RDB_type RDB_NOT_SUPPORTED_ERROR;
RDB_type RDB_IN_USE_ERROR;
RDB_type RDB_NO_MEMORY_ERROR;
RDB_type RDB_LOCK_NOT_GRANTED_ERROR;
RDB_type RDB_DEADLOCK_ERROR;
RDB_type RDB_RESOURCE_NOT_FOUND_ERROR;
RDB_type RDB_INTERNAL_ERROR;
RDB_type RDB_FATAL_ERROR;
RDB_type RDB_SYSTEM_ERROR;

RDB_type RDB_SYNTAX_ERROR;

RDB_hashmap _RDB_builtin_type_map;

static int
compare_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->val.int_val - argv[1]->val.int_val);
    return RDB_OK;
}

static int
compare_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int res;

    if (argv[0]->val.float_val < argv[1]->val.float_val) {
        res = -1;
    } else if (argv[0]->val.float_val > argv[1]->val.float_val) {
        res = 1;
    } else {
        res = 0;
    }
    RDB_int_to_obj(retvalp, res);
    return RDB_OK;
}

static int
compare_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp,
            strcoll(argv[0]->val.bin.datap, argv[1]->val.bin.datap));
    return RDB_OK;
}

int
_RDB_add_type(RDB_type *typ, RDB_exec_context *ecp)
{
    int ret = RDB_hashmap_put(&_RDB_builtin_type_map, RDB_type_name(typ), typ);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, NULL);
        return RDB_ERROR;
    }

    /*
     * Add selector if the type has a possrep (applies to error types)
     */
    if (typ->def.scalar.repc == 1) {
        if (_RDB_add_selector(typ, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    return RDB_OK;
}

int
_RDB_init_builtin_types(RDB_exec_context *ecp)
{
    static RDB_bool initialized = RDB_FALSE;

    /*
     * Add error types
     */

    static RDB_possrep no_memory_rep = {
        "no_memory_error",
        0
    };

    static RDB_possrep no_running_tx_rep = {
        "no_running_tx_error",
        0
    };

    static RDB_attr not_found_comp = { "msg", &RDB_STRING };

    static RDB_possrep not_found_rep = {
        "not_found_error",
        1,
        &not_found_comp
    };

    static RDB_attr type_mismatch_comp = { "msg", &RDB_STRING };

    static RDB_possrep type_mismatch_rep = {
        "type_mismatch_error",
        1,
        &type_mismatch_comp
    };

    static RDB_attr invalid_argument_comp = { "msg", &RDB_STRING };

    static RDB_possrep invalid_argument_rep = {
        "invalid_argument_error",
        1,
        &invalid_argument_comp
    };

    static RDB_attr type_constraint_violation_comp = { "msg", &RDB_STRING };

    static RDB_possrep type_constraint_violation_rep = {
        "type_constraint_violation_error",
        1,
        &type_constraint_violation_comp
    };

    static RDB_attr operator_not_found_comp = { "msg", &RDB_STRING };

    static RDB_possrep operator_not_found_rep = {
        "operator_not_found_error",
        1,
        &operator_not_found_comp
    };

    static RDB_attr element_exists_comp = { "msg", &RDB_STRING };

    static RDB_possrep element_exists_rep = {
        "element_exists_error",
        1,
        &element_exists_comp
    };

    static RDB_attr key_violation_comp = { "msg", &RDB_STRING };

    static RDB_possrep key_violation_rep = {
        "key_violation_error",
        1,
        &key_violation_comp
    };

    static RDB_attr not_supported_comp = { "msg", &RDB_STRING };

    static RDB_possrep not_supported_rep = {
        "not_supported_error",
        1,
        &not_supported_comp
    };

    static RDB_attr in_use_comp = { "msg", &RDB_STRING };

    static RDB_possrep in_use_rep = {
        "in_use_error",
        1,
        &in_use_comp
    };

    static RDB_attr name_comp = { "msg", &RDB_STRING };

    static RDB_possrep name_rep = {
        "name_error",
        1,
        &name_comp
    };

    static RDB_attr predicate_violation_comp = { "msg", &RDB_STRING };

    static RDB_possrep predicate_violation_rep = {
        "predicate_violation_error",
        1,
        &predicate_violation_comp
    };

    static RDB_attr system_comp = { "msg", &RDB_STRING };

    static RDB_possrep system_rep = {
        "system_error",
        1,
        &system_comp
    };

    static RDB_attr resource_not_found_comp = { "msg", &RDB_STRING };

    static RDB_possrep resource_not_found_rep = {
        "resource_not_found_error",
        1,
        &resource_not_found_comp
    };

    static RDB_attr internal_comp = { "msg", &RDB_STRING };

    static RDB_possrep internal_rep = {
        "internal_error",
        1,
        &internal_comp
    };

    static RDB_possrep lock_not_granted_rep = {
        "lock_not_granted_error",
        0,
    };

    static RDB_possrep aggregate_undefined_rep = {
        "aggregate_undefined_error",
        0,
    };

    static RDB_possrep version_mismatch_rep = {
        "version_mismatch_error",
        0,
    };

    static RDB_possrep deadlock_rep = {
        "deadlock_error",
        0,
    };

    static RDB_possrep fatal_rep = {
        "fatal_error",
        0,
    };

    static RDB_attr syntax_comp = { "msg", &RDB_STRING };

    static RDB_possrep syntax_rep = {
        "syntax_error",
        1,
        &syntax_comp
    };

    static RDB_operator compare_string_op = {
        "cmp",
        &RDB_INTEGER
    };

    static RDB_operator compare_int_op = {
        "cmp",
        &RDB_INTEGER
    };

    static RDB_operator compare_float_op = {
        "cmp",
        &RDB_INTEGER
    };

    if (initialized) {
        return RDB_OK;
    }
    initialized = RDB_TRUE;

    RDB_BOOLEAN.kind = RDB_TP_SCALAR;
    RDB_BOOLEAN.ireplen = 1;
    RDB_BOOLEAN.name = "boolean";
    RDB_BOOLEAN.def.scalar.builtin = RDB_TRUE;
    RDB_BOOLEAN.def.scalar.repc = 0;
    RDB_BOOLEAN.def.scalar.arep = NULL;
    RDB_BOOLEAN.def.scalar.constraintp = NULL;
    RDB_BOOLEAN.compare_op = NULL;

    compare_string_op.opfn.ro_fp = &compare_string;

    RDB_STRING.kind = RDB_TP_SCALAR;
    RDB_STRING.ireplen = RDB_VARIABLE_LEN;
    RDB_STRING.name = "string";
    RDB_STRING.def.scalar.builtin = RDB_TRUE;
    RDB_STRING.def.scalar.repc = 0;
    RDB_STRING.def.scalar.arep = NULL;
    RDB_STRING.def.scalar.constraintp = NULL;
    RDB_STRING.compare_op = &compare_string_op;

    compare_int_op.opfn.ro_fp = &compare_int;

    RDB_INTEGER.kind = RDB_TP_SCALAR;
    RDB_INTEGER.ireplen = sizeof (RDB_int);
    RDB_INTEGER.name = "integer";
    RDB_INTEGER.def.scalar.builtin = RDB_TRUE;
    RDB_INTEGER.def.scalar.repc = 0;
    RDB_INTEGER.def.scalar.arep = NULL;
    RDB_INTEGER.def.scalar.constraintp = NULL;
    RDB_INTEGER.compare_op = &compare_int_op;

    compare_float_op.opfn.ro_fp = &compare_float;

    RDB_FLOAT.kind = RDB_TP_SCALAR;
    RDB_FLOAT.ireplen = sizeof (RDB_float);
    RDB_FLOAT.name = "float";
    RDB_FLOAT.def.scalar.builtin = RDB_TRUE;
    RDB_FLOAT.def.scalar.repc = 0;
    RDB_FLOAT.def.scalar.arep = NULL;
    RDB_FLOAT.def.scalar.constraintp = NULL;
    RDB_FLOAT.compare_op = &compare_float_op;

    RDB_BINARY.kind = RDB_TP_SCALAR;
    RDB_BINARY.ireplen = RDB_VARIABLE_LEN;
    RDB_BINARY.name = "binary";
    RDB_BINARY.def.scalar.repc = 0;
    RDB_BINARY.def.scalar.arep = NULL;
    RDB_BINARY.def.scalar.builtin = RDB_TRUE;
    RDB_BINARY.def.scalar.constraintp = NULL;
    RDB_BINARY.compare_op = NULL;

    RDB_NO_MEMORY_ERROR.kind = RDB_TP_SCALAR;
    RDB_NO_MEMORY_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NO_MEMORY_ERROR.name = "no_memory_error";
    RDB_NO_MEMORY_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NO_MEMORY_ERROR.def.scalar.repc = 1;
    RDB_NO_MEMORY_ERROR.def.scalar.repv = &no_memory_rep;
    RDB_NO_MEMORY_ERROR.def.scalar.arep = RDB_new_tuple_type(0, NULL, ecp);
    if (RDB_NO_MEMORY_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_NO_MEMORY_ERROR.def.scalar.constraintp = NULL;
    RDB_NO_MEMORY_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NO_MEMORY_ERROR.compare_op = NULL;

    RDB_NO_RUNNING_TX_ERROR.kind = RDB_TP_SCALAR;
    RDB_NO_RUNNING_TX_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NO_RUNNING_TX_ERROR.name = "no_running_tx_error";
    RDB_NO_RUNNING_TX_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NO_RUNNING_TX_ERROR.def.scalar.repc = 1;
    RDB_NO_RUNNING_TX_ERROR.def.scalar.repv = &no_running_tx_rep;
    RDB_NO_RUNNING_TX_ERROR.def.scalar.arep = RDB_new_tuple_type(0, NULL, ecp);
    if (RDB_NO_RUNNING_TX_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_NO_RUNNING_TX_ERROR.def.scalar.arep = NULL;
    RDB_NO_RUNNING_TX_ERROR.def.scalar.constraintp = NULL;
    RDB_NO_RUNNING_TX_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NO_RUNNING_TX_ERROR.compare_op = NULL;

    RDB_NOT_FOUND_ERROR.kind = RDB_TP_SCALAR;
    RDB_NOT_FOUND_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NOT_FOUND_ERROR.name = "not_found_error";
    RDB_NOT_FOUND_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NOT_FOUND_ERROR.def.scalar.repc = 1;
    RDB_NOT_FOUND_ERROR.def.scalar.repv = &not_found_rep;
    RDB_NOT_FOUND_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_NOT_FOUND_ERROR.def.scalar.constraintp = NULL;
    RDB_NOT_FOUND_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NOT_FOUND_ERROR.compare_op = NULL;

    RDB_INVALID_ARGUMENT_ERROR.kind = RDB_TP_SCALAR;
    RDB_INVALID_ARGUMENT_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_INVALID_ARGUMENT_ERROR.name = "invalid_argument_error";
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.repc = 1;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.repv = &invalid_argument_rep;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.constraintp = NULL;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_INVALID_ARGUMENT_ERROR.compare_op = NULL;

    RDB_TYPE_MISMATCH_ERROR.kind = RDB_TP_SCALAR;
    RDB_TYPE_MISMATCH_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_TYPE_MISMATCH_ERROR.name = "type_mismatch_error";
    RDB_TYPE_MISMATCH_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.repc = 1;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.repv = &type_mismatch_rep;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.constraintp = NULL;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_TYPE_MISMATCH_ERROR.compare_op = NULL;

    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.name = "type_constraint_violation_error";
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.def.scalar.repc = 1;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.def.scalar.repv =
                &type_constraint_violation_rep;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.def.scalar.constraintp = NULL;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.compare_op = NULL;

    RDB_OPERATOR_NOT_FOUND_ERROR.kind = RDB_TP_SCALAR;
    RDB_OPERATOR_NOT_FOUND_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_OPERATOR_NOT_FOUND_ERROR.name = "operator_not_found_error";
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.repc = 1;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.repv = &operator_not_found_rep;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.constraintp = NULL;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_OPERATOR_NOT_FOUND_ERROR.compare_op = NULL;

    RDB_ELEMENT_EXISTS_ERROR.kind = RDB_TP_SCALAR;
    RDB_ELEMENT_EXISTS_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_ELEMENT_EXISTS_ERROR.name = "element_exists_error";
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.repc = 1;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.repv = &element_exists_rep;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.constraintp = NULL;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_ELEMENT_EXISTS_ERROR.compare_op = NULL;

    RDB_KEY_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_KEY_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_KEY_VIOLATION_ERROR.name = "key_violation_error";
    RDB_KEY_VIOLATION_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_KEY_VIOLATION_ERROR.def.scalar.repc = 1;
    RDB_KEY_VIOLATION_ERROR.def.scalar.repv = &key_violation_rep;
    RDB_KEY_VIOLATION_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_KEY_VIOLATION_ERROR.def.scalar.constraintp = NULL;
    RDB_KEY_VIOLATION_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_KEY_VIOLATION_ERROR.compare_op = NULL;

    RDB_NOT_SUPPORTED_ERROR.kind = RDB_TP_SCALAR;
    RDB_NOT_SUPPORTED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NOT_SUPPORTED_ERROR.name = "not_supported_error";
    RDB_NOT_SUPPORTED_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.repc = 1;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.repv = &not_supported_rep;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.constraintp = NULL;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NOT_SUPPORTED_ERROR.compare_op = NULL;

    RDB_NAME_ERROR.kind = RDB_TP_SCALAR;
    RDB_NAME_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NAME_ERROR.name = "name_error";
    RDB_NAME_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NAME_ERROR.def.scalar.repc = 1;
    RDB_NAME_ERROR.def.scalar.repv = &name_rep;
    RDB_NAME_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_NAME_ERROR.def.scalar.constraintp = NULL;
    RDB_NAME_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NAME_ERROR.compare_op = NULL;

    RDB_PREDICATE_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_PREDICATE_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_PREDICATE_VIOLATION_ERROR.name = "predicate_violation_error";
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.repc = 1;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.repv = &predicate_violation_rep;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.constraintp = NULL;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_PREDICATE_VIOLATION_ERROR.compare_op = NULL;

    RDB_IN_USE_ERROR.kind = RDB_TP_SCALAR;
    RDB_IN_USE_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_IN_USE_ERROR.name = "in_use_error";
    RDB_IN_USE_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_IN_USE_ERROR.def.scalar.repc = 1;
    RDB_IN_USE_ERROR.def.scalar.repv = &in_use_rep;
    RDB_IN_USE_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_IN_USE_ERROR.def.scalar.constraintp = NULL;
    RDB_IN_USE_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_IN_USE_ERROR.compare_op = NULL;

    RDB_SYSTEM_ERROR.kind = RDB_TP_SCALAR;
    RDB_SYSTEM_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_SYSTEM_ERROR.name = "system_error";
    RDB_SYSTEM_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_SYSTEM_ERROR.def.scalar.repc = 1;
    RDB_SYSTEM_ERROR.def.scalar.repv = &system_rep;
    RDB_SYSTEM_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_SYSTEM_ERROR.def.scalar.constraintp = NULL;
    RDB_SYSTEM_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_SYSTEM_ERROR.compare_op = NULL;

    RDB_RESOURCE_NOT_FOUND_ERROR.kind = RDB_TP_SCALAR;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_RESOURCE_NOT_FOUND_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_RESOURCE_NOT_FOUND_ERROR.name = "resource_not_found_error";
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.repc = 1;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.repv = &resource_not_found_rep;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.constraintp = NULL;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_RESOURCE_NOT_FOUND_ERROR.compare_op = NULL;

    RDB_INTERNAL_ERROR.kind = RDB_TP_SCALAR;
    RDB_INTERNAL_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_INTERNAL_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_INTERNAL_ERROR.name = "internal_error";
    RDB_INTERNAL_ERROR.def.scalar.repc = 1;
    RDB_INTERNAL_ERROR.def.scalar.repv = &internal_rep;
    RDB_INTERNAL_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_INTERNAL_ERROR.def.scalar.constraintp = NULL;
    RDB_INTERNAL_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_INTERNAL_ERROR.compare_op = NULL;

    RDB_LOCK_NOT_GRANTED_ERROR.kind = RDB_TP_SCALAR;
    RDB_LOCK_NOT_GRANTED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_LOCK_NOT_GRANTED_ERROR.name = "lock_not_granted_error";
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.repc = 1;
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.repv = &lock_not_granted_rep;
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.arep =
            RDB_new_tuple_type(0, NULL, ecp);
    if (RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.constraintp = NULL;
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_LOCK_NOT_GRANTED_ERROR.compare_op = NULL;

    RDB_AGGREGATE_UNDEFINED_ERROR.kind = RDB_TP_SCALAR;
    RDB_AGGREGATE_UNDEFINED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_AGGREGATE_UNDEFINED_ERROR.name = "aggregate_undefined_error";
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.repc = 1;
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.repv = &aggregate_undefined_rep;
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.arep =
            RDB_new_tuple_type(0, NULL, ecp);
    if (RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.constraintp = NULL;
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_AGGREGATE_UNDEFINED_ERROR.compare_op = NULL;

    RDB_VERSION_MISMATCH_ERROR.kind = RDB_TP_SCALAR;
    RDB_VERSION_MISMATCH_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_VERSION_MISMATCH_ERROR.name = "version_mismatch_error";
    RDB_VERSION_MISMATCH_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_VERSION_MISMATCH_ERROR.def.scalar.repc = 1;
    RDB_VERSION_MISMATCH_ERROR.def.scalar.repv = &version_mismatch_rep;
    RDB_VERSION_MISMATCH_ERROR.def.scalar.arep =
            RDB_new_tuple_type(0, NULL, ecp);
    if (RDB_VERSION_MISMATCH_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_VERSION_MISMATCH_ERROR.def.scalar.constraintp = NULL;
    RDB_VERSION_MISMATCH_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_VERSION_MISMATCH_ERROR.compare_op = NULL;

    RDB_DEADLOCK_ERROR.kind = RDB_TP_SCALAR;
    RDB_DEADLOCK_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_DEADLOCK_ERROR.name = "deadlock_error";
    RDB_DEADLOCK_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_DEADLOCK_ERROR.def.scalar.repc = 1;
    RDB_DEADLOCK_ERROR.def.scalar.repv = &deadlock_rep;
    RDB_DEADLOCK_ERROR.def.scalar.arep =
            RDB_new_tuple_type(0, NULL, ecp);
    if (RDB_DEADLOCK_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_DEADLOCK_ERROR.def.scalar.constraintp = NULL;
    RDB_DEADLOCK_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_DEADLOCK_ERROR.compare_op = NULL;

    RDB_FATAL_ERROR.kind = RDB_TP_SCALAR;
    RDB_FATAL_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_FATAL_ERROR.name = "fatal_error";
    RDB_FATAL_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_FATAL_ERROR.def.scalar.repc = 1;
    RDB_FATAL_ERROR.def.scalar.repv = &fatal_rep;
    RDB_FATAL_ERROR.def.scalar.arep =
            RDB_new_tuple_type(0, NULL, ecp);
    if (RDB_FATAL_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_FATAL_ERROR.def.scalar.constraintp = NULL;
    RDB_FATAL_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_FATAL_ERROR.compare_op = NULL;

    RDB_SYNTAX_ERROR.kind = RDB_TP_SCALAR;
    RDB_SYNTAX_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_SYNTAX_ERROR.name = "syntax_error";
    RDB_SYNTAX_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_SYNTAX_ERROR.def.scalar.repc = 1;
    RDB_SYNTAX_ERROR.def.scalar.repv = &syntax_rep;
    RDB_SYNTAX_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_SYNTAX_ERROR.def.scalar.constraintp = NULL;
    RDB_SYNTAX_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_SYNTAX_ERROR.compare_op = NULL;

    RDB_init_hashmap(&_RDB_builtin_type_map, 32);

    /*
     * Put built-in types into type map
     */
    if (_RDB_add_type(&RDB_BOOLEAN, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_INTEGER, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_FLOAT, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_STRING, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_BINARY, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_NO_MEMORY_ERROR, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_NOT_FOUND_ERROR, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_NO_RUNNING_TX_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_INVALID_ARGUMENT_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_TYPE_MISMATCH_ERROR, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_TYPE_CONSTRAINT_VIOLATION_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_OPERATOR_NOT_FOUND_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_ELEMENT_EXISTS_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_KEY_VIOLATION_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_NOT_SUPPORTED_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_NAME_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_PREDICATE_VIOLATION_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_IN_USE_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_SYSTEM_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_RESOURCE_NOT_FOUND_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_INTERNAL_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_LOCK_NOT_GRANTED_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_AGGREGATE_UNDEFINED_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_VERSION_MISMATCH_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_DEADLOCK_ERROR,
            ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_FATAL_ERROR, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_add_type(&RDB_SYNTAX_ERROR, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    /* Add io_stream */
    if (_RDB_add_io(ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    return RDB_OK;
}
