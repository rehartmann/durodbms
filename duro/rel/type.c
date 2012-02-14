/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "serialize.h"
#include "io.h"
#include <gen/strfns.h>

#include <string.h>
#include <locale.h>

/** @page builtin-types Built-in types
@section basic-types Basic data types

<table border="1" summary="Built-in basic data types">
<tr><th>Name<th>RDB_type variable<th>C type
<tr><td>BOOLEAN<td>RDB_BOOLEAN<td>RDB_bool
<tr><td>INTEGER<td>RDB_INTEGER<td>RDB_int
<tr><td>FLOAT<td>RDB_FLOAT<td>RDB_float
<tr><td>STRING<td>RDB_STRING<td>char *
<tr><td>BINARY<td>RDB_BINARY<td>&nbsp;-
</table>

(Support for BINARY is incomplete.)

@section error-types Error types

Duro errors are scalar types.
They are shown below in Tutorial D notation.

<pre>
TYPE NO_RUNNING_TRANSACTION_ERROR POSSREP { };

TYPE INVALID_ARGUMENT_ERROR POSSREP { MSG STRING };

TYPE TYPE_MISMATCH_ERROR POSSREP { MSG STRING };

TYPE NOT_FOUND_ERROR POSSREP { MSG STRING };

TYPE OPERATOR_NOT_FOUND_ERROR POSSREP { MSG STRING };

TYPE RDB_NAME_ERROR POSSREP { MSG STRING };

TYPE ELEMENT_EXISTS_ERROR POSSREP { MSG STRING };

TYPE TYPE_CONSTRAINT_VIOLATION_ERROR POSSREP { MSG STRING };

TYPE KEY_VIOLATION_ERROR POSSREP { MSG STRING };

TYPE PREDICATE_VIOLATION_ERROR POSSREP { MSG STRING };

TYPE AGGREGATE_UNDEFINED_ERROR POSSREP { };

TYPE VERSION_MISMATCH_ERROR POSSREP { };

TYPE NOT_SUPPORTED_ERROR POSSREP { MSG STRING };

TYPE SYNTAX_ERROR POSSREP { MSG STRING };

TYPE IN_USE_ERROR POSSREP { MSG STRING };
</pre>

@subsection system-errors System errors

<pre>
TYPE NO_MEMORY_ERROR POSSREP {  };
</pre>

Insufficient memory.

<pre>
TYPE SYSTEM_ERROR POSSREP { MSG STRING };
</pre>

Unspecified system error.

<pre>
TYPE LOCK_NOT_GRANTED_ERROR POSSREP { };
</pre>

A lock was requested but could not be granted.

<pre>
TYPE DEADLOCK_ERROR POSSREP { };
</pre>

A deadlock condition was detected.

<pre>
TYPE RESOURCE_NOT_FOUND_ERROR POSSREP { MSG STRING };
</pre>

A system resource, usually a file, could not be found.

<pre>
TYPE INTERNAL_ERROR POSSREP { MSG STRING };
</pre>

Internal error.

<pre>
TYPE FATAL_ERROR POSSREP { };
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
        "NO_MEMORY_ERROR",
        0
    };

    static RDB_possrep no_running_tx_rep = {
        "NO_RUNNING_TX_ERROR",
        0
    };

    static RDB_attr not_found_comp = { "MSG", &RDB_STRING };

    static RDB_possrep not_found_rep = {
        "NOT_FOUND_ERROR",
        1,
        &not_found_comp
    };

    static RDB_attr type_mismatch_comp = { "MSG", &RDB_STRING };

    static RDB_possrep type_mismatch_rep = {
        "TYPE_MISMATCH_ERROR",
        1,
        &type_mismatch_comp
    };

    static RDB_attr invalid_argument_comp = { "MSG", &RDB_STRING };

    static RDB_possrep invalid_argument_rep = {
        "INVALID_ARGUMENT_ERROR",
        1,
        &invalid_argument_comp
    };

    static RDB_attr type_constraint_violation_comp = { "MSG", &RDB_STRING };

    static RDB_possrep type_constraint_violation_rep = {
        "TYPE_CONSTRAINT_VIOLATION_ERROR",
        1,
        &type_constraint_violation_comp
    };

    static RDB_attr operator_not_found_comp = { "MSG", &RDB_STRING };

    static RDB_possrep operator_not_found_rep = {
        "OPERATOR_NOT_FOUND_ERROR",
        1,
        &operator_not_found_comp
    };

    static RDB_attr element_exists_comp = { "MSG", &RDB_STRING };

    static RDB_possrep element_exists_rep = {
        "ELEMENT_EXISTS_ERROR",
        1,
        &element_exists_comp
    };

    static RDB_attr key_violation_comp = { "MSG", &RDB_STRING };

    static RDB_possrep key_violation_rep = {
        "KEY_VIOLATION_ERROR",
        1,
        &key_violation_comp
    };

    static RDB_attr not_supported_comp = { "MSG", &RDB_STRING };

    static RDB_possrep not_supported_rep = {
        "NOT_SUPPORTED_ERROR",
        1,
        &not_supported_comp
    };

    static RDB_attr in_use_comp = { "MSG", &RDB_STRING };

    static RDB_possrep in_use_rep = {
        "IN_USE_ERROR",
        1,
        &in_use_comp
    };

    static RDB_attr name_comp = { "MSG", &RDB_STRING };

    static RDB_possrep name_rep = {
        "NAME_ERROR",
        1,
        &name_comp
    };

    static RDB_attr predicate_violation_comp = { "MSG", &RDB_STRING };

    static RDB_possrep predicate_violation_rep = {
        "PREDICATE_VIOLATION_ERROR",
        1,
        &predicate_violation_comp
    };

    static RDB_attr system_comp = { "MSG", &RDB_STRING };

    static RDB_possrep system_rep = {
        "SYSTEM_ERROR",
        1,
        &system_comp
    };

    static RDB_attr resource_not_found_comp = { "MSG", &RDB_STRING };

    static RDB_possrep resource_not_found_rep = {
        "RESOURCE_NOT_FOUND_ERROR",
        1,
        &resource_not_found_comp
    };

    static RDB_attr internal_comp = { "MSG", &RDB_STRING };

    static RDB_possrep internal_rep = {
        "INTERNAL_ERROR",
        1,
        &internal_comp
    };

    static RDB_possrep lock_not_granted_rep = {
        "LOCK_NOT_GRANTED_ERROR",
        0,
    };

    static RDB_possrep aggregate_undefined_rep = {
        "AGGREGATE_UNDEFINED_ERROR",
        0,
    };

    static RDB_possrep version_mismatch_rep = {
        "VERSION_MISMATCH_ERROR",
        0,
    };

    static RDB_possrep deadlock_rep = {
        "DEADLOCK_ERROR",
        0,
    };

    static RDB_possrep fatal_rep = {
        "FATAL_ERROR",
        0,
    };

    static RDB_attr syntax_comp = { "MSG", &RDB_STRING };

    static RDB_possrep syntax_rep = {
        "SYNTAX_ERROR",
        1,
        &syntax_comp
    };

    if (initialized) {
        return RDB_OK;
    }
    initialized = RDB_TRUE;

    RDB_BOOLEAN.kind = RDB_TP_SCALAR;
    RDB_BOOLEAN.ireplen = 1;
    RDB_BOOLEAN.name = "BOOLEAN";
    RDB_BOOLEAN.def.scalar.builtin = RDB_TRUE;
    RDB_BOOLEAN.def.scalar.repc = 0;
    RDB_BOOLEAN.def.scalar.arep = NULL;
    RDB_BOOLEAN.def.scalar.constraintp = NULL;
    RDB_BOOLEAN.compare_op = NULL;

    static RDB_operator compare_string_op = {
        "CMP",
        &RDB_INTEGER
    };
    compare_string_op.opfn.ro_fp = &compare_string;

    RDB_STRING.kind = RDB_TP_SCALAR;
    RDB_STRING.ireplen = RDB_VARIABLE_LEN;
    RDB_STRING.name = "STRING";
    RDB_STRING.def.scalar.builtin = RDB_TRUE;
    RDB_STRING.def.scalar.repc = 0;
    RDB_STRING.def.scalar.arep = NULL;
    RDB_STRING.def.scalar.constraintp = NULL;
    RDB_STRING.compare_op = &compare_string_op;

    static RDB_operator compare_int_op = {
        "CMP",
        &RDB_INTEGER
    };
    compare_int_op.opfn.ro_fp = &compare_int;

    RDB_INTEGER.kind = RDB_TP_SCALAR;
    RDB_INTEGER.ireplen = sizeof (RDB_int);
    RDB_INTEGER.name = "INTEGER";
    RDB_INTEGER.def.scalar.builtin = RDB_TRUE;
    RDB_INTEGER.def.scalar.repc = 0;
    RDB_INTEGER.def.scalar.arep = NULL;
    RDB_INTEGER.def.scalar.constraintp = NULL;
    RDB_INTEGER.compare_op = &compare_int_op;

    static RDB_operator compare_float_op = {
        "CMP",
        &RDB_INTEGER
    };
    compare_float_op.opfn.ro_fp = &compare_float;

    RDB_FLOAT.kind = RDB_TP_SCALAR;
    RDB_FLOAT.ireplen = sizeof (RDB_float);
    RDB_FLOAT.name = "FLOAT";
    RDB_FLOAT.def.scalar.builtin = RDB_TRUE;
    RDB_FLOAT.def.scalar.repc = 0;
    RDB_FLOAT.def.scalar.arep = NULL;
    RDB_FLOAT.def.scalar.constraintp = NULL;
    RDB_FLOAT.compare_op = &compare_float_op;

    RDB_BINARY.kind = RDB_TP_SCALAR;
    RDB_BINARY.ireplen = RDB_VARIABLE_LEN;
    RDB_BINARY.name = "BINARY";
    RDB_BINARY.def.scalar.repc = 0;
    RDB_BINARY.def.scalar.arep = NULL;
    RDB_BINARY.def.scalar.builtin = RDB_TRUE;
    RDB_BINARY.def.scalar.constraintp = NULL;
    RDB_BINARY.compare_op = NULL;

    RDB_NO_MEMORY_ERROR.kind = RDB_TP_SCALAR;
    RDB_NO_MEMORY_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NO_MEMORY_ERROR.name = "NO_MEMORY_ERROR";
    RDB_NO_MEMORY_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NO_MEMORY_ERROR.def.scalar.repc = 1;
    RDB_NO_MEMORY_ERROR.def.scalar.repv = &no_memory_rep;
    RDB_NO_MEMORY_ERROR.def.scalar.arep = RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_NO_MEMORY_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_NO_MEMORY_ERROR.def.scalar.constraintp = NULL;
    RDB_NO_MEMORY_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NO_MEMORY_ERROR.compare_op = NULL;

    RDB_NO_RUNNING_TX_ERROR.kind = RDB_TP_SCALAR;
    RDB_NO_RUNNING_TX_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NO_RUNNING_TX_ERROR.name = "NO_RUNNING_TX_ERROR";
    RDB_NO_RUNNING_TX_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NO_RUNNING_TX_ERROR.def.scalar.repc = 1;
    RDB_NO_RUNNING_TX_ERROR.def.scalar.repv = &no_running_tx_rep;
    RDB_NO_RUNNING_TX_ERROR.def.scalar.arep = RDB_create_tuple_type(0, NULL, ecp);
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
    RDB_NOT_FOUND_ERROR.name = "NOT_FOUND_ERROR";
    RDB_NOT_FOUND_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NOT_FOUND_ERROR.def.scalar.repc = 1;
    RDB_NOT_FOUND_ERROR.def.scalar.repv = &not_found_rep;
    RDB_NOT_FOUND_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_NOT_FOUND_ERROR.def.scalar.constraintp = NULL;
    RDB_NOT_FOUND_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NOT_FOUND_ERROR.compare_op = NULL;

    RDB_INVALID_ARGUMENT_ERROR.kind = RDB_TP_SCALAR;
    RDB_INVALID_ARGUMENT_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_INVALID_ARGUMENT_ERROR.name = "INVALID_ARGUMENT_ERROR";
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.repc = 1;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.repv = &invalid_argument_rep;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.constraintp = NULL;
    RDB_INVALID_ARGUMENT_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_INVALID_ARGUMENT_ERROR.compare_op = NULL;

    RDB_TYPE_MISMATCH_ERROR.kind = RDB_TP_SCALAR;
    RDB_TYPE_MISMATCH_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_TYPE_MISMATCH_ERROR.name = "TYPE_MISMATCH_ERROR";
    RDB_TYPE_MISMATCH_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.repc = 1;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.repv = &type_mismatch_rep;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.constraintp = NULL;
    RDB_TYPE_MISMATCH_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_TYPE_MISMATCH_ERROR.compare_op = NULL;

    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.name = "TYPE_CONSTRAINT_VIOLATION_ERROR";
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
    RDB_OPERATOR_NOT_FOUND_ERROR.name = "OPERATOR_NOT_FOUND_ERROR";
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.repc = 1;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.repv = &operator_not_found_rep;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.constraintp = NULL;
    RDB_OPERATOR_NOT_FOUND_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_OPERATOR_NOT_FOUND_ERROR.compare_op = NULL;

    RDB_ELEMENT_EXISTS_ERROR.kind = RDB_TP_SCALAR;
    RDB_ELEMENT_EXISTS_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_ELEMENT_EXISTS_ERROR.name = "ELEMENT_EXISTS_ERROR";
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.repc = 1;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.repv = &element_exists_rep;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.constraintp = NULL;
    RDB_ELEMENT_EXISTS_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_ELEMENT_EXISTS_ERROR.compare_op = NULL;

    RDB_KEY_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_KEY_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_KEY_VIOLATION_ERROR.name = "KEY_VIOLATION_ERROR";
    RDB_KEY_VIOLATION_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_KEY_VIOLATION_ERROR.def.scalar.repc = 1;
    RDB_KEY_VIOLATION_ERROR.def.scalar.repv = &key_violation_rep;
    RDB_KEY_VIOLATION_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_KEY_VIOLATION_ERROR.def.scalar.constraintp = NULL;
    RDB_KEY_VIOLATION_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_KEY_VIOLATION_ERROR.compare_op = NULL;

    RDB_NOT_SUPPORTED_ERROR.kind = RDB_TP_SCALAR;
    RDB_NOT_SUPPORTED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NOT_SUPPORTED_ERROR.name = "NOT_SUPPORTED_ERROR";
    RDB_NOT_SUPPORTED_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.repc = 1;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.repv = &not_supported_rep;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.constraintp = NULL;
    RDB_NOT_SUPPORTED_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NOT_SUPPORTED_ERROR.compare_op = NULL;

    RDB_NAME_ERROR.kind = RDB_TP_SCALAR;
    RDB_NAME_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NAME_ERROR.name = "NAME_ERROR";
    RDB_NAME_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_NAME_ERROR.def.scalar.repc = 1;
    RDB_NAME_ERROR.def.scalar.repv = &name_rep;
    RDB_NAME_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_NAME_ERROR.def.scalar.constraintp = NULL;
    RDB_NAME_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_NAME_ERROR.compare_op = NULL;

    RDB_PREDICATE_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_PREDICATE_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_PREDICATE_VIOLATION_ERROR.name = "PREDICATE_VIOLATION_ERROR";
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.repc = 1;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.repv = &predicate_violation_rep;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.constraintp = NULL;
    RDB_PREDICATE_VIOLATION_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_PREDICATE_VIOLATION_ERROR.compare_op = NULL;

    RDB_IN_USE_ERROR.kind = RDB_TP_SCALAR;
    RDB_IN_USE_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_IN_USE_ERROR.name = "IN_USE_ERROR";
    RDB_IN_USE_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_IN_USE_ERROR.def.scalar.repc = 1;
    RDB_IN_USE_ERROR.def.scalar.repv = &in_use_rep;
    RDB_IN_USE_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_IN_USE_ERROR.def.scalar.constraintp = NULL;
    RDB_IN_USE_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_IN_USE_ERROR.compare_op = NULL;

    RDB_SYSTEM_ERROR.kind = RDB_TP_SCALAR;
    RDB_SYSTEM_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_SYSTEM_ERROR.name = "SYSTEM_ERROR";
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
    RDB_RESOURCE_NOT_FOUND_ERROR.name = "RESOURCE_NOT_FOUND_ERROR";
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.repc = 1;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.repv = &resource_not_found_rep;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.constraintp = NULL;
    RDB_RESOURCE_NOT_FOUND_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_RESOURCE_NOT_FOUND_ERROR.compare_op = NULL;

    RDB_INTERNAL_ERROR.kind = RDB_TP_SCALAR;
    RDB_INTERNAL_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_INTERNAL_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_INTERNAL_ERROR.name = "INTERNAL_ERROR";
    RDB_INTERNAL_ERROR.def.scalar.repc = 1;
    RDB_INTERNAL_ERROR.def.scalar.repv = &internal_rep;
    RDB_INTERNAL_ERROR.def.scalar.arep = &RDB_STRING;
    RDB_INTERNAL_ERROR.def.scalar.constraintp = NULL;
    RDB_INTERNAL_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_INTERNAL_ERROR.compare_op = NULL;

    RDB_LOCK_NOT_GRANTED_ERROR.kind = RDB_TP_SCALAR;
    RDB_LOCK_NOT_GRANTED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_LOCK_NOT_GRANTED_ERROR.name = "LOCK_NOT_GRANTED_ERROR";
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.repc = 1;
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.repv = &lock_not_granted_rep;
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.constraintp = NULL;
    RDB_LOCK_NOT_GRANTED_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_LOCK_NOT_GRANTED_ERROR.compare_op = NULL;

    RDB_AGGREGATE_UNDEFINED_ERROR.kind = RDB_TP_SCALAR;
    RDB_AGGREGATE_UNDEFINED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_AGGREGATE_UNDEFINED_ERROR.name = "AGGREGATE_UNDEFINED_ERROR";
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.repc = 1;
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.repv = &aggregate_undefined_rep;
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.constraintp = NULL;
    RDB_AGGREGATE_UNDEFINED_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_AGGREGATE_UNDEFINED_ERROR.compare_op = NULL;

    RDB_VERSION_MISMATCH_ERROR.kind = RDB_TP_SCALAR;
    RDB_VERSION_MISMATCH_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_VERSION_MISMATCH_ERROR.name = "VERSION_MISMATCH_ERROR";
    RDB_VERSION_MISMATCH_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_VERSION_MISMATCH_ERROR.def.scalar.repc = 1;
    RDB_VERSION_MISMATCH_ERROR.def.scalar.repv = &version_mismatch_rep;
    RDB_VERSION_MISMATCH_ERROR.def.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_VERSION_MISMATCH_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_VERSION_MISMATCH_ERROR.def.scalar.constraintp = NULL;
    RDB_VERSION_MISMATCH_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_VERSION_MISMATCH_ERROR.compare_op = NULL;

    RDB_DEADLOCK_ERROR.kind = RDB_TP_SCALAR;
    RDB_DEADLOCK_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_DEADLOCK_ERROR.name = "DEADLOCK_ERROR";
    RDB_DEADLOCK_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_DEADLOCK_ERROR.def.scalar.repc = 1;
    RDB_DEADLOCK_ERROR.def.scalar.repv = &deadlock_rep;
    RDB_DEADLOCK_ERROR.def.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_DEADLOCK_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_DEADLOCK_ERROR.def.scalar.constraintp = NULL;
    RDB_DEADLOCK_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_DEADLOCK_ERROR.compare_op = NULL;

    RDB_FATAL_ERROR.kind = RDB_TP_SCALAR;
    RDB_FATAL_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_FATAL_ERROR.name = "FATAL_ERROR";
    RDB_FATAL_ERROR.def.scalar.builtin = RDB_TRUE;
    RDB_FATAL_ERROR.def.scalar.repc = 1;
    RDB_FATAL_ERROR.def.scalar.repv = &fatal_rep;
    RDB_FATAL_ERROR.def.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_FATAL_ERROR.def.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_FATAL_ERROR.def.scalar.constraintp = NULL;
    RDB_FATAL_ERROR.def.scalar.sysimpl = RDB_TRUE;
    RDB_FATAL_ERROR.compare_op = NULL;

    RDB_SYNTAX_ERROR.kind = RDB_TP_SCALAR;
    RDB_SYNTAX_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_SYNTAX_ERROR.name = "SYNTAX_ERROR";
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

    /* Add IO_STREAM */
    if (_RDB_add_io(ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    return RDB_OK;
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
        RDB_free(typ);
    } else {
        ret = RDB_del_nonscalar_type(typ, ecp);
    }
    return ret;
}    

static RDB_possrep *
_RDB_get_possrep(const RDB_type *typ, const char *repname)
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
 * Implements a system-generated selector
 */
int
_RDB_sys_select(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_possrep *prp;

    /* Find possrep */
    prp = _RDB_get_possrep(op->rtyp, op->name);
    if (prp == NULL) {
        RDB_raise_invalid_argument("component name is NULL", ecp);
        return RDB_ERROR;
    }

    /* If *retvalp carries a value, it must match the type */
    if (retvalp->kind != RDB_OB_INITIAL
            && (retvalp->typ == NULL
                || !RDB_type_equals(retvalp->typ, op->rtyp))) {
        RDB_raise_type_mismatch("invalid selector return type", ecp);
        return RDB_ERROR;
    }

    if (argc == 1) {
        /* Copy value */
        if (_RDB_copy_obj(retvalp, argv[0], ecp, NULL) != RDB_OK)
            return RDB_ERROR;
    } else {
        /* Copy tuple attributes */
        int i;

        for (i = 0; i < argc; i++) {
            if (RDB_tuple_set(retvalp, op->rtyp->def.scalar.repv[0].compv[i].name,
                    argv[i], ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }
    retvalp->typ = op->rtyp;
    return RDB_OK;
}

/** @addtogroup type
 * @{
 */

/**
 * RDB_type_is_numeric checks if a type is numeric.

@returns

RDB_TRUE if the type is INTEGER or FLOAT, RDB_FALSE otherwise.
 */
RDB_bool
RDB_type_is_numeric(const RDB_type *typ) {
    return (RDB_bool)(typ == &RDB_INTEGER || typ == &RDB_FLOAT);
}

/**
 * If *<var>typ</var> is non-scalar, RDB_dup_nonscalar_creates a copy of it.

@returns

A pointer to a copy of *<var>typ</var>, if *<var>typ</var> is non-scalar.
<var>typ</var>, if *<var>typ</var> is scalar.

 If the operation fails, NULL is returned.
 */
RDB_type *
RDB_dup_nonscalar_type(RDB_type *typ, RDB_exec_context *ecp)
{
    RDB_type *restyp;

    switch (typ->kind) {
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            restyp = RDB_alloc(sizeof (RDB_type), ecp);
            if (restyp == NULL) {
                return NULL;
            }
            restyp->name = NULL;
            restyp->kind = typ->kind;
            restyp->ireplen = RDB_VARIABLE_LEN;
            restyp->def.basetyp = RDB_dup_nonscalar_type(typ->def.basetyp,
                    ecp);
            if (restyp->def.basetyp == NULL) {
                RDB_free(restyp);
                return NULL;
            }
            return restyp;
        case RDB_TP_TUPLE:
            return RDB_create_tuple_type(typ->def.tuple.attrc,
                    typ->def.tuple.attrv, ecp);
        case RDB_TP_SCALAR:
            return typ;
    }
    abort();
}

/**
 * RDB_create_tuple_type creates a tuple type and stores
a pointer to the type at the location pointed to by <var>typp</var>.
The attributes are specified by <var>attrc</var> and <var>attrv</var>.
The fields defaultp and options of RDB_attr are ignored.

@returns

A pointer to the tuple type, or NULL if an error occured.

@par Errors:

<dl>
<dt>INVALID_ARGUMENT_ERROR
<dd><var>attrv</var> contains two attributes with the same name.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
RDB_type *
RDB_create_tuple_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *tuptyp;
    int i, j;

    tuptyp = RDB_alloc(sizeof(RDB_type), ecp);
    if (tuptyp == NULL) {
        return NULL;
    }
    tuptyp->name = NULL;
    tuptyp->compare_op = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->ireplen = RDB_VARIABLE_LEN;
    if (attrc > 0) {
        tuptyp->def.tuple.attrv = RDB_alloc(sizeof(RDB_attr) * attrc, ecp);
        if (tuptyp->def.tuple.attrv == NULL) {
            RDB_free(tuptyp);
            return NULL;
        }
    }
    for (i = 0; i < attrc; i++) {
        tuptyp->def.tuple.attrv[i].typ = NULL;
        tuptyp->def.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < attrc; i++) {
        /* Check if name appears twice */
        for (j = i + 1; j < attrc; j++) {
            if (strcmp(attrv[i].name, attrv[j].name) == 0) {
                RDB_raise_invalid_argument("duplicate attribute name", ecp);
                goto error;
            }
        }

        tuptyp->def.tuple.attrv[i].typ = RDB_dup_nonscalar_type(
                attrv[i].typ, ecp);
        if (tuptyp->def.tuple.attrv[i].typ == NULL) {
            goto error;
        }
        tuptyp->def.tuple.attrv[i].name = RDB_dup_str(attrv[i].name);
        if (tuptyp->def.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        tuptyp->def.tuple.attrv[i].defaultp = NULL;
        tuptyp->def.tuple.attrv[i].options = 0;
    }
    tuptyp->def.tuple.attrc = attrc;

    return tuptyp;

error:
    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp = &tuptyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            RDB_free(attrp->name);
        if (attrp->typ != NULL) {
            if (!RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(tuptyp->def.tuple.attrv);
    RDB_free(tuptyp);

    return NULL;
}

/**
 * Create a relation type and return a pointer to it.
The attributes are specified by <var>attrc</var> and <var>attrv</var>.
The fields defaultp and options of RDB_attr are ignored.

@returns

On success, a pointer to the type. On failure, NULL is returned.

@par Errors:

<dl>
<dt>INVALID_ARGUMENT_ERROR
<dd><var>attrv</var> contains two attributes with the same name.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
RDB_type *
RDB_create_relation_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *tpltyp = RDB_create_tuple_type(attrc, attrv, ecp);
    if (tpltyp == NULL) {
        return NULL;
    }

    return RDB_create_relation_type_from_base(tpltyp, ecp);
}

/**
 * Create a relation type from a tuple type.

@returns

On success, a pointer to the type. On failure, NULL is returned.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
RDB_type *
RDB_create_relation_type_from_base(RDB_type *tpltyp, RDB_exec_context *ecp)
{
    RDB_type *typ = RDB_alloc(sizeof (RDB_type), ecp);
    if (typ == NULL) {
        return NULL;
    }

    typ->name = NULL;
    typ->compare_op = NULL;
    typ->kind = RDB_TP_RELATION;
    typ->ireplen = RDB_VARIABLE_LEN;
    typ->def.basetyp = tpltyp;
    return typ;
}

/**
 * RDB_create_array_type creates an array type.
The base type is specified by <var>typ</var>.

@returns

The new array type, or NULL if the creation failed due to insufficient memory.
 */
RDB_type *
RDB_create_array_type(RDB_type *basetyp, RDB_exec_context *ecp)
{
    RDB_type *typ = RDB_alloc(sizeof (RDB_type), ecp);
    if (typ == NULL) {
        return NULL;
    }
    
    typ->name = NULL;
    typ->compare_op = NULL;
    typ->kind = RDB_TP_ARRAY;
    typ->ireplen = RDB_VARIABLE_LEN;
    typ->def.basetyp = basetyp;

    return typ;
}

/**
RDB_type_is_scalar checks if a type is scalar.

@returns

RDB_TRUE if *<var>typ</var> is scalar, RDB_FALSE if not.
*/
RDB_bool
RDB_type_is_scalar(const RDB_type *typ)
{
    return (RDB_bool) (typ->kind == RDB_TP_SCALAR);
}

/**
 * Checks if a type is a relation type.

@returns

RDB_TRUE if *<var>typ</var> is a relation type, RDB_FALSE if not.
*/
RDB_bool
RDB_type_is_relation(const RDB_type *typ)
{
    return (RDB_bool) (typ->kind == RDB_TP_RELATION);
}

/**
 * Checks if a type is a tuple type.

@returns

RDB_TRUE if *<var>typ</var> is a tuple type, RDB_FALSE if not.
*/
RDB_bool
RDB_type_is_tuple(const RDB_type *typ)
{
    return (RDB_bool) (typ->kind == RDB_TP_TUPLE);
}

/**
 * Checks if a type is an array type.
 *
 * @returns
 * RDB_TRUE if *<var>typ</var> is an array type, RDB_FALSE if not.
 */
RDB_bool
RDB_type_is_array(const RDB_type *typ)
{
    return (RDB_bool) (typ->kind == RDB_TP_ARRAY);
}

/**
 * Return the base type of a relation or array type.
 *
 * @returns
 * The base type if <var>typ</var> is a relation or array type,
 * NULL otherwise.
 */
RDB_type *
RDB_base_type(RDB_type *typ)
{
    return typ->kind == RDB_TP_ARRAY || typ->kind == RDB_TP_RELATION
            ? typ->def.basetyp : NULL;
}

/**
 * RDB_type_attrs returns a pointer to an array of
RDB_attr structures
describing the attributes of the tuple or relation type
specified by *<var>typ</var> and stores the number of attributes in
*<var>attrcp</var>.

@returns

A pointer to an array of RDB_attr structures or NULL if the type
is not a tuple or relation type.
 */
RDB_attr *
RDB_type_attrs(RDB_type *typ, int *attrcp)
{
    if (typ->kind == RDB_TP_RELATION) {
        typ = typ->def.basetyp;
    }
    if (typ->kind != RDB_TP_TUPLE) {
        return NULL;
    }
    *attrcp = typ->def.tuple.attrc;
    return typ->def.tuple.attrv;
}

/**
 *
RDB_define_type defines a type with the name <var>name</var> and
<var>repc</var> possible representations.
The individual possible representations are
described by the elements of <var>repv</var>.

If <var>constraintp</var> is not NULL, it specifies the type constraint.
When the constraint is evaluated, the value to check is made available
as an attribute with the same name as the type.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>ELEMENT_EXISTS_ERROR
<dd>There is already a type with name <var>name</var>.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_define_type(const char *name, int repc, const RDB_possrep repv[],
                RDB_expression *constraintp, RDB_exec_context *ecp,
                RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object conval;
    RDB_object typedata;
    int i, j;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&conval);
    RDB_init_obj(&typedata);

    if (RDB_binary_set(&typedata, 0, NULL, 0, ecp) != RDB_OK)
        return RDB_ERROR;

    /*
     * Insert tuple into SYS_TYPES
     */

    if (RDB_tuple_set_string(&tpl, "TYPENAME", name, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set(&tpl, "I_AREP_TYPE", &typedata, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set_int(&tpl, "I_AREP_LEN", RDB_NOT_IMPLEMENTED, ecp)
            != RDB_OK)
        goto error;
    if (RDB_tuple_set_bool(&tpl, "I_SYSIMPL", RDB_FALSE, ecp) != RDB_OK)
        goto error;

    /* Store constraint in tuple */
    if (_RDB_expr_to_binobj(&conval, constraintp, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set(&tpl, "I_CONSTRAINT", &conval, ecp) != RDB_OK)
        goto error;

    if (RDB_insert(txp->dbp->dbrootp->types_tbp, &tpl, ecp, txp) != RDB_OK)
        goto error;

    /*
     * Insert tuple into SYS_POSSREPS
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
        if (RDB_tuple_set_string(&tpl, "POSSREPNAME", prname, ecp) != RDB_OK)
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
            if (RDB_tuple_set_int(&tpl, "COMPNO", (RDB_int)j, ecp) != RDB_OK)
                goto error;
            if (RDB_tuple_set_string(&tpl, "COMPNAME", cname, ecp) != RDB_OK)
                goto error;

            if (_RDB_type_to_binobj(&typedata, repv[i].compv[j].typ,
                    ecp) != RDB_OK)
                goto error;
            if (RDB_tuple_set(&tpl, "COMPTYPE", &typedata, ecp) != RDB_OK)
                goto error;

            if (RDB_insert(txp->dbp->dbrootp->possrepcomps_tbp, &tpl, ecp, txp)
                    != RDB_OK)
                goto error;
        }
    }

    RDB_destroy_obj(&typedata, ecp);
    RDB_destroy_obj(&conval, ecp);    
    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;
    
error:
    RDB_destroy_obj(&typedata, ecp);
    RDB_destroy_obj(&conval, ecp);    
    RDB_destroy_obj(&tpl, ecp);

    return RDB_ERROR;
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
            "", "_RDB_sys_select", typ->name,
            ecp, txp);
    RDB_free(paramv);
    return ret;
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
    return (RDB_bool) (_RDB_get_possrep(op->rtyp, RDB_operator_name(op)) != NULL);
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

If <var>arep</var> is NULL and <var>areplen</var> is not -1,
<var>areplen</var> specifies the length, in bytes,
of the physical representation, which then is a fixed-length array of bytes.
The getter, setter, and selector operators must be provided by the caller.
RDB_irep_to_obj() can be used by the selector to assign a value to an RDB_object.
RDB_obj_irep() can be used by setters and getters to access the actual representation.

If <var>arep</var> is NULL and <var>areplen</var> is -1,
the getter and setter operators and the selector operator are provided by Duro.
In this case, the type must have exactly one possible representation.
If this representation has exactly one property, the type of this representation will become
the physical representation. Otherwise the type will be represented by a tuple type with
one attribute for each property.

For user-provided setters, getters, and selectors,
the following conventions apply:

<dl>
<dt>Selectors
<dd>A selector is a read-only operator whose name is is the name of a possible
representation. It takes one argument for each component.
<dt>Getters
<dd>A getter is a read-only operator whose name consists of the
type and a component name, separated by "_get_".
It takes one argument. The argument must be of the user-defined type in question.
The return type must be the component type.
<dt>Setters
<dd>A setter is an update operator whose name consists of the
type and a component name, separated by "_set_".
It takes two arguments. The first argument is an update argument
and must be of the user-defined type in question.
The second argument is read-only and must be of the type of
the component.
</dl>

A user-defined comparison operator CMP returning an INTEGER may be supplied.
CMP must have two arguments, both of the user-defined type
for which the comparison is to be defined.

CMP must return -1, 0, or 1 if the first argument is lower than,
equal to, or greater than the second argument, respectively.

If CMP has been defined, it will be called by the built-in comparison
operators =, <>, <= etc. 

@returns

On success, RDB_OK is returned. Any other return value indicates an error.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd>*<var>txp</var> is not a running transaction.
<dt>NOT_FOUND_ERROR
<dd>The type has not been previously defined.
<dt>INVALID_ARGUMENT_ERROR
<dd><var>arep</var> is NULL and <var>areplen</var> is -1,
and the type was defined with more than one possible representation.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_implement_type(const char *name, RDB_type *arep, RDB_int areplen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp, *wherep, *argp;
    RDB_attr_update upd[3];
    RDB_object typedata;
    int ret;
    int i;
    RDB_bool sysimpl = (arep == NULL) && (areplen == -1);

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (sysimpl) {
        /*
         * No actual rep given, so selector and getters/setters must be provided
         * by the system
         */
        RDB_type *typ;
        int compc;

        typ = RDB_get_type(name, ecp, txp);
        if (typ == NULL)
            return RDB_ERROR;

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
            arep = RDB_create_tuple_type(typ->def.scalar.repv[0].compc,
                    typ->def.scalar.repv[0].compv, ecp);
            if (arep == NULL)
                return RDB_ERROR;
        }

        typ->def.scalar.arep = arep;
        typ->def.scalar.sysimpl = sysimpl;
        typ->ireplen = arep->ireplen;

        ret = create_selector(typ, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    exp = RDB_var_ref("TYPENAME", ecp);
    if (exp == NULL) {
        return RDB_ERROR;
    }
    wherep = RDB_ro_op("=", ecp);
    if (wherep == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, exp);
    argp = RDB_string_to_expr(name, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);

    upd[0].exp = upd[1].exp = upd[2].exp = NULL;

    upd[0].name = "I_AREP_LEN";
    upd[0].exp = RDB_int_to_expr(arep == NULL ? areplen : arep->ireplen, ecp);
    if (upd[0].exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    upd[1].name = "I_SYSIMPL";
    upd[1].exp = RDB_bool_to_expr(sysimpl, ecp);
    if (upd[1].exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    if (arep != NULL) {
        RDB_init_obj(&typedata);
        ret = _RDB_type_to_binobj(&typedata, arep, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typedata, ecp);
            goto cleanup;
        }

        upd[2].name = "I_AREP_TYPE";
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

cleanup:    
    for (i = 0; i < 3; i++) {
        if (upd[i].exp != NULL)
            RDB_drop_expr(upd[i].exp, ecp);
    }
    RDB_drop_expr(wherep, ecp);

    return ret;
}

/*@}*/

/**
 * Delete *typ from memory. The type must be non-scalar.
 */
int
RDB_del_nonscalar_type(RDB_type *typ, RDB_exec_context *ecp)
{
    int i;
    int ret = RDB_OK;

    switch (typ->kind) {
        case RDB_TP_TUPLE:
            for (i = 0; i < typ->def.tuple.attrc; i++) {
                RDB_type *attrtyp = typ->def.tuple.attrv[i].typ;

                RDB_free(typ->def.tuple.attrv[i].name);
                if (!RDB_type_is_scalar(attrtyp))
                    ret = RDB_del_nonscalar_type(attrtyp, ecp);
                if (typ->def.tuple.attrv[i].defaultp != NULL) {
                    ret = RDB_destroy_obj(typ->def.tuple.attrv[i].defaultp, ecp);
                    RDB_free(typ->def.tuple.attrv[i].defaultp);
                }
            }
            if (typ->def.tuple.attrc > 0)
                RDB_free(typ->def.tuple.attrv);
            break;
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            if (!RDB_type_is_scalar(typ->def.basetyp))
                ret = RDB_del_nonscalar_type(typ->def.basetyp, ecp);
            break;
        default:
            RDB_raise_invalid_argument("type is scalar", ecp);
            ret = RDB_ERROR;
    }
    typ->kind = (enum _RDB_tp_kind) -1; /* for error detection */
    RDB_free(typ);
    return ret;
}

/**
 * Delete the user-defined type with name specified by <var>name</var>.

It is not possible to destroy built-in types.

@returns

On success, RDB_OK is returned. Any other return value indicates an error.

@par Errors:

<dl>
<dt>NO_RUNNING_TX_ERROR
<dd>*<var>txp</var> is not a running transaction.
<dt>NAME_ERROR
<dd>A type with name <var>name</var> was not found.
<dt>INVALID_ARGUMENT_ERROR
<dd>The type is not user-defined.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_drop_type(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
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
    if (RDB_hashmap_get(&_RDB_builtin_type_map, name) != NULL) {
        RDB_raise_invalid_argument("cannot drop a built-in type", ecp);
        return RDB_ERROR;
    }

    /* !! should check if the type is still used by a table */

    /* Delete selector */
    ret = RDB_drop_op(name, ecp, txp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR) {
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);
    }


    /* Delete type from database */
    wherep = RDB_ro_op("=", ecp);
    if (wherep == NULL) {
        return RDB_ERROR;
    }
    argp = RDB_var_ref("TYPENAME", ecp);
    if (argp == NULL) {
        RDB_drop_expr(wherep, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);
    argp = RDB_string_to_expr(name, ecp);
    if (argp == NULL) {
        RDB_drop_expr(wherep, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);

    cnt = RDB_delete(txp->dbp->dbrootp->types_tbp, wherep, ecp, txp);
    if (cnt == 0) {
        RDB_raise_name("type not found", ecp);
        return RDB_ERROR;
    }
    if (cnt == (RDB_int) RDB_ERROR) {
        RDB_drop_expr(wherep, ecp);
        return RDB_ERROR;
    }
    cnt = RDB_delete(txp->dbp->dbrootp->possrepcomps_tbp, wherep, ecp,
            txp);
    if (cnt == (RDB_int) RDB_ERROR) {
        RDB_drop_expr(wherep, ecp);
        return ret;
    }

    /*
     * Delete type in memory, if it's in the type map
     */
    typ = RDB_hashmap_get(&txp->dbp->dbrootp->typemap, name);
    if (typ != NULL) {
        /* Delete type from type map by puting a NULL pointer into it */
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->typemap, name, ntp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /*
         * Delete RDB_type structure last because name may be identical
         * to typ->name
         */
        if (del_type(typ, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    return RDB_OK;
}

/**
 * RDB_type_equals checks if two types are equal.

Nonscalar types are equal if their definition is the same.

@returns

RDB_TRUE if the types are equal, RDB_FALSE otherwise.
 */
RDB_bool
RDB_type_equals(const RDB_type *typ1, const RDB_type *typ2)
{
    if (typ1 == typ2)
        return RDB_TRUE;
    if (typ1->kind != typ2->kind)
        return RDB_FALSE;

    /* If the two types both have a name, they are equal iff their name
     * is equal */
    if (typ1->name != NULL && typ2->name != NULL)
         return (RDB_bool) (strcmp(typ1->name, typ2->name) == 0);
    
    switch (typ1->kind) {
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            return RDB_type_equals(typ1->def.basetyp, typ2->def.basetyp);
        case RDB_TP_TUPLE:
            {
                int i, j;
                int attrcnt = typ1->def.tuple.attrc;

                if (attrcnt != typ2->def.tuple.attrc)
                    return RDB_FALSE;
                    
                /* check if all attributes of typ1 also appear in typ2 */
                for (i = 0; i < attrcnt; i++) {
                    for (j = 0; j < attrcnt; j++) {
                        if (RDB_type_equals(typ1->def.tuple.attrv[i].typ,
                                typ2->def.tuple.attrv[j].typ)
                                && (strcmp(typ1->def.tuple.attrv[i].name,
                                typ2->def.tuple.attrv[j].name) == 0))
                            break;
                    }
                    if (j >= attrcnt) {
                        /* not found */
                        return RDB_FALSE;
                    }
                }
                return RDB_TRUE;
            }
        default:
            ;
    }
    abort();
}  

/**
 * RDB_type_name returns the name of a type.

@returns

A pointer to the name of the type or NULL if the type has no name.
 */
char *
RDB_type_name(const RDB_type *typ)
{
    return typ->name;
}

/*@}*/

RDB_type *
RDB_type_attr_type(const RDB_type *typ, const char *name)
{
    RDB_attr *attrp;

    switch (typ->kind) {
        case RDB_TP_RELATION:
            attrp = _RDB_tuple_type_attr(typ->def.basetyp, name);
            break;
        case RDB_TP_TUPLE:
            attrp = _RDB_tuple_type_attr(typ, name);
            break;
        case RDB_TP_ARRAY:
        case RDB_TP_SCALAR:
            return NULL;
    }
    if (attrp == NULL)
        return NULL;
    return attrp->typ;
}

RDB_type *
RDB_extend_tuple_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    int i;
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->def.tuple.attrc = typ->def.tuple.attrc + attrc;
    newtyp->def.tuple.attrv = RDB_alloc(sizeof (RDB_attr)
            * (newtyp->def.tuple.attrc), ecp);
    if (newtyp->def.tuple.attrv == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        newtyp->def.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        newtyp->def.tuple.attrv[i].name =
                RDB_dup_str(typ->def.tuple.attrv[i].name);
        if (newtyp->def.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtyp->def.tuple.attrv[i].typ =
                RDB_dup_nonscalar_type(typ->def.tuple.attrv[i].typ, ecp);
        if (newtyp->def.tuple.attrv[i].typ == NULL) {
            goto error;
        }
        newtyp->def.tuple.attrv[i].defaultp = NULL;
        newtyp->def.tuple.attrv[i].options = 0;
    }
    for (i = 0; i < attrc; i++) {
        /*
         * Check if the attribute is already present in the original tuple type
         */
        if (_RDB_tuple_type_attr(typ, attrv[i].name) != NULL) {
            RDB_raise_invalid_argument("attribute exists", ecp);
            goto error;
        }

        newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].name =
                RDB_dup_str(attrv[i].name);
        if (newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].typ =
                RDB_dup_nonscalar_type(attrv[i].typ, ecp);
        if (newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].typ == NULL) {
            goto error;
        }
        newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].defaultp = NULL;
        newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].options = 0;
    }
    return newtyp;

error:
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        RDB_free(newtyp->def.tuple.attrv[i].name);
        if (newtyp->def.tuple.attrv[i].typ != NULL
                && !RDB_type_is_scalar(newtyp->def.tuple.attrv[i].typ)) {
            RDB_del_nonscalar_type(newtyp->def.tuple.attrv[i].typ, ecp);
        }
    }
    RDB_free(newtyp->def.tuple.attrv);
    RDB_free(newtyp);
    return NULL;
}

RDB_type *
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;
    newtyp->def.basetyp = RDB_extend_tuple_type(typ->def.basetyp, attrc, attrv,
            ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_type *
RDB_union_tuple_types(const RDB_type *typ1, const RDB_type *typ2,
        RDB_exec_context *ecp)
{
    RDB_type *newtyp;
    int attrc;
    int i, j;

    /* Create new tuple type */
    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    
    /* calculate new # of attributes as the sum of the # of attributes
     * of both types.
     * That often will be too high; in this case it is reduced later.
     */
    newtyp->def.tuple.attrc = typ1->def.tuple.attrc
            + typ2->def.tuple.attrc;
    newtyp->def.tuple.attrv = RDB_alloc(sizeof (RDB_attr)
            * newtyp->def.tuple.attrc, ecp);
    if (newtyp->def.tuple.attrv == NULL)
        return NULL;

    for (i = 0; i < typ1->def.tuple.attrc; i++)
        newtyp->def.tuple.attrv[i].name = NULL;

    /* copy attributes from first tuple type */
    for (i = 0; i < typ1->def.tuple.attrc; i++) {
        newtyp->def.tuple.attrv[i].name = RDB_dup_str(
                typ1->def.tuple.attrv[i].name);
        newtyp->def.tuple.attrv[i].typ =
                RDB_dup_nonscalar_type(typ1->def.tuple.attrv[i].typ, ecp);
        if (newtyp->def.tuple.attrv[i].typ == NULL)
            goto error;
        newtyp->def.tuple.attrv[i].defaultp = NULL;
        newtyp->def.tuple.attrv[i].options = 0;
    }
    attrc = typ1->def.tuple.attrc;

    /* add attributes from second tuple type */
    for (i = 0; i < typ2->def.tuple.attrc; i++) {
        for (j = 0; j < typ1->def.tuple.attrc; j++) {
            if (strcmp(typ2->def.tuple.attrv[i].name,
                    typ1->def.tuple.attrv[j].name) == 0) {
                /* If two attributes match by name, they must be of
                   the same type */
                if (!RDB_type_equals(typ2->def.tuple.attrv[i].typ,
                        typ1->def.tuple.attrv[j].typ)) {
                    RDB_raise_type_mismatch("JOIN attribute types do not match",
                            ecp);
                    goto error;
                }
                break;
            }
        }
        if (j >= typ1->def.tuple.attrc) {
            /* attribute not found, so add it to result type */
            newtyp->def.tuple.attrv[attrc].name = RDB_dup_str(
                    typ2->def.tuple.attrv[i].name);
            newtyp->def.tuple.attrv[attrc].typ =
                    RDB_dup_nonscalar_type(typ2->def.tuple.attrv[i].typ, ecp);
            if (newtyp->def.tuple.attrv[attrc].typ == NULL)
                goto error;
            newtyp->def.tuple.attrv[attrc].defaultp = NULL;
            newtyp->def.tuple.attrv[attrc].options = 0;
            attrc++;
        }
    }

    /* adjust array size, if necessary */    
    if (attrc < newtyp->def.tuple.attrc) {
        void *p = RDB_realloc(newtyp->def.tuple.attrv,
                sizeof(RDB_attr) * attrc, ecp);
        if (p == NULL)
            goto error;

        newtyp->def.tuple.attrc = attrc;
        newtyp->def.tuple.attrv = p;
    }
    return newtyp;

error:
    for (i = 0; i < typ1->def.tuple.attrc; i++)
        RDB_free(newtyp->def.tuple.attrv[i].name);

    RDB_free(newtyp);
    return NULL;
}

RDB_type *
RDB_join_relation_types(const RDB_type *typ1, const RDB_type *typ2,
                     RDB_exec_context *ecp)
{
    RDB_type *newtyp;

    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }
    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->def.basetyp = RDB_union_tuple_types(typ1->def.basetyp,
            typ2->def.basetyp, ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

/* Return a pointer to the RDB_attr strcuture of the attribute with name attrname in the tuple
   type *tpltyp. */
RDB_attr *
_RDB_tuple_type_attr(const RDB_type *tpltyp, const char *attrname)
{
    int i;
    
    for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
        if (strcmp(tpltyp->def.tuple.attrv[i].name, attrname) == 0)
            return &tpltyp->def.tuple.attrv[i];
    }
    /* not found */
    return NULL;
}

RDB_type *
RDB_project_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
                       RDB_exec_context *ecp)
{
    RDB_type *tuptyp;
    int i;

    tuptyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (tuptyp == NULL) {
        return NULL;
    }
    tuptyp->name = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->def.tuple.attrc = attrc;
    tuptyp->def.tuple.attrv = RDB_alloc(attrc * sizeof (RDB_attr), ecp);
    if (tuptyp->def.tuple.attrv == NULL) {
        RDB_free(tuptyp);
        return NULL;
    }
    for (i = 0; i < attrc; i++)
        tuptyp->def.tuple.attrv[i].name = NULL;

    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp;
        char *attrname = RDB_dup_str(attrv[i]);
        if (attrname == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }

        tuptyp->def.tuple.attrv[i].name = attrname;

        attrp = _RDB_tuple_type_attr(typ, attrname);
        if (attrp == NULL) {
            RDB_raise_name(attrname, ecp);
            goto error;
        }
        tuptyp->def.tuple.attrv[i].typ = RDB_dup_nonscalar_type(attrp->typ, ecp);
        if (tuptyp->def.tuple.attrv[i].typ == NULL)
            goto error;

        tuptyp->def.tuple.attrv[i].defaultp = NULL;
        tuptyp->def.tuple.attrv[i].options = 0;
    }

    return tuptyp;

error:
    for (i = 0; i < attrc; i++)
        RDB_free(tuptyp->def.tuple.attrv[i].name);
    RDB_free(tuptyp->def.tuple.attrv);
    RDB_free(tuptyp);
    return NULL;
}

RDB_type *
RDB_project_relation_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_exec_context *ecp)
{
    RDB_type *reltyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (reltyp == NULL) {
        return NULL;
    }

    reltyp->def.basetyp = RDB_project_tuple_type(typ->def.basetyp, attrc,
            attrv, ecp);
    if (reltyp->def.basetyp == NULL) {
        RDB_free(reltyp);
        return NULL;
    }
    reltyp->name = NULL;
    reltyp->kind = RDB_TP_RELATION;

    return reltyp;
}

int
_RDB_find_rename_from(int renc, const RDB_renaming renv[], const char *name)
{
    int i;

    for (i = 0; i < renc && strcmp(renv[i].from, name) != 0; i++);
    if (i >= renc)
        return -1; /* not found */
    /* found */
    return i;
}

RDB_type *
RDB_rename_tuple_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp;
    int i, j;

    /*
     * Check arguments
     */
    for (i = 0; i < renc; i++) {
        /* Check if source attribute exists */
        if (_RDB_tuple_type_attr(typ, renv[i].from) == NULL) {
            RDB_raise_name(renv[i].from, ecp);
            return NULL;
        }

        /* Check if the dest attribute does not exist */
        if (_RDB_tuple_type_attr(typ, renv[i].to) != NULL) {
            RDB_raise_name(renv[i].to, ecp);
            return NULL;
        }

        for (j = i + 1; j < renc; j++) {
            /* Check if source or dest appears twice */
            if (strcmp(renv[i].from, renv[j].from) == 0
                    || strcmp(renv[i].to, renv[j].to) == 0) {
                RDB_raise_invalid_argument("invalid RENAME arguments", ecp);
                return NULL;
            }
        }
    }

    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->def.tuple.attrc = typ->def.tuple.attrc;
    newtyp->def.tuple.attrv = RDB_alloc (typ->def.tuple.attrc * sizeof(RDB_attr), ecp);
    if (newtyp->def.tuple.attrv == NULL) {
        goto error;
    }
    for (i = 0; i < typ->def.tuple.attrc; i++)
        newtyp->def.tuple.attrv[i].name = NULL;
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        char *attrname = typ->def.tuple.attrv[i].name;
        int ai = _RDB_find_rename_from(renc, renv, attrname);

        /* check if the attribute has been renamed */
        newtyp->def.tuple.attrv[i].typ = RDB_dup_nonscalar_type(
                typ->def.tuple.attrv[i].typ, ecp);
        if (newtyp->def.tuple.attrv[i].typ == NULL)
            goto error;

        if (ai >= 0)
            newtyp->def.tuple.attrv[i].name = RDB_dup_str(renv[ai].to);
        else
            newtyp->def.tuple.attrv[i].name = RDB_dup_str(attrname);
        if (newtyp->def.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtyp->def.tuple.attrv[i].defaultp = NULL;
        newtyp->def.tuple.attrv[i].options = 0;
     }
     return newtyp;

error:
    if (newtyp->def.tuple.attrv != NULL) {
        for (i = 0; i < newtyp->def.tuple.attrc; i++)
            RDB_free(newtyp->def.tuple.attrv[i].name);
        RDB_free(newtyp->def.tuple.attrv);
    }

    RDB_free(newtyp);
    return NULL;
}

/**
 * Rename the attributes of the relation type *<var>typ</var> as specified by <var>renc</var>
 * and <var>renv<var> and return the new relation type.
 */
RDB_type *
RDB_rename_relation_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->def.basetyp = RDB_rename_tuple_type(typ->def.basetyp, renc, renv,
            ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_attr *
_RDB_get_icomp(RDB_type *typ, const char *compname)
{
    int i, j;

    for (i = 0; i < typ->def.scalar.repc; i++) {
        for (j = 0; j < typ->def.scalar.repv[i].compc; j++) {
            if (strcmp(typ->def.scalar.repv[i].compv[j].name, compname) == 0)
                return &typ->def.scalar.repv[i].compv[j];
        }
    }
    return NULL;
}

static int
copy_attr(RDB_attr *dstp, const RDB_attr *srcp, RDB_exec_context *ecp)
{
    dstp->name = RDB_dup_str(srcp->name);
    if (dstp->name == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    dstp->typ = RDB_dup_nonscalar_type(srcp->typ, ecp);
    if (dstp->typ == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    dstp->defaultp = NULL;
    dstp->options = 0;
    return RDB_OK;
}

static RDB_type *
aggr_type(const RDB_expression *exp, const RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (exp->kind != RDB_EX_RO_OP) {
        RDB_raise_invalid_argument("invalid summarize argument", ecp);
        return NULL;
    }

    if (strcmp(exp->def.op.name, "COUNT") == 0) {
        return &RDB_INTEGER;
    } else if (strcmp(exp->def.op.name, "AVG") == 0) {
        return &RDB_FLOAT;
    } else if (strcmp(exp->def.op.name, "SUM") == 0
            || strcmp(exp->def.op.name, "MAX") == 0
            || strcmp(exp->def.op.name, "MIN") == 0) {
        if (exp->def.op.args.firstp == NULL
                || exp->def.op.args.firstp->nextp != NULL) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments", ecp);
            return NULL;
        }
        return _RDB_expr_type(exp->def.op.args.firstp, tpltyp, ecp, txp);
    } else if (strcmp(exp->def.op.name, "ANY") == 0
            || strcmp(exp->def.op.name, "ALL") == 0) {
        return &RDB_BOOLEAN;
    }
    RDB_raise_operator_not_found(exp->def.op.name, ecp);
    return NULL;
}

RDB_type *
RDB_summarize_type(RDB_expr_list *expsp,
        int avgc, char **avgv, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_type *newtyp;
    int addc, attrc;
    RDB_expression *argp;
    RDB_attr *attrv = NULL;
    RDB_type *tb1typ = NULL;
    RDB_type *tb2typ = NULL;
    int expc = RDB_expr_list_length(expsp);

    if (expc < 2 || (expc % 2) != 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    addc = (expc - 2) / 2;
    attrc = addc + avgc;

    tb1typ = RDB_expr_type(expsp->firstp, NULL, NULL, ecp, txp);
    if (tb1typ == NULL)
        return NULL;
    tb2typ = RDB_expr_type(expsp->firstp->nextp, NULL, NULL, ecp, txp);
    if (tb2typ == NULL)
        goto error;
   
    if (tb2typ->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("relation type required", ecp);
        goto error;
    }

    attrv = RDB_alloc(sizeof (RDB_attr) * attrc, ecp);
    if (attrv == NULL) {
        goto error;
    }

    argp = expsp->firstp->nextp->nextp;
    for (i = 0; i < addc; i++) {
        attrv[i].typ = aggr_type(argp, tb1typ->def.basetyp,
                ecp, txp);
        if (attrv[i].typ == NULL)
            goto error;
        if (argp->nextp->kind != RDB_EX_OBJ) {
            RDB_raise_invalid_argument("invalid SUMMARIZE argument", ecp);
            goto error;
        }
        attrv[i].name = RDB_obj_string(&argp->nextp->def.obj);
        if (attrv[i].name == NULL) {
            RDB_raise_invalid_argument("invalid SUMMARIZE argument", ecp);
            goto error;
        }
        argp = argp->nextp->nextp;
    }
    for (i = 0; i < avgc; i++) {
        attrv[addc + i].name = avgv[i];
        attrv[addc + i].typ = &RDB_INTEGER;
    }

    newtyp = RDB_extend_relation_type(tb2typ, attrc, attrv, ecp);
    if (newtyp == NULL) {
        goto error;
    }

    RDB_free(attrv);
    return newtyp;

error:
    RDB_free(attrv);    
    return NULL;
}

RDB_type *
RDB_wrap_tuple_type(const RDB_type *typ, int wrapc, const RDB_wrapping wrapv[],
        RDB_exec_context *ecp)
{
    int i, j, k;
    int ret;
    RDB_attr *attrp;
    int attrc;
    RDB_type *newtyp;

    /* Compute # of attributes */
    attrc = typ->def.tuple.attrc;
    for (i = 0; i < wrapc; i++)
        attrc += 1 - wrapv[i].attrc;

    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->def.tuple.attrc = attrc;
    newtyp->def.tuple.attrv = RDB_alloc(attrc * sizeof(RDB_attr), ecp);
    if (newtyp->def.tuple.attrv == NULL) {
        RDB_free(newtyp);
        return NULL;
    }

    for (i = 0; i < attrc; i++) {
        newtyp->def.tuple.attrv[i].name = NULL;
        newtyp->def.tuple.attrv[i].typ = NULL;
        newtyp->def.tuple.attrv[i].defaultp = NULL;
        newtyp->def.tuple.attrv[i].options = 0;
    }

    /*
     * Copy attributes from wrapv
     */
    for (i = 0; i < wrapc; i++) {
        RDB_type *tuptyp = RDB_alloc(sizeof(RDB_type), ecp);
        if (tuptyp == NULL) {
            goto error;
        }
        tuptyp->name = NULL;
        tuptyp->kind = RDB_TP_TUPLE;
        tuptyp->def.tuple.attrc = wrapv[i].attrc;
        tuptyp->def.tuple.attrv = RDB_alloc(sizeof(RDB_attr) * wrapv[i].attrc, ecp);
        if (tuptyp->def.tuple.attrv == NULL) {
            RDB_free(tuptyp);
            goto error;
        }

        for (j = 0; j < wrapv[i].attrc; j++) {
            attrp = _RDB_tuple_type_attr(typ, wrapv[i].attrv[j]);
            if (attrp == NULL) {
                RDB_raise_name(wrapv[i].attrv[j], ecp);
                RDB_free(tuptyp->def.tuple.attrv);
                RDB_free(tuptyp);
                goto error;
            }

            ret = copy_attr(&tuptyp->def.tuple.attrv[j], attrp, ecp);
            if (ret != RDB_OK) {
                RDB_free(tuptyp->def.tuple.attrv);
                RDB_free(tuptyp);
                goto error;
            }
        }
        newtyp->def.tuple.attrv[i].name = RDB_dup_str(wrapv[i].attrname);
        if (newtyp->def.tuple.attrv[i].name == NULL)
            goto error;

        newtyp->def.tuple.attrv[i].typ = tuptyp;
    }

    /*
     * Copy remaining attributes
     */
    k = wrapc;
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        /* Copy attribute if it does not appear in wrapv */
        for (j = 0; j < wrapc && RDB_find_str(wrapv[j].attrc, wrapv[j].attrv,
                typ->def.tuple.attrv[i].name) == -1; j++);
        if (j == wrapc) {
            /* Not found */
            ret = copy_attr(&newtyp->def.tuple.attrv[k],
                    &typ->def.tuple.attrv[i], ecp);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }
    if (k != attrc) {
        RDB_raise_invalid_argument("invalid WRAP", ecp);
        goto error;
    }

    return newtyp;

error:
    for (i = 0; i < attrc; i++) {
        attrp = &newtyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            RDB_free(attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ == NULL && !RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(newtyp->def.tuple.attrv);
    RDB_free(newtyp);
    return NULL;
}

RDB_type *
RDB_wrap_relation_type(const RDB_type *typ, int wrapc,
        const RDB_wrapping wrapv[], RDB_exec_context *ecp)
{
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->def.basetyp = RDB_wrap_tuple_type(typ->def.basetyp, wrapc, wrapv,
            ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_type *
RDB_unwrap_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_exec_context *ecp)
{
    int nattrc;
    int i, j, k;
    int ret;
    RDB_attr *attrp;
    RDB_type *newtyp;

    /* Compute # of attributes */
    nattrc = typ->def.tuple.attrc;
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_type_attr_type(typ, attrv[i]);
        if (tuptyp == NULL) {
            RDB_raise_name(attrv[i], ecp);
            goto error;
        }
        if (tuptyp->kind != RDB_TP_TUPLE) {
            RDB_raise_invalid_argument("not a tuple", ecp);
            goto error;
        }        
        nattrc += tuptyp->def.tuple.attrc - 1;
    }

    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->def.tuple.attrc = nattrc;
    newtyp->def.tuple.attrv = RDB_alloc(nattrc * sizeof(RDB_attr), ecp);
    if (newtyp->def.tuple.attrv == NULL) {
        RDB_free(newtyp);
        return NULL;
    }

    for (i = 0; i < nattrc; i++) {
        newtyp->def.tuple.attrv[i].name = NULL;
        newtyp->def.tuple.attrv[i].typ = NULL;
    }

    k = 0;

    /* Copy sub-attributes of attrv */
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_type_attr_type(typ, attrv[i]);

        for (j = 0; j < tuptyp->def.tuple.attrc; j++) {
            ret = copy_attr(&newtyp->def.tuple.attrv[k],
                    &tuptyp->def.tuple.attrv[j], ecp);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }

    /* Copy remaining attributes */
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        /* Copy attribute if it does not appear in attrv */
        if (RDB_find_str(attrc, attrv, typ->def.tuple.attrv[i].name) == -1) {
            ret = copy_attr(&newtyp->def.tuple.attrv[k],
                    &typ->def.tuple.attrv[i], ecp);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }

    if (k != nattrc) {
        RDB_raise_invalid_argument("invalid UNWRAP", ecp);
        goto error;
    }

    return newtyp;

error:
    for (i = 0; i < attrc; i++) {
        attrp = &newtyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            free (attrp->name);
        if (attrp->typ != NULL) {
            if (!RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(newtyp->def.tuple.attrv);
    RDB_free(newtyp);
    return NULL;
}    

RDB_type *
RDB_unwrap_relation_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->def.basetyp = RDB_unwrap_tuple_type(typ->def.basetyp, attrc, attrv,
            ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_type *
RDB_group_type(RDB_type *typ, int attrc, char *attrv[], const char *gattr,
        RDB_exec_context *ecp)
{
    int i, j;
    int ret;
    RDB_attr *rtattrv;
    RDB_attr *attrp;
    RDB_type *tuptyp;
    RDB_type *gattrtyp;
    RDB_type *newtyp;

    /*
     * Create relation type for attribute gattr
     */
    rtattrv = RDB_alloc(sizeof(RDB_attr) * attrc, ecp);
    if (rtattrv == NULL) {
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        attrp = _RDB_tuple_type_attr(typ->def.basetyp, attrv[i]);
        if (attrp == NULL) {
            RDB_free(rtattrv);
            RDB_raise_name(attrv[i], ecp);
            return NULL;
        }
        rtattrv[i].typ = attrp->typ;
        rtattrv[i].name = attrp->name;
    }
    gattrtyp = RDB_create_relation_type(attrc, rtattrv, ecp);
    RDB_free(rtattrv);
    if (gattrtyp == NULL)
        return NULL;

    /*
     * Create tuple type
     */
    tuptyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (tuptyp == NULL) {
        RDB_del_nonscalar_type(gattrtyp, ecp);
        return NULL;
    }

    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->name = NULL;
    tuptyp->def.tuple.attrc = typ->def.basetyp->def.tuple.attrc + 1 - attrc;
    tuptyp->def.tuple.attrv = RDB_alloc(tuptyp->def.tuple.attrc * sizeof(RDB_attr), ecp);
    if (tuptyp->def.tuple.attrv == NULL) {
        RDB_free(tuptyp);
        RDB_del_nonscalar_type(gattrtyp, ecp);
        return NULL;
    }

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        tuptyp->def.tuple.attrv[i].name = NULL;
        tuptyp->def.tuple.attrv[i].typ = NULL;
    }

    j = 0;
    for (i = 0; i < typ->def.basetyp->def.tuple.attrc; i++) {
        char *attrname = typ->def.basetyp->def.tuple.attrv[i].name;

        if (_RDB_tuple_type_attr(gattrtyp->def.basetyp, attrname) == NULL) {
            if (strcmp(attrname, gattr) == 0) {
                RDB_del_nonscalar_type(gattrtyp, ecp);
                RDB_raise_invalid_argument("invalid GROUP", ecp);
                goto error;
            }
            ret = copy_attr(&tuptyp->def.tuple.attrv[j],
                    &typ->def.basetyp->def.tuple.attrv[i], ecp);
            if (ret != RDB_OK) {
                RDB_del_nonscalar_type(gattrtyp, ecp);
                goto error;
            }
            tuptyp->def.tuple.attrv[j].defaultp = NULL;
            tuptyp->def.tuple.attrv[j].options = 0;
            j++;
        }
    }
    tuptyp->def.tuple.attrv[j].typ = gattrtyp;
    tuptyp->def.tuple.attrv[j].name = RDB_dup_str(gattr);
    if (tuptyp->def.tuple.attrv[j].name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    tuptyp->def.tuple.attrv[j].defaultp = NULL;
    tuptyp->def.tuple.attrv[j].options = 0;

    /*
     * Create relation type
     */
    newtyp = RDB_alloc(sizeof(RDB_type), ecp);
    if (newtyp == NULL) {
        goto error;
    }
    newtyp->kind = RDB_TP_RELATION;
    newtyp->name = NULL;
    newtyp->def.basetyp = tuptyp;

    return newtyp;

error:
    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        RDB_attr *attrp = &tuptyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            RDB_free(attrp->name);
        if (attrp->typ != NULL) {
            if (!RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(tuptyp->def.tuple.attrv);
    RDB_free(tuptyp);

    return NULL;
}

RDB_type *
RDB_ungroup_type(RDB_type *typ, const char *attr, RDB_exec_context *ecp)
{
    int i, j;
    int ret;
    RDB_type *tuptyp;
    RDB_type *newtyp;
    RDB_attr *relattrp = _RDB_tuple_type_attr(typ->def.basetyp, attr);

    if (relattrp == NULL) {
        RDB_raise_name(attr, ecp);
        return NULL;
    }
    if (relattrp->typ->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("attribute is not a relation", ecp);
        return NULL;
    }

    tuptyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (tuptyp == NULL) {
        return NULL;
    }

    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->name = NULL;

    /* Compute # of attributes */
    tuptyp->def.tuple.attrc = typ->def.basetyp->def.tuple.attrc
            + relattrp->typ->def.basetyp->def.tuple.attrc - 1;

    /* Allocate tuple attributes */
    tuptyp->def.tuple.attrv = RDB_alloc(tuptyp->def.tuple.attrc * sizeof(RDB_attr), ecp);
    if (tuptyp->def.tuple.attrv == NULL) {
        RDB_free(tuptyp);
        return NULL;
    }

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        tuptyp->def.tuple.attrv[i].name = NULL;
        tuptyp->def.tuple.attrv[i].typ = NULL;
    }

    /* Copy attributes from the original type */
    j = 0;
    for (i = 0; i < typ->def.basetyp->def.tuple.attrc; i++) {
        if (strcmp(typ->def.basetyp->def.tuple.attrv[i].name,
                attr) != 0) {
            ret = copy_attr(&tuptyp->def.tuple.attrv[j],
                    &typ->def.basetyp->def.tuple.attrv[i], ecp);
            if (ret != RDB_OK)
                goto error;
            j++;
        }
    }

    /* Copy attributes from the attribute type */
    for (i = 0; i < relattrp->typ->def.basetyp->def.tuple.attrc; i++) {
        char *attrname = relattrp->typ->def.basetyp->def.tuple.attrv[i].name;

        /* Check for attribute name clash */
        if (strcmp(attrname, attr) != 0
                && _RDB_tuple_type_attr(typ->def.basetyp, attrname)
                != NULL) {
            RDB_raise_invalid_argument("invalid UNGROUP", ecp);
            goto error;
        }
        ret = copy_attr(&tuptyp->def.tuple.attrv[j],
                    &relattrp->typ->def.basetyp->def.tuple.attrv[i], ecp);
        if (ret != RDB_OK)
            goto error;
        j++;
    }

    /*
     * Create relation type
     */
    newtyp = RDB_alloc(sizeof(RDB_type), ecp);
    if (newtyp == NULL) {
        goto error;
    }
    newtyp->kind = RDB_TP_RELATION;
    newtyp->name = NULL;
    newtyp->def.basetyp = tuptyp;

    return newtyp;    

error:
    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        RDB_attr *attrp = &tuptyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            RDB_free(attrp->name);
        if (attrp->typ != NULL) {
            if (!RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(tuptyp->def.tuple.attrv);
    RDB_free(tuptyp);

    return NULL;
}

/**
 * Assume the attributes were ordered by the alphabetical order of their names.
 * Return the next attribute after *lastname.
 * If lastname is NULL, return the first.
 */
int
RDB_next_attr_sorted(const RDB_type *typ, const char *lastname) {
    int i;
    int attridx = -1;

    for (i = 0; i < typ->def.tuple.attrc; i++) {
        if ((lastname == NULL
                    || strcmp(typ->def.tuple.attrv[i].name, lastname) > 0)
                && (attridx == -1
                    || strcmp(typ->def.tuple.attrv[i].name,
                            typ->def.tuple.attrv[attridx].name) < 0)) {
            attridx = i;
        }
    }
    return attridx;
}
