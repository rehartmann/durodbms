/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>
#include <locale.h>

RDB_type RDB_BOOLEAN;
RDB_type RDB_INTEGER;
RDB_type RDB_FLOAT;
RDB_type RDB_DOUBLE;
RDB_type RDB_STRING;
RDB_type RDB_BINARY;

RDB_type RDB_INVALID_TRANSACTION_ERROR;
RDB_type RDB_INVALID_ARGUMENT_ERROR;
RDB_type RDB_TYPE_MISMATCH_ERROR;
RDB_type RDB_NOT_FOUND_ERROR;
RDB_type RDB_OPERATOR_NOT_FOUND_ERROR;
RDB_type RDB_ATTRIBUTE_NOT_FOUND_ERROR;
RDB_type RDB_ELEMENT_EXISTS_ERROR;
RDB_type RDB_TYPE_CONSTRAINT_VIOLATION_ERROR;
RDB_type RDB_KEY_VIOLATION_ERROR;
RDB_type RDB_PREDICATE_VIOLATION_ERROR;
RDB_type RDB_AGGREGATE_UNDEFINED_ERROR;
RDB_type RDB_VERSION_MISMATCH_ERROR;
RDB_type RDB_NOT_SUPPORTED_ERROR;
RDB_type RDB_NO_MEMORY_ERROR;
RDB_type RDB_LOCK_NOT_GRANTED_ERROR;
RDB_type RDB_DEADLOCK_ERROR;
RDB_type RDB_RESOURCE_NOT_FOUND_ERROR;
RDB_type RDB_INTERNAL_ERROR;
RDB_type RDB_FATAL_ERROR;
RDB_type RDB_SYSTEM_ERROR;

RDB_type RDB_SYNTAX_ERROR;

static int
compare_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val - argv[1]->var.int_val);
    return RDB_OK;
}

static int
compare_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int res;

    if (argv[0]->var.float_val < argv[1]->var.float_val) {
        res = -1;
    } else if (argv[0]->var.float_val > argv[1]->var.float_val) {
        res = 1;
    } else {
        res = 0;
    }
    RDB_int_to_obj(retvalp, res);
    return RDB_OK;
}

static int
compare_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int res;

    if (argv[0]->var.double_val < argv[1]->var.double_val) {
        res = -1;
    } else if (argv[0]->var.double_val > argv[1]->var.double_val) {
        res = 1;
    } else {
        res = 0;
    }
    RDB_int_to_obj(retvalp, res);
    return RDB_OK;
}

static int
compare_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp,
            strcoll(argv[0]->var.bin.datap, argv[1]->var.bin.datap));
    return RDB_OK;
}

int
_RDB_init_builtin_types(RDB_exec_context *ecp)
{
    static RDB_bool initialized = RDB_FALSE;

    static RDB_possrep no_memory_rep = {
        "NO_MEMORY_ERROR",
        0
    };

    static RDB_possrep invalid_transaction_rep = {
        "INVALID_TRANSACTION_ERROR",
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

    static RDB_attr attribute_not_found_comp = { "MSG", &RDB_STRING };

    static RDB_possrep attribute_not_found_rep = {
        "ATTRIBUTE_NOT_FOUND_ERROR",
        1,
        &attribute_not_found_comp
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
    RDB_BOOLEAN.var.scalar.builtin = RDB_TRUE;
    RDB_BOOLEAN.ireplen = 1;
    RDB_BOOLEAN.name = "BOOLEAN";
    RDB_BOOLEAN.var.scalar.repc = 0;
    RDB_BOOLEAN.var.scalar.arep = NULL;
    RDB_BOOLEAN.var.scalar.constraintp = NULL;
    RDB_BOOLEAN.comparep = NULL;

    RDB_STRING.kind = RDB_TP_SCALAR;
    RDB_STRING.var.scalar.builtin = RDB_TRUE;
    RDB_STRING.ireplen = RDB_VARIABLE_LEN;
    RDB_STRING.name = "STRING";
    RDB_STRING.var.scalar.repc = 0;
    RDB_STRING.var.scalar.arep = NULL;
    RDB_STRING.var.scalar.constraintp = NULL;
    RDB_STRING.comparep = &compare_string;

    RDB_INTEGER.kind = RDB_TP_SCALAR;
    RDB_INTEGER.var.scalar.builtin = RDB_TRUE;
    RDB_INTEGER.ireplen = sizeof (RDB_int);
    RDB_INTEGER.name = "INTEGER";
    RDB_INTEGER.var.scalar.repc = 0;
    RDB_INTEGER.var.scalar.arep = NULL;
    RDB_INTEGER.var.scalar.constraintp = NULL;
    RDB_INTEGER.comparep = &compare_int;

    RDB_FLOAT.kind = RDB_TP_SCALAR;
    RDB_FLOAT.var.scalar.builtin = RDB_TRUE;
    RDB_FLOAT.ireplen = sizeof (RDB_float);
    RDB_FLOAT.name = "FLOAT";
    RDB_FLOAT.var.scalar.repc = 0;
    RDB_FLOAT.var.scalar.arep = NULL;
    RDB_FLOAT.var.scalar.constraintp = NULL;
    RDB_FLOAT.comparep = &compare_float;

    RDB_DOUBLE.kind = RDB_TP_SCALAR;
    RDB_DOUBLE.var.scalar.builtin = RDB_TRUE;
    RDB_DOUBLE.ireplen = sizeof (RDB_double);
    RDB_DOUBLE.name = "DOUBLE";
    RDB_DOUBLE.var.scalar.repc = 0;
    RDB_DOUBLE.var.scalar.arep = NULL;
    RDB_DOUBLE.var.scalar.constraintp = NULL;
    RDB_DOUBLE.comparep = &compare_double;

    RDB_BINARY.kind = RDB_TP_SCALAR;
    RDB_BINARY.var.scalar.builtin = RDB_TRUE;
    RDB_BINARY.ireplen = RDB_VARIABLE_LEN;
    RDB_BINARY.name = "BINARY";
    RDB_BINARY.var.scalar.repc = 0;
    RDB_BINARY.var.scalar.arep = NULL;
    RDB_BINARY.var.scalar.constraintp = NULL;
    RDB_BINARY.comparep = NULL;

    RDB_NO_MEMORY_ERROR.kind = RDB_TP_SCALAR;
    RDB_NO_MEMORY_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_NO_MEMORY_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NO_MEMORY_ERROR.name = "NO_MEMORY_ERROR";
    RDB_NO_MEMORY_ERROR.var.scalar.repc = 1;
    RDB_NO_MEMORY_ERROR.var.scalar.repv = &no_memory_rep;
    RDB_NO_MEMORY_ERROR.var.scalar.arep = RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_NO_MEMORY_ERROR.var.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_NO_MEMORY_ERROR.var.scalar.constraintp = NULL;
    RDB_NO_MEMORY_ERROR.comparep = NULL;

    RDB_INVALID_TRANSACTION_ERROR.kind = RDB_TP_SCALAR;
    RDB_INVALID_TRANSACTION_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_INVALID_TRANSACTION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_INVALID_TRANSACTION_ERROR.name = "INVALID_TRANSACTION_ERROR";
    RDB_INVALID_TRANSACTION_ERROR.var.scalar.repc = 1;
    RDB_INVALID_TRANSACTION_ERROR.var.scalar.repv = &invalid_transaction_rep;
    RDB_INVALID_TRANSACTION_ERROR.var.scalar.arep = RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_INVALID_TRANSACTION_ERROR.var.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_INVALID_TRANSACTION_ERROR.var.scalar.arep = NULL;
    RDB_INVALID_TRANSACTION_ERROR.var.scalar.constraintp = NULL;
    RDB_INVALID_TRANSACTION_ERROR.comparep = NULL;

    RDB_NOT_FOUND_ERROR.kind = RDB_TP_SCALAR;
    RDB_NOT_FOUND_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_NOT_FOUND_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NOT_FOUND_ERROR.name = "NOT_FOUND_ERROR";
    RDB_NOT_FOUND_ERROR.var.scalar.repc = 1;
    RDB_NOT_FOUND_ERROR.var.scalar.repv = &not_found_rep;
    RDB_NOT_FOUND_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_NOT_FOUND_ERROR.var.scalar.constraintp = NULL;
    RDB_NOT_FOUND_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_NOT_FOUND_ERROR.comparep = NULL;

    RDB_INVALID_ARGUMENT_ERROR.kind = RDB_TP_SCALAR;
    RDB_INVALID_ARGUMENT_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_INVALID_ARGUMENT_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_INVALID_ARGUMENT_ERROR.name = "INVALID_ARGUMENT_ERROR";
    RDB_INVALID_ARGUMENT_ERROR.var.scalar.repc = 1;
    RDB_INVALID_ARGUMENT_ERROR.var.scalar.repv = &invalid_argument_rep;
    RDB_INVALID_ARGUMENT_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_INVALID_ARGUMENT_ERROR.var.scalar.constraintp = NULL;
    RDB_INVALID_ARGUMENT_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_INVALID_ARGUMENT_ERROR.comparep = NULL;

    RDB_TYPE_MISMATCH_ERROR.kind = RDB_TP_SCALAR;
    RDB_TYPE_MISMATCH_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_TYPE_MISMATCH_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_TYPE_MISMATCH_ERROR.name = "TYPE_MISMATCH_ERROR";
    RDB_TYPE_MISMATCH_ERROR.var.scalar.repc = 1;
    RDB_TYPE_MISMATCH_ERROR.var.scalar.repv = &type_mismatch_rep;
    RDB_TYPE_MISMATCH_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_TYPE_MISMATCH_ERROR.var.scalar.constraintp = NULL;
    RDB_TYPE_MISMATCH_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_TYPE_MISMATCH_ERROR.comparep = NULL;

    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.name = "TYPE_CONSTRAINT_VIOLATION_ERROR";
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.var.scalar.repc = 1;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.var.scalar.repv =
                &type_constraint_violation_rep;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.var.scalar.constraintp = NULL;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_TYPE_CONSTRAINT_VIOLATION_ERROR.comparep = NULL;

    RDB_OPERATOR_NOT_FOUND_ERROR.kind = RDB_TP_SCALAR;
    RDB_OPERATOR_NOT_FOUND_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_OPERATOR_NOT_FOUND_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_OPERATOR_NOT_FOUND_ERROR.name = "OPERATOR_NOT_FOUND_ERROR";
    RDB_OPERATOR_NOT_FOUND_ERROR.var.scalar.repc = 1;
    RDB_OPERATOR_NOT_FOUND_ERROR.var.scalar.repv = &operator_not_found_rep;
    RDB_OPERATOR_NOT_FOUND_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_OPERATOR_NOT_FOUND_ERROR.var.scalar.constraintp = NULL;
    RDB_OPERATOR_NOT_FOUND_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_OPERATOR_NOT_FOUND_ERROR.comparep = NULL;

    RDB_ELEMENT_EXISTS_ERROR.kind = RDB_TP_SCALAR;
    RDB_ELEMENT_EXISTS_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_ELEMENT_EXISTS_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_ELEMENT_EXISTS_ERROR.name = "ELEMENT_EXISTS_ERROR";
    RDB_ELEMENT_EXISTS_ERROR.var.scalar.repc = 1;
    RDB_ELEMENT_EXISTS_ERROR.var.scalar.repv = &element_exists_rep;
    RDB_ELEMENT_EXISTS_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_ELEMENT_EXISTS_ERROR.var.scalar.constraintp = NULL;
    RDB_ELEMENT_EXISTS_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_ELEMENT_EXISTS_ERROR.comparep = NULL;

    RDB_KEY_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_KEY_VIOLATION_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_KEY_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_KEY_VIOLATION_ERROR.name = "KEY_VIOLATION_ERROR";
    RDB_KEY_VIOLATION_ERROR.var.scalar.repc = 1;
    RDB_KEY_VIOLATION_ERROR.var.scalar.repv = &key_violation_rep;
    RDB_KEY_VIOLATION_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_KEY_VIOLATION_ERROR.var.scalar.constraintp = NULL;
    RDB_KEY_VIOLATION_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_KEY_VIOLATION_ERROR.comparep = NULL;

    RDB_NOT_SUPPORTED_ERROR.kind = RDB_TP_SCALAR;
    RDB_NOT_SUPPORTED_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_NOT_SUPPORTED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_NOT_SUPPORTED_ERROR.name = "NOT_SUPPORTED_ERROR";
    RDB_NOT_SUPPORTED_ERROR.var.scalar.repc = 1;
    RDB_NOT_SUPPORTED_ERROR.var.scalar.repv = &not_supported_rep;
    RDB_NOT_SUPPORTED_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_NOT_SUPPORTED_ERROR.var.scalar.constraintp = NULL;
    RDB_NOT_SUPPORTED_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_NOT_SUPPORTED_ERROR.comparep = NULL;

    RDB_ATTRIBUTE_NOT_FOUND_ERROR.kind = RDB_TP_SCALAR;
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.name = "ATTRIBUTE_NOT_FOUND_ERROR";
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.var.scalar.repc = 1;
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.var.scalar.repv = &attribute_not_found_rep;
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.var.scalar.constraintp = NULL;
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_ATTRIBUTE_NOT_FOUND_ERROR.comparep = NULL;

    RDB_PREDICATE_VIOLATION_ERROR.kind = RDB_TP_SCALAR;
    RDB_PREDICATE_VIOLATION_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_PREDICATE_VIOLATION_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_PREDICATE_VIOLATION_ERROR.name = "PREDICATE_VIOLATION_ERROR";
    RDB_PREDICATE_VIOLATION_ERROR.var.scalar.repc = 1;
    RDB_PREDICATE_VIOLATION_ERROR.var.scalar.repv = &predicate_violation_rep;
    RDB_PREDICATE_VIOLATION_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_PREDICATE_VIOLATION_ERROR.var.scalar.constraintp = NULL;
    RDB_PREDICATE_VIOLATION_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_PREDICATE_VIOLATION_ERROR.comparep = NULL;

    RDB_SYSTEM_ERROR.kind = RDB_TP_SCALAR;
    RDB_SYSTEM_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_SYSTEM_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_SYSTEM_ERROR.name = "SYSTEM_ERROR";
    RDB_SYSTEM_ERROR.var.scalar.repc = 1;
    RDB_SYSTEM_ERROR.var.scalar.repv = &system_rep;
    RDB_SYSTEM_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_SYSTEM_ERROR.var.scalar.constraintp = NULL;
    RDB_SYSTEM_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_SYSTEM_ERROR.comparep = NULL;

    RDB_RESOURCE_NOT_FOUND_ERROR.kind = RDB_TP_SCALAR;
    RDB_RESOURCE_NOT_FOUND_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_RESOURCE_NOT_FOUND_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_RESOURCE_NOT_FOUND_ERROR.name = "RESOURCE_NOT_FOUND_ERROR";
    RDB_RESOURCE_NOT_FOUND_ERROR.var.scalar.repc = 1;
    RDB_RESOURCE_NOT_FOUND_ERROR.var.scalar.repv = &resource_not_found_rep;
    RDB_RESOURCE_NOT_FOUND_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_RESOURCE_NOT_FOUND_ERROR.var.scalar.constraintp = NULL;
    RDB_RESOURCE_NOT_FOUND_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_RESOURCE_NOT_FOUND_ERROR.comparep = NULL;

    RDB_INTERNAL_ERROR.kind = RDB_TP_SCALAR;
    RDB_INTERNAL_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_INTERNAL_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_INTERNAL_ERROR.name = "INTERNAL_ERROR";
    RDB_INTERNAL_ERROR.var.scalar.repc = 1;
    RDB_INTERNAL_ERROR.var.scalar.repv = &internal_rep;
    RDB_INTERNAL_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_INTERNAL_ERROR.var.scalar.constraintp = NULL;
    RDB_INTERNAL_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_INTERNAL_ERROR.comparep = NULL;

    RDB_LOCK_NOT_GRANTED_ERROR.kind = RDB_TP_SCALAR;
    RDB_LOCK_NOT_GRANTED_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_LOCK_NOT_GRANTED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_LOCK_NOT_GRANTED_ERROR.name = "LOCK_NOT_GRANTED_ERROR";
    RDB_LOCK_NOT_GRANTED_ERROR.var.scalar.repc = 1;
    RDB_LOCK_NOT_GRANTED_ERROR.var.scalar.repv = &lock_not_granted_rep;
    RDB_LOCK_NOT_GRANTED_ERROR.var.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_LOCK_NOT_GRANTED_ERROR.var.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_LOCK_NOT_GRANTED_ERROR.var.scalar.constraintp = NULL;
    RDB_LOCK_NOT_GRANTED_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_LOCK_NOT_GRANTED_ERROR.comparep = NULL;

    RDB_AGGREGATE_UNDEFINED_ERROR.kind = RDB_TP_SCALAR;
    RDB_AGGREGATE_UNDEFINED_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_AGGREGATE_UNDEFINED_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_AGGREGATE_UNDEFINED_ERROR.name = "AGGREGATE_UNDEFINED_ERROR";
    RDB_AGGREGATE_UNDEFINED_ERROR.var.scalar.repc = 1;
    RDB_AGGREGATE_UNDEFINED_ERROR.var.scalar.repv = &aggregate_undefined_rep;
    RDB_AGGREGATE_UNDEFINED_ERROR.var.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_AGGREGATE_UNDEFINED_ERROR.var.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_AGGREGATE_UNDEFINED_ERROR.var.scalar.constraintp = NULL;
    RDB_AGGREGATE_UNDEFINED_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_AGGREGATE_UNDEFINED_ERROR.comparep = NULL;

    RDB_VERSION_MISMATCH_ERROR.kind = RDB_TP_SCALAR;
    RDB_VERSION_MISMATCH_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_VERSION_MISMATCH_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_VERSION_MISMATCH_ERROR.name = "VERSION_MISMATCH_ERROR";
    RDB_VERSION_MISMATCH_ERROR.var.scalar.repc = 1;
    RDB_VERSION_MISMATCH_ERROR.var.scalar.repv = &version_mismatch_rep;
    RDB_VERSION_MISMATCH_ERROR.var.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_VERSION_MISMATCH_ERROR.var.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_VERSION_MISMATCH_ERROR.var.scalar.constraintp = NULL;
    RDB_VERSION_MISMATCH_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_VERSION_MISMATCH_ERROR.comparep = NULL;

    RDB_DEADLOCK_ERROR.kind = RDB_TP_SCALAR;
    RDB_DEADLOCK_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_DEADLOCK_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_DEADLOCK_ERROR.name = "DEADLOCK_ERROR";
    RDB_DEADLOCK_ERROR.var.scalar.repc = 1;
    RDB_DEADLOCK_ERROR.var.scalar.repv = &deadlock_rep;
    RDB_DEADLOCK_ERROR.var.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_DEADLOCK_ERROR.var.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_DEADLOCK_ERROR.var.scalar.constraintp = NULL;
    RDB_DEADLOCK_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_DEADLOCK_ERROR.comparep = NULL;

    RDB_FATAL_ERROR.kind = RDB_TP_SCALAR;
    RDB_FATAL_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_FATAL_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_FATAL_ERROR.name = "FATAL_ERROR";
    RDB_FATAL_ERROR.var.scalar.repc = 1;
    RDB_FATAL_ERROR.var.scalar.repv = &fatal_rep;
    RDB_FATAL_ERROR.var.scalar.arep =
            RDB_create_tuple_type(0, NULL, ecp);
    if (RDB_FATAL_ERROR.var.scalar.arep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_FATAL_ERROR.var.scalar.constraintp = NULL;
    RDB_FATAL_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_FATAL_ERROR.comparep = NULL;

    RDB_SYNTAX_ERROR.kind = RDB_TP_SCALAR;
    RDB_SYNTAX_ERROR.var.scalar.builtin = RDB_TRUE;
    RDB_SYNTAX_ERROR.ireplen = RDB_VARIABLE_LEN;
    RDB_SYNTAX_ERROR.name = "SYNTAX_ERROR";
    RDB_SYNTAX_ERROR.var.scalar.repc = 1;
    RDB_SYNTAX_ERROR.var.scalar.repv = &syntax_rep;
    RDB_SYNTAX_ERROR.var.scalar.arep = &RDB_STRING;
    RDB_SYNTAX_ERROR.var.scalar.constraintp = NULL;
    RDB_SYNTAX_ERROR.var.scalar.sysimpl = RDB_TRUE;
    RDB_SYNTAX_ERROR.comparep = NULL;

    return RDB_OK;
}

RDB_bool
RDB_type_is_numeric(const RDB_type *typ) {
    return (RDB_bool)(typ == &RDB_INTEGER || typ == &RDB_FLOAT
            || typ == &RDB_DOUBLE);
}

RDB_type *
_RDB_dup_nonscalar_type(RDB_type *typ, RDB_exec_context *ecp)
{
    RDB_type *restyp;

    switch (typ->kind) {
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            restyp = malloc(sizeof (RDB_type));
            if (restyp == NULL) {
                RDB_raise_no_memory(ecp);
                return NULL;
            }
            restyp->name = NULL;
            restyp->kind = typ->kind;
            restyp->ireplen = RDB_VARIABLE_LEN;
            restyp->var.basetyp = _RDB_dup_nonscalar_type(typ->var.basetyp,
                    ecp);
            if (restyp->var.basetyp == NULL) {
                free(restyp);
                return NULL;
            }
            return restyp;
        case RDB_TP_TUPLE:
            return RDB_create_tuple_type(typ->var.tuple.attrc,
                    typ->var.tuple.attrv, ecp);
        case RDB_TP_SCALAR:
            return typ;
    }
    abort();
}

RDB_type *
RDB_create_tuple_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *tuptyp;
    int i, j;

    tuptyp = malloc(sizeof(RDB_type));
    if (tuptyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    tuptyp->name = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->ireplen = RDB_VARIABLE_LEN;
    tuptyp->var.tuple.attrv = malloc(sizeof(RDB_attr) * attrc);
    if (tuptyp->var.tuple.attrv == NULL) {
        free(tuptyp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        tuptyp->var.tuple.attrv[i].typ = NULL;
        tuptyp->var.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < attrc; i++) {
        /* Check if name appears twice */
        for (j = i + 1; j < attrc; j++) {
            if (strcmp(attrv[i].name, attrv[j].name) == 0) {
                RDB_raise_invalid_argument("duplicate attribute name", ecp);
                goto error;
            }
        }

        tuptyp->var.tuple.attrv[i].typ = _RDB_dup_nonscalar_type(
                attrv[i].typ, ecp);
        if (tuptyp->var.tuple.attrv[i].typ == NULL) {
            goto error;
        }
        tuptyp->var.tuple.attrv[i].name = RDB_dup_str(attrv[i].name);
        if (tuptyp->var.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        tuptyp->var.tuple.attrv[i].defaultp = NULL;
    }
    tuptyp->var.tuple.attrc = attrc;

    return tuptyp;

error:
    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp = &tuptyp->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free(attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ->name == NULL)
                RDB_drop_type(attrp->typ, ecp, NULL);
        }
    }
    free(tuptyp->var.tuple.attrv);
    free(tuptyp);

    return NULL;
}

RDB_type *
RDB_create_relation_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *typ = malloc(sizeof (RDB_type));    
    if (typ == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    
    typ->name = NULL;
    typ->kind = RDB_TP_RELATION;
    typ->ireplen = RDB_VARIABLE_LEN;
    typ->var.basetyp = RDB_create_tuple_type(attrc, attrv, ecp);
    if (typ->var.basetyp == NULL) {
        free(typ);
        return NULL;
    }
    return typ;
}

RDB_type *
RDB_create_array_type(RDB_type *basetyp)
{
    RDB_type *typ = malloc(sizeof (RDB_type));
    
    if (typ == NULL)
        return NULL;
    
    typ->name = NULL;
    typ->kind = RDB_TP_ARRAY;
    typ->ireplen = RDB_VARIABLE_LEN;
    typ->var.basetyp = basetyp;

    return typ;
}

RDB_bool
RDB_type_is_scalar(const RDB_type *typ)
{
    return (typ->kind == RDB_TP_SCALAR);
}

RDB_attr *
RDB_type_attrs(RDB_type *typ, int *attrc)
{
    if (typ->kind == RDB_TP_RELATION) {
        typ = typ->var.basetyp;
    }
    if (typ->kind != RDB_TP_TUPLE) {
        return NULL;
    }
    *attrc = typ->var.tuple.attrc;
    return typ->var.tuple.attrv;
}

static void
free_type(RDB_type *typ, RDB_exec_context *ecp)
{
    int i;

    free(typ->name);

    switch (typ->kind) {
        case RDB_TP_TUPLE:
            for (i = 0; i < typ->var.tuple.attrc; i++) {
                RDB_type *attrtyp = typ->var.tuple.attrv[i].typ;
            
                free(typ->var.tuple.attrv[i].name);
                if (!RDB_type_is_scalar(attrtyp))
                    RDB_drop_type(attrtyp, ecp, NULL);
                if (typ->var.tuple.attrv[i].defaultp != NULL) {
                    RDB_destroy_obj(typ->var.tuple.attrv[i].defaultp, ecp);
                    free(typ->var.tuple.attrv[i].defaultp);
                }
            }
            free(typ->var.tuple.attrv);
            break;
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            if (!RDB_type_is_scalar(typ->var.basetyp))
                RDB_drop_type(typ->var.basetyp, ecp, NULL);
            break;
        case RDB_TP_SCALAR:
            if (typ->var.scalar.repc > 0) {
                int i, j;
                
                for (i = 0; i < typ->var.scalar.repc; i++) {
                    for (j = 0; j < typ->var.scalar.repv[i].compc; j++) {
                        free(typ->var.scalar.repv[i].compv[i].name);
                    }
                    free(typ->var.scalar.repv[i].compv);
                }
                free(typ->var.scalar.repv);
            }
            if (typ->var.scalar.arep != NULL
                    && typ->var.scalar.arep->name == NULL)
                RDB_drop_type(typ->var.scalar.arep, ecp, NULL);
            break;
        default:
            abort();
    }
    typ->kind = (enum _RDB_tp_kind) -1;
    free(typ);
}    

int
RDB_define_type(const char *name, int repc, const RDB_possrep repv[],
                RDB_expression *constraintp, RDB_exec_context *ecp,
                RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object conval;
    RDB_object typedata;
    int ret;
    int i, j;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&conval);
    RDB_init_obj(&typedata);

    ret = RDB_binary_set(&typedata, 0, NULL, 0, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    /*
     * Insert tuple into SYS_TYPES
     */

    ret = RDB_tuple_set_string(&tpl, "TYPENAME", name, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set(&tpl, "I_AREP_TYPE", &typedata, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "I_AREP_LEN", -2, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_bool(&tpl, "I_SYSIMPL", RDB_FALSE, ecp);
    if (ret != RDB_OK)
        goto error;

    /* Store constraint in tuple */
    ret = _RDB_expr_to_binobj(&conval, constraintp, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set(&tpl, "I_CONSTRAINT", &conval, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(txp->dbp->dbrootp->types_tbp, &tpl, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    /*
     * Insert tuple into SYS_POSSREPS
     */   

    for (i = 0; i < repc; i++) {
        char *prname = repv[i].name;

        if (prname == NULL) {
            /* possrep name may be NULL if there's only 1 possrep */
            if (repc > 1) {
                RDB_raise_invalid_argument("possrep name is NULL", ecp);
                goto error;
            }
            prname = (char *)name;
        }
        ret = RDB_tuple_set_string(&tpl, "POSSREPNAME", prname, ecp);
        if (ret != RDB_OK)
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
            ret = RDB_tuple_set_int(&tpl, "COMPNO", (RDB_int)j, ecp);
            if (ret != RDB_OK)
                goto error;
            ret = RDB_tuple_set_string(&tpl, "COMPNAME", cname, ecp);
            if (ret != RDB_OK)
                goto error;
            ret = RDB_tuple_set_string(&tpl, "COMPTYPENAME",
                    repv[i].compv[j].typ->name, ecp);
            if (ret != RDB_OK)
                goto error;

            ret = RDB_insert(txp->dbp->dbrootp->possrepcomps_tbp, &tpl, ecp,
                    txp);
            if (ret != RDB_OK)
                goto error;
        }
    }

    RDB_destroy_obj(&typedata, ecp);
    RDB_destroy_obj(&conval, ecp);    
    RDB_destroy_obj(&tpl, ecp);

    if (constraintp != NULL)
        RDB_drop_expr(constraintp, ecp);
    
    return RDB_OK;
    
error:
    RDB_destroy_obj(&typedata, ecp);
    RDB_destroy_obj(&conval, ecp);    
    RDB_destroy_obj(&tpl, ecp);

    return RDB_ERROR;
}

/* Implements a system-generated selector */
int
_RDB_sys_select(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_type *typ = retvalp->typ;
    RDB_possrep *prp;

    /* Find possrep */
    prp = _RDB_get_possrep(typ, name);
    if (prp == NULL) {
        RDB_raise_invalid_argument("component name is NULL", ecp);
        return RDB_ERROR;
    }

    /* If *retvalp carries a value, it must match the type */
    if (retvalp->kind != RDB_OB_INITIAL
            && (retvalp->typ == NULL
                || !RDB_type_equals(retvalp->typ, typ))) {
        RDB_raise_type_mismatch("wrong selectot ret urn type", ecp);
        return RDB_ERROR;
    }

    if (argc == 1) {
        /* Copy value */
        if (_RDB_copy_obj(retvalp, argv[0], ecp, NULL) != RDB_OK)
            return RDB_ERROR;
    } else {
        /* Copy tuple attributes */
        int i;

        RDB_destroy_obj(retvalp, ecp);
        RDB_init_obj(retvalp);
        for (i = 0; i < argc; i++) {
            if (RDB_tuple_set(retvalp, typ->var.scalar.repv[0].compv[i].name,
                    argv[i], ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }
    retvalp->typ = typ;
    return RDB_OK;
}

static int
create_selector(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int compc = typ->var.scalar.repv[0].compc;
    RDB_type **argtv = malloc(sizeof(RDB_type *) * compc);
    if (argtv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < compc; i++)
        argtv[i] = typ->var.scalar.repv[0].compv[i].typ;
    ret = RDB_create_ro_op(typ->var.scalar.repv[0].name, compc, argtv, typ,
            "libduro", "_RDB_sys_select", typ->name, strlen(typ->name) + 1,
            ecp, txp);
    free(argtv);
    return ret;
}

int
RDB_implement_type(const char *name, RDB_type *arep, RDB_int areplen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp, *wherep;
    RDB_attr_update upd[3];
    RDB_object typedata;
    int ret;
    int i;
    RDB_bool sysimpl = (arep == NULL) && (areplen == -1);

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
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
        if (typ->var.scalar.repc != 1) {
            RDB_raise_invalid_argument("invalid # of possreps", ecp);
            return RDB_ERROR;
        }

        compc = typ->var.scalar.repv[0].compc;
        if (compc == 1) {
            arep = typ->var.scalar.repv[0].compv[0].typ;
        } else {
            /* More than one component, so internal rep is a tuple */
            arep = RDB_create_tuple_type(typ->var.scalar.repv[0].compc,
                    typ->var.scalar.repv[0].compv, ecp);
            if (arep == NULL)
                return RDB_ERROR;
        }

        typ->var.scalar.arep = arep;
        typ->var.scalar.sysimpl = sysimpl;
        typ->ireplen = arep->ireplen;

        ret = create_selector(typ, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    exp = RDB_expr_var("TYPENAME", ecp);
    if (exp == NULL) {
        return RDB_ERROR;
    }
    wherep = RDB_ro_op_va("=", ecp, exp, RDB_string_to_expr(name, ecp),
            (RDB_expression *) NULL);
    if (wherep == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }    

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
        if (upd[2].exp == NULL) {
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            RDB_destroy_obj(&typedata, ecp);
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

int
RDB_drop_type(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (typ->kind == RDB_TP_SCALAR && typ->var.scalar.builtin) {
        RDB_raise_invalid_argument("attempt to drop built-in type", ecp);
        return RDB_ERROR;
    }

    if (RDB_type_is_scalar(typ)) {
        RDB_expression *wherep;
        RDB_type *ntp = NULL;

        if (!RDB_tx_is_running(txp)) {
            RDB_raise_invalid_tx(ecp);
            return RDB_ERROR;
        }

        /* !! should check if the type is still used by a table */

        /* Delete selector */
        ret = RDB_drop_op(typ->name, ecp, txp);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        }

        /* Delete type from type table by puting a NULL pointer into it */
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->typemap, typ->name, ntp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* Delete type from database */
        wherep = RDB_ro_op_va("=", ecp, RDB_expr_var("TYPENAME", ecp),
                RDB_string_to_expr(typ->name, ecp), (RDB_expression *) NULL);
        if (wherep == NULL) {
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->types_tbp, wherep, ecp, txp);
        if (ret == RDB_ERROR) {
            RDB_drop_expr(wherep, ecp);
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->possrepcomps_tbp, wherep, ecp,
                txp);
        if (ret == RDB_ERROR) {
            RDB_drop_expr(wherep, ecp);
            return ret;
        }
    }
    free_type(typ, ecp);
    return RDB_OK;
}

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
            return RDB_type_equals(typ1->var.basetyp, typ2->var.basetyp);
        case RDB_TP_TUPLE:
            {
                int i, j;
                int attrcnt = typ1->var.tuple.attrc;

                if (attrcnt != typ2->var.tuple.attrc)
                    return RDB_FALSE;
                    
                /* check if all attributes of typ1 also appear in typ2 */
                for (i = 0; i < attrcnt; i++) {
                    for (j = 0; j < attrcnt; j++) {
                        if (RDB_type_equals(typ1->var.tuple.attrv[i].typ,
                                typ2->var.tuple.attrv[j].typ)
                                && (strcmp(typ1->var.tuple.attrv[i].name,
                                typ2->var.tuple.attrv[j].name) == 0))
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

char *
RDB_type_name(const RDB_type *typ)
{
    return typ->name;
}

RDB_type *
RDB_type_attr_type(const RDB_type *typ, const char *name)
{
    RDB_attr *attrp;

    switch (typ->kind) {
        case RDB_TP_RELATION:
            attrp = _RDB_tuple_type_attr(typ->var.basetyp, name);
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
    RDB_type *newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->var.tuple.attrc = typ->var.tuple.attrc + attrc;
    newtyp->var.tuple.attrv = malloc(sizeof (RDB_attr)
            * (newtyp->var.tuple.attrc));
    if (newtyp->var.tuple.attrv == NULL) {
        free(newtyp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        newtyp->var.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        newtyp->var.tuple.attrv[i].name =
                RDB_dup_str(typ->var.tuple.attrv[i].name);
        if (newtyp->var.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtyp->var.tuple.attrv[i].typ =
                _RDB_dup_nonscalar_type(typ->var.tuple.attrv[i].typ, ecp);
        if (newtyp->var.tuple.attrv[i].typ == NULL) {
            goto error;
        }
        newtyp->var.tuple.attrv[i].defaultp = NULL;
    }
    for (i = 0; i < attrc; i++) {
        /*
         * Check if the attribute is already present in the original tuple type
         */
        if (_RDB_tuple_type_attr(typ, attrv[i].name) != NULL) {
            RDB_raise_invalid_argument("attribute exists", ecp);
            goto error;
        }

        newtyp->var.tuple.attrv[typ->var.tuple.attrc + i].name =
                RDB_dup_str(attrv[i].name);
        if (newtyp->var.tuple.attrv[typ->var.tuple.attrc + i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtyp->var.tuple.attrv[typ->var.tuple.attrc + i].typ =
                _RDB_dup_nonscalar_type(attrv[i].typ, ecp);
        if (newtyp->var.tuple.attrv[typ->var.tuple.attrc + i].typ == NULL) {
            goto error;
        }
        newtyp->var.tuple.attrv[typ->var.tuple.attrc + i].defaultp = NULL;
    }
    return newtyp;

error:
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        free(newtyp->var.tuple.attrv[i].name);
        if (newtyp->var.tuple.attrv[i].typ != NULL
                && !RDB_type_is_scalar(newtyp->var.tuple.attrv[i].typ)) {
            RDB_drop_type(newtyp->var.tuple.attrv[i].typ, ecp, NULL);
        }
    }
    free(newtyp->var.tuple.attrv);
    free(newtyp);
    return NULL;
}

RDB_type *
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;
    newtyp->var.basetyp = RDB_extend_tuple_type(typ->var.basetyp, attrc, attrv,
            ecp);
    if (newtyp->var.basetyp == NULL) {
        free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_type *
RDB_join_tuple_types(const RDB_type *typ1, const RDB_type *typ2,
        RDB_exec_context *ecp)
{
    RDB_type *newtyp;
    int attrc;
    int i, j;
    
    /* Create new tuple type */
    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    
    /* calculate new # of attributes as the sum of the # of attributes
     * of both types.
     * That often will be too high; in this case it is reduced later.
     */
    newtyp->var.tuple.attrc = typ1->var.tuple.attrc
            + typ2->var.tuple.attrc;
    newtyp->var.tuple.attrv = malloc(sizeof (RDB_attr)
            * newtyp->var.tuple.attrc);

    for (i = 0; i < typ1->var.tuple.attrc; i++)
        newtyp->var.tuple.attrv[i].name = NULL;

    /* copy attributes from first tuple type */
    for (i = 0; i < typ1->var.tuple.attrc; i++) {
        newtyp->var.tuple.attrv[i].name = RDB_dup_str(
                typ1->var.tuple.attrv[i].name);
        newtyp->var.tuple.attrv[i].typ = 
                _RDB_dup_nonscalar_type(typ1->var.tuple.attrv[i].typ, ecp);
        if (newtyp->var.tuple.attrv[i].typ == NULL)
            goto error;
        newtyp->var.tuple.attrv[i].defaultp = NULL;
    }
    attrc = typ1->var.tuple.attrc;

    /* add attributes from second tuple type */
    for (i = 0; i < typ2->var.tuple.attrc; i++) {
        for (j = 0; j < typ1->var.tuple.attrc; j++) {
            if (strcmp(typ2->var.tuple.attrv[i].name,
                    typ1->var.tuple.attrv[j].name) == 0) {
                /* If two attributes match by name, they must be of
                   the same type */
                if (!RDB_type_equals(typ2->var.tuple.attrv[i].typ,
                        typ1->var.tuple.attrv[j].typ)) {
                    RDB_raise_type_mismatch("JOIN attribute types do not match",
                            ecp);
                    goto error;
                }
                break;
            }
        }
        if (j >= typ1->var.tuple.attrc) {
            /* attribute not found, so add it to result type */
            newtyp->var.tuple.attrv[attrc].name = RDB_dup_str(
                    typ2->var.tuple.attrv[i].name);
            newtyp->var.tuple.attrv[attrc].typ =
                    _RDB_dup_nonscalar_type(typ2->var.tuple.attrv[i].typ, ecp);
            if (newtyp->var.tuple.attrv[attrc].typ == NULL)
                goto error;
            newtyp->var.tuple.attrv[attrc].defaultp = NULL;
            attrc++;
        }
    }

    /* adjust array size, if necessary */    
    if (attrc < newtyp->var.tuple.attrc) {
        newtyp->var.tuple.attrc = attrc;
        newtyp->var.tuple.attrv = realloc(newtyp->var.tuple.attrv,
                sizeof(RDB_attr) * attrc);
    }
    return newtyp;

error:
    for (i = 0; i < typ1->var.tuple.attrc; i++)
        free(newtyp->var.tuple.attrv[i].name);

    free(newtyp);
    return NULL;
}

RDB_type *
RDB_join_relation_types(const RDB_type *typ1, const RDB_type *typ2,
                     RDB_exec_context *ecp)
{
    RDB_type *newtyp;

    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->var.basetyp = RDB_join_tuple_types(typ1->var.basetyp,
            typ2->var.basetyp, ecp);
    if (newtyp->var.basetyp == NULL) {
        free(newtyp);
        return NULL;
    }
    return newtyp;
}

/* Return a pointer to the RDB_attr strcuture of the attribute with name attrname in the tuple
   type pointed to by tutyp. */
RDB_attr *
_RDB_tuple_type_attr(const RDB_type *tuptyp, const char *attrname)
{
    int i;
    
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        if (strcmp(tuptyp->var.tuple.attrv[i].name, attrname) == 0)
            return &tuptyp->var.tuple.attrv[i];
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

    tuptyp = malloc(sizeof (RDB_type));
    if (tuptyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    tuptyp->name = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->var.tuple.attrc = attrc;
    tuptyp->var.tuple.attrv = malloc(attrc * sizeof (RDB_attr));
    if (tuptyp->var.tuple.attrv == NULL) {
        free(tuptyp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < attrc; i++)
        tuptyp->var.tuple.attrv[i].name = NULL;

    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp;
        char *attrname = RDB_dup_str(attrv[i]);
        if (attrname == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }

        tuptyp->var.tuple.attrv[i].name = attrname;

        attrp = _RDB_tuple_type_attr(typ, attrname);
        if (attrp == NULL) {
            RDB_raise_attribute_not_found(attrname, ecp);
            goto error;
        }
        tuptyp->var.tuple.attrv[i].typ = _RDB_dup_nonscalar_type(attrp->typ, ecp);
        if (tuptyp->var.tuple.attrv[i].typ == NULL)
            goto error;

        tuptyp->var.tuple.attrv[i].defaultp = NULL;
    }

    return tuptyp;

error:
    for (i = 0; i < attrc; i++)
        free(tuptyp->var.tuple.attrv[i].name);
    free(tuptyp->var.tuple.attrv);
    free(tuptyp);
    return NULL;
}

RDB_type *
RDB_project_relation_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_exec_context *ecp)
{
    RDB_type *reltyp = malloc(sizeof (RDB_type));
    if (reltyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    reltyp->var.basetyp = RDB_project_tuple_type(typ->var.basetyp, attrc,
            attrv, ecp);
    if (reltyp->var.basetyp == NULL) {
        free(reltyp);
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
            RDB_raise_attribute_not_found(renv[i].from, ecp);
            return NULL;
        }

        /* Check if the dest attribute does not exist */
        if (_RDB_tuple_type_attr(typ, renv[i].to) != NULL) {
            RDB_raise_attribute_not_found(renv[i].to, ecp);
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

    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->var.tuple.attrc = typ->var.tuple.attrc;
    newtyp->var.tuple.attrv = malloc (typ->var.tuple.attrc * sizeof(RDB_attr));
    if (newtyp->var.tuple.attrv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    for (i = 0; i < typ->var.tuple.attrc; i++)
        newtyp->var.tuple.attrv[i].name = NULL;
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        char *attrname = typ->var.tuple.attrv[i].name; 
        int ai = _RDB_find_rename_from(renc, renv, attrname);

        /* check if the attribute has been renamed */
        newtyp->var.tuple.attrv[i].typ = _RDB_dup_nonscalar_type(
                typ->var.tuple.attrv[i].typ, ecp);
        if (newtyp->var.tuple.attrv[i].typ == NULL)
            goto error;

        if (ai >= 0)
            newtyp->var.tuple.attrv[i].name = RDB_dup_str(renv[ai].to);
        else
            newtyp->var.tuple.attrv[i].name = RDB_dup_str(attrname);
        if (newtyp->var.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtyp->var.tuple.attrv[i].defaultp = NULL;
     }
     return newtyp;

error:
    if (newtyp->var.tuple.attrv != NULL) {
        for (i = 0; i < newtyp->var.tuple.attrc; i++)
            free(newtyp->var.tuple.attrv[i].name);
        free(newtyp->var.tuple.attrv);
    }

    free(newtyp);
    return NULL;
}

RDB_type *
RDB_rename_relation_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->var.basetyp = RDB_rename_tuple_type(typ->var.basetyp, renc, renv,
            ecp);
    if (newtyp->var.basetyp == NULL) {
        free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_possrep *
_RDB_get_possrep(RDB_type *typ, const char *repname)
{
    int i;

    if (!RDB_type_is_scalar(typ))
        return NULL;
    for (i = 0; i < typ->var.scalar.repc
            && strcmp(typ->var.scalar.repv[i].name, repname) != 0;
            i++);
    if (i >= typ->var.scalar.repc)
        return NULL;
    return &typ->var.scalar.repv[i];
}

RDB_attr *
_RDB_get_icomp(RDB_type *typ, const char *compname)
{
    int i, j;

    for (i = 0; i < typ->var.scalar.repc; i++) {
        for (j = 0; j < typ->var.scalar.repv[i].compc; j++) {
            if (strcmp(typ->var.scalar.repv[i].compv[j].name, compname) == 0)
                return &typ->var.scalar.repv[i].compv[j];
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
    dstp->typ = _RDB_dup_nonscalar_type(srcp->typ, ecp);
    if (dstp->typ == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    dstp->defaultp = NULL;
    return RDB_OK;
}

RDB_type *
aggr_type(RDB_type *tuptyp, RDB_type *attrtyp, RDB_aggregate_op op,
          RDB_exec_context *ecp)
{
    if (op == RDB_COUNT || op == RDB_COUNTD) {
        return &RDB_INTEGER;
    }

    switch (op) {
        /* only to avoid compiler warnings */
        case RDB_COUNTD:
        case RDB_COUNT:

        case RDB_AVGD:
        case RDB_AVG:
            if (!RDB_type_is_numeric(attrtyp)) {
                RDB_raise_type_mismatch("attribute type must be numeric", ecp);
                return NULL;
            }
            return &RDB_DOUBLE;
        case RDB_SUM:
        case RDB_SUMD:
        case RDB_MAX:
        case RDB_MIN:
            if (!RDB_type_is_numeric(attrtyp)) {
                RDB_raise_type_mismatch("attribute type must be numeric", ecp);
                return NULL;
            }
            return attrtyp;
        case RDB_ALL:
        case RDB_ANY:
            if (attrtyp != &RDB_BOOLEAN) {
                RDB_raise_type_mismatch("attribute type must be BOOLEAN", ecp);
                return NULL;
            }
            return &RDB_BOOLEAN;
     }
     /* Must never be reached */
     abort();
}

RDB_type *
RDB_summarize_type(RDB_type *tb1typ, RDB_type *tb2typ,
        int addc, const RDB_summarize_add addv[],
        int avgc, char **avgv, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_type *newtyp;
    int attrc = addc + avgc;
    RDB_attr *attrv = malloc(sizeof (RDB_attr) * attrc);
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < addc; i++) {
        if (addv[i].op == RDB_COUNT) {
            attrv[i].typ = &RDB_INTEGER;
        } else {
            RDB_type *typ;

            typ = RDB_expr_type(addv[i].exp, tb1typ->var.basetyp, ecp, txp);
            if (typ == NULL)
                goto error;
            attrv[i].typ = aggr_type(tb1typ->var.basetyp, typ, addv[i].op,
                    ecp);
            if (attrv[i].typ == NULL) {
                if (!RDB_type_is_scalar(typ))
                    RDB_drop_type(typ, ecp, NULL);
                goto error;
            }
        }

        attrv[i].name = addv[i].name;
    }
    for (i = 0; i < avgc; i++) {
        attrv[addc + i].name = avgv[i];
        attrv[addc + i].typ = &RDB_INTEGER;
    }

    newtyp = RDB_extend_relation_type(tb2typ, attrc, attrv, ecp);
    if (newtyp == NULL) {
        goto error;
    }

    free(attrv);
    return newtyp;

error:
    free(attrv);    
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
    attrc = typ->var.tuple.attrc;
    for (i = 0; i < wrapc; i++)
        attrc += 1 - wrapv[i].attrc;

    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->var.tuple.attrc = attrc;
    newtyp->var.tuple.attrv = malloc(attrc * sizeof(RDB_attr));
    if (newtyp->var.tuple.attrv == NULL) {
        free(newtyp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < attrc; i++) {
        newtyp->var.tuple.attrv[i].name = NULL;
        newtyp->var.tuple.attrv[i].typ = NULL;
    }

    /*
     * Copy attributes from wrapv
     */
    for (i = 0; i < wrapc; i++) {
        RDB_type *tuptyp = malloc(sizeof(RDB_type));
        if (tuptyp == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        tuptyp->name = NULL;
        tuptyp->kind = RDB_TP_TUPLE;
        tuptyp->var.tuple.attrc = wrapv[i].attrc;
        tuptyp->var.tuple.attrv = malloc(sizeof(RDB_attr) * wrapv[i].attrc);
        if (tuptyp->var.tuple.attrv == NULL) {
            RDB_raise_no_memory(ecp);
            free(tuptyp);
            goto error;
        }

        for (j = 0; j < wrapv[i].attrc; j++) {
            attrp = _RDB_tuple_type_attr(typ, wrapv[i].attrv[j]);
            if (attrp == NULL) {
                RDB_raise_attribute_not_found(wrapv[i].attrv[j], ecp);
                free(tuptyp->var.tuple.attrv);
                free(tuptyp);
                goto error;
            }

            ret = copy_attr(&tuptyp->var.tuple.attrv[j], attrp, ecp);
            if (ret != RDB_OK) {
                free(tuptyp->var.tuple.attrv);
                free(tuptyp);
                goto error;
            }
        }
        newtyp->var.tuple.attrv[i].name = RDB_dup_str(wrapv[i].attrname);
        newtyp->var.tuple.attrv[i].typ = tuptyp;        
        newtyp->var.tuple.attrv[i].defaultp = NULL;
    }

    /*
     * Copy remaining attributes
     */
    k = wrapc;
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        /* Copy attribute if it does not appear in wrapv */
        for (j = 0; j < wrapc && RDB_find_str(wrapv[j].attrc, wrapv[j].attrv,
                typ->var.tuple.attrv[i].name) == -1; j++);
        if (j == wrapc) {
            /* Not found */
            ret = copy_attr(&newtyp->var.tuple.attrv[k],
                    &typ->var.tuple.attrv[i], ecp);
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
        attrp = &newtyp->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free (attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ == NULL)
                RDB_drop_type(attrp->typ, ecp, NULL);
        }
    }
    free(newtyp->var.tuple.attrv);
    free(newtyp);
    return NULL;
}

RDB_type *
RDB_wrap_relation_type(const RDB_type *typ, int wrapc,
        const RDB_wrapping wrapv[], RDB_exec_context *ecp)
{
    RDB_type *newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->var.basetyp = RDB_wrap_tuple_type(typ->var.basetyp, wrapc, wrapv,
            ecp);
    if (newtyp->var.basetyp == NULL) {
        free(newtyp);
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
    nattrc = typ->var.tuple.attrc;
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_type_attr_type(typ, attrv[i]);
        if (tuptyp == NULL) {
            RDB_raise_attribute_not_found(attrv[i], ecp);
            goto error;
        }
        if (tuptyp->kind != RDB_TP_TUPLE) {
            RDB_raise_invalid_argument("not a tuple", ecp);
            goto error;
        }        
        nattrc += tuptyp->var.tuple.attrc - 1;
    }

    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->var.tuple.attrc = nattrc;
    newtyp->var.tuple.attrv = malloc(nattrc * sizeof(RDB_attr));
    if (newtyp->var.tuple.attrv == NULL) {
        free(newtyp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < nattrc; i++) {
        newtyp->var.tuple.attrv[i].name = NULL;
        newtyp->var.tuple.attrv[i].typ = NULL;
    }

    k = 0;

    /* Copy sub-attributes of attrv */
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_type_attr_type(typ, attrv[i]);

        for (j = 0; j < tuptyp->var.tuple.attrc; j++) {
            ret = copy_attr(&newtyp->var.tuple.attrv[k],
                    &tuptyp->var.tuple.attrv[j], ecp);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }

    /* Copy remaining attributes */
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        /* Copy attribute if it does not appear in attrv */
        if (RDB_find_str(attrc, attrv, typ->var.tuple.attrv[i].name) == -1) {
            ret = copy_attr(&newtyp->var.tuple.attrv[k],
                    &typ->var.tuple.attrv[i], ecp);
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
        attrp = &newtyp->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free (attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ->name == NULL)
                RDB_drop_type(attrp->typ, ecp, NULL);
        }
    }
    free(newtyp->var.tuple.attrv);
    free(newtyp);
    return NULL;
}    

RDB_type *
RDB_unwrap_relation_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->var.basetyp = RDB_unwrap_tuple_type(typ->var.basetyp, attrc, attrv,
            ecp);
    if (newtyp->var.basetyp == NULL) {
        free(newtyp);
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
    rtattrv = malloc(sizeof(RDB_attr) * attrc);
    if (rtattrv == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        attrp = _RDB_tuple_type_attr(typ->var.basetyp, attrv[i]);
        if (attrp == NULL) {
            free(rtattrv);
            RDB_raise_attribute_not_found(attrv[i], ecp);
            return NULL;
        }
        rtattrv[i].typ = attrp->typ;
        rtattrv[i].name = attrp->name;
    }
    gattrtyp = RDB_create_relation_type(attrc, rtattrv, ecp);
    free(rtattrv);
    if (gattrtyp == NULL)
        return NULL;

    /*
     * Create tuple type
     */
    tuptyp = malloc(sizeof (RDB_type));
    if (tuptyp == NULL) {
        RDB_drop_type(gattrtyp, ecp, NULL);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->name = NULL;
    tuptyp->var.tuple.attrc = typ->var.basetyp->var.tuple.attrc + 1 - attrc;
    tuptyp->var.tuple.attrv = malloc(tuptyp->var.tuple.attrc * sizeof(RDB_attr));
    if (tuptyp->var.tuple.attrv == NULL) {
        free(tuptyp);
        RDB_drop_type(gattrtyp, ecp, NULL);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        tuptyp->var.tuple.attrv[i].name = NULL;
        tuptyp->var.tuple.attrv[i].typ = NULL;
    }

    j = 0;
    for (i = 0; i < typ->var.basetyp->var.tuple.attrc; i++) {
        char *attrname = typ->var.basetyp->var.tuple.attrv[i].name;

        if (_RDB_tuple_type_attr(gattrtyp->var.basetyp, attrname) == NULL) {
            if (strcmp(attrname, gattr) == 0) {
                RDB_drop_type(gattrtyp, ecp, NULL);
                RDB_raise_invalid_argument("invalid GROUP", ecp);
                goto error;
            }
            ret = copy_attr(&tuptyp->var.tuple.attrv[j],
                    &typ->var.basetyp->var.tuple.attrv[i], ecp);
            if (ret != RDB_OK) {
                RDB_drop_type(gattrtyp, ecp, NULL);
                goto error;
            }
            tuptyp->var.tuple.attrv[j].defaultp = NULL;
            j++;
        }
    }
    tuptyp->var.tuple.attrv[j].typ = gattrtyp;
    tuptyp->var.tuple.attrv[j].name = RDB_dup_str(gattr);
    if (tuptyp->var.tuple.attrv[j].name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    tuptyp->var.tuple.attrv[j].defaultp = NULL;

    /*
     * Create relation type
     */
    newtyp = malloc(sizeof(RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    newtyp->kind = RDB_TP_RELATION;
    newtyp->name = NULL;
    newtyp->var.basetyp = tuptyp;

    return newtyp;

error:
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        RDB_attr *attrp = &tuptyp->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free(attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ->name == NULL)
                RDB_drop_type(attrp->typ, ecp, NULL);
        }
    }
    free(tuptyp->var.tuple.attrv);
    free(tuptyp);

    return NULL;
}

RDB_type *
RDB_ungroup_type(RDB_type *typ, const char *attr, RDB_exec_context *ecp)
{
    int i, j;
    int ret;
    RDB_type *tuptyp;
    RDB_type *newtyp;
    RDB_attr *relattrp = _RDB_tuple_type_attr(typ->var.basetyp, attr);

    if (relattrp == NULL) {
        RDB_raise_attribute_not_found(attr, ecp);
        return NULL;
    }
    if (relattrp->typ->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("not a relation", ecp);
        return NULL;
    }

    tuptyp = malloc(sizeof (RDB_type));
    if (tuptyp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->name = NULL;

    /* Compute # of attributes */
    tuptyp->var.tuple.attrc = typ->var.basetyp->var.tuple.attrc
            + relattrp->typ->var.basetyp->var.tuple.attrc - 1;

    /* Allocate tuple attributes */
    tuptyp->var.tuple.attrv = malloc(tuptyp->var.tuple.attrc * sizeof(RDB_attr));
    if (tuptyp->var.tuple.attrv == NULL) {
        free(tuptyp);
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        tuptyp->var.tuple.attrv[i].name = NULL;
        tuptyp->var.tuple.attrv[i].typ = NULL;
    }

    /* Copy attributes from the original type */
    j = 0;
    for (i = 0; i < typ->var.basetyp->var.tuple.attrc; i++) {
        if (strcmp(typ->var.basetyp->var.tuple.attrv[i].name,
                attr) != 0) {
            ret = copy_attr(&tuptyp->var.tuple.attrv[j],
                    &typ->var.basetyp->var.tuple.attrv[i], ecp);
            if (ret != RDB_OK)
                goto error;
            j++;
        }
    }

    /* Copy attributes from the attribute type */
    for (i = 0; i < relattrp->typ->var.basetyp->var.tuple.attrc; i++) {
        char *attrname = relattrp->typ->var.basetyp->var.tuple.attrv[i].name;

        /* Check for attribute name clash */
        if (strcmp(attrname, attr) != 0
                && _RDB_tuple_type_attr(typ->var.basetyp, attrname)
                != NULL) {
            RDB_raise_invalid_argument("invalid UNGROUP", ecp);
            goto error;
        }
        ret = copy_attr(&tuptyp->var.tuple.attrv[j],
                    &relattrp->typ->var.basetyp->var.tuple.attrv[i], ecp);
        if (ret != RDB_OK)
            goto error;
        j++;
    }

    /*
     * Create relation type
     */
    newtyp = malloc(sizeof(RDB_type));
    if (newtyp == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    newtyp->kind = RDB_TP_RELATION;
    newtyp->name = NULL;
    newtyp->var.basetyp = tuptyp;

    return newtyp;    

error:
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        RDB_attr *attrp = &tuptyp->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free(attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ->name == NULL)
                RDB_drop_type(attrp->typ, ecp, NULL);
        }
    }
    free(tuptyp->var.tuple.attrv);
    free(tuptyp);

    return NULL;
}
