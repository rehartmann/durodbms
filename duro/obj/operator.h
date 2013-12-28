/*
 * operator.h
 *
 *  Created on: 04.10.2013
 *      Author: Rene Hartmann
 */

#ifndef OPERATOR_H_
#define OPERATOR_H_

#include "object.h"
#include "excontext.h"

#include <ltdl.h>

/**@addtogroup operator
 * @{
 */

typedef struct RDB_op_data RDB_operator;
typedef struct RDB_transaction RDB_transaction;

typedef int RDB_ro_op_func(int, RDB_object *[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *,
        RDB_object *);

typedef int RDB_upd_op_func(int, RDB_object *[], RDB_operator *,
    RDB_exec_context *, RDB_transaction *);

typedef void RDB_op_cleanup_func(RDB_operator *);

/**
 * Represents a parameter of an operator.
 */
typedef struct RDB_parameter {
    /**
     * Parameter type
     */
    RDB_type *typ;

    /**
     * RDB_TRUE if and only if it's an update parameter.
     * Defined only for update operators.
     */
    RDB_bool update;
} RDB_parameter;

struct RDB_op_data {
    char *name;
    RDB_type *rtyp;
    RDB_object source;
    lt_dlhandle modhdl;
    int paramc;
    RDB_parameter *paramv;
    union {
        RDB_upd_op_func *upd_fp;
        RDB_ro_op_func *ro_fp;
    } opfn;
    void *u_data;
    RDB_op_cleanup_func *cleanup_fp;
};

RDB_parameter *
RDB_get_parameter(const RDB_operator *, int);

/**
 * Return the name of *<var>op</var>
 */
const char *
RDB_operator_name(const RDB_operator *);

/**
 * Return the return type of *<var>op</var> if it's a read-only operator.
 */
RDB_type *
RDB_return_type(const RDB_operator *);

/**
 * Return the source code of operator *<var>op</var>.
 *
 * @returns a pointer to the source code, or NULL if no source code
 * was specified when the operator was created.
 */
const char *
RDB_operator_source(const RDB_operator *);

RDB_parameter *
RDB_get_parameter(const RDB_operator *, int);

const char *
RDB_operator_name(const RDB_operator *);

RDB_type *
RDB_return_type(const RDB_operator *);

const char *
RDB_operator_source(const RDB_operator *);

RDB_type *
RDB_return_type(const RDB_operator *);

void *
RDB_op_u_data(const RDB_operator *);

void
RDB_set_op_u_data(RDB_operator *, void *);

void
RDB_set_op_cleanup_fn(RDB_operator *,  RDB_op_cleanup_func*);

RDB_operator *
RDB_new_op_data(const char *, int, RDB_type *[], RDB_type *,
        RDB_exec_context *);

RDB_operator *
RDB_new_upd_op(const char *, int, RDB_parameter[],
        RDB_upd_op_func *, RDB_exec_context *);

int
RDB_free_op_data(RDB_operator *, RDB_exec_context *);

/**
 * @}
 */

#endif /* OPERATOR_H_ */
