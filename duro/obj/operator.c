/*
 * operator.c
 *
 *  Created on: 04.10.2013
 *      Author: Rene Hartmann
 */

#include "operator.h"
#include "type.h"

/** @addtogroup op
 * @{
 */

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
 * Return the source code of operator *<var>op</var>.
 *
 * @returns a pointer to the source code, or NULL if no source code
 * was specified when the operator was created.
 */
const char *
RDB_operator_source(const RDB_operator *op)
{
    const char *srcp = RDB_obj_string(&op->source);
    return *srcp == '\0' ? NULL : srcp;
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

RDB_operator *
RDB_new_op_data(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
        RDB_exec_context *ecp)
{
    int i;
    RDB_operator *op = RDB_alloc(sizeof (RDB_operator), ecp);
    if (op == NULL) {
        return NULL;
    }

    RDB_init_obj(&op->source);
    op->name = RDB_dup_str(name);
    if (op->name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    op->paramc = argc;
    if (argc > 0) {
        op->paramv = RDB_alloc(sizeof (RDB_parameter) * argc, ecp);
        if (op->paramv == NULL) {
            goto error;
        }

        for (i = 0; i < argc; i++) {
            op->paramv[i].typ = NULL;
        }
        for (i = 0; i < argc; i++) {
            op->paramv[i].typ = RDB_dup_nonscalar_type(argtv[i], ecp);
            if (op->paramv[i].typ == NULL) {
                goto error;
            }
        }
    } else {
        op->paramv = NULL;
    }

    op->rtyp = rtyp;
    op->modhdl = NULL;
    op->u_data = NULL;
    op->cleanup_fp = NULL;

    return op;

error:
    RDB_destroy_obj(&op->source, ecp);
    if (op->name != NULL)
        RDB_free(op->name);
    if (op->paramv != NULL) {
        for (i = 0; i < op->paramc; i++) {
            if (op->paramv[i].typ != NULL
                   && !RDB_type_is_scalar(op->paramv[i].typ)) {
                RDB_del_nonscalar_type(op->paramv[i].typ, ecp);
            }
        }
    }
    RDB_free(op);
    return NULL;
}

int
RDB_free_op_data(RDB_operator *op, RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (op->rtyp != NULL && !RDB_type_is_scalar(op->rtyp))
        RDB_del_nonscalar_type(op->rtyp, ecp);
    if (op->modhdl != NULL) {
        /* Operator loaded from module */
        lt_dlclose(op->modhdl);
    }
    for (i = 0; i < op->paramc; i++) {
        if (!RDB_type_is_scalar(op->paramv[i].typ))
            RDB_del_nonscalar_type(op->paramv[i].typ, ecp);
    }
    RDB_free(op->paramv);
    RDB_free(op->name);
    if (op->cleanup_fp != NULL)
        (*op->cleanup_fp) (op);
    ret = RDB_destroy_obj(&op->source, ecp);
    RDB_free(op);
    return ret;
}
