/*
 * Execution context functions.
 *
 * Copyright (C) 2005, 2013, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "excontext.h"
#include "builtintypes.h"
#include <gen/hashmap.h>

#include <errno.h>
#include <string.h>

/** @defgroup excontext Execution context functions
 * @{
 */

/** RDB_init_exec_context initializes the RDB_exec_context structure given by
 * <var>ecp</var>. RDB_init_exec_context must be called before any other
 * operation can be performed on a RDB_exec_context structure.
 */
void
RDB_init_exec_context(RDB_exec_context *ecp)
{
    ecp->error_active = RDB_FALSE;
    ecp->error_retryable = RDB_FALSE;
    ecp->rollback = RDB_FALSE;
    RDB_init_hashmap(&ecp->pmap, 16);
}

/** RDB_destroy_exec_context frees all resources associated with a
 * RDB_exec_context.
 */
void
RDB_destroy_exec_context(RDB_exec_context *ecp)
{
    if (ecp->error_active) {
        RDB_destroy_obj(&ecp->error, ecp);
    }
    RDB_destroy_hashmap(&ecp->pmap);
}

/**
<strong>RDB_raise_err</strong> initializes a RDB_object structure,
makes it the error associated with the RDB_exec_context given
by <var>ecp</var>, and returns a pointer to it.
A previously raised error is overwritten. 
After RDB_raise_err() has been called succssfully, the RDB_object is in the
same state as after a call to RDB_init_obj(). It is the caller's
responsibility to assign a value to the RDB_object,
for example, by calling a selector.

@returns

A pointer to the error, or NULL if the call fails.

@par Errors:

The call may fail and return NULL if an error was already
associated with the RDB_exec_context and destroying this error
failed.
*/
RDB_object *
RDB_raise_err(RDB_exec_context *ecp)
{
    if (ecp->error_active) {
        /* Replace existing error */
        if (RDB_destroy_obj(&ecp->error, ecp) != RDB_OK) {
            return NULL;
        }
    }
    RDB_init_obj(&ecp->error);
    ecp->error_active = RDB_TRUE;
    return &ecp->error;
}

/** Returns the error associated with the RDB_exec_context given by
<var>ecp</var>.

@returns

A pointer to the error associated with the RDB_exec_context, or NULL
if no error has been raised.
 */
RDB_object *
RDB_get_err(RDB_exec_context *ecp)
{
    return ecp->error_active ? &ecp->error : NULL;
}

/** RDB_clear_err clears the error associated with the RDB_exec_context given by
<var>ecp</var>, freing all resources associated with it.
Subsequent calls calls to RDB_get_err will return NULL.
 */
void
RDB_clear_err(RDB_exec_context *ecp)
{
    if (ecp->error_active) {
        RDB_destroy_obj(&ecp->error, ecp);
        ecp->error_active = RDB_FALSE;
    }
    ecp->error_retryable = RDB_FALSE;
}

/**
 * Returns RDB_TRUE if a retryable error has been raised, RDB_FALSE if not.
 */
RDB_bool
RDB_err_retryable(RDB_exec_context *ecp)
{
    return ecp->error_active && ecp->error_retryable ? RDB_TRUE : RDB_FALSE;
}

/** Convenience function to raise a system-provided error.
 */
RDB_object *
RDB_raise_no_memory(RDB_exec_context *ecp)
{
    /*
     * Since the error type is internally represented by an empty tuple,
     * we can simply call RDB_raise_err() and then set the type.
     */

    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_NO_MEMORY_ERROR;
    return errp;
}

RDB_object *
RDB_raise_no_running_tx(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_NO_RUNNING_TX_ERROR;
    return errp;
}

static RDB_object *
raise_msg_err(RDB_type *errtyp, const char *msg, RDB_exec_context *ecp)
{
    /*
     * The only possrep consists only of the MSG attribute of type string.
     * This means the internal representation is a string,
     * so we can call RDB_raise_err(), set the string value and the type.
     */
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    /* Set value */
    if (RDB_string_to_obj(errp, msg, ecp) != RDB_OK)
        return NULL;

    /* Set type */
    errp->typ = errtyp;

    return errp;
}

RDB_object *
RDB_raise_not_found(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_NOT_FOUND_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_invalid_argument(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_INVALID_ARGUMENT_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_type_mismatch(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_TYPE_MISMATCH_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_operator_not_found(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_OPERATOR_NOT_FOUND_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_type_not_found(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_TYPE_NOT_FOUND_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_type_constraint_violation(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_TYPE_CONSTRAINT_VIOLATION_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_element_exists(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_ELEMENT_EXISTS_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_key_violation(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_KEY_VIOLATION_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_not_supported(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_NOT_SUPPORTED_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_name(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_NAME_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_predicate_violation(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_PREDICATE_VIOLATION_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_in_use(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_IN_USE_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_system(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_SYSTEM_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_resource_not_found(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_RESOURCE_NOT_FOUND_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_internal(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_INTERNAL_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_concurrency(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_CONCURRENCY_ERROR;
    return errp;
}

RDB_object *
RDB_raise_aggregate_undefined(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_AGGREGATE_UNDEFINED_ERROR;
    return errp;
}

RDB_object *
RDB_raise_version_mismatch(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_VERSION_MISMATCH_ERROR;
    return errp;
}

RDB_object *
RDB_raise_deadlock(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_DEADLOCK_ERROR;
    return errp;
}

RDB_object *
RDB_raise_data_corrupted(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_DATA_CORRUPTED_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_run_recovery(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_RUN_RECOVERY_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_syntax(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_SYNTAX_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_connection(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_CONNECTION_ERROR, msg, ecp);
}

/** <strong>RDB_ec_set_property</strong> sets the property <var>name</var>
of the RDB_exec_context given by <var>ecp</var> to <var>val</var>.

@returns

RDB_OK on success, RDB_ERROR on failure.
 */
int
RDB_ec_set_property(RDB_exec_context *ecp, const char *name, void *val)
{
    int ret = RDB_hashmap_put(&ecp->pmap, name, val);
    if (ret != RDB_OK) {
        RDB_errno_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/** Return the value of the property <var>name</var>
of the RDB_exec_context given by <var>ecp</var>.
 * 
@returns

The property value.
 */
void *
RDB_ec_property(RDB_exec_context *ecp, const char *name)
{
    return RDB_hashmap_get(&ecp->pmap, name);
}

/**
 * Raises an error that corresponds to the POSIX error code <var>errcode</var>.
 */
void
RDB_errno_to_error(int errcode, RDB_exec_context *ecp)
{
    switch (errcode) {
        case ENOMEM:
            RDB_raise_no_memory(ecp);
            break;
        case EINVAL:
            RDB_raise_invalid_argument("", ecp);
            break;
        case ENOENT:
            RDB_raise_resource_not_found(strerror(errcode), ecp);
            break;
        default:
            RDB_raise_system(strerror(errcode), ecp);
    }
}

/*@}*/
