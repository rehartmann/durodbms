#include "rdb.h"
#include "internal.h"
#include <gen/hashmap.h>
#include <errno.h>

void
RDB_init_exec_context(RDB_exec_context *ecp)
{
    ecp->error_active = RDB_FALSE;
    RDB_init_hashmap(&ecp->pmap, 16);
}

void
RDB_destroy_exec_context(RDB_exec_context *ecp)
{
    if (ecp->error_active) {
        RDB_destroy_obj(&ecp->error, ecp);
    }
    RDB_destroy_hashmap(&ecp->pmap);
}

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

RDB_object *
RDB_get_err(RDB_exec_context *ecp)
{
    return ecp->error_active ? &ecp->error : NULL;
}

void
RDB_clear_err(RDB_exec_context *ecp)
{
    if (ecp->error_active) {
        RDB_destroy_obj(&ecp->error, ecp);
        ecp->error_active = RDB_FALSE;
    }
}

RDB_object *
RDB_raise_no_memory(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_NO_MEMORY_ERROR;
    return errp;
}

RDB_object *
RDB_raise_invalid_tx(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_INVALID_TRANSACTION_ERROR;
    return errp;
}

static RDB_object *
raise_msg_err(RDB_type *errtyp, const char *msg, RDB_exec_context *ecp)
{
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
RDB_raise_attribute_not_found(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_ATTRIBUTE_NOT_FOUND_ERROR, msg, ecp);
}

RDB_object *
RDB_raise_predicate_violation(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_PREDICATE_VIOLATION_ERROR, msg, ecp);
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
RDB_raise_lock_not_granted(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_LOCK_NOT_GRANTED_ERROR;
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
RDB_raise_fatal(RDB_exec_context *ecp)
{
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    errp->typ = &RDB_FATAL_ERROR;
    return errp;
}

RDB_object *
RDB_raise_syntax(const char *msg, RDB_exec_context *ecp)
{
    return raise_msg_err(&RDB_SYNTAX_ERROR, msg, ecp);
}

void
_RDB_handle_errcode(int errcode, RDB_exec_context *ecp, RDB_transaction *txp)
{
    switch (errcode) {
        case ENOMEM:
            RDB_raise_no_memory(ecp);
            break;
        case EINVAL:
            RDB_raise_invalid_argument("", ecp);
            break;
        case DB_KEYEXIST:
            RDB_raise_key_violation("", ecp);
            break;
        case RDB_ELEMENT_EXISTS:
            RDB_raise_element_exists("", ecp);
            break;
        case DB_NOTFOUND:
            RDB_raise_not_found("", ecp);
            break;
        case DB_LOCK_NOTGRANTED:
            RDB_raise_lock_not_granted(ecp);
            break;
        case DB_LOCK_DEADLOCK:
            RDB_rollback_all(ecp, txp);
            RDB_raise_deadlock(ecp);
            break;
        case DB_RUNRECOVERY:
            RDB_raise_fatal(ecp);
            break;
        default:
            RDB_raise_system(db_strerror(errcode), ecp);
    }
}

int
RDB_ec_set_property(RDB_exec_context *ecp, const char *name, void *p)
{
    int ret = RDB_hashmap_put(&ecp->pmap, name, p);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, NULL);
        return RDB_ERROR;
    }
    return RDB_OK;
}

void *
RDB_ec_get_property(RDB_exec_context *ecp, const char *name)
{
    return RDB_hashmap_get(&ecp->pmap, name);
}
