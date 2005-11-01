#include "rdb.h"
#include <gen/hashmap.h>

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
        /* Don't overwrite existing error */
        return NULL;
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
    RDB_destroy_obj(&ecp->error, ecp);
    ecp->error_active = RDB_FALSE;
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

RDB_object *
RDB_raise_not_found(const char *info, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    /* Set value */
    ret = RDB_string_to_obj(errp, info, ecp);
    if (ret != RDB_OK)
        return NULL;

    /* Set type */
    errp->typ = &RDB_NOT_FOUND_ERROR;
    
    return errp;
}

RDB_object *
RDB_raise_invalid_argument(const char *info, RDB_exec_context *ecp)
{
    int ret;
    RDB_object infoobj;
    RDB_object *argp = &infoobj;
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    RDB_init_obj(&infoobj);
    ret = RDB_string_to_obj(&infoobj, info, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&infoobj, ecp);
        return NULL;
    }

    /* Call selector */
    ret = RDB_call_ro_op("INVALID_ARGUMENT_ERROR", 1, &argp, ecp, NULL, errp);
    RDB_destroy_obj(&infoobj, ecp);
    return ret == RDB_OK ? errp : NULL;
}

RDB_object *
RDB_raise_type_mismatch(const char *info, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    /* Set value */
    ret = RDB_string_to_obj(errp, info, ecp);
    if (ret != RDB_OK)
        return NULL;

    /* Set type */
    errp->typ = &RDB_TYPE_MISMATCH_ERROR;
    
    return errp;
}

RDB_object *
RDB_raise_operator_not_found(const char *info, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    /* Set value */
    ret = RDB_string_to_obj(errp, info, ecp);
    if (ret != RDB_OK)
        return NULL;

    /* Set type */
    errp->typ = &RDB_OPERATOR_NOT_FOUND_ERROR;
    
    return errp;
}

RDB_object *
RDB_raise_type_constraint_violation(const char *info, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *errp = RDB_raise_err(ecp);
    if (errp == NULL)
        return NULL;

    /* Set value */
    ret = RDB_string_to_obj(errp, info, ecp);
    if (ret != RDB_OK)
        return NULL;

    /* Set type */
    errp->typ = &RDB_TYPE_CONSTRAINT_VIOLATION_ERROR;
    
    return errp;
}
