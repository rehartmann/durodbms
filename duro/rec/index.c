/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "indeximpl.h"
#include "envimpl.h"
#include "recmapimpl.h"
#include <bdbrec/bdbindex.h>
#include <obj/excontext.h>

RDB_index *
RDB_create_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const RDB_field_descriptor fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*rmp->create_index_fn)(rmp, namp, filenamp, envp, fieldc, fieldv,
            cmpv, flags, rtxp, ecp);
}

RDB_index *
RDB_open_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    if (RDB_env_queries(envp)) {
        RDB_raise_not_supported("RDB_open_index", ecp);
        return NULL;
    }
    return RDB_open_bdb_index(rmp, namp, filenamp, envp, fieldc, fieldv, cmpv, flags,
            rtxp, ecp);
}

int
RDB_close_index(RDB_index *ixp, RDB_exec_context *ecp)
{
    if (ixp->rmp->envp == NULL) {
        return RDB_close_bdb_index(ixp, ecp);
    }
    return (*ixp->close_index_fn)(ixp, ecp);
}

RDB_bool
RDB_index_is_ordered(RDB_index *ixp)
{
    if (ixp->rmp->envp != NULL && RDB_env_queries(ixp->rmp->envp)) {
        return RDB_FALSE;
    }
    return RDB_bdb_index_is_ordered(ixp);
}

/* Delete an index. */
int
RDB_delete_index(RDB_index *ixp, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*ixp->delete_index_fn)(ixp, envp, rtxp, ecp);
}

int
RDB_index_get_fields(RDB_index *ixp, RDB_field keyv[], int fieldc, RDB_rec_transaction *rtxp,
           RDB_field retfieldv[], RDB_exec_context *ecp)
{
    if (ixp->rmp->envp != NULL && RDB_env_queries(ixp->rmp->envp)) {
        RDB_raise_not_supported("RDB_index_get_fields", ecp);
        return RDB_ERROR;
    }
    return RDB_bdb_index_get_fields(ixp, keyv, fieldc, rtxp, retfieldv, ecp);
}

int
RDB_index_delete_rec(RDB_index *ixp, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    if (ixp->rmp->envp != NULL && RDB_env_queries(ixp->rmp->envp)) {
        RDB_raise_not_supported("RDB_index_delete_rec", ecp);
        return RDB_ERROR;
    }
    return RDB_bdb_index_delete_rec(ixp, keyv, rtxp, ecp);
}
