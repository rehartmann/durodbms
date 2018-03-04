/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "pgindex.h"
#include "pgenv.h"
#include <rec/envimpl.h>
#include <rec/recmapimpl.h>
#include <rec/indeximpl.h>
#include <rec/dbdefs.h>
#include <obj/excontext.h>
#include <gen/strfns.h>

#include <libpq-fe.h>

static RDB_index *
new_min_index(const char *name, RDB_recmap *rmp, RDB_exec_context *ecp)
{
    RDB_index *ixp = RDB_alloc(sizeof(RDB_index), ecp);
    if (ixp == NULL) {
        return NULL;
    }
    ixp->rmp = rmp;
    ixp->namp = RDB_dup_str(name);
    if (ixp->namp == NULL) {
        RDB_raise_no_memory(ecp);
        free(ixp);
        return NULL;
    }
    return ixp;
}

RDB_index *
RDB_create_pg_index(RDB_recmap *rmp, const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const RDB_field_descriptor fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_object command;
    PGresult *res;
    RDB_index *ixp = new_min_index(name, rmp, ecp);
    if (rmp == NULL)
        return NULL;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command,
            RDB_UNIQUE & flags ? "CREATE UNIQUE INDEX "
                               : "CREATE INDEX ",
            ecp) != RDB_OK) {
       goto error;
    }
    if (RDB_append_string(&command, name, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, " ON \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, "\"(", ecp) != RDB_OK)
        goto error;
    for (int i = 0; i < fieldc; i++) {
        if (RDB_append_char(&command, '"', ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, fieldv[i].attrname, ecp) != RDB_OK)
            goto error;
        if (RDB_append_char(&command, '"', ecp) != RDB_OK)
            goto error;
        if (i < fieldc - 1) {
            if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                goto error;
        }
    }
    if (RDB_append_char(&command, ')', ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexec(envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(envp, res, ecp);
        PQclear(res);
        goto error;
    }
    RDB_destroy_obj(&command, ecp);
    PQclear(res);
    return ixp;

error:
    RDB_free(ixp->namp);
    RDB_free(ixp);
    RDB_destroy_obj(&command, ecp);
    return NULL;
}

int
RDB_close_pg_index(RDB_index *ixp, RDB_exec_context *ecp)
{
    RDB_free(ixp->namp);
    RDB_free(ixp);
    return RDB_OK;
}

/* Delete an index. */
int
RDB_delete_pg_index(RDB_index *ixp, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_object command;
    PGresult *res;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "DROP INDEX ", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, ixp->namp, ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexec(envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(envp, res, ecp);
        PQclear(res);
        goto error;
    }
    PQclear(res);
    RDB_destroy_obj(&command, ecp);
    RDB_free(ixp->namp);
    RDB_free(ixp);
    return RDB_OK;

error:
    RDB_destroy_obj(&command, ecp);
    return RDB_ERROR;
}
