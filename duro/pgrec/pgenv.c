/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rec/envimpl.h>
#include <rec/recmap.h>
#include <gen/types.h>
#include <obj/excontext.h>
#include "pgenv.h"
#include "pgrecmap.h"
#include "pgtx.h"
#include "pgindex.h"

RDB_environment *
RDB_pg_open_env(const char *path, RDB_exec_context *ecp)
{
    RDB_environment *envp = malloc(sizeof (RDB_environment));
    if (envp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    envp->close_fn = &RDB_pg_close_env;
    envp->create_recmap_fn = &RDB_create_pg_recmap;
    envp->open_recmap_fn = &RDB_open_pg_recmap;
    envp->open_sequence_fn = NULL;
    envp->rename_sequence_fn = NULL;
    envp->begin_tx_fn = &RDB_pg_begin_tx;
    envp->commit_fn = &RDB_pg_commit;
    envp->abort_fn = &RDB_pg_abort;
    envp->tx_id_fn = &RDB_pg_tx_id;
    envp->create_index_fn = &RDB_create_pg_index;
    envp->close_index_fn = &RDB_close_pg_index;
    envp->delete_index_fn = &RDB_delete_pg_index;

    envp->cleanup_fn = NULL;
    envp->xdata = NULL;
    envp->trace = 0;
    envp->queries = RDB_TRUE;

    envp->env.pgconn = PQconnectdb(path);
    if (PQstatus(envp->env.pgconn) != CONNECTION_OK)
    {
        RDB_raise_system(PQerrorMessage(envp->env.pgconn), ecp);
        PQfinish(envp->env.pgconn);
        free(envp);
        return NULL;
    }
    return envp;
}

/**
 * RDB_close_env closes the database environment specified by
 * \a envp.
 * 
 * @param envp     the pointer to the environment.
 * 
 * @returns On success, RDB_OK is returned. On failure, an error code is returned.
 * 
 * @par Errors:
 * See the documentation of the Berkeley function DB_ENV->close for details.
 */
int
RDB_pg_close_env(RDB_environment *envp, RDB_exec_context *ecp)
{
    PQfinish(envp->env.pgconn);
    free(envp);
    return RDB_OK;
}
