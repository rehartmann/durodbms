/*
 * Internal transaction
 *
 * Copyright (C) 2016, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "pgtx.h"
#include "pgenv.h"
#include <rec/envimpl.h>
#include <obj/excontext.h>

#include <libpq-fe.h>

unsigned next_savepoint_id = 0;

typedef struct {
    RDB_environment *envp;
    int savepoint_id;   /* -1 for top-level transaction */
} RDB_pg_tx;

static RDB_rec_transaction *
nested_begin_tx(RDB_environment *envp,
        RDB_rec_transaction *parent_rtxp, RDB_exec_context *ecp)
{
    RDB_object command;
    PGresult *res;
    char idbuf[12];
    RDB_pg_tx *tx = RDB_alloc(sizeof(RDB_pg_tx), ecp);
    if (tx == NULL)
        return NULL;
    tx->envp = envp;
    tx->savepoint_id = next_savepoint_id++;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "SAVEPOINT s", ecp) != RDB_OK)
        goto error;
    sprintf(idbuf, "%u", tx->savepoint_id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;

    res = PQexec(tx->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        RDB_pgresult_to_error(envp, res, ecp);
        PQclear(res);
        RDB_free(tx);
        return NULL;
    }

    PQclear(res);

    RDB_destroy_obj(&command, ecp);
    return (RDB_rec_transaction *) tx;

error:
    RDB_destroy_obj(&command, ecp);
    return NULL;
}

RDB_rec_transaction *
RDB_pg_begin_tx(RDB_environment *envp,
        RDB_rec_transaction *parent_rtxp, RDB_exec_context *ecp)
{
    PGresult *res;
    RDB_pg_tx *tx = RDB_alloc(sizeof(RDB_pg_tx), ecp);
    if (tx == NULL)
        return NULL;
    tx->envp = envp;
    tx->savepoint_id = -1;

    if (parent_rtxp != NULL) {
        return nested_begin_tx(envp, parent_rtxp, ecp);
    }

    res = PQexec(envp->env.pgconn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        RDB_pgresult_to_error(envp, res, ecp);
        PQclear(res);
        RDB_free(tx);
        return NULL;
    }

    PQclear(res);
    return (RDB_rec_transaction *)tx;
}

int
nested_commit(RDB_pg_tx *tx, RDB_exec_context *ecp)
{
    RDB_object command;
    PGresult *res;
    char idbuf[12];

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "RELEASE SAVEPOINT s", ecp) != RDB_OK)
        goto error;
    sprintf(idbuf, "%u", tx->savepoint_id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;

    res = PQexec(tx->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        RDB_pgresult_to_error(tx->envp, res, ecp);
        PQclear(res);
        goto error;
    }

    PQclear(res);
    RDB_destroy_obj(&command, ecp);
    RDB_free(tx);
    return RDB_OK;

error:
    RDB_destroy_obj(&command, ecp);
    RDB_free(tx);
    return RDB_ERROR;
}

int
RDB_pg_commit(RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_pg_tx *tx = (RDB_pg_tx *)rtxp;
    PGresult *res;

    if(tx->savepoint_id != -1) {
        return nested_commit(tx, ecp);
    }

    res = PQexec(tx->envp->env.pgconn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(tx->envp, res, ecp);
        PQclear(res);
        RDB_free(tx);
        return RDB_ERROR;
    }

    PQclear(res);
    RDB_free(tx);
    return RDB_OK;
}

int
nested_abort(RDB_pg_tx *tx, RDB_exec_context *ecp)
{
    RDB_object command;
    PGresult *res;
    char idbuf[12];

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "ROLLBACK TO SAVEPOINT s", ecp) != RDB_OK)
        goto error;
    sprintf(idbuf, "%u", tx->savepoint_id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;

    res = PQexec(tx->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        RDB_pgresult_to_error(tx->envp, res, ecp);
        PQclear(res);
        goto error;
    }

    PQclear(res);
    RDB_destroy_obj(&command, ecp);
    RDB_free(tx);
    return RDB_OK;

error:
    RDB_destroy_obj(&command, ecp);
    RDB_free(tx);
    return RDB_ERROR;
}

int
RDB_pg_abort(RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_pg_tx *tx = (RDB_pg_tx *)rtxp;
    PGresult *res;

    if(tx->savepoint_id != -1) {
        return nested_abort(tx, ecp);
    }

    res = PQexec(tx->envp->env.pgconn, "ROLLBACK");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(tx->envp, res, ecp);
        PQclear(res);
        RDB_free(tx);
        return RDB_ERROR;
    }

    PQclear(res);
    RDB_free(tx);
    return RDB_OK;
}

int
RDB_pg_tx_id(RDB_rec_transaction *rtxp) {
    RDB_pg_tx *tx = (RDB_pg_tx *)rtxp;
    return tx->savepoint_id;
}
