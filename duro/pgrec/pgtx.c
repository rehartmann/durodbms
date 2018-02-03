/*
 * Internal transaction
 *
 * Copyright (C) 2016, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "pgtx.h"
#include <rec/envimpl.h>
#include <obj/excontext.h>

#include <libpq-fe.h>

typedef struct {
    RDB_environment *envp;
    int savepoint_id;   /* -1 for top-level transaction */
} RDB_pg_tx;

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
        puts("Ignoring nested begin");
        tx->savepoint_id = 0;
        return (RDB_rec_transaction *)tx;
    }

    res = PQexec(envp->env.pgconn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(envp->env.pgconn), ecp);
        PQclear(res);
        RDB_free(tx);
        return NULL;
    }

    PQclear(res);
    return (RDB_rec_transaction *)tx;
}

int
RDB_pg_commit(RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_pg_tx *tx = (RDB_pg_tx *)rtxp;
    PGresult *res;

    if(tx->savepoint_id != -1) {
        puts("Ignoring nested commit");
        RDB_free(tx);
        return RDB_OK;
    }

    res = PQexec(tx->envp->env.pgconn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(tx->envp->env.pgconn), ecp);
        PQclear(res);
        RDB_free(tx);
        return RDB_ERROR;
    }

    puts("committed");
    PQclear(res);
    RDB_free(tx);
    return RDB_OK;
}

int
RDB_pg_abort(RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_pg_tx *tx = (RDB_pg_tx *)rtxp;
    PGresult *res;

    if(tx->savepoint_id != -1) {
        puts("Ignoring nested commit");
        RDB_free(tx);
        return RDB_OK;
    }

    res = PQexec(tx->envp->env.pgconn, "ROLLBACK");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(tx->envp->env.pgconn), ecp);
        PQclear(res);
        RDB_free(tx);
        return RDB_ERROR;
    }

    puts("aborted");
    PQclear(res);
    RDB_free(tx);
    return RDB_OK;
}

int
RDB_pg_tx_id(RDB_rec_transaction *rtxp) {
    RDB_pg_tx *tx = (RDB_pg_tx *)rtxp;
    return tx->savepoint_id;
}
