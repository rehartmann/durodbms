
/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rec/envimpl.h>
#include <rec/recmap.h>
#include <gen/types.h>
#include <obj/excontext.h>
#include <obj/builtintypes.h>
#include "pgenv.h"
#include "pgrecmap.h"
#include "pgtx.h"
#include "pgindex.h"

#include <string.h>

static void
write_notice(void *arg, const char *message)
{
    fputs(message, (FILE *)arg);
    fputs("\n", (FILE *)arg);
}

static void
pg_set_errfile(RDB_environment *envp, FILE *file)
{
    envp->errfile = file;
    PQsetNoticeProcessor(envp->env.pgconn,
        &write_notice, file);
}

static FILE *
pg_get_errfile(const RDB_environment *envp)
{
    return envp->errfile;
}

RDB_environment *
RDB_pg_open_env(const char *path, RDB_exec_context *ecp)
{
    RDB_environment *envp = calloc(1, sizeof (RDB_environment));
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
    envp->set_errfile_fn = &pg_set_errfile;
    envp->get_errfile_fn = &pg_get_errfile;

    envp->cleanup_fn = NULL;
    envp->xdata = NULL;
    envp->trace = 0;
    envp->queries = RDB_TRUE;

    envp->env.pgconn = PQconnectdb(path);
    if (PQstatus(envp->env.pgconn) != CONNECTION_OK)
    {
        RDB_raise_connection(PQerrorMessage(envp->env.pgconn), ecp);
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

void
RDB_pgresult_to_error(const RDB_environment *envp, const PGresult *res,
        RDB_exec_context *ecp)
{
    char *errmsg = PQerrorMessage(envp->env.pgconn);
    char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);

    if (strcmp(sqlstate, "02000") == 0) {
        RDB_raise_not_found(errmsg, ecp);
    } else if (strncmp(sqlstate, "08", 2) == 0) {
        RDB_raise_connection(errmsg, ecp);
    } else if (strncmp(sqlstate, "0A", 2) == 0) {
        RDB_raise_not_supported(errmsg, ecp);
    } else if (strncmp(sqlstate, "22", 2) == 0
            || strncmp(sqlstate, "42", 2) == 0) {
        RDB_raise_invalid_argument(errmsg, ecp);
    } else if (strcmp(sqlstate, "23505") == 0) {
        RDB_raise_key_violation(errmsg, ecp);
    } else if (strcmp(sqlstate, "25P01") == 0) {
        RDB_raise_no_running_tx(ecp);
    } else if (strncmp(sqlstate, "23", 2) == 0) {
        RDB_raise_predicate_violation(errmsg, ecp);
    } else if (strcmp(sqlstate, "42804") == 0) {
        RDB_raise_type_mismatch(errmsg, ecp);
    } else if (strcmp(sqlstate, "53100") == 0
            || strcmp(sqlstate, "53200") == 0) {
        RDB_raise_no_memory(ecp);
    } else if (strcmp(sqlstate, "40P01") == 0) {
        RDB_raise_deadlock(ecp);
    } else if (strcmp(sqlstate, "55006") == 0) {
        RDB_raise_in_use(errmsg, ecp);
    } else if (strcmp(sqlstate, "55P03") == 0) {
        RDB_raise_lock_not_granted(ecp);
    } else if (strncmp(sqlstate, "57", 2) == 0
            || strncmp(sqlstate, "58", 2) == 0) {
        RDB_raise_system(errmsg, ecp);
    } else if (strcmp(sqlstate, "XX001") == 0) {
        RDB_raise_data_corrupted(errmsg, ecp);
    } else {
        RDB_raise_internal(errmsg, ecp);
    }
}

RDB_int
RDB_update_pg_sql(RDB_environment *envp, const char *command,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    PGresult *res;
    RDB_int ret;

    if (RDB_env_trace(envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", command);
    }
    res = PQexec(envp->env.pgconn, command);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(envp, res, ecp);
        ret = (RDB_int) RDB_ERROR;
    } else {
        ret = (RDB_int) atoi(PQcmdTuples(res));
    }
    PQclear(res);
    return ret;
}

int
RDB_pg_string_literal(RDB_environment *envp, RDB_object *dstp, const char *src,
        RDB_exec_context *ecp)
{
    int ret;
    char *estr = PQescapeLiteral(envp->env.pgconn, src, strlen(src));
    if (estr == NULL) {
        RDB_raise_system(PQerrorMessage(envp->env.pgconn), ecp);
        return RDB_ERROR;
    }
    ret = RDB_string_to_obj(dstp, estr, ecp);
    PQfreemem(estr);
    return ret;
}

int
RDB_pg_binary_literal(RDB_environment *envp, RDB_object *dstp, const void *src,
        size_t srclen, RDB_exec_context *ecp)
{
    size_t tolen;
    unsigned char *ebin = PQescapeByteaConn(envp->env.pgconn, src, srclen, &tolen);
    if (ebin == NULL) {
        RDB_raise_system(PQerrorMessage(envp->env.pgconn), ecp);
        return RDB_ERROR;
    }
    if (RDB_string_to_obj(dstp, "E'\\", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(dstp, (char *) ebin, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(dstp, "'::bytea", ecp) != RDB_OK)
        goto error;

    PQfreemem(ebin);
    return RDB_OK;

error:
    PQfreemem(ebin);
    return RDB_ERROR;
}
