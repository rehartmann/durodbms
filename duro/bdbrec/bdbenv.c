/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "bdbenv.h"
#include <rec/envimpl.h>
#include <rec/recmap.h>
#include <bdbrec/bdbrecmap.h>
#include <gen/types.h>
#include <obj/excontext.h>
#include <bdbrec/bdbsequence.h>
#include <bdbrec/bdbtx.h>

#include <stdlib.h>
#include <errno.h>

static void
bdb_set_errfile(RDB_environment *envp, FILE *file)
{
    envp->env.envp->set_errfile(envp->env.envp, file);
}

static FILE *
bdb_get_errfile(const RDB_environment *envp)
{
    FILE *file;
    envp->env.envp->get_errfile(envp->env.envp, &file);
    return file;
}

static int
open_env(const char *path, RDB_environment **envpp, int bdb_flags)
{
    RDB_environment *envp;
    int ret;

    envp = malloc(sizeof (RDB_environment));
    if (envp == NULL)
        return ENOMEM;

    envp->close_fn = &RDB_bdb_close_env;
    envp->create_recmap_fn = &RDB_create_bdb_recmap;
    envp->open_recmap_fn = &RDB_open_bdb_recmap;
    envp->open_sequence_fn = &RDB_open_bdb_sequence;
    envp->rename_sequence_fn = &RDB_rename_bdb_sequence;
    envp->begin_tx_fn = &RDB_bdb_begin_tx;
    envp->commit_fn = &RDB_bdb_commit;
    envp->abort_fn = &RDB_bdb_abort;
    envp->tx_id_fn = &RDB_bdb_tx_id;
    envp->set_errfile_fn = &bdb_set_errfile;
    envp->get_errfile_fn = &bdb_get_errfile;

    envp->cleanup_fn = NULL;
    envp->xdata = NULL;
    envp->trace = 0;
    envp->queries = RDB_FALSE;

    /* create environment handle */
    *envpp = envp;
    ret = db_env_create(&envp->env.envp, 0);
    if (ret != 0) {
        free(envp);
        return ret;
    }

    /*
     * Configure alloc, realloc, and free explicity
     * because on Windows Berkeley DB may use a different heap
     */
    ret = envp->env.envp->set_alloc(envp->env.envp, malloc, realloc, free);
    if (ret != 0) {
        envp->env.envp->close(envp->env.envp, 0);
        free(envp);
        return ret;
    }

    /*
     * Suppress error output by default
     */
    if (envp->trace == 0)
        envp->env.envp->set_errfile(envp->env.envp, NULL);

    /* Open DB environment */
    ret = envp->env.envp->open(envp->env.envp, path, bdb_flags, 0);
    if (ret != 0) {
        envp->env.envp->close(envp->env.envp, 0);
        free(envp);
        return ret;
    }

    /*
     * When acquiring locks, distinguish between timeout and deadlock
     */
    ret = envp->env.envp->set_flags(envp->env.envp, DB_TIME_NOTGRANTED, 1);
    if (ret != 0) {
        envp->env.envp->close(envp->env.envp, 0);
        free(envp);
        return ret;
    }
    
    return RDB_OK;
}

int
RDB_bdb_open_env(const char *path, RDB_environment **envpp, int flags)
{
    return open_env(path, envpp, DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN
            | (flags & RDB_RECOVER ? DB_CREATE | DB_RECOVER : DB_CREATE));
}

/**
 * Creates a database environment identified by the system resource \a path.
 *
 * @param path  pathname of the direcory where the data is stored.
 * @param envpp   location where the pointer to the environment is stored.
 *
 * @return On success, RDB_OK is returned. On failure, an error code is returned.
 *
 * @par Errors:
 * See the documentation of the Berkeley DB function DB_ENV->open for details.
 */
int
RDB_bdb_create_env(const char *path, RDB_environment **envpp)
{
    return open_env(path, envpp,
            DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_CREATE);
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
RDB_bdb_close_env(RDB_environment *envp, RDB_exec_context *ecp)
{
    int ret;

    ret = envp->env.envp->close(envp->env.envp, 0);
    free(envp);
    if (ret != RDB_OK) {
        RDB_bdb_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return ret;
}

DB_ENV *
RDB_bdb_env(RDB_environment *envp)
{
    return envp->env.envp;
}

/**
 * Raises an error that corresponds to the error code <var>errcode</var>.
 * <var>errcode</var> can be a POSIX error code,
 * a Berkeley DB error code or an error code from the record layer.
 *
 * If txp is not NULL and the error is a deadlock, abort the transaction
 */
void
RDB_bdb_errcode_to_error(int errcode, RDB_exec_context *ecp)
{
    switch (errcode) {
        case ENOMEM:
            RDB_raise_no_memory(ecp);
            break;
        case EINVAL:
            RDB_raise_invalid_argument("", ecp);
            break;
        case ENOENT:
            RDB_raise_resource_not_found(db_strerror(errcode), ecp);
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
            RDB_raise_concurrency(ecp);
            break;
        case DB_LOCK_DEADLOCK:
            RDB_raise_deadlock(ecp);
            break;
        case DB_RUNRECOVERY:
            RDB_raise_run_recovery("run Berkeley DB database recovery", ecp);
            break;
        case DB_SECONDARY_BAD:
            RDB_raise_data_corrupted("secondary index corrupted", ecp);
            break;
        case RDB_RECORD_CORRUPTED:
            RDB_raise_data_corrupted("record corrupted", ecp);
            break;
        default:
            RDB_raise_system(db_strerror(errcode), ecp);
    }
}
