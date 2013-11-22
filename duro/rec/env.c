/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "env.h"
#include "recmap.h"
#include <gen/types.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>

/** @defgroup env Database environment functions 
 * @{
 * \#include <rec/env.h>
 */

/**
 * RDB_open_env opens a database environment identified by the
 * system resource \a path.
 *
 * In the current implementation, the path passed to RDB_open_env
 * is a Berkeley DB database environment directory.
 * To create a new empty environment, simply pass an empty directory
 * to RDB_open_env.
 *
 * @param path  pathname of the direcory where the data is stored.
 * @param envpp   location where the pointer to the environment is stored.
 * @param flags can be zero or RDB_CREATE. If it is RDB_CREATE,
 *        all necessary files will be created.
 * 
 * @return On success, RDB_OK is returned. On failure, an error code is returned.
 * 
 * @par Errors:
 * See the documentation of the Berkeley DB function DB_ENV->open for details.
 */
int
RDB_open_env(const char *path, RDB_environment **envpp, int flags)
{
    RDB_environment *envp;
    int ret;

    envp = malloc(sizeof (RDB_environment));
    if (envp == NULL)
       return ENOMEM;

    envp->closefn = NULL;
    envp->xdata = NULL;
    envp->trace = 0;

    /* create environment handle */
    *envpp = envp;
    ret = db_env_create(&envp->envp, 0);
    if (ret != 0) {
        free(envp);
        return ret;
    }
    envp->seq_dbp = NULL;

    /*
     * Configure alloc, realloc, and free explicity
     * because on Windows Berkeley DB may use a different heap
     */
    ret = envp->envp->set_alloc(envp->envp, malloc, realloc, free);
    if (ret != 0) {
        envp->envp->close(envp->envp, 0);
        free(envp);
        return ret;
    }

    /*
     * Suppress error output by default
     */
    envp->envp->set_errfile(envp->envp, NULL);

    /* Open DB environment */
    ret = envp->envp->open(envp->envp, path,
            DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN
                    | (flags & RDB_CREATE ? DB_CREATE | DB_RECOVER : 0),
            0);
    if (ret != 0) {
        envp->envp->close(envp->envp, 0);
        free(envp);
        return ret;
    }

    ret = envp->envp->set_flags(envp->envp, DB_TIME_NOTGRANTED, 1);
    if (ret != 0) {
        envp->envp->close(envp->envp, 0);
        free(envp);
        return ret;
    }
    
    return RDB_OK;
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
RDB_close_env(RDB_environment *envp)
{
    int ret;

    if (envp->closefn != NULL)
        (*envp->closefn)(envp);
    ret = envp->envp->close(envp->envp, 0);
    free(envp);
    return ret;
}

/**
 * Set trace level. Zero means no trace messages.
 *
 * @param envp     the pointer to the environment.
 * @param level    the new trace level.
 *
 */
void
RDB_env_set_trace(RDB_environment *envp, unsigned level)
{
    envp->trace = level;
}

/*@}*/

void
RDB_set_env_closefn(RDB_environment *envp, void (*fn)(struct RDB_environment *))
{
    envp->closefn = fn;
}

DB_ENV *
RDB_bdb_env(RDB_environment *envp)
{
    return envp->envp;
}

void *
RDB_env_xdata(RDB_environment *envp)
{
    return envp->xdata;
}

void
RDB_env_set_xdata(RDB_environment *envp, void *xd)
{
    envp->xdata = xd;
}

unsigned
RDB_env_trace(RDB_environment *envp)
{
    return envp->trace;
}

/**
 * Raise an error that corresponds to the error code <var>errcode</var>.
 * <var>errcode</var> can be a POSIX error code,
 * a Berkeley DB error code or an error code from the record layer.
 *
 * If txp is not NULL and the error is a deadlock, abort the transaction
 */
void
RDB_errcode_to_error(int errcode, RDB_exec_context *ecp)
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
            RDB_raise_lock_not_granted(ecp);
            break;
        case DB_LOCK_DEADLOCK:
            RDB_raise_deadlock(ecp);
            break;
        case DB_RUNRECOVERY:
            RDB_raise_fatal(ecp);
            break;
        case RDB_RECORD_CORRUPTED:
            RDB_raise_fatal(ecp);
            break;
        default:
            RDB_raise_system(db_strerror(errcode), ecp);
    }
}

int
RDB_close_seq_container(RDB_environment *envp)
{
    int ret;
    if (envp->seq_dbp != NULL) {
        ret = envp->seq_dbp->close(envp->seq_dbp, 0);
        envp->seq_dbp = NULL;
        return ret;
    }
    return 0;
}
