/*
 * Copyright (C) 2003, 2012, 2015, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "envimpl.h"
#include <obj/excontext.h>

#ifdef BERKELEYDB
#include <bdbrec/bdbenv.h>
#endif

#ifdef POSTGRESQL
#include <pgrec/pgenv.h>
#endif

#ifdef FOUNDATIONDB
#include <fdbrec/fdbenv.h>
#endif

#include <string.h>
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
 * If DuroDBMS has been built with PostgreSQL support, \a path can be
 * a PostgreSQL URI.
 * If DuroDBMS has been built with FoundationSQL support, \a path can be
 * a URI of the form foundationdb://<file> where file is a cluster file path.
 *
 * @param path  pathname of the direcory where the data is stored.
 * @param flags can be zero or RDB_RECOVER. If it is RDB_RECOVER,
 *        all necessary files will be created.
 *
 * @return On success, a pointer to the environment is returned.
 * On failure, NULL is returned and an error value is stored in *ecp.
 *
 */
RDB_environment *
RDB_open_env(const char *path, int flags, RDB_exec_context *ecp)
{
    RDB_environment *envp;
#ifdef BERKELEYDB
    int ret;
#endif

#ifdef POSTGRESQL
    if (strstr(path, "postgresql://") == path) {
        return RDB_pg_open_env(path, ecp);
    }
#endif
#ifdef FOUNDATIONDB
	if (strstr(path, "foundationdb://") == path) {
		return RDB_fdb_open_env(path + 15, ecp);
	}
#endif
#ifdef BERKELEYDB
    ret = RDB_bdb_open_env(path, &envp, flags);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return NULL;
    }
#else
    RDB_raise_not_supported("environment type not supported", ecp);
    envp = NULL;
#endif
    return envp;
}

/**
 * Creates a database environment identified by the system resource \a path.
 *
 * @param path  pathname of the direcory where the data is stored.
 * @param envpp   location where the pointer to the environment is stored.
 *
 * @return On success, a pointer to the environment is returned.
 * On failure, NULL is returned and an error value is stored in *ecp.
 *
 */
RDB_environment *
RDB_create_env(const char *path, RDB_exec_context *ecp)
{
#ifdef BERKELEYDB
    RDB_environment *envp;
    int ret = RDB_bdb_create_env(path, &envp);
    if (ret != RDB_OK) {
        RDB_bdb_errcode_to_error(ret, ecp);
        return NULL;
    }
    return envp;
#else
    RDB_raise_not_supported("environment type not supported", ecp);
    return NULL;
#endif
}

/**
 * RDB_close_env closes the database environment specified by
 * \a envp.
 * 
 * @param envp     the pointer to the environment.
 * 
 * @returns RDB_OK on success, RDB_ERROR if an error occurred.
 * 
 * @par Errors:
 * See the documentation of the Berkeley function DB_ENV->close for details.
 */
int
RDB_close_env(RDB_environment *envp, RDB_exec_context *ecp)
{
    if (envp->cleanup_fn != NULL)
        (*envp->cleanup_fn)(envp);
    return (*envp->close_fn)(envp, ecp);
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

/**
 * Set error output stream.
 */
void
RDB_env_set_errfile(RDB_environment *envp, FILE *file)
{
    (*envp->set_errfile_fn)(envp, file);
}

/**
 * Get error output stream.
 */
FILE *
RDB_env_get_errfile(const RDB_environment *envp)
{
    return (*envp->get_errfile_fn)(envp);
}

/*@}*/

void
RDB_set_env_closefn(RDB_environment *envp, void (*fn)(struct RDB_environment *))
{
    envp->cleanup_fn = fn;
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
 * Raises an error that corresponds to the error code <var>errcode</var>.
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
            RDB_raise_resource_not_found(strerror(errcode), ecp);
            break;
        default:
#ifdef BERKELEYDB
            RDB_bdb_errcode_to_error(errcode, ecp);
#else
            RDB_raise_system(strerror(errcode), ecp);
#endif
    }
}

RDB_bool
RDB_env_queries(const RDB_environment *envp) {
    return envp->queries;
}
