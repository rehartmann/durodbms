/*
 * Copyright (C) 2003, 2012, 2015, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "envimpl.h"
#include <bdbrec/bdbenv.h>

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
 * @param flags can be zero or RDB_RECOVER. If it is RDB_RECOVER,
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
    return RDB_bdb_open_env(path, envpp, flags);
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
RDB_create_env(const char *path, RDB_environment **envpp)
{
    return RDB_bdb_create_env(path, envpp);
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
    if (envp->cleanup_fn != NULL)
        (*envp->cleanup_fn)(envp);
    return (*envp->close_fn)(envp);
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
    return RDB_bdb_errcode_to_error(errcode, ecp);
}
