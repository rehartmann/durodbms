#ifndef RDB_ENV_H
#define RDB_ENV_H

/* $Id$ */

#include <db.h>
#include <stdio.h>

typedef struct RDB_environment {
    DB_ENV *envp;
    void (*closefn)(struct RDB_environment *);
    FILE *errfilep;
} RDB_environment;

#define RDB_internal_env(renvp) (renvp->envp)

#define RDB_env_private(renvp) (renvp->envp->app_private)

/*
 * Create a database environment.
 *
 * Arguments:
 * path		pathname of the direcory where the data is stored.
 * options      currently ignored.
 * dspp		location where the pointer to the environment is stored.
 */
int
RDB_create_env(const char *path, int options, RDB_environment **);

/*
 * Open a database environment.
 *
 * Arguments:
 * path		pathname of the direcory where the data is stored.
 * dspp		location where the pointer to the environment is stored.
 */
int
RDB_open_env(const char *path, RDB_environment **envp);

/*
 * close a database environment.
 *
 * Arguments:
 * dsp		the pointer to the environment.
 */
int
RDB_close_env(RDB_environment *dsp);

void
RDB_set_env_closefn(RDB_environment *, void (*)(struct RDB_environment *));

void
RDB_set_errfile(RDB_environment *, FILE *errfile);

void
RDB_errmsg(RDB_environment *envp, const char *msg);

#define ERRMSG(envp,msg) RDB_errmsg(envp, msg)

#endif
