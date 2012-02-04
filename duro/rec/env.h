#ifndef RDB_ENV_H
#define RDB_ENV_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <db.h>

typedef void (RDB_errfn)(const char *msg, void *arg);

typedef struct RDB_environment {
    /* The Berkeley DB environment */
    DB_ENV *envp;

    /* Function which is invoked by RDB_close_env() */
    void (*closefn)(struct RDB_environment *);

    /*
     * Used to store additional data. The relational layer
     * uses this to store a pointer to the dbroot structure.
     */
    void *user_data;

    /* Trace level. 0 means no trace. */
    unsigned trace;
} RDB_environment;

#define RDB_internal_env(renvp) (renvp->envp)

#define RDB_env_private(renvp) (renvp->user_data)

#define RDB_env_trace(renvp) (renvp->trace)

int
RDB_open_env(const char *, RDB_environment **);

int
RDB_close_env(RDB_environment *);

void
RDB_set_env_closefn(RDB_environment *, void (*)(RDB_environment *));

DB_ENV *
RDB_bdb_env(RDB_environment *);

void
RDB_env_set_trace(RDB_environment *, unsigned);

#endif
