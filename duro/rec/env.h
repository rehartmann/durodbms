#ifndef RDB_ENV_H
#define RDB_ENV_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <db.h>

typedef void (RDB_errfn)(const char *msg, void *arg);

typedef struct RDB_environment {
    DB_ENV *envp;
    void (*closefn)(struct RDB_environment *);
    void *user_data;
} RDB_environment;

#define RDB_internal_env(renvp) (renvp->envp)

#define RDB_env_private(renvp) (renvp->user_data)

int
RDB_open_env(const char *path, RDB_environment **envpp);

int
RDB_close_env(RDB_environment *envp);

void
RDB_set_env_closefn(RDB_environment *, void (*)(RDB_environment *));

DB_ENV *
RDB_bdb_env(RDB_environment *);

#endif
