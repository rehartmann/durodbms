/*
 * $Id$
 *
 * Copyright (C) 2003-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "env.h"
#include <gen/types.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>

int
RDB_open_env(const char *path, RDB_environment **envpp)
{
    RDB_environment *envp;
    int ret;

    envp = malloc(sizeof (RDB_environment));
    if (envp == NULL)
       return ENOMEM;

    envp->closefn = NULL;
    envp->user_data = NULL;

    /* create environment handle */
    *envpp = envp;
    ret = db_env_create(&envp->envp, 0);
    if (ret != 0) {
        free(envp);
        return ret;
    }

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

    /* open DB environment */
    ret = envp->envp->open(envp->envp, path,
            DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_CREATE,
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
