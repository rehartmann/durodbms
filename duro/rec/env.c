/* $Id$ */

#include "env.h"
#include <gen/errors.h>
#include <stdlib.h>

int
RDB_create_env(const char *path, int options, RDB_environment **envpp)
{
    RDB_environment *envp;
    int err;
    
    envp = malloc(sizeof (RDB_environment));
    if (envp == NULL)
       return RDB_NO_MEMORY;

    envp->closefn = NULL;

    /* create environment handle */
    *envpp = envp;
    err = db_env_create(&envp->envp, 0);
    if (err != 0) {
        free(envp);
        return err;
    }
    
    /* create environment */
    err = envp->envp->open(envp->envp, path,
            DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_CREATE,
            0);
    if (err != 0) {
        envp->envp->close(envp->envp, 0);
        free(envp);
        return err;
    }
    
    return RDB_OK;
}

int
RDB_open_env(const char *path, RDB_environment **envpp)
{
    RDB_environment *envp;
    int err;

    envp = malloc(sizeof (RDB_environment));
    if (envp == NULL)
       return RDB_NO_MEMORY;

    envp->closefn = NULL;

    /* create environment handle */
    *envpp = envp;
    err = db_env_create(&envp->envp, 0);
    if (err != 0) {
        free(envp);
        return err;
    }
    
    /* open DB environment */
    err = envp->envp->open(envp->envp, path, DB_JOINENV, 0);
    if (err != 0) {
        envp->envp->close(envp->envp, 0);
        free(envp);
        return err;
    }
    
    return RDB_OK;
}

int
RDB_close_env(RDB_environment *envp)
{
    int err;

    if (envp->closefn != NULL)
        (*envp->closefn)(envp);
    err = envp->envp->close(envp->envp, 0);
    free(envp);
    return err;
}

void
RDB_set_env_closefn(RDB_environment *envp, void (*fn)(struct RDB_environment *))
{
    envp->closefn = fn;
}
