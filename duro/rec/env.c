/* $Id$ */

#include "env.h"
#include <gen/errors.h>
#include <stdlib.h>

int
RDB_create_env(const char *path, int options, RDB_environment **envpp)
{
    RDB_environment *envp;
    int ret;
    
    envp = malloc(sizeof (RDB_environment));
    if (envp == NULL)
       return RDB_NO_MEMORY;

    envp->closefn = NULL;
    envp->errfilep = NULL;

    /* create environment handle */
    *envpp = envp;
    ret = db_env_create(&envp->envp, 0);
    if (ret != 0) {
        free(envp);
        return RDB_convert_err(ret);
    }
    
    /* create environment */
    ret = envp->envp->open(envp->envp, path,
            DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_CREATE,
            0);
    if (ret != 0) {
        envp->envp->close(envp->envp, 0);
        free(envp);
        return RDB_convert_err(ret);
    }
    
    return RDB_OK;
}

int
RDB_open_env(const char *path, RDB_environment **envpp)
{
    RDB_environment *envp;
    int ret;

    envp = malloc(sizeof (RDB_environment));
    if (envp == NULL)
       return RDB_NO_MEMORY;

    envp->closefn = NULL;
    envp->errfilep = NULL;

    /* create environment handle */
    *envpp = envp;
    ret = db_env_create(&envp->envp, 0);
    if (ret != 0) {
        free(envp);
        return RDB_convert_err(ret);
    }
    
    /* open DB environment */
    ret = envp->envp->open(envp->envp, path, DB_JOINENV, 0);
    if (ret != 0) {
        envp->envp->close(envp->envp, 0);
        free(envp);
        return RDB_convert_err(ret);
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
    return RDB_convert_err(ret);
}

void
RDB_set_env_closefn(RDB_environment *envp, void (*fn)(struct RDB_environment *))
{
    envp->closefn = fn;
}

void
RDB_set_errfile(RDB_environment *envp, FILE *errfile)
{
    envp->errfilep = errfile;
    envp->envp->set_errfile(envp->envp, errfile);
}

void
RDB_errmsg(RDB_environment *envp, const char *msg)
{
    if (envp->errfilep != NULL) {
        fprintf(envp->errfilep, "%s\n", msg);
    }
}
