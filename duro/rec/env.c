/* $Id$ */

#include "env.h"
#include <gen/errors.h>
#include <stdlib.h>
#include <stdarg.h>

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
    envp->errfn = NULL;
    envp->user_data = NULL;

    /* create environment handle */
    *envpp = envp;
    ret = db_env_create(&envp->envp, 0);
    if (ret != 0) {
        free(envp);
        return RDB_convert_err(ret);
    }
    
    /* open DB environment */
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

static void
dberrfn(const DB_ENV *dbenv, const char *errpfx, const char *msg)
{
    RDB_environment *envp = dbenv->app_private;

    (*envp->errfn)(msg, envp->errfn_arg);
}

void
RDB_set_errfn(RDB_environment *envp,
        void (*errfn)(const char *msg, void *arg), void *arg)
{
    envp->errfn = errfn;
    envp->errfn_arg = arg;

    envp->envp->app_private = envp;
    envp->envp->set_errcall(envp->envp, &dberrfn);
}

void
RDB_errmsg(RDB_environment *envp, const char *format, ...)
{
    if (envp->errfilep != NULL) {
        va_list ap;

        va_start(ap,format);
        vfprintf(envp->errfilep, format, ap);
        va_end(ap);
        fputs("\n", envp->errfilep);
    }
    if (envp->errfn != NULL) {
        char buf[1024];

        va_list ap;

        va_start(ap,format);
        vsnprintf(buf, sizeof (buf), format, ap);
        va_end(ap);
        (*envp->errfn)(buf, envp->errfn_arg);
    }
}
