/* $Id$ */

#include "rdb.h"
#include <string.h>

int
RDB_getargs(int *argcp, char **argvp[], RDB_environment **envpp, RDB_database **dbpp)
{
    int err;
    char *envnamp = NULL;
    char *dbnamp = NULL;

    *envpp = NULL;
    *dbpp = NULL;

    (*argcp)--;
    (*argvp)++;
    while (*argcp >= 2) {
        if (strcmp((*argvp)[0], "-e") == 0) {
            envnamp = (*argvp)[1];
            *argvp += 2;
            *argcp -= 2;
        } else if (strcmp((*argvp)[0], "-d") == 0) {
            dbnamp = (*argvp)[1];
            *argvp += 2;
            *argcp -= 2;
        } else
            break;
    }
    if (envnamp != NULL) {
        err = RDB_open_env(envnamp, envpp);
        if (err != RDB_OK)
            return err;
        if (dbnamp != NULL) {
            err = RDB_get_db(dbnamp, *envpp, dbpp);
            if (err != RDB_OK) {
                RDB_close_env(*envpp);
                return err;
            }
        }
    }
    return RDB_OK;
}

