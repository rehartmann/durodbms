/* $Id$ */

#include "rdb.h"
#include <string.h>

int
RDB_getargs(int *argcp, char **argvp[], RDB_environment **envpp, RDB_database **dbpp)
{
    int ret;
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
        ret = RDB_open_env(envnamp, envpp);
        if (ret != RDB_OK)
            return ret;
        if (dbnamp != NULL) {
            ret = RDB_get_db_from_env(dbnamp, *envpp, dbpp);
            if (ret != RDB_OK) {
                RDB_close_env(*envpp);
                return ret;
            }
        }
    }
    return RDB_OK;
}

