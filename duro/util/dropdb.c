/* $Id$ */

#include <rel/rdb.h>
#include <rel/internal.h>
#include <stdio.h>

int
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    int err;

    err = RDB_getargs(&argc, &argv, &envp, &dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "droptable: %s\n", RDB_strerror(err));
        return 2;
    }

    if (envp == NULL || dbp == NULL) {
        fprintf(stderr, "usage: dropdb -e <environment> -d <database>\n");
        return 1;
    }

    RDB_internal_env(envp)->set_errfile(RDB_internal_env(envp), stderr);

    err = RDB_drop_db(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "dropdb: %s\n", RDB_strerror(err));
        return 1;
    }

    err = RDB_close_env(envp);
    if (err != RDB_OK) {
        fprintf(stderr, "dropdb: %s\n", RDB_strerror(err));
        return 1;
    }
    
    return 0;
}
