/* $Id$ */

#include <rel/rdb.h>
#include <rel/typeimpl.h>
#include <stdlib.h>
#include <stdio.h>

RDB_attr pointcompv[] = {
    { "X", &RDB_RATIONAL, NULL, 0 },
    { "Y", &RDB_RATIONAL, NULL, 0 }
};

RDB_attr polarcompv[] = {
    { "THETA", &RDB_RATIONAL, NULL, 0 },
    { "LENGTH", &RDB_RATIONAL, NULL, 0 }
};

RDB_possrep prv[] = {
    { "POINT", 2, pointcompv, NULL },
    { "POLAR", 2, polarcompv, NULL }
};

int
test_type(RDB_database *dbp)
{
    RDB_transaction tx;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Defining type\n");
    ret = RDB_define_type("POINT", 2, prv, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Implementing type\n");
    ret = RDB_implement_type("POINT", "libpoint", NULL, RDB_FIXED_BINARY,
            sizeof(RDB_rational) * 2, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_commit(&tx);
    if (ret != RDB_OK) {
        return ret;
    }
    return RDB_OK;
}

int
main()
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("db", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    ret = RDB_get_db_from_env("TEST", envp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_type(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
