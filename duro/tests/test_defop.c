/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

RDB_type *plusargtv[] = {
    &RDB_INTEGER,
    &RDB_INTEGER
};

RDB_type *addargtv[] = {
    &RDB_INTEGER,
    &RDB_INTEGER
};

RDB_bool updv[] = { RDB_TRUE, RDB_FALSE };

int
test_defop(RDB_database *dbp)
{
    RDB_transaction tx;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Defining PLUS\n");
    ret = RDB_create_ro_op("PLUS", 2, plusargtv, &RDB_INTEGER, "libplus", "RDBU_plus",
            NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Defining ADD\n");
    ret = RDB_create_update_op("ADD", 2, addargtv, updv, "libplus", "RDBU_add",
            NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    return RDB_commit(&tx);
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &envp);
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

    ret = test_defop(dbp);
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
