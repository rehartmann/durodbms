/* $Id$ */

#include <rel/rdb.h>
#include <rel/typeimpl.h>
#include <tests/point.h>
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

RDB_type *argtv[] = { &RDB_RATIONAL, &RDB_RATIONAL };

int
test_type(RDB_database *dbp)
{
    RDB_transaction tx;
    int ret;
    RDB_type *typ;
    RDB_type *updargtv[2];
    RDB_bool updv[2];
    RDB_type *getargtv[1];

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
    ret = RDB_implement_type("POINT", NULL, sizeof(i_point), &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_get_type("POINT", &tx, &typ);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    /*
     * Create selectors
     */
    ret = RDB_create_ro_op("POINT", 2, argtv, typ, "libpoint", "POINT",
            NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_create_ro_op("POLAR", 2, argtv, typ, "libpoint", "POLAR",
            NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    /*
     * Create setters
     */
    updargtv[0] = typ;
    updargtv[1] = &RDB_RATIONAL;
    updv[0] = RDB_TRUE;
    updv[1] = RDB_FALSE;
     
    ret = RDB_create_update_op("POINT_set_X", 2, updargtv, updv, "libpoint",
            "POINT_set_X", NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_create_update_op("POINT_set_Y", 2, updargtv, updv, "libpoint",
            "POINT_set_Y", NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_create_update_op("POINT_set_THETA", 2, updargtv, updv, "libpoint",
            "POINT_set_THETA", NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_create_update_op("POINT_set_LENGTH", 2, updargtv, updv, "libpoint",
            "POINT_set_LENGTH", NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    /*
     * Create getters
     */
    getargtv[0] = typ;

    ret = RDB_create_ro_op("POINT_get_X", 1, getargtv, &RDB_RATIONAL, "libpoint",
            "POINT_get_X", NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_create_ro_op("POINT_get_Y", 1, getargtv, &RDB_RATIONAL, "libpoint",
            "POINT_get_Y", NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_create_ro_op("POINT_get_THETA", 1, getargtv, &RDB_RATIONAL, "libpoint",
            "POINT_get_THETA", NULL, 0, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_create_ro_op("POINT_get_LENGTH", 1, getargtv, &RDB_RATIONAL, "libpoint",
            "POINT_get_LENGTH", NULL, 0, &tx);
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
