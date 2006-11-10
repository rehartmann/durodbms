/* $Id$ */

#include <rel/rdb.h>
#include <rel/typeimpl.h>
#include <tests/point.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#ifdef _WIN32
#define SHLIB "point"
#else
#define SHLIB "libpoint"
#endif

RDB_attr pointcompv[] = {
    { "X", NULL, NULL, 0 },
    { "Y", NULL, NULL, 0 }
};

RDB_attr polarcompv[] = {
    { "THETA", NULL, NULL, 0 },
    { "LENGTH", NULL, NULL, 0 }
};

RDB_possrep prv[] = {
    { "POINT", 2, pointcompv },
    { "POLAR", 2, polarcompv }
};

void
test_type(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    int ret;
    RDB_type *typ;
    RDB_type *updargtv[2];
    RDB_bool updv[2];
    RDB_type *getargtv[1];
    RDB_type *argtv[2];

    argtv[0] = &RDB_DOUBLE;
    argtv[1] = &RDB_DOUBLE;

    pointcompv[0].typ = &RDB_DOUBLE;
    pointcompv[1].typ = &RDB_DOUBLE;

    polarcompv[0].typ = &RDB_DOUBLE;
    polarcompv[1].typ = &RDB_DOUBLE;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    ret = RDB_define_type("POINT", 2, prv, NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_implement_type("POINT", NULL, sizeof(i_point), ecp, &tx);
    assert(ret == RDB_OK);

    typ = RDB_get_type("POINT", ecp, &tx);
    assert(ret == RDB_OK);

    /*
     * Create selectors
     */
    ret = RDB_create_ro_op("POINT", 2, argtv, typ, SHLIB, "POINT",
            NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("POLAR", 2, argtv, typ, SHLIB, "POLAR",
            NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    /*
     * Create setters
     */
    updargtv[0] = typ;
    updargtv[1] = &RDB_DOUBLE;
    updv[0] = RDB_TRUE;
    updv[1] = RDB_FALSE;
     
    ret = RDB_create_update_op("POINT_set_X", 2, updargtv, updv, SHLIB,
            "POINT_set_X", NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_update_op("POINT_set_Y", 2, updargtv, updv, SHLIB,
            "POINT_set_Y", NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_update_op("POINT_set_THETA", 2, updargtv, updv, SHLIB,
            "POINT_set_THETA", NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_update_op("POINT_set_LENGTH", 2, updargtv, updv, SHLIB,
            "POINT_set_LENGTH", NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    /*
     * Create getters
     */
    getargtv[0] = typ;

    ret = RDB_create_ro_op("POINT_get_X", 1, getargtv, &RDB_DOUBLE, SHLIB,
            "POINT_get_X", NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("POINT_get_Y", 1, getargtv, &RDB_DOUBLE, SHLIB,
            "POINT_get_Y", NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("POINT_get_THETA", 1, getargtv, &RDB_DOUBLE, SHLIB,
            "POINT_get_THETA", NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("POINT_get_LENGTH", 1, getargtv, &RDB_DOUBLE, SHLIB,
            "POINT_get_LENGTH", NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    test_type(dbp, &ec);
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
