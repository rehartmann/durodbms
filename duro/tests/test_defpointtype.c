#include <rel/rdb.h>
#include <rel/typeimpl.h>

#include <tests/point.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#define SHLIB "point"

RDB_attr pointcompv[] = {
    { "x", NULL, NULL, 0 },
    { "y", NULL, NULL, 0 }
};

RDB_attr polarcompv[] = {
    { "theta", NULL, NULL, 0 },
    { "length", NULL, NULL, 0 }
};

RDB_possrep prv[] = {
    { "point", 2, pointcompv },
    { "polar", 2, polarcompv }
};

void
test_type(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    int ret;
    RDB_type *typ;
    RDB_parameter updparamv[2];
    RDB_parameter getparamv[1];
    RDB_parameter paramv[2];
    RDB_expression *initexp;

    paramv[0].typ = &RDB_FLOAT;
    paramv[1].typ = &RDB_FLOAT;

    pointcompv[0].typ = &RDB_FLOAT;
    pointcompv[1].typ = &RDB_FLOAT;

    polarcompv[0].typ = &RDB_FLOAT;
    polarcompv[1].typ = &RDB_FLOAT;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    initexp = RDB_ro_op("point", ecp);
    assert(initexp != NULL);

    RDB_add_arg(initexp, RDB_float_to_expr((RDB_float) 0.0, ecp));
    RDB_add_arg(initexp, RDB_float_to_expr((RDB_float) 0.0, ecp));

    ret = RDB_define_type("point", 2, prv, NULL, initexp, 0, ecp, &tx);
    assert(ret == RDB_OK);

    /*
     * Create selectors
     */
    typ = RDB_get_type("point", ecp, &tx);
    assert(typ != NULL);

    ret = RDB_create_ro_op("point", 2, paramv, typ, SHLIB, "point",
            NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("polar", 2, paramv, typ, SHLIB, "polar",
            NULL, ecp, &tx);
    assert(ret == RDB_OK);

    /*
     * Create setters
     */
    updparamv[0].typ = typ;
    updparamv[0].update = RDB_TRUE;
    updparamv[1].typ = &RDB_FLOAT;
    updparamv[1].update = RDB_FALSE;

    ret = RDB_create_update_op("point" RDB_SETTER_INFIX "x", 2, updparamv, SHLIB,
            "point_set_x", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_update_op("point" RDB_SETTER_INFIX "y", 2, updparamv, SHLIB,
            "point_set_y", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_update_op("point" RDB_SETTER_INFIX "theta", 2, updparamv, SHLIB,
            "point_set_theta", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_update_op("point" RDB_SETTER_INFIX "length", 2, updparamv, SHLIB,
            "point_set_length", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    /*
     * Create getters
     */
    getparamv[0].typ = typ;

    ret = RDB_create_ro_op("point" RDB_GETTER_INFIX "x", 1, getparamv, &RDB_FLOAT, SHLIB,
            "point" RDB_GETTER_INFIX "x", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("point" RDB_GETTER_INFIX "y", 1, getparamv, &RDB_FLOAT, SHLIB,
            "point" RDB_GETTER_INFIX "y", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("point" RDB_GETTER_INFIX "theta", 1, getparamv, &RDB_FLOAT, SHLIB,
            "point" RDB_GETTER_INFIX "theta", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("point" RDB_GETTER_INFIX "length", 1, getparamv, &RDB_FLOAT, SHLIB,
            "point" RDB_GETTER_INFIX "length", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    /*
     * Implement type
     */
    ret = RDB_implement_type("point", NULL, sizeof(i_point), ecp, &tx);
    if (ret != RDB_OK) {
        fprintf(stderr, "%s\n", RDB_type_name(RDB_obj_type(RDB_get_err(ecp))));
        assert(RDB_FALSE);
    }
    assert(ret == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

int
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;

    envp = RDB_open_env(argc <= 1 ? "dbenv" : argv[1], RDB_RECOVER, &ec);
    if (envp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    RDB_env_set_errfile(envp, stderr);

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    test_type(dbp, &ec);

    ret = RDB_close_env(envp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
