#include <rel/rdb.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

static void
test_keys1(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 1, ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson", ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_float(&tpl, "SALARY", (RDB_float)4000.0, ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    assert(ret == RDB_OK);

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret == RDB_ERROR
            && RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR);

    RDB_destroy_obj(&tpl, ecp);
    RDB_clear_err(ecp);
    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

void
test_keys2(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3, ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_string(&tpl, "NAME", "Smith", ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_float(&tpl, "SALARY", (RDB_float)4000.0, ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    assert(ret == RDB_OK);

    ret = RDB_insert(tbp, &tpl, ecp, &tx);

    assert(ret == RDB_ERROR
            && RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR);

    RDB_destroy_obj(&tpl, ecp);
    RDB_clear_err(ecp);
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
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec, NULL);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    test_keys1(dbp, &ec);
    test_keys2(dbp, &ec);
    
    ret = RDB_close_env(envp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
