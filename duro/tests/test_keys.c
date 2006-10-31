/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

void
test_keys(RDB_database *dbp, RDB_exec_context *ecp)
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
    ret = RDB_tuple_set_double(&tpl, "SALARY", (RDB_double)4000.0, ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    assert(ret == RDB_OK);

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR);
    RDB_clear_err(ecp);

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3, ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_string(&tpl, "NAME", "Smith", ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_double(&tpl, "SALARY", (RDB_double)4000.0, ecp);
    assert(ret == RDB_OK);
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    assert(ret == RDB_OK);

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR);
    RDB_destroy_obj(&tpl, ecp);
    RDB_clear_err(ecp);
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

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    test_keys(dbp, &ec);
    RDB_destroy_exec_context(&ec);
    
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
