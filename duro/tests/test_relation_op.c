#include <rel/rdb.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

void
test_relation(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_type *reltyp;
    RDB_object array;
    RDB_expression *exp;
    RDB_int len;

    assert(RDB_begin_tx(ecp, &tx, dbp, NULL) == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    RDB_init_obj(&array);

    exp = RDB_ro_op("relation", ecp);
    assert(exp != NULL);

    /*
     * Must set type of RELATION expression if there is no argument
     */
    reltyp = RDB_new_relation_type(0, NULL, ecp);
    assert(reltyp != NULL);
    RDB_set_expr_type(exp, reltyp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);

    assert(RDB_table_to_array(&array, vtbp, 0, NULL, 0, ecp, &tx) == RDB_OK);

    len = RDB_array_length(&array, ecp);
    assert(len == 0);

    assert(RDB_destroy_obj(&array, ecp) == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

int
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    envp = RDB_open_env(argc <= 1 ? "dbenv" : argv[1], RDB_RECOVER, &ec);
    assert(envp != NULL);

    RDB_env_set_errfile(envp, stderr);

    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    assert(dbp != NULL);

    test_relation(dbp, &ec);
    RDB_destroy_exec_context(&ec);

    assert(RDB_close_env(envp, &ec) == RDB_OK);

    return 0;
}
