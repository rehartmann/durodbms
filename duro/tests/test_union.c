/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <rel/qresult.h>

static void
print_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;
    RDB_seq_item sq;

    RDB_init_obj(&array);

    /* Test sorting too */
    sq.attrname = "NAME";
    sq.asc = RDB_FALSE;

    ret = RDB_table_to_array(&array, tbp, 1, &sq, ecp, txp);
    assert(ret == RDB_OK);

    assert(RDB_array_length(&array, ecp) == 3);

    tplp = RDB_array_get(&array, 0, ecp);
    assert(tplp != NULL);

    ret = RDB_tuple_get_int(tplp, "EMPNO");
    assert(ret == 1);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Smith") == 0);
    assert(RDB_tuple_get_int(tplp, "DEPTNO") == 1);
    assert(RDB_tuple_get_double(tplp, "SALARY") == 4000.0);

    tplp = RDB_array_get(&array, 1, ecp);
    assert(tplp != NULL);

    assert(RDB_tuple_get_int(tplp, "EMPNO") == 2);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Jones") == 0);
    assert(RDB_tuple_get_int(tplp, "DEPTNO") == 2);
    assert(RDB_tuple_get_double(tplp, "SALARY") == 4100.0);

    tplp = RDB_array_get(&array, 2, ecp);
    assert(tplp != NULL);

    assert(RDB_tuple_get_int(tplp, "EMPNO") == 3);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Clarke") == 0);
    assert(RDB_tuple_get_int(tplp, "DEPTNO") == 2);
    assert(RDB_tuple_get_double(tplp, "SALARY") == 4000.0);

    assert(RDB_destroy_obj(&array, ecp) == RDB_OK);
}

void
test_union(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_expression *exp, *argp;
    RDB_object *tbp, *tbp2, *vtbp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    tbp2 = RDB_get_table("EMPS2", ecp, &tx);
    assert(tbp2 != NULL);

    exp = RDB_ro_op("UNION", 2, ecp);
    assert(exp != NULL);
    
    argp = RDB_table_ref(tbp2, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);
    
    print_table(vtbp, ecp, &tx);

    RDB_drop_table(vtbp, ecp, &tx);

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

    test_union(dbp, &ec);

    RDB_destroy_exec_context(&ec);
    
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
