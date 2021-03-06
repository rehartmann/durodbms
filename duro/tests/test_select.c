#include <rel/rdb.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

void
test_select(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_object array;
    RDB_object *tplp;
    RDB_expression *exp, *argp;
    RDB_int i;
    RDB_object tpl;

    assert(RDB_begin_tx(ecp, &tx, dbp, NULL) == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    RDB_init_obj(&array);

    exp = RDB_ro_op("where", ecp);
    assert(exp != NULL);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    argp = RDB_eq(RDB_var_ref("NAME", ecp), RDB_string_to_expr("Smith", ecp),
            ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);

    assert(RDB_table_to_array(&array, vtbp, 0, NULL, 0, ecp, &tx) == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_float(tplp, "SALARY"));
    }
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);
    RDB_clear_err(ecp);

    /* Must destroy the array before the table because of RDB_UNBUFFERED. */
    assert(RDB_destroy_obj(&array, ecp) == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);

    RDB_init_obj(&tpl);
    assert(RDB_extract_tuple(vtbp, ecp, &tx, &tpl) == RDB_ERROR);
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NO_RUNNING_TX_ERROR);
    RDB_destroy_obj(&tpl, ecp);

    assert(RDB_drop_table(vtbp, ecp, NULL) == RDB_OK);

    assert(RDB_begin_tx(ecp, &tx, dbp, NULL) == RDB_OK);

    RDB_init_obj(&array);

    exp = RDB_ro_op("where", ecp);
    assert(exp != NULL);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    argp = RDB_eq(RDB_var_ref("EMPNO", ecp), RDB_int_to_expr(1, ecp), ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);
    
    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);

    RDB_init_obj(&tpl);
    if (RDB_extract_tuple(vtbp, ecp, &tx, &tpl) != RDB_OK) {
        puts(RDB_type_name(RDB_obj_type(RDB_get_err(ecp))));
        assert(0);
    }
    printf("EMPNO: %d\n", (int)RDB_tuple_get_int(&tpl, "EMPNO"));
    printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
    printf("SALARY: %f\n", (double)RDB_tuple_get_float(&tpl, "SALARY"));
    RDB_destroy_obj(&tpl, ecp);

    assert(RDB_table_to_array(&array, vtbp, 0, NULL, 0, ecp, &tx) == RDB_OK);

    /*
     * Dropping the table must be OK, since RDB_table_to_array was not called
     * with RDB_UNBUFFERED.
     */
    assert(RDB_drop_table(vtbp, ecp, &tx) == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_float(tplp, "SALARY"));
    }

    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);

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

    dbp = RDB_get_db_from_env("TEST", envp, &ec, NULL);
    assert(dbp != NULL);

    test_select(dbp, &ec);

    assert(RDB_close_env(envp, &ec) == RDB_OK);

    RDB_destroy_exec_context(&ec);
    return 0;
}
