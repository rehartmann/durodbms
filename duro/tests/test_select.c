/* $Id$ */

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

    assert(RDB_table_to_array(&array, vtbp, 0, NULL, RDB_UNBUFFERED, ecp, &tx) == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_float(tplp, "SALARY"));
    }
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);
    RDB_clear_err(ecp);

    /* Must destroy the array before the table because of RDB_UNBUFFERED. */
    assert(RDB_destroy_obj(&array, ecp) == RDB_OK);

    assert(RDB_drop_table(vtbp, ecp, &tx) == RDB_OK);

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
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    RDB_exec_context ec;
    
    assert(RDB_open_env("dbenv", &envp, RDB_CREATE) == RDB_OK);

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    assert(dbp != NULL);

    test_select(dbp, &ec);
    RDB_destroy_exec_context(&ec);

    assert(RDB_close_env(envp) == RDB_OK);

    return 0;
}
