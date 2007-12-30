/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_select(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_object array;
    RDB_object *tplp;
    RDB_expression *exp, *argp;
    int ret;
    RDB_int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    exp = RDB_ro_op("WHERE", ecp);
    if (exp == NULL)
        goto error;

    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(exp, argp);

    argp = RDB_eq(RDB_var_ref("NAME", ecp), RDB_string_to_expr("Smith", ecp),
            ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        goto error;
    }

    ret = RDB_table_to_array(&array, vtbp, 0, NULL, 0, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_float(tplp, "SALARY"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);

    RDB_drop_table(vtbp, ecp, &tx);

    RDB_init_obj(&array);

    exp = RDB_ro_op("WHERE", ecp);
    if (exp == NULL)
        goto error;

    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(exp, argp);

    argp = RDB_eq(RDB_var_ref("EMPNO", ecp), RDB_int_to_expr(1, ecp), ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(exp, argp);
    
    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    ret = RDB_table_to_array(&array, vtbp, 0, NULL, 0, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_float(tplp, "SALARY"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }

    RDB_destroy_obj(&array, ecp);

    RDB_drop_table(vtbp, ecp, &tx);

    return RDB_commit(ecp, &tx);

error:
    RDB_destroy_obj(&array, ecp);
    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
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
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    ret = test_select(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
