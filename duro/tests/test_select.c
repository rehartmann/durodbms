/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_select(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_array array;
    RDB_tuple tpl;
    RDB_expression *exprp;
    int ret;
    RDB_int i;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_array(&array);

    printf("Creating selection (name=\"Smith\")\n");

    exprp = RDB_eq(RDB_expr_attr("NAME", &RDB_STRING),
                    RDB_string_const("Smith"));
    
    ret = RDB_select(tbp, exprp, &vtbp);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(vtbp, &array, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    RDB_init_tuple(&tpl);

    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);

    printf("Dropping selection\n");
    RDB_drop_table(vtbp, &tx);

    RDB_init_array(&array);

    printf("Creating selection (EMPNO=1)\n");

    exprp = RDB_eq(RDB_expr_attr("EMPNO", &RDB_INTEGER),
                    RDB_int_const(1));
    
    ret = RDB_select(tbp, exprp, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(vtbp, &array, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    RDB_init_tuple(&tpl);

    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);

    printf("Dropping selection\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);
error:
    RDB_destroy_array(&array);
    RDB_rollback(&tx);
    return ret;
}

int
main()
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("db", &envp);
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

    ret = test_select(dbp);
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
