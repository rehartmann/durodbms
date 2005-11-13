/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_select(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_object array;
    RDB_object *tplp;
    RDB_expression *exprp;
    int ret;
    RDB_int i;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    printf("Creating selection (name=\"Smith\")\n");

    exprp = RDB_eq(RDB_expr_attr("NAME"), RDB_string_to_expr("Smith"));
    if (exprp == NULL)
        goto error;
    
    vtbp = RDB_select(tbp, exprp, ecp, &tx);
    if (vtbp == NULL) {
        goto error;
    }

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(&array, vtbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_rational(tplp, "SALARY"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);

    printf("Dropping selection\n");
    RDB_drop_table(vtbp, ecp, &tx);

    RDB_init_obj(&array);

    printf("Creating selection (EMPNO=1)\n");

    exprp = RDB_eq(RDB_expr_attr("EMPNO"), RDB_int_to_expr(1));
    
    vtbp = RDB_select(tbp, exprp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(&array, vtbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_rational(tplp, "SALARY"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/

    RDB_destroy_obj(&array, ecp);

    printf("Dropping selection\n");
    RDB_drop_table(vtbp, ecp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);

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
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_select(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
