/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_regexp(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_array array;
    RDB_tuple tpl;
    RDB_expression *exprp;
    int ret;
    RDB_int i;

    ret = RDB_get_table(dbp, "EMPS1", &tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_array(&array);

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        RDB_destroy_array(&array);
        return ret;
    }

    printf("Creating selection (NAME regmatch \"o\")\n");

    exprp = RDB_regmatch(RDB_expr_attr("NAME", &RDB_STRING),
                    RDB_string_const("o"));
    
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
        printf("EMPNO: %d\n", RDB_tuple_get_int(&tpl, "EMPNO"));
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
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("db", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_regexp(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
