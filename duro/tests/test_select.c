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
    int err;
    RDB_int i;

    err = RDB_get_table(dbp, "EMPS1", &tbp);
    if (err != RDB_OK) {
        return err;
    }

    RDB_init_array(&array);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        RDB_deinit_array(&array);
        return err;
    }

    printf("Creating selection (name=\"Smith\")\n");

    exprp = RDB_eq(RDB_expr_attr("NAME", &RDB_STRING),
                    RDB_string_const("Smith"));
    
    err = RDB_select(tbp, exprp, &vtbp);
    if (err != RDB_OK) {
        goto error;
    }

    printf("Converting selection table to array\n");
    err = RDB_table_to_array(vtbp, &array, &tx);
    if (err != RDB_OK) {
        goto error;
    } 

    RDB_init_tuple(&tpl);

    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_deinit_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_deinit_array(&array);

    printf("Dropping selection\n");
    RDB_drop_table(vtbp, &tx);

    RDB_init_array(&array);

    printf("Creating selection (EMPNO=1)\n");

    exprp = RDB_eq(RDB_expr_attr("EMPNO", &RDB_INTEGER),
                    RDB_int_const(1));
    
    err = RDB_select(tbp, exprp, &vtbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Converting selection table to array\n");
    err = RDB_table_to_array(vtbp, &array, &tx);
    if (err != RDB_OK) {
        goto error;
    } 

    RDB_init_tuple(&tpl);

    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_deinit_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_deinit_array(&array);

    printf("Dropping selection\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);
error:
    RDB_deinit_array(&array);
    RDB_rollback(&tx);
    return err;
}

int
main()
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int err;
    
    printf("Opening DB\n");
    err = RDB_open_env("db", &dsp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }
    err = RDB_get_db("TEST", dsp, &dbp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }

    err = test_select(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    printf ("Closing DB\n");
    err = RDB_release_db(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }
    err = RDB_close_env(dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    return 0;
}
