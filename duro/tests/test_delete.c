/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

static int
print_table(RDB_table *tbp, RDB_transaction *txp)
{
    int err;
    RDB_tuple tpl;
    RDB_array array;
    RDB_int i;

    RDB_init_array(&array);

    printf("converting table to array\n");
    err = RDB_table_to_array(tbp, &array, 0, NULL, txp);
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
    
    return RDB_OK;
error:
    RDB_deinit_array(&array);
    
    return err;
}

int
test_delete(RDB_database *dbp)
{
    int err;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_expression *exprp;

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    RDB_get_table(dbp, "EMPS1", &tbp);

    printf("Deleting #1 from EMPS1\n");
    exprp = RDB_eq(RDB_expr_attr("EMPNO", &RDB_INTEGER),
            RDB_int_const(1));
    err = RDB_delete(tbp, exprp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    RDB_drop_expr(exprp);

    err = print_table(tbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Deleting all tuples from EMPS1\n");
    err = RDB_delete(tbp, NULL, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    err = print_table(tbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main()
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int err;
    
    printf("Opening environment\n");
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

    err = test_delete(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    printf ("Closing environment\n");
    err = RDB_close_env(dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    return 0;
}
