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
test_update(RDB_database *dbp)
{
    int err;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_attr_update attrs[1];
    RDB_expression *exprp;

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    RDB_get_table(dbp, "EMPS1", &tbp);

    printf("Updating table, setting SALARY to 4500\n");
    attrs[0].name = "SALARY";
    attrs[0].valuep = RDB_rational_const(4500.0);
    err = RDB_update(tbp, NULL, 1, attrs, &tx);
    if (err != 0) {
        goto error;
    }

    printf("Updating table, setting EMPNO from 2 to 3\n");
    attrs[0].name = "EMPNO";
    attrs[0].valuep = RDB_int_const(3);
    exprp = RDB_eq(RDB_expr_attr("EMPNO", &RDB_INTEGER),
            RDB_int_const(2));
    err = RDB_update(tbp, exprp, 1, attrs, &tx);
    if (err != 0) {
        goto error;
    }

    RDB_drop_expr(exprp);

    printf("Updating table, setting NAME of no 1 to Smythe\n");
    attrs[0].name = "NAME";
    attrs[0].valuep = RDB_string_const("Smythe");
    exprp = RDB_eq(RDB_expr_attr("EMPNO", &RDB_INTEGER),
            RDB_int_const(1));
    err = RDB_update(tbp, exprp, 1, attrs, &tx);
    if (err != 0) {
        goto error;
    }

    RDB_drop_expr(exprp);

    printf("Updating table, setting NAME of no 3 to Johnson\n");
    attrs[0].name = "NAME";
    attrs[0].valuep = RDB_string_const("Johnson");
    exprp = RDB_eq(RDB_expr_attr("EMPNO", &RDB_INTEGER),
            RDB_int_const(3));
    err = RDB_update(tbp, exprp, 1, attrs, &tx);
    if (err != 0) {
        goto error;
    }

    RDB_drop_expr(exprp);

    printf("Converting table to array\n");
    err = print_table(tbp, &tx);
    if (err != RDB_OK) {
        goto error;
    }
    
    printf("End of transaction\n");
    return RDB_commit(&tx);

error:
    RDB_rollback(&tx);
    return err;
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

    err = test_update(dbp);
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
