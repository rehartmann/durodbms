/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
print_extend(RDB_table *vtbp, RDB_transaction *txp)
{
    RDB_array array;
    RDB_tuple tpl;
    int err;
    int i;

    RDB_init_array(&array);

    err = RDB_table_to_array(vtbp, &array, 0, NULL, txp);
    if (err != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(&tpl, "SALARY"));
        printf("SALARY_W_BONUS: %f\n",
                (float)RDB_tuple_get_rational(&tpl, "SALARY_W_BONUS"));
    }
    RDB_deinit_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        RDB_rollback(txp);
        goto error;
    }

    RDB_deinit_array(&array);
    return RDB_OK;
error:
    RDB_deinit_array(&array);
    return err;
}

int
insert_extend(RDB_table *vtbp, RDB_transaction *txp)
{
    RDB_tuple tpl;
    int err;

    RDB_init_tuple(&tpl);    

    printf("Inserting tuple #1\n");

    err = RDB_tuple_set_int(&tpl, "EMPNO", 3);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&tpl, "NAME", "Johnson");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&tpl, "SALARY_W_BONUS", (RDB_rational)4200.0);
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(vtbp, &tpl, txp);
    if (err == RDB_PREDICATE_VIOLATION) {
        printf("predicate violation - OK\n");
    } else {
        printf("Error: %s\n", RDB_strerror(err));
    }

    printf("Inserting tuple #2\n");

    err = RDB_tuple_set_int(&tpl, "EMPNO", 3);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&tpl, "NAME", "Johnson");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&tpl, "SALARY_W_BONUS", (RDB_rational)4100.0);
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(vtbp, &tpl, txp);
    if (err != RDB_OK)
        goto error;
    
    RDB_deinit_tuple(&tpl);
    return RDB_OK;

error:
    RDB_deinit_tuple(&tpl);
    return err;
}

int
test_extend(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    int err;
    RDB_virtual_attr extend = {
        "SALARY_W_BONUS",
        RDB_add(RDB_expr_attr("SALARY", &RDB_RATIONAL), RDB_rational_const(100))
    };

    RDB_get_table(dbp, "EMPS1", &tbp);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Extending EMPS1 (SALARY_W_BONUS)\n");

    err = RDB_extend(tbp, 1, &extend, &vtbp);
    if (err != RDB_OK) {
        goto error;
    }
    
    printf("converting extended table to array\n");
    err = print_extend(vtbp, &tx);
    if (err != RDB_OK) {
        goto error;
    }

    err = insert_extend(vtbp, &tx);
    if (err != RDB_OK) {
        goto error;
    }

    printf("Dropping extension\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");
    /* Abort transaction, since we don't want the update to be persistent */
    return RDB_rollback(&tx);

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

    err = test_extend(dbp);
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
