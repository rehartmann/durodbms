/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *projattrs1[] = { "SALARY" };

char *projattrs2[] = { "EMPNO", "NAME" };

static int
print_table1(RDB_table *tbp, RDB_transaction *txp)
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
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_destroy_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    
    return RDB_OK;
error:
    RDB_destroy_array(&array);
    
    return err;
}

static int
print_table2(RDB_table *tbp, RDB_transaction *txp)
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
    }
    RDB_destroy_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    
    return RDB_OK;
error:
    RDB_destroy_array(&array);
    
    return err;
}

int
test_project(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_tuple tpl;
    int err;

    RDB_get_table(dbp, "EMPS2", &tbp);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Creating projection (SALARY)\n");

    err = RDB_project(tbp, 1, projattrs1, &vtbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Converting projection table to array\n");
    err = print_table1(vtbp, &tx);

    RDB_init_tuple(&tpl);
    RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    err = RDB_table_contains(vtbp, &tpl, &tx);
    printf("Projection contains SALARY(4000.0): %d %s\n", err, RDB_strerror(err));

    if (err != RDB_OK && err != RDB_NOT_FOUND) {
        RDB_rollback(&tx);
        return err;
    }

    RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4400.0);
    err = RDB_table_contains(vtbp, &tpl, &tx);
    printf("Projection contains SALARY(4400.0): %d %s\n", err, RDB_strerror(err));

    if (err != RDB_OK && err != RDB_NOT_FOUND) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Dropping projection\n");
    RDB_drop_table(vtbp, &tx);

    printf("Creating projection (EMPNO,NAME)\n");

    err = RDB_project(tbp, 2, projattrs2, &vtbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Converting projection table to array\n");
    err = print_table2(vtbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    } 

    printf("Dropping projection\n");
    RDB_drop_table(vtbp, &tx);

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

    err = test_project(dbp);
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
