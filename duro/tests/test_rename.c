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
        printf("EMP#: %d\n", RDB_tuple_get_int(&tpl, "EMP#"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SAL: %f\n", (double)RDB_tuple_get_rational(&tpl, "SAL"));
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

RDB_renaming renv[] = {
    { "SALARY", "SAL" },
    { "EMPNO", "EMP#" }
};

int
test_rename(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_tuple tpl;
    int err;

    RDB_get_table(dbp, "EMPS1", &tbp);

    printf("Creating EMPS1 RENAME (SALARY AS SAL, EMPNO AS EMP#)\n");

    err = RDB_rename(tbp, 2, renv, &vtbp);
    if (err != RDB_OK) {
        return err;
    }

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Printing renaming table\n");
    err = print_table(vtbp, &tx);

    RDB_init_tuple(&tpl);
    err = RDB_tuple_set_int(&tpl, "EMP#", 1);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&tpl, "NAME", "Smith");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&tpl, "SAL", (RDB_rational)4000.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (err != RDB_OK)
        goto error;

    err = RDB_table_contains(vtbp, &tpl, &tx);
    printf("Result of RDB_table_contains(): %d %s\n", err, RDB_strerror(err));

    if (err != RDB_OK && err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_tuple(&tpl);

    printf("Dropping rename\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);

error:
    RDB_destroy_tuple(&tpl);

    printf("Dropping renaming\n");
    RDB_drop_table(vtbp, &tx);

    printf("Dropping rename\n");
    RDB_drop_table(vtbp, &tx);

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
    err = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }

    err = test_rename(dbp);
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
