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
        printf("DEPTNO: %d\n", RDB_tuple_get_int(&tpl, "DEPTNO"));
        printf("DEPTNAME: %s\n", RDB_tuple_get_string(&tpl, "DEPTNAME"));
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
test_join(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp1, *tbp2, *vtbp;
    int err;

    RDB_get_table(dbp, "EMPS1", &tbp1);
    RDB_get_table(dbp, "DEPTS", &tbp2);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Joining EMPS1 with DEPTS\n");

    err = RDB_join(tbp1, tbp2, &vtbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }
    
    printf("Converting joined table to array\n");
    err = print_table(vtbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Dropping join\n");
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

    err = test_join(dbp);
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
