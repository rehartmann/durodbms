/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *projattrs1[] = { "NAME" };

int
test_ra(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tb1p, *tb2p, *vtbp;
    RDB_array array;
    RDB_tuple tpl;
    int err;
    RDB_int i;

    RDB_get_table(dbp, "EMPS1", &tb1p);
    RDB_get_table(dbp, "EMPS2", &tb2p);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Creating intersection (EMPS1, EMPS2)\n");
    err = RDB_intersect(tb1p, tb2p, &vtbp);
    if (err != RDB_OK) {
        RDB_commit(&tx);
        return err;
    }

    printf("Creating projection (NAME)\n");

    err = RDB_project(vtbp, 1, projattrs1, &vtbp);
    if (err != RDB_OK) {
        RDB_drop_table(vtbp, &tx);
        RDB_commit(&tx);
        return err;
    }

    RDB_init_tuple(&tpl);

    RDB_init_array(&array);

    printf("Converting virtual table to array\n");
    err = RDB_table_to_array(vtbp, &array, 0, NULL, &tx);
    if (err != RDB_OK) {
        RDB_destroy_array(&array);
        RDB_commit(&tx);
        return err;
    } 

    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
    }
    RDB_destroy_tuple(&tpl);
    RDB_destroy_array(&array);

    if (err != RDB_NOT_FOUND) {
        RDB_commit(&tx);
        return err;
    }

    printf("Dropping virtual table\n");
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

    err = test_ra(dbp);
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
