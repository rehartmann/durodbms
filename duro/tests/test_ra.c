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
    int ret;
    RDB_int i;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tb1p);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }
    ret = RDB_get_table("EMPS2", &tx, &tb2p);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating intersection (EMPS1, EMPS2)\n");
    ret = RDB_intersect(tb1p, tb2p, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating projection (NAME)\n");

    ret = RDB_project(vtbp, 1, projattrs1, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, &tx);
        RDB_commit(&tx);
        return ret;
    }

    RDB_init_tuple(&tpl);

    RDB_init_array(&array);

    printf("Converting virtual table to array\n");
    ret = RDB_table_to_array(vtbp, &array, 0, NULL, &tx);
    if (ret != RDB_OK) {
        RDB_destroy_array(&array);
        RDB_commit(&tx);
        return ret;
    } 

    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
    }
    RDB_destroy_tuple(&tpl);
    RDB_destroy_array(&array);

    if (ret != RDB_NOT_FOUND) {
        RDB_commit(&tx);
        return ret;
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

    ret = test_ra(dbp);
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
