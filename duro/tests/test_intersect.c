/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

static int
print_table(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;
    RDB_int i;

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (ret = RDB_array_get(&array, i, &tplp)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
    }
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_obj(&array);
    
    return RDB_OK;
error:
    RDB_destroy_obj(&array);
    
    return ret;
}

int
test_intersect(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *tbp2, *vtbp;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }
    ret = RDB_get_table("EMPS2", &tx, &tbp2);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating EMPS1 intersect EMPS2\n");

    ret = RDB_intersect(tbp2, tbp, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("converting intersect table to array\n");
    ret = print_table(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Dropping intersect\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_intersect(dbp);
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
