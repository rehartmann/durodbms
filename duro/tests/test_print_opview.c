/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
print_deptsx_view(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_tuple tpl;
    RDB_array array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table DEPTSX\n");
    ret = RDB_get_table("DEPTSX", &tx, &tmpvtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return ret;
    }

    RDB_init_array(&array);

    ret = RDB_table_to_array(tmpvtbp, &array, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("DEPTNO: %d\n", (int)RDB_tuple_get_int(&tpl, "DEPTNO"));
        printf("DEPTNAME: %s\n", RDB_tuple_get_string(&tpl, "DEPTNAME"));
        printf("XDEPTNO: %d\n", (int)RDB_tuple_get_int(&tpl, "XDEPTNO"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    RDB_commit(&tx);
    return RDB_OK;
error:
    RDB_rollback(&tx);
    return ret;
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("db", &dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = print_deptsx_view(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
