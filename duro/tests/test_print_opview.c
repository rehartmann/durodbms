/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
print_deptsx_view(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;
    int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table DEPTSX\n");
    tmpvtbp = RDB_get_table("DEPTSX", ecp, &tx);
    if (tmpvtbp == NULL) {
        RDB_rollback(ecp, &tx);
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("DEPTNO: %d\n", (int)RDB_tuple_get_int(tplp, "DEPTNO"));
        printf("DEPTNAME: %s\n", RDB_tuple_get_string(tplp, "DEPTNAME"));
        printf("XDEPTNO: %d\n", (int)RDB_tuple_get_int(tplp, "XDEPTNO"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/

    RDB_destroy_obj(&array, ecp);
    RDB_commit(ecp, &tx);
    return RDB_OK;

error:
    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = print_deptsx_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
