#include <rel/rdb.h>
#include <rec/envimpl.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

int
print_deptsx_view(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tmpvtbp = RDB_get_table("DEPTSX", ecp, &tx);
    if (tmpvtbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 0, NULL, 0, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    tplp = RDB_array_get(&array, 0, ecp);
    assert(RDB_tuple_get_int(tplp, "DEPTNO") == 2);
    assert(strcmp(RDB_tuple_get_string(tplp, "DEPTNAME"), "Dept. II") == 0);
    assert(RDB_tuple_get_int(tplp, "XDEPTNO") == 102);

    tplp = RDB_array_get(&array, 1, ecp);
    assert(RDB_tuple_get_int(tplp, "DEPTNO") == 1);
    assert(strcmp(RDB_tuple_get_string(tplp, "DEPTNAME"), "Dept. I") == 0);
    assert(RDB_tuple_get_int(tplp, "XDEPTNO") == 101);

    assert(RDB_array_get(&array, 2, ecp) == NULL);
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);
 
    RDB_clear_err(ecp);

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
    
    ret = RDB_open_env("dbenv", &dsp, RDB_RECOVER);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = print_deptsx_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
