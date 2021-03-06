#include <rel/rdb.h>

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
    RDB_seq_item seqit;
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

    seqit.attrname = "DEPTNO";
    seqit.asc = RDB_FALSE;
    ret = RDB_table_to_array(&array, tmpvtbp, 1, &seqit, 0, ecp, &tx);
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
main(int argc, char *argv[])
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    RDB_init_exec_context(&ec);
    dsp = RDB_open_env(argc <= 1 ? "dbenv" : argv[1], RDB_RECOVER, &ec);
    if (dsp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    dbp = RDB_get_db_from_env("TEST", dsp, &ec, NULL);
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

    ret = RDB_close_env(dsp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
