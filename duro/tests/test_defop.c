#include <rel/rdb.h>
#include <bdbrec/bdbenv.h>
#include <db.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#define SHLIB "plus"

void
test_defop(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    int ret;

    RDB_parameter plusparamv[] = {
        { &RDB_INTEGER },
        { &RDB_INTEGER }
    };

    RDB_parameter addparamv[] = {
        { &RDB_INTEGER, RDB_TRUE },
        { &RDB_INTEGER, RDB_TRUE }
    };

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    ret = RDB_create_ro_op("plus", 2, plusparamv, &RDB_INTEGER, SHLIB,
            "RDBU_plus", NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_create_update_op("add", 2, addparamv, SHLIB, "RDBU_add",
            NULL, ecp, &tx);
    assert(ret == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    RDB_init_exec_context(&ec);
    envp = RDB_open_env("dbenv", RDB_RECOVER, &ec);
    if (envp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    test_defop(dbp, &ec);

    ret = RDB_close_env(envp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
