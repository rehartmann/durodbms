/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_defop(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    int ret;

    RDB_type *plusargtv[] = {
        &RDB_INTEGER,
        &RDB_INTEGER
    };

    RDB_type *addargtv[] = {
        &RDB_INTEGER,
        &RDB_INTEGER
    };

    RDB_bool updv[] = { RDB_TRUE, RDB_FALSE };

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_create_ro_op("PLUS", 2, plusargtv, &RDB_INTEGER, "libplus",
            "RDBU_plus", NULL, 0, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    ret = RDB_create_update_op("ADD", 2, addargtv, updv, "libplus", "RDBU_add",
            NULL, 0, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    return RDB_commit(ecp, &tx);
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);
    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = test_defop(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
