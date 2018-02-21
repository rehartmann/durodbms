#include <rel/rdb.h>

#include <stdio.h>
#include <assert.h>

int
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    RDB_init_exec_context(&ec);
    envp = RDB_open_env(argc <= 1 ? "dbenv" : argv[1], RDB_RECOVER, &ec);
    if (envp == NULL) {
        fprintf(stderr, "Error: %s\n",
                RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    RDB_env_set_errfile(envp, stderr);

    dbp = RDB_create_db_from_env("TEST2", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n",
                RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    dbp = RDB_create_db_from_env("TEST2", envp, &ec);
    if (dbp != NULL) {
        RDB_destroy_exec_context(&ec);
        return 1;
    }
    assert(RDB_obj_type(RDB_get_err(&ec)) == &RDB_ELEMENT_EXISTS_ERROR);

    ret = RDB_close_env(envp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n",
                RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
