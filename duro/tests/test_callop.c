/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

RDB_type *plusargtv[] = {
    &RDB_INTEGER,
    &RDB_INTEGER
};

RDB_type *addargtv[] = {
    &RDB_INTEGER,
    &RDB_INTEGER
};

int updv[] = { 0 };

int
test_callop(RDB_database *dbp)
{
    RDB_transaction tx;
    int ret;
    RDB_object arg1, arg2;
    RDB_object retval;
    RDB_object *argv[2];

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&arg1);
    RDB_init_obj(&arg2);
    RDB_init_obj(&retval);

    RDB_obj_set_int(&arg1, 2);
    RDB_obj_set_int(&arg2, 2);
    argv[0] = &arg1;
    argv[1] = &arg2;

    printf("Calling PLUS\n");
    ret = RDB_call_ro_op("PLUS", 2, argv, &retval, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }

    printf("Result value is %d\n", RDB_obj_int(&retval));

    printf("Calling ADD\n");
    ret = RDB_call_update_op("ADD", 2, argv, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }

    printf("Value of arg #1 is %d\n", RDB_obj_int(&arg1));

    return RDB_commit(&tx);
error:
    RDB_destroy_obj(&arg1);
    RDB_destroy_obj(&arg2);
    RDB_destroy_obj(&retval);

    RDB_rollback(&tx);
    return ret;
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("db", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    ret = RDB_get_db_from_env("TEST", envp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_callop(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
