/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_keys(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_tuple tpl;
    int ret;

    RDB_init_tuple(&tpl);    

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        RDB_destroy_tuple(&tpl);
        return ret;
    }

    printf("Inserting tuple #1\n");

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret == RDB_KEY_VIOLATION) {
        printf("Error: key violation - OK\n");
    } else {
        printf("Error: %s\n", RDB_strerror(ret));
    }

    printf("Inserting tuple #2\n");

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Smith");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret == RDB_KEY_VIOLATION) {
        printf("key violation - OK\n");
    } else {
        printf("Error: %s\n", RDB_strerror(ret));
    }
    RDB_destroy_tuple(&tpl);

    printf("End of transaction\n");
    return RDB_commit(&tx);
error:
    RDB_rollback(&tx);
    RDB_destroy_tuple(&tpl);
    return ret;
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", envp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_keys(dbp);
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
