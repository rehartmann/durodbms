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
    int err;

    RDB_get_table(dbp, "EMPS1", &tbp);

    RDB_init_tuple(&tpl);    

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        return err;
    }

    printf("Inserting tuple #1\n");

    err = RDB_tuple_set_int(&tpl, "EMPNO", 1);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&tpl, "NAME", "Johnson");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(tbp, &tpl, &tx);
    if (err == RDB_KEY_VIOLATION) {
        printf("Error: key violation - OK\n");
    } else {
        printf("Error: %s\n", RDB_strerror(err));
    }

    printf("Inserting tuple #2\n");

    err = RDB_tuple_set_int(&tpl, "EMPNO", 3);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&tpl, "NAME", "Smith");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(tbp, &tpl, &tx);
    if (err == RDB_KEY_VIOLATION) {
        printf("key violation - OK\n");
    } else {
        printf("Error: %s\n", RDB_strerror(err));
    }
    RDB_destroy_tuple(&tpl);

    printf("End of transaction\n");
    return RDB_commit(&tx);
error:
    RDB_rollback(&tx);
    RDB_destroy_tuple(&tpl);
    return err;
}

int
main()
{
    RDB_environment *envp;
    RDB_database *dbp;
    int err;
    
    printf("Opening environment\n");
    err = RDB_open_env("db", &envp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }
    err = RDB_get_db_from_env("TEST", envp, &dbp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }

    err = test_keys(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }
    
    printf ("Closing environment\n");
    err = RDB_close_env(envp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    return 0;
}
