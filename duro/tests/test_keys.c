/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_keys(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);    

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    printf("Inserting tuple #1\n");

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp))
            == &RDB_KEY_VIOLATION_ERROR) {
        printf("Error: key violation - OK\n");
    } else {
        printf("Wrong result of RDB_insert()\n");
    }

    printf("Inserting tuple #2\n");

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Smith", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp))
            == &RDB_KEY_VIOLATION_ERROR) {
        printf("key violation - OK\n");
    } else {
        printf("Wrong result of RDB_insert()\n");
    }
    RDB_destroy_obj(&tpl, ecp);

    printf("End of transaction\n");
    return RDB_commit(&tx);

error:
    RDB_rollback(ecp, &tx);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_ERROR;
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_keys(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);
    
    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
