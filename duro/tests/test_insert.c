/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_insert(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_object tpl;
    int ret;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS2", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_obj(&tpl);

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 4);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Taylor");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 2);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_destroy_obj(&tpl);
    return RDB_commit(&tx);

error:
    RDB_rollback(&tx);
    return ret;
}

static int
print_table(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_object *tplp;
    RDB_object array;
    RDB_int i;
    int ret;
    RDB_seq_item sqv[2] = {
        { "DEPTNO", RDB_TRUE },
        { "NAME", RDB_FALSE }
    };

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS2", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 2, sqv, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (ret = RDB_array_get(&array, i, &tplp)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double) RDB_tuple_get_rational(tplp, "SALARY"));
        printf("DEPTNO: %d\n", (int) RDB_tuple_get_int(tplp, "DEPTNO"));
    }
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_obj(&array);
    
    return RDB_commit(&tx);
error:
    RDB_destroy_obj(&array);
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
    ret = RDB_open_env("dbenv", &envp);
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

    ret = test_insert(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = print_table(dbp);
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
