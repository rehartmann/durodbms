/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *projattrs1[] = { "SALARY" };

char *projattrs2[] = { "EMPNO", "NAME" };

static int
print_table1(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_tuple tpl;
    RDB_array array;
    RDB_int i;

    RDB_init_array(&array);

    ret = RDB_table_to_array(tbp, &array, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    
    return RDB_OK;
error:
    RDB_destroy_array(&array);
    
    return ret;
}

static int
print_table2(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_tuple tpl;
    RDB_array array;
    RDB_int i;

    RDB_init_array(&array);

    ret = RDB_table_to_array(tbp, &array, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    
    return RDB_OK;
error:
    RDB_destroy_array(&array);
    
    return ret;
}

int
test_project(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_tuple tpl;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS2", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating projection (SALARY)\n");

    ret = RDB_project(tbp, 1, projattrs1, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Converting projection table to array\n");
    ret = print_table1(vtbp, &tx);

    RDB_init_tuple(&tpl);
    RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    ret = RDB_table_contains(vtbp, &tpl, &tx);
    printf("Projection contains SALARY(4000.0): %d %s\n", ret, RDB_strerror(ret));

    if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4400.0);
    ret = RDB_table_contains(vtbp, &tpl, &tx);
    printf("Projection contains SALARY(4400.0): %d %s\n", ret, RDB_strerror(ret));

    if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Dropping projection\n");
    RDB_drop_table(vtbp, &tx);

    printf("Creating projection (EMPNO,NAME)\n");

    ret = RDB_project(tbp, 2, projattrs2, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Converting projection table to array\n");
    ret = print_table2(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    printf("Dropping projection\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");

    /* Test if rollback works after projection with keyloss */
    return RDB_rollback(&tx);
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

    ret = test_project(dbp);
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
