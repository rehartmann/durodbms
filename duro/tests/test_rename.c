/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

static int
print_table(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;
    RDB_int i;

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (ret = RDB_array_get(&array, i, &tplp)) == RDB_OK; i++) {
        printf("EMP#: %d\n", (int) RDB_tuple_get_int(tplp, "EMP#"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SAL: %f\n", (double) RDB_tuple_get_rational(tplp, "SAL"));
    }
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_obj(&array);
    
    return RDB_OK;
error:
    RDB_destroy_obj(&array);
    
    return ret;
}

RDB_renaming renv[] = {
    { "SALARY", "SAL" },
    { "EMPNO", "EMP#" }
};

int
test_rename(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_object tpl;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating EMPS1 RENAME (SALARY AS SAL, EMPNO AS EMP#)\n");

    ret = RDB_rename(tbp, 2, renv, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Printing renaming table\n");
    ret = print_table(vtbp, &tx);

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_int(&tpl, "EMP#", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Smith");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SAL", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_table_contains(vtbp, &tpl, &tx);
    printf("Result of RDB_table_contains(): %d %s\n", ret, RDB_strerror(ret));

    if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_obj(&tpl);

    printf("Dropping rename\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);

error:
    RDB_destroy_obj(&tpl);

    printf("Dropping rename\n");
    RDB_drop_table(vtbp, &tx);

    RDB_rollback(&tx);
    return ret;
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_rename(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
