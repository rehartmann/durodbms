/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

static int
print_table(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;
    RDB_int i;

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMP#: %d\n", (int) RDB_tuple_get_int(tplp, "EMP#"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SAL: %f\n", (double) RDB_tuple_get_rational(tplp, "SAL"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/
    RDB_destroy_obj(&array, ecp);
    
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    
    return RDB_ERROR;
}

RDB_renaming renv[] = {
    { "SALARY", "SAL" },
    { "EMPNO", "EMP#" }
};

int
test_rename(RDB_database *dbp, RDB_exec_context *ecp)
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

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating EMPS1 RENAME (SALARY AS SAL, EMPNO AS EMP#)\n");

    vtbp = RDB_rename(tbp, 2, renv, ecp);
    if (vtbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    printf("Printing renaming table\n");
    ret = print_table(vtbp, ecp, &tx);

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_int(&tpl, "EMP#", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Smith", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SAL", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_table_contains(vtbp, &tpl, ecp, &tx);
    printf("Result of RDB_table_contains(): %d %s\n", ret, RDB_strerror(ret));

    if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);

    printf("Dropping rename\n");
    RDB_drop_table(vtbp, ecp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);

error:
    RDB_destroy_obj(&tpl, ecp);

    printf("Dropping rename\n");
    RDB_drop_table(vtbp, ecp, &tx);

    RDB_rollback(&tx);
    return RDB_ERROR;
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = test_rename(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
