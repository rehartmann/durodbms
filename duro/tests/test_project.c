/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *projattrs1[] = { "SALARY" };

char *projattrs2[] = { "EMPNO", "NAME" };

static int
print_table1(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(tplp, "SALARY"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    
    return RDB_ERROR;
}

static int
print_table2(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    
    return RDB_ERROR;
}

int
test_project(RDB_database *dbp, RDB_exec_context *ecp)
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

    tbp = RDB_get_table("EMPS2", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    printf("Creating projection (SALARY)\n");

    vtbp = RDB_project(tbp, 1, projattrs1, ecp);
    if (vtbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    printf("Converting projection table to array\n");
    ret = print_table1(vtbp, ecp, &tx);

    RDB_init_obj(&tpl);
    RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    ret = RDB_table_contains(vtbp, &tpl, ecp, &tx);
    printf("Projection contains SALARY(4000.0): %d %s\n", ret, RDB_strerror(ret));

    if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4400.0);
    ret = RDB_table_contains(vtbp, &tpl, ecp, &tx);
    printf("Projection contains SALARY(4400.0): %d %s\n", ret, RDB_strerror(ret));

    if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Dropping projection\n");
    RDB_drop_table(vtbp, ecp, &tx);

    printf("Creating projection (EMPNO,NAME)\n");

    vtbp = RDB_project(tbp, 2, projattrs2, ecp);
    if (vtbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    printf("Converting projection table to array\n");
    ret = print_table2(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    printf("Dropping projection\n");
    RDB_drop_table(vtbp, ecp, &tx);

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
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = test_project(dbp, &ec);
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
