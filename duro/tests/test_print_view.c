/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
print_salary_view(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_tuple tpl;
    RDB_array array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table SALARIES\n");
    ret = RDB_get_table("SALARIES", &tx, &tmpvtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return ret;
    }

    RDB_init_array(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 0, NULL, &tx);
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
    RDB_commit(&tx);
    return RDB_OK;
error:
    RDB_rollback(&tx);
    return ret;
}

int
print_emp_view(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_tuple tpl;
    RDB_array array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1H\n");
    ret = RDB_get_table("EMPS1H", &tx, &tmpvtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return ret;
    }

    RDB_init_array(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    RDB_commit(&tx);
    return RDB_OK;
error:
    RDB_rollback(&tx);
    return ret;
}

int
print_emps_view(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_tuple tpl;
    RDB_array array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1S\n");
    ret = RDB_get_table("EMPS1S", &tx, &tmpvtbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    RDB_init_array(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        RDB_bool b;
    
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (float) RDB_tuple_get_rational(&tpl, "SALARY"));
        b = RDB_tuple_get_bool(&tpl, "HIGHSAL");
        printf("HIGHSAL: %s\n", b ? "TRUE" : "FALSE");
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    RDB_commit(&tx);
    return RDB_OK;
error:
    RDB_rollback(&tx);
    return ret;
}

int
print_emps2_view(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_tuple tpl;
    RDB_array array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1S2\n");
    ret = RDB_get_table("EMPS1S2", &tx, &tmpvtbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    RDB_init_array(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_init_tuple(&tpl);    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("DEPARTMENT: %d\n", (int) RDB_tuple_get_int(&tpl, "DEPARTMENT"));
        printf("MAX_SALARY: %f\n", (float) RDB_tuple_get_rational(&tpl, "MAX_SALARY"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    RDB_commit(&tx);
    return RDB_OK;
error:
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
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = print_salary_view(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = print_emp_view(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = print_emps_view(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = print_emps2_view(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
