/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
print_salary_view(RDB_database *dbp, RDB_table *vtbp)
{
    RDB_transaction tx;
    RDB_tuple tpl;
    RDB_array array;
    int err;
    int i;

    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    RDB_init_array(&array);

    err = RDB_table_to_array(vtbp, &array, &tx);
    if (err != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_deinit_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_deinit_array(&array);
    RDB_commit(&tx);
    return RDB_OK;
error:
    RDB_rollback(&tx);
    return err;
}

int
print_emp_view(RDB_database *dbp, RDB_table *vtbp)
{
    RDB_transaction tx;
    RDB_tuple tpl;
    RDB_array array;
    int err;
    int i;

    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    RDB_init_array(&array);

    err = RDB_table_to_array(vtbp, &array, &tx);
    if (err != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(&tpl, "SALARY"));
    }
    RDB_deinit_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_deinit_array(&array);
    RDB_commit(&tx);
    return RDB_OK;
error:
    RDB_rollback(&tx);
    return err;
}

int
print_empsq_view(RDB_database *dbp, RDB_table *vtbp)
{
    RDB_transaction tx;
    RDB_tuple tpl;
    RDB_array array;
    int err;
    int i;

    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    RDB_init_array(&array);

    err = RDB_table_to_array(vtbp, &array, &tx);
    if (err != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        RDB_bool b;
    
        printf("EMPNO: %d\n", RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(&tpl, "SALARY"));
        b = RDB_tuple_get_bool(&tpl, "HIGHSAL");
        printf("HIGHSAL: %s\n", b ? "TRUE" : "FALSE");
    }
    RDB_deinit_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_deinit_array(&array);
    RDB_commit(&tx);
    return RDB_OK;
error:
    RDB_rollback(&tx);
    return err;
}

int
main()
{
    RDB_environment *dsp;
    RDB_database *dbp;
    RDB_table *viewp;
    int err;
    
    printf("Opening Environment\n");
    err = RDB_open_env("db", &dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }
    err = RDB_get_db("TEST", dsp, &dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }

    printf("Table SALARIES\n");
    err = RDB_get_table(dbp, "SALARIES", &viewp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = print_salary_view(dbp, viewp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    printf("Table EMPS1H\n");
    err = RDB_get_table(dbp, "EMPS1H", &viewp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = print_emp_view(dbp, viewp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    printf("Table EMPS1SQ\n");
    err = RDB_get_table(dbp, "EMPS1SQ", &viewp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = print_empsq_view(dbp, viewp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = RDB_close_env(dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    return 0;
}
