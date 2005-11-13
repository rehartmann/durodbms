/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
print_salary_view(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table SALARIES\n");
    tmpvtbp = RDB_get_table("SALARIES", ecp, &tx);
    if (tmpvtbp == NULL) {
        RDB_rollback(ecp, &tx);
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 0, NULL, ecp, &tx);
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
    RDB_commit(&tx);
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

RDB_seq_item empseqit = { "EMPNO", RDB_TRUE };

int
print_emp_view(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1H\n");
    tmpvtbp = RDB_get_table("EMPS1H", ecp, &tx);
    if (tmpvtbp == NULL) {
        RDB_rollback(ecp, &tx);
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 1, &empseqit, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(tplp, "SALARY"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    RDB_commit(&tx);
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

int
print_emps_view(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1S\n");
    tmpvtbp = RDB_get_table("EMPS1S", ecp, &tx);
    if (tmpvtbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 1, &empseqit, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_bool b;
    
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (float) RDB_tuple_get_rational(tplp, "SALARY"));
        b = RDB_tuple_get_bool(tplp, "HIGHSAL");
        printf("HIGHSAL: %s\n", b ? "TRUE" : "FALSE");
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    RDB_commit(&tx);
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

RDB_seq_item depseqit = { "DEPARTMENT", RDB_TRUE };

int
print_emps2_view(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;
    int i;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1S2\n");
    tmpvtbp = RDB_get_table("EMPS1S2", ecp, &tx);
    if (tmpvtbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 1, &depseqit, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("DEPARTMENT: %d\n", (int) RDB_tuple_get_int(tplp, "DEPARTMENT"));
        printf("MAX_SALARY: %f\n", (float) RDB_tuple_get_rational(tplp,
                "MAX_SALARY"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    RDB_commit(&tx);
    return RDB_OK;

error:
    RDB_rollback(ecp, &tx);
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
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = print_salary_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = print_emp_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = print_emps_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = print_emps2_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
