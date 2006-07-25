/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

int
print_salary_view(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tmpvtbp;
    RDB_object array;
    RDB_double d1, d2;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table SALARIES\n");
    tmpvtbp = RDB_get_table("SALARIES", ecp, &tx);
    if (tmpvtbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    assert(RDB_array_length(&array, ecp) == 2);
    
    d1 = RDB_tuple_get_double(RDB_array_get(&array, 0, ecp), "SALARY");
    d2 = RDB_tuple_get_double(RDB_array_get(&array, 1, ecp), "SALARY");
    
    assert ((d1 == 4000.0 && d2 == 4100.0) || (d1 == 4100.0 && d2 == 4000.0));

    RDB_destroy_obj(&array, ecp);
    RDB_commit(ecp, &tx);
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
    RDB_object *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;
    int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1H\n");
    tmpvtbp = RDB_get_table("EMPS1H", ecp, &tx);
    if (tmpvtbp == NULL) {
        RDB_rollback(ecp, &tx);
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
        printf("SALARY: %f\n", (float)RDB_tuple_get_double(tplp, "SALARY"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    RDB_commit(ecp, &tx);
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
    RDB_object *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;
    int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1S\n");
    tmpvtbp = RDB_get_table("EMPS1S", ecp, &tx);
    if (tmpvtbp == NULL) {
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
        printf("SALARY: %f\n", (float) RDB_tuple_get_double(tplp, "SALARY"));
        b = RDB_tuple_get_bool(tplp, "HIGHSAL");
        printf("HIGHSAL: %s\n", b ? "TRUE" : "FALSE");
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    RDB_commit(ecp, &tx);
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
    RDB_object *tmpvtbp;
    RDB_object *tplp;
    RDB_object array;
    int ret;
    int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Table EMPS1S2\n");
    tmpvtbp = RDB_get_table("EMPS1S2", ecp, &tx);
    if (tmpvtbp == NULL) {
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmpvtbp, 1, &depseqit, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("DEPARTMENT: %d\n", (int) RDB_tuple_get_int(tplp, "DEPARTMENT"));
        printf("MAX_SALARY: %f\n", (float) RDB_tuple_get_double(tplp,
                "MAX_SALARY"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    RDB_commit(ecp, &tx);
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
    
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = print_salary_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = print_emp_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = print_emps_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = print_emps2_view(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
