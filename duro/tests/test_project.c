/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

static int
print_table1(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
        printf("SALARY: %f\n", (float)RDB_tuple_get_double(tplp, "SALARY"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    
    return RDB_ERROR;
}

static int
print_table2(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
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
    RDB_expression *exp, *argp;
    RDB_object *tbp, *vtbp;
    RDB_object tpl;
    int ret;
    RDB_bool b;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS2", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    exp = RDB_ro_op("PROJECT", ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_string_to_expr("SALARY", ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);    

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    ret = print_table1(vtbp, ecp, &tx);

    RDB_init_obj(&tpl);
    RDB_tuple_set_double(&tpl, "SALARY", (RDB_double)4000.0, ecp);
    assert(RDB_table_contains(vtbp, &tpl, ecp, &tx, &b) == RDB_OK);

    assert(b);

    RDB_tuple_set_double(&tpl, "SALARY", (RDB_double)4400.0, ecp);
    assert(RDB_table_contains(vtbp, &tpl, ecp, &tx, &b) == RDB_OK);
    assert(!b);

    RDB_drop_table(vtbp, ecp, &tx);

    exp = RDB_ro_op("PROJECT", ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_string_to_expr("EMPNO", ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);    

    argp = RDB_string_to_expr("NAME", ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);    

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    ret = print_table2(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 

    RDB_drop_table(vtbp, ecp, &tx);

    /* Test if rollback works after projection with keyloss */
    return RDB_rollback(ecp, &tx);
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;

    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = test_project(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
