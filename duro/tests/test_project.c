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

    ret = RDB_table_to_array(&array, tbp, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("SALARY: %f\n", (float)RDB_tuple_get_float(tplp, "SALARY"));
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

    ret = RDB_table_to_array(&array, tbp, 0, NULL, 0, ecp, txp);
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

    exp = RDB_ro_op("project", ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_string_to_expr("SALARY", ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);    

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_del_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    ret = print_table1(vtbp, ecp, &tx);

    RDB_init_obj(&tpl);
    RDB_tuple_set_float(&tpl, "SALARY", (RDB_float)4000.0, ecp);
    assert(RDB_table_contains(vtbp, &tpl, ecp, &tx, &b) == RDB_OK);

    assert(b);

    RDB_tuple_set_float(&tpl, "SALARY", (RDB_float)4400.0, ecp);
    assert(RDB_table_contains(vtbp, &tpl, ecp, &tx, &b) == RDB_OK);
    assert(!b);

    RDB_drop_table(vtbp, ecp, &tx);

    exp = RDB_ro_op("project", ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_string_to_expr("EMPNO", ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);    

    argp = RDB_string_to_expr("NAME", ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);    

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_del_expr(exp, ecp);
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
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    envp = RDB_open_env(argc <= 1 ? "dbenv" : argv[1], RDB_RECOVER, &ec);
    if (envp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

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

    ret = RDB_close_env(envp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
