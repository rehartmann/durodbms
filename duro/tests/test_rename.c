#include <rel/rdb.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

static int
print_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
        printf("EMP_NO: %d\n", (int) RDB_tuple_get_int(tplp, "EMP_NO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SAL: %f\n", (double) RDB_tuple_get_float(tplp, "SAL"));
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
test_rename(RDB_database *dbp, RDB_exec_context *ecp)
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

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    /* Creating EMPS1 RENAME (SALARY AS SAL, EMPNO AS EMP_NO) */

    exp = RDB_ro_op("rename", ecp);
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
    argp = RDB_string_to_expr("SAL", ecp);
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
    argp = RDB_string_to_expr("EMP_NO", ecp);
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

    ret = print_table(vtbp, ecp, &tx);

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_int(&tpl, "EMP_NO", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Smith", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_float(&tpl, "SAL", (RDB_float) 4000.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_table_contains(vtbp, &tpl, ecp, &tx, &b);
    if (ret != RDB_OK) {
        goto error;
    }
    assert(b);

    RDB_destroy_obj(&tpl, ecp);

    RDB_drop_table(vtbp, ecp, &tx);

    return RDB_commit(ecp, &tx);

error:
    RDB_destroy_obj(&tpl, ecp);

    RDB_drop_table(vtbp, ecp, &tx);

    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

int
main(int argc, char *argv[])
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    RDB_init_exec_context(&ec);
    dsp = RDB_open_env(argc <= 1 ? "dbenv" : argv[1], RDB_RECOVER, &ec);
    if (dsp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = test_rename(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = RDB_close_env(dsp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
