/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

static int
print_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
        printf("SAL: %f\n", (double) RDB_tuple_get_double(tplp, "SAL"));
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

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Creating EMPS1 RENAME (SALARY AS SAL, EMPNO AS EMP#)\n");

    exp = RDB_ro_op("RENAME", 5, NULL, ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
    	return RDB_ERROR;
    }

    argp = RDB_table_ref_to_expr(tbp, ecp);
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
    argp = RDB_string_to_expr("SAL", ecp);
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
    argp = RDB_string_to_expr("EMP#", ecp);
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

    printf("Printing renaming table\n");
    ret = print_table(vtbp, ecp, &tx);

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_int(&tpl, "EMP#", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Smith", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_double(&tpl, "SAL", (RDB_double)4000.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_table_contains(vtbp, &tpl, ecp, &tx, &b);
    if (ret != RDB_OK) {
        goto error;
    }
    printf("Result of RDB_table_contains(): %s\n", b ? "TRUE" : "FALSE");

    RDB_destroy_obj(&tpl, ecp);

    printf("Dropping rename\n");
    RDB_drop_table(vtbp, ecp, &tx);

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);

error:
    RDB_destroy_obj(&tpl, ecp);

    printf("Dropping rename\n");
    RDB_drop_table(vtbp, ecp, &tx);

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
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (ret != 0) {
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

    RDB_destroy_exec_context(&ec);
    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
