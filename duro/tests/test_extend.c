/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

int
print_extend(RDB_object *vtbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object array;
    RDB_object *tplp;
    int ret;
    int i;
    RDB_seq_item sq;

    RDB_init_obj(&array);

    sq.attrname = "SALARY_AFTER_TAX";
    sq.asc = RDB_TRUE;

    ret = RDB_table_to_array(&array, vtbp, 1, &sq, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (float)RDB_tuple_get_double(tplp, "SALARY"));
        printf("SALARY_AFTER_TAX: %f\n",
                (float)RDB_tuple_get_double(tplp, "SALARY_AFTER_TAX"));
        printf("NAME_LEN: %d\n", (int)RDB_tuple_get_int(tplp, "NAME_LEN"));
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
insert_extend(RDB_object *vtbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);    

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_double(&tpl, "SALARY", (RDB_double)4000.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_double(&tpl, "SALARY_AFTER_TAX",
            (RDB_double)4200.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "NAME_LEN", (RDB_int)7, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(vtbp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
        assert(errtyp == &RDB_PREDICATE_VIOLATION_ERROR);
    }
    RDB_clear_err(ecp);

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_double(&tpl, "SALARY", (RDB_double)4000.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_double(&tpl, "SALARY_AFTER_TAX",
            (RDB_double)-100.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "NAME_LEN", (RDB_int)7, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(vtbp, &tpl, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    RDB_destroy_obj(&tpl, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

int
test_extend(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_expression *exp, *texp, *mexp, *argp;
    int ret;
    RDB_object *vtbp = NULL;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        goto error;
    }

    exp = RDB_ro_op("EXTEND", 5, ecp);
    if (exp == NULL)
        goto error;

    argp = RDB_table_ref_to_expr(tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);

    mexp = RDB_ro_op("-", 2, ecp);
    assert(mexp != NULL);
    argp = RDB_expr_var("SALARY", ecp);
    assert(argp != NULL);
    RDB_add_arg(mexp, argp);
    argp = RDB_double_to_expr(4100, ecp);
    assert(argp != NULL);
    RDB_add_arg(mexp, argp);

    RDB_add_arg(exp, mexp);

    argp = RDB_string_to_expr("SALARY_AFTER_TAX", ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);

    texp = RDB_ro_op("LENGTH", 1, ecp);
    if (texp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, texp);

    argp = RDB_expr_var("NAME", ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(texp, argp);

    argp = RDB_string_to_expr("NAME_LEN", ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }

    ret = print_extend(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = insert_extend(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_drop_table(vtbp, ecp, &tx);

    /* Abort transaction, since we don't want the update to be persistent */
    return RDB_rollback(ecp, &tx);

error:
    if (vtbp != NULL)
        RDB_drop_table(vtbp, ecp, &tx);

    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
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

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    ret = test_extend(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        goto error;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;

error:
    RDB_destroy_exec_context(&ec);
    RDB_close_env(envp);
    return 2;
}
