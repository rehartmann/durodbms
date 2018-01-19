#include <rel/rdb.h>
#include <rec/envimpl.h>
#include <db.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

RDB_seq_item depseqitv[] = { { "DEPTNO", RDB_TRUE } };

static int
print_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;
    RDB_int i;

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 1, depseqitv, 0, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("DEPTNO: %d\n", (int) RDB_tuple_get_int(tplp, "DEPTNO"));
        printf("COUNT_EMPS: %d\n",
               (int) RDB_tuple_get_int(tplp, "COUNT_EMPS"));
        printf("SUM_SALARY: %f\n",
               (double)RDB_tuple_get_float(tplp, "SUM_SALARY"));
        printf("AVG_SALARY: %f\n",
               (double)RDB_tuple_get_float(tplp, "AVG_SALARY"));
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
check_contains(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_bool b;

    RDB_init_obj(&tpl);
    
    RDB_tuple_set_int(&tpl, "DEPTNO", 2, ecp);
    RDB_tuple_set_int(&tpl, "COUNT_EMPS", 2, ecp);
    RDB_tuple_set_float(&tpl, "SUM_SALARY", 8100.0, ecp);
    RDB_tuple_set_float(&tpl, "AVG_SALARY", 4050.0, ecp);

    ret = RDB_table_contains(tbp, &tpl, ecp, txp, &b);    
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }
    assert(b);

    RDB_tuple_set_float(&tpl, "SUM_SALARY", 4100, ecp);
    ret = RDB_table_contains(tbp, &tpl, ecp, txp, &b);    
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }
    assert(!b);

    return RDB_OK;
}

int
test_summarize(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_expression *exp, *texp, *argp;
    RDB_object *tbp, *tbp2, *vtbp, *untbp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    tbp2 = RDB_get_table("EMPS2", ecp, &tx);
    if (tbp2 == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    exp = RDB_ro_op("union", ecp);
    assert(exp != NULL);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    argp = RDB_table_ref(tbp2, ecp);    
    assert(argp != NULL);
    RDB_add_arg(exp, argp);
    
    untbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(untbp != NULL);

    /* Give the table a name, because both arguments of SUMMARIZE
     * must not share an unnamed table.
     */
    ret = RDB_set_table_name(untbp, "UTABLE", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    /*
     * Summarizing union PER { DEPTNO } ADD COUNT AS COUNT_EMPS
     * SUM(SALARY) AS SUM_SALARY, AVG(SALARY) AS AVG_SALARY
     */

    exp = RDB_ro_op("summarize", ecp);
    assert(exp != NULL);

    RDB_add_arg(exp, RDB_table_ref(untbp, ecp));

    texp = RDB_ro_op("project", ecp);
    assert(texp != NULL);

    argp = RDB_table_ref(untbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(texp, argp);

    argp = RDB_string_to_expr("DEPTNO", ecp);
    assert(argp != NULL);
    RDB_add_arg(texp, argp);

    RDB_add_arg(exp, texp);

    argp = RDB_ro_op("count", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);
    
    argp = RDB_string_to_expr("COUNT_EMPS", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);
    
    argp = RDB_ro_op("sum", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);
    
    RDB_add_arg(argp, RDB_var_ref("SALARY", ecp));
    
    argp = RDB_string_to_expr("SUM_SALARY", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    argp = RDB_ro_op("avg", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);
    
    RDB_add_arg(argp, RDB_var_ref("SALARY", ecp));
    
    argp = RDB_string_to_expr("AVG_SALARY", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);

    if (vtbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    ret = print_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 

    ret = check_contains(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 

    RDB_drop_table(vtbp, ecp, &tx);

    RDB_drop_table(untbp, ecp, &tx);

    return RDB_commit(ecp, &tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    ret = RDB_open_env("dbenv", &dsp, RDB_RECOVER);
    if (ret != 0) {
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

    ret = test_summarize(dbp, &ec);
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
