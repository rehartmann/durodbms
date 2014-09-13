/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

RDB_seq_item empseqitv[] = { { "EMPNO", RDB_TRUE } };

static void
check_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tplp;
    RDB_object array;

    RDB_init_obj(&array);

    assert(RDB_table_to_array(&array, tbp, 1, empseqitv, 0, ecp, txp) == RDB_OK);

    assert(RDB_array_length(&array, ecp) == 2);

    tplp = RDB_array_get(&array, 0, ecp);
    assert(RDB_tuple_get_int(tplp, "EMPNO") == 1);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Smith") == 0);
    assert(RDB_tuple_get_int(tplp, "DEPTNO") == 1);
    assert(strcmp(RDB_tuple_get_string(tplp, "DEPTNAME"), "Dept. I") == 0);

    tplp = RDB_array_get(&array, 1, ecp);
    assert(RDB_tuple_get_int(tplp, "EMPNO") == 2);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Jones") == 0);
    assert(RDB_tuple_get_int(tplp, "DEPTNO") == 2);
    assert(strcmp(RDB_tuple_get_string(tplp, "DEPTNAME"), "Dept. II") == 0);    

    assert(RDB_destroy_obj(&array, ecp) == RDB_OK);
}

int
test_join(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_expression *exp, *argp;
    RDB_object *tbp1, *tbp2, *vtbp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp1 = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp1 == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    tbp2 = RDB_get_table("depts", ecp, &tx);
    if (tbp2 == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    exp = RDB_ro_op("join", ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    argp = RDB_table_ref(tbp1, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_table_ref(tbp2, ecp);
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
    
    check_table(vtbp, ecp, &tx);

    assert(RDB_drop_table(vtbp, ecp, &tx) == RDB_OK);

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
        return 1;
    }

    ret = test_join(dbp, &ec);
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
