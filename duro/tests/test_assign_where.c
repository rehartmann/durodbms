/* $Id$ */

#include <rel/rdb.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

void
test_assign_select(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_expression *exp, *argp;
    RDB_object tpl;
    RDB_object *tplp;
    RDB_object array;
    RDB_ma_insert mains;
    RDB_seq_item seq;
    int i;

    assert(RDB_begin_tx(ecp, &tx, dbp, NULL) == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    exp = RDB_ro_op("where", ecp);
    assert(exp != NULL);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    argp = RDB_eq(RDB_var_ref("DEPTNO", ecp), RDB_int_to_expr(1, ecp),
            ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);

    RDB_init_obj(&tpl);

    assert(RDB_tuple_set_int(&tpl, "EMPNO", 4, ecp) == RDB_OK);
    assert(RDB_tuple_set_string(&tpl, "NAME", "Miller", ecp) == RDB_OK);
    assert(RDB_tuple_set_float(&tpl, "SALARY", (RDB_float)5000.0, ecp) == RDB_OK);
    assert(RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp) == RDB_OK);

    mains.tbp = vtbp;
    mains.objp = &tpl;

    assert(RDB_multi_assign(1, &mains, 0, NULL, 0, NULL, 0, NULL, 0, NULL,
            ecp, &tx) != (RDB_int)RDB_ERROR);

    assert(RDB_commit(ecp, &tx) == RDB_OK);

    /* Try insert with Tx no longer active */
    assert(RDB_tuple_set_int(&tpl, "EMPNO", 5, ecp) == RDB_OK);
    assert(RDB_tuple_set_string(&tpl, "NAME", "Webb", ecp) == RDB_OK);
    assert(RDB_multi_assign(1, &mains, 0, NULL, 0, NULL, 0, NULL, 0, NULL,
            ecp, &tx) == (RDB_int)RDB_ERROR);
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NO_RUNNING_TX_ERROR);

    assert(RDB_begin_tx(ecp, &tx, dbp, NULL) == RDB_OK);

    RDB_init_obj(&array);

    seq.attrname = "EMPNO";
    seq.asc = RDB_TRUE;

    assert(RDB_table_to_array(&array, vtbp, 1, &seq, 0, ecp, &tx) == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double)RDB_tuple_get_float(tplp, "SALARY"));
    }

    RDB_destroy_obj(&array, ecp);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    RDB_exec_context ec;
    
    assert(RDB_open_env("dbenv", &envp, RDB_RECOVER) == RDB_OK);

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    assert(dbp != NULL);

    test_assign_select(dbp, &ec);
    RDB_destroy_exec_context(&ec);

    assert(RDB_close_env(envp) == RDB_OK);

    return 0;
}
