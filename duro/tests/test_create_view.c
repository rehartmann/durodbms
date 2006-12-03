/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

char *projattrs[] = { "SALARY" };

void
create_view1(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_expression *exp, *texp, *argp;
    RDB_object *tbp, *tbp2, *vtbp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);
    tbp2 = RDB_get_table("EMPS2", ecp, &tx);
    assert(tbp2 != NULL);

    exp = RDB_ro_op("PROJECT", 2, ecp);
    assert(exp != NULL);
    texp = RDB_ro_op("UNION", 2, ecp);
    assert(texp != NULL);
    RDB_add_arg(exp, texp);
    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(texp, argp);
    argp = RDB_table_ref(tbp2, ecp);
    assert(argp != NULL);
    RDB_add_arg(texp, argp);
    argp = RDB_string_to_expr("SALARY", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);
    
    ret = RDB_set_table_name(vtbp, "SALARIES", ecp, &tx);
    assert(ret == RDB_OK);
    ret = RDB_add_table(vtbp, ecp, &tx);
    assert(ret == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

void
create_view2(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_expression *exp, *argp, *hexp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(ret == RDB_OK);

    exp = RDB_ro_op("WHERE", 2, ecp);
    assert(exp != NULL);
    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    hexp = RDB_var_ref("SALARY", ecp);
    assert(hexp != NULL);
    argp = RDB_ro_op(">", 2, ecp);
    RDB_add_arg(argp, hexp);
    hexp = RDB_double_to_expr(4000.0, ecp);
    assert(hexp != NULL);
    RDB_add_arg(argp, hexp);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);

    ret = RDB_set_table_name(vtbp, "EMPS1H", ecp, &tx);
    assert(ret == RDB_OK);
    ret = RDB_add_table(vtbp, ecp, &tx);
    assert(ret == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

void
create_view3(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_expression *hexp, *exp, *argp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    exp = RDB_ro_op("EXTEND", 3, ecp);
    assert(exp != NULL);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp); 
    hexp = RDB_ro_op(">", 2, ecp);
    assert(hexp != NULL);
    RDB_add_arg(hexp, RDB_var_ref("SALARY", ecp));
    RDB_add_arg(hexp, RDB_double_to_expr(4000.0, ecp));
    RDB_add_arg(exp, hexp);
    argp = RDB_string_to_expr("HIGHSAL", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);
    
    ret = RDB_set_table_name(vtbp, "EMPS1S", ecp, &tx);
    assert(ret == RDB_OK);
    ret = RDB_add_table(vtbp, ecp, &tx);
    assert(ret == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

void
create_view4(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_expression *exp, *sexp, *texp, *argp;
    RDB_object *tbp, *vtbp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    /*
     * Creating ( SUMMARIZE EMPS1 PER ( EMPS1 { DEPTNO } )
     * ADD MAX (SALARY) AS MAX_SALARY ) RENAME DEPTNO AS DEPARTMENT
     */

    exp = RDB_ro_op("RENAME", 3, ecp);
    assert(exp != NULL);

    sexp = RDB_ro_op("SUMMARIZE", 4, ecp);
    assert(sexp != NULL);
    RDB_add_arg(exp, sexp);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(sexp, argp);

    texp = RDB_ro_op("PROJECT", 2, ecp);
    assert(texp != NULL);
    RDB_add_arg(sexp, texp);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(texp, argp);

    argp = RDB_string_to_expr("DEPTNO", ecp);
    assert(argp != NULL);
    RDB_add_arg(texp, argp);

    texp = RDB_ro_op("MAX", 1, ecp);
    assert(texp != NULL);
    RDB_add_arg(sexp, texp);

    argp = RDB_var_ref("SALARY", ecp);
    assert(argp != NULL);
    RDB_add_arg(texp, argp);

    argp = RDB_string_to_expr("MAX_SALARY", ecp);
    assert(argp != NULL);
    RDB_add_arg(sexp, argp);

    argp = RDB_string_to_expr("DEPTNO", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    argp = RDB_string_to_expr("DEPARTMENT", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);

    assert(RDB_set_table_name(vtbp, "EMPS1S2", ecp, &tx) == RDB_OK);
    assert(RDB_add_table(vtbp, ecp, &tx) == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
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

    create_view1(dbp, &ec);

    create_view2(dbp, &ec);

    create_view3(dbp, &ec);

    create_view4(dbp, &ec);

    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
