/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

char *projattrs[] = { "SALARY" };

int
create_view1(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_expression *exp, *texp, *argp;
    RDB_object *tbp, *tbp2, *vtbp;
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
        return ret;
    }

    exp = RDB_ro_op("PROJECT", 2, NULL, ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    texp = RDB_ro_op("UNION", 2, NULL, ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, texp);
    argp = RDB_table_ref_to_expr(tbp, ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(texp, argp);
    argp = RDB_table_ref_to_expr(tbp2, ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(texp, argp);
    argp = RDB_string_to_expr("SALARY", ecp);
    if (exp == NULL) {
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
    
    ret = RDB_set_table_name(vtbp, "SALARIES", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 
    ret = RDB_add_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 

    return RDB_commit(ecp, &tx);
}

int
create_view2(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_expression *exp, *argp, *hexprp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    exp = RDB_ro_op("WHERE", 2, NULL, ecp);
    if (exp == NULL)
        return RDB_ERROR;
    argp = RDB_table_ref_to_expr(tbp, ecp);
    if (argp == NULL)
        return RDB_ERROR;
    RDB_add_arg(exp, argp);

    hexprp = RDB_expr_var("SALARY", ecp);
    if (hexprp == NULL)
        return RDB_ERROR;
    argp = RDB_ro_op_va(">", ecp, hexprp, RDB_double_to_expr(4000.0, ecp),
            (RDB_expression *) NULL);
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    ret = RDB_set_table_name(vtbp, "EMPS1H", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 
    ret = RDB_add_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 

    return RDB_commit(ecp, &tx);
}

int
create_view3(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_expression *exprp, *exp, *argp;
    int ret;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    exp = RDB_ro_op("EXTEND", 3, NULL, ecp);
    if (exp == NULL)
        return RDB_ERROR;

    argp = RDB_table_ref_to_expr(tbp, ecp);
    if (argp == NULL)
        return RDB_ERROR;
    RDB_add_arg(exp, argp); 
    exprp = RDB_expr_var("SALARY", ecp);
    if (exprp == NULL)
        return RDB_ERROR;
    exprp = RDB_ro_op_va(">", ecp, exprp, RDB_double_to_expr(4000.0, ecp),
            (RDB_expression *) NULL);
    if (exprp == NULL)
        return RDB_ERROR;
    RDB_add_arg(exp, exprp);
    argp = RDB_string_to_expr("HIGHSAL", ecp);
    if (argp == NULL)
        return RDB_ERROR;
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    
    ret = RDB_set_table_name(vtbp, "EMPS1S", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 
    ret = RDB_add_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 

    return RDB_commit(ecp, &tx);
}

int
create_view4(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_expression *exp, *sexp, *texp, *argp;
    RDB_object *tbp, *vtbp;
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

    /*
     * Creating ( SUMMARIZE EMPS1 PER ( EMPS1 { DEPTNO } )
     * ADD MAX (SALARY) AS MAX_SALARY ) RENAME DEPTNO AS DEPARTMENT
     */

    exp = RDB_ro_op("RENAME", 3, NULL, ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    sexp = RDB_ro_op("SUMMARIZE", 4, NULL, ecp);
    if (sexp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, sexp);

    argp = RDB_table_ref_to_expr(tbp, ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(sexp, argp);

    texp = RDB_ro_op("PROJECT", 2, NULL, ecp);
    if (texp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(sexp, texp);

    argp = RDB_table_ref_to_expr(tbp, ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(texp, argp);

    argp = RDB_string_to_expr("DEPTNO", ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(texp, argp);

    texp = RDB_ro_op("MAX", 1, NULL, ecp);
    if (texp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(sexp, texp);

    argp = RDB_expr_var("SALARY", ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(texp, argp);

    argp = RDB_string_to_expr("MAX_SALARY", ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(sexp, argp);

    argp = RDB_string_to_expr("DEPTNO", ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_string_to_expr("DEPARTMENT", ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(vtbp != NULL);

    assert(RDB_set_table_name(vtbp, "EMPS1S2", ecp, &tx) == RDB_OK);
    assert(RDB_add_table(vtbp, ecp, &tx) == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
    return RDB_OK;
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

    ret = create_view1(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = create_view2(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = create_view3(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = create_view4(dbp, &ec);
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
