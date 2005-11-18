/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *projattrs[] = { "SALARY" };

int
create_view1(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp, *tbp2, *vtbp;
    int ret;

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
    tbp2 = RDB_get_table("EMPS2", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    printf("Creating (EMPS1 union EMPS2) { SALARY }\n");

    vtbp = RDB_union(tbp2, tbp, ecp);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    vtbp = RDB_project(vtbp, 1, projattrs, ecp);
    if (vtbp == NULL) {
        RDB_drop_table(vtbp, ecp, &tx);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    
    printf("Making virtual table persistent as SALARIES\n");
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

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);
}

int
create_view2(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_expression *exprp, *hexprp;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Creating EMPS1 WHERE (SALARY > 4000)\n");

    hexprp = RDB_expr_attr("SALARY", ecp);
    if (hexprp == NULL)
        return RDB_ERROR;
    exprp = RDB_ro_op_va(">", ecp, hexprp, RDB_rational_to_expr(4000.0, ecp),
            (RDB_expression *) NULL);
    if (exprp == NULL)
        return RDB_ERROR;

    vtbp = RDB_select(tbp, exprp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exprp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    
    printf("Making virtual table persistent as EMPS1H\n");
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

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);
}

int
create_view3(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_expression *exprp;
    RDB_virtual_attr vattr;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    printf("Creating EXTEND EMPS1 ADD (SALARY > 4000 AS HIGHSAL)\n");

    exprp = RDB_expr_attr("SALARY", ecp);
    if (exprp == NULL)
        return RDB_ERROR;
    exprp = RDB_ro_op_va(">", ecp, exprp, RDB_rational_to_expr(4000.0, ecp),
            (RDB_expression *) NULL);
    if (exprp == NULL)
        return RDB_ERROR;

    vattr.name = "HIGHSAL";
    vattr.exp = exprp;
    vtbp = RDB_extend(tbp, 1, &vattr, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exprp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    
    printf("Making virtual table persistent as EMPS1S\n");
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

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);
}

static char *projattr = "DEPTNO";

int
create_view4(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp, *projtbp;
    int ret;
    RDB_summarize_add add;
    RDB_renaming ren;

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

    printf("Creating ( SUMMARIZE EMPS1 PER ( EMPS1 { DEPTNO } )"
           " ADD MAX (SALARY) AS MAX_SALARY ) RENAME DEPTNO AS DEPARTMENT\n");

    projtbp = RDB_project(tbp, 1, &projattr, ecp);
    if (projtbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    add.op = RDB_MAX;
    add.exp = RDB_expr_attr("SALARY", ecp);
    add.name = "MAX_SALARY";

    vtbp = RDB_summarize(tbp, projtbp, 1, &add, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    ren.from = "DEPTNO";
    ren.to = "DEPARTMENT";

    vtbp = RDB_rename(vtbp, 1, &ren, ecp);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    printf("Making virtual table persistent as EMPS1S2\n");

    ret = RDB_set_table_name(vtbp, "EMPS1S2", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 
    ret = RDB_add_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);
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
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = create_view1(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = create_view2(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = create_view3(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = create_view4(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
