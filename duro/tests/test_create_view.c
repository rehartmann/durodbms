/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *projattrs[] = { "SALARY" };

int
create_view1(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *tbp2, *vtbp;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }
    ret = RDB_get_table("EMPS2", &tx, &tbp2);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating (EMPS1 union EMPS2) { SALARY }\n");

    ret = RDB_union(tbp2, tbp, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_project(vtbp, 1, projattrs, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, &tx);
        RDB_rollback(&tx);
        return ret;
    }
    
    printf("Making virtual table persistent as SALARIES\n");
    ret = RDB_set_table_name(vtbp, "SALARIES", &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 
    ret = RDB_add_table(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
create_view2(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_expression *exprp, *hexprp;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating EMPS1 WHERE (SALARY > 4000)\n");

    hexprp = RDB_expr_attr("SALARY");
    if (hexprp == NULL)
        return RDB_NO_MEMORY;
    ret = RDB_ro_op_2(">", hexprp, RDB_rational_to_expr(4000.0), &tx, &exprp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_select(tbp, exprp, &tx, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        RDB_rollback(&tx);
        return ret;
    }
    
    printf("Making virtual table persistent as EMPS1H\n");
    ret = RDB_set_table_name(vtbp, "EMPS1H", &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 
    ret = RDB_add_table(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
create_view3(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    RDB_expression *exprp;
    RDB_virtual_attr vattr;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating EXTEND EMPS1 ADD (SALARY > 4000 AS HIGHSAL)\n");

    exprp = RDB_expr_attr("SALARY");
    if (exprp == NULL)
        return RDB_NO_MEMORY;
    ret = RDB_ro_op_2(">", exprp, RDB_rational_to_expr(4000.0), &tx, &exprp);
    if (ret != RDB_OK)
        return ret;

    vattr.name = "HIGHSAL";
    vattr.exp = exprp;
    ret = RDB_extend(tbp, 1, &vattr, &tx, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        RDB_rollback(&tx);
        return ret;
    }
    
    printf("Making virtual table persistent as EMPS1S\n");
    ret = RDB_set_table_name(vtbp, "EMPS1S", &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 
    ret = RDB_add_table(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

static char *projattr = "DEPTNO";

int
create_view4(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp, *projtbp;
    int ret;
    RDB_summarize_add add;
    RDB_renaming ren;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating ( SUMMARIZE EMPS1 PER ( EMPS1 { DEPTNO } )"
           " ADD MAX (SALARY) AS MAX_SALARY ) RENAME DEPTNO AS DEPARTMENT\n");

    ret = RDB_project(tbp, 1, &projattr, &projtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    add.op = RDB_MAX;
    add.exp = RDB_expr_attr("SALARY");
    add.name = "MAX_SALARY";

    ret = RDB_summarize(tbp, projtbp, 1, &add, &tx, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ren.from = "DEPTNO";
    ren.to = "DEPARTMENT";

    ret = RDB_rename(vtbp, 1, &ren, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Making virtual table persistent as EMPS1S2\n");

    ret = RDB_set_table_name(vtbp, "EMPS1S2", &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 
    ret = RDB_add_table(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = create_view1(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = create_view2(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = create_view3(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = create_view4(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
