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
    ret = RDB_make_persistent(vtbp, &tx);
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
    RDB_expression *exprp;
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

    printf("Creating EMPS1 select (SALARY > 4000)\n");

    exprp = RDB_expr_attr("SALARY", &RDB_RATIONAL);
    if (exprp == NULL)
        return RDB_NO_MEMORY;
    exprp = RDB_gt(exprp, RDB_rational_const(4000.0));
    if (exprp == NULL)
        return RDB_NO_MEMORY;

    ret = RDB_select(tbp, exprp, &vtbp);
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
    ret = RDB_make_persistent(vtbp, &tx);
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

    printf("Creating extend EMPS1 add (SALARY > 4000 AS HIGHSAL)\n");

    exprp = RDB_expr_attr("SALARY", &RDB_RATIONAL);
    if (exprp == NULL)
        return RDB_NO_MEMORY;
    exprp = RDB_gt(exprp, RDB_rational_const(4000.0));
    if (exprp == NULL)
        return RDB_NO_MEMORY;

    vattr.name = "HIGHSAL";
    vattr.exp = exprp;
    ret = RDB_extend(tbp, 1, &vattr, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        RDB_rollback(&tx);
        return ret;
    }
    
    printf("Making virtual table persistent as EMPS1SQ\n");
    ret = RDB_set_table_name(vtbp, "EMPS1SQ", &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 
    ret = RDB_make_persistent(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main()
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    ret = RDB_open_env("db", &dsp);
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

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
