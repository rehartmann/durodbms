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
    int err;

    RDB_get_table(dbp, "EMPS1", &tbp);
    RDB_get_table(dbp, "EMPS2", &tbp2);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Creating (EMPS1 union EMPS2) { SALARY }\n");

    err = RDB_union(tbp2, tbp, &vtbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    err = RDB_project(vtbp, 1, projattrs, &vtbp);
    if (err != RDB_OK) {
        RDB_drop_table(vtbp, &tx);
        RDB_rollback(&tx);
        return err;
    }
    
    printf("Making virtual table persistent as SALARIES\n");
    err = RDB_set_table_name(vtbp, "SALARIES", &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    } 
    err = RDB_make_persistent(vtbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
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
    int err;

    RDB_get_table(dbp, "EMPS1", &tbp);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Creating EMPS1 select (SALARY > 4000)\n");

    exprp = RDB_expr_attr("SALARY", &RDB_RATIONAL);
    if (exprp == NULL)
        return RDB_NO_MEMORY;
    exprp = RDB_gt(exprp, RDB_rational_const(4000.0));
    if (exprp == NULL)
        return RDB_NO_MEMORY;

    err = RDB_select(tbp, exprp, &vtbp);
    if (err != RDB_OK) {
        RDB_drop_expr(exprp);
        RDB_rollback(&tx);
        return err;
    }
    
    printf("Making virtual table persistent as EMPS1H\n");
    err = RDB_set_table_name(vtbp, "EMPS1H", &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    } 
    err = RDB_make_persistent(vtbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
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
    int err;

    RDB_get_table(dbp, "EMPS1", &tbp);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
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
    err = RDB_extend(tbp, 1, &vattr, &vtbp);
    if (err != RDB_OK) {
        RDB_drop_expr(exprp);
        RDB_rollback(&tx);
        return err;
    }
    
    printf("Making virtual table persistent as EMPS1SQ\n");
    err = RDB_set_table_name(vtbp, "EMPS1SQ", &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    } 
    err = RDB_make_persistent(vtbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    } 

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main()
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int err;
    
    err = RDB_open_env("db", &dsp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }
    err = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }

    err = create_view1(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = create_view2(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = create_view3(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = RDB_close_env(dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    return 0;
}
