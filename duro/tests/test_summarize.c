/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

static int
print_table(RDB_table *tbp, RDB_transaction *txp)
{
    int err;
    RDB_tuple tpl;
    RDB_array array;
    RDB_int i;

    RDB_init_array(&array);

    err = RDB_table_to_array(tbp, &array, 0, NULL, txp);
    if (err != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("DEPTNO: %d\n", RDB_tuple_get_int(&tpl, "DEPTNO"));
        printf("COUNT_EMPS: %d\n",
               RDB_tuple_get_int(&tpl, "COUNT_EMPS"));
        printf("SUM_SALARY: %f\n",
               (double)RDB_tuple_get_rational(&tpl, "SUM_SALARY"));
        printf("AVG_SALARY: %f\n",
               (double)RDB_tuple_get_rational(&tpl, "AVG_SALARY"));
    }
    RDB_destroy_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    
    return RDB_OK;
error:
    RDB_destroy_array(&array);
    
    return err;
}

static int
check_contains(RDB_table *tbp, RDB_transaction *txp)
{
    int err;
    RDB_tuple tpl;

    RDB_init_tuple(&tpl);
    
    RDB_tuple_set_int(&tpl, "DEPTNO", 2);
    RDB_tuple_set_int(&tpl, "COUNT_EMPS", 2);
    RDB_tuple_set_rational(&tpl, "SUM_SALARY", 8100);
    RDB_tuple_set_rational(&tpl, "AVG_SALARY", 4050);

    printf("Calling RDB_table_contains()...");
    err = RDB_table_contains(tbp, &tpl, txp);
    
    if (err == RDB_OK) {
        puts("Yes - OK");
    } else if (err == RDB_NOT_FOUND) {
        puts("Not found");
    } else {
        puts(RDB_strerror(err));
        return err;
    }

    RDB_tuple_set_rational(&tpl, "SUM_SALARY", 4100);
    printf("Calling RDB_table_contains()...");
    err = RDB_table_contains(tbp, &tpl, txp);
    
    if (err == RDB_OK) {
        puts("Yes");
    } else if (err == RDB_NOT_FOUND) {
        puts("Not found - OK");
    } else {
        puts(RDB_strerror(err));
        return err;
    }

    return RDB_OK;
}

static char *projattr = "DEPTNO";

int
test_summarize(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *tbp2, *vtbp, *projtbp;
    int err;
    RDB_summarize_add addv[3];

    RDB_get_table(dbp, "EMPS1", &tbp);
    RDB_get_table(dbp, "EMPS2", &tbp2);

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Creating EMPS1 union EMPS2\n");

    err = RDB_union(tbp2, tbp, &vtbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Summarizing union PER { DEPTNO } ADD COUNT AS COUNT_EMPS,\n");
    printf("    SUM(SALARY) AS SUM_SALARY, AVG(SALARY) AS AVG_SALARY\n");

    err = RDB_project(vtbp, 1, &projattr, &projtbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    addv[0].op = RDB_COUNT;
    addv[0].name = "COUNT_EMPS";

    addv[1].op = RDB_SUM;
    addv[1].exp = RDB_expr_attr("SALARY", &RDB_RATIONAL);
    addv[1].name = "SUM_SALARY";

    addv[2].op = RDB_AVG;
    addv[2].exp = RDB_expr_attr("SALARY", &RDB_RATIONAL);
    addv[2].name = "AVG_SALARY";

    err = RDB_summarize(vtbp, projtbp, 3, addv, &vtbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Printing table\n");
    
    err = print_table(vtbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    } 

    err = check_contains(vtbp, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    } 

    printf("Dropping summarize\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main()
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int err;
    
    printf("Opening environment\n");
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

    err = test_summarize(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }
    
    printf ("Closing environment\n");
    err = RDB_close_env(dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    return 0;
}
