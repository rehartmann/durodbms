/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

static int
print_table(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_tuple tpl;
    RDB_array array;
    RDB_int i;

    RDB_init_array(&array);

    ret = RDB_table_to_array(tbp, &array, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("DEPTNO: %d\n", (int) RDB_tuple_get_int(&tpl, "DEPTNO"));
        printf("COUNT_EMPS: %d\n",
               (int) RDB_tuple_get_int(&tpl, "COUNT_EMPS"));
        printf("SUM_SALARY: %f\n",
               (double)RDB_tuple_get_rational(&tpl, "SUM_SALARY"));
        printf("AVG_SALARY: %f\n",
               (double)RDB_tuple_get_rational(&tpl, "AVG_SALARY"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);
    
    return RDB_OK;
error:
    RDB_destroy_array(&array);
    
    return ret;
}

static int
check_contains(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_tuple tpl;

    RDB_init_tuple(&tpl);
    
    RDB_tuple_set_int(&tpl, "DEPTNO", 2);
    RDB_tuple_set_int(&tpl, "COUNT_EMPS", 2);
    RDB_tuple_set_rational(&tpl, "SUM_SALARY", 8100);
    RDB_tuple_set_rational(&tpl, "AVG_SALARY", 4050);

    printf("Calling RDB_table_contains()...");
    ret = RDB_table_contains(tbp, &tpl, txp);
    
    if (ret == RDB_OK) {
        puts("Yes - OK");
    } else if (ret == RDB_NOT_FOUND) {
        puts("Not found");
    } else {
        puts(RDB_strerror(ret));
        return ret;
    }

    RDB_tuple_set_rational(&tpl, "SUM_SALARY", 4100);
    printf("Calling RDB_table_contains()...");
    ret = RDB_table_contains(tbp, &tpl, txp);
    
    if (ret == RDB_OK) {
        puts("Yes");
    } else if (ret == RDB_NOT_FOUND) {
        puts("Not found - OK");
    } else {
        puts(RDB_strerror(ret));
        return ret;
    }

    return RDB_OK;
}

static char *projattr = "DEPTNO";

int
test_summarize(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *tbp2, *vtbp, *projtbp;
    int ret;
    RDB_summarize_add addv[3];

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

    printf("Creating EMPS1 union EMPS2\n");

    ret = RDB_union(tbp2, tbp, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Summarizing union PER { DEPTNO } ADD COUNT AS COUNT_EMPS,\n");
    printf("    SUM(SALARY) AS SUM_SALARY, AVG(SALARY) AS AVG_SALARY\n");

    ret = RDB_project(vtbp, 1, &projattr, &projtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    addv[0].op = RDB_COUNT;
    addv[0].name = "COUNT_EMPS";

    addv[1].op = RDB_SUM;
    addv[1].exp = RDB_expr_attr("SALARY", &RDB_RATIONAL);
    addv[1].name = "SUM_SALARY";

    addv[2].op = RDB_AVG;
    addv[2].exp = RDB_expr_attr("SALARY", &RDB_RATIONAL);
    addv[2].name = "AVG_SALARY";

    ret = RDB_summarize(vtbp, projtbp, 3, addv, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Printing table\n");
    
    ret = print_table(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    ret = check_contains(vtbp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
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
    int ret;
    
    printf("Opening environment\n");
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

    ret = test_summarize(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }
    
    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
