/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

RDB_seq_item depseqitv[] = { { "DEPTNO", RDB_TRUE } };

static int
print_table(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;
    RDB_int i;

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 1, depseqitv, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("DEPTNO: %d\n", (int) RDB_tuple_get_int(tplp, "DEPTNO"));
        printf("COUNT_EMPS: %d\n",
               (int) RDB_tuple_get_int(tplp, "COUNT_EMPS"));
        printf("SUM_SALARY: %f\n",
               (double)RDB_tuple_get_rational(tplp, "SUM_SALARY"));
        printf("AVG_SALARY: %f\n",
               (double)RDB_tuple_get_rational(tplp, "AVG_SALARY"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/
    RDB_destroy_obj(&array, ecp);
    
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    
    return RDB_ERROR;
}

static int
check_contains(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_bool b;

    RDB_init_obj(&tpl);
    
    RDB_tuple_set_int(&tpl, "DEPTNO", 2);
    RDB_tuple_set_int(&tpl, "COUNT_EMPS", 2);
    RDB_tuple_set_rational(&tpl, "SUM_SALARY", 8100);
    RDB_tuple_set_rational(&tpl, "AVG_SALARY", 4050);

    printf("Calling RDB_table_contains()...");
    ret = RDB_table_contains(tbp, &tpl, ecp, txp, &b);
    
    if (ret != RDB_OK) {
        puts(RDB_strerror(ret));
        return ret;
    }    
    if (b) {
        puts("Yes - OK");
    } else {
        puts("No");
    }

    RDB_tuple_set_rational(&tpl, "SUM_SALARY", 4100);
    printf("Calling RDB_table_contains()...");
    ret = RDB_table_contains(tbp, &tpl, ecp, txp, &b);
    
    if (ret != RDB_OK) {
        puts(RDB_strerror(ret));
        return ret;
    }
    if (b) {
        puts("Yes");
    } else {
        puts("No - OK");
    }

    return RDB_OK;
}

static char *projattr = "DEPTNO";

int
test_summarize(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp, *tbp2, *vtbp, *untbp, *projtbp;
    int ret;
    RDB_summarize_add addv[3];

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }
    tbp2 = RDB_get_table("EMPS2", ecp, &tx);
    if (tbp2 == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    printf("Creating EMPS1 union EMPS2\n");

    untbp = RDB_union(tbp2, tbp, ecp);
    if (untbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    /* Give the table a name, because both arguments of SUMMARIZE
     * must not share an unnamed table.
     */
    ret = RDB_set_table_name(untbp, "UTABLE", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Summarizing union PER { DEPTNO } ADD COUNT AS COUNT_EMPS,\n");
    printf("    SUM(SALARY) AS SUM_SALARY, AVG(SALARY) AS AVG_SALARY\n");

    projtbp = RDB_project(untbp, 1, &projattr, ecp);
    if (projtbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    addv[0].op = RDB_COUNT;
    addv[0].name = "COUNT_EMPS";

    addv[1].op = RDB_SUM;
    addv[1].exp = RDB_expr_attr("SALARY");
    addv[1].name = "SUM_SALARY";

    addv[2].op = RDB_AVG;
    addv[2].exp = RDB_expr_attr("SALARY");
    addv[2].name = "AVG_SALARY";

    vtbp = RDB_summarize(untbp, projtbp, 3, addv, ecp, &tx);
    if (vtbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    printf("Printing table\n");
    
    ret = print_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    ret = check_contains(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    printf("Dropping summarize\n");
    RDB_drop_table(vtbp, ecp, &tx);

    RDB_drop_table(untbp, ecp, &tx);

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = test_summarize(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
