/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

RDB_seq_item empseqitv[] = { { "EMPNO", RDB_TRUE } };

static int
print_table(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;
    RDB_int i;

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 1, empseqitv, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("DEPTNO: %d\n", (int) RDB_tuple_get_int(tplp, "DEPTNO"));
        printf("DEPTNAME: %s\n", RDB_tuple_get_string(tplp, "DEPTNAME"));
    }
/* !!
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }
*/
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    
    return RDB_ERROR;
}

int
test_join(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp1, *tbp2, *vtbp;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp1 = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp1 == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    tbp2 = RDB_get_table("DEPTS", ecp, &tx);
    if (tbp2 == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Joining EMPS1 with DEPTS\n");

    vtbp = RDB_join(tbp1, tbp2, ecp);
    if (vtbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    
    printf("Converting joined table to array\n");
    ret = print_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    printf("Dropping join\n");
    RDB_drop_table(vtbp, ecp, &tx);

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
        return 1;
    }

    ret = test_join(dbp, &ec);
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
