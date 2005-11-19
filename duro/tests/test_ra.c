/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *projattrs1[] = { "NAME" };

int
test_ra(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tb1p, *tb2p, *vtbp;
    RDB_object array;
    RDB_object *tplp;
    int ret;
    RDB_int i;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tb1p = RDB_get_table("EMPS1", ecp, &tx);
    if (tb1p == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    tb2p = RDB_get_table("EMPS2", ecp, &tx);
    if (tb2p == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Creating intersection (EMPS1, EMPS2)\n");
    vtbp = RDB_intersect(tb1p, tb2p, ecp);
    if (vtbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Creating projection (NAME)\n");

    vtbp = RDB_project(vtbp, 1, projattrs1, ecp);
    if (vtbp == NULL) {
        RDB_drop_table(vtbp, ecp, &tx);
        RDB_commit(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    printf("Converting virtual table to array\n");
    ret = RDB_table_to_array(&array, vtbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&array, ecp);
        RDB_commit(ecp, &tx);
        return ret;
    }

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
    }
    RDB_destroy_obj(&array, ecp);

/* !!
    if (ret != RDB_NOT_FOUND) {
        RDB_commit(ecp, &tx);
        return ret;
    }
*/

    printf("Dropping virtual table\n");
    RDB_drop_table(vtbp, ecp, &tx);

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
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    ret = test_ra(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
