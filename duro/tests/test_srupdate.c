/* $Id$ */

/*
 * Test table update which references the table itself.
 */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

RDB_attr srtest_attrs[] = {
    { "NO", &RDB_INTEGER, NULL, 0 },
    { "O_NO", &RDB_INTEGER, NULL, 0 },
    { "COUNT", &RDB_INTEGER, NULL, 0 }
};

char *srtest_keys[] = { "NO" };

RDB_string_vec srtest_keyattrs[] = {
    { 1, srtest_keys }
};

int
create_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_object tpl;
    int ret;
    int i;
   
    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Creating table SRTEST\n");
    tbp = RDB_create_table("SRTEST", RDB_TRUE, 3, srtest_attrs,
            1, srtest_keyattrs, ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    RDB_init_obj(&tpl);

    for (i = 0; i < 3; i++) {
        RDB_tuple_set_int(&tpl, "NO", (RDB_int) i);
        RDB_tuple_set_int(&tpl, "O_NO", (RDB_int) i);
        RDB_tuple_set_int(&tpl, "COUNT", (RDB_int) 0);

        ret = RDB_insert(tbp, &tpl, ecp, &tx);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            RDB_rollback(&tx);
            return ret;
        }
    }

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
test_update1(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_expression *exp;
    RDB_attr_update upd;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    printf("Updating table\n");

    exp = RDB_int_to_expr(2);
    exp = RDB_ro_op_va("-", exp, RDB_expr_attr("NO"), (RDB_expression *) NULL);
    if (exp == NULL) {
        RDB_rollback(&tx);
        return RDB_NO_MEMORY;
    }

    upd.name = "NO";
    upd.exp = exp;

    ret = RDB_update(tbp, NULL, 1, &upd, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    return RDB_commit(&tx);
}

int
test_update2(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_expression *exp;
    RDB_attr_update upd;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    printf("Updating table\n");

    exp = RDB_expr_sum(RDB_table_to_expr(tbp, ecp), "COUNT");
    exp = RDB_ro_op_va("+", exp, RDB_int_to_expr(1), (RDB_expression *) NULL);
    if (exp == NULL) {
        RDB_rollback(&tx);
        return RDB_NO_MEMORY;
    }

    upd.name = "COUNT";
    upd.exp = exp;

    ret = RDB_update(tbp, NULL, 1, &upd, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    return RDB_commit(&tx);
}

RDB_seq_item noseqitv[] = { { "NO", RDB_TRUE } };

int
test_print(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_object array;
    RDB_object *tplp;
    int ret;
    int i;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_obj(&array);

    printf("Converting table to array\n");
    ret = RDB_table_to_array(&array, tbp, 1, noseqitv, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("NO=%d, O_NO=%d, COUNT=%d\n", (int)RDB_tuple_get_int(tplp, "NO"),
                (int)RDB_tuple_get_int(tplp, "O_NO"),
                (int)RDB_tuple_get_int(tplp, "COUNT"));
    }
    RDB_destroy_obj(&array, ecp);
    /* !!
    if (ret != RDB_NOT_FOUND) {
        RDB_rollback(&tx);
        return ret;
    }
    */

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

    ret = create_table(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_update1(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_update2(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_print(dbp, &ec);
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
