/* $Id$ */

/*
 * Test table update which references the table itself.
 */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

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
    RDB_object *tbp;
    RDB_object tpl;
    int ret;
    int i;
   
    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    printf("Creating table SRTEST\n");
    tbp = RDB_create_table("SRTEST", RDB_TRUE, 3, srtest_attrs,
            1, srtest_keyattrs, ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    RDB_init_obj(&tpl);

    for (i = 0; i < 3; i++) {
        RDB_tuple_set_int(&tpl, "NO", (RDB_int) i, ecp);
        RDB_tuple_set_int(&tpl, "O_NO", (RDB_int) i, ecp);
        RDB_tuple_set_int(&tpl, "COUNT", (RDB_int) 0, ecp);

        ret = RDB_insert(tbp, &tpl, ecp, &tx);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            RDB_rollback(ecp, &tx);
            return ret;
        }
    }

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);
}

int
test_update1(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_expression *exp;
    RDB_attr_update upd;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Updating table\n");

    exp = RDB_int_to_expr(2, ecp);
    exp = RDB_ro_op_va("-", ecp, exp, RDB_expr_var("NO", ecp),
            (RDB_expression *) NULL);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    upd.name = "NO";
    upd.exp = exp;

    ret = RDB_update(tbp, NULL, 1, &upd, ecp, &tx);
    if (ret == RDB_ERROR) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    return RDB_commit(ecp, &tx);
}

int
test_update2(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_expression *exp, *argp;
    RDB_attr_update upd;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Updating table\n");

    exp = RDB_ro_op("SUM", 2, NULL, ecp);
    assert(exp != NULL);
    argp = RDB_table_ref_to_expr(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);
    argp = RDB_expr_var("COUNT", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    exp = RDB_ro_op_va("+", ecp, exp, RDB_int_to_expr(1, ecp),
            (RDB_expression *) NULL);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    upd.name = "COUNT";
    upd.exp = exp;

    ret = RDB_update(tbp, NULL, 1, &upd, ecp, &tx);
    if (ret == RDB_ERROR) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    return RDB_commit(ecp, &tx);
}

RDB_seq_item noseqitv[] = { { "NO", RDB_TRUE } };

int
test_print(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_object array;
    RDB_object *tplp;
    int ret;
    int i;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    RDB_init_obj(&array);

    printf("Converting table to array\n");
    ret = RDB_table_to_array(&array, tbp, 1, noseqitv, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("NO=%d, O_NO=%d, COUNT=%d\n", (int)RDB_tuple_get_int(tplp, "NO"),
                (int)RDB_tuple_get_int(tplp, "O_NO"),
                (int)RDB_tuple_get_int(tplp, "COUNT"));
    }
    RDB_destroy_obj(&array, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);

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
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = create_table(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_update1(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_update2(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_print(dbp, &ec);
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
