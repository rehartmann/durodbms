/*
 * Test table update which references the table itself.
 */

#include <rel/rdb.h>
#include <db.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

RDB_attr srtest_attrs[] = {
    { "NO", NULL, NULL, 0 },
    { "O_NO", NULL, NULL, 0 },
    { "COUNT", NULL, NULL, 0 }
};

char *srtest_keys[] = { "NO" };

RDB_string_vec srtest_keyattrs[] = {
    { 1, srtest_keys }
};

static void
create_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_object tpl;
    int ret;
    int i;

    srtest_attrs[0].typ = &RDB_INTEGER;
    srtest_attrs[1].typ = &RDB_INTEGER;
    srtest_attrs[2].typ = &RDB_INTEGER;
   
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_create_table("SRTEST", 3, srtest_attrs,
            1, srtest_keyattrs, ecp, &tx);
    assert(tbp != NULL);

    RDB_init_obj(&tpl);

    for (i = 0; i < 3; i++) {
        RDB_tuple_set_int(&tpl, "NO", (RDB_int) i, ecp);
        RDB_tuple_set_int(&tpl, "O_NO", (RDB_int) i, ecp);
        RDB_tuple_set_int(&tpl, "COUNT", (RDB_int) 0, ecp);

        ret = RDB_insert(tbp, &tpl, ecp, &tx);
        assert(ret == RDB_OK);
    }

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

static void
test_update1(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_expression *exp;
    RDB_attr_update upd;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    assert(tbp != NULL);

    exp = RDB_ro_op("-", ecp);
    assert(exp != NULL);

    RDB_add_arg(exp, RDB_int_to_expr(2, ecp));
    RDB_add_arg(exp, RDB_var_ref("NO", ecp));

    upd.name = "NO";
    upd.exp = exp;

    ret = RDB_update(tbp, NULL, 1, &upd, ecp, &tx);
    assert(ret != RDB_ERROR);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

static void
test_update2(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_expression *exp, *argp, *condp;
    RDB_attr_update upd;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    assert(tbp != NULL);

    exp = RDB_ro_op("sum", ecp);
    assert(exp != NULL);
    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);
    argp = RDB_var_ref("COUNT", ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    condp = RDB_ro_op("+", ecp);
    assert(condp != NULL);

    RDB_add_arg(condp, exp);
    RDB_add_arg(condp, RDB_int_to_expr(1, ecp));

    upd.name = "COUNT";
    upd.exp = condp;

    ret = RDB_update(tbp, NULL, 1, &upd, ecp, &tx);
    assert(ret != RDB_ERROR);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

RDB_seq_item noseqitv[] = { { "NO", RDB_TRUE } };

static void
test_print(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_object array;
    RDB_object *tplp;
    int ret;
    int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("SRTEST", ecp, &tx);
    assert(tbp != NULL);

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 1, noseqitv, 0, ecp, &tx);
    assert(ret == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("NO=%d, O_NO=%d, COUNT=%d\n", (int)RDB_tuple_get_int(tplp, "NO"),
                (int)RDB_tuple_get_int(tplp, "O_NO"),
                (int)RDB_tuple_get_int(tplp, "COUNT"));
    }
    RDB_destroy_obj(&array, ecp);
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);
    RDB_clear_err(ecp);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    ret = RDB_open_env("dbenv", &dsp, RDB_RECOVER);
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

    create_table(dbp, &ec);

    test_update1(dbp, &ec);

    test_update2(dbp, &ec);

    test_print(dbp, &ec);

    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
