/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

static void
create_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;

    RDB_attr attrs[] = {
        {"A", &RDB_INTEGER, NULL, 0 },
        {"B", &RDB_STRING, NULL, 0 }
    };

    char *keyattrs[] = { "A" };

    RDB_string_vec key[] = {
        { 1, keyattrs }
    };

    assert(RDB_init_table(tbp, NULL, 2, attrs, 1, key, ecp) == RDB_OK);

    RDB_init_obj(&tpl);

    assert(RDB_tuple_set_int(&tpl, "A", 1, ecp) == RDB_OK);
    assert(RDB_tuple_set_string(&tpl, "B", "One", ecp) == RDB_OK);
    
    assert(RDB_insert(tbp, &tpl, ecp, txp) == RDB_OK);

    assert(RDB_tuple_set_int(&tpl, "A", 2, ecp) == RDB_OK);
    assert(RDB_tuple_set_string(&tpl, "B", "Two", ecp) == RDB_OK);
    
    assert(RDB_insert(tbp, &tpl, ecp, txp) == RDB_OK);   

    assert(RDB_destroy_obj(&tpl, ecp) == RDB_OK);
}

static void
test_update(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_attr_update attrs[1];
    RDB_expression *condp;

    /* Update table */
    attrs[0].name = "B";
    attrs[0].exp = RDB_string_to_expr("Eins", ecp);
    assert(attrs[0].exp != NULL);

    condp = RDB_eq(RDB_var_ref("A", ecp), RDB_int_to_expr(1, ecp), ecp);
    assert(condp != NULL);

    ret = RDB_update(tbp, condp, 1, attrs, ecp, txp);
    assert(ret != RDB_ERROR);

    assert(RDB_del_expr(condp, ecp) == RDB_OK);
    assert(RDB_del_expr(attrs[0].exp, ecp) == RDB_OK);
}

static void
check_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;

    static const RDB_seq_item seq = { "A", RDB_TRUE };

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 1, &seq, 0, ecp, txp);
    assert(ret == RDB_OK);

    tplp = RDB_array_get(&array, 0, ecp);
    assert(RDB_tuple_get_int(tplp, "A") == 1);
    assert(strcmp(RDB_tuple_get_string(tplp, "B"), "Eins") == 0);

    tplp = RDB_array_get(&array, 1, ecp);
    assert(RDB_tuple_get_int(tplp, "A") == 2);
    assert(strcmp(RDB_tuple_get_string(tplp, "B"), "Two") == 0);

    assert(RDB_array_get(&array, 2, ecp) == NULL
            && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);

    assert(RDB_destroy_obj(&array, ecp) == RDB_OK);
}


int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    RDB_transaction tx;
    RDB_object tb;

    ret = RDB_open_env("dbenv", &dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = RDB_begin_tx(&ec, &tx, dbp, NULL);
    assert (ret == RDB_OK);

    RDB_init_obj(&tb);

    create_table(&tb, &ec, &tx);

    test_update(&tb, &ec, &tx);

    check_table(&tb, &ec, &tx);

    assert(RDB_destroy_obj(&tb, &ec) == RDB_OK);

    assert(RDB_commit(&ec, &tx) == RDB_OK);

    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
