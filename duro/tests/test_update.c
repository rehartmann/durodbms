/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

static void
check_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;

    static const RDB_seq_item seq = { "EMPNO", RDB_TRUE };

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 1, &seq, 0, ecp, txp);
    assert(ret == RDB_OK);

    tplp = RDB_array_get(&array, 0, ecp);
    assert(RDB_tuple_get_int(tplp, "EMPNO") == 1);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Smythe") == 0);
    assert(RDB_tuple_get_float(tplp, "SALARY") == 4500.0);

    tplp = RDB_array_get(&array, 1, ecp);
    assert(RDB_tuple_get_int(tplp, "EMPNO") == 3);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Jones") == 0);
    assert(RDB_tuple_get_float(tplp, "SALARY") == 4600.0);

    assert(RDB_array_get(&array, 2, ecp) == NULL && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);

    assert(RDB_destroy_obj(&array, ecp) == RDB_OK);
}

int
test_update(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_attr_update attrs[1];
    RDB_expression *exprp;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert (ret == RDB_OK);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);

    /* Updating table, setting SALARY to 4500 */
    attrs[0].name = "SALARY";
    attrs[0].exp = RDB_float_to_expr(4500.0, ecp);
    ret = RDB_update(tbp, NULL, 1, attrs, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    /* Updating table, setting EMPNO from 2 to 3 */
    attrs[0].name = "EMPNO";
    attrs[0].exp = RDB_int_to_expr(3, ecp);
    exprp = RDB_eq(RDB_var_ref("EMPNO", ecp), RDB_int_to_expr(2, ecp), ecp);
    ret = RDB_update(tbp, exprp, 1, attrs, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    RDB_del_expr(exprp, ecp);

    /* Updating table, setting NAME of no 1 to Smythe */
    attrs[0].name = "NAME";
    attrs[0].exp = RDB_string_to_expr("Smythe", ecp);
    exprp = RDB_eq(RDB_var_ref("EMPNO", ecp), RDB_int_to_expr(1, ecp), ecp);
    ret = RDB_update(tbp, exprp, 1, attrs, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    RDB_del_expr(exprp, ecp);
    RDB_del_expr(attrs[0].exp, ecp);

    /* Updating table, setting SALARY of no 3 to SALARY + 100 */
    attrs[0].name = "SALARY";
    attrs[0].exp = RDB_ro_op("+", ecp);
    if (attrs[0].exp == NULL) {
        ret = RDB_ERROR;
        goto error;
    }
    RDB_add_arg(attrs[0].exp, RDB_float_to_expr(100, ecp));
    RDB_add_arg(attrs[0].exp, RDB_var_ref("SALARY", ecp));
    exprp = RDB_eq(RDB_var_ref("EMPNO", ecp), RDB_int_to_expr(3, ecp), ecp);
    ret = RDB_update(tbp, exprp, 1, attrs, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    RDB_del_expr(exprp, ecp);

    /* Converting table to array */
    check_table(tbp, ecp, &tx);

    return RDB_commit(ecp, &tx);

error:
    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    ret = RDB_open_env("dbenv", &dsp, RDB_RECOVER);
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

    ret = test_update(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);
    
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
