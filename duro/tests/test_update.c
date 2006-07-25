/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

static int
print_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;

    static const RDB_seq_item seq = { "EMPNO", RDB_TRUE };

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 1, &seq, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    tplp = RDB_array_get(&array, 0, ecp);
    assert(RDB_tuple_get_int(tplp, "EMPNO") == 1);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Smythe") == 0);
    assert(RDB_tuple_get_double(tplp, "SALARY") == 4500.0);

    tplp = RDB_array_get(&array, 1, ecp);
    assert(RDB_tuple_get_int(tplp, "EMPNO") == 3);
    assert(strcmp(RDB_tuple_get_string(tplp, "NAME"), "Jones") == 0);
    assert(RDB_tuple_get_double(tplp, "SALARY") == 4600.0);

    assert(RDB_array_get(&array, 2, ecp) == NULL && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);

    return RDB_destroy_obj(&array, ecp);

error:
    RDB_destroy_obj(&array, ecp);
    
    return RDB_ERROR;
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
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        goto error;
    }

    /* Updating table, setting SALARY to 4500 */
    attrs[0].name = "SALARY";
    attrs[0].exp = RDB_double_to_expr(4500.0, ecp);
    ret = RDB_update(tbp, NULL, 1, attrs, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    /* Updating table, setting EMPNO from 2 to 3 */
    attrs[0].name = "EMPNO";
    attrs[0].exp = RDB_int_to_expr(3, ecp);
    exprp = RDB_eq(RDB_expr_var("EMPNO", ecp), RDB_int_to_expr(2, ecp), ecp);
    ret = RDB_update(tbp, exprp, 1, attrs, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    RDB_drop_expr(exprp, ecp);

    /* Updating table, setting NAME of no 1 to Smythe */
    attrs[0].name = "NAME";
    attrs[0].exp = RDB_string_to_expr("Smythe", ecp);
    exprp = RDB_eq(RDB_expr_var("EMPNO", ecp), RDB_int_to_expr(1, ecp), ecp);
    ret = RDB_update(tbp, exprp, 1, attrs, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    RDB_drop_expr(exprp, ecp);

    /* Updating table, setting SALARY of no 3 to SALARY + 100 */
    attrs[0].name = "SALARY";
    attrs[0].exp = RDB_ro_op_va("+", ecp, RDB_double_to_expr(100, ecp),
            RDB_expr_var("SALARY", ecp), (RDB_expression *) NULL);
    if (attrs[0].exp == NULL) {
        ret = RDB_ERROR;
        goto error;
    }
    exprp = RDB_eq(RDB_expr_var("EMPNO", ecp), RDB_int_to_expr(3, ecp), ecp);
    ret = RDB_update(tbp, exprp, 1, attrs, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    RDB_drop_expr(exprp, ecp);

    /* Converting table to array */
    ret = print_table(tbp, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
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
