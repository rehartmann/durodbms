/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

static int
print_table(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tplp;
    RDB_object array;
    RDB_int i;

    static const RDB_seq_item seq = { "EMPNO", RDB_TRUE };

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tbp, 1, &seq, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (ret = RDB_array_get(&array, i, &tplp)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double) RDB_tuple_get_rational(tplp, "SALARY"));
    }
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_obj(&array);
    
    return RDB_OK;
error:
    RDB_destroy_obj(&array);
    
    return ret;
}

int
test_update(RDB_database *dbp)
{
    int ret;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_attr_update attrs[1];
    RDB_expression *exprp;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Updating table, setting SALARY to 4500\n");
    attrs[0].name = "SALARY";
    attrs[0].exp = RDB_rational_to_expr(4500.0);
    ret = RDB_update(tbp, NULL, 1, attrs, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Updating table, setting EMPNO from 2 to 3\n");
    attrs[0].name = "EMPNO";
    attrs[0].exp = RDB_int_to_expr(3);
    exprp = RDB_eq(RDB_expr_attr("EMPNO"), RDB_int_to_expr(2));
    ret = RDB_update(tbp, exprp, 1, attrs, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_drop_expr(exprp);

    printf("Updating table, setting NAME of no 1 to Smythe\n");
    attrs[0].name = "NAME";
    attrs[0].exp = RDB_string_to_expr("Smythe");
    exprp = RDB_eq(RDB_expr_attr("EMPNO"), RDB_int_to_expr(1));
    ret = RDB_update(tbp, exprp, 1, attrs, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_drop_expr(exprp);

    printf("Updating table, setting SALARY of no 3 to SALARY + 100\n");
    attrs[0].name = "SALARY";
    attrs[0].exp = RDB_ro_op_va("+", RDB_rational_to_expr(100),
            RDB_expr_attr("SALARY"), (RDB_expression *) NULL);
    if (attrs[0].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    exprp = RDB_eq(RDB_expr_attr("EMPNO"), RDB_int_to_expr(3));
    ret = RDB_update(tbp, exprp, 1, attrs, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_drop_expr(exprp);

    printf("Converting table to array\n");
    ret = print_table(tbp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    
    printf("End of transaction\n");
    return RDB_commit(&tx);

error:
    RDB_rollback(&tx);
    return ret;
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_update(dbp);
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
