/* $Id$ */

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
create_table(RDB_database *dbp)
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
    ret = RDB_create_table("SRTEST", RDB_TRUE, 3, srtest_attrs,
            1, srtest_keyattrs, &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    RDB_init_obj(&tpl);

    for (i = 0; i < 3; i++) {
        RDB_tuple_set_int(&tpl, "NO", (RDB_int) i);
        RDB_tuple_set_int(&tpl, "O_NO", (RDB_int) i);
        RDB_tuple_set_int(&tpl, "COUNT", (RDB_int) 0);

        ret = RDB_insert(tbp, &tpl, &tx);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            RDB_rollback(&tx);
            return ret;
        }
    }

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
test_update1(RDB_database *dbp)
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

    ret = RDB_get_table("SRTEST", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Updating table\n");

    exp = RDB_int_const(2);
    exp = RDB_subtract(exp, RDB_expr_attr("NO"));

    upd.name = "NO";
    upd.exp = exp;

    ret = RDB_update(tbp, NULL, 1, &upd, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    return RDB_commit(&tx);
}

int
test_update2(RDB_database *dbp)
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

    ret = RDB_get_table("SRTEST", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Updating table\n");

    exp = RDB_expr_sum(RDB_table_to_expr(tbp), "COUNT");
    exp = RDB_add(exp, RDB_int_const(1));

    upd.name = "COUNT";
    upd.exp = exp;

    ret = RDB_update(tbp, NULL, 1, &upd, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    return RDB_commit(&tx);
}

int
test_print(RDB_database *dbp)
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

    ret = RDB_get_table("SRTEST", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_obj(&array);

    printf("Converting table to array\n");
    ret = RDB_table_to_array(&array, tbp, 0, NULL, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    } 

    for (i = 0; (ret = RDB_array_get(&array, i, &tplp)) == RDB_OK; i++) {
        printf("NO=%d, O_NO=%d, COUNT=%d\n", (int)RDB_tuple_get_int(tplp, "NO"),
                (int)RDB_tuple_get_int(tplp, "O_NO"),
                (int)RDB_tuple_get_int(tplp, "COUNT"));
    }
    RDB_destroy_obj(&array);
    if (ret != RDB_NOT_FOUND) {
        RDB_rollback(&tx);
        return ret;
    }

    return RDB_commit(&tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = create_table(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = test_update1(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = test_update2(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = test_print(dbp);
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
