/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *upoint_keyattrs1[] = { "POINT" };

RDB_key_attrs upoint_keyattrs[] = {
    { 1, upoint_keyattrs1 }
};

RDB_type *pointtyp;

int
create_table(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_attr utype_attrs[2];
    int ret;
   
    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    utype_attrs[0].name = "POINT";
    ret = RDB_get_type("POINT", &tx, &pointtyp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }
    utype_attrs[0].typ = pointtyp;
    utype_attrs[0].defaultp = NULL;

    printf("Creating table POINTTEST\n");
    ret = RDB_create_table("POINTTEST", RDB_TRUE, 1, utype_attrs,
            1, upoint_keyattrs, &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
test_insert(RDB_database *dbp)
{
    int ret;
    RDB_tuple tpl;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_value xval, yval;
    RDB_value pval;
    RDB_value lenval, thval;
    RDB_value *compv[2];

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("POINTTEST", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_tuple(&tpl);
    RDB_init_value(&xval);
    RDB_init_value(&yval);
    RDB_init_value(&pval);
    RDB_init_value(&lenval);
    RDB_init_value(&thval);

    RDB_value_set_rational(&xval, 1.0);
    RDB_value_set_rational(&yval, 2.0);

    printf("Invoking selector POINT(1,2)\n");

    compv[0] = &xval;
    compv[1] = &yval;
    ret = RDB_select_value(&pval, pointtyp, "POINT", compv);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_value_get_comp(&pval, "THETA", &thval);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_value_get_comp(&pval, "LENGTH", &lenval);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("THETA = %f, LENGTH = %f\n", (float) RDB_value_rational(&thval),
            (float) RDB_value_rational(&lenval));

    ret = RDB_tuple_set(&tpl, "POINT", &pval);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Inserting Tuple\n");

    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_destroy_tuple(&tpl);
    RDB_destroy_value(&xval);
    RDB_destroy_value(&yval);
    RDB_destroy_value(&pval);

    printf("End of transaction\n");
    return RDB_commit(&tx);
error:
    RDB_destroy_tuple(&tpl);
    RDB_destroy_value(&xval);
    RDB_destroy_value(&yval);
    RDB_destroy_value(&pval);

    RDB_rollback(&tx);
    return ret;
}

int
test_query(RDB_database *dbp)
{
    RDB_tuple tpl;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_table *tmptbp = NULL;
    RDB_expression *wherep;
    RDB_array array;
    RDB_value xval;
    RDB_value yval;
    int ret;
    int i;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_array(&array);
    RDB_init_tuple(&tpl);
    RDB_init_value(&xval);
    RDB_init_value(&yval);

    ret = RDB_get_table("POINTTEST", &tx, &tbp);
    if (ret != RDB_OK) {
        goto error;
    }

    wherep = RDB_expr_attr("POINT", pointtyp);
    wherep = RDB_get_comp(wherep, "X");
    wherep = RDB_eq(wherep, RDB_rational_const(1.0));

    ret = RDB_select(tbp, wherep, &tmptbp);

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(tmptbp, &array, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        RDB_value *pvalp = RDB_tuple_get(&tpl, "POINT");

        ret = RDB_value_get_comp(pvalp, "X", &xval);
        ret = RDB_value_get_comp(pvalp, "Y", &yval);

        printf("X: %f, Y: %f\n", (float)RDB_value_rational(&xval),
                (float)RDB_value_rational(&yval));
    }
    if (ret != RDB_NOT_FOUND)
        goto error;

    RDB_destroy_value(&xval);
    RDB_destroy_value(&yval);
    
    RDB_destroy_tuple(&tpl);

    RDB_destroy_array(&array);

    printf("Dropping selection\n");
    ret = RDB_drop_table(tmptbp, &tx);

    return RDB_commit(&tx);

error:
    RDB_destroy_value(&xval);
    RDB_destroy_value(&yval);

    RDB_destroy_tuple(&tpl);

    RDB_destroy_array(&array);

    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, &tx);

    return ret;
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("db", &dsp);
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

    ret = test_insert(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = test_query(dbp);
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
