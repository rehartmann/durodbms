/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *upoint_keyattrs1[] = { "POINT" };

RDB_string_vec upoint_keyattrs[] = {
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
    RDB_object tpl;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_object xval, yval;
    RDB_object pval;
    RDB_object lenval, thval;
    RDB_object *compv[2];

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

    RDB_init_obj(&tpl);

    RDB_init_obj(&xval);
    RDB_init_obj(&yval);
    RDB_init_obj(&pval);
    RDB_init_obj(&lenval);
    RDB_init_obj(&thval);

    RDB_rational_to_obj(&xval, 1.0);
    RDB_rational_to_obj(&yval, 2.0);

    printf("Invoking selector POINT(1,2)\n");

    compv[0] = &xval;
    compv[1] = &yval;
    ret = RDB_call_ro_op("POINT", 2, compv, &tx, &pval);

    if (ret != RDB_OK)
        goto error;

    printf("Inserting Tuple\n");

    ret = RDB_tuple_set(&tpl, "POINT", &pval);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_obj_comp(&pval, "THETA", &thval, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_obj_comp(&pval, "LENGTH", &lenval, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Doubling LENGTH\n");

    RDB_rational_to_obj(&lenval, RDB_obj_rational(&lenval) * 2.0);

    RDB_obj_set_comp(&pval, "LENGTH", &lenval, &tx);

    printf("Inserting Tuple\n");

    ret = RDB_tuple_set(&tpl, "POINT", &pval);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&xval);
    RDB_destroy_obj(&yval);
    RDB_destroy_obj(&pval);
    RDB_destroy_obj(&lenval);
    RDB_destroy_obj(&thval);

    printf("End of transaction\n");
    return RDB_commit(&tx);
error:
    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&xval);
    RDB_destroy_obj(&yval);
    RDB_destroy_obj(&pval);
    RDB_destroy_obj(&lenval);
    RDB_destroy_obj(&thval);

    RDB_rollback(&tx);
    return ret;
}

int
test_query(RDB_database *dbp)
{
    RDB_object *tplp;
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_table *tmptbp = NULL;
    RDB_expression *wherep;
    RDB_expression *compv[2];
    RDB_object array;
    RDB_object xval;
    RDB_object yval;
    int ret;
    int i;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&array);
    RDB_init_obj(&xval);
    RDB_init_obj(&yval);

    ret = RDB_get_table("POINTTEST", &tx, &tbp);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Converting table to array\n");
    ret = RDB_table_to_array(&array, tbp, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 
 
    for (i = 0; (ret = RDB_array_get(&array, i, &tplp)) == RDB_OK; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, &tx);
        ret = RDB_obj_comp(pvalp, "Y", &yval, &tx);

        printf("X=%f, Y=%f\n", (float)RDB_obj_rational(&xval),
                (float)RDB_obj_rational(&yval));
    }
    if (ret != RDB_NOT_FOUND)
        goto error;

    printf("Creating POINTTEST WHERE POINT.THE_X=1\n");

    wherep = RDB_expr_attr("POINT");
    wherep = RDB_expr_comp(wherep, "X");
    wherep = RDB_eq(wherep, RDB_rational_to_expr(1.0));

    ret = RDB_select(tbp, wherep, &tx, &tmptbp);

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(&array, tmptbp, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (ret = RDB_array_get(&array, i, &tplp)) == RDB_OK; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, &tx);
        if (ret != RDB_OK) {
            goto error;
        } 
        ret = RDB_obj_comp(pvalp, "Y", &yval, &tx);
        if (ret != RDB_OK) {
            goto error;
        } 

        printf("X=%f, Y=%f\n", (float)RDB_obj_rational(&xval),
                (float)RDB_obj_rational(&yval));
    }
    if (ret != RDB_NOT_FOUND)
        goto error;

    RDB_destroy_obj(&array);

    RDB_drop_table(tmptbp, &tx);

    printf("Creating POINTTEST WHERE POINT=POINT(1,2)\n");

    compv[0] = RDB_rational_to_expr(1.0);
    compv[1] = RDB_rational_to_expr(2.0);
    wherep = RDB_ro_op("POINT", 2, compv);
    if (wherep == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    } 
    wherep = RDB_eq(wherep, RDB_expr_attr("POINT"));

    ret = RDB_select(tbp, wherep, &tx, &tmptbp);
    if (ret != RDB_OK) {
        goto error;
    } 

    RDB_init_obj(&array);

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(&array, tmptbp, 0, NULL, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (ret = RDB_array_get(&array, i, &tplp)) == RDB_OK; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, &tx);
        if (ret != RDB_OK) {
            goto error;
        } 
        ret = RDB_obj_comp(pvalp, "Y", &yval, &tx);
        if (ret != RDB_OK) {
            goto error;
        } 

        printf("X=%f, Y=%f\n", (float)RDB_obj_rational(&xval),
                (float)RDB_obj_rational(&yval));
    }
    if (ret != RDB_NOT_FOUND)
        goto error;

    RDB_destroy_obj(&xval);
    RDB_destroy_obj(&yval);
    
    RDB_destroy_obj(&array);

    ret = RDB_drop_table(tmptbp, &tx);
    if (ret != RDB_OK) {
        tmptbp = NULL;
        goto error;
    }

    return RDB_commit(&tx);

error:
    RDB_destroy_obj(&xval);
    RDB_destroy_obj(&yval);

    RDB_destroy_obj(&array);

    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, &tx);

    return ret;
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", envp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_set_errfile(envp, stderr);

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
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
