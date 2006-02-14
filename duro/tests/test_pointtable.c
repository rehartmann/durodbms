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
create_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_attr utype_attrs[2];
    int ret;
   
    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    utype_attrs[0].name = "POINT";
    pointtyp = RDB_get_type("POINT", ecp, &tx);
    if (pointtyp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    utype_attrs[0].typ = pointtyp;
    utype_attrs[0].defaultp = NULL;

    printf("Creating table POINTTEST\n");
    tbp = RDB_create_table("POINTTEST", RDB_TRUE, 1, utype_attrs,
            1, upoint_keyattrs, ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);
}

int
test_insert(RDB_database *dbp, RDB_exec_context *ecp)
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
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("POINTTEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    RDB_init_obj(&xval);
    RDB_init_obj(&yval);
    RDB_init_obj(&pval);
    RDB_init_obj(&lenval);
    RDB_init_obj(&thval);

    RDB_double_to_obj(&xval, 1.0);
    RDB_double_to_obj(&yval, 2.0);

    printf("Invoking selector POINT(1,2)\n");

    compv[0] = &xval;
    compv[1] = &yval;
    ret = RDB_call_ro_op("POINT", 2, compv, ecp, &tx, &pval);
    if (ret != RDB_OK)
        goto error;

    printf("Inserting Tuple\n");

    ret = RDB_tuple_set(&tpl, "POINT", &pval, ecp);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_obj_comp(&pval, "THETA", &thval, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_obj_comp(&pval, "LENGTH", &lenval, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Doubling LENGTH\n");

    RDB_double_to_obj(&lenval, RDB_obj_double(&lenval) * 2.0);

    RDB_obj_set_comp(&pval, "LENGTH", &lenval, ecp, &tx);

    printf("Inserting Tuple\n");

    ret = RDB_tuple_set(&tpl, "POINT", &pval, ecp);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&xval, ecp);
    RDB_destroy_obj(&yval, ecp);
    RDB_destroy_obj(&pval, ecp);
    RDB_destroy_obj(&lenval, ecp);
    RDB_destroy_obj(&thval, ecp);

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&xval, ecp);
    RDB_destroy_obj(&yval, ecp);
    RDB_destroy_obj(&pval, ecp);
    RDB_destroy_obj(&lenval, ecp);
    RDB_destroy_obj(&thval, ecp);

    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

int
test_query(RDB_database *dbp, RDB_exec_context *ecp)
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
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&array);
    RDB_init_obj(&xval);
    RDB_init_obj(&yval);

    tbp = RDB_get_table("POINTTEST", ecp, &tx);
    if (tbp == NULL) {
        goto error;
    }

    printf("Converting table to array\n");
    ret = RDB_table_to_array(&array, tbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 
 
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, ecp, &tx);
        ret = RDB_obj_comp(pvalp, "Y", &yval, ecp, &tx);

        printf("X=%f, Y=%f\n", (float)RDB_obj_double(&xval),
                (float)RDB_obj_double(&yval));
    }
/* !!
    if (ret != RDB_NOT_FOUND)
        goto error;
*/
    RDB_clear_err(ecp);
    printf("Creating POINTTEST WHERE POINT.THE_X=1\n");

    wherep = RDB_expr_var("POINT", ecp);
    wherep = RDB_expr_comp(wherep, "X", ecp);
    wherep = RDB_eq(wherep, RDB_double_to_expr(1.0, ecp), ecp);

    tmptbp = RDB_select(tbp, wherep, ecp, &tx);
    if (tmptbp == NULL)
        goto error;

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(&array, tmptbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, ecp, &tx);
        if (ret != RDB_OK) {
            goto error;
        } 
        ret = RDB_obj_comp(pvalp, "Y", &yval, ecp, &tx);
        if (ret != RDB_OK) {
            goto error;
        } 

        printf("X=%f, Y=%f\n", (float)RDB_obj_double(&xval),
                (float)RDB_obj_double(&yval));
    }
/* !!
    if (ret != RDB_NOT_FOUND)
        goto error;
*/
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);

    RDB_drop_table(tmptbp, ecp, &tx);

    printf("Creating POINTTEST WHERE POINT=POINT(1,2)\n");

    compv[0] = RDB_double_to_expr(1.0, ecp);
    compv[1] = RDB_double_to_expr(2.0, ecp);
    wherep = RDB_ro_op("POINT", 2, compv, ecp);
    if (wherep == NULL) {
        ret = RDB_ERROR;
        goto error;
    } 
    wherep = RDB_eq(wherep, RDB_expr_var("POINT", ecp), ecp);

    tmptbp = RDB_select(tbp, wherep, ecp, &tx);
    if (tmptbp == NULL) {
        goto error;
    } 

    RDB_init_obj(&array);

    printf("Converting selection table to array\n");
    ret = RDB_table_to_array(&array, tmptbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, ecp, &tx);
        if (ret != RDB_OK) {
            goto error;
        } 
        ret = RDB_obj_comp(pvalp, "Y", &yval, ecp, &tx);
        if (ret != RDB_OK) {
            goto error;
        } 

        printf("X=%f, Y=%f\n", (float)RDB_obj_double(&xval),
                (float)RDB_obj_double(&yval));
    }
/* !!
    if (ret != RDB_NOT_FOUND)
        goto error;
*/
    RDB_clear_err(ecp);

    RDB_destroy_obj(&xval, ecp);
    RDB_destroy_obj(&yval, ecp);
    
    RDB_destroy_obj(&array, ecp);

    ret = RDB_drop_table(tmptbp, ecp, &tx);
    if (ret != RDB_OK) {
        tmptbp = NULL;
        goto error;
    }

    return RDB_commit(ecp, &tx);

error:
    RDB_destroy_obj(&xval, ecp);
    RDB_destroy_obj(&yval, ecp);

    RDB_destroy_obj(&array, ecp);

    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, ecp, &tx);

    return ret;
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    ret = create_table(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_insert(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_query(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
