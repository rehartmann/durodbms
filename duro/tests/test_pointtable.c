/* $Id$ */

#include <rel/rdb.h>
#include <dli/tabletostr.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

char *upoint_keyattrs1[] = { "POINT" };

RDB_string_vec upoint_keyattrs[] = {
    { 1, upoint_keyattrs1 }
};

RDB_type *pointtyp;

void
create_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object*tbp;
    RDB_attr utype_attrs[2];
    int ret;
   
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    utype_attrs[0].name = "POINT";
    pointtyp = RDB_get_type("POINT", ecp, &tx);
    assert(pointtyp != NULL);
    utype_attrs[0].typ = pointtyp;
    utype_attrs[0].defaultp = NULL;

    tbp = RDB_create_table("POINTTEST", RDB_TRUE, 1, utype_attrs,
            1, upoint_keyattrs, ecp, &tx);
    assert(tbp != NULL);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

void
test_insert(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object tpl;
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_object xval, yval;
    RDB_object pval;
    RDB_object lenval, thval;
    RDB_object *compv[2];

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    tbp = RDB_get_table("POINTTEST", ecp, &tx);
    assert(tbp != NULL);

    RDB_init_obj(&tpl);

    RDB_init_obj(&xval);
    RDB_init_obj(&yval);
    RDB_init_obj(&pval);
    RDB_init_obj(&lenval);
    RDB_init_obj(&thval);

    RDB_double_to_obj(&xval, 1.0);
    RDB_double_to_obj(&yval, 2.0);

    compv[0] = &xval;
    compv[1] = &yval;
    ret = RDB_call_ro_op("POINT", 2, compv, ecp, &tx, &pval);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error at %s:%d: ", __FILE__, __LINE__);
        RDB_print_obj(RDB_get_err(ecp), stderr, ecp, &tx);
        fputs("\n", stderr);
        abort();
    }

    assert(ret == RDB_OK);

    ret = RDB_tuple_set(&tpl, "POINT", &pval, ecp);
    assert(ret == RDB_OK);

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_obj_comp(&pval, "THETA", &thval, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_obj_comp(&pval, "LENGTH", &lenval, ecp, &tx);
    assert(ret == RDB_OK);

    RDB_double_to_obj(&lenval, RDB_obj_double(&lenval) * 2.0);

    RDB_obj_set_comp(&pval, "LENGTH", &lenval, ecp, &tx);

    ret = RDB_tuple_set(&tpl, "POINT", &pval, ecp);
    assert(ret == RDB_OK);

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret == RDB_OK);

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&xval, ecp);
    RDB_destroy_obj(&yval, ecp);
    RDB_destroy_obj(&pval, ecp);
    RDB_destroy_obj(&lenval, ecp);
    RDB_destroy_obj(&thval, ecp);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

void
test_query(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_object *tplp;
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_object *tmptbp = NULL;
    RDB_expression *exp, *argp, *wherep;
    RDB_expression *compv[2];
    RDB_object array;
    RDB_object xval;
    RDB_object yval;
    int ret;
    int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    RDB_init_obj(&array);
    RDB_init_obj(&xval);
    RDB_init_obj(&yval);

    tbp = RDB_get_table("POINTTEST", ecp, &tx);
    assert(tbp != NULL);

    ret = RDB_table_to_array(&array, tbp, 0, NULL, ecp, &tx);
    assert(ret == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, ecp, &tx);
        assert(ret == RDB_OK);
        ret = RDB_obj_comp(pvalp, "Y", &yval, ecp, &tx);
        assert(ret == RDB_OK);

        printf("X=%f, Y=%f\n", (float)RDB_obj_double(&xval),
                (float)RDB_obj_double(&yval));
    }
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);
    RDB_clear_err(ecp);

    exp = RDB_ro_op("WHERE", 2, ecp);
    assert(exp != NULL);
    
    argp = RDB_table_ref_to_expr(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    wherep = RDB_expr_var("POINT", ecp);
    wherep = RDB_expr_comp(wherep, "X", ecp);
    wherep = RDB_eq(wherep, RDB_double_to_expr(1.0, ecp), ecp);
    RDB_add_arg(exp, wherep);

    tmptbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(tmptbp != NULL);

    ret = RDB_table_to_array(&array, tmptbp, 0, NULL, ecp, &tx);
    assert(ret == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, ecp, &tx);
        assert(ret == RDB_OK);
        ret = RDB_obj_comp(pvalp, "Y", &yval, ecp, &tx);
        assert(ret == RDB_OK);

        printf("X=%f, Y=%f\n", (float)RDB_obj_double(&xval),
                (float)RDB_obj_double(&yval));
    }
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);

    RDB_drop_table(tmptbp, ecp, &tx);

    /* Creating POINTTEST WHERE POINT=POINT(1,2) */

    exp = RDB_ro_op("WHERE", 2, ecp);
    assert(exp != NULL);

    argp = RDB_table_ref_to_expr(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    compv[0] = RDB_double_to_expr(1.0, ecp);
    compv[1] = RDB_double_to_expr(2.0, ecp);
    wherep = RDB_ro_op("POINT", 2, ecp);
    assert(wherep != NULL);

    RDB_add_arg(wherep, compv[0]);
    RDB_add_arg(wherep, compv[1]);
    wherep = RDB_eq(wherep, RDB_expr_var("POINT", ecp), ecp);
    RDB_add_arg(exp, wherep);

    tmptbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(tmptbp != NULL);

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmptbp, 0, NULL, ecp, &tx);
    assert(ret == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, ecp, &tx);
        assert(ret == RDB_OK);
        ret = RDB_obj_comp(pvalp, "Y", &yval, ecp, &tx);
        assert(ret == RDB_OK);

        printf("X=%f, Y=%f\n", (float)RDB_obj_double(&xval),
                (float)RDB_obj_double(&yval));
    }
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);
    RDB_clear_err(ecp);

    RDB_destroy_obj(&xval, ecp);
    RDB_destroy_obj(&yval, ecp);
    
    RDB_destroy_obj(&array, ecp);

    ret = RDB_drop_table(tmptbp, ecp, &tx);
    assert(ret == RDB_OK);

    assert(RDB_commit(ecp, &tx) == RDB_OK);
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);

    create_table(dbp, &ec);

    test_insert(dbp, &ec);

    test_query(dbp, &ec);
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
