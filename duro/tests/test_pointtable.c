/* $Id$ */

#include <rel/rdb.h>
#include <rel/tostr.h>
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
    utype_attrs[0].options = 0;

    tbp = RDB_create_table("POINTTEST", 1, utype_attrs,
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

    RDB_float_to_obj(&xval, 1.0);
    RDB_float_to_obj(&yval, 2.0);

    compv[0] = &xval;
    compv[1] = &yval;
    ret = RDB_call_ro_op_by_name("POINT", 2, compv, ecp, &tx, &pval);
    assert(ret == RDB_OK);

    ret = RDB_tuple_set(&tpl, "POINT", &pval, ecp);
    assert(ret == RDB_OK);

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_obj_comp(&pval, "THETA", &thval, NULL, ecp, &tx);
    assert(ret == RDB_OK);

    ret = RDB_obj_comp(&pval, "LENGTH", &lenval, NULL, ecp, &tx);
    assert(ret == RDB_OK);

    RDB_float_to_obj(&lenval, RDB_obj_float(&lenval) * 2.0);

    RDB_obj_set_comp(&pval, "LENGTH", &lenval, NULL, ecp, &tx);

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

static int
ftoi(RDB_float f)
{
    return (int) (f + 0.5);
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
    RDB_object xval2;
    RDB_object yval2;
    int ret;
    int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    assert(ret == RDB_OK);

    RDB_init_obj(&array);
    RDB_init_obj(&xval);
    RDB_init_obj(&yval);
    RDB_init_obj(&xval2);
    RDB_init_obj(&yval2);

    tbp = RDB_get_table("POINTTEST", ecp, &tx);
    assert(tbp != NULL);

    ret = RDB_table_to_array(&array, tbp, 0, NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    assert(RDB_array_length(&array, ecp) == 2);

    ret = RDB_obj_comp(RDB_tuple_get(RDB_array_get(&array, 0, ecp), "POINT"),
            "X", &xval, NULL, ecp, &tx);
    assert(ret == RDB_OK);
    ret = RDB_obj_comp(RDB_tuple_get(RDB_array_get(&array, 0, ecp), "POINT"),
            "Y", &yval, NULL, ecp, &tx);
    assert(ret == RDB_OK);
    ret = RDB_obj_comp(RDB_tuple_get(RDB_array_get(&array, 1, ecp), "POINT"),
            "X", &xval2, NULL, ecp, &tx);
    assert(ret == RDB_OK);
    ret = RDB_obj_comp(RDB_tuple_get(RDB_array_get(&array, 1, ecp), "POINT"),
            "Y", &yval2, NULL, ecp, &tx);
    assert(ret == RDB_OK);

    assert((ftoi(RDB_obj_float(&xval)) == 1 && ftoi(RDB_obj_float(&yval)) == 2
                && ftoi(RDB_obj_float(&xval2)) == 2 && ftoi(RDB_obj_float(&yval2)) == 4)
            || (ftoi(RDB_obj_float(&xval)) == 2 && ftoi(RDB_obj_float(&yval)) == 4
                && ftoi(RDB_obj_float(&xval2)) == 1 && ftoi(RDB_obj_float(&yval2)) == 2));

    /* Creating POINTTEST WHERE THE_X(POINT) = 1.0) */

    exp = RDB_ro_op("WHERE", ecp);
    assert(exp != NULL);
    
    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    wherep = RDB_var_ref("POINT", ecp);
    wherep = RDB_expr_comp(wherep, "X", ecp);
    wherep = RDB_eq(wherep, RDB_float_to_expr(1.0, ecp), ecp);
    RDB_add_arg(exp, wherep);

    tmptbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(tmptbp != NULL);

    ret = RDB_table_to_array(&array, tmptbp, 0, NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, NULL, ecp, &tx);
        assert(ret == RDB_OK);
        ret = RDB_obj_comp(pvalp, "Y", &yval, NULL, ecp, &tx);
        assert(ret == RDB_OK);

        printf("X=%f, Y=%f\n", (float)RDB_obj_float(&xval),
                (float)RDB_obj_float(&yval));
    }
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR);
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);

    RDB_drop_table(tmptbp, ecp, &tx);

    /* Creating POINTTEST WHERE POINT=POINT(1.0, 2.0) */

    exp = RDB_ro_op("WHERE", ecp);
    assert(exp != NULL);

    argp = RDB_table_ref(tbp, ecp);
    assert(argp != NULL);
    RDB_add_arg(exp, argp);

    compv[0] = RDB_float_to_expr(1.0, ecp);
    compv[1] = RDB_float_to_expr(2.0, ecp);
    wherep = RDB_ro_op("POINT", ecp);
    assert(wherep != NULL);

    RDB_add_arg(wherep, compv[0]);
    RDB_add_arg(wherep, compv[1]);
    wherep = RDB_eq(wherep, RDB_var_ref("POINT", ecp), ecp);
    RDB_add_arg(exp, wherep);

    tmptbp = RDB_expr_to_vtable(exp, ecp, &tx);
    assert(tmptbp != NULL);

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, tmptbp, 0, NULL, 0, ecp, &tx);
    assert(ret == RDB_OK);

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        RDB_object *pvalp = RDB_tuple_get(tplp, "POINT");

        ret = RDB_obj_comp(pvalp, "X", &xval, NULL, ecp, &tx);
        assert(ret == RDB_OK);
        ret = RDB_obj_comp(pvalp, "Y", &yval, NULL, ecp, &tx);
        assert(ret == RDB_OK);

        printf("X=%f, Y=%f\n", (float)RDB_obj_float(&xval),
                (float)RDB_obj_float(&yval));
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
