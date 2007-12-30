/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *projattrs1[] = { "NAME" };

int
test_ra(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_expression *exp, *argp;
    RDB_object *tb1p, *tb2p, *vtbp;
    RDB_object array;
    RDB_object *tplp;
    int ret;
    RDB_int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tb1p = RDB_get_table("EMPS1", ecp, &tx);
    if (tb1p == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    tb2p = RDB_get_table("EMPS2", ecp, &tx);
    if (tb2p == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    exp = RDB_ro_op("INTERSECT", ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    argp = RDB_table_ref(tb1p, ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_table_ref(tb2p, ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    
    argp = exp;

    exp = RDB_ro_op("PROJECT", ecp);
    if (exp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    
    argp = RDB_string_to_expr("NAME", ecp);
    if (argp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);    

    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    ret = RDB_table_to_array(&array, vtbp, 0, NULL, 0, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&array, ecp);
        RDB_commit(ecp, &tx);
        return ret;
    }

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
    }
    RDB_destroy_obj(&array, ecp);

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_commit(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_drop_table(vtbp, ecp, &tx);

    return RDB_commit(ecp, &tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    ret = test_ra(dbp, &ec);
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
