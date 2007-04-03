/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_regexp(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp, *vtbp;
    RDB_object array;
    RDB_object *tplp;
    RDB_expression *exp, *argp;
    int ret;
    RDB_int i;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&array);

    /* Creating selection (NAME regmatch "o") */

    exp = RDB_ro_op("WHERE", ecp);
    if (exp == NULL) {
        goto error;
    }
    
    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        goto error;
    }
    RDB_add_arg(exp, argp);

    argp = RDB_ro_op("MATCHES", ecp);
    if (argp == NULL) {
        goto error;
    }
    RDB_add_arg(argp, RDB_var_ref("NAME", ecp));
    RDB_add_arg(argp, RDB_string_to_expr("o", ecp));
    RDB_add_arg(exp, argp);
    
    vtbp = RDB_expr_to_vtable(exp, ecp, &tx);
    if (vtbp == NULL) {
        goto error;
    }

    ret = RDB_table_to_array(&array, vtbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    } 

    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int) RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (double) RDB_tuple_get_double(tplp, "SALARY"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }

    RDB_destroy_obj(&array, ecp);

    RDB_drop_table(vtbp, ecp, &tx);

    return RDB_commit(ecp, &tx);

error:
    RDB_destroy_obj(&array, ecp);
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
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = test_regexp(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    return 0;
}
