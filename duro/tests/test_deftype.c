/* $Id$ */

#include <rel/rdb.h>
#include <rel/typeimpl.h>
#include <stdlib.h>
#include <stdio.h>

int
test_type(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_possrep pr;
    RDB_expression *constraintp;
    RDB_attr comp;
    int ret;

    if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK) {
        return RDB_ERROR;
    }

    comp.name = NULL;
    comp.typ = &RDB_INTEGER;
    pr.name = NULL;
    pr.compc = 1;
    pr.compv = &comp;
    constraintp = RDB_ro_op("<", ecp);
    if (constraintp == NULL) {
        return RDB_ERROR;
    }
    RDB_add_arg(constraintp,
            RDB_expr_comp(RDB_var_ref("TINYINT", ecp), "TINYINT", ecp));
    RDB_add_arg(constraintp, RDB_int_to_expr(100, ecp));

    ret = RDB_define_type("TINYINT", 1, &pr, constraintp, ecp, &tx);
    RDB_del_expr(constraintp, ecp);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    ret = RDB_implement_type("TINYINT", NULL, -1, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK) {
        return ret;
    }
    return RDB_OK;
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    ret = RDB_open_env("dbenv", &envp, RDB_CREATE);
    if (ret != 0) {
        fprintf(stderr, "Cannot open db environment: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n",
                RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = test_type(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n",
                 RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;
}
