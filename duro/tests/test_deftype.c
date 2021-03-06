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
    RDB_expression *initexp;
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
    RDB_add_arg(constraintp, RDB_var_ref("TINYINT", ecp));
    RDB_add_arg(constraintp, RDB_int_to_expr(100, ecp));

    initexp = RDB_ro_op("TINYINT", ecp);
    if (initexp == NULL) {
        return RDB_ERROR;
    }
    RDB_add_arg(initexp, RDB_int_to_expr(0, ecp));

    ret = RDB_define_type("TINYINT", 1, &pr, constraintp, initexp, 0,
            ecp, &tx);
    RDB_del_expr(constraintp, ecp);
    RDB_del_expr(initexp, ecp);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    ret = RDB_implement_type("TINYINT", NULL, RDB_SYS_REP, ecp, &tx);
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
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    RDB_init_exec_context(&ec);
    envp = RDB_open_env(argc <= 1 ? "dbenv" : argv[1], RDB_RECOVER, &ec);
    if (envp == NULL) {
        fprintf(stderr, "Error: %s\n",
                RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    RDB_env_set_errfile(envp, stderr);

    dbp = RDB_get_db_from_env("TEST", envp, &ec, NULL);
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

    ret = RDB_close_env(envp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n",
                 RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
