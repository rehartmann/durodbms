#include <rel/rdb.h>
#include <rec/env.h>

#include <stdio.h>
#include <string.h>

/*
 * lstables - list table names
 *
 * Usage: lstables -e <environment> -d <db> [-a]
 *
 * List the names of all tables in the database db.
 * Virtual tables are marked by a trailing '*'.
 * Catalog tables are only listed if option '-a' is given.
 */

static int
getargs(RDB_exec_context *ecp, int *argcp, char **argvp[],
        RDB_environment **envpp, RDB_database **dbpp)
{
    char *envnamp = NULL;
    char *dbnamp = NULL;

    *envpp = NULL;
    *dbpp = NULL;

    (*argcp)--;
    (*argvp)++;
    while (*argcp >= 2) {
        if (strcmp((*argvp)[0], "-e") == 0) {
            envnamp = (*argvp)[1];
            *argvp += 2;
            *argcp -= 2;
        } else if (strcmp((*argvp)[0], "-d") == 0) {
            dbnamp = (*argvp)[1];
            *argvp += 2;
            *argcp -= 2;
        } else
            break;
    }
    if (envnamp != NULL) {
        *envpp = RDB_open_env(envnamp, 0, ecp);
        if (*envpp == NULL)
            return RDB_ERROR;
        if (dbnamp != NULL) {
            *dbpp = RDB_get_db_from_env(dbnamp, *envpp, ecp, NULL);
            if (*dbpp == NULL) {
                RDB_close_env(*envpp, ecp);
                return RDB_ERROR;
            }
        }
    }
    return RDB_OK;
}

int
print_tables(RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool all,
        RDB_bool real)
{
    int ret;
    RDB_object *rt_tbp, *db_tbp;
    RDB_object *vtbp = NULL;
    RDB_object array;
    RDB_object *tplp;
    RDB_expression *exp = NULL;
    RDB_expression *texp, *argp;
    RDB_int i;

    rt_tbp = RDB_get_table(real ? "sys_rtables" : "sys_vtables", ecp, txp);
    if (rt_tbp == NULL) {
        return RDB_ERROR;
    }

    db_tbp = RDB_get_table("sys_dbtables", ecp, txp);
    if (db_tbp == NULL) {
        return RDB_ERROR;
    } 

    RDB_init_obj(&array);

    exp = RDB_ro_op("join", ecp);
    if (exp == NULL)
        goto error;

    texp = RDB_ro_op("where", ecp);
    if (texp == NULL)
        goto error;
    RDB_add_arg(exp, texp);

    argp = RDB_table_ref(rt_tbp, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(texp, argp);

    if (all) {
        argp = RDB_bool_to_expr(RDB_TRUE, ecp);
    } else {
        argp = RDB_var_ref("is_user", ecp);
    }
    if (argp == NULL)
        goto error;
    RDB_add_arg(texp, argp);

    texp = RDB_ro_op("where", ecp);
    if (texp == NULL)
        goto error;
    RDB_add_arg(exp, texp);

    argp = RDB_table_ref(db_tbp, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(texp, argp);

    argp = RDB_eq(RDB_var_ref("dbname", ecp),
                   RDB_string_to_expr(RDB_db_name(RDB_tx_db(txp)), ecp), ecp);
    RDB_add_arg(texp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (vtbp == NULL) {
        goto error;
    }

    ret = RDB_table_to_array(&array, vtbp, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    } 
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf(real ? "%s\n" : "%s*\n", RDB_tuple_get_string(tplp, "tablename"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);

    RDB_drop_table(vtbp, ecp, txp);

    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    if (vtbp != NULL)
        RDB_drop_table(vtbp, ecp, txp);
    return RDB_ERROR;
}

int
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    RDB_transaction tx;
    int ret;
    RDB_exec_context ec;
    RDB_bool all = RDB_FALSE;
    /* DB_ENV *bdbenv; */

    RDB_init_exec_context(&ec);
    if (RDB_init_builtin(&ec) != RDB_OK) {
        fputs("FATAL: cannot initialize\n", stderr);
        return 2;
    }

    ret = getargs(&ec, &argc, &argv, &envp, &dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n",
                RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    if (envp == NULL || dbp == NULL) {
        fprintf(stderr, "usage: lstables -e <environment> -d <database> [-a]\n");
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    RDB_env_set_errfile(envp, stderr);

    if (argc == 1 && strcmp(argv[0], "-a") == 0)
        all = RDB_TRUE;

    ret = RDB_begin_tx(&ec, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }
    
    ret = print_tables(&ec, &tx, all, RDB_TRUE);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_rollback(&ec, &tx);
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = print_tables(&ec, &tx, all, RDB_FALSE);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_rollback(&ec, &tx);
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = RDB_commit(&ec, &tx);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(envp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }
    
    return 0;
}
