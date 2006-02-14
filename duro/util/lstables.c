/* $Id$ */

#include <rel/rdb.h>
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
    int ret;
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
        ret = RDB_open_env(envnamp, envpp);
        if (ret != RDB_OK)
            return ret;
        if (dbnamp != NULL) {
            *dbpp = RDB_get_db_from_env(dbnamp, *envpp, ecp);
            if (*dbpp == NULL) {
                RDB_close_env(*envpp);
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
    RDB_table *rt_tbp, *db_tbp;
    RDB_table *vtb1p = NULL;
    RDB_table *vtb2p = NULL;
    RDB_object array;
    RDB_object *tplp;
    RDB_expression *condp = NULL;
    RDB_int i;

    rt_tbp = RDB_get_table(real ? "SYS_RTABLES" : "SYS_VTABLES", ecp, txp);
    if (rt_tbp == NULL) {
        return RDB_ERROR;
    }

    db_tbp = RDB_get_table("SYS_DBTABLES", ecp, txp);
    if (db_tbp == NULL) {
        return RDB_ERROR;
    } 

    RDB_init_obj(&array);

    if (all) {
        condp = RDB_bool_to_expr(RDB_TRUE, ecp);
    } else {
        condp = RDB_expr_var("IS_USER", ecp);
    }
    vtb1p = RDB_select(rt_tbp, condp, ecp, txp);
    if (vtb1p == NULL) {
        RDB_drop_expr(condp, ecp);
        return RDB_ERROR;
    }

    condp = RDB_eq(RDB_expr_var("DBNAME", ecp),
                   RDB_string_to_expr(RDB_db_name(RDB_tx_db(txp)), ecp), ecp);
    vtb2p = RDB_select(db_tbp, condp, ecp, txp);
    if (vtb2p == NULL) {
        RDB_drop_expr(condp, ecp);
        goto error;
    }

    vtb1p = RDB_join(vtb1p, vtb2p, ecp);
    if (vtb1p == NULL) {
        goto error;
    }
    vtb2p = NULL;

    ret = RDB_table_to_array(&array, vtb1p, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    } 
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf(real ? "%s\n" : "%s*\n", RDB_tuple_get_string(tplp, "TABLENAME"));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);

    RDB_drop_table(vtb1p, ecp, txp);

    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    if (vtb1p != NULL)
        RDB_drop_table(vtb1p, ecp, txp);
    if (vtb2p != NULL)
        RDB_drop_table(vtb2p, ecp, txp);
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

    RDB_init_exec_context(&ec);
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

    RDB_set_errfile(envp, stderr);

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

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }
    
    return 0;
}
