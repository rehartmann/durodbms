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

int
print_tables(RDB_database *dbp, RDB_bool all, RDB_bool real)
{
    int err;
    RDB_table *rt_tbp, *db_tbp;
    RDB_table *vtb1p = NULL;
    RDB_table *vtb2p = NULL;
    RDB_array array;
    RDB_tuple tpl;
    RDB_transaction tx;
    RDB_expression *condp = NULL;
    RDB_int i;

    err = RDB_get_table(dbp, real ? "SYSRTABLES" : "SYSVTABLES", &rt_tbp);
    if (err != RDB_OK) {
        return err;
    } 

    err = RDB_get_table(dbp, "SYSDBTABLES", &db_tbp);
    if (err != RDB_OK) {
        return err;
    } 

    RDB_init_array(&array);

    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        RDB_destroy_array(&array);
        return err;
    } 

    if (all) {
        condp = RDB_bool_const(RDB_TRUE);
    } else {
        condp = RDB_expr_attr("IS_USER", &RDB_BOOLEAN);
    }
    err = RDB_select(rt_tbp, condp, &vtb1p);
    if (err != RDB_OK) {
        RDB_drop_expr(condp);
        return err;
    }

    condp = RDB_eq(RDB_expr_attr("DBNAME", &RDB_STRING),
                   RDB_string_const(RDB_db_name(dbp)));
    err = RDB_select(db_tbp, condp, &vtb2p);
    if (err != RDB_OK) {
        RDB_drop_expr(condp);
        goto error;
    }

    err = RDB_join(vtb1p, vtb2p, &vtb1p);
    if (err != RDB_OK) {
        goto error;
    }
    vtb2p = NULL;

    err = RDB_table_to_array(vtb1p, &array, 0, NULL, &tx);
    if (err != RDB_OK) {
        goto error;
    } 
    
    RDB_init_tuple(&tpl);
    for (i = 0; (err = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf(real ? "%s\n" : "%s*\n", RDB_tuple_get_string(&tpl, "TABLENAME"));
    }
    RDB_destroy_tuple(&tpl);
    if (err != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);

    RDB_drop_table(vtb1p, &tx);

    RDB_commit(&tx);   
    return RDB_OK;

error:
    RDB_destroy_array(&array);
    if (vtb1p != NULL)
        RDB_drop_table(vtb1p, &tx);
    if (vtb2p != NULL)
        RDB_drop_table(vtb2p, &tx);
    RDB_rollback(&tx);
    return err;
}

int
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    int err;
    RDB_bool all = RDB_FALSE;

    err = RDB_getargs(&argc, &argv, &envp, &dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(err));
        return 2;
    }

    if (envp == NULL || dbp == NULL) {
        fprintf(stderr, "usage: lstables -e <environment> -d <database> [-a]\n");
        return 1;
    }

    RDB_internal_env(envp)->set_errfile(RDB_internal_env(envp), stderr);

    if (argc == 1 && strcmp(argv[0], "-a") == 0)
        all = RDB_TRUE;
    
    err = print_tables(dbp, all, RDB_TRUE);
    if (err != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(err));
        return 1;
    }

    err = print_tables(dbp, all, RDB_FALSE);
    if (err != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(err));
        return 1;
    }

    err = RDB_close_env(envp);
    if (err != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(err));
        return 1;
    }
    
    return 0;
}
