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
print_tables(RDB_transaction *txp, RDB_bool all, RDB_bool real)
{
    int ret;
    RDB_table *rt_tbp, *db_tbp;
    RDB_table *vtb1p = NULL;
    RDB_table *vtb2p = NULL;
    RDB_array array;
    RDB_tuple *tplp;
    RDB_expression *condp = NULL;
    RDB_int i;

    ret = RDB_get_table(real ? "SYS_RTABLES" : "SYS_VTABLES", txp, &rt_tbp);
    if (ret != RDB_OK) {
        return ret;
    } 

    ret = RDB_get_table("SYS_DBTABLES", txp, &db_tbp);
    if (ret != RDB_OK) {
        return ret;
    } 

    RDB_init_array(&array);

    if (all) {
        condp = RDB_bool_const(RDB_TRUE);
    } else {
        condp = RDB_expr_attr("IS_USER", &RDB_BOOLEAN);
    }
    ret = RDB_select(rt_tbp, condp, &vtb1p);
    if (ret != RDB_OK) {
        RDB_drop_expr(condp);
        return ret;
    }

    condp = RDB_eq(RDB_expr_attr("DBNAME", &RDB_STRING),
                   RDB_string_const(RDB_db_name(RDB_tx_db(txp))));
    ret = RDB_select(db_tbp, condp, &vtb2p);
    if (ret != RDB_OK) {
        RDB_drop_expr(condp);
        goto error;
    }

    ret = RDB_join(vtb1p, vtb2p, &vtb1p);
    if (ret != RDB_OK) {
        goto error;
    }
    vtb2p = NULL;

    ret = RDB_table_to_array(&array, vtb1p, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    } 
    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tplp)) == RDB_OK; i++) {
        printf(real ? "%s\n" : "%s*\n", RDB_tuple_get_string(tplp, "TABLENAME"));
    }
    if (ret != RDB_NOT_FOUND) {
        goto error;
    }

    RDB_destroy_array(&array);

    RDB_drop_table(vtb1p, txp);

    return RDB_OK;

error:
    RDB_destroy_array(&array);
    if (vtb1p != NULL)
        RDB_drop_table(vtb1p, txp);
    if (vtb2p != NULL)
        RDB_drop_table(vtb2p, txp);
    return ret;
}

int
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    RDB_transaction tx;
    int ret;
    RDB_bool all = RDB_FALSE;

    ret = RDB_getargs(&argc, &argv, &envp, &dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(ret));
        return 2;
    }

    if (envp == NULL || dbp == NULL) {
        fprintf(stderr, "usage: lstables -e <environment> -d <database> [-a]\n");
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    if (argc == 1 && strcmp(argv[0], "-a") == 0)
        all = RDB_TRUE;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(ret));
        return 1;
    }
    
    ret = print_tables(&tx, all, RDB_TRUE);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(ret));
        RDB_rollback(&tx);
        return 1;
    }

    ret = print_tables(&tx, all, RDB_FALSE);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(ret));
        RDB_rollback(&tx);
        return 1;
    }

    ret = RDB_commit(&tx);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "lstables: %s\n", RDB_strerror(ret));
        return 1;
    }
    
    return 0;
}
