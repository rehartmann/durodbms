/* $Id$ */

#include <rel/rdb.h>
#include <stdio.h>

int
delete_table(RDB_database *dbp, const char *tbnamp)
{
    int err;
    RDB_table *tbp;
    RDB_transaction tx;

    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    } 

    err = RDB_get_table(tbnamp, &tx, &tbp);
    if (err != RDB_OK) {
        goto error;
    } 

    err = RDB_commit(&tx);
    if (err != RDB_OK) {
        return err;
    } 

    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    } 

    err = RDB_drop_table(tbp, &tx);
    if (err != RDB_OK)
        goto error;

    RDB_commit(&tx);   
    return RDB_OK;
error:
    RDB_rollback(&tx);
    return err;
}

int
main(int argc, char *argv[])
{
    RDB_environment *envp;
    RDB_database *dbp;
    int err;
    int i;

    err = RDB_getargs(&argc, &argv, &envp, &dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "droptable: %s\n", RDB_strerror(err));
        return 2;
    }

    if (envp == NULL || dbp == NULL) {
        fprintf(stderr, "usage: droptable -e <environment> -d <database> table ...\n");
        return 1;
    }

    RDB_internal_env(envp)->set_errfile(RDB_internal_env(envp), stderr);

    if (argc == 0) {
        fputs("droptable: missing argument(s)\n", stderr);
        return 1;
    }

    for (i = 0; i < argc; i++) {
        err = delete_table(dbp, argv[i]);
        if (err != RDB_OK) {
            fprintf(stderr, "error: %s\n", RDB_strerror(err));
            return 1;
        }
    }

    err = RDB_release_db(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "droptable: %s\n", RDB_strerror(err));
        return 1;
    }

    err = RDB_close_env(envp);
    if (err != RDB_OK) {
        fprintf(stderr, "droptable: %s\n", RDB_strerror(err));
        return 1;
    }
    
    return 0;
}
