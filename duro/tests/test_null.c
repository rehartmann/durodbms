/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
create_table(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_str_vec key;
    int ret;
    
    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return ret;
    }

    printf("Creating table DEEDUM\n");
    key.strv = NULL;
    key.strc = 0;
    ret = RDB_create_table("DEEDUM", RDB_TRUE, 0, NULL, 1, &key, &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    printf("End of transaction\n");
    ret = RDB_commit(&tx);
    return ret;
}

int
test_table(RDB_database *dbp)
{
    int ret;
    RDB_tuple tpl;
    RDB_transaction tx;
    RDB_table *tbp;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("DEEDUM", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_tuple(&tpl);

    printf("Inserting tuple #1\n");
    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Inserting tuple #2\n");
    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS ) {
        RDB_rollback(&tx);
        return ret;
    }
    if (ret == RDB_ELEMENT_EXISTS)
        printf("Error: element exists - OK\n");    
    RDB_destroy_tuple(&tpl);

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("db", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = create_table(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = test_table(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
