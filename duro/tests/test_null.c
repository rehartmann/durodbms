/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
create_table(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_key_attrs key;
    int err;
    
    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return err;
    }

    printf("Creating table DEEDUM\n");
    key.attrv = NULL;
    key.attrc = 0;
    err = RDB_create_table("DEEDUM", RDB_TRUE, 0, NULL, 1, &key, &tx, &tbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    printf("End of transaction\n");
    err = RDB_commit(&tx);
    return err;
}

int
test_table(RDB_database *dbp)
{
    int err;
    RDB_tuple tpl;
    RDB_transaction tx;
    RDB_table *tbp;

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    RDB_get_table(dbp, "DEEDUM", &tbp);

    RDB_init_tuple(&tpl);

    printf("Inserting tuple #1\n");
    err = RDB_insert(tbp, &tpl, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Inserting tuple #2\n");
    err = RDB_insert(tbp, &tpl, &tx);
    if (err != RDB_OK && err != RDB_ELEMENT_EXISTS ) {
        RDB_rollback(&tx);
        return err;
    }
    if (err == RDB_ELEMENT_EXISTS)
        printf("Error: element exists - OK\n");    
    RDB_destroy_tuple(&tpl);

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main()
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int err;
    
    printf("Opening environment\n");
    err = RDB_open_env("db", &dsp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }
    err = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (err != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }

    err = create_table(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = test_table(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    printf ("Closing environment\n");
    err = RDB_close_env(dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    return 0;
}
