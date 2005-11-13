/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
create_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_string_vec key;
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
    tbp = RDB_create_table("DEEDUM", RDB_TRUE, 0, NULL, 1, &key, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
test_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object tpl;
    RDB_transaction tx;
    RDB_table *tbp;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("DEEDUM", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    printf("Inserting tuple #1\n");
    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }

    printf("Inserting tuple #2\n");
    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_ELEMENT_EXISTS_ERROR) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        printf("Error: element exists - OK\n");
        RDB_clear_err(ecp);
    }
    RDB_destroy_obj(&tpl, ecp);

    printf("End of transaction\n");
    return RDB_commit(&tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = create_table(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_table(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
