#include <rel/rdb.h>
#include <db.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

int
create_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_string_vec key;
    int ret;
    
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    key.strv = NULL;
    key.strc = 0;
    tbp = RDB_create_table("DEEDUM", 0, NULL, 1, &key, ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    return RDB_commit(ecp, &tx);
}

int
test_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object tpl;
    RDB_transaction tx;
    RDB_object *tbp;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("DEEDUM", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    /* Inserting tuple #1 */
    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }

    /* Inserting tuple #2 */
    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret == RDB_ERROR && RDB_obj_type(RDB_get_err(ecp)) == &RDB_ELEMENT_EXISTS_ERROR);
    RDB_clear_err(ecp);
    RDB_destroy_obj(&tpl, ecp);

    return RDB_commit(ecp, &tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    dsp = RDB_open_env("dbenv", RDB_RECOVER, &ec);
    if (dsp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = create_table(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = test_table(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = RDB_close_env(dsp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
