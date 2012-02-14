/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

char *utype_keyattrs1[] = { "NUMBER" };

RDB_string_vec utype_keyattrs[] = {
    { 1, utype_keyattrs1 }
};

RDB_type *tinyintp;

int
create_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_attr utype_attrs[2];
    int ret;
   
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    utype_attrs[0].name = "NUMBER";
    tinyintp = RDB_get_type("TINYINT", ecp, &tx);
    if (tinyintp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    
    utype_attrs[0].typ = tinyintp;
    utype_attrs[0].defaultp = NULL;
    utype_attrs[0].options = 0;

    tbp = RDB_create_table("UTYPETEST", 1, utype_attrs,
            1, utype_keyattrs, ecp, &tx);
    assert(tbp != NULL);

    return RDB_commit(ecp, &tx);
}

int
test_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object tpl;
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_object ival;
    RDB_object tival;
    RDB_object *ivalp;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("UTYPETEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&ival);
    RDB_init_obj(&tival);

    ret = RDB_tuple_set_int(&tpl, "NUMBER", 50, ecp);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret != RDB_OK);

    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_TYPE_MISMATCH_ERROR);
    RDB_clear_err(ecp);

    RDB_int_to_obj(&ival, (RDB_int)200);
    ivalp = &ival;

    ret = RDB_call_ro_op_by_name("TINYINT", 1, &ivalp, ecp, &tx, &tival);
    assert(ret != RDB_OK);
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_TYPE_CONSTRAINT_VIOLATION_ERROR);
    RDB_clear_err(ecp);

    RDB_int_to_obj(&ival, (RDB_int) 99);
    ivalp = &ival;

    ret = RDB_call_ro_op_by_name("TINYINT", 1, &ivalp, ecp, &tx, &tival);
    if (ret != RDB_OK) {
        goto error;
    }

    /*
     * Try to set tival to illegal value by calling the setter function
     */
    RDB_int_to_obj(&ival, 200);
    ret = RDB_obj_set_comp(&tival, "TINYINT", &ival, NULL, ecp, &tx);
    assert(ret != RDB_OK);
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_TYPE_CONSTRAINT_VIOLATION_ERROR);
    RDB_clear_err(ecp);

    /*
     * Call selector again
     */
    RDB_int_to_obj(&ival, 99);
    ret = RDB_call_ro_op_by_name("TINYINT", 1, &ivalp, ecp, &tx, &tival);
    if (ret != RDB_OK) {
        goto error;
    }

    /*
     * Call setter with valid value
     */
    ret = RDB_obj_set_comp(&tival, "TINYINT", &ival, NULL, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_tuple_set(&tpl, "NUMBER", &tival, ecp);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&ival, ecp);
    RDB_destroy_obj(&tival, ecp);

    return RDB_commit(ecp, &tx);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&ival, ecp);
    RDB_destroy_obj(&tival, ecp);

    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

int
test_drop(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *tbp;
    RDB_transaction tx;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("UTYPETEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    ret = RDB_drop_table(tbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    ret = RDB_drop_type("TINYINT", ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    tinyintp = RDB_get_type("TINYINT", ecp, &tx);
    assert(tinyintp == NULL);
    assert(RDB_obj_type(RDB_get_err(ecp)) == &RDB_NAME_ERROR);
    RDB_clear_err(ecp);

    return RDB_commit(ecp, &tx);
}

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;

    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
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

    ret = test_drop(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    RDB_destroy_exec_context(&ec);

    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 2;
    }

    return 0;
}
