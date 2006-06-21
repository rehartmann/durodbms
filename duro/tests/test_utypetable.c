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
   
    printf("Starting transaction\n");
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

    printf("Creating table UTYPETEST\n");
    tbp = RDB_create_table("UTYPETEST", RDB_TRUE, 1, utype_attrs,
            1, utype_keyattrs, ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }
    printf("Table %s created.\n", RDB_table_name(tbp));

    printf("End of transaction\n");
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
    RDB_type *errtyp;

    printf("Starting transaction\n");
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

    printf("Trying to insert tuple with INTEGER\n");

    ret = RDB_tuple_set_int(&tpl, "NUMBER", 50, ecp);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    assert(ret != RDB_OK);

    errtyp = RDB_obj_type(RDB_get_err(ecp));
    if (errtyp != &RDB_TYPE_MISMATCH_ERROR) {
        fprintf(stderr, "Wrong error type: %s\n", RDB_type_name(errtyp));
        goto error;
    }
    RDB_clear_err(ecp);
    printf("Error: type mismatch - OK\n");

    printf("Trying to create TINYINT from INTEGER=200\n");

    RDB_int_to_obj(&ival, (RDB_int)200);
    ivalp = &ival;

    ret = RDB_call_ro_op("TINYINT", 1, &ivalp, ecp, &tx, &tival);
    if (ret == RDB_OK) {
        fprintf(stderr, "Operator call should fail, but did not\n");
        goto error;
    }
    errtyp = RDB_obj_type(RDB_get_err(ecp));
    if (errtyp != &RDB_TYPE_CONSTRAINT_VIOLATION_ERROR) {
        fprintf(stderr, "Wrong error type: %s\n", RDB_type_name(errtyp));
        goto error;
    }
    RDB_clear_err(ecp);
    printf("Error: type constraint violation - OK\n");

    printf("Creating TINYINT from INTEGER=99\n");

    RDB_int_to_obj(&ival, (RDB_int) 99);
    ivalp = &ival;

    ret = RDB_call_ro_op("TINYINT", 1, &ivalp, ecp, &tx, &tival);
    if (ret != RDB_OK) {
        goto error;
    }

    /*
     * Try to set tival to illegal value by calling the setter function
     */
    RDB_int_to_obj(&ival, 200);
    ret = RDB_obj_set_comp(&tival, "TINYINT", &ival, ecp, &tx);
    if (ret == RDB_OK) {
        fprintf(stderr, "Setter call should fail, but did not\n");
        goto error;
    }
    errtyp = RDB_obj_type(RDB_get_err(ecp));
    if (errtyp != &RDB_TYPE_CONSTRAINT_VIOLATION_ERROR) {
        fprintf(stderr, "Wrong error type: %s\n", RDB_type_name(errtyp));
        goto error;
    }
    RDB_clear_err(ecp);

    /*
     * Call selector again
     */
    RDB_int_to_obj(&ival, 99);
    ret = RDB_call_ro_op("TINYINT", 1, &ivalp, ecp, &tx, &tival);
    if (ret != RDB_OK) {
        goto error;
    }

    /*
     * Call setter with valid value
     */
    ret = RDB_obj_set_comp(&tival, "TINYINT", &ival, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_tuple_set(&tpl, "NUMBER", &tival, ecp);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Inserting Tuple\n");

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&ival, ecp);
    RDB_destroy_obj(&tival, ecp);

    printf("End of transaction\n");
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

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("UTYPETEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    printf("Dropping table %s\n", RDB_table_name(tbp));
    ret = RDB_drop_table(tbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    printf("Dropping type %s\n", RDB_type_name(tinyintp));
    ret = RDB_drop_type(tinyintp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        return ret;
    }

    printf("Trying to get type\n");
    tinyintp = RDB_get_type("TINYINT", ecp, &tx);
    if (tinyintp == NULL) {
        RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
        if (errtyp != &RDB_NOT_FOUND_ERROR) {
            fprintf(stderr, "Wrong error type: %s\n", RDB_type_name(errtyp));
            RDB_rollback(ecp, &tx);
            return RDB_ERROR;
        }
    }
    printf("Error: not found - OK\n");

    printf("End of transaction\n");
    return RDB_commit(ecp, &tx);
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

    printf ("Closing environment\n");
    ret = RDB_close_env(dsp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 2;
    }

    return 0;
}
