/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *utype_keyattrs1[] = { "NUMBER" };

RDB_string_vec utype_keyattrs[] = {
    { 1, utype_keyattrs1 }
};

RDB_type *tinyintp;

int
create_table(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_attr utype_attrs[2];
    int ret;
   
    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    utype_attrs[0].name = "NUMBER";
    tinyintp = RDB_get_type("TINYINT", ecp, &tx);
    if (tinyintp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }
    
    utype_attrs[0].typ = tinyintp;
    utype_attrs[0].defaultp = NULL;

    printf("Creating table UTYPETEST\n");
    tbp = RDB_create_table("UTYPETEST", RDB_TRUE, 1, utype_attrs,
            1, utype_keyattrs, ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(&tx);
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
    RDB_object ival;
    RDB_object tival;
    RDB_object *ivalp;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("UTYPETEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(&tx);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&ival);
    RDB_init_obj(&tival);

    printf("Trying to insert tuple with INTEGER\n");

    ret = RDB_tuple_set_int(&tpl, "NUMBER", 50);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, ecp, &tx);
    if (ret != RDB_TYPE_MISMATCH) {
        fprintf(stderr, "Wrong return code: %s\n", RDB_strerror(ret));
        goto error;
    }
    printf("Return code: %s - OK\n", RDB_strerror(ret));

    printf("Trying to create TINYINT from INTEGER=200\n");

    RDB_int_to_obj(&ival, (RDB_int)200);
    ivalp = &ival;

    ret = RDB_call_ro_op("TINYINT", 1, &ivalp, ecp, &tx, &tival);
    if (ret != RDB_TYPE_CONSTRAINT_VIOLATION) {
        fprintf(stderr, "Wrong return code: %s\n", RDB_strerror(ret));
        goto error;
    }
    printf("Return code: %s - OK\n", RDB_strerror(ret));

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
    if (ret != RDB_TYPE_CONSTRAINT_VIOLATION) {
        fprintf(stderr, "Wrong return code: %s\n", RDB_strerror(ret));
        goto error;
    }

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
        RDB_rollback(&tx);
        return ret;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&ival, ecp);
    RDB_destroy_obj(&tival, ecp);

    printf("End of transaction\n");
    return RDB_commit(&tx);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&ival, ecp);
    RDB_destroy_obj(&tival, ecp);

    RDB_rollback(&tx);
    return RDB_ERROR;
}

int
test_drop(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_table *tbp;
    RDB_transaction tx;
    RDB_object *errp;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    tbp = RDB_get_table("UTYPETEST", ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Dropping table %s\n", RDB_table_name(tbp));
    ret = RDB_drop_table(tbp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Dropping type %s\n", RDB_type_name(tinyintp));
    ret = RDB_drop_type(tinyintp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Trying to get type\n");
    tinyintp = RDB_get_type("TINYINT", ecp, &tx);
    errp = RDB_get_err(ecp);
    if (errp == NULL || RDB_obj_type(errp) != &RDB_NOT_FOUND_ERROR) {
        char *errtypename = NULL;
        if (errp != NULL && RDB_obj_type(errp) != NULL) {
            errtypename = RDB_type_name(RDB_obj_type(errp));
        }

        fprintf(stderr, "Wrong error type: %s\n", errtypename != NULL ?
                errtypename : "(null)");
        RDB_rollback(&tx);
        return RDB_ERROR;
    }
    printf("Return code: %s - OK\n", RDB_strerror(ret));

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

    ret = test_drop(dbp, &ec);
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
