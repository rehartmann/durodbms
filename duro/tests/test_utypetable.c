/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *utype_keyattrs1[] = { "NUMBER" };

RDB_key_attrs utype_keyattrs[] = {
    { 1, utype_keyattrs1 }
};

RDB_type *tinyintp;

int
create_table(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_attr utype_attrs[2];
    int ret;
   
    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return ret;
    }

    utype_attrs[0].name = "NUMBER";
    ret = RDB_get_type("TINYINT", &tx, &tinyintp);
    utype_attrs[0].type = tinyintp;
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_rollback(&tx);
        return ret;
    }
    utype_attrs[0].defaultp = NULL;

    printf("Creating table UTYPETEST\n");
    ret = RDB_create_table("UTYPETEST", RDB_TRUE, 1, utype_attrs,
            1, utype_keyattrs, &tx, &tbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
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
    RDB_value ival;
    RDB_value tival;
    RDB_value *ivalp;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("UTYPETEST", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_tuple(&tpl);
    RDB_init_value(&ival);
    RDB_init_value(&tival);

    printf("Trying to insert tuple with INTEGER\n");

    ret = RDB_tuple_set_int(&tpl, "NUMBER", 50);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_TYPE_MISMATCH) {
        fprintf(stderr, "Wrong return code: %s\n", RDB_strerror(ret));
        goto error;
    }
    printf("Return code: %s - OK\n", RDB_strerror(ret));

    printf("Trying to create TINYINT fromt INTEGER=200\n");

    RDB_value_set_int(&ival, (RDB_int)200);
    ivalp = &ival;

    ret = RDB_select_value(&tival, tinyintp, "TINYINT", &ivalp);
    if (ret != RDB_TYPE_CONSTRAINT_VIOLATION) {
        fprintf(stderr, "Wrong return code: %s\n", RDB_strerror(ret));
        goto error;
    }
    printf("Return code: %s - OK\n", RDB_strerror(ret));

    printf("Creating TINYINT from INTEGER=99\n");

    RDB_value_set_int(&ival, (RDB_int)99);
    ivalp = &ival;

    ret = RDB_select_value(&tival, tinyintp, "TINYINT", &ivalp);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_tuple_set(&tpl, "NUMBER", &tival);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Inserting Tuple\n");

    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_destroy_tuple(&tpl);
    RDB_destroy_value(&ival);
    RDB_destroy_value(&tival);

    printf("End of transaction\n");
    return RDB_commit(&tx);
error:
    RDB_destroy_tuple(&tpl);
    RDB_destroy_value(&ival);
    RDB_destroy_value(&tival);

    RDB_rollback(&tx);
    return ret;
}

int
main()
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