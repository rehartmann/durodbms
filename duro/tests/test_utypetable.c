/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

char *utype_keyattrs1[] = { "NAME" };

RDB_key_attrs utype_keyattrs[] = {
    { utype_keyattrs1, 1 }
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

    RDB_value_set_int(&ival, (RDB_int)200);
    ivalp = &ival;

    ret = RDB_value_set(&tival, tinyintp, "TINYINT", &ivalp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_tuple_set(&tpl, "NUMBER", &tival);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Inserting tuple #1\n");
    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }
/*
    printf("Inserting tuple #2\n");
    ret = RDB_insert(tbp, &tpl, &tx);
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS ) {
        RDB_rollback(&tx);
        return ret;
    }
    if (ret == RDB_ELEMENT_EXISTS)
        printf("Error: element exists - OK\n");
*/
    RDB_destroy_tuple(&tpl);
    RDB_destroy_value(&tival);

    printf("End of transaction\n");
    return RDB_commit(&tx);
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
