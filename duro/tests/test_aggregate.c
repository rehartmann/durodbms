/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_aggregate(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_value val;
    int err;

    err = RDB_get_table(dbp, "EMPS1", &tbp);
    if (err != RDB_OK) {
        return err;
    }

    printf("Starting transaction\n");
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        return err;
    }

    printf("Creating aggregation COUNT ( EMPS1 )\n");

    err = RDB_aggregate(tbp, RDB_COUNT, NULL, &tx, &val);
    if (err != RDB_OK) {
        RDB_commit(&tx);
        return err;
    }

    printf("Count is %d\n", (int)val.var.int_val);

    printf("Creating aggregation AVG ( EMPS1, SALARY )\n");

    err = RDB_aggregate(tbp, RDB_AVG, "SALARY", &tx, &val);
    if (err != RDB_OK) {
        RDB_commit(&tx);
        return err;
    }

    printf("Average is %f\n", (float)val.var.rational_val);

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

    err = test_aggregate(dbp);
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
