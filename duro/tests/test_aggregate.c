/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
test_aggregate(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_rational avg;
    int ret;

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating aggregation COUNT ( EMPS1 )\n");

    ret = RDB_cardinality(tbp, &tx);
    if (ret < 0) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Count is %d\n", ret);

    printf("Creating aggregation AVG ( EMPS1, SALARY )\n");

    ret = RDB_avg(tbp, "SALARY", &tx, &avg);
    if (ret != RDB_OK) {
        RDB_commit(&tx);
        return ret;
    }

    printf("Average is %f\n", (float)avg);

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
    ret = RDB_open_env("dbenv", &dsp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    ret = RDB_get_db_from_env("TEST", dsp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_aggregate(dbp);
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
