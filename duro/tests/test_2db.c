/* $Id$ */

#include <rel/rdb.h>
#include <stdio.h>

int
main(void) {
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("db", &envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    printf("Creating DB\n");
    ret = RDB_create_db_from_env("TEST2", envp, &dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    printf("Creating DB\n");
    ret = RDB_create_db_from_env("TEST2", envp, &dbp);
    if (ret != RDB_ELEMENT_EXISTS) {
        puts(RDB_strerror(ret));
        return 1;
    }
    puts("Element exists - OK");

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
