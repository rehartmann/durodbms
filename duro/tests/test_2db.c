/* $Id$ */

#include <rel/rdb.h>
#include <stdio.h>

int
main(void) {
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    RDB_init_exec_context(&ec);
    printf("Creating DB\n");
    dbp = RDB_create_db_from_env("TEST2", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    printf("Creating DB\n");
    dbp = RDB_create_db_from_env("TEST2", envp, &ec);
    if (ret != RDB_ELEMENT_EXISTS) { /* !! */
        puts(RDB_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 1;
    }
    puts("Element exists - OK");
    RDB_destroy_exec_context(&ec);

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
