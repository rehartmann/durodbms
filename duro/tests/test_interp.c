/*
 * test_interp.c
 *
 *  Created on: 17.08.2013
 *      Author: Rene Hartmann
 */

/* $Id$ */

#include <rel/rdb.h>
#include <dli/iinterp.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

int
main(void)
{
    RDB_environment *dsp;
    RDB_database *dbp;
    RDB_exec_context ec;
    int ret;

    ret = RDB_open_env("dbenv", &dsp, RDB_CREATE);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", dsp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    assert(Duro_init_interp(&ec, RDB_db_name(dbp)) == RDB_OK);

    if (Duro_dt_execute_str(RDB_db_env(dbp),
            "put_line ('Test');"
            "begin tx;"
            "put_line (cast_as_string(count(depts)));"
            "commit;",
            &ec) != RDB_OK) {
        Duro_print_error(RDB_get_err(&ec));
        abort();
    }

    RDB_destroy_exec_context(&ec);
    Duro_exit_interp();

    return 0;
}
