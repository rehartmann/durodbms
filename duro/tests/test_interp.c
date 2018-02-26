/*
 * test_interp.c
 *
 *  Created on: 17.08.2013
 *      Author: Rene Hartmann
 */

#include <rel/rdb.h>

#include <dli/iinterp.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

int
main(int argc, char *argv[])
{
    RDB_environment *dsp;
    RDB_database *dbp;
    RDB_exec_context ec;
    Duro_interp interp;

    RDB_init_exec_context(&ec);
    dsp = RDB_open_env(argc <= 1 ? "dbenv" : argv[1], RDB_RECOVER, &ec);
    if (dsp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    dbp = RDB_get_db_from_env("TEST", dsp, &ec, NULL);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    assert(Duro_init_interp(&interp, &ec, RDB_db_env(dbp),
            RDB_db_name(dbp)) == RDB_OK);

    if (Duro_dt_execute_str("io.put_line ('Test');"
            "begin tx;"
            "io.put_line (cast_as_string(count(depts)));"
            "commit;",
            &interp, &ec) != RDB_OK) {
        Duro_println_error(RDB_get_err(&ec));
        abort();
    }

    RDB_destroy_exec_context(&ec);
    Duro_destroy_interp(&interp);

    return 0;
}
