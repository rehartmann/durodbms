#include <rel/rdb.h>

/*
 * Implementation of user-defined read-only operator PLUS
 */
int
RDBU_plus(const char *name, int argc, RDB_value *argv[],
        RDB_value *retvalp, const char *iargp, RDB_transaction *txp)
{
    RDB_value_set_int(retvalp, RDB_value_int(argv[0]) + RDB_value_int(argv[1]));
    
    return RDB_OK;
}

/*
 * Implementation of user-defined update operator ADD
 */
int
RDBU_add(const char *name, int argc, RDB_value *argv[],
        int updargc, int updargv[], const char *iargp, RDB_transaction *txp)
{
    RDB_value_set_int(argv[0], RDB_value_int(argv[0]) + RDB_value_int(argv[1]));

    return RDB_OK;
}
