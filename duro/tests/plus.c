#include <rel/rdb.h>

/*
 * Implementation of user-defined read-only operator PLUS
 */
int
RDBU_plus(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, RDB_obj_int(argv[0]) + RDB_obj_int(argv[1]));
    
    return RDB_OK;
}

/*
 * Implementation of user-defined update operator ADD
 */
int
RDBU_add(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen, RDB_transaction *txp)
{
    RDB_int_to_obj(argv[0], RDB_obj_int(argv[0]) + RDB_obj_int(argv[1]));

    return RDB_OK;
}
