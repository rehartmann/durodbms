#include <rel/rdb.h>

/*
 * Implementation of user-defined read-only operator 'plus'
 */
int
RDBU_plus(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, RDB_obj_int(argv[0]) + RDB_obj_int(argv[1]));
    
    return RDB_OK;
}

/*
 * Implementation of user-defined update operator ADD
 */
int
RDBU_add(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_int_to_obj(argv[0], RDB_obj_int(argv[0]) + RDB_obj_int(argv[1]));

    return RDB_OK;
}
