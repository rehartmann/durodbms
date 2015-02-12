/* $Id$ */

#include <rel/rdb.h>
#include <rel/typeimpl.h>
#include <math.h>
#include <tests/point.h>

/*
 * Defines selector, setters, and getters for type point
 */

int
point(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point ipt;

    ipt.x = RDB_obj_float(argv[0]);
    ipt.y = RDB_obj_float(argv[1]);

    return RDB_irep_to_obj(valp, RDB_return_type(op), &ipt, sizeof ipt, ecp);
}

int
point_set_x(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    i_point *iptp = RDB_obj_irep(argv[0], NULL);

    iptp->x = RDB_obj_float(argv[1]);

    return RDB_OK;
}

int
point_get_x(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) argv[0], NULL);

    RDB_float_to_obj(valp, iptp->x);

    return RDB_OK;
}

int
point_set_y(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    i_point *iptp = RDB_obj_irep(argv[0], NULL);

    iptp->y = RDB_obj_float(argv[1]);

    return RDB_OK;
}

int
point_get_y(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) argv[0], NULL);

    RDB_float_to_obj(valp, iptp->y);

    return RDB_OK;
}

/*
 * Selector, setters, and getter for POLAR
 */

int
polar(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point ipt;
    double th = (double) RDB_obj_float(argv[0]);
    double len = (double) RDB_obj_float(argv[1]);

    ipt.x = cos(th) * len;
    ipt.y = sin(th) * len;

    return RDB_irep_to_obj(valp, RDB_return_type(op), &ipt, sizeof ipt, ecp);
}

int
point_set_theta(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) argv[0], NULL);
    float len = sqrt(iptp->x * iptp->x + iptp->y * iptp->y);
    float theta = RDB_obj_float(argv[1]);

    iptp->x = cos(theta) * len;
    iptp->y = sin(theta) * len;

    return RDB_OK;
}

int
point_get_theta(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) argv[0], NULL);

    RDB_float_to_obj(valp, atan2((double) iptp->y, (double) iptp->x));

    return RDB_OK;
}

int
point_set_length(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    i_point *iptp = RDB_obj_irep(argv[0], NULL);
    float len = (float) RDB_obj_float(argv[1]);
    float theta = atan2((double) iptp->y, (double) iptp->x);

    iptp->x = cos(theta) * len;
    iptp->y = sin(theta) * len;

    return RDB_OK;
}

int
point_get_length(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) argv[0], NULL);

    RDB_float_to_obj(valp, sqrt(iptp->x * iptp->x + iptp->y * iptp->y));

    return RDB_OK;
}
