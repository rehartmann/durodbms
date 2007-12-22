/* $Id$ */

#include <rel/rdb.h>
#include <rel/typeimpl.h>
#include <math.h>
#include <tests/point.h>

/*
 * Defines selector, setters, and getters for type POINT
 */

#ifndef M_PI_2
#define M_PI_2 (1.57079632679489661923)
#endif

int
POINT(const char *name, int argc, RDB_object *compv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point ipt;
    RDB_type *typ = RDB_get_type("POINT", ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    ipt.x = RDB_obj_float(compv[0]);
    ipt.y = RDB_obj_float(compv[1]);

    return RDB_irep_to_obj(valp, typ, &ipt, sizeof ipt, ecp);
}

int
POINT_set_X(const char *name, int argc, RDB_object *argv[], RDB_bool updv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    i_point *iptp = RDB_obj_irep(argv[0], NULL);

    iptp->x = RDB_obj_float(argv[1]);

    return RDB_OK;
}

int
POINT_get_X(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) argv[0], NULL);

    RDB_float_to_obj(valp, iptp->x);

    return RDB_OK;
}

int
POINT_set_Y(const char *name, int argc, RDB_object *argv[], RDB_bool updv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    i_point *iptp = RDB_obj_irep(argv[0], NULL);

    iptp->y = RDB_obj_float(argv[1]);

    return RDB_OK;
}

int
POINT_get_Y(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
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
POLAR(const char *name, int argc, RDB_object *compv[], const void *iargp,
        size_t iarglen, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *valp)
{
    i_point ipt;
    double th = (double) RDB_obj_float(compv[0]);
    double len = (double) RDB_obj_float(compv[1]);
    RDB_type *typ = RDB_get_type("POINT", ecp, txp);
    if (typ == NULL)
        return RDB_ERROR;

    ipt.x = cos(th) * len;
    ipt.y = sin(th) * len;

    return RDB_irep_to_obj(valp, typ, &ipt, sizeof ipt, ecp);
}

int
POINT_set_THETA(const char *name, int argc, RDB_object *argv[], RDB_bool updv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
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
POINT_get_THETA(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) argv[0], NULL);

    RDB_float_to_obj(valp, iptp->x != 0.0 ?
            atan(iptp->y / iptp->x) : M_PI_2);

    return RDB_OK;
}

int
POINT_set_LENGTH(const char *name, int argc, RDB_object *argv[], RDB_bool updv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    i_point *iptp = RDB_obj_irep(argv[0], NULL);
    float len = (float) RDB_obj_float(argv[1]);
    float theta = iptp->x != 0.0 ? atan(iptp->y / iptp->x) : M_PI_2;

    iptp->x = cos(theta) * len;
    iptp->y = sin(theta) * len;

    return RDB_OK;
}

int
POINT_get_LENGTH(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *valp)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) argv[0], NULL);

    RDB_float_to_obj(valp, sqrt(iptp->x * iptp->x + iptp->y * iptp->y));

    return RDB_OK;
}
