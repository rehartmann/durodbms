/* $Id$ */

#include <rel/rdb.h>
#include <rel/typeimpl.h>
#include <math.h>
#include <tests/point.h>

/*
 * Selector, setters, and getter for POINT
 */

int
RDBU_select_POINT(RDB_object *valp, RDB_object *compv[],
        RDB_type *typ, const char *possrepname)
{
    i_point ipt;

    ipt.x = RDB_obj_rational(compv[0]);
    ipt.y = RDB_obj_rational(compv[1]);

    return RDB_irep_to_obj(valp, typ, &ipt, sizeof ipt);
}

int
RDBU_set_X(RDB_object *valp, const RDB_object *comp,
        RDB_type *typ, const char *compname)
{
    i_point *iptp = RDB_obj_irep(valp, NULL);

    iptp->x = RDB_obj_rational(comp);

    return RDB_OK;
}

int
RDBU_get_X(const RDB_object *valp, RDB_object *comp,
        RDB_type *typ, const char *compname)
{
    i_point *iptp = RDB_obj_irep((RDB_object *)valp, NULL);

    RDB_rational_to_obj(comp, iptp->x);

    return RDB_OK;
}

int
RDBU_set_Y(RDB_object *valp, const RDB_object *comp,
        RDB_type *typ, const char *compname)
{
    i_point *iptp = RDB_obj_irep(valp, NULL);

    iptp->y = RDB_obj_rational(comp);

    return RDB_OK;
}

int
RDBU_get_Y(const RDB_object *valp, RDB_object *comp,
        RDB_type *typ, const char *compname)
{
    i_point *iptp = RDB_obj_irep((RDB_object *)valp, NULL);

    RDB_rational_to_obj(comp, iptp->y);

    return RDB_OK;
}

/*
 * Selector, setters, and getter for POLAR
 */

int
RDBU_select_POLAR(RDB_object *valp, RDB_object *compv[],
        RDB_type *typ, const char *possrepname)
{
    i_point ipt;
    double th = (double) RDB_obj_rational(compv[0]);
    double len = (double) RDB_obj_rational(compv[1]);

    ipt.x = cos(th) * len;
    ipt.y = sin(th) * len;

    return RDB_irep_to_obj(valp, typ, &ipt, sizeof ipt);
}

int
RDBU_set_THETA(RDB_object *valp, const RDB_object *comp,
        RDB_type *typ, const char *compname)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) valp, NULL);
    double len = sqrt(iptp->x * iptp->x + iptp->y * iptp->y);
    double theta = RDB_obj_rational(comp);

    iptp->x = cos(theta) * len;
    iptp->y = sin(theta) * len;

    return RDB_OK;
}

int
RDBU_get_THETA(const RDB_object *valp, RDB_object *comp,
        RDB_type *typ, const char *compname)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) valp, NULL);

    RDB_rational_to_obj(comp, iptp->x != 0.0 ?
            atan(iptp->y / iptp->x) : M_PI_2);

    return RDB_OK;
}

int
RDBU_set_LENGTH(RDB_object *valp, const RDB_object *comp,
        RDB_type *typ, const char *compname)
{
    i_point *iptp = RDB_obj_irep(valp, NULL);
    double len = (double) RDB_obj_rational(comp);
    double theta = iptp->x != 0.0 ? atan(iptp->y / iptp->x) : M_PI_2;

    iptp->x = cos(theta) * len;
    iptp->y = sin(theta) * len;

    return RDB_OK;
}

int
RDBU_get_LENGTH(const RDB_object *valp, RDB_object *comp,
        RDB_type *typ, const char *compname)
{
    i_point *iptp = RDB_obj_irep((RDB_object *) valp, NULL);

    RDB_rational_to_obj(comp, sqrt(iptp->x * iptp->x + iptp->y * iptp->y));

    return RDB_OK;
}
