/*
 * Built-in operators for type datetime.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "datetimeops.h"
#include "builtintypes.h"

#include <stdio.h>

#ifdef _WIN32
#define snprintf sprintf_s
#endif

/* Selector of type datetime */
static int
datetime(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    struct tm tm;

    tm.tm_year = RDB_obj_int(argv[0]) - 1900;
    tm.tm_mon = RDB_obj_int(argv[1]) - 1;
    tm.tm_mday = RDB_obj_int(argv[2]);
    tm.tm_hour = RDB_obj_int(argv[3]);
    tm.tm_min = RDB_obj_int(argv[4]);
    tm.tm_sec = RDB_obj_int(argv[5]);
    tm.tm_isdst = 0;

    RDB_tm_to_obj(retvalp, &tm);
    return RDB_OK;
}

static int
datetime_get_year(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.time.year);
    return RDB_OK;
}

static int
datetime_get_month(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.time.month);
    return RDB_OK;
}

static int
datetime_get_day(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.time.day);
    return RDB_OK;
}

static int
datetime_get_hour(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.time.hour);
    return RDB_OK;
}

static int
datetime_get_min(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.time.min);
    return RDB_OK;
}

static int
datetime_get_sec(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.time.sec);
    return RDB_OK;
}

static int
now_utc_datetime(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    struct tm *tm;
    time_t t = time(NULL);

    tm = gmtime(&t);
    if (tm == NULL) {
        RDB_raise_system("gmtime() failed", ecp);
        return RDB_ERROR;
    }

    RDB_tm_to_obj(retvalp, tm);
    return RDB_OK;
}

static int
now_datetime(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    struct tm *tm;
    time_t t = time(NULL);

    tm = localtime(&t);
    if (tm == NULL) {
        RDB_raise_system("gmtime() failed", ecp);
        return RDB_ERROR;
    }

    RDB_tm_to_obj(retvalp, tm);
    return RDB_OK;
}

static int
cast_as_string_datetime(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)

{
    char buf[20];
    snprintf(buf, sizeof(buf), "%4d-%02d-%02dT%02d:%02d:%02d",
            argv[0]->val.time.year,
            argv[0]->val.time.month,
            argv[0]->val.time.day,
            argv[0]->val.time.hour,
            argv[0]->val.time.min,
            argv[0]->val.time.sec);

    return RDB_string_to_obj(retvalp, buf, ecp);
}

static int
datetime_set_year(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    argv[0]->val.time.year = (int16_t) RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_month(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    argv[0]->val.time.month = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_day(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    argv[0]->val.time.day = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_hour(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    argv[0]->val.time.hour = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_min(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    argv[0]->val.time.min = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_sec(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    argv[0]->val.time.sec = RDB_obj_int(argv[1]);
    return RDB_OK;
}

int
RDB_add_datetime_ro_ops(RDB_op_map *opmap, RDB_exec_context *ecp)
{
    RDB_type *paramtv[6];

    paramtv[0] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, "datetime_get_year", 1, paramtv, &RDB_INTEGER,
            &datetime_get_year, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "datetime_get_month", 1, paramtv, &RDB_INTEGER,
            &datetime_get_month, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "datetime_get_day", 1, paramtv, &RDB_INTEGER,
            &datetime_get_day, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "datetime_get_hour", 1, paramtv, &RDB_INTEGER,
            &datetime_get_hour, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "datetime_get_min", 1, paramtv, &RDB_INTEGER,
            &datetime_get_min, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "datetime_get_sec", 1, paramtv, &RDB_INTEGER,
            &datetime_get_sec, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "cast_as_string", 1, paramtv, &RDB_STRING,
            &cast_as_string_datetime, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;
    paramtv[2] = &RDB_INTEGER;
    paramtv[3] = &RDB_INTEGER;
    paramtv[4] = &RDB_INTEGER;
    paramtv[5] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "datetime", 6, paramtv, &RDB_DATETIME,
            &datetime, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "now_utc", 0, NULL, &RDB_DATETIME,
            &now_utc_datetime, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "now", 0, NULL, &RDB_DATETIME,
            &now_datetime, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}

int
RDB_add_datetime_upd_ops(RDB_op_map *opmap, RDB_exec_context *ecp)
{
    RDB_parameter paramv[2];

    paramv[0].typ = &RDB_DATETIME;
    paramv[0].update = RDB_TRUE;
    paramv[1].typ = &RDB_INTEGER;
    paramv[1].update = RDB_FALSE;

    if (RDB_put_upd_op(opmap, "datetime_set_year", 2, paramv,
            &datetime_set_year, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmap, "datetime_set_month", 2, paramv,
            &datetime_set_month, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmap, "datetime_set_day", 2, paramv,
            &datetime_set_day, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmap, "datetime_set_hour", 2, paramv,
            &datetime_set_hour, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmap, "datetime_set_min", 2, paramv,
            &datetime_set_min, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmap, "datetime_set_sec", 2, paramv,
            &datetime_set_sec, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}

