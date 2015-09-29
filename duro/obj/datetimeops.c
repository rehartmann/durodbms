/*
 * Built-in operators for type datetime.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "datetimeops.h"
#include "builtintypes.h"
#include <gen/strfns.h>

#include <stdio.h>

#ifdef _WIN32
#define snprintf sprintf_s
#endif

static int datetime_check_month(int m, RDB_exec_context *ecp)
{
    if (m < 1 || m > 12) {
        RDB_raise_type_constraint_violation("datetime: month", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static RDB_bool is_leap_year(int y) {
    if (y % 400 == 0)
        return RDB_TRUE;
    return y % 4 == 0 && y % 100 != 0 ? RDB_TRUE : RDB_FALSE;
}

static int datetime_check_day(int y, int m, int d, RDB_exec_context *ecp)
{
    int days;

    switch(m) {
    case 1:
    case 3:
    case 5:
    case 7:
    case 8:
    case 10:
    case 12:
        days = 31;
        break;
    case 4:
    case 6:
    case 9:
    case 11:
        days = 30;
        break;
    case 2:
        /* Ignore Gregorian leap year rule for years before 1924 */
        if (y < 1924) {
            days = y % 4 == 0 ? 29 : 28;
        } else {
            days = is_leap_year(y) ? 29 : 28;
        }
        break;
    }

    if (d < 1 || d > days) {
        RDB_raise_type_constraint_violation("datetime: day", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int datetime_check_hour(int h, RDB_exec_context *ecp)
{
    if (h < 0 || h > 23) {
        RDB_raise_type_constraint_violation("datetime: hour", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int datetime_check_minute(int m, RDB_exec_context *ecp)
{
    if (m < 0 || m > 59) {
        RDB_raise_type_constraint_violation("datetime: minute", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int datetime_check_second(int sec, RDB_exec_context *ecp)
{
    if (sec < 0 || sec > 60) {
        RDB_raise_type_constraint_violation("datetime: minute", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/** @page datetime-ops Built-in datetime type and operators

<pre>
TYPE datetime
POSSREP {
    year integer,
    month integer,
    day integer,
    hour integer,
    minute integer,
    second integer
};
</pre>

 *
 * operator now() returns datetime;
 *
 * operator now_utc() returns datetime;
 *
 * Returns the time as a datetime.
 * now() returns the time according to the current timezone.
 * now_utc() returns the time as UTC.
 *
 *
 * operator add_seconds(dt datetime, seconds integer) returns datetime;
 *
 * Adds the number of seconds specified by <var>seconds</var>
 * to the datetime specified by <var>dt</var> using the current time zone
 * and returns the result.
 */

/* Selector of type datetime */
static int
datetime(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    struct tm tm;

    if (datetime_check_month(RDB_obj_int(argv[1]), ecp) != RDB_OK)
        return RDB_ERROR;
    if (datetime_check_day(RDB_obj_int(argv[0]), RDB_obj_int(argv[1]),
            RDB_obj_int(argv[2]), ecp) != RDB_OK)
        return RDB_ERROR;
    if (datetime_check_hour(RDB_obj_int(argv[3]), ecp) != RDB_OK)
        return RDB_ERROR;
    if (datetime_check_minute(RDB_obj_int(argv[4]), ecp) != RDB_OK)
        return RDB_ERROR;
    if (datetime_check_second(RDB_obj_int(argv[5]), ecp) != RDB_OK)
        return RDB_ERROR;

    tm.tm_year = RDB_obj_int(argv[0]) - 1900;
    tm.tm_mon = RDB_obj_int(argv[1]) - 1;
    tm.tm_mday = RDB_obj_int(argv[2]);
    tm.tm_hour = RDB_obj_int(argv[3]);
    tm.tm_min = RDB_obj_int(argv[4]);
    tm.tm_sec = RDB_obj_int(argv[5]);

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
datetime_get_minute(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.time.minute);
    return RDB_OK;
}

static int
datetime_get_second(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.time.second);
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
        RDB_raise_system("localtime() failed", ecp);
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
            argv[0]->val.time.minute,
            argv[0]->val.time.second);

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
    if (datetime_check_month(RDB_obj_int(argv[1]), ecp) != RDB_OK)
        return RDB_ERROR;

    argv[0]->val.time.month = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_day(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (datetime_check_day(argv[0]->val.time.year, argv[0]->val.time.month,
            RDB_obj_int(argv[1]), ecp) != RDB_OK)
        return RDB_ERROR;

    argv[0]->val.time.day = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_hour(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (datetime_check_hour(RDB_obj_int(argv[1]), ecp) != RDB_OK)
        return RDB_ERROR;

    argv[0]->val.time.hour = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_minute(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (datetime_check_minute(RDB_obj_int(argv[1]), ecp) != RDB_OK)
        return RDB_ERROR;

    argv[0]->val.time.minute = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_set_second(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (datetime_check_second(RDB_obj_int(argv[1]), ecp) != RDB_OK)
        return RDB_ERROR;

    argv[0]->val.time.second = RDB_obj_int(argv[1]);
    return RDB_OK;
}

static int
datetime_add_seconds(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    time_t t;
    struct tm *restm;
    struct tm tm;

    tm.tm_year = argv[0]->val.time.year - 1900;
    tm.tm_mon = argv[0]->val.time.month - 1;
    tm.tm_mday = argv[0]->val.time.day;
    tm.tm_hour = argv[0]->val.time.hour;
    tm.tm_min = argv[0]->val.time.minute;
    tm.tm_sec = argv[0]->val.time.second;
    tm.tm_isdst = -1;

    t = mktime(&tm);
    if (t == -1) {
        RDB_raise_invalid_argument("converting datetime to time_t failed", ecp);
        return RDB_ERROR;
    }

    t += RDB_obj_int(argv[1]);

    restm = localtime(&t);
    if (restm == NULL) {
        RDB_raise_system("localtime() failed", ecp);
        return RDB_ERROR;
    }

    RDB_tm_to_obj(retvalp, restm);
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

    if (RDB_put_ro_op(opmap, "datetime_get_minute", 1, paramtv, &RDB_INTEGER,
            &datetime_get_minute, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "datetime_get_second", 1, paramtv, &RDB_INTEGER,
            &datetime_get_second, ecp) != RDB_OK)
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

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "add_seconds", 2, paramtv, &RDB_DATETIME,
            &datetime_add_seconds, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

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
    if (RDB_put_upd_op(opmap, "datetime_set_minute", 2, paramv,
            &datetime_set_minute, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmap, "datetime_set_second", 2, paramv,
            &datetime_set_second, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}

