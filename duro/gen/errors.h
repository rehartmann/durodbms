#ifndef RDB_ERRORS_H
#define RDB_ERRORS_H

/* $Id$ */

enum {
    RDB_OK = 0,

    RDB_NO_SPACE = -100,
    RDB_NO_MEMORY = -102,
    RDB_SYSTEM_ERROR = -103,
    RDB_DEADLOCK = -104,
    RDB_INTERNAL = -105,

    RDB_ILLEGAL_ARG = -200,
    RDB_NOT_FOUND = -201,
    RDB_INVALID_TRANSACTION = -202,
    RDB_ELEMENT_EXISTS = -203,
    RDB_TYPE_MISMATCH = -204,
    RDB_KEY_VIOLATION = -205,
    RDB_PREDICATE_VIOLATION = -206,
    RDB_AGGREGATE_UNDEFINED = -207,
    RDB_TYPE_CONSTRAINT_VIOLATION = -208,

    RDB_NOT_SUPPORTED = -300
};

/* Check for a system error */
#define RDB_is_syserr(err) ((err) <= -100 && (err) >= -105)

const char *
RDB_strerror(int err);

/* Convert a POSIX to a Duro error. */
int
RDB_convert_err(int);

#endif
