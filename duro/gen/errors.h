#ifndef RDB_ERRORS_H
#define RDB_ERRORS_H

/* $Id$ */

enum {
    RDB_OK = 0,
    RDB_ILLEGAL_ARG = -101,
    RDB_NOT_SUPPORTED = -102,
    RDB_NO_DISK_SPACE = -103,
    RDB_NO_MEMORY = -104,
    RDB_NOT_FOUND = -105,
    RDB_ELEMENT_EXISTS = -106,
    RDB_KEY_VIOLATION = -107,
    RDB_DEADLOCK = -108,
    RDB_TYPE_MISMATCH = -109,
    RDB_PREDICATE_VIOLATION = -110,
    RDB_INVALID_TRANSACTION = -111,
    RDB_INTERNAL = -112
};

const char *
RDB_strerror(int err);

#endif
