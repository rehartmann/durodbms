#include "errors.h"
#include <db.h>

/* $Id$ */

const char *
RDB_strerror(int err)
{
    if (err < -30800 || err > 0) {
        /* It's a Berkeley DB or system error */
        return db_strerror(err);
    }
    switch (err) {
        case RDB_OK:
            return "OK";
        case RDB_ILLEGAL_ARG:
            return "illegal argument";
        case RDB_NOT_SUPPORTED:
            return "operation or option not supported";
        case RDB_NO_DISK_SPACE:
            return "out of disk space";
        case RDB_NO_MEMORY:
            return "out of memory";
        case RDB_ELEMENT_EXISTS:
            return "element already exists in set";
        case RDB_NOT_FOUND:
            return "not found";
        case RDB_KEY_VIOLATION:
            return "key constraint violation";
        case RDB_DEADLOCK:
            return "deadlock occured";
        case RDB_TYPE_MISMATCH:
            return "type mismatch";
        case RDB_PREDICATE_VIOLATION:
            return "table predicate violation";
        case RDB_INTERNAL:
            return "internal error";
    }
    return "unknown error code";
}
