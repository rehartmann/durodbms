#include "errors.h"
#include <db.h>
#include <errno.h>

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

        case RDB_NO_SPACE:
            return "out of secondary storage space";
        case RDB_NO_MEMORY:
            return "out of memory";
        case RDB_SYSTEM_ERROR:
            return "system error";
        case RDB_DEADLOCK:
            return "deadlock occured";
        case RDB_INTERNAL:
            return "internal error";
        case RDB_RESOURCE_NOT_FOUND:
            return "resource not found";

        case RDB_INVALID_ARGUMENT:
            return "invalid argument";
        case RDB_NOT_FOUND:
            return "data not found";
        case RDB_INVALID_TRANSACTION:
            return "invalid transaction";
        case RDB_ELEMENT_EXISTS:
            return "element already exists in set";
        case RDB_TYPE_MISMATCH:
            return "type mismatch";
        case RDB_KEY_VIOLATION:
            return "key constraint violation";
        case RDB_PREDICATE_VIOLATION:
            return "table predicate violation";
        case RDB_AGGREGATE_UNDEFINED:
            return "result of aggregate operation is undefined";
        case RDB_TYPE_CONSTRAINT_VIOLATION:
            return "type constraint violation";

        case RDB_NOT_SUPPORTED:
            return "operation or option not supported";
    }
    return "unknown error code";
}

int
RDB_is_syserr(int err) {
    return (err <= -100) && (err >= -199);
}

int
RDB_convert_err(int err)
{
    switch(err) {
        case ENOSPC:
            err = RDB_NO_SPACE;
            break;
        case ENOMEM:
            err = RDB_NO_MEMORY;
            break;
        case DB_LOCK_DEADLOCK:
            err = RDB_DEADLOCK;
            break;
        case EINVAL:
            err = RDB_INTERNAL;
            break;
        case ENOENT:
            err = RDB_RESOURCE_NOT_FOUND;
            break;
        case DB_NOTFOUND:
            err = RDB_NOT_FOUND;
            break;
        case DB_KEYEXIST:
            err = RDB_KEY_VIOLATION;
            break;
        default:
            if (err > 0)
               err = RDB_SYSTEM_ERROR;
    }        
        
    return err;
}
