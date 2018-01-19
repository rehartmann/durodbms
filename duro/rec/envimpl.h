/*
 * Environment internals
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_ENVIMPL_H_
#define REC_ENVIMPL_H_

#include "env.h"

/*
 * Copyright (C) 2003-2007, 2012-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <db.h>

typedef struct RDB_environment {
    /* The Berkeley DB environment */
    DB_ENV *envp;

    /* Function which is invoked by RDB_close_env() */
    void (*closefn)(struct RDB_environment *);

    /*
     * Used by higher layers to store additional data.
     * The relational layer uses this to store a pointer to the dbroot structure.
     */
    void *xdata;

    /* Trace level. 0 means no trace. */
    unsigned trace;
} RDB_environment;

DB_ENV *
RDB_bdb_env(RDB_environment *);

#endif /* REC_ENVIMPL_H_ */
