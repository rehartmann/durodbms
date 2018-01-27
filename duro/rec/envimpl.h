/*
 * Environment internals
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_ENVIMPL_H_
#define REC_ENVIMPL_H_

#include "env.h"
#include "recmap.h"
#include <db.h>

typedef struct RDB_sequence RDB_sequence;
typedef struct RDB_exec_context RDB_exec_context;

typedef struct RDB_environment {
    /* The Berkeley DB environment */
    DB_ENV *envp;

    /* Functions implementing environment close and recmap functions */
    int (*close_fn)(struct RDB_environment *, RDB_exec_context *);
    RDB_recmap *(*create_recmap_fn)(const char *, const char *,
            RDB_environment *, int, const int[], int,
            const RDB_compare_field[], int, RDB_rec_transaction *, RDB_exec_context *);
    RDB_recmap *(*open_recmap_fn)(const char *, const char *,
            RDB_environment *, int, const int[], int,
            RDB_rec_transaction *, RDB_exec_context *);
    RDB_sequence *(*open_sequence_fn)(const char *, const char *, RDB_environment *, RDB_rec_transaction *,
            RDB_exec_context *);
    int (*rename_sequence_fn)(const char *, const char *, const char *, RDB_environment *,
            RDB_rec_transaction *, RDB_exec_context *);

    /* Function which is invoked by RDB_close_env() */
    void (*cleanup_fn)(struct RDB_environment *);

    /*
     * Used by higher layers to store additional data.
     * The relational layer uses this to store a pointer to the dbroot structure.
     */
    void *xdata;

    /* Trace level. 0 means no trace. */
    unsigned trace;
} RDB_environment;

#endif /* REC_ENVIMPL_H_ */
