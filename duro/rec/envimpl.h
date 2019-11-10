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

#ifdef BERKELEYDB
#include <db.h>
#endif

#ifdef POSTGRESQL
#include <libpq-fe.h>
#endif

#ifdef FOUNDATIONDB
#define FDB_API_VERSION 600 
#include <foundationdb/fdb_c.h>
#endif

typedef struct RDB_sequence RDB_sequence;
typedef struct RDB_index RDB_index;
typedef struct RDB_exec_context RDB_exec_context;

typedef struct RDB_environment {
    /* The Berkeley DB environment */
    union {
#ifdef BERKELEYDB
        DB_ENV *envp;
#endif
#ifdef POSTGRESQL
        PGconn *pgconn;
#endif
#ifdef FOUNDATIONDB
		FDBDatabase *fdb;
#endif
    } env;
    FILE *errfile;

    /* Functions implementing environment close and recmap functions */
    int (*close_fn)(struct RDB_environment *, RDB_exec_context *);
    RDB_recmap *(*create_recmap_fn)(const char *,
            RDB_environment *, int, const RDB_field_info[], int,
            int, const RDB_compare_field[], int,
            int, const RDB_string_vec *,
            RDB_rec_transaction *, RDB_exec_context *);
    RDB_recmap *(*open_recmap_fn)(const char *,
            RDB_environment *, int, const RDB_field_info[], int,
            RDB_rec_transaction *, RDB_exec_context *);
    RDB_sequence *(*open_sequence_fn)(const char *, RDB_environment *,
            RDB_rec_transaction *, RDB_exec_context *);
    int (*rename_sequence_fn)(const char *, const char *, RDB_environment *,
            RDB_rec_transaction *, RDB_exec_context *);
    RDB_rec_transaction *(*begin_tx_fn)(RDB_environment *,
            RDB_rec_transaction *, RDB_exec_context *);
    int (*commit_fn)(RDB_rec_transaction *, RDB_exec_context *);
    int (*abort_fn)(RDB_rec_transaction *, RDB_exec_context *);
    int (*tx_id_fn)(RDB_rec_transaction *);
    void (*set_errfile_fn)(RDB_environment *, FILE *);
    FILE *(*get_errfile_fn)(const RDB_environment *);

    /* Function which is invoked by RDB_close_env() */
    void (*cleanup_fn)(struct RDB_environment *);

    /*
     * Used by higher layers to store additional data.
     * The relational layer uses this to store a pointer to the dbroot structure.
     */
    void *xdata;

    /* Trace level. 0 means no trace. */
    unsigned trace;

    /* TRUE if the storage engine supports queries (SQL), FALSE if not (Berkeley DB) */
    RDB_bool queries;
} RDB_environment;

#endif /* REC_ENVIMPL_H_ */
