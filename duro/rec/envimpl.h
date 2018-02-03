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

#ifdef POSTGRESQL
#include <libpq-fe.h>
#endif

typedef struct RDB_sequence RDB_sequence;
typedef struct RDB_index RDB_index;
typedef struct RDB_exec_context RDB_exec_context;

typedef struct RDB_environment {
    /* The Berkeley DB environment */
    union {
        DB_ENV *envp;
#ifdef POSTGRESQL
        PGconn *pgconn;
#endif
    } env;

    /* Functions implementing environment close and recmap functions */
    int (*close_fn)(struct RDB_environment *, RDB_exec_context *);
    RDB_recmap *(*create_recmap_fn)(const char *, const char *,
            RDB_environment *, int, const RDB_field_info[], int,
            const RDB_compare_field[], int, RDB_rec_transaction *, RDB_exec_context *);
    RDB_recmap *(*open_recmap_fn)(const char *, const char *,
            RDB_environment *, int, const RDB_field_info[], int,
            RDB_rec_transaction *, RDB_exec_context *);
    RDB_sequence *(*open_sequence_fn)(const char *, const char *, RDB_environment *,
            RDB_rec_transaction *, RDB_exec_context *);
    int (*rename_sequence_fn)(const char *, const char *, const char *, RDB_environment *,
            RDB_rec_transaction *, RDB_exec_context *);
    RDB_rec_transaction *(*begin_tx_fn)(RDB_environment *,
            RDB_rec_transaction *, RDB_exec_context *);
    int (*commit_fn)(RDB_rec_transaction *, RDB_exec_context *);
    int (*abort_fn)(RDB_rec_transaction *, RDB_exec_context *);
    int (*tx_id_fn)(RDB_rec_transaction *);
    RDB_index *(*create_index_fn)(RDB_recmap *, const char *, const char *,
            RDB_environment *, int, const RDB_field_descriptor[],
            const RDB_compare_field[], int, RDB_rec_transaction *,
            RDB_exec_context *);
    int (*close_index_fn)(RDB_index *, RDB_exec_context *);
    int (*delete_index_fn)(RDB_index *, RDB_environment *, RDB_rec_transaction *,
            RDB_exec_context *);

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
