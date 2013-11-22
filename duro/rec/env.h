#ifndef RDB_ENV_H
#define RDB_ENV_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <db.h>
#include <obj/excontext.h>

enum {
    RDB_CREATE = 1
};

typedef void (RDB_errfn)(const char *msg, void *arg);

typedef struct RDB_environment {
    /* The Berkeley DB environment */
    DB_ENV *envp;

    /* The database for sequences */
    DB *seq_dbp;

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

void *
RDB_env_xdata(RDB_environment *);

void
RDB_env_set_xdata(RDB_environment *, void *);

unsigned
RDB_env_trace(RDB_environment *);

void
RDB_env_set_trace(RDB_environment *, unsigned);

int
RDB_open_env(const char *, RDB_environment **, int);

int
RDB_close_env(RDB_environment *);

int
RDB_close_seq_container(RDB_environment *);

void
RDB_set_env_closefn(RDB_environment *, void (*)(RDB_environment *));

DB_ENV *
RDB_bdb_env(RDB_environment *);

void
RDB_env_set_trace(RDB_environment *, unsigned);

void
RDB_errcode_to_error(int, RDB_exec_context *);

#endif
