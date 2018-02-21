#ifndef RDB_ENV_H
#define RDB_ENV_H

/*
 * Copyright (C) 2003-2007, 2012-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <gen/types.h>
#include <stdio.h>

enum {
    RDB_RECOVER = 1
};

typedef void (RDB_errfn)(const char *msg, void *arg);

typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_environment RDB_environment;

void *
RDB_env_xdata(RDB_environment *);

void
RDB_env_set_xdata(RDB_environment *, void *);

unsigned
RDB_env_trace(RDB_environment *);

void
RDB_env_set_trace(RDB_environment *, unsigned);

RDB_environment *
RDB_open_env(const char *, int, RDB_exec_context *);

RDB_environment *
RDB_create_env(const char *, RDB_exec_context *);

int
RDB_close_env(RDB_environment *, RDB_exec_context *);

void
RDB_set_env_closefn(RDB_environment *, void (*)(RDB_environment *));

void
RDB_env_set_trace(RDB_environment *, unsigned);

void
RDB_errcode_to_error(int, RDB_exec_context *);

RDB_bool
RDB_env_queries(const RDB_environment *);

void
RDB_env_set_errfile(RDB_environment *, FILE *);

#endif
