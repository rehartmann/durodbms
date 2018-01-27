/*
 * Copyright (C) 2003-2007, 2012-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef BDBREC_BDBENV_H_
#define BDBREC_BDBENV_H_

#include <db.h>

typedef struct RDB_environment RDB_environment;
typedef struct RDB_exec_context RDB_exec_context;

int
RDB_bdb_open_env(const char *, RDB_environment **, int);

int
RDB_bdb_create_env(const char *, RDB_environment **);

int
RDB_bdb_close_env(RDB_environment *, RDB_exec_context *);

DB_ENV *
RDB_bdb_env(RDB_environment *);

void
RDB_bdb_errcode_to_error(int, RDB_exec_context *);

#endif /* BDBREC_BDBENV_H_ */
