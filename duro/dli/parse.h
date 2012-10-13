#ifndef RDB_PARSE_H
#define RDB_PARSE_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <stdio.h>
#include <rel/rdb.h>
#include "parsenode.h"

extern int RDB_parse_tokens[];

void
RDB_parse_init_buf(FILE *);

void
RDB_parse_destroy_buf(void);

RDB_expression *
RDB_parse_node_expr(RDB_parse_node *, RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_parse_node_to_type(RDB_parse_node *, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *, RDB_transaction *);

RDB_parse_node *
RDB_parse_expr(const char *, RDB_exec_context *);

RDB_parse_node *
RDB_parse_stmt(RDB_exec_context *);

RDB_parse_node *
RDB_parse_stmt_string(const char *, RDB_exec_context *);

void
RDB_parse_set_readline_fn(RDB_readline_fn *fnp);

RDB_bool
RDB_parse_get_interactive(void);

void
RDB_parse_set_interactive(RDB_bool ia);

void
RDB_parse_set_case_insensitive(RDB_bool is);

#endif
