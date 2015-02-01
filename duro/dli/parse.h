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
RDB_parse_flush_buf(void);

void
RDB_parse_destroy_buf(void);

RDB_expression *
RDB_parse_node_expr(RDB_parse_node *, RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_parse_node_to_type(RDB_parse_node *, RDB_gettypefn *getfnp, void *arg,
        RDB_exec_context *, RDB_transaction *);

RDB_parse_node *
RDB_parse_expr(const char *, RDB_exec_context *);

int
RDB_parse_node_modname(RDB_parse_node *, RDB_object *, RDB_exec_context *);

int
RDB_parse_node_qid(RDB_parse_node *, RDB_object *, RDB_exec_context *);

RDB_parse_node *
RDB_parse_stmt(RDB_exec_context *);

RDB_parse_node *
RDB_parse_stmt_string(const char *, RDB_exec_context *);

/**@addtogroup parse
 * @{
 */

/**
 * Provide a function that will be used to read the next line
 * in interactive mode.
 */
void
RDB_parse_set_read_line_fn(RDB_read_line_fn *fnp);

/**
 * Provide a function that will be used to free a line of input
 * read by the function passed to RDB_parse_set_read_line_fn().
 */
void
RDB_parse_set_free_line_fn(RDB_free_line_fn *fnp);

/**
 * @}
 */

RDB_bool
RDB_parse_get_interactive(void);

void
RDB_parse_set_interactive(RDB_bool ia);

void
RDB_parse_set_case_insensitive(RDB_bool is);

#endif
