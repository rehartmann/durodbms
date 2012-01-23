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

#if defined (_WIN32) && !defined (NO_DLL_IMPORT)
#define _RDB_EXTERN_VAR __declspec(dllimport)
#else
#define _RDB_EXTERN_VAR extern
#endif

_RDB_EXTERN_VAR int _RDB_parse_interactive;
_RDB_EXTERN_VAR int _RDB_parse_case_insensitive;
_RDB_EXTERN_VAR char *_RDB_parse_prompt;

void
_RDB_parse_init_buf(FILE *);

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

#endif
