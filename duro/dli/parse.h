#ifndef RDB_PARSE_H
#define RDB_PARSE_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>

typedef RDB_object *RDB_ltablefn(const char *, void *);

RDB_expression *
RDB_parse_expr(const char *, RDB_ltablefn *, void *, RDB_exec_context *,
        RDB_transaction *);

#endif
