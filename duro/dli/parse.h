#ifndef RDB_PARSE_H
#define RDB_PARSE_H

/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include <rel/rdb.h>

typedef RDB_table *RDB_ltablefn(const char *, void *);

int
RDB_parse_expr(const char *, RDB_ltablefn *, void *, RDB_transaction *,
        RDB_expression **);

int
RDB_parse_table(const char *txt, RDB_ltablefn *, void *, RDB_transaction *,
        RDB_table **);

#endif
