#ifndef RDB_PARSE_H
#define RDB_PARSE_H

/* $Id$ */

#include <rel/rdb.h>

typedef RDB_table *RDB_ltablefunc(const char *, void *);

int
RDB_parse_expr(const char *, RDB_ltablefunc *, void *, RDB_transaction *,
        RDB_expression **);

int
RDB_parse_table(const char *txt, RDB_ltablefunc *, void *, RDB_transaction *,
        RDB_table **);

#endif
