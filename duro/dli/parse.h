#ifndef RDB_PARSE_H
#define RDB_PARSE_H

/* $Id$ */

#include <rel/rdb.h>

int
RDB_parse_expr(const char *, RDB_transaction *, RDB_expression **);

int
RDB_parse_table(const char *txt, RDB_transaction *, RDB_table **tbpp);

#endif
