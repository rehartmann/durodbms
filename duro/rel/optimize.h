#ifndef OPTIMIZE_H
#define OPTIMIZE_H

/*
 * $Id$
 *
 * Copyright (C) 2006-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Declares an internal function for inserting tuples into tables.
 */

#include "rdb.h"

struct RDB_tbindex;

RDB_bool
RDB_index_sorts(struct RDB_tbindex *indexp, int seqitc,
        const RDB_seq_item seqitv[]);

RDB_expression *
RDB_optimize_expr(RDB_expression *, int, const RDB_seq_item[],
        RDB_expression *, RDB_exec_context *, RDB_transaction *);

RDB_expression *
RDB_optimize(RDB_object *, int, const RDB_seq_item[],
        RDB_exec_context *, RDB_transaction *);

#endif /*OPTIMIZE_H*/
