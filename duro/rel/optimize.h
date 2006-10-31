#ifndef OPTIMIZE_H
#define OPTIMIZE_H

#include "rdb.h"

struct _RDB_tbindex;

RDB_bool
_RDB_index_sorts(struct _RDB_tbindex *indexp, int seqitc,
        const RDB_seq_item seqitv[]);

RDB_expression *
_RDB_optimize_expr(RDB_expression *, int seqitc, const RDB_seq_item seqitv[],
        RDB_exec_context *, RDB_transaction *);

RDB_expression *
_RDB_optimize(RDB_object *, int seqitc, const RDB_seq_item seqitv[],
        RDB_exec_context *, RDB_transaction *);

#endif /*OPTIMIZE_H*/
