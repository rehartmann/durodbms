#ifndef QRESULT_H
#define QRESULT_H

/*
 * $Id$
 *
 * Copyright (C) 2006 René Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Internal functions for iterating over query results.
 */

#include "rdb.h"
#include <rec/cursor.h>

struct _RDB_tbindex;

typedef struct RDB_qresult {
    /* May be NULL */
    RDB_expression *exp;
    RDB_bool nested;
    union {
        struct {
            /* May be a descendant of *exp, NULL for sorter */
            RDB_object *tbp;

            /* NULL if a unique index is used */
            RDB_cursor *curp;
        } stored;
        struct {
            struct RDB_qresult *qrp;

            /* Only for some operators, may be NULL */
            struct RDB_qresult *qr2p;
            
            /* only used for join and ungroup */
            RDB_object tpl;
            RDB_bool tpl_valid;
        } children;
    } var;
    RDB_bool endreached;
 
    /*
     * 'materialized' table, needed for SUMMARIZE PER and sorting.
     */
    RDB_object *matp;
} RDB_qresult;

/*
 * Iterator over the tuples of a RDB_object. Used internally.
 * Using it from an application is possible, but violates RM proscription 7.
 */
RDB_qresult *
_RDB_table_qresult(RDB_object *, RDB_exec_context *, RDB_transaction *);

RDB_qresult *
_RDB_expr_qresult(RDB_expression *, RDB_exec_context *, RDB_transaction *);

int
_RDB_index_qresult(RDB_object *, struct _RDB_tbindex *, RDB_transaction *,
        RDB_qresult **);

int
_RDB_sorter(RDB_expression *, RDB_qresult **qrespp, RDB_exec_context *,
        RDB_transaction *, int seqitc, const RDB_seq_item seqitv[]);

int
_RDB_next_tuple(RDB_qresult *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

int
_RDB_duprem(RDB_qresult *, RDB_exec_context *, RDB_transaction *);

int
_RDB_get_by_cursor(RDB_object *, RDB_cursor *, RDB_type *, RDB_object *,
        RDB_exec_context *, RDB_transaction *);

int
_RDB_get_by_uindex(RDB_object *tbp, RDB_object *objpv[],
        struct _RDB_tbindex *indexp, RDB_type *, RDB_exec_context *,
        RDB_transaction *, RDB_object *tplp);

int
_RDB_drop_qresult(RDB_qresult *, RDB_exec_context *, RDB_transaction *);

int
_RDB_reset_qresult(RDB_qresult *, RDB_exec_context *, RDB_transaction *);

int
_RDB_sdivide_preserves(RDB_expression *, const RDB_object *tplp, RDB_qresult *qr3p,
        RDB_exec_context *, RDB_transaction *, RDB_bool *);

#endif /*QRESULT_H*/
