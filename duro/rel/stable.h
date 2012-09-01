#ifndef STABLE_H
#define STABLE_H

/*
 * $Id$
 *
 * Copyright (C) 2007-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */
#include "rdb.h"
#include <rec/index.h>

typedef struct RDB_tbindex {
    char *name;
    int attrc;
    RDB_seq_item *attrv;
    RDB_bool unique;
    RDB_bool ordered;
    RDB_index *idxp;    /* NULL for the primary index */
} RDB_tbindex;

typedef struct RDB_stored_table {
    RDB_recmap *recmapp;
    RDB_hashtable attrmap;   /* Maps attr names to field numbers */

    /* Table indexes */
    int indexc;
    struct RDB_tbindex *indexv;
    unsigned est_cardinality; /* estimated cardinality (from statistics) */
} RDB_stored_table;

void
RDB_free_tbindex(RDB_tbindex *);

int
RDB_create_stored_table(RDB_object *tbp, RDB_environment *envp,
        const RDB_bool ascv[], RDB_exec_context *, RDB_transaction *);

int
RDB_open_stored_table(RDB_object *tbp, RDB_environment *envp, const char *,
           int indexc, RDB_tbindex *indexv, RDB_exec_context *,
           RDB_transaction *);

int
RDB_close_stored_table(RDB_stored_table *, RDB_exec_context *);

int
RDB_delete_stored_table(RDB_stored_table *, RDB_exec_context *, RDB_transaction *);

RDB_int *
RDB_field_no(RDB_stored_table *, const char *attrname);

int
RDB_create_tbindex(RDB_object *tbp, RDB_environment *, RDB_exec_context *,
        RDB_transaction *, RDB_tbindex *, int);

int
RDB_open_table_index(RDB_object *tbp, RDB_tbindex *indexp,
        RDB_environment *, RDB_exec_context *, RDB_transaction *);

#endif /*STABLE_H*/
