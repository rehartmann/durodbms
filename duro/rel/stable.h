#ifndef STABLE_H
#define STABLE_H

/*
 * Copyright (C) 2007, 2012-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */
#include "rdb.h"
#include <rec/index.h>

/* Name of the file in which the tables are physically stored */
#define RDB_DATAFILE "rdata"

typedef struct RDB_tbindex RDB_tbindex;

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
RDB_provide_stored_table(RDB_object *, RDB_bool,
        RDB_exec_context *, RDB_transaction *);

int
RDB_create_stored_table(RDB_object *tbp, RDB_environment *envp,
        int, const RDB_compare_field[], RDB_exec_context *, RDB_transaction *);

int
RDB_open_stored_table(RDB_object *tbp, RDB_environment *envp, const char *,
           RDB_exec_context *, RDB_transaction *);

int
RDB_close_stored_table(RDB_stored_table *, RDB_exec_context *);

int
RDB_delete_stored_table(RDB_stored_table *, RDB_exec_context *, RDB_transaction *);

RDB_int *
RDB_field_no(RDB_stored_table *, const char *attrname);

int
RDB_create_tbindex(RDB_object *, RDB_tbindex *, RDB_environment *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_open_tbindex(RDB_object *, RDB_tbindex *, RDB_environment *,
        RDB_exec_context *, RDB_transaction *);

#endif /*STABLE_H*/
