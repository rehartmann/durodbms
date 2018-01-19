#ifndef RDB_RECMAPIMPL_H
#define RDB_RECMAPIMPL_H

/*
 * Record map internals
 *
 * Copyright (C) 2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "recmap.h"
#include "env.h"
#include "db.h"

typedef struct RDB_recmap {
    /* internal */
    RDB_environment *envp;
    DB *dbp;
    char *namp;
    char *filenamp;
    int fieldcount; /* # of fields total */
    int keyfieldcount;  /* # of primary index fields */

    /*
     * The length of each field, if it's fixed-length field,
     * RDB_VARIABLE_LEN if it's a variable-length field
     */
    RDB_int *fieldlens;

    int varkeyfieldcount; /* # of variable-length key fields */
    int vardatafieldcount; /* # of variable-length nonkey fields */

    /* For sorted recmaps */
    RDB_compare_field *cmpv;

    /* RDB_TRUE if duplicate keys are allowed */
    RDB_bool dup_keys;
} RDB_recmap;

int
RDB_set_field(RDB_recmap *, DBT *, const RDB_field *,
               int);

int
RDB_get_DBT_fields(RDB_recmap *, const DBT *, const DBT *, int,
           RDB_field[]);

int
RDB_fields_to_DBT(RDB_recmap *, int, const RDB_field[],
                   DBT *);

int
RDB_update_DBT_rec(RDB_recmap *, DBT *, DBT *,
        int, const RDB_field[], DB_TXN *);

#endif
