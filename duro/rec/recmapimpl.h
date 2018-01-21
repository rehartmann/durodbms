#ifndef RDB_RECMAPIMPL_H
#define RDB_RECMAPIMPL_H

/*
 * Record map internals
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "recmap.h"
#include "env.h"
#include "db.h"

typedef struct RDB_cursor RDB_cursor;

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

    int (*close_recmap_fn)(RDB_recmap *);
    int (*delete_recmap_fn)(RDB_recmap *, RDB_rec_transaction *);
    int (*insert_rec_fn)(RDB_recmap *, RDB_field[], RDB_rec_transaction *);
    int (*update_rec_fn)(RDB_recmap *, RDB_field[],
                   int, const RDB_field[], RDB_rec_transaction *);
    int (*delete_rec_fn)(RDB_recmap *, RDB_field[], RDB_rec_transaction *);
    int (*get_fields_fn)(RDB_recmap *, RDB_field[],
               int, RDB_rec_transaction *, RDB_field[]);
    int (*contains_rec_fn)(RDB_recmap *, RDB_field[], RDB_rec_transaction *);
    int (*recmap_est_size_fn)(RDB_recmap *, RDB_rec_transaction *, unsigned *);
    int (*cursor_fn)(RDB_cursor **, RDB_recmap *, RDB_bool, RDB_rec_transaction *);
} RDB_recmap;

size_t
RDB_get_vflen(RDB_byte *, size_t, int, int);

int
RDB_get_field(RDB_recmap *, int, void *, size_t,
        size_t *, int *);

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
