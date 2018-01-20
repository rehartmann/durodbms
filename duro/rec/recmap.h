#ifndef RDB_RECMAP_H
#define RDB_RECMAP_H

/*
 * Copyright (C) 2003-2005, 2009, 2012-2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <gen/types.h>
#include <stdlib.h>

typedef struct RDB_recmap RDB_recmap;
typedef struct RDB_environment RDB_environment;
typedef struct RDB_rec_transaction RDB_rec_transaction;

/*
 * Functions for managing record maps. A record map contains
 * records which consist of a key and a data part.
 */

enum {
    RDB_ELEMENT_EXISTS = -200,
    RDB_KEY_VIOLATION = -201,
    RDB_RECORD_CORRUPTED = -202
};

typedef struct {
    int no;
    void *datap;
    size_t len;
    void *(*copyfp)(void *, const void *, size_t);
} RDB_field;

typedef int RDB_field_compare_func(const void *, size_t,
                const void *, size_t, RDB_environment *,
                void *);

typedef struct {
    RDB_field_compare_func *comparep;
    void *arg;
    RDB_bool asc;
} RDB_compare_field;

int
RDB_create_recmap(const char *name, const char *filename,
        RDB_environment *, int fieldc, const int fieldlenv[], int keyfieldc,
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *, RDB_recmap **);

int
RDB_open_recmap(const char *name, const char *filename,
        RDB_environment *, int fieldc, const int fieldlenv[], int keyfieldc,
        RDB_rec_transaction *, RDB_recmap **);

int
RDB_close_recmap(RDB_recmap *);

int
RDB_delete_recmap(RDB_recmap *, RDB_rec_transaction *);

int
RDB_insert_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *);

int
RDB_update_rec(RDB_recmap *, RDB_field[],
               int, const RDB_field[], RDB_rec_transaction *);

int
RDB_delete_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *);

int
RDB_get_fields(RDB_recmap *, RDB_field[],
           int, RDB_rec_transaction *, RDB_field[]);

int
RDB_contains_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *);

int
RDB_recmap_est_size(RDB_recmap *, RDB_rec_transaction *, unsigned *);

#endif
