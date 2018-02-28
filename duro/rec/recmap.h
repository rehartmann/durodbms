#ifndef RDB_RECMAP_H
#define RDB_RECMAP_H

/*
 * Copyright (C) 2003-2005, 2009, 2012-2013, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <gen/types.h>
#include <obj/object.h>
#include <stdlib.h>

typedef struct RDB_recmap RDB_recmap;
typedef struct RDB_environment RDB_environment;
typedef struct RDB_rec_transaction RDB_rec_transaction;
typedef struct RDB_exec_context RDB_exec_context;

/*
 * Functions for managing record maps. A record map contains
 * records which consist of a key and a data part.
 */

enum {
    RDB_FTYPE_CHAR = 1,
    RDB_FTYPE_BOOLEAN = 2,
    RDB_FTYPE_INTEGER = 4,
    RDB_FTYPE_FLOAT = 8,
    RDB_FTYPE_SERIAL = 256,

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
    int len;
    const char *attrname;
    int flags;
} RDB_field_info;

typedef struct {
    int no;
    const char *attrname;
} RDB_field_descriptor;

typedef struct {
    RDB_field_compare_func *comparep;
    void *arg;
    RDB_bool asc;
} RDB_compare_field;

RDB_recmap *
RDB_create_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[], int,
        const RDB_compare_field[], int,
        int uniquec, const RDB_string_vec *uniquev,
        RDB_rec_transaction *, RDB_exec_context *);

RDB_recmap *
RDB_open_recmap(const char *, const char *,
        RDB_environment *, int, const RDB_field_info[], int,
        RDB_rec_transaction *, RDB_exec_context *);

int
RDB_close_recmap(RDB_recmap *, RDB_exec_context *);

int
RDB_delete_recmap(RDB_recmap *, RDB_rec_transaction *, RDB_exec_context *);

int
RDB_insert_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_update_rec(RDB_recmap *, RDB_field[],
               int, const RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_delete_rec(RDB_recmap *, int, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_get_fields(RDB_recmap *, RDB_field[], int,
        RDB_rec_transaction *, RDB_field[], RDB_exec_context *);

int
RDB_contains_rec(RDB_recmap *, RDB_field[], RDB_rec_transaction *, RDB_exec_context *);

int
RDB_recmap_est_size(RDB_recmap *, RDB_rec_transaction *, unsigned *, RDB_exec_context *);

#endif
