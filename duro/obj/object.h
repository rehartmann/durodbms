/*
 * object.h
 *
 *  Created on: 29.09.2013
 *      Author: rene
 */

#ifndef OBJECT_H_
#define OBJECT_H_

#include <gen/types.h>
#include <gen/strfns.h>
#include <gen/hashtable.h>
#include <gen/hashmap.h>

#include <stddef.h>

enum RDB_obj_kind {
    RDB_OB_INITIAL,
    RDB_OB_BOOL,
    RDB_OB_INT,
    RDB_OB_FLOAT,
    RDB_OB_BIN,
    RDB_OB_TABLE,
    RDB_OB_TUPLE,
    RDB_OB_ARRAY
};

/**
 * Represents a vector of strings.
 */
typedef struct {
    /** The number of strings. */
    int strc;
    /** The array holding pointers to the strings. */
    char **strv;
} RDB_string_vec;

typedef struct RDB_expression RDB_expression;
typedef struct RDB_type RDB_type;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_object RDB_object;

typedef int RDB_obj_cleanup_func(RDB_object *, RDB_exec_context *);

/**
 * A RDB_object structure carries a value of an arbitrary type,
 * together with the type information.
 */
struct RDB_object {
    /*
     * Internal
     */

    /*
     * The type of the RDB_object.
     * If the value is non-scalar and not a table, it is NULL by default,
     * but can be set by calling RDB_obj_set_typeinfo().
     * In this case, the caller is responsible for managing the type
     * (e.g. destroying the type when the RDB_object is destroyed).
     */
    RDB_type *typ;

    enum RDB_obj_kind kind;
    union {
        RDB_bool bool_val;
        RDB_int int_val;
        RDB_float float_val;
        struct {
            void *datap;
            size_t len;
        } bin;
        struct {
            int flags;
            char *name;

            /*
             * Candidate keys. NULL if table is virtual and the keys have not been
             * inferred.
             */
            int keyc;
            RDB_string_vec *keyv;

            /* NULL if it's a real table */
            RDB_expression *exp;

            RDB_hashmap *default_map; /* Default values */

            struct RDB_stored_table *stp;
        } tb;
        RDB_hashtable tpl_tab;
        struct {
            RDB_expression *texp;
            struct RDB_transaction *txp;
            struct RDB_qresult *qrp;

            /* Position of next element returned by qresult */
            RDB_int pos;

            RDB_int length; /* length of array; -1 means unknown */

            /* Elements (buffer, if tbp is not NULl) */
            int elemc;
            struct RDB_object *elemv;

            /* Buffers elements beyond elemc */
            struct RDB_object *tplp;
        } arr;
     } val;

     /* Used internally for conversion into the internal representation */
     RDB_type *store_typ;

     RDB_obj_cleanup_func *cleanup_fp;
};

/* Internal */
typedef struct {
    char *key;
    RDB_object obj;
} tuple_entry;

void *
RDB_alloc(size_t, RDB_exec_context *);

void *
RDB_realloc(void *, size_t, RDB_exec_context *);

void
RDB_free(void *);

RDB_type *
RDB_obj_type(const RDB_object *);

void
RDB_set_obj_type(RDB_object *, RDB_type *);

void
RDB_obj_set_typeinfo(RDB_object *, RDB_type *);

int
RDB_obj_to_string(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *);

int
RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *);

void
RDB_init_obj(RDB_object *);

int
RDB_destroy_obj(RDB_object *, RDB_exec_context *);

int
RDB_free_obj(RDB_object *, RDB_exec_context *);

void
RDB_bool_to_obj(RDB_object *, RDB_bool v);

void
RDB_int_to_obj(RDB_object *, RDB_int v);

void
RDB_float_to_obj(RDB_object *, RDB_float v);

int
RDB_string_to_obj(RDB_object *, const char *, RDB_exec_context *);

int
RDB_string_n_to_obj(RDB_object *, const char *, size_t,
        RDB_exec_context *);

RDB_bool
RDB_obj_bool(const RDB_object *);

RDB_int
RDB_obj_int(const RDB_object *);

RDB_float
RDB_obj_float(const RDB_object *);

int
RDB_append_string(RDB_object *, const char *, RDB_exec_context *);

int
RDB_binary_set(RDB_object *, size_t pos, const void *srcp, size_t len,
        RDB_exec_context *);

int
RDB_binary_get(const RDB_object *, size_t pos, size_t len,
        RDB_exec_context *, void **pp, size_t *alenp);


char *
RDB_obj_string(const RDB_object *);

size_t
RDB_binary_length(const RDB_object *);

int
RDB_binary_resize(RDB_object *, size_t, RDB_exec_context *);

#endif /* OBJECT_H_ */
