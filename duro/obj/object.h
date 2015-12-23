/*
 * Copyright (C) 2013-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef OBJECT_H_
#define OBJECT_H_

#include <gen/types.h>
#include <gen/hashtable.h>
#include <gen/hashmap.h>

#include <stddef.h>
#include <stdint.h>
#include <time.h>

enum RDB_obj_kind {
    RDB_OB_INITIAL,
    RDB_OB_BOOL,
    RDB_OB_INT,
    RDB_OB_FLOAT,
    RDB_OB_BIN,
    RDB_OB_TABLE,
    RDB_OB_TUPLE,
    RDB_OB_ARRAY,
    RDB_OB_TIME
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

typedef struct RDB_time {
    int16_t year;
    RDB_byte month;
    RDB_byte day;
    RDB_byte hour;
    RDB_byte minute;
    RDB_byte second;
} RDB_time;

struct tm;

/**
 * A RDB_object structure carries a value of an arbitrary type,
 * together with the type information.
 */
struct RDB_object {
    /*
     * Internal
     */

    /*
     * The type of the RDB_object. Corresponds to the declared type in TTM.
     * If the value is non-scalar and not a table, it is NULL by default,
     * but can be set by calling RDB_obj_set_typeinfo().
     * In this case, the caller is responsible for managing the type
     * (e.g. destroying the type when the RDB_object is destroyed).
     */
    RDB_type *typ;

    /*
     * An implemented type (with an internal represenation) if typ is a
     * dummy type.
     * Currently it is the MST because all supertypes are dummy types.
     */
    RDB_type *impl_typ;

    enum RDB_obj_kind kind;
    union {
        RDB_bool bool_val;
        RDB_int int_val;
        RDB_float float_val;
        RDB_time time;
        struct {
            void *datap;
            size_t len;
        } bin;
        struct {
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
            RDB_int length; /* length of array */

            /* Elements (buffer, if tbp is not NULl) */
            int capacity; /* # of RDB_objects allocated, may be larger than length */
            struct RDB_object *elemv;
        } arr;
     } val;

     unsigned int flags;

     /* Used internally for conversion into the internal representation */
     RDB_type *store_typ;

     RDB_obj_cleanup_func *cleanup_fp;
};

/* Internal */
typedef struct {
    char *key;
    RDB_object obj;
} tuple_entry;

typedef struct RDB_sequence RDB_sequence;

typedef struct {
    RDB_expression *exp;
    RDB_sequence *seqp;
} RDB_attr_default;

void *
RDB_alloc(size_t, RDB_exec_context *);

void *
RDB_realloc(void *, size_t, RDB_exec_context *);

void
RDB_free(void *);

RDB_object *
RDB_new_obj(RDB_exec_context *ecp);

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
RDB_bool_to_obj(RDB_object *, RDB_bool);

void
RDB_int_to_obj(RDB_object *, RDB_int);

void
RDB_float_to_obj(RDB_object *, RDB_float);

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

void
RDB_tm_to_obj(RDB_object *, const struct tm *);

int
RDB_append_string(RDB_object *, const char *, RDB_exec_context *);

int
RDB_append_char(RDB_object *, char, RDB_exec_context *);

int
RDB_binary_set(RDB_object *, size_t, const void *, size_t,
        RDB_exec_context *);

int
RDB_binary_get(const RDB_object *, size_t, size_t,
        RDB_exec_context *, void **, size_t *);

char *
RDB_obj_string(const RDB_object *);

size_t
RDB_binary_length(const RDB_object *);

int
RDB_binary_resize(RDB_object *, size_t, RDB_exec_context *);

RDB_bool
RDB_obj_is_const(const RDB_object *);

void
RDB_obj_set_const(RDB_object *, RDB_bool);

void
RDB_datetime_to_tm(struct tm *, const RDB_object *);

#endif /* OBJECT_H_ */
