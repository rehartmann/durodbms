/*
 * Type functions and definitions
 *
 * Copyright (C) 2013-2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef TYPE_H_
#define TYPE_H_

#include <gen/types.h>
#include "object.h"

typedef struct RDB_op_data RDB_operator;
typedef struct RDB_expression RDB_expression;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_type RDB_type;

/**@addtogroup table
 * @{
 */

/**
 * This struct is used to specify attribute definitions.
 */
typedef struct {
    /** The name of the attribute. */
    char *name;

    /** The type of the attribute. */
    RDB_type *typ;

    /**
     * If not NULL, this field must point to an RDB_expression structure
     * that specifies the default value for the attribute.
     */
    RDB_expression *defaultp;

    /**
     * This field is currently ignored.
     * It should be set to zero for compatibility
     * with future versions of DuroDBMS.
     */
    int options;
} RDB_attr;

/**
 * @}
 */

/* internal */
enum RDB_tp_kind {
    RDB_TP_SCALAR,
    RDB_TP_TUPLE,
    RDB_TP_RELATION,
    RDB_TP_ARRAY
};

/**@addtogroup type
 * @{
 */

/**
 * Specifies a possible representation.
 */
typedef struct {
    char *name;
    int compc;
    RDB_attr *compv;
} RDB_possrep;

/**
 * @}
 */

typedef struct RDB_type {
    /* internal */
    char *name;
    enum RDB_tp_kind kind;

    /* comparison function */
    RDB_operator *compare_op;

    RDB_int ireplen;

    union {
        struct RDB_type *basetyp; /* relation or array type */
        struct {
            int attrc;
            RDB_attr *attrv;
        } tuple;
        struct {
            int repc;
            RDB_possrep *repv;
            RDB_bool builtin;

            /* RDB_TRUE if selector/getters/setters are provided by the system */
            RDB_bool sysimpl;

            RDB_bool ordered;

            /* Actual representation, if the type is represented by another type.
               Otherwise NULL. */
            struct RDB_type *arep;

            RDB_expression *constraintp;
            RDB_expression *initexp;
            RDB_object init_val;
            RDB_bool init_val_is_valid;

            int supertypec;
            struct RDB_type **supertypev;
            int subtypec;
            struct RDB_type **subtypev;
        } scalar;
    } def;
    RDB_bool locked;
} RDB_type;

/**@addtogroup tuple
 * @{
 */

/** @struct RDB_renaming rdb.h <rel/rdb.h>
 * Represents an attribute renaming.
 */
typedef struct {
    char *from;
    char *to;
} RDB_renaming;

/** @struct RDB_wrapping rdb.h <rel/rdb.h>
 * Represents an attribute wrapping.
 */
typedef struct {
    int attrc;
    char **attrv;
    char *attrname;
} RDB_wrapping;

/*@}*/

RDB_bool
RDB_type_is_numeric(const RDB_type *);

RDB_bool
RDB_type_is_valid(const RDB_type *);

RDB_bool
RDB_type_is_ordered(const RDB_type *);

RDB_bool
RDB_type_is_generic(const RDB_type *);

RDB_bool
RDB_type_is_union(const RDB_type *);

RDB_bool
RDB_type_is_dummy(const RDB_type *);

RDB_bool
RDB_is_subtype(const RDB_type *, const RDB_type *);

RDB_bool
RDB_share_subtype(const RDB_type *, const RDB_type *);

RDB_bool
RDB_type_depends_type(const RDB_type *, const RDB_type *);

RDB_bool
RDB_type_has_possreps(const RDB_type *);

RDB_possrep *
RDB_type_possreps(const RDB_type *, int *);

char *
RDB_type_name(const RDB_type *);

RDB_bool
RDB_type_is_scalar(const RDB_type *);

RDB_bool
RDB_type_is_relation(const RDB_type *);

RDB_bool
RDB_type_is_tuple(const RDB_type *);

RDB_bool
RDB_type_is_array(const RDB_type *);

RDB_type *
RDB_base_type(const RDB_type *typ);

RDB_attr *
RDB_type_attrs(RDB_type *, int *);

RDB_type *
RDB_new_scalar_type(const char *, RDB_int, RDB_bool, RDB_bool,
        RDB_exec_context *);

RDB_type *
RDB_new_tuple_type(int attrc, const RDB_attr[],
        RDB_exec_context *);

RDB_type *
RDB_new_relation_type(int attrc, const RDB_attr[],
        RDB_exec_context *);

RDB_type *
RDB_new_relation_type_from_base(RDB_type *, RDB_exec_context *);

RDB_type *
RDB_new_array_type(RDB_type *, RDB_exec_context *);

RDB_type *
RDB_dup_nonscalar_type(RDB_type *typ, RDB_exec_context *);

int
RDB_del_nonscalar_type(RDB_type *, RDB_exec_context*);

RDB_bool
RDB_type_equals(const RDB_type *, const RDB_type *);

RDB_bool
RDB_type_matches(const RDB_type *, const RDB_type *);

RDB_attr *
RDB_tuple_type_attr(const RDB_type *, const char *attrname);

RDB_type *
RDB_wrap_tuple_type(const RDB_type *typ, int wrapc,
        const RDB_wrapping wrapv[], RDB_exec_context *ecp);

RDB_type *
RDB_wrap_relation_type(const RDB_type *typ, int wrapc,
        const RDB_wrapping wrapv[], RDB_exec_context *ecp);

RDB_type *
RDB_unwrap_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_exec_context *);

RDB_type *
RDB_unwrap_relation_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_exec_context *);

RDB_type *
RDB_extend_tuple_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *);

RDB_type *
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *);

RDB_type *
RDB_project_tuple_type(const RDB_type *typ, int attrc, const char *attrv[],
                          RDB_exec_context *);

RDB_type *
RDB_project_relation_type(const RDB_type *typ, int, const char *[],
                          RDB_exec_context *);

RDB_type *
RDB_group_type(const RDB_type *, int, char *[], const char *,
        RDB_exec_context *);

RDB_type *
RDB_ungroup_type(RDB_type *typ, const char *attr, RDB_exec_context *);

RDB_type *
RDB_rename_tuple_type(const RDB_type *, int, const RDB_renaming[],
        RDB_exec_context *);

RDB_type *
RDB_rename_relation_type(const RDB_type *, int, const RDB_renaming[],
        RDB_exec_context *);

void
RDB_lock_type(RDB_type *);

#endif /* TYPE_H_ */
