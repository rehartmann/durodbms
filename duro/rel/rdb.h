#ifndef RDB_RDB_H
#define RDB_RDB_H

/*
$Id$

This file is part of Duro, a relational database management system.
Copyright (C) 2003-2012 Rene Hartmann.

Duro is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

Duro is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Duro; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#include <rec/env.h>
#include <rec/recmap.h>
#include <gen/hashmap.h>
#include <gen/hashtable.h>
#include <gen/types.h>

#include <stdlib.h>

enum {
    RDB_ERROR = -1,
    RDB_UNBUFFERED = 1
};

typedef struct {
    char *name;
    int compc;
    struct RDB_attr *compv;
} RDB_possrep;

enum _RDB_obj_kind {
    RDB_OB_INITIAL,
    RDB_OB_BOOL,
    RDB_OB_INT,
    RDB_OB_FLOAT,
    RDB_OB_BIN,
    RDB_OB_TABLE,
    RDB_OB_TUPLE,
    RDB_OB_ARRAY
};

typedef struct {
    /** The number of strings. */
    int strc;
    /** The array holding pointers to the strings. */
    char **strv;
} RDB_string_vec;

typedef struct RDB_expression RDB_expression;

typedef struct RDB_expr_list {
    RDB_expression *firstp;
    RDB_expression *lastp;
    struct RDB_expr_list *nextp;
} RDB_expr_list;

/**
 * A RDB_object structure carries a value of an arbitrary type,
 * together with the type information.
 */
typedef struct RDB_object {
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
    struct RDB_type *typ;

    enum _RDB_obj_kind kind;
    union {
        RDB_bool bool_val;
        RDB_int int_val;
        RDB_float float_val;
        struct {
            void *datap;
            size_t len;
        } bin;
        struct {
            RDB_bool is_user;
            RDB_bool is_persistent;
            char *name;

            /*
             * Candidate keys. NULL if table is virtual and the keys have not been
             * inferred.
             */
            int keyc;
            RDB_string_vec *keyv;
            
            /* NULL if it's a real table */
            RDB_expression *exp;

            struct RDB_stored_table *stp;
        } tb;
        RDB_hashtable tpl_tab;
        struct {
            struct RDB_expression *texp;
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
     } var;

     /* Used internally for conversion into the internal representation */
     struct RDB_type *store_typ;
} RDB_object;

typedef struct {
    RDB_bool error_active;
    RDB_object error;
    RDB_hashmap pmap;
} RDB_exec_context;

typedef struct RDB_type RDB_type;

typedef struct RDB_op_data RDB_operator;

/* internal */
enum _RDB_tp_kind {
    RDB_TP_SCALAR,
    RDB_TP_TUPLE,
    RDB_TP_RELATION,
    RDB_TP_ARRAY
};

struct RDB_type {
    /* internal */
    char *name;
    enum _RDB_tp_kind kind;

    /* comparison function */
    RDB_operator *compare_op;

    RDB_int ireplen;

    union {
        struct RDB_type *basetyp; /* relation or array type */
        struct {
            int attrc;
            struct RDB_attr *attrv;
        } tuple;
        struct {
            int repc;
            RDB_possrep *repv;
            RDB_bool builtin;

            /* RDB_TRUE if selector/getters/setters are provided by the system */
            RDB_bool sysimpl; 

            /* Actual representation, if the type is represented by another type.
               Otherwise NULL. */
            struct RDB_type *arep; 

            RDB_expression *constraintp;
        } scalar;
    } var;
};

typedef struct RDB_parameter {
    /**
     * Parameter type
     */
    RDB_type *typ;
    /**
     * RDB_TRUE if and only if it's an update parameter.
     * Defined only for update operators.
     */
    RDB_bool update;
} RDB_parameter;

typedef void RDB_op_cleanup_func(RDB_operator *);

#if defined (_WIN32) && !defined (NO_DLL_IMPORT)
#define _RDB_EXTERN_VAR __declspec(dllimport)
#else
#define _RDB_EXTERN_VAR extern
#endif

/*
 * Built-in primitive data types
 */
_RDB_EXTERN_VAR RDB_type RDB_BOOLEAN;
_RDB_EXTERN_VAR RDB_type RDB_INTEGER;
_RDB_EXTERN_VAR RDB_type RDB_FLOAT;
_RDB_EXTERN_VAR RDB_type RDB_STRING;
_RDB_EXTERN_VAR RDB_type RDB_BINARY;

/*
 * Error types
 */
_RDB_EXTERN_VAR RDB_type RDB_NO_RUNNING_TX_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_INVALID_ARGUMENT_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_TYPE_MISMATCH_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_NOT_FOUND_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_OPERATOR_NOT_FOUND_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_NAME_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_ELEMENT_EXISTS_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_TYPE_CONSTRAINT_VIOLATION_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_KEY_VIOLATION_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_PREDICATE_VIOLATION_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_IN_USE_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_AGGREGATE_UNDEFINED_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_VERSION_MISMATCH_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_NOT_SUPPORTED_ERROR;

_RDB_EXTERN_VAR RDB_type RDB_NO_MEMORY_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_LOCK_NOT_GRANTED_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_DEADLOCK_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_RESOURCE_NOT_FOUND_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_INTERNAL_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_FATAL_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_SYSTEM_ERROR;

_RDB_EXTERN_VAR RDB_type RDB_SYNTAX_ERROR;

/* I/O type */

_RDB_EXTERN_VAR RDB_type RDB_IO_STREAM;

typedef struct {
    char *name;
    RDB_expression *exp;
} RDB_virtual_attr;

typedef struct {
    char *from;
    char *to;
} RDB_renaming;

typedef struct {
    int attrc;
    char **attrv;
    char *attrname;
} RDB_wrapping;

typedef struct {
    /** Attribute name. */
    char *attrname;
    /** RDB_TRUE if order is ascending, RDB_FALSE if order is descending. */
    RDB_bool asc;
} RDB_seq_item;

typedef struct RDB_database RDB_database;

typedef struct RDB_transaction {
    /* internal */
    RDB_database *dbp;
    RDB_environment *envp;
    DB_TXN *txid;
    void *user_data;
    struct RDB_transaction *parentp;
    struct RDB_rmlink *delrmp;
    struct RDB_ixlink *delixp;
} RDB_transaction;

typedef RDB_object *RDB_getobjfn(const char *, void *);

typedef RDB_type *RDB_gettypefn(const char *, void *);

char *
RDB_db_name(RDB_database *dbp);

RDB_environment *
RDB_db_env(RDB_database *);

RDB_database *
RDB_get_sys_db(RDB_environment *, RDB_exec_context *);

RDB_database *
RDB_create_db_from_env(const char *name, RDB_environment *envp,
        RDB_exec_context *);

RDB_database *
RDB_get_db_from_env(const char *name, RDB_environment *, RDB_exec_context *);

int
RDB_drop_db(RDB_database *, RDB_exec_context *);

int
RDB_get_dbs(RDB_environment *, RDB_object *, RDB_exec_context *);

typedef struct RDB_attr {
    /** The name of the attribute. */
    char *name;

    /** The type of the attribute. */
    RDB_type *typ;

    /**
     * If not NULL, this field must point to an RDB_object structure
     * that specifies the default value for the attribute.
     */
    RDB_object *defaultp;

    /**
     * This field is currently ignored.
     * It should be set to zero for compatibility
     * with future versions of Duro.
     */
    int options;
} RDB_attr;

RDB_object *
RDB_create_table(const char *name,
        int attrc, const RDB_attr attrv[],
        int keyc, const RDB_string_vec keyv[],
        RDB_exec_context *, RDB_transaction *);

RDB_object *
RDB_create_table_from_type(const char *name,
                RDB_type *reltyp,
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *, RDB_transaction *);

int
RDB_init_table(RDB_object *tbp, const char *name,
        int attrc, const RDB_attr attrv[],
        int keyc, const RDB_string_vec keyv[],
        RDB_exec_context *ecp);

int
RDB_init_table_from_type(RDB_object *tbp, const char *name, RDB_type *reltyp,
        int keyc, const RDB_string_vec keyv[], RDB_exec_context *);

RDB_object *
RDB_get_table(const char *name, RDB_exec_context *, RDB_transaction *);

int
RDB_drop_table(RDB_object *tbp, RDB_exec_context *, RDB_transaction *);

int
RDB_table_keys(RDB_object *, RDB_exec_context *, RDB_string_vec **keyvp);

char *
RDB_table_name(const RDB_object *);

int
RDB_set_table_name(RDB_object *tbp, const char *name, RDB_exec_context *,
        RDB_transaction *);

int
RDB_add_table(RDB_object *, RDB_exec_context *, RDB_transaction *);

typedef struct {
    const char *name;
    RDB_expression *exp;
} RDB_attr_update;

int
RDB_insert(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *,
        RDB_transaction *);

RDB_int
RDB_update(RDB_object *, RDB_expression *, int attrc,
        const RDB_attr_update updv[], RDB_exec_context *, RDB_transaction *);

RDB_int
RDB_delete(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *,
        RDB_transaction *);

int
RDB_copy_table(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *,
        RDB_transaction *);

RDB_int
RDB_move_tuples(RDB_object *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

typedef struct {
    RDB_object *tbp;
    RDB_object *objp;
} RDB_ma_insert;

typedef struct {
    RDB_object *tbp;
    RDB_expression *condp;
    int updc;
    RDB_attr_update *updv;
} RDB_ma_update;

typedef struct {
    RDB_object *tbp;
    RDB_expression *condp;
} RDB_ma_delete;

typedef struct {
    RDB_object *dstp;
    RDB_object *srcp;
} RDB_ma_copy;

RDB_int
RDB_multi_assign(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *, RDB_transaction *);

int
RDB_max(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *, RDB_object *resultp);

int
RDB_min(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *, RDB_object *resultp);

int
RDB_all(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *, RDB_bool *resultp);

int
RDB_any(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *, RDB_bool *resultp);

int
RDB_sum(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *, RDB_object *resultp);

int
RDB_avg(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *, RDB_float *resultp);

int
RDB_table_contains(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *,
        RDB_transaction *, RDB_bool *resultp);

int
RDB_subset(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *,
        RDB_transaction *, RDB_bool *resultp);

int
RDB_extract_tuple(RDB_object *, RDB_exec_context *, RDB_transaction *,
        RDB_object *);

RDB_bool
RDB_table_is_persistent(const RDB_object *);

RDB_bool
RDB_table_is_real(const RDB_object *);

int
RDB_table_is_empty(RDB_object *, RDB_exec_context *, RDB_transaction *,
        RDB_bool *resultp);

RDB_int
RDB_cardinality(RDB_object *tbp, RDB_exec_context *, RDB_transaction *);

RDB_object *
RDB_expr_to_vtable(RDB_expression *, RDB_exec_context *, RDB_transaction *);

int
RDB_create_table_index(const char *name, RDB_object *tbp, int idxcompc,
        const RDB_seq_item idxcompv[], int flags, RDB_exec_context *,
        RDB_transaction *);

int
RDB_drop_table_index(const char *name, RDB_exec_context *, RDB_transaction *);

int
RDB_define_type(const char *name, int repc, const RDB_possrep repv[],
                RDB_expression *constraintp, RDB_exec_context *,
                RDB_transaction *);

RDB_type *
RDB_get_type(const char *name, RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_dup_nonscalar_type(RDB_type *typ, RDB_exec_context *);

RDB_bool
RDB_type_is_numeric(const RDB_type *);

RDB_type *
RDB_create_tuple_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *);

RDB_type *
RDB_create_relation_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *);

RDB_type *
RDB_create_relation_type_from_base(RDB_type *, RDB_exec_context *);

RDB_type *
RDB_create_array_type(RDB_type *, RDB_exec_context *);

int
RDB_drop_type(RDB_type *, RDB_exec_context *, RDB_transaction *);

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
RDB_base_type(RDB_type *typ);

RDB_attr *
RDB_type_attrs(RDB_type *, int *);

RDB_bool
RDB_type_equals(const RDB_type *, const RDB_type *);

int
RDB_next_attr_sorted(const RDB_type *, const char *);

RDB_type *
RDB_obj_type(const RDB_object *);

void
RDB_obj_set_typeinfo(RDB_object *, RDB_type *);

RDB_type *
RDB_type_attr_type(const RDB_type *, const char *);

RDB_bool
RDB_is_selector(const RDB_operator *);

/** @addtogroup tx
 * @{
 */

/**
Return the database the transaction pointed
to by <var>txp</var> interacts with.

@returns

A pointer to the RDB_database structure that represents the database.
 */
#define RDB_tx_db(txp) ((txp)->dbp)

/** @} */

RDB_bool
RDB_tx_is_running(RDB_transaction *);

int
RDB_begin_tx(RDB_exec_context *, RDB_transaction *, RDB_database *dbp,
        RDB_transaction *parent);

int
RDB_commit(RDB_exec_context *, RDB_transaction *);

int
RDB_rollback(RDB_exec_context *, RDB_transaction *);

int
RDB_tuple_set(RDB_object *, const char *name, const RDB_object *,
        RDB_exec_context *);

int
RDB_tuple_set_bool(RDB_object *, const char *name, RDB_bool val,
        RDB_exec_context *);

int
RDB_tuple_set_int(RDB_object *, const char *name, RDB_int val,
        RDB_exec_context *);

int
RDB_tuple_set_float(RDB_object *, const char *name, RDB_float val,
        RDB_exec_context *);

int
RDB_tuple_set_string(RDB_object *, const char *name, const char *,
        RDB_exec_context *);

RDB_object *
RDB_tuple_get(const RDB_object *, const char *name);

RDB_bool
RDB_tuple_get_bool(const RDB_object *, const char *name);

RDB_int
RDB_tuple_get_int(const RDB_object *, const char *name);

RDB_float
RDB_tuple_get_float(const RDB_object *, const char *name);

RDB_int
RDB_tuple_size(const RDB_object *);

void
RDB_tuple_attr_names(const RDB_object *, char **namev);

char *
RDB_tuple_get_string(const RDB_object *, const char *name);

int
RDB_project_tuple(const RDB_object *, int attrc, char *attrv[],
                 RDB_exec_context *, RDB_object *restplp);

int
RDB_remove_tuple(const RDB_object *, int attrc, char *attrv[],
                 RDB_exec_context *, RDB_object *restplp);

int
RDB_extend_tuple(RDB_object *, int attrc, const RDB_virtual_attr attrv[],
                 RDB_exec_context *, RDB_transaction *);

int
RDB_add_tuple(RDB_object *, const RDB_object *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_join_tuples(const RDB_object *, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, RDB_object *);

int
RDB_rename_tuple(const RDB_object *, int renc, const RDB_renaming renv[],
                 RDB_exec_context *, RDB_object *restplp);

int
RDB_wrap_tuple(const RDB_object *tplp, int wrapc, const RDB_wrapping wrapv[],
               RDB_exec_context *, RDB_object *restplp);

int
RDB_unwrap_tuple(const RDB_object *tplp, int attrc, char *attrv[],
        RDB_exec_context *, RDB_object *restplp);

int
RDB_table_to_array(RDB_object *arrp, RDB_object *, 
                   int seqitc, const RDB_seq_item seqitv[], int flags,
                   RDB_exec_context *, RDB_transaction *);

RDB_object *
RDB_array_get(RDB_object *, RDB_int idx, RDB_exec_context *);

int
RDB_array_set(RDB_object *, RDB_int idx, const RDB_object *tplp,
        RDB_exec_context *);

RDB_int
RDB_array_length(RDB_object *, RDB_exec_context *);

int
RDB_set_array_length(RDB_object *arrp, RDB_int len, RDB_exec_context *);

int
RDB_obj_equals(const RDB_object *, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

int
RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *);

void
RDB_init_obj(RDB_object *);

int
RDB_destroy_obj(RDB_object *, RDB_exec_context *);

void
RDB_bool_to_obj(RDB_object *, RDB_bool v);

void
RDB_int_to_obj(RDB_object *, RDB_int v);

void
RDB_float_to_obj(RDB_object *, RDB_float v);

int
RDB_string_to_obj(RDB_object *, const char *str, RDB_exec_context *);

int
RDB_obj_to_string(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *);

int
RDB_append_string(RDB_object *, const char *, RDB_exec_context *);

int
RDB_obj_comp(const RDB_object *, const char *compname,
                   RDB_object *comp, RDB_exec_context *, RDB_transaction *);

int
RDB_obj_set_comp(RDB_object *, const char *compname,
                 const RDB_object *comp, RDB_exec_context *,
                 RDB_transaction *);

RDB_bool
RDB_obj_bool(const RDB_object *);

RDB_int
RDB_obj_int(const RDB_object *);

RDB_float
RDB_obj_float(const RDB_object *);

char *
RDB_obj_string(const RDB_object *);

int
RDB_binary_set(RDB_object *, size_t pos, const void *srcp, size_t len,
        RDB_exec_context *);

int
RDB_binary_get(const RDB_object *, size_t pos, size_t len,
        RDB_exec_context *, void **pp, size_t *alenp);

size_t
RDB_binary_length(const RDB_object *);

void
_RDB_set_obj_type(RDB_object *, RDB_type *typ);

RDB_bool
RDB_expr_is_const(const RDB_expression *);

RDB_expression *
RDB_bool_to_expr(RDB_bool, RDB_exec_context *);

RDB_expression *
RDB_int_to_expr(RDB_int, RDB_exec_context *);

RDB_expression *
RDB_float_to_expr(RDB_float, RDB_exec_context *);

RDB_expression *
RDB_string_to_expr(const char *, RDB_exec_context *);

RDB_expression *
RDB_obj_to_expr(const RDB_object *, RDB_exec_context *);

RDB_expression *
RDB_table_ref(RDB_object *tbp, RDB_exec_context *);

RDB_expression *
RDB_var_ref(const char *varname, RDB_exec_context *);

RDB_expression *
RDB_eq(RDB_expression *, RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_tuple_attr(RDB_expression *, const char *attrname, RDB_exec_context *);

RDB_expression *
RDB_expr_comp(RDB_expression *, const char *, RDB_exec_context *);

RDB_expression *
RDB_ro_op(const char *opname, RDB_exec_context *);

void
RDB_add_arg(RDB_expression *, RDB_expression *argp);

RDB_object *
RDB_expr_obj(RDB_expression *);

void
RDB_set_expr_type(RDB_expression *, RDB_type *);

void
RDB_init_expr_list(RDB_expr_list *explistp);

int
RDB_destroy_expr_list(RDB_expr_list *, RDB_exec_context *);

RDB_int
RDB_expr_list_length(const RDB_expr_list *);

void
RDB_expr_list_append(RDB_expr_list *, RDB_expression *);

void
RDB_join_expr_lists(RDB_expr_list *, RDB_expr_list *);

int
RDB_evaluate(RDB_expression *, RDB_getobjfn *, void *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

RDB_type *
RDB_expr_type(RDB_expression *, RDB_gettypefn *, void *,
        RDB_exec_context *, RDB_transaction *);

const char *
RDB_expr_op_name(const RDB_expression *);

const char *
RDB_expr_var_name(const RDB_expression *);

int
RDB_evaluate_bool(RDB_expression *, RDB_getobjfn *getfnp, void *getdata,
        RDB_exec_context *, RDB_transaction *, RDB_bool *);

int
RDB_drop_expr(RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_dup_expr(const RDB_expression *, RDB_exec_context *);

int
RDB_expr_resolve_varname_expr(RDB_expression **, const char *,
        RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_expr_resolve_varnames(RDB_expression *, RDB_getobjfn *,
        void *, RDB_exec_context *, RDB_transaction *);

int
RDB_create_ro_op(const char *name, int, RDB_parameter[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const void *iargp, size_t iarglen, 
                 RDB_exec_context *, RDB_transaction *);

int
RDB_create_update_op(const char *name, int, RDB_parameter[],
                  const char *libname, const char *symname,
                  const void *iargp, size_t iarglen,
                  RDB_exec_context *, RDB_transaction *);

RDB_operator *
RDB_get_update_op(const char *name, int argc, RDB_type *argtv[],
               RDB_exec_context *ecp, RDB_transaction *txp);

int
RDB_call_ro_op_by_name(const char *, int, RDB_object *[],
               RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_call_update_op_by_name(const char *name, int argc, RDB_object *argv[],
                RDB_exec_context *, RDB_transaction *);

int
RDB_call_ro_op(RDB_operator *, int, RDB_object *[],
               RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_call_update_op(RDB_operator *, int argc, RDB_object *[],
                RDB_exec_context *, RDB_transaction *);

int
RDB_drop_op(const char *name, RDB_exec_context *, RDB_transaction *);

RDB_parameter *
RDB_get_parameter(const RDB_operator *, int);

const char *
RDB_operator_name(const RDB_operator *);

RDB_type *
RDB_return_type(const RDB_operator *);

size_t
RDB_operator_iarglen(const RDB_operator *);

void *
RDB_operator_iargp(const RDB_operator *);

void *
RDB_op_u_data(const RDB_operator *);

void
RDB_set_op_u_data(RDB_operator *, void *);

void
RDB_set_op_cleanup_fn(RDB_operator *,  RDB_op_cleanup_func*);

int
RDB_create_constraint(const char *name, RDB_expression *,
                      RDB_exec_context *, RDB_transaction *);

int
RDB_drop_constraint(const char *name, RDB_exec_context *,
        RDB_transaction *);

void
RDB_init_exec_context(RDB_exec_context *);

void
RDB_destroy_exec_context(RDB_exec_context *);

RDB_object *
RDB_raise_err(RDB_exec_context *);

RDB_object *
RDB_get_err(RDB_exec_context *);

void
RDB_clear_err(RDB_exec_context *);

RDB_object *
RDB_raise_no_memory(RDB_exec_context *);

RDB_object *
RDB_raise_invalid_argument(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_no_running_tx(RDB_exec_context *);

RDB_object *
RDB_raise_not_found(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_type_mismatch(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_operator_not_found(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_type_constraint_violation(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_element_exists(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_not_supported(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_name(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_predicate_violation(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_in_use(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_system(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_resource_not_found(const char *info, RDB_exec_context *);

RDB_object *
RDB_raise_internal(const char *info, RDB_exec_context *);

RDB_object *
RDB_raise_lock_not_granted(RDB_exec_context *);

RDB_object *
RDB_raise_aggregate_undefined(RDB_exec_context *);

RDB_object *
RDB_raise_version_mismatch(RDB_exec_context *);

RDB_object *
RDB_raise_syntax(const char *, RDB_exec_context *);

void
RDB_errcode_to_error(int errcode, RDB_exec_context *, RDB_transaction *);

int
RDB_ec_set_property(RDB_exec_context *, const char *name, void *);

void *
RDB_ec_get_property(RDB_exec_context *, const char *name);

void *
RDB_alloc(size_t, RDB_exec_context *);

void *
RDB_realloc(void *, size_t, RDB_exec_context *);

void
RDB_free(void *);

int
RDB_major_version(void);

int
RDB_minor_version(void);

#endif
