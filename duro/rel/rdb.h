#ifndef RDB_RDB_H
#define RDB_RDB_H

/*
$Id$

This file is part of Duro, a relational database library.
Copyright (C) 2003-2006 René Hartmann.

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

enum {
    RDB_ERROR = -1
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
    RDB_OB_DOUBLE,
    RDB_OB_BIN,
    RDB_OB_TABLE,
    RDB_OB_TUPLE,
    RDB_OB_ARRAY
};

typedef struct {
    int strc;
    char **strv;
} RDB_string_vec;

typedef struct RDB_expression RDB_expression;

/*
 * A RDB_object structure carries a value of an arbitrary type,
 * together with the type information.
 */
typedef struct RDB_object {
    /* internal */
    struct RDB_type *typ;	/* Type, NULL for non-scalar types */
    enum _RDB_obj_kind kind;
    union {
        RDB_bool bool_val;
        RDB_int int_val;
        RDB_float float_val;
        RDB_double double_val;
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
} RDB_object;

typedef struct {
    RDB_bool error_active;
    RDB_object error;
    RDB_hashmap pmap;
} RDB_exec_context;

typedef int RDB_ro_op_func(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *,
        struct RDB_transaction *txp,
        RDB_object *retvalp);

/* internal */
enum _RDB_tp_kind {
    RDB_TP_SCALAR,
    RDB_TP_TUPLE,
    RDB_TP_RELATION,
    RDB_TP_ARRAY
};

typedef struct RDB_type {
    /* internal */
    char *name;
    enum _RDB_tp_kind kind;

    /* comparison function */
    RDB_ro_op_func *comparep;
    size_t compare_iarglen;
    void *compare_iargp;

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
} RDB_type;

#if defined (_WIN32) && !defined (NO_DLL_IMPORT)
#define _RDB_EXTERN_VAR __declspec(dllimport)
#else
#define _RDB_EXTERN_VAR extern
#endif

/*
 * Built-in types
 */

_RDB_EXTERN_VAR RDB_type RDB_BOOLEAN;
_RDB_EXTERN_VAR RDB_type RDB_INTEGER;
_RDB_EXTERN_VAR RDB_type RDB_FLOAT;
_RDB_EXTERN_VAR RDB_type RDB_DOUBLE;
_RDB_EXTERN_VAR RDB_type RDB_STRING;
_RDB_EXTERN_VAR RDB_type RDB_BINARY;

/*
 * Error types
 */
_RDB_EXTERN_VAR RDB_type RDB_INVALID_TRANSACTION_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_INVALID_ARGUMENT_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_TYPE_MISMATCH_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_NOT_FOUND_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_OPERATOR_NOT_FOUND_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_ATTRIBUTE_NOT_FOUND_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_ELEMENT_EXISTS_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_TYPE_CONSTRAINT_VIOLATION_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_KEY_VIOLATION_ERROR;
_RDB_EXTERN_VAR RDB_type RDB_PREDICATE_VIOLATION_ERROR;
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

typedef struct {
    char *name;
    RDB_expression *exp;
} RDB_virtual_attr;

typedef enum {
    RDB_COUNT, RDB_SUM, RDB_AVG, RDB_MAX, RDB_MIN, RDB_ALL, RDB_ANY,
    RDB_COUNTD, RDB_SUMD, RDB_AVGD
} RDB_aggregate_op;

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
    char *attrname;
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

/*
 * Return the name of the database.
 */
char *
RDB_db_name(RDB_database *dbp);

/*
 * Return the DB environment the database belongs to.
 */
RDB_environment *
RDB_db_env(RDB_database *);

/*
 * Create a database with name name in the environment pointed to
 * by envp.
 * Return a pointer to the database on success, or NULL on error.
 */
RDB_database *
RDB_create_db_from_env(const char *name, RDB_environment *envp,
        RDB_exec_context *);

/*
 * Get the database with name name in the environment pointed to
 * by envp. Return a pointer to the newly created RDB_database structure
 * on success, or NULL on error.
 */
RDB_database *
RDB_get_db_from_env(const char *name, RDB_environment *, RDB_exec_context *);

/*
 * Drop the database with name dbname.
 * The database must not contain any user tables.
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_drop_db(RDB_database *, RDB_exec_context *);

typedef struct RDB_attr {
    char *name;
    RDB_type *typ;
    RDB_object *defaultp;
    int options;
} RDB_attr;

/*
 * Create a table with name name in the database associated with the transaction
 * pointed to by txp.
 * If persistent is RDB_FALSE, create a transient table. In this case,
 * name may be NULL.
 * The attributes are specified by attrc and heading.
 * The candidate keys are specified by keyc and keyv.
 * At least one candidate key must be specified.
 *
 * Return a pointer to the newly created table on success, or NULL on error.
 */
RDB_object *
RDB_create_table(const char *name, RDB_bool persistent,
        int attrc, const RDB_attr attrv[],
        int keyc, const RDB_string_vec keyv[],
        RDB_exec_context *, RDB_transaction *);

RDB_object *
RDB_create_table_from_type(const char *name, RDB_bool persistent,
                RDB_type *reltyp,
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *, RDB_transaction *);

int
RDB_init_table(RDB_object *tbp, const char *name, RDB_type *reltyp,
        int keyc, const RDB_string_vec keyv[], RDB_exec_context *);

/*
 * Lookup the table with name name from the database pointed to by dbp.
 * Return a pointer to a RDB_object structure on success, or NULL
 * if the table could not be found.
 */
RDB_object *
RDB_get_table(const char *name, RDB_exec_context *, RDB_transaction *);

/*
 * Drop the table pointed to by tbp. If it's a persistent table,
 * delete it.
 * If it's a virtual table, drop all unnamed "subtables".
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_drop_table(RDB_object *tbp, RDB_exec_context *, RDB_transaction *);

int
RDB_table_keys(RDB_object *, RDB_exec_context *, RDB_string_vec **keyvp);

char *
RDB_table_name(const RDB_object *);

int
RDB_set_table_name(RDB_object *tbp, const char *name, RDB_exec_context *,
        RDB_transaction *);

/*
 * Associate a table with a database.
 * The table must be named.
 */
int
RDB_add_table(RDB_object *, RDB_exec_context *, RDB_transaction *);

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

/*
 * Lookup the type with name name from the database pointed to by dbp.
 * Return a pointer to a RDB_type structure on success, or NULL
 * if the table could not be found.
 */
RDB_type *
RDB_get_type(const char *name, RDB_exec_context *, RDB_transaction *);

/*
 * Return the database associated with the transaction.
 */
#define RDB_tx_db(txp) ((txp)->dbp)

/*
 * Return the database environment associated with the transaction.
 */
#define RDB_tx_env(txp) ((txp)->envp)

RDB_bool
RDB_tx_is_running(RDB_transaction *txp);

/*
 * Start the transaction pointed to by txp.
 * The transaction will be associated to the database pointed to by dbp.
 * If parentp is not NULL, the transaction becomes a subtransaction
 * of the transaction pointed to by parentp.
 * 
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_begin_tx(RDB_exec_context *, RDB_transaction *txp, RDB_database *dbp,
        RDB_transaction *parent);

/*
 * Commit the transaction pointed to by txp.
 *
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 * If the transaction has already been committed or aborted,
 * DB_INVALID_TRANSACTION is returned.
 */
int
RDB_commit(RDB_exec_context *, RDB_transaction *);

/*
 * Abort the transaction pointed to by txp.
 *
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 * If the transaction is already committed or aborted, INVALID_TRANSACTION_ERROR
 * is raised.
 * Note that a transaction may be aborted implicity by a database operation.
 * (see TTM, OO prescription 4)
 */
int
RDB_rollback(RDB_exec_context *, RDB_transaction *);

typedef struct {
    char *name;
    RDB_expression *exp;
} RDB_attr_update;

/*
 * Insert the tuple pointed to by tplp into the table pointed to by tbp.
 * All attributes of the table for which a default attribute is not provided
 * must be set. Other tuple attributes are ignored.
 */
int
RDB_insert(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *,
        RDB_transaction *);

/*
 * Update the tuple which satisfy the condition pointed to by condp.
 * RDB_update() is currently not supported for virtual relvars.
 *
 * If condp is NULL, all tuples will be updated.
 */
RDB_int
RDB_update(RDB_object *, RDB_expression *, int attrc,
        const RDB_attr_update updv[], RDB_exec_context *, RDB_transaction *);

/*
 * Delete the tuple for which the expression pointedto by condp
 * evaluates to true from the table pointed to by tbp.
 * 
 * If condp is NULL, all tuples will be deleted.
 *
 * Deleting records from virtual relvars is currently only supported for
 * UNION and INTERSECT.
 */
RDB_int
RDB_delete(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *,
        RDB_transaction *);

/*
 * Assign table srcp to table dstp.
 */
int
RDB_copy_table(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *,
        RDB_transaction *);

/*
 * Aggregate operators.
 */

int
RDB_max(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *txp, RDB_object *resultp);

int
RDB_min(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *txp, RDB_object *resultp);

int
RDB_all(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *txp, RDB_bool *resultp);

int
RDB_any(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *txp, RDB_bool *resultp);

int
RDB_sum(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *txp, RDB_object *resultp);

int
RDB_avg(RDB_object *tbp, const char *attrname, RDB_exec_context *,
        RDB_transaction *txp, RDB_double *resultp);

/*
 * Check if the table pointed to by tbp contains the tuple
 * pointed to by tplp.
 */
int
RDB_table_contains(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *,
        RDB_transaction *, RDB_bool *resultp);

int
RDB_subset(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *,
        RDB_transaction *, RDB_bool *resultp);

/*
 * Extract a tuple from the table. The table must contain
 * exactly one tuple.
 *
 * Returns RDB_OK if the tuple has been successfully extracted,
 * Returns RDB_ERROR and raises RDB_NOT_FOUND_ERROR if the table is empty.
 * Returns RDB_ERROR and raises RDB_INVALID_ARGUMENT_ERROR the table contains
 * more than one tuple.
 */
int
RDB_extract_tuple(RDB_object *, RDB_exec_context *, RDB_transaction *,
        RDB_object *);

RDB_bool
RDB_table_is_persistent(const RDB_object *);

/*
 * Store RDB_TRUE in the location pointed to by resultp if the table
 * is nonempty, RDB_FALSE otherwise.
 */
int
RDB_table_is_empty(RDB_object *, RDB_exec_context *, RDB_transaction *,
        RDB_bool *resultp);

RDB_int
RDB_cardinality(RDB_object *tbp, RDB_exec_context *, RDB_transaction *);

/*
 * Create a virtual relvar from an expression.
 */
RDB_object *
RDB_expr_to_vtable(RDB_expression *, RDB_exec_context *, RDB_transaction *);

/*
 * Functions for creation/destruction of tuples and reading/modifying attributes.
 * RDB_object represents a tuple variable.
 */

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
RDB_tuple_set_double(RDB_object *, const char *name, RDB_double val,
        RDB_exec_context *);

int
RDB_tuple_set_string(RDB_object *, const char *name, const char *valp,
        RDB_exec_context *);

/*
 * Return a pointer to the tuple's value corresponding to name name.
 * The pointer returned will become invalid if RDB_destroy_tuple()
 * is called if the attribute is overwritten
 * by calling RDB_tuple_set() with the same name argument.
 * If an attribute of name name does not exist, NULL is returned.
 */
RDB_object *
RDB_tuple_get(const RDB_object *, const char *name);

RDB_bool
RDB_tuple_get_bool(const RDB_object *, const char *name);

RDB_int
RDB_tuple_get_int(const RDB_object *, const char *name);

RDB_float
RDB_tuple_get_float(const RDB_object *, const char *name);

RDB_double
RDB_tuple_get_double(const RDB_object *, const char *name);

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

/*
 * Convert a RDB_object to a RDB_object and store the array in
 * the location pointed to by arrpp.
 * Specifying the order of the elements by seqitc and seqitv
 * is currently not supported - the only permissible
 * value of seqitc is 0.
 * The array becomes invalid when the transaction ends.
 */
int
RDB_table_to_array(RDB_object *arrp, RDB_object *, 
                   int seqitc, const RDB_seq_item seqitv[],
                   RDB_exec_context *, RDB_transaction *);

/*
 * Get a pointer to the tuple with index idx.
 */
RDB_object *
RDB_array_get(RDB_object *, RDB_int idx, RDB_exec_context *);

int
RDB_array_set(RDB_object *, RDB_int idx, const RDB_object *tplp,
        RDB_exec_context *);

/*
 * Return the length of the array.
 * A return value < 0 indicates an error.
 */
RDB_int
RDB_array_length(RDB_object *, RDB_exec_context *);

int
RDB_set_array_length(RDB_object *arrp, RDB_int len, RDB_exec_context *);

RDB_bool
RDB_type_is_numeric(const RDB_type *);

/*
 * Create an anonymous tuple type from the attributes given by attrv.
 */
RDB_type *
RDB_create_tuple_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *);

/*
 * Create an anonymous relation type from the attributes given by attrv.
 */
RDB_type *
RDB_create_relation_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *);

RDB_type *
RDB_create_array_type(RDB_type *, RDB_exec_context *);

/*
 * Drop a type.
 * The transaction argument may be NULL if the type has no name.
 */
int
RDB_drop_type(RDB_type *, RDB_exec_context *, RDB_transaction *);

/*
 * Return the name of the type pointed to by typ.
 * If it's an anonymous type, NULL is returned.
 */
char *
RDB_type_name(const RDB_type *);

/*
 * Return RDB_TRUE if the type pointed to by typ is a scalar type,
 * or RDB_FALSE if it's a user type.
 */
RDB_bool
RDB_type_is_scalar(const RDB_type *);

RDB_attr *
RDB_type_attrs(RDB_type *, int *);

/*
 * Return RDB_TRUE if the two types are equal
 * or RDB_FALSE if they are not .
 * Two types are equal if there definition is equal.
 */
RDB_bool
RDB_type_equals(const RDB_type *, const RDB_type *);

RDB_type *
RDB_obj_type(const RDB_object *);

RDB_type *
RDB_type_attr_type(const RDB_type *, const char *);

/*
 * Stores RDB_TRUE in the result if the two RDB_objects are equal
 * or RDB_FALSE if they are not.
 */
int
RDB_obj_equals(const RDB_object *, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

/*
 * Initialize the value pointed to by dstvalp with the value
 * pointed to by scrvalp.
 * Return RDB_OK on success, RDB_NO_MEMORY if allocating memory failed.
 */
int
RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *);

/*
 * This function must be called before copying a value into a RDB_obj.
 */
void
RDB_init_obj(RDB_object *valp);

/*
 * Release the resources associated with the value pointed to by valp.
 */
int
RDB_destroy_obj(RDB_object *valp, RDB_exec_context *);

void
RDB_bool_to_obj(RDB_object *valp, RDB_bool v);

void
RDB_int_to_obj(RDB_object *valp, RDB_int v);

void
RDB_float_to_obj(RDB_object *valp, RDB_float v);

void
RDB_double_to_obj(RDB_object *valp, RDB_double v);

int
RDB_string_to_obj(RDB_object *valp, const char *str, RDB_exec_context *);

int
RDB_obj_to_string(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *);

int
RDB_obj_comp(const RDB_object *valp, const char *compname,
                   RDB_object *comp, RDB_exec_context *, RDB_transaction *);

int
RDB_obj_set_comp(RDB_object *valp, const char *compname,
                 const RDB_object *comp, RDB_exec_context *,
                 RDB_transaction *);

RDB_bool
RDB_obj_bool(const RDB_object *valp);

RDB_int
RDB_obj_int(const RDB_object *valp);

RDB_float
RDB_obj_float(const RDB_object *valp);

RDB_double
RDB_obj_double(const RDB_object *valp);

char *
RDB_obj_string(const RDB_object *valp);

/*
 * Copy len bytes from srcp into the RDB_object at position pos.
 * The RDB_object must be of type BINARY or newly initialized.
 */
int
RDB_binary_set(RDB_object *, size_t pos, const void *srcp, size_t len,
        RDB_exec_context *);

/*
 * Obtain a pointer to len bytes from the RDB_object at position pos.
 */
int
RDB_binary_get(const RDB_object *, size_t pos, size_t len,
        RDB_exec_context *, void **pp, size_t *alenp);

size_t
RDB_binary_length(const RDB_object *);

void
_RDB_set_obj_type(RDB_object *valp, RDB_type *typ);

/*
 * Functions for creating expressions
 */

RDB_bool
RDB_expr_is_const(const RDB_expression *);

RDB_expression *
RDB_bool_to_expr(RDB_bool, RDB_exec_context *);

RDB_expression *
RDB_int_to_expr(RDB_int, RDB_exec_context *);

RDB_expression *
RDB_float_to_expr(RDB_float, RDB_exec_context *);

RDB_expression *
RDB_double_to_expr(RDB_double, RDB_exec_context *);

RDB_expression *
RDB_string_to_expr(const char *, RDB_exec_context *);

RDB_expression *
RDB_obj_to_expr(const RDB_object *valp, RDB_exec_context *);

RDB_expression *
RDB_table_ref(RDB_object *tbp, RDB_exec_context *);

RDB_expression *
RDB_expr_var(const char *varname, RDB_exec_context *);

RDB_expression *
RDB_eq(RDB_expression *, RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_tuple_attr(RDB_expression *, const char *attrname, RDB_exec_context *);

RDB_expression *
RDB_expr_comp(RDB_expression *, const char *, RDB_exec_context *);

RDB_expression *
RDB_ro_op(const char *opname, int argc, RDB_exec_context *);

void
RDB_add_arg(RDB_expression *exp, RDB_expression *argp);

/* Return address of encapsulated object, or NULL if not a value */
RDB_object *
RDB_expr_obj(RDB_expression *exp);

/*
 * Destroy the expression and all its subexpressions
 */
int
RDB_drop_expr(RDB_expression *, RDB_exec_context *);

int
RDB_create_ro_op(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const void *iargp, size_t iarglen, 
                 RDB_exec_context *, RDB_transaction *txp);

int
RDB_create_update_op(const char *name, int argc, RDB_type *argtv[],
                  RDB_bool upd[], const char *libname, const char *symname,
                  const void *iargp, size_t iarglen,
                  RDB_exec_context *, RDB_transaction *txp);

int
RDB_call_ro_op(const char *name, int argc, RDB_object *argv[],
               RDB_exec_context *, RDB_transaction *txp, RDB_object *retvalp);

int
RDB_call_update_op(const char *name, int argc, RDB_object *argv[],
                RDB_exec_context *, RDB_transaction *txp);

int
RDB_drop_op(const char *name, RDB_exec_context *, RDB_transaction *);

int
RDB_get_dbs(RDB_environment *, RDB_object *, RDB_exec_context *);

int
RDB_create_constraint(const char *name, RDB_expression *,
                      RDB_exec_context *, RDB_transaction *);

int
RDB_drop_constraint(const char *name, RDB_exec_context *,
        RDB_transaction *);

typedef struct {
    RDB_object *tbp;
    RDB_object *tplp;
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
RDB_raise_invalid_tx(RDB_exec_context *);

RDB_object *
RDB_raise_not_found(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_type_mismatch(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_operator_not_found(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_type_constraint_violation(const char *, RDB_exec_context *);

RDB_object *
RDB_raise_element_exists(const char *info, RDB_exec_context *);

RDB_object *
RDB_raise_not_supported(const char *info, RDB_exec_context *);

RDB_object *
RDB_raise_attribute_not_found(const char *info, RDB_exec_context *);

RDB_object *
RDB_raise_predicate_violation(const char *info, RDB_exec_context *);

RDB_object *
RDB_raise_system(const char *info, RDB_exec_context *);

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

int
RDB_ec_set_property(RDB_exec_context *, const char *name, void *);

void *
RDB_ec_get_property(RDB_exec_context *, const char *name);

#endif
