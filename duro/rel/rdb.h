#ifndef RDB_RDB_H
#define RDB_RDB_H

/* $Id$ */

/*
This file is part of Duro, a relational database library.
Copyright (C) 2003 René Hartmann.

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

#include <gen/errors.h>
#include <rec/env.h>
#include <rec/cursor.h>
#include <rec/recmap.h>
#include <rec/index.h>
#include <gen/hashmap.h>
#include <gen/types.h>
#include <stdlib.h>

/* internal */
enum RDB_tp_kind {
    RDB_TP_BOOLEAN,
    RDB_TP_INTEGER,
    RDB_TP_RATIONAL,
    RDB_TP_STRING,
    RDB_TP_BINARY,
    RDB_TP_USER,
    RDB_TP_TUPLE,
    RDB_TP_RELATION
};

typedef struct RDB_type {
    /* internal */
    char *name;
    enum RDB_tp_kind kind;
    union {
        struct RDB_type *basetyp;
        struct {
            struct RDB_attr *attrv;
            int attrc;
        } tuple;
    } complex;
} RDB_type;

/* built-in types */
extern RDB_type RDB_BOOLEAN;
extern RDB_type RDB_INTEGER;
extern RDB_type RDB_RATIONAL;
extern RDB_type RDB_STRING;
extern RDB_type RDB_BINARY;

/* A RDB_value struct carries a value of an arbitrary type,
 * together with the type information.
 * (Note that a variable of type RDB_value represents a
 * variable, not a value)
 */
typedef struct {
    /* internal */
    RDB_type *typ;
    union {
        RDB_bool bool_val;
        RDB_int int_val;
        RDB_rational rational_val;
        struct {
            void *datap;
            size_t len;
        } bin;
        struct RDB_table *tbp;
        struct RDB_tuple *tplp;
     } var;
} RDB_value;

enum _RDB_expr_kind {
    RDB_CONST,

    RDB_ATTR,

    RDB_TABLE,

    RDB_OP_EQ,
    RDB_OP_NEQ,
    RDB_OP_GT,
    RDB_OP_LT,
    RDB_OP_GET,
    RDB_OP_LET,
    RDB_OP_AND,
    RDB_OP_OR,
    RDB_OP_NOT,
    RDB_OP_ADD,

    RDB_OP_REL_IS_EMPTY
};

typedef struct RDB_expression {
    /* internal */
    enum _RDB_expr_kind kind;
    union {
        struct {
            struct RDB_expression *arg1;
            struct RDB_expression *arg2;
        } op;
        struct {
            char *name;
            RDB_type *typ;
        } attr;
        struct RDB_table *tbp;
        RDB_value const_val;
    } var;
} RDB_expression;

typedef struct RDB_virtual_attr {
    char *name;
    RDB_expression *exp;
} RDB_virtual_attr;

typedef struct {
    char **attrv;
    int attrc;
} RDB_key_attrs;

typedef enum {
    RDB_COUNT, RDB_SUM, RDB_AVG, RDB_MAX, RDB_MIN, RDB_ALL, RDB_ANY,
    RDB_COUNTD, RDB_SUMD, RDB_AVGD
} RDB_aggregate_op;

typedef struct {
    RDB_aggregate_op op;
    RDB_expression *exp;
    char *name;
} RDB_summarize_add;

typedef struct {
    char *from;
    char *to;
} RDB_renaming;

/* internal */
enum _RDB_tb_kind {
    RDB_TB_STORED,
    RDB_TB_SELECT,
    RDB_TB_SELECT_PINDEX,
    RDB_TB_UNION,
    RDB_TB_MINUS,
    RDB_TB_INTERSECT,
    RDB_TB_JOIN,
    RDB_TB_EXTEND,
    RDB_TB_PROJECT,
    RDB_TB_SUMMARIZE,
    RDB_TB_RENAME
};

typedef struct RDB_table {
    /* internal */
    RDB_type *typ;
    RDB_bool is_user;
    RDB_bool is_persistent;
    enum _RDB_tb_kind kind;
    char *name;
    int keyc;
    RDB_key_attrs *keyv;
    union {
        struct {
            RDB_recmap *recmapp;
            RDB_hashmap attrmap;   /* Maps attr names to field numbers */
            RDB_index **keyidxv;   /* Secondary key indexes */
        } stored;
        struct {
            struct RDB_table *tbp;
            RDB_expression *exprp;
            /* for RDB_TB_SELECT_PINDEX */
            RDB_value val;
        } select;
        struct {
            struct RDB_table *tbp;
        } select_index;
        struct {
            struct RDB_table *tbp1;
            struct RDB_table *tbp2;
        } _union;
        struct {
            struct RDB_table *tbp1;
            struct RDB_table *tbp2;
        } minus;
        struct {
            struct RDB_table *tbp1;
            struct RDB_table *tbp2;
        } intersect;
        struct {
            char **common_attrv;
            int common_attrc;
            struct RDB_table *tbp1;
            struct RDB_table *tbp2;
        } join;
        struct {
            struct RDB_table *tbp;
            int attrc;
            RDB_virtual_attr *attrv;
        } extend;
        struct {
            struct RDB_table *tbp;
            RDB_bool keyloss;
        } project;
        struct {
            struct RDB_table *tb1p;
            struct RDB_table *tb2p;
            int addc;
            RDB_summarize_add *addv;
        } summarize;
        struct {
            struct RDB_table *tbp;
            int renc;
            RDB_renaming *renv;
        } rename;
    } var;
    int refcount;
} RDB_table;

typedef struct RDB_database {
    /* internal */

    char *name;
    RDB_environment *envp;
    RDB_hashmap tbmap;
    
    /* catalog tables */
    RDB_table *table_attr_tbp;
    RDB_table *rtables_tbp;
    RDB_table *vtables_tbp;
    RDB_table *dbtables_tbp;
    RDB_table *keys_tbp;
    
    /* pointer to next DB in environment */
    struct RDB_database *nextdbp;
    
    /* reference count */
    int refcount;
} RDB_database;

typedef struct RDB_transaction {
    /* internal */
    RDB_database *dbp;
    DB_TXN *txid;
    struct RDB_transaction *parentp;
    struct RDB_qresult *first_qrp;
} RDB_transaction;

/*
 * The RDB_tuple structure represents a tuple.
 * It does not carry type information for the tuple itself,
 * but the type information can be extracted from the values
 * the tuple contains.
 */
typedef struct {
    /* internal */
    RDB_hashmap map;
} RDB_tuple;

typedef struct {
    /* internal */
    RDB_table *tbp;
    RDB_transaction *txp;
    struct RDB_qresult *qrp;
    RDB_int pos;
    int length;	/* length of array; -1 means unknown */
} RDB_array;

typedef struct RDB_attr {
    char *name;
    RDB_type *type;
    RDB_value *defaultp;
    int options;
} RDB_attr;

/*
 * Return the name of the database.
 */
#define RDB_db_name(dbp) ((dbp)->name)

/*
 * Create a database with name name in the environment pointed to
 * by envp. Store a pointer to the newly created RDB_database structure
 * in dbpp.
 * Return RDB_OK on success. A return other than RDB_OK indicates an error.
 */
int
RDB_create_db(const char *name, RDB_environment *envp, RDB_database **dbpp);

/*
 * Get the database with name name in the environment pointed to
 * by envp. Store a pointer to the newly created RDB_database structure
 * in dbpp.
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_get_db(const char *name, RDB_environment *, RDB_database **dbpp);

/*
 * Release the RDB_database structure pointed to by dbp.
 * Return RDB_OK on success. A return other than RDB_OK indicates an error.
 */
int
RDB_release_db(RDB_database *dbp);

/*
 * Drop the database with name dbname.
 * The database must not contain any user tables.
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_drop_db(RDB_database *dbp);

/*
 * Create a table with name name in the database associated with the transaction
 * pointed to by txp. Store a pointer to the newly created RDB_table structure
 * in tbpp.
 * If persistent is DB_FALSE, create a transient table. In this case,
 * name may be NULL.
 * The attributes are specified by attrc and heading.
 * The candidate keys are specified by keyc and keyv.
 * At least one candidate key must be specified.
 *
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_create_table(const char *name, RDB_bool persistent,
        int attrc, RDB_attr heading[],
        int keyc, RDB_key_attrs keyv[],
        RDB_transaction *txp, RDB_table **tbpp);

/*
 * Lookup the table with name name from the database pointed to by dbp.
 * Store a pointer to a RDB_table structure in tbpp.
 * Return RDB_OK on success, RDB_NOT_FOUND if the table could not be found.
 */
int
RDB_get_table(RDB_database *dbp, const char *name, RDB_table **tbpp);

/*
 * Drop the table pointed to by tbp. If it's a persistent table,
 * delete it.
 * If it's a virtual table, drop all unnamed "subtables".
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_drop_table(RDB_table *tbp, RDB_transaction *);

int
RDB_set_table_name(RDB_table *tbp, const char *name, RDB_transaction *);

/*
 * Make the table persistent. If it is already persistent, do nothing.
 * The table must be named.
 */
int
RDB_make_persistent(RDB_table *, RDB_transaction *);

/*
 * Lookup the type with name name from the database pointed to by dbp.
 * Store a pointer to a RDB_type structure in typp.
 * Return RDB_OK on success, RDB_NOT_FOUND if the table could not be found.
 */
int
RDB_get_type(RDB_database *dbp, const char *name, RDB_type **typp);

/*
 * Return the database assoctiated with the transaction.
 */
#define RDB_tx_db(txp) ((txp)->dbp)

/*
 * Start the transaction pointed to by txp.
 * The transaction will be associated to the database pointed to by dbp.
 * If parentp is not NULL, the transaction becomes a subtransaction
 * of the transaction pointed to by parentp.
 * 
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_begin_tx(RDB_transaction *txp, RDB_database *dbp,
        RDB_transaction *parent);

/*
 * Commit the transaction pointed to by txp.
 *
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 * If the transaction has already been committed or aborted,
 * DB_INVALID_TRANSACTION is returned.
 */
int
RDB_commit(RDB_transaction *);

/*
 * Abort the transaction pointed to by txp.
 *
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 * If the transaction is already committed or aborted, DB_INVALID_TRANSACTION
 * is returned.
 * Note that a transaction may be aborted implicity by a database operation.
 * (see TTM, OO prescription 4)
 */
int
RDB_rollback(RDB_transaction *);

typedef struct {
    char *name;
    RDB_expression *valuep; /* !! */
} RDB_attr_update;

#define RDB_table_name(tbp) ((tbp)->name)

/*
 * Insert the tuple pointed to by tplp into the table pointed to by tbp.
 * Insertion into virtual relvars is currently only supported for
 * SELECT, UNION, INTERSECT, JOIN, and EXTEND.
 *
 * Return value:
 * RDB_OK	if the insertion was successful
 * RDB_ELEMENT_EXISTS	if the table already contained the tuple
 * Other	if an error occured.
 */
int
RDB_insert(RDB_table *tbp, const RDB_tuple *tplp, RDB_transaction *);

/*
 * Update the tuple which satisfy the condition pointed to by condp.
 * RDB_update() is currently not supported for virtual relvars.
 *
 * If condp is NULL, all tuples will be updated.
 */
int
RDB_update(RDB_table *, RDB_expression *, int attrc,
        const RDB_attr_update attrv[], RDB_transaction *);

/*
 * Delete the tuple for which the expression pointedto by exprp
 * evaluates to true from the table pointed to by tbp.
 * 
 * If condp is NULL, all tuples will be deleted.
 *
 * Deleting records from virtual relvars is currently only supported for
 * MINUS, UNION, and INTERSECT.
 */
int
RDB_delete(RDB_table *tbp, RDB_expression *exprp, RDB_transaction *);

/*
 * Assign table srcp to table dstp.
 */
int
RDB_copy_table(RDB_table *dstp, RDB_table *srcp, RDB_transaction *);

/*
 * Aggregate operation.
 * op may be RDB_COUNT, RDB_SUM, RDB_AVG, RDB_MAX, RDB_MIN, RDB_ALL, or RDB_ANY.
 * attrname may be NULL if op is RDB_COUNT or the table is unary.
 * If op is RDB_COUNT, the result value is of type RDB_INTEGER.
 * If op is AVG, the result value is of type RDB_RATIONAL.
 * Otherwise, it is of the same type as the table attribute.
 *
 * Errors:
 * RDB_TYPE_MISMATCH
 *       op is RDB_COUNT, RDB_SUM, RDB_AVG, RDB_MAX, or RDB_MIN
 *       and the attribute is non-numeric.
 *
 *       op IS ALL or ANY and the attribute is not of type RDB_BOOLEAN.
 *
 *       op is not one of the above.
 *
 * RDB_ILLEGAL_ARG
 *       op is not of a permissible value (see above).
 *
 *       attrname is NULL while op is not RDB_COUNT and the table is not unary.
 *
 * Other values
 *       A database error occured.
 */
int
RDB_aggregate(RDB_table *, RDB_aggregate_op op, const char *attrname,
              RDB_transaction *, RDB_value *resultp);

/*
 * Check if the table pointed to by tbp contains the tuple
 * pointed to by tplp.
 *
 * Return value:
 * RDB_OK	if the table contains the tuple
 * RDB_NOT_FOUND	if the table does not cointain the tuple
 * Other	if an error occured.
 */
int
RDB_table_contains(RDB_table *, const RDB_tuple *, RDB_transaction *);

/*
 * Store RDB_TRUE in the location pointed to by resultp if the table
 * is nonempty, RDB_FALSE otherwise.
 */
int
RDB_table_is_empty(RDB_table *, RDB_transaction *, RDB_bool *resultp);

/*
 * The following functions create virtual relvars.
 * The created relvar will have no name and be transient.
 */
 
/* Create a selection table. The resulting table will take
 * reponsibility for the condition.
 */
int
RDB_select(RDB_table *, RDB_expression *condition, RDB_table **resultpp);

int
RDB_union(RDB_table *, RDB_table *, RDB_table **resultpp);

int
RDB_minus(RDB_table *, RDB_table *, RDB_table **resultpp);

int
RDB_intersect(RDB_table *, RDB_table *, RDB_table **resultpp);

/* Create a table which is a natural join of the two tables. */
int
RDB_join(RDB_table *, RDB_table *, RDB_table **resultpp);

/* Create a table which is the result of a EXTEND operation.
 * The table created takes resposibility for the RDB_expressions
 *  passed through attrv.
 */
int
RDB_extend(RDB_table *, int attrc, RDB_virtual_attr attrv[],
        RDB_table **resultpp);

int
RDB_project(RDB_table *, int attrc, char *attrv[], RDB_table **resultpp);

/* Create a table which is the result of a SUMMARIZE ADD operation.
 * The table created takes resposibility for the RDB_expressions
 *  passed through addvv.
 */
int
RDB_summarize(RDB_table *, RDB_table *, int addc, RDB_summarize_add addv[],
              RDB_table **resultpp);

int
RDB_rename(RDB_table *tbp, int renc, RDB_renaming renv[],
           RDB_table **resultpp);

/*
 * Functions for creation/destruction of tuples and reading/modifying attributes.
 * RDB_tuple represents a tuple variable.
 */

void
RDB_init_tuple(RDB_tuple *);

void
RDB_destroy_tuple(RDB_tuple *);

int
RDB_tuple_set(RDB_tuple *, const char *name, const RDB_value *);

int
RDB_tuple_set_bool(RDB_tuple *, const char *name, RDB_bool val);

int
RDB_tuple_set_int(RDB_tuple *, const char *name, RDB_int val);

int
RDB_tuple_set_rational(RDB_tuple *, const char *name, RDB_rational val);

int
RDB_tuple_set_string(RDB_tuple *, const char *name, const char *valp);

/**
 * Return a pointer to the tuple's value corresponding to name name.
 * The pointer returned will become invalid if RDB_destroy_tuple()
 * is called if the attribute is overwritten
 * by calling RDB_set_tuple_attr() with the same name argument.
 * If an attribute of name name does not exist, NULL is returned.
 */
RDB_value *
RDB_tuple_get(const RDB_tuple *, const char *name);

RDB_bool
RDB_tuple_get_bool(const RDB_tuple *, const char *name);

RDB_int
RDB_tuple_get_int(const RDB_tuple *, const char *name);

RDB_rational
RDB_tuple_get_rational(const RDB_tuple *, const char *name);

char *
RDB_tuple_get_string(const RDB_tuple *, const char *name);

int
RDB_tuple_extend(RDB_tuple *, int attrc, RDB_virtual_attr attrv[],
                 RDB_transaction *);

int
RDB_tuple_rename(const RDB_tuple *, int renc, RDB_renaming renv[],
                 RDB_tuple *restup);

void
RDB_init_array(RDB_array *);

void
RDB_destroy_array(RDB_array *);

typedef struct {
    char *attrname;
    RDB_bool asc;
} RDB_seq_item;

/*
 * Convert a RDB_table to a RDB_array and store the array in
 * the location pointed to by arrpp.
 * Specifying the order of the elements by seqitc and seqitv
 * is currently not supported - the only permissible
 * value of seqitc is 0.
 * The array becomes invalid when the transaction ends.
 */
int
RDB_table_to_array(RDB_table *, RDB_array *arrp,
                   int seqitc, RDB_seq_item seqitv[],
                   RDB_transaction *);

/*
 * Read the tuple with index idx from the array.
 */
int
RDB_array_get_tuple(RDB_array *, RDB_int idx, RDB_tuple *);

/*
 * Return the length of the array.
 * A return value < 0 indicates an error.
 */
int
RDB_array_length(RDB_array *);

RDB_bool
RDB_type_is_numeric(const RDB_type *);

/*
 * Create an anonymous tuple type from the attributes given by attrv.
 */
RDB_type *
RDB_create_tuple_type(int attrc, RDB_attr attrv[]);

/*
 * Create an anonymous relation type from the attributes given by attrv.
 */
RDB_type *
RDB_create_relation_type(int attrc, RDB_attr attrv[]);

int
RDB_drop_type(RDB_type *);

/*
 * Return the name of the type pointed to by typ.
 * If it's an anonymous type, NULL is returned.
 */
char *
RDB_type_name(const RDB_type *);

/*
 * Return RDB_TRUE if the type pointed to by typ is a bultin type,
 * or RDB_FALSE if it's a user type.
 */
RDB_bool
RDB_is_builtin_type(const RDB_type *);

/*
 * Return RDB_TRUE if the two types are equal
 * or RDB_FALSE if they are not .
 * Two types are equal if there definition is equal.
 */
RDB_bool
RDB_type_equals(const RDB_type *, const RDB_type *);

/*
 * Extend the tuple type pointed to by typ by the attributes given by
 * attrv and return the new tuple type.
 */
RDB_type *
RDB_extend_tuple_type(const RDB_type *typ, int attrc, RDB_attr attrv[]);

/*
 * Extend the relation type pointed to by typ by the attributes given by
 * attrv and return the new relation type.
 */
RDB_type *
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[]);

/*
 * Join the tuple types pointed to by typ1 and typ2 and store a pointer to
 * The new type in the location pointed to by newtypp.
 * The new type has the attributes from both types.
 * If both types have an attribute with the same name but a different type,
 * RDB_TYPE_MISMATCH is returned.
 */
int
RDB_join_tuple_types(const RDB_type *typ1, const RDB_type *typ2,
                     RDB_type **newtypp);

/*
 * Join the relation types pointed to by typ1 and typ2 and store a pointer to
 * The new type in the location pointed to by newtypp.
 * The new type has the attributes from both types.
 * If both types have an attribute with the same name but a different type,
 * RDB_TYPE_MISMATCH is returned.
 */
int
RDB_join_relation_types(const RDB_type *typ1, const RDB_type *typ2,
                     RDB_type **newtypp);

/*
 * Create a type that is a projection of the relation type pointed to by typ
 * over the attributes given by attrc and attrv.
 * The new type in the location pointed to by newtypp.
 * If one of the attributes in attrv is not found in the relation type,
 * RDB_ILLEGAL_ARG is returned.
 */
int
RDB_project_relation_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_type **newtypp);

/*
 * Rename the attributes of the tuple type pointed to by typ according to renc
 * and renv return the new tuple type.
 */
int
RDB_rename_tuple_type(const RDB_type *typ, int renc, RDB_renaming renv[],
        RDB_type **);

/*
 * Rename the attributes of the relation type pointed to by typ according to renc
 * and renv return the new tuple type.
 */
int
RDB_rename_relation_type(const RDB_type *typ, int renc, RDB_renaming renv[],
        RDB_type **);

/*
 * Return RDB_TRUE if the two types are equal
 * or RDB_FALSE if they are not.
 */
RDB_bool
RDB_value_equals(const RDB_value *, const RDB_value *);

/*
 * Initialize the value pointed to by dstvalp with the value
 * pointed to by scrvalp.
 * Return RDB_OK on success, RDB_NO_MEMORY if allocating memory failed.
 */
int
RDB_copy_value(RDB_value *dstvalp, const RDB_value *srcvalp);

/*
 * This function must be called before copying a value into a RDB_value.
 */
void
RDB_init_value(RDB_value *valp);

/*
 * Release the resources associated with the value pointed to by valp.
 */
void
RDB_destroy_value(RDB_value *valp);

void
RDB_value_set_int(RDB_value *valp, RDB_int v);

void
RDB_value_set_rational(RDB_value *valp, RDB_rational v);

int
RDB_value_set_string(RDB_value *valp, const char *str);

RDB_int
RDB_value_int(const RDB_value *valp);

RDB_rational
RDB_value_rational(const RDB_value *valp);

char *
RDB_value_string(RDB_value *valp);

/*
 * Copy len bytes from srcp into the RDB_value at position pos.
 * The RDB_value must be of type BINARY or newly initialized.
 */
int
RDB_binary_set(RDB_value *, size_t pos, void *srcp, size_t len);

/*
 * Copy len bytes from the RDB_value at position pos to dstp.
 */
int
RDB_binary_get(const RDB_value *, size_t pos, void *dstp, size_t len);

size_t
RDB_binary_get_length(const RDB_value *);

/*
 * Return the type of a RDB_expression.
 * Return NULL if type informaton is not available.
 */
RDB_type *
RDB_expr_type(const RDB_expression *);

/*
 * Functions for creating expressions
 */

RDB_bool
RDB_expr_is_const(const RDB_expression *);

RDB_expression *
RDB_bool_const(RDB_bool);

RDB_expression *
RDB_int_const(RDB_int);

RDB_expression *
RDB_rational_const(RDB_rational);

RDB_expression *
RDB_string_const(const char *);

RDB_expression *
RDB_value_const(const RDB_value *valp);

RDB_expression *
RDB_expr_attr(const char *attrname, RDB_type *);

/* Create a copy of the expression pointed to by exprp.
 * Works only for scalar expressions.
 */
RDB_expression *
RDB_dup_expr(const RDB_expression *exprp);

RDB_expression *
RDB_eq(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_neq(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_lt(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_gt(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_let(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_get(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_and(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_or(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_not(RDB_expression *);

RDB_expression *
RDB_add(RDB_expression *, RDB_expression *);

/*
 * Create table-valued expression
 */
RDB_expression *
RDB_rel_table(RDB_table *);

RDB_expression *
RDB_rel_is_empty(RDB_expression *);

/* Destroy the expression and all its subexpressions */
void
RDB_drop_expr(RDB_expression *);

/* Extract the "-e <environment> -d <database>" arguments from the command line */
int
RDB_getargs(int *argcp, char **argvp[], RDB_environment **envpp, RDB_database **dbpp);

#endif
