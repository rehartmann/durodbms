#ifndef RDB_RDB_H
#define RDB_RDB_H

/* $Id$ */

/*
This file is part of Duro, a relational database library.
Copyright (C) 2003-2005 René Hartmann.

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

typedef struct {
    char *name;
    int compc;
    struct RDB_attr *compv;
    struct RDB_expression *constraintp;
} RDB_possrep;

enum _RDB_obj_kind {
    RDB_OB_INITIAL,
    RDB_OB_BOOL,
    RDB_OB_INT,
    RDB_OB_RATIONAL,
    RDB_OB_BIN,
    RDB_OB_TABLE,
    RDB_OB_TUPLE,
    RDB_OB_ARRAY
};

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
        RDB_rational rational_val;
        struct {
            void *datap;
            size_t len;
        } bin;
        struct RDB_table *tbp;
        RDB_hashmap tpl_map;
        struct {
            struct RDB_table *tbp;
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

typedef int RDB_compare_func(const RDB_object *, const RDB_object *);

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
    RDB_compare_func *comparep;

    RDB_int ireplen;

    union {
        struct RDB_type *basetyp; /* relation or array type */
        struct {
            int attrc;
            struct RDB_attr *attrv;
        } tuple;
        struct {
            int repc;
            struct RDB_ipossrep *repv;

            /* RDB_TRUE if selector/getters/setters are provided by the system */
            RDB_bool sysimpl; 

            /* Actual representation, if the type is represented by another type.
               Otherwise NULL. */
            struct RDB_type *arep; 
        } scalar;
    } var;
} RDB_type;

/* built-in types */
extern RDB_type RDB_BOOLEAN;
extern RDB_type RDB_INTEGER;
extern RDB_type RDB_RATIONAL;
extern RDB_type RDB_STRING;
extern RDB_type RDB_BINARY;

enum _RDB_expr_kind {
    RDB_EX_OBJ,

    RDB_EX_ATTR,

    RDB_EX_AGGREGATE,
    RDB_EX_TUPLE_ATTR,
    RDB_EX_GET_COMP,
    RDB_EX_RO_OP
};

typedef enum {
    RDB_COUNT, RDB_SUM, RDB_AVG, RDB_MAX, RDB_MIN, RDB_ALL, RDB_ANY,
    RDB_COUNTD, RDB_SUMD, RDB_AVGD
} RDB_aggregate_op;

typedef struct RDB_expression {
    /* internal */
    enum _RDB_expr_kind kind;
    union {
        char *attrname;
        RDB_object obj;
        struct {
            int argc;
            struct RDB_expression **argv;
            char *name;
            RDB_aggregate_op op; /* only for RDB_EX_AGGREGATE */
        } op;
    } var;
} RDB_expression;

typedef struct {
    char *name;
    RDB_expression *exp;
} RDB_virtual_attr;

typedef struct {
    int strc;
    char **strv;
} RDB_string_vec;

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
    RDB_TB_REAL,
    RDB_TB_SELECT,
    RDB_TB_UNION,
    RDB_TB_MINUS,
    RDB_TB_INTERSECT,
    RDB_TB_JOIN,
    RDB_TB_EXTEND,
    RDB_TB_PROJECT,
    RDB_TB_SUMMARIZE,
    RDB_TB_RENAME,
    RDB_TB_WRAP,
    RDB_TB_UNWRAP,
    RDB_TB_SDIVIDE,
    RDB_TB_GROUP,
    RDB_TB_UNGROUP
};

typedef struct {
    int attrc;
    char **attrv;
    char *attrname;
} RDB_wrapping;

typedef struct {
    char *attrname;
    RDB_bool asc;
} RDB_seq_item;

typedef struct RDB_table {
    /* internal */
    RDB_type *typ;
    RDB_bool is_user;
    RDB_bool is_persistent;
    enum _RDB_tb_kind kind;
    char *name;
    int keyc;

    /*
     * Candidate keys. NULL if table is virtual and the keys have not been
     * inferred.
     */
    RDB_string_vec *keyv;

    /*
     * Only used if the table is referred to by RDB_object.var.tbp
     */
    int refcount;

    union {
        struct {
            RDB_recmap *recmapp;
            RDB_hashmap attrmap;   /* Maps attr names to field numbers */

            /* Table indexes */
            int indexc;
            struct _RDB_tbindex *indexv;
            int est_cardinality; /* estimated cardinality (from statistics) */
        } real;
        struct {
            /*
             * If indexp != NULL, tbp must point to a projection, which in
             * turn must point to a real table.
             */
            struct RDB_table *tbp;
            RDB_expression *exp;

            /* Only used if indexp != NULL */
            RDB_object **objpv;
            int objpc;
            RDB_bool asc;
            RDB_bool all_eq;
            RDB_expression *stopexp;
        } select;
        struct {
            struct RDB_table *tb1p;
            struct RDB_table *tb2p;
        } _union;
        struct {
            struct RDB_table *tb1p;
            struct RDB_table *tb2p;
        } minus;
        struct {
            struct RDB_table *tb1p;
            struct RDB_table *tb2p;
        } intersect;
        struct {
            struct RDB_table *tb1p;
            struct RDB_table *tb2p;
        } join;
        struct {
            struct RDB_table *tbp;
            int attrc;
            RDB_virtual_attr *attrv;
        } extend;
        struct {
            struct RDB_table *tbp;
            RDB_bool keyloss;
            struct _RDB_tbindex *indexp;
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
        struct {
            struct RDB_table *tbp;
            int wrapc;
            RDB_wrapping *wrapv;
        } wrap;
        struct {
            struct RDB_table *tbp;
            int attrc;
            char **attrv;            
        } unwrap;
        struct {
            struct RDB_table *tb1p;
            struct RDB_table *tb2p;
            struct RDB_table *tb3p;
        } sdivide;
        struct {
            struct RDB_table *tbp;
            int attrc;
            char **attrv;
            char *gattr;
        } group;
        struct {
            struct RDB_table *tbp;
            char *attr;
        } ungroup;
    } var;
} RDB_table;

typedef struct RDB_database {
    /* internal */

    char *name;
    RDB_hashmap tbmap;
    
    /* pointer to next DB in environment */
    struct RDB_database *nextdbp;

    struct RDB_dbroot *dbrootp;
} RDB_database;

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
#define RDB_db_name(dbp) ((dbp)->name)

RDB_environment *
RDB_db_env(RDB_database *);

/*
 * Create a database with name name in the environment pointed to
 * by envp. Store a pointer to the newly created RDB_database structure
 * in dbpp.
 * Return RDB_OK on success. A return other than RDB_OK indicates an error.
 */
int
RDB_create_db_from_env(const char *name, RDB_environment *envp, RDB_database **dbpp);

/*
 * Get the database with name name in the environment pointed to
 * by envp. Store a pointer to the newly created RDB_database structure
 * in dbpp.
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_get_db_from_env(const char *name, RDB_environment *, RDB_database **dbpp);

/*
 * Drop the database with name dbname.
 * The database must not contain any user tables.
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_drop_db(RDB_database *dbp);

typedef struct RDB_attr {
    char *name;
    RDB_type *typ;
    RDB_object *defaultp;
    int options;
} RDB_attr;

/*
 * Create a table with name name in the database associated with the transaction
 * pointed to by txp. Store a pointer to the newly created RDB_table structure
 * in tbpp.
 * If persistent is RDB_FALSE, create a transient table. In this case,
 * name may be NULL.
 * The attributes are specified by attrc and heading.
 * The candidate keys are specified by keyc and keyv.
 * At least one candidate key must be specified.
 *
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_create_table(const char *name, RDB_bool persistent,
        int attrc, const RDB_attr attrv[],
        int keyc, const RDB_string_vec keyv[],
        RDB_transaction *txp, RDB_table **tbpp);

/*
 * Lookup the table with name name from the database pointed to by dbp.
 * Store a pointer to a RDB_table structure in tbpp.
 * Return RDB_OK on success, RDB_NOT_FOUND if the table could not be found.
 */
int
RDB_get_table(const char *name, RDB_transaction *txp, RDB_table **tbpp);

/*
 * Drop the table pointed to by tbp. If it's a persistent table,
 * delete it.
 * If it's a virtual table, drop all unnamed "subtables".
 * Return RDB_OK on success. A return value other than RDB_OK indicates an error.
 */
int
RDB_drop_table(RDB_table *tbp, RDB_transaction *);

int
RDB_table_keys(RDB_table *, RDB_string_vec **keyvp);

int
RDB_set_table_name(RDB_table *tbp, const char *name, RDB_transaction *);

/*
 * Associate a table with a database.
 * The table must be named.
 */
int
RDB_add_table(RDB_table *, RDB_transaction *);

int
RDB_create_table_index(const char *name, RDB_table *tbp, int idxcompc,
        const RDB_seq_item idxcompv[], int flags, RDB_transaction *);

int
RDB_drop_table_index(const char *name, RDB_transaction *);

int
RDB_define_type(const char *name, int repc, const RDB_possrep repv[],
                RDB_transaction *txp);

/*
 * Lookup the type with name name from the database pointed to by dbp.
 * Store a pointer to a RDB_type structure in typp.
 * Return RDB_OK on success, RDB_NOT_FOUND if the table could not be found.
 */
int
RDB_get_type(const char *name, RDB_transaction *, RDB_type **typp);

/*
 * Return the database assoctiated with the transaction.
 */
#define RDB_tx_db(txp) ((txp)->dbp)

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
    RDB_expression *exp;
} RDB_attr_update;

#define RDB_table_name(tbp) ((tbp)->name)

RDB_type *
RDB_table_type(const RDB_table *);

/*
 * Insert the tuple pointed to by tplp into the table pointed to by tbp.
 * Insertion into virtual relvars is currently only supported for
 * SELECT, UNION, INTERSECT, JOIN, and EXTEND.
 * All attributes of the table for which a default attribute is not provided
 * must be set. Other tuple attributes are ignored.
 *
 * Return value:
 * RDB_OK	if the insertion was successful
 * RDB_ELEMENT_EXISTS	if the table already contained the tuple
 * Other	if an error occured.
 */
int
RDB_insert(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *);

/*
 * Update the tuple which satisfy the condition pointed to by condp.
 * RDB_update() is currently not supported for virtual relvars.
 *
 * If condp is NULL, all tuples will be updated.
 */
int
RDB_update(RDB_table *, RDB_expression *, int attrc,
        const RDB_attr_update updv[], RDB_transaction *);

/*
 * Delete the tuple for which the expression pointedto by condp
 * evaluates to true from the table pointed to by tbp.
 * 
 * If condp is NULL, all tuples will be deleted.
 *
 * Deleting records from virtual relvars is currently only supported for
 * MINUS, UNION, and INTERSECT.
 */
int
RDB_delete(RDB_table *tbp, RDB_expression *condp, RDB_transaction *);

/*
 * Assign table srcp to table dstp.
 */
int
RDB_copy_table(RDB_table *dstp, RDB_table *srcp, RDB_transaction *);

/*
 * !! Needs rewriting
 *
 * Aggregate operation.
 * op may be RDB_COUNT, RDB_SUM, RDB_AVG, RDB_MAX, RDB_MIN, RDB_ALL, or RDB_ANY.
 * attrname may be NULL if op is RDB_COUNT or the table is unary.
 * If op is RDB_COUNT, the result value is of type RDB_INTEGER.
 * If op is RDB_AVG, the result value is of type RDB_RATIONAL.
 * Otherwise, it is of the same type as the table attribute.
 *
 * Errors:
 * RDB_TYPE_MISMATCH
 *       op is RDB_COUNT, RDB_SUM, RDB_AVG, RDB_MAX, or RDB_MIN
 *       and the attribute is non-numeric.
 *
 *       op is ALL or ANY and the attribute is not of type RDB_BOOLEAN.
 *
 *       op is not one of the above.
 *
 * RDB_INVALID_ARGUMENT
 *       op is not of a permissible value (see above).
 *
 *       attrname is NULL while op is not RDB_COUNT and the table is not unary.
 *
 * Other values
 *       A database error occured.
 */

int
RDB_max(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp);

int
RDB_min(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp);

int
RDB_all(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_bool *resultp);

int
RDB_any(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_bool *resultp);

int
RDB_sum(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp);

int
RDB_avg(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_rational *resultp);

/*
 * Check if the table pointed to by tbp contains the tuple
 * pointed to by tplp.
 *
 * Return value:
 * RDB_OK	the table contains the tuple
 * RDB_NOT_FOUND the table does not cointain the tuple
 * Other	a system error occured.
 */
int
RDB_table_contains(RDB_table *, const RDB_object *, RDB_transaction *);

int
RDB_table_equals(RDB_table *, RDB_table *, RDB_transaction *, RDB_bool *);

int
RDB_subset(RDB_table *tb1p, RDB_table *tb2p, RDB_transaction *txp,
              RDB_bool *resultp);

/*
 * Extract a tuple from the table. The table must contain
 * exactly one tuple.
 *
 * Return value:
 * RDB_OK	the tuple has been successfully extracted.
 * RDB_NOT_FOUND the table is empty.
 * RDB_INVALID_ARGUMENT the table contains more than one tuple.
 */
int
RDB_extract_tuple(RDB_table *, RDB_transaction *, RDB_object *);

/*
 * Store RDB_TRUE in the location pointed to by resultp if the table
 * is nonempty, RDB_FALSE otherwise.
 */
int
RDB_table_is_empty(RDB_table *, RDB_transaction *, RDB_bool *resultp);

int
RDB_cardinality(RDB_table *tbp, RDB_transaction *txp);

/*
 * The following functions create virtual relvars.
 * The created relvar will have no name and be transient.
 */
 
/*
 * Create a selection table. The resulting table will take
 * reponsibility for the expression.
 */
int
RDB_select(RDB_table *, RDB_expression *, RDB_transaction *,
        RDB_table **resultpp);

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
RDB_extend(RDB_table *, int attrc, const RDB_virtual_attr attrv[],
        RDB_transaction *, RDB_table **resultpp);

int
RDB_project(RDB_table *, int attrc, char *attrv[], RDB_table **resultpp);

int
RDB_remove(RDB_table *, int attrc, char *attrv[], RDB_table **resultpp);

/* Create a table which is the result of a SUMMARIZE PER operation.
 * The table created takes resposibility for the RDB_expressions
 *  passed through addv.
 */
int
RDB_summarize(RDB_table *, RDB_table *, int addc,
        const RDB_summarize_add addv[], RDB_transaction *,
        RDB_table **resultpp);

int
RDB_rename(RDB_table *tbp, int renc, const RDB_renaming renv[],
           RDB_table **resultpp);

int
RDB_wrap(RDB_table *tbp, int wrapc, const RDB_wrapping wrapv[],
         RDB_table **resultpp);

int
RDB_unwrap(RDB_table *tbp, int attrc, char *attrv[], RDB_table **resultpp);

int
RDB_sdivide(RDB_table *, RDB_table *, RDB_table *, RDB_table **resultpp);

int
RDB_group(RDB_table *, int attrc, char *attrv[], const char *gattr,
        RDB_table **);

int
RDB_ungroup(RDB_table *, const char *, RDB_table **);

RDB_bool
RDB_table_refers(RDB_table *tbp, RDB_table *rtbp);

/*
 * Functions for creation/destruction of tuples and reading/modifying attributes.
 * RDB_object represents a tuple variable.
 */

int
RDB_tuple_set(RDB_object *, const char *name, const RDB_object *);

int
RDB_tuple_set_bool(RDB_object *, const char *name, RDB_bool val);

int
RDB_tuple_set_int(RDB_object *, const char *name, RDB_int val);

int
RDB_tuple_set_rational(RDB_object *, const char *name, RDB_rational val);

int
RDB_tuple_set_string(RDB_object *, const char *name, const char *valp);

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

RDB_rational
RDB_tuple_get_rational(const RDB_object *, const char *name);

RDB_int
RDB_tuple_size(const RDB_object *);

void
RDB_tuple_attr_names(const RDB_object *, char **namev);

char *
RDB_tuple_get_string(const RDB_object *, const char *name);

int
RDB_copy_tuple(RDB_object *dstp, const RDB_object *srcp);

int
RDB_project_tuple(const RDB_object *, int attrc, char *attrv[],
                 RDB_object *restplp);

int
RDB_remove_tuple(const RDB_object *, int attrc, char *attrv[],
                 RDB_object *restplp);

int
RDB_extend_tuple(RDB_object *, int attrc, const RDB_virtual_attr attrv[],
                 RDB_transaction *);

int
RDB_join_tuples(const RDB_object *, const RDB_object *, RDB_transaction *,
        RDB_object *);

int
RDB_rename_tuple(const RDB_object *, int renc, const RDB_renaming renv[],
                 RDB_object *restup);

int
RDB_wrap_tuple(const RDB_object *tplp, int wrapc, const RDB_wrapping wrapv[],
               RDB_object *restplp);

int
RDB_unwrap_tuple(const RDB_object *tplp, int attrc, char *attrv[],
        RDB_object *restplp);

/*
 * Convert a RDB_table to a RDB_object and store the array in
 * the location pointed to by arrpp.
 * Specifying the order of the elements by seqitc and seqitv
 * is currently not supported - the only permissible
 * value of seqitc is 0.
 * The array becomes invalid when the transaction ends.
 */
int
RDB_table_to_array(RDB_object *arrp, RDB_table *, 
                   int seqitc, const RDB_seq_item seqitv[],
                   RDB_transaction *);

/*
 * Get a pointer to the tuple with index idx.
 */
int
RDB_array_get(RDB_object *, RDB_int idx, RDB_object **);

int
RDB_array_set(RDB_object *, RDB_int idx, const RDB_object *tplp);

/*
 * Return the length of the array.
 * A return value < 0 indicates an error.
 */
RDB_int
RDB_array_length(RDB_object *);

int
RDB_set_array_length(RDB_object *arrp, RDB_int len);

RDB_bool
RDB_type_is_numeric(const RDB_type *);

/*
 * Create an anonymous tuple type from the attributes given by attrv.
 */
int
RDB_create_tuple_type(int attrc, const RDB_attr attrv[], RDB_type **typp);

/*
 * Create an anonymous relation type from the attributes given by attrv.
 */
int
RDB_create_relation_type(int attrc, const RDB_attr attrv[], RDB_type **typp);

RDB_type *
RDB_create_array_type(RDB_type *);

/*
 * Drop a type.
 * The transaction argument may be NULL if the type has no name.
 */
int
RDB_drop_type(RDB_type *, RDB_transaction *);

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
RDB_type_is_builtin(const RDB_type *);

/*
 * Return RDB_TRUE if the type pointed to by typ is a scalar type,
 * or RDB_FALSE if it's a user type.
 */
RDB_bool
RDB_type_is_scalar(const RDB_type *);

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
 * Return RDB_TRUE if the two types are equal
 * or RDB_FALSE if they are not.
 */
int
RDB_obj_equals(const RDB_object *, const RDB_object *, RDB_transaction *,
        RDB_bool *);

/*
 * Initialize the value pointed to by dstvalp with the value
 * pointed to by scrvalp.
 * Return RDB_OK on success, RDB_NO_MEMORY if allocating memory failed.
 */
int
RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp);

/*
 * This function must be called before copying a value into a RDB_obj.
 */
void
RDB_init_obj(RDB_object *valp);

/*
 * Release the resources associated with the value pointed to by valp.
 */
int
RDB_destroy_obj(RDB_object *valp);

void
RDB_bool_to_obj(RDB_object *valp, RDB_bool v);

void
RDB_int_to_obj(RDB_object *valp, RDB_int v);

void
RDB_rational_to_obj(RDB_object *valp, RDB_rational v);

int
RDB_string_to_obj(RDB_object *valp, const char *str);

int
RDB_obj_to_string(RDB_object *dstp, const RDB_object *srcp);

int
RDB_obj_comp(const RDB_object *valp, const char *compname,
                   RDB_object *comp, RDB_transaction *);

void
RDB_table_to_obj(RDB_object *valp, RDB_table *tbp);

int
RDB_obj_set_comp(RDB_object *valp, const char *compname,
                   const RDB_object *comp, RDB_transaction *);

RDB_table *
RDB_obj_table(const RDB_object *objp);

RDB_bool
RDB_obj_bool(const RDB_object *valp);

RDB_int
RDB_obj_int(const RDB_object *valp);

RDB_rational
RDB_obj_rational(const RDB_object *valp);

char *
RDB_obj_string(const RDB_object *valp);

/*
 * Copy len bytes from srcp into the RDB_object at position pos.
 * The RDB_object must be of type BINARY or newly initialized.
 */
int
RDB_binary_set(RDB_object *, size_t pos, const void *srcp, size_t len);

/*
 * Obtain a pointer to len bytes from the RDB_object at position pos.
 */
int
RDB_binary_get(const RDB_object *, size_t pos, void **pp, size_t len,
        size_t *alenp);

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
RDB_bool_to_expr(RDB_bool);

RDB_expression *
RDB_int_to_expr(RDB_int);

RDB_expression *
RDB_rational_to_expr(RDB_rational);

RDB_expression *
RDB_string_to_expr(const char *);

RDB_expression *
RDB_obj_to_expr(const RDB_object *valp);

RDB_expression *
RDB_expr_attr(const char *attrname);

RDB_expression *
RDB_eq(RDB_expression *, RDB_expression *);

/*
 * Create table-valued expression
 */
RDB_expression *
RDB_table_to_expr(RDB_table *);

RDB_expression *
RDB_expr_sum(RDB_expression *, const char *attrname);

RDB_expression *
RDB_expr_avg(RDB_expression *, const char *attrname);

RDB_expression *
RDB_expr_max(RDB_expression *, const char *attrname);

RDB_expression *
RDB_expr_min(RDB_expression *, const char *attrname);

RDB_expression *
RDB_expr_all(RDB_expression *, const char *attrname);

RDB_expression *
RDB_expr_any(RDB_expression *, const char *attrname);

RDB_expression *
RDB_tuple_attr(RDB_expression *, const char *attrname);

RDB_expression *
RDB_expr_comp(RDB_expression *, const char *);

RDB_expression *
RDB_ro_op(const char *opname, int argc, RDB_expression *argv[]);

RDB_expression *
RDB_ro_op_va(const char *opname, RDB_expression *arg, ...
        /* (RDB_expression *) NULL */ );

/* Return address of encapsulated object, or NULL if not a value */
RDB_object *
RDB_expr_obj(RDB_expression *exp);

/*
 * Destroy the expression and all its subexpressions
 */
void
RDB_drop_expr(RDB_expression *);

int
RDB_create_ro_op(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const void *iargp, size_t iarglen, 
                 RDB_transaction *txp);

int
RDB_create_update_op(const char *name, int argc, RDB_type *argtv[],
                  RDB_bool upd[], const char *libname, const char *symname,
                  const void *iargp, size_t iarglen,
                  RDB_transaction *txp);

int
RDB_call_ro_op(const char *name, int argc, RDB_object *argv[],
               RDB_transaction *txp, RDB_object *retvalp);

int
RDB_call_update_op(const char *name, int argc, RDB_object *argv[],
                RDB_transaction *txp);

int
RDB_drop_op(const char *name, RDB_transaction *txp);

int
RDB_get_dbs(RDB_environment *, RDB_object *);

int
RDB_create_constraint(const char *name, RDB_expression *exp,
                      RDB_transaction *);

int
RDB_drop_constraint(const char *name, RDB_transaction *);

/* Extract the "-e <environment> -d <database>" arguments from the command line */
int
RDB_getargs(int *argcp, char **argvp[], RDB_environment **envpp, RDB_database **dbpp);

#endif
