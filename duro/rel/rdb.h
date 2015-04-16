#ifndef RDB_RDB_H
#define RDB_RDB_H

/*
Main DuroDBMS header file.

This file is part of DuroDBMS, a relational database management system.
Copyright (C) 2003-2009, 2011-2015 Rene Hartmann.

DuroDBMS is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

DuroDBMS is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with DuroDBMS; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#include <rec/env.h>
#include <rec/dbdefs.h>
#include <gen/hashmap.h>
#include <gen/hashtable.h>
#include <gen/types.h>
#include <obj/type.h>
#include <obj/object.h>
#include <obj/excontext.h>
#include <obj/expression.h>
#include <obj/builtintypes.h>
#include <obj/tuple.h>
#include <obj/operator.h>

#include <stdlib.h>

#define RDB_THE_PREFIX "the_"

typedef struct RDB_expression RDB_expression;

/**@addtogroup array
 * @{
 */

/**
 * This struct is used to specify an attribute and a direction
 * for tuple ordering.
 */
typedef struct {
    /** Attribute name. */
    char *attrname;
    /** RDB_TRUE if order is ascending, RDB_FALSE if order is descending. */
    RDB_bool asc;
} RDB_seq_item;

/**
 * @}
 */

/**@addtogroup table
 * @{
 */

/**
 * @}
 */

typedef struct RDB_qresult RDB_qresult;

/* Function definition for reading lines of input */
typedef char *RDB_read_line_fn(void);
typedef void RDB_free_line_fn(char *);

#ifndef DOXYGEN_SHOULD_SKIP_THIS

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

#endif

typedef int RDB_apply_constraint_fn(RDB_expression *, const char *,
        RDB_exec_context *, struct RDB_transaction *);

/** @addtogroup tuple
 * @{
 */

/** @struct RDB_virtual_attr rdb.h <rel/rdb.h>
 * Represents a virtual attribute, used for EXTEND.
 */
typedef struct {
    char *name;
    RDB_expression *exp;
} RDB_virtual_attr;

/**
 * @}
 */

typedef RDB_object *RDB_getobjfn(const char *, void *);

typedef RDB_type *RDB_gettypefn(const char *, void *);

/** @addtogroup generic
 * @{
 */

enum {
    RDB_DISTINCT = 1,
    RDB_INCLUDED = 2
};

/**
 * Represents an insert.
 */
typedef struct {
    RDB_object *tbp;
    RDB_object *objp;
    int flags;
} RDB_ma_insert;

typedef struct {
    const char *name;
    RDB_expression *exp;
} RDB_attr_update;

/**
 * Represents an update.
 */
typedef struct {
    RDB_object *tbp;
    RDB_expression *condp;
    int updc;
    RDB_attr_update *updv;
} RDB_ma_update;

/**
 * Represents a delete with a WHERE clause.
 */
typedef struct {
    RDB_object *tbp;
    RDB_expression *condp;
} RDB_ma_delete;

/**
 * Represents a delete with a tuple or relation argument.
 */
typedef struct {
    RDB_object *tbp;
    RDB_object *objp;
    int flags;
} RDB_ma_vdelete;

typedef struct {
    RDB_object *dstp;
    RDB_object *srcp;
} RDB_ma_copy;

/**
 * @}
 */

/** @addtogroup type
 * @{
 */

enum {
    RDB_TYPE_ORDERED = 1
};

/**
 * @}
 */

int
RDB_init_builtin(RDB_exec_context *);

const char *
RDB_db_name(const RDB_database *);

RDB_environment *
RDB_db_env(RDB_database *);

RDB_database *
RDB_create_db_from_env(const char *, RDB_environment *,
        RDB_exec_context *);

RDB_database *
RDB_get_db_from_env(const char *, RDB_environment *, RDB_exec_context *);

int
RDB_drop_db(RDB_database *, RDB_exec_context *);

int
RDB_get_dbs(RDB_environment *, RDB_object *, RDB_exec_context *);

RDB_object *
RDB_create_table(const char *,
        int, const RDB_attr[],
        int, const RDB_string_vec[],
        RDB_exec_context *, RDB_transaction *);

RDB_object *
RDB_create_table_from_type(const char *name,
                RDB_type *,
                int keyc, const RDB_string_vec[],
                int, const RDB_attr[],
                RDB_exec_context *, RDB_transaction *);

int
RDB_init_table(RDB_object *, const char *,
        int, const RDB_attr[],
        int, const RDB_string_vec[],
        RDB_exec_context *);

int
RDB_init_table_from_type(RDB_object *, const char *, RDB_type *,
        int, const RDB_string_vec keyv[],
        int, const RDB_attr[],
        RDB_exec_context *);

int
RDB_create_public_table(const char *,
        int, const RDB_attr[],
        int, const RDB_string_vec[],
        RDB_exec_context *, RDB_transaction *);

int
RDB_create_public_table_from_type(const char *,
                RDB_type *,
                int, const RDB_string_vec[],
                RDB_exec_context *, RDB_transaction *);

int
RDB_map_public_table(const char *, RDB_expression *,
        RDB_exec_context *, RDB_transaction *);

RDB_object *
RDB_get_table(const char *, RDB_exec_context *, RDB_transaction *);

int
RDB_drop_table(RDB_object *, RDB_exec_context *, RDB_transaction *);

int
RDB_drop_table_by_name(const char *, RDB_exec_context *, RDB_transaction *);

int
RDB_table_keys(RDB_object *, RDB_exec_context *, RDB_string_vec **);

const char *
RDB_table_name(const RDB_object *);

int
RDB_set_table_name(RDB_object *, const char *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_add_table(RDB_object *, RDB_exec_context *, RDB_transaction *);

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
RDB_move_tuples(RDB_object *, RDB_object *, int, RDB_exec_context *,
        RDB_transaction *);

RDB_int
RDB_multi_assign(int, const RDB_ma_insert[],
        int, const RDB_ma_update[],
        int, const RDB_ma_delete[],
        int, const RDB_ma_vdelete[],
        int, const RDB_ma_copy[],
        RDB_exec_context *, RDB_transaction *);

int
RDB_apply_constraints(int, const RDB_ma_insert[],
        int, const RDB_ma_update[],
        int, const RDB_ma_delete[],
        int, const RDB_ma_vdelete[],
        int, const RDB_ma_copy[],
        RDB_apply_constraint_fn *,
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
RDB_table_contains(RDB_object *tbp, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

int
RDB_subset(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

int
RDB_table_matching_tuple(RDB_object *, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

int
RDB_extract_tuple(RDB_object *, RDB_exec_context *, RDB_transaction *,
        RDB_object *);

RDB_bool
RDB_table_is_persistent(const RDB_object *);

RDB_bool
RDB_table_is_real(const RDB_object *);

RDB_bool
RDB_table_is_stored(const RDB_object *);

RDB_bool
RDB_table_is_user(const RDB_object *);

int
RDB_table_is_empty(RDB_object *, RDB_exec_context *, RDB_transaction *,
        RDB_bool *resultp);

RDB_int
RDB_cardinality(RDB_object *tbp, RDB_exec_context *, RDB_transaction *);

RDB_object *
RDB_expr_to_vtable(RDB_expression *, RDB_exec_context *, RDB_transaction *);

RDB_attr *
RDB_table_attrs(const RDB_object *, int *);

RDB_bool
RDB_expr_is_serial(const RDB_expression *);

int
RDB_create_table_index(const char *name, RDB_object *tbp, int idxcompc,
        const RDB_seq_item idxcompv[], int flags, RDB_exec_context *,
        RDB_transaction *);

int
RDB_drop_table_index(const char *name, RDB_exec_context *, RDB_transaction *);

int
RDB_infer_keys(RDB_expression *, RDB_getobjfn *, void *,
        RDB_environment *, RDB_exec_context *, RDB_transaction *,
        RDB_string_vec **, RDB_bool *);

int
RDB_define_type(const char *name, int repc, const RDB_possrep repv[],
                RDB_expression *, RDB_expression *, int,
                RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_get_type(const char *name, RDB_exec_context *, RDB_transaction *);

int
RDB_drop_type(const char *name, RDB_exec_context *, RDB_transaction *);

int
RDB_next_attr_sorted(const RDB_type *, const char *);

RDB_type *
RDB_type_attr_type(const RDB_type *, const char *);

RDB_bool
RDB_is_selector(const RDB_operator *);

RDB_possrep *
RDB_comp_possrep(const RDB_type *, const char *);

RDB_database *
RDB_tx_db(RDB_transaction *);

RDB_bool
RDB_tx_is_running(RDB_transaction *);

int
RDB_begin_tx(RDB_exec_context *, RDB_transaction *, RDB_database *dbp,
        RDB_transaction *parent);

int
RDB_commit(RDB_exec_context *, RDB_transaction *);

int
RDB_rollback(RDB_exec_context *, RDB_transaction *);

void
RDB_handle_errcode(int errcode, RDB_exec_context *, RDB_transaction *);

int
RDB_obj_equals(const RDB_object *, const RDB_object *,
        RDB_exec_context *, RDB_transaction *, RDB_bool *);

int
RDB_set_init_value(RDB_object *, RDB_type *, RDB_environment *,
        RDB_exec_context *);

int
RDB_extend_tuple(RDB_object *, int attrc, const RDB_virtual_attr attrv[],
                 RDB_exec_context *, RDB_transaction *);

int
RDB_add_tuple(RDB_object *, const RDB_object *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_union_tuples(const RDB_object *, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, RDB_object *);

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
RDB_array_get(const RDB_object *, RDB_int idx, RDB_exec_context *);

int
RDB_array_set(RDB_object *, RDB_int idx, const RDB_object *tplp,
        RDB_exec_context *);

RDB_int
RDB_array_length(const RDB_object *, RDB_exec_context *);

int
RDB_set_array_length(RDB_object *arrp, RDB_int len, RDB_exec_context *);

RDB_qresult *
RDB_table_iterator(RDB_object *, int, const RDB_seq_item[],
                   RDB_exec_context *, RDB_transaction *);

int
RDB_del_table_iterator(RDB_qresult *, RDB_exec_context *, RDB_transaction *);

int
RDB_next_tuple(RDB_qresult *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_obj_property(const RDB_object *, const char *compname,
                   RDB_object *comp, RDB_environment *,
                   RDB_exec_context *, RDB_transaction *);

int
RDB_obj_set_property(RDB_object *, const char *compname,
                 const RDB_object *comp, RDB_environment *,
                 RDB_exec_context *, RDB_transaction *);

int
RDB_evaluate(RDB_expression *, RDB_getobjfn *, void *, RDB_environment *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

RDB_type *
RDB_expr_type(RDB_expression *, RDB_gettypefn *, void *,
        RDB_environment *, RDB_exec_context *, RDB_transaction *);

int
RDB_expr_equals(const RDB_expression *, const RDB_expression *,
        RDB_exec_context *, RDB_transaction *, RDB_bool *);

int
RDB_evaluate_bool(RDB_expression *, RDB_getobjfn *getfnp, void *getdata,
        RDB_environment *, RDB_exec_context *, RDB_transaction *, RDB_bool *);

int
RDB_expr_resolve_varname_expr(RDB_expression **, const char *,
        RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_expr_resolve_varnames(RDB_expression *, RDB_getobjfn *,
        void *, RDB_exec_context *, RDB_transaction *);

int
RDB_create_ro_op(const char *name, int, RDB_parameter[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const char *sourcep,
                 RDB_exec_context *, RDB_transaction *);

int
RDB_create_update_op(const char *name, int, RDB_parameter[],
                  const char *libname, const char *symname,
                  const char *sourcep,
                  RDB_exec_context *, RDB_transaction *);

RDB_operator *
RDB_get_update_op(const char *, int, RDB_type *[],
               RDB_exec_context *, RDB_transaction *);

RDB_operator *
RDB_get_update_op_e(const char *, int, RDB_type *[],
               RDB_environment *, RDB_exec_context *, RDB_transaction *);

int
RDB_call_ro_op_by_name(const char *, int, RDB_object *[],
               RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_call_ro_op_by_name_e(const char *, int, RDB_object *[], RDB_environment *,
               RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_call_update_op_by_name(const char *name, int argc, RDB_object *argv[],
                RDB_exec_context *, RDB_transaction *);

int
RDB_call_update_op(RDB_operator *, int argc, RDB_object *[],
                RDB_exec_context *, RDB_transaction *);

int
RDB_drop_op(const char *name, RDB_exec_context *, RDB_transaction *);

int
RDB_put_global_ro_op(const char *, int, RDB_type **, RDB_type *,
        RDB_ro_op_func *, RDB_exec_context *);

int
RDB_create_constraint(const char *name, RDB_expression *,
                      RDB_exec_context *, RDB_transaction *);

int
RDB_drop_constraint(const char *name, RDB_exec_context *,
        RDB_transaction *);

int
RDB_www_form_to_tuple(RDB_object *, const char *, RDB_exec_context *);

#endif
