#ifndef RDB_INTERNAL_H
#define RDB_INTERNAL_H

/*
 * Copyright (C) 2003-2009, 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "obj/opmap.h"
#include "obj/object.h"
#include <rec/cursor.h>
#include <gen/hashtable.h>

#define AVG_COUNT "$COUNT"

enum {
    /** initial capacities of attribute map and table map */
    RDB_DFL_MAP_CAPACITY = 37,

    RDB_TB_USER = 1,
    RDB_TB_PERSISTENT = 2,
    RDB_TB_CHECK = 4
};

struct RDB_database {
    char *name;
    RDB_hashmap tbmap;

    /* pointer to next DB in environment */
    struct RDB_database *nextdbp;

    struct RDB_dbroot *dbrootp;
};

typedef struct RDB_constraint {
    char *name;
    RDB_expression *exp;
    struct RDB_constraint *nextp;
} RDB_constraint;

typedef struct RDB_dbroot {
    RDB_environment *envp;

    /* Cached user-defined types */
    RDB_hashmap utypemap;

    /* Cached operators */
    RDB_op_map ro_opmap;
    RDB_op_map upd_opmap;

    /* List of databases */
    RDB_database *first_dbp;

    /* List of constraints */
    RDB_constraint *first_constrp;
    RDB_bool constraints_read;

    /* Public tables */
    RDB_hashmap ptbmap;

    /*
     * Catalog tables
     */

    /** Real tables */
    RDB_object *rtables_tbp;

    /** Table attributes */
    RDB_object *table_attr_tbp;

    /** Attribute default values */
    RDB_object *table_attr_defvals_tbp;

    /** Virtual tables */
    RDB_object *vtables_tbp;

    /** Public tables */
    RDB_object *ptables_tbp;

    RDB_object *table_recmap_tbp;
    RDB_object *dbtables_tbp;
    RDB_object *keys_tbp;
    RDB_object *types_tbp;    
    RDB_object *possrepcomps_tbp;
    RDB_object *ro_ops_tbp;
    RDB_object *ro_op_versions_tbp;
    RDB_object *upd_ops_tbp;
    RDB_object *upd_op_versions_tbp;
    RDB_object *indexes_tbp;
    RDB_object *constraints_tbp;
    RDB_object *version_info_tbp;
    RDB_object *subtype_tbp;
} RDB_dbroot;

typedef struct RDB_table {
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

    unsigned int flags;
} RDB_table;

typedef struct RDB_tbindex {
    char *name;
    int attrc;
    RDB_seq_item *attrv;
    RDB_bool unique;
    RDB_bool ordered;
    RDB_index *idxp;    /* NULL for the primary index */
} RDB_tbindex;

typedef struct {
    char *key;
    RDB_int fno;
} RDB_attrmap_entry;

struct RDB_tx_and_ec {
    RDB_transaction *txp;
    RDB_exec_context *ecp;
};

struct RDB_tuple_and_getfn {
    RDB_object *tplp; /* Pointer to the updated tuple, must not be NULL */
    RDB_getobjfn *getfn;
    void *getarg;
};

extern RDB_hashmap RDB_builtin_type_map;

/* Used to pass the execution context (not MT-safe!) */
extern RDB_exec_context *RDB_cmp_ecp;

extern RDB_op_map RDB_builtin_ro_op_map;
extern RDB_op_map RDB_builtin_upd_op_map;

/* Internal functions */

int
RDB_begin_tx_env(RDB_exec_context *, RDB_transaction *, RDB_environment *,
        RDB_transaction *);

int
RDB_expr_matching_tuple(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp);

RDB_bool
RDB_expr_depends_table(const RDB_expression *, const RDB_object *);

RDB_bool
RDB_expr_depends_expr(const RDB_expression *, const RDB_expression *);

RDB_bool
RDB_table_refers(const RDB_object *, const RDB_object *);

RDB_object *
RDB_new_rtable(const char *, RDB_bool,
                RDB_type *,
                int, const RDB_string_vec[],
                int, const RDB_attr[],
                RDB_bool, RDB_exec_context *);

int
RDB_init_table_i(RDB_object *, const char *, RDB_bool,
        RDB_type *, int keyc, const RDB_string_vec keyv[],
        int, const RDB_attr[],
        RDB_bool, RDB_expression *, RDB_exec_context *);

int
RDB_table_ilen(const RDB_object *, size_t *, RDB_exec_context *);

int
RDB_assoc_table_db(RDB_object *, RDB_database *, RDB_exec_context *);

int
RDB_table_equals(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resp);

int
RDB_close_user_tables(RDB_database *, RDB_exec_context *);

int
RDB_set_user_tables_check(RDB_database *, RDB_exec_context *);

int
RDB_check_table(RDB_object *, RDB_exec_context *, RDB_transaction *);

void
RDB_expr_list_set_lastp(RDB_expr_list *);

/*
 * Join the tuple types pointed to by typ1 and typ2 and store a pointer to
 * The new type in the location pointed to by newtypp.
 * The new type has the attributes from both types.
 * If both types have an attribute with the same name but a different type,
 * RDB_TYPE_MISMATCH is returned.
 */
RDB_type *
RDB_union_tuple_types(const RDB_type *typ1, const RDB_type *typ2,
        RDB_exec_context *);

/*
 * Join the relation types pointed to by typ1 and typ2 and store a pointer to
 * The new type in the location pointed to by newtypp.
 * The new type has the attributes from both types.
 * If both types have an attribute with the same name but a different type,
 * RDB_TYPE_MISMATCH is returned.
 */
RDB_type *
RDB_join_relation_types(const RDB_type *typ1, const RDB_type *typ2,
                     RDB_exec_context *);

RDB_type *
RDB_summarize_type(RDB_expr_list *, RDB_gettypefn *, void *,
        RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_new_nonscalar_obj_type(RDB_object *, RDB_exec_context *);

RDB_string_vec *
RDB_dup_rename_keys(int keyc, const RDB_string_vec[], RDB_expression *,
        RDB_exec_context *);

char *
RDB_rename_attr(const char *srcname, const RDB_expression *);

RDB_bool
RDB_legal_name(const char *name);

int
RDB_set_defvals(RDB_object *tbp, int attrc, const RDB_attr attrv[],
        RDB_exec_context *);

int
RDB_expr_to_empty_table(RDB_expression *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_expr_resolve_tbnames(RDB_expression *, RDB_exec_context *,
        RDB_transaction *);

/*
 * Internal tuple functions
 */

int
RDB_array_equals(RDB_object *arr1p, RDB_object *arr2p, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

int
RDB_tuple_equals(const RDB_object *, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

int
RDB_tuple_matches(const RDB_object *tpl1p, const RDB_object *tpl2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp);

int
RDB_rename_tuple_ex(RDB_object *, const RDB_object *,
        const RDB_expression *, RDB_exec_context *);

int
RDB_invrename_tuple_ex(const RDB_object *, const RDB_expression *,
                 RDB_exec_context *, RDB_object *restup);

int
RDB_invwrap_tuple(const RDB_object *tplp, RDB_expression *,
        RDB_exec_context *, RDB_object *restplp);

int
RDB_invunwrap_tuple(const RDB_object *, RDB_expression *,
        RDB_exec_context *, RDB_transaction *, RDB_object *restplp);

RDB_object *
RDB_tpl_get(const char *, void *);

RDB_object *
RDB_get_from_tuple_or_fn(const char *, void *);

int
RDB_copy_array(RDB_object *dstp, const RDB_object *srcp, RDB_exec_context *);

RDB_object *
RDB_dup_vtable(RDB_object *, RDB_exec_context *);

int
RDB_vtexp_to_obj(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *tbp);

RDB_attr *
RDB_prop_attr(RDB_type *, const char *compname);

int
RDB_obj_ilen(const RDB_object *, size_t *, RDB_exec_context *);

void
RDB_obj_to_irep(void *dstp, const RDB_object *, size_t);

int
RDB_dfl_obj_equals(int argc, RDB_object *argv[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *retvalp);

int
RDB_obj_not_equals(int argc, RDB_object *argv[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *retvalp);

int
RDB_obj_to_field(RDB_field *, RDB_object *, RDB_exec_context *);

#define RDB_pkey_len(tbobjp) ((tbobjp)->val.tbp->keyv[0].strc)

RDB_type *
RDB_expr_type_tpltyp(RDB_expression *, const RDB_type *,
        RDB_gettypefn *getfnp, void *getarg, RDB_environment *,
        RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_tuple_type(const RDB_object *, RDB_exec_context *);

int
RDB_check_expr_type(RDB_expression *exp, const RDB_type *,
        const RDB_type *, RDB_getobjfn *, void *, RDB_environment *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_check_type_constraint(RDB_object *, RDB_environment *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_load_type_ops(RDB_type *, RDB_exec_context *, RDB_transaction *);

int
RDB_constraint_count(RDB_dbroot *dbrootp);

int
RDB_copy_obj_data(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *, RDB_transaction *);

int
RDB_check_project_keyloss(RDB_expression *exp,
        int keyc, RDB_string_vec *keyv, RDB_bool presv[],
        RDB_exec_context *ecp);

int
RDB_init_builtin_ops(RDB_exec_context *);

int
RDB_op_sys_select(int, RDB_object *[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_sys_lt(int, RDB_object *[], RDB_operator *, RDB_exec_context *,
        RDB_transaction *, RDB_object *);

int
RDB_sys_let(int, RDB_object *[], RDB_operator *, RDB_exec_context *,
        RDB_transaction *, RDB_object *);

int
RDB_sys_gt(int, RDB_object *[], RDB_operator *, RDB_exec_context *,
        RDB_transaction *, RDB_object *);

int
RDB_sys_get(int, RDB_object *[], RDB_operator *, RDB_exec_context *,
        RDB_transaction *, RDB_object *);

typedef struct RDB_tbindex RDB_tbindex;

RDB_object **
RDB_index_objpv(RDB_tbindex *, RDB_expression *, RDB_type *,
        int, RDB_bool, RDB_exec_context *);

struct RDB_tbindex *
RDB_expr_sortindex (RDB_expression *);

RDB_expression *
RDB_attr_node(RDB_expression *exp, const char *attrname, char *opname);

int
RDB_read_constraints(RDB_exec_context *, RDB_transaction *);

int
RDB_check_constraints(const RDB_constraint *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_apply_constraints_i(int, const RDB_ma_insert[],
        int, const RDB_ma_update[],
        int, const RDB_ma_delete[],
        int, const RDB_ma_vdelete[],
        int, const RDB_ma_copy[],
        RDB_apply_constraint_fn *,
        RDB_getobjfn *, void *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_add_del_recmap(RDB_transaction *, RDB_recmap *, RDB_exec_context *);

int
RDB_add_del_index(RDB_transaction *, RDB_index *, RDB_exec_context *);

int
RDB_op_type_relation(int , RDB_object *[], RDB_type *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_op_relation(int, RDB_object *[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

RDB_operator *
RDB_get_ro_op(const char *, int, RDB_type *[],
               RDB_environment *, RDB_exec_context *, RDB_transaction *);

RDB_operator *
RDB_get_ro_op_by_args(const char *, int, RDB_object *[],
               RDB_environment *, RDB_exec_context *, RDB_transaction *);

int
RDB_getter_name(const RDB_type *, const char *,
        RDB_object *, RDB_exec_context *);

int
RDB_setter_name(const RDB_type *typ, const char *,
        RDB_object *, RDB_exec_context *);

RDB_operator *
RDB_get_cmp_op(RDB_type *, RDB_exec_context *, RDB_transaction *);

int
RDB_add_comparison_ops(RDB_type *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_seq_container_name(const char *, const char *, RDB_object *,
        RDB_exec_context *);

void
RDB_close_sequences(RDB_object *, RDB_exec_context *);

RDB_expression *
RDB_attr_eq_strval(const char *, const char *, RDB_exec_context *);

int
RDB_possrep_to_selector(RDB_object *, const char *,
        const char *, RDB_exec_context *);

RDB_type *
RDB_get_tuple_attr_type(const char *, void *);

RDB_type *
RDB_get_subtype(RDB_type *, const char *);

RDB_type *
RDB_get_supertype_of_subtype(RDB_type *, const char *);

RDB_bool
RDB_irep_is_string(const RDB_type *);

int
RDB_type_field_flags(const RDB_type *);

#endif
