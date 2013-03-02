#ifndef RDB_INTERNAL_H
#define RDB_INTERNAL_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "opmap.h"
#include <rec/cursor.h>
#include <gen/hashtable.h>

#define AVG_COUNT "$COUNT"

enum {
    /** initial capacities of attribute map and table map */
    RDB_DFL_MAP_CAPACITY = 37,

    /** marks types which have been defined but not implemented */
    RDB_NOT_IMPLEMENTED = -2
};

enum RDB_expr_kind {
    RDB_EX_OBJ,
    RDB_EX_TBP,

    RDB_EX_VAR,

    RDB_EX_TUPLE_ATTR,
    RDB_EX_GET_COMP,
    RDB_EX_RO_OP
};

struct RDB_expression {
    enum RDB_expr_kind kind;
    union {
        char *varname;
        RDB_object obj;
        struct {
            RDB_object *tbp;
            struct RDB_tbindex *indexp;
        } tbref;
        struct {
            RDB_expr_list args;
            char *name;
            struct {
                int objc;
                RDB_expression *stopexp;

                /*
                 * The following fields are only valid if objc > 0
                 * or stopexp != NULL
                 */

                /* Optionally stores the values in objpv */
                RDB_object *objv;

                RDB_object **objpv;
                RDB_bool asc;
                RDB_bool all_eq;
            } optinfo;
        } op;
    } def;

    /*
     * The expression type. NULL if the type has not been determined.
     * If typ is non-scalar, it is destroyed by RDB_drop_expr().
     */
    RDB_type *typ; 
    struct RDB_expression *nextp;

    /* RDB_TRUE if the expression has been transformed by RDB_transform(). */
    RDB_bool transformed;
    RDB_bool optimized;
};

struct RDB_database {
    char *name;
    RDB_hashmap tbmap;
    
    /* pointer to next DB in environment */
    struct RDB_database *nextdbp;

    struct RDB_dbroot *dbrootp;
};

typedef struct {
    char *key;
    RDB_object obj;
} tuple_entry;

typedef struct RDB_constraint {
    char *name;
    RDB_expression *exp;
    struct RDB_constraint *nextp;
} RDB_constraint;

typedef struct RDB_dbroot {
    RDB_environment *envp;
    RDB_hashmap typemap;
    RDB_op_map ro_opmap;
    RDB_op_map upd_opmap;
    RDB_database *first_dbp;
    RDB_constraint *first_constrp;
    RDB_bool constraints_read;

    /* catalog tables */
    RDB_object *rtables_tbp;
    RDB_object *table_attr_tbp;
    RDB_object *table_attr_defvals_tbp;
    RDB_object *vtables_tbp;
    RDB_object *table_recmap_tbp;
    RDB_object *dbtables_tbp;
    RDB_object *keys_tbp;
    RDB_object *types_tbp;    
    RDB_object *possrepcomps_tbp;
    RDB_object *ro_ops_tbp;
    RDB_object *upd_ops_tbp;
    RDB_object *indexes_tbp;
    RDB_object *constraints_tbp;
    RDB_object *version_info_tbp;
} RDB_dbroot;

struct RDB_op_data {
    char *name;
    RDB_type *rtyp;
    RDB_object source;
    lt_dlhandle modhdl;
    int paramc;
    struct RDB_parameter *paramv;
    union {
        RDB_upd_op_func *upd_fp;
        RDB_ro_op_func *ro_fp;
    } opfn;
    void *u_data;
    RDB_op_cleanup_func *cleanup_fp;
};

typedef struct {
    char *key;
    RDB_int fno;
} RDB_attrmap_entry;

struct RDB_tx_and_ec {
    RDB_transaction *txp;
    RDB_exec_context *ecp;
};

extern RDB_hashmap RDB_builtin_type_map;

extern RDB_op_map RDB_builtin_ro_op_map;

/* Used to pass the execution context (not MT-safe!) */
extern RDB_exec_context *RDB_cmp_ecp;

/* Internal functions */

int
RDB_add_type(RDB_type *, RDB_exec_context *);

/**
 * Abort transaction and all parent transactions
 */
int
RDB_rollback_all(RDB_exec_context *, RDB_transaction *);

int
RDB_begin_tx_env(RDB_exec_context *, RDB_transaction *, RDB_environment *,
        RDB_transaction *);

int
RDB_expr_matching_tuple(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp);

RDB_object *
RDB_new_obj(RDB_exec_context *ecp);

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
RDB_free_obj(RDB_object *, RDB_exec_context *);

int
RDB_assoc_table_db(RDB_object *, RDB_database *, RDB_exec_context *);

int
RDB_table_equals(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resp);

int
RDB_expr_equals(const RDB_expression *, const RDB_expression *,
        RDB_exec_context *, RDB_transaction *, RDB_bool *);

RDB_bool
RDB_expr_is_string(const RDB_expression *);

int
RDB_destroy_expr(RDB_expression *, RDB_exec_context *);

void
RDB_expr_list_set_lastp(RDB_expr_list *);

/*
 * Extend the tuple type pointed to by typ by the attributes given by
 * attrv and return the new tuple type.
 */
RDB_type *
RDB_extend_tuple_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *);

/*
 * Extend the relation type pointed to by typ by the attributes given by
 * attrv and return the new relation type.
 */
RDB_type *
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *);

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
RDB_project_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_exec_context *ecp);

/*
 * Create a type that is a projection of the relation type pointed to by typ
 * over the attributes given by attrc and attrv.
 * The new type in the location pointed to by newtypp.
 * If one of the attributes in attrv is not found in the relation type,
 * RDB_INVALID_ARGUMENT is returned.
 */
RDB_type *
RDB_project_relation_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_exec_context *ecp);

/*
 * Rename the attributes of the tuple type pointed to by typ according to renc
 * and renv return the new tuple type.
 */
RDB_type *
RDB_rename_tuple_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_exec_context *);

RDB_type *
RDB_rename_relation_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_exec_context *);

RDB_type *
RDB_summarize_type(RDB_expr_list *, int avgc, char **avgv,
        RDB_exec_context *ecp, RDB_transaction *txp);

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
RDB_group_type(RDB_type *typ, int attrc, char *attrv[], const char *gattr,
        RDB_exec_context *);

RDB_type *
RDB_ungroup_type(RDB_type *typ, const char *attr, RDB_exec_context *);

RDB_string_vec *
RDB_dup_rename_keys(int keyc, const RDB_string_vec keyv[], RDB_expression *,
        RDB_exec_context *);

char *
RDB_rename_attr(const char *srcname, RDB_expression *);

RDB_attr *
RDB_tuple_type_attr(const RDB_type *tuptyp, const char *attrname);

RDB_bool
RDB_legal_name(const char *name);

int
RDB_set_defvals(RDB_object *tbp, int attrc, const RDB_attr attrv[],
        RDB_exec_context *);

RDB_object *
RDB_tpl_get(const char *, void *);

int
RDB_find_rename_from(int renc, const RDB_renaming renv[], const char *name);

RDB_expression *
RDB_create_unexpr(RDB_expression *arg, enum RDB_expr_kind kind,
        RDB_exec_context *);

RDB_expression *
RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2,
                    enum RDB_expr_kind kind, RDB_exec_context *);

RDB_bool
RDB_expr_refers(const RDB_expression *, const RDB_object *);

RDB_bool
RDB_expr_refers_var(const RDB_expression *, const char *attrname);

RDB_bool
RDB_expr_table_depend(const RDB_expression *, const RDB_object *);

RDB_bool
RDB_expr_expr_depend(const RDB_expression *, const RDB_expression *);

int
RDB_invrename_expr(RDB_expression *, RDB_expression *,
        RDB_exec_context *);

int
RDB_resolve_exprnames(RDB_expression **, RDB_expression *,
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
RDB_copy_tuple(RDB_object *dstp, const RDB_object *srcp, RDB_exec_context *);

int
RDB_copy_array(RDB_object *dstp, const RDB_object *srcp, RDB_exec_context *);

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
RDB_invrename_tuple(const RDB_object *, const RDB_expression *,
                 RDB_exec_context *, RDB_object *restup);

int
RDB_invwrap_tuple(const RDB_object *tplp, RDB_expression *,
        RDB_exec_context *, RDB_object *restplp);

int
RDB_invunwrap_tuple(const RDB_object *, RDB_expression *,
        RDB_exec_context *, RDB_transaction *, RDB_object *restplp);

RDB_object *
RDB_dup_vtable(RDB_object *, RDB_exec_context *);

int
RDB_vtexp_to_obj(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *tbp);

RDB_attr *
RDB_get_comp_attr(RDB_type *, const char *compname);

int
RDB_obj_ilen(const RDB_object *, size_t *, RDB_exec_context *);

void
RDB_obj_to_irep(void *dstp, const RDB_object *, size_t);

RDB_operator *
RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_environment *, RDB_exec_context *, RDB_transaction *txp);

int
RDB_eq_bool(int, RDB_object *[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_dfl_obj_equals(int argc, RDB_object *argv[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *retvalp);

int
RDB_eq_binary(int, RDB_object *[], RDB_operator *, RDB_exec_context *,
        RDB_transaction *, RDB_object *retvalp);

int
RDB_obj_not_equals(int argc, RDB_object *argv[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *retvalp);

int
RDB_obj_to_field(RDB_field *, RDB_object *, RDB_exec_context *);

#define RDB_pkey_len(tbp) ((tbp)->val.tb.keyv[0].strc)

RDB_type *
RDB_expr_type_tpltyp(RDB_expression *, const RDB_type *, RDB_environment *,
        RDB_exec_context *, RDB_transaction *);

RDB_type *
RDB_tuple_type(const RDB_object *, RDB_exec_context *);

RDB_bool
RDB_obj_matches_type(RDB_object *, RDB_type *);

int
RDB_check_expr_type(RDB_expression *exp, const RDB_type *tuptyp,
        const RDB_type *checktyp, RDB_environment *, RDB_exec_context *, RDB_transaction *);

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
RDB_infer_keys(RDB_expression *, RDB_getobjfn *, void *,
        RDB_environment *, RDB_exec_context *, RDB_transaction *,
        RDB_string_vec **, RDB_bool *);

void
RDB_free_keys(int keyc, RDB_string_vec *keyv);

int
RDB_check_project_keyloss(RDB_expression *exp,
        int keyc, RDB_string_vec *keyv, RDB_bool presv[],
        RDB_exec_context *ecp);

int
RDB_init_builtin_ops(RDB_exec_context *);

int
RDB_add_selector(RDB_type *, RDB_exec_context *);

int
RDB_sys_select(int argc, RDB_object *argv[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *retvalp);

RDB_object **
RDB_index_objpv(struct RDB_tbindex *indexp, RDB_expression *exp, RDB_type *tbtyp,
        int objpc, RDB_bool asc, RDB_exec_context *);

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
RDB_add_del_recmap(RDB_transaction *, RDB_recmap *, RDB_exec_context *);

int
RDB_add_del_index(RDB_transaction *, RDB_index *, RDB_exec_context *);

int
RDB_op_type_relation(int , RDB_object *[], RDB_type *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_op_relation(int, RDB_object *[], RDB_operator *,
        RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_getter_name(const RDB_type *, const char *,
        RDB_object *, RDB_exec_context *);

int
RDB_setter_name(const RDB_type *typ, const char *,
        RDB_object *, RDB_exec_context *);

#endif
