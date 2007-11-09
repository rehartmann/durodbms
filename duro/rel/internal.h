#ifndef RDB_INTERNAL_H
#define RDB_INTERNAL_H

/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "opmap.h"
#include <rec/cursor.h>
#include <gen/hashtable.h>

#include <ltdl.h>

#define AVG_COUNT "$COUNT"

enum {
    /** initial capacities of attribute map and table map */
    RDB_DFL_MAP_CAPACITY = 37,

    /** marks types which have been defined but not implemented */
    RDB_NOT_IMPLEMENTED = -2
};

enum _RDB_expr_kind {
    RDB_EX_OBJ,
    RDB_EX_TBP,

    RDB_EX_VAR,

    RDB_EX_TUPLE_ATTR,
    RDB_EX_GET_COMP,
    RDB_EX_RO_OP
};

struct RDB_expression {
    enum _RDB_expr_kind kind;
    union {
        char *varname;
        RDB_object obj;
        struct {
            RDB_object *tbp;
            struct _RDB_tbindex *indexp;
        } tbref;
        struct {
            RDB_expr_list args;
            char *name;
            struct {
                int objpc;

                /* The following fields are only valid if objpc > 0 */
                RDB_object **objpv;
                RDB_bool asc;
                RDB_bool all_eq;
                RDB_expression *stopexp;
            } optinfo;
        } op;
    } var;
    RDB_type *typ; /* NULL if the type has not been determined */
    struct RDB_expression *nextp;
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
    RDB_hashmap ro_opmap;
    RDB_op_map upd_opmap;
    RDB_database *first_dbp;
    RDB_constraint *first_constrp;
    RDB_bool constraints_read;
    RDB_hashtable empty_tbtab;

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

typedef struct {
    char *key;
    RDB_int fno;
} _RDB_attrmap_entry;

typedef struct RDB_ro_op_desc {
    char *name;
    int argc;
    RDB_type **argtv;
    RDB_type *rtyp;
    RDB_object iarg;
    lt_dlhandle modhdl;
    RDB_ro_op_func *funcp;
    struct RDB_ro_op_desc *nextp;
} RDB_ro_op_desc;

typedef int RDB_upd_op_func(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *, RDB_transaction *);

typedef struct RDB_upd_op_data {
    RDB_object iarg;
    lt_dlhandle modhdl;
    RDB_upd_op_func *funcp;
    RDB_bool *updv;
} RDB_upd_op_data;

struct _RDB_tx_and_ec {
    RDB_transaction *txp;
    RDB_exec_context *ecp;
};

extern RDB_hashmap _RDB_builtin_type_map;

extern RDB_hashmap _RDB_builtin_ro_op_map;

/* Used to pass the execution context (not MT-safe!) */
extern RDB_exec_context *_RDB_cmp_ecp;

/* Internal functions */

int
_RDB_init_builtin_types(RDB_exec_context *);

/**
 * Abort transaction and all parent transactions
 */
int
RDB_rollback_all(RDB_exec_context *, RDB_transaction *);

int
_RDB_begin_tx(RDB_exec_context *, RDB_transaction *, RDB_environment *,
        RDB_transaction *);

int
_RDB_matching_tuple(RDB_object *, const RDB_object *tplp, RDB_exec_context *,
        RDB_transaction *, RDB_bool *resultp);

int
_RDB_expr_matching_tuple(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp);

RDB_object *
_RDB_new_obj(RDB_exec_context *ecp);

RDB_object *
_RDB_new_rtable(const char *name, RDB_bool persistent,
                RDB_type *,
                int keyc, const RDB_string_vec keyv[], RDB_bool usr,
                RDB_exec_context *);

int
_RDB_init_table(RDB_object *, const char *, RDB_bool,
        RDB_type *, int keyc, const RDB_string_vec keyv[], RDB_bool,
        RDB_expression *, RDB_exec_context *);

int
_RDB_free_obj(RDB_object *tbp, RDB_exec_context *);

int
_RDB_assoc_table_db(RDB_object *tbp, RDB_database *dbp, RDB_exec_context *);

RDB_bool
_RDB_table_refers(const RDB_object *tbp, const RDB_object *rtbp);

int
_RDB_table_equals(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resp);

int
_RDB_expr_equals(const RDB_expression *, const RDB_expression *,
        RDB_exec_context *, RDB_transaction *, RDB_bool *);

int
_RDB_destroy_expr(RDB_expression *, RDB_exec_context *);

void
_RDB_expr_list_set_lastp(RDB_expr_list *);

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
RDB_join_tuple_types(const RDB_type *typ1, const RDB_type *typ2,
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
_RDB_dup_rename_keys(int keyc, const RDB_string_vec keyv[], RDB_expression *,
        RDB_exec_context *);

char *
_RDB_rename_attr(const char *srcname, RDB_expression *);

RDB_attr *
_RDB_tuple_type_attr(const RDB_type *tuptyp, const char *attrname);

RDB_bool
_RDB_legal_name(const char *name);

int
_RDB_set_defvals(RDB_type *tbtyp, int attrc, const RDB_attr attrv[],
        RDB_exec_context *);

int
_RDB_del_recmap(RDB_transaction *, RDB_recmap *, RDB_exec_context *);

int
_RDB_del_index(RDB_transaction *, RDB_index *, RDB_exec_context *);

RDB_object *
_RDB_tpl_get(const char *, void *);

int
_RDB_find_rename_from(int renc, const RDB_renaming renv[], const char *name);

RDB_expression *
_RDB_create_unexpr(RDB_expression *arg, enum _RDB_expr_kind kind,
        RDB_exec_context *);

RDB_expression *
_RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2,
                    enum _RDB_expr_kind kind, RDB_exec_context *);

RDB_bool
_RDB_expr_refers(const RDB_expression *, const RDB_object *);

RDB_bool
_RDB_expr_refers_var(const RDB_expression *, const char *attrname);

RDB_bool
_RDB_expr_table_depend(const RDB_expression *, const RDB_object *);

RDB_bool
_RDB_expr_expr_depend(const RDB_expression *, const RDB_expression *);

RDB_expression *
RDB_dup_expr(const RDB_expression *, RDB_exec_context *);

int
_RDB_invrename_expr(RDB_expression *exp, RDB_expression *texp,
        RDB_exec_context *);

int
_RDB_resolve_extend_expr(RDB_expression **expp, RDB_expression *texp,
        RDB_exec_context *);

int
_RDB_expr_to_empty_table(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp);

/*
 * Internal tuple functions
 */

int
_RDB_copy_tuple(RDB_object *dstp, const RDB_object *srcp, RDB_exec_context *);

int
_RDB_copy_array(RDB_object *dstp, const RDB_object *srcp, RDB_exec_context *);

int
_RDB_array_equals(RDB_object *arr1p, RDB_object *arr2p, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

int
_RDB_tuple_equals(const RDB_object *, const RDB_object *, RDB_exec_context *,
        RDB_transaction *, RDB_bool *);

int
_RDB_tuple_matches(const RDB_object *tpl1p, const RDB_object *tpl2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp);

int
_RDB_invrename_tuple(const RDB_object *, const RDB_expression *,
                 RDB_exec_context *, RDB_object *restup);

int
_RDB_invwrap_tuple(const RDB_object *tplp, RDB_expression *,
        RDB_exec_context *, RDB_object *restplp);

int
_RDB_invunwrap_tuple(const RDB_object *, RDB_expression *,
        RDB_exec_context *, RDB_transaction *, RDB_object *restplp);

RDB_object *
_RDB_dup_vtable(RDB_object *, RDB_exec_context *);

int
_RDB_vtexp_to_obj(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *tbp);

RDB_possrep *
_RDB_get_possrep(RDB_type *typ, const char *repname);

RDB_attr *
_RDB_get_icomp(RDB_type *, const char *compname);

int
_RDB_obj_ilen(const RDB_object *, size_t *, RDB_exec_context *);

void
_RDB_obj_to_irep(void *dstp, const RDB_object *, size_t);

int
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_exec_context *, RDB_transaction *txp, RDB_ro_op_desc **opp);

RDB_upd_op_data *
_RDB_get_upd_op(const char *name, int argc, RDB_type *argtv[],
               RDB_exec_context *ecp, RDB_transaction *txp);

RDB_ro_op_desc *
_RDB_new_ro_op(const char *name, int argc, RDB_type *rtyp, RDB_ro_op_func *funcp,
        RDB_exec_context *);

int
_RDB_eq_bool(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp);

int
_RDB_obj_equals(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *,
        RDB_transaction *, RDB_object *retvalp);

int
_RDB_eq_binary(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *,
        RDB_transaction *, RDB_object *retvalp);

int
_RDB_obj_not_equals(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *,
        RDB_transaction *, RDB_object *retvalp);

int
_RDB_put_ro_op(RDB_dbroot *dbrootp, RDB_ro_op_desc *op, RDB_exec_context *);

int
_RDB_put_builtin_ro_op(RDB_ro_op_desc *op, RDB_exec_context *ecp);

void
_RDB_free_ro_ops(RDB_ro_op_desc *op, RDB_exec_context *);

RDB_int
_RDB_move_tuples(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *,
        RDB_transaction *);

int
_RDB_obj_to_field(RDB_field *, RDB_object *, RDB_exec_context *);

#define _RDB_pkey_len(tbp) ((tbp)->var.tb.keyv[0].strc)

RDB_type *
_RDB_expr_type(RDB_expression *, const RDB_type *, RDB_exec_context *,
        RDB_transaction *);

RDB_type *
_RDB_tuple_type(const RDB_object *tplp, RDB_exec_context *);

int
_RDB_check_expr_type(RDB_expression *exp, const RDB_type *tuptyp,
        const RDB_type *checktyp, RDB_exec_context *, RDB_transaction *);

int
_RDB_check_type_constraint(RDB_object *valp, RDB_exec_context *,
        RDB_transaction *);

int
_RDB_constraint_count(RDB_dbroot *dbrootp);

int
_RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *, RDB_transaction *);

int
_RDB_infer_keys(RDB_expression *exp, RDB_exec_context *, RDB_string_vec **,
        RDB_bool *caller_must_freep);

void
_RDB_free_keys(int keyc, RDB_string_vec *keyv);

int
_RDB_check_project_keyloss(RDB_expression *exp,
        int keyc, RDB_string_vec *keyv, RDB_bool presv[],
        RDB_exec_context *ecp);

int
_RDB_init_builtin_ops(RDB_exec_context *);

int
_RDB_add_selector(RDB_type *, RDB_exec_context *);

int
_RDB_sys_select(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *,
        RDB_transaction *, RDB_object *retvalp);

RDB_object **
_RDB_index_objpv(struct _RDB_tbindex *indexp, RDB_expression *exp, RDB_type *tbtyp,
        int objpc, RDB_bool all_eq, RDB_bool asc, RDB_exec_context *);

struct _RDB_tbindex *
_RDB_expr_sortindex (RDB_expression *);

RDB_expression *
_RDB_attr_node(RDB_expression *exp, const char *attrname, char *opname);

int
_RDB_read_constraints(RDB_exec_context *, RDB_transaction *);

int
_RDB_check_constraints(const RDB_constraint *, RDB_exec_context *,
        RDB_transaction *);

void
_RDB_handle_errcode(int errcode, RDB_exec_context *, RDB_transaction *);

#endif
