#ifndef RDB_INTERNAL_H
#define RDB_INTERNAL_H

/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include <ltdl.h>

#define AVG_COUNT_SUFFIX "$C"

/* initial capacities of attribute map and table map */
enum {
    RDB_DFL_MAP_CAPACITY = 37
};

typedef struct RDB_dbroot {
    RDB_environment *envp;
    RDB_hashmap typemap;
    RDB_hashmap ro_opmap;
    RDB_hashmap upd_opmap;
    RDB_database *firstdbp;

    /* catalog tables */
    RDB_table *rtables_tbp;
    RDB_table *table_attr_tbp;
    RDB_table *table_attr_defvals_tbp;
    RDB_table *vtables_tbp;
    RDB_table *dbtables_tbp;
    RDB_table *keys_tbp;
    RDB_table *types_tbp;    
    RDB_table *possreps_tbp;
    RDB_table *possrepcomps_tbp;
    RDB_table *ro_ops_tbp;
    RDB_table *ro_op_rtypes_tbp;
    RDB_table *upd_ops_tbp;
    RDB_table *indexes_tbp;
} RDB_dbroot;

typedef struct RDB_qresult {
    RDB_table *tbp; /* NULL for sorter */
    union {
        RDB_cursor *curp;
        struct {
            struct RDB_qresult *qrp;
            struct RDB_qresult *qr2p;
            
            /* only used for join */
            RDB_object tpl;
            RDB_bool tpl_valid;
        } virtual;
    } var;
    int endreached;
 
    /*
     * 'materialized' table, needed for PROJECT with duplicate elimination,
     *  SUMMARIZE PER, and sorting.
     */
    RDB_table *matp;
} RDB_qresult;

typedef struct RDB_ipossrep {
    char *name;
    int compc;
    RDB_attr *compv;
    struct RDB_expression *constraintp;
} RDB_ipossrep;

typedef int RDB_ro_op_func(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp);

typedef struct RDB_ro_op {
    char *name;
    int argc;
    RDB_type **argtv;
    RDB_type *rtyp;
    RDB_object iarg;
    lt_dlhandle modhdl;
    RDB_ro_op_func *funcp;
    struct RDB_ro_op *nextp;
} RDB_ro_op;

typedef int RDB_upd_op_func(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_transaction *txp);

typedef struct RDB_upd_op {
    char *name;
    int argc;
    RDB_type **argtv;
    RDB_object iarg;
    lt_dlhandle modhdl;
    RDB_upd_op_func *funcp;
    RDB_bool *updv;
    struct RDB_upd_op *nextp;
} RDB_upd_op;

/* Internal functions */

void
_RDB_init_builtin_types(void);

/* Abort transaction and all parent transactions */
int
RDB_rollback_all(RDB_transaction *txp);

int
_RDB_begin_tx(RDB_transaction *, RDB_environment *,
        RDB_transaction *);

/*
 * Iterator over the tuples of a RDB_table. Used internally.
 * Using it from an application is possible, but violates RM proscription 7.
 */
int
_RDB_table_qresult(RDB_table *, RDB_transaction *, RDB_qresult **);

int
_RDB_sorter(RDB_table *tbp, RDB_qresult **qrespp, RDB_transaction *txp,
            int seqitc, const RDB_seq_item seqitv[]);

int
_RDB_next_tuple(RDB_qresult *, RDB_object *, RDB_transaction *);

int
_RDB_qresult_contains(RDB_qresult *, const RDB_object *, RDB_transaction *);

int
_RDB_reset_qresult(RDB_qresult *, RDB_transaction *);

int
_RDB_get_by_uindex(RDB_table *tbp, RDB_object valv[], _RDB_tbindex *indexp,
        RDB_transaction *txp, RDB_object *tplp);

int
_RDB_drop_qresult(RDB_qresult *, RDB_transaction *);

int
_RDB_new_stored_table(const char *name, RDB_bool persistent,
                RDB_type *reltyp,
                int keyc, RDB_string_vec keyv[], RDB_bool usr,
                RDB_table **tbpp);

void
_RDB_free_table(RDB_table *tbp, RDB_environment *envp);

int
_RDB_open_table(RDB_table *tbp,
           int piattrc, char *piattrv[], RDB_bool create,
           RDB_transaction *txp, RDB_environment *envp, RDB_bool ascv[]);

int
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_string_vec keyv[],
                RDB_transaction *txp,
                RDB_table **tbpp);

int
_RDB_provide_table(const char *name, RDB_bool persistent,
           int attrc, RDB_attr heading[],
           int keyc, RDB_string_vec keyv[], RDB_bool usr,
           RDB_bool create, RDB_transaction *txp, RDB_environment *envp,
           RDB_table **tbpp);

int
_RDB_assoc_table_db(RDB_table *tbp, RDB_database *dbp);

int
_RDB_drop_rtable(RDB_table *tbp, RDB_transaction *txp);

int
_RDB_drop_table(RDB_table *tbp, RDB_transaction *txp, RDB_bool rec);

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

int
RDB_project_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_type **newtypp);

/*
 * Create a type that is a projection of the relation type pointed to by typ
 * over the attributes given by attrc and attrv.
 * The new type in the location pointed to by newtypp.
 * If one of the attributes in attrv is not found in the relation type,
 * RDB_INVALID_ARGUMENT is returned.
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

int
RDB_wrap_relation_type(const RDB_type *typ, int wrapc, RDB_wrapping wrapv[],
        RDB_type **newtypp);

int
RDB_unwrap_relation_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_type **newtypp);

RDB_attr *
_RDB_tuple_type_attr(const RDB_type *tuptyp, const char *attrname);

RDB_bool
_RDB_legal_name(const char *name);

int
_RDB_del_recmap(RDB_transaction *, RDB_recmap *);

int
_RDB_del_index(RDB_transaction *, RDB_index *);

int
RDB_evaluate_bool(RDB_expression *, const RDB_object *tup, RDB_transaction *,
                  RDB_bool *);

int
RDB_evaluate(RDB_expression *, const RDB_object *, RDB_transaction *, RDB_object *);

int
_RDB_find_rename_from(int renc, RDB_renaming renv[], const char *name);

RDB_expression *
_RDB_create_unexpr(RDB_expression *arg, enum _RDB_expr_kind kind);

RDB_expression *
_RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2,
                    enum _RDB_expr_kind kind);

RDB_expression *
RDB_expr_aggregate(RDB_expression *arg, RDB_aggregate_op op,
        const char *attrname);

RDB_bool
_RDB_expr_refers(RDB_expression *, RDB_table *);

/*
 * Internal tuple functions
 */

int
_RDB_copy_tuple(RDB_object *dstp, const RDB_object *srcp);

int
_RDB_copy_array(RDB_object *dstp, const RDB_object *srcp);

RDB_bool
_RDB_array_equals(RDB_object *arr1p, RDB_object *arr2p);

RDB_bool
_RDB_tuple_equals(const RDB_object *, const RDB_object *);

int
_RDB_invrename_tuple(const RDB_object *, int renc, RDB_renaming renv[],
                 RDB_object *restup);

int
_RDB_invwrap_tuple(const RDB_object *tplp, int wrapc, RDB_wrapping wrapv[],
        RDB_object *restplp);

int
_RDB_invunwrap_tuple(const RDB_object *, int attrc, char *attrv[],
        RDB_type *srctuptyp, RDB_object *restplp);


int
_RDB_sdivide_preserves(RDB_table *, const RDB_object *tplp, RDB_qresult *qr3p,
        RDB_transaction *);

RDB_ipossrep *
_RDB_get_possrep(RDB_type *typ, const char *repname);

RDB_attr *
_RDB_get_icomp(RDB_type *, const char *compname);

void
_RDB_set_obj_type(RDB_object *, RDB_type *);

void
_RDB_set_nonsc_type(RDB_object *objp, RDB_type *typ);

int
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_transaction *txp, RDB_ro_op **opp);

int
_RDB_get_upd_op(const char *name, int argc, RDB_type *argtv[],
               RDB_transaction *txp, RDB_upd_op **opp);

void
_RDB_free_ro_ops(RDB_ro_op *op);

void
_RDB_free_upd_ops(RDB_upd_op *op);

int
_RDB_move_tuples(RDB_table *dstp, RDB_table *srcp, RDB_transaction *);

void
_RDB_obj_to_field(RDB_field *, RDB_object *);

#define _RDB_pkey_len(tbp) ((tbp)->keyv[0].strc)

RDB_type *
RDB_expr_type(const RDB_expression *exp, const RDB_type *);

int
_RDB_check_type_constraint(RDB_object *valp, RDB_transaction *txp);

int
_RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp);

RDB_table *
_RDB_new_table(void);

int
_RDB_open_table_index(RDB_table *tbp, _RDB_tbindex *indexp,
        RDB_environment *, RDB_transaction *txp);

int
_RDB_optimize(RDB_table *tbp, RDB_transaction *);

#endif
