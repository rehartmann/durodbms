#ifndef RDB_INTERNAL_H
#define RDB_INTERNAL_H

/* $Id$ */

#include <ltdl.h>

#define AVG_COUNT_SUFFIX "$C"

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
    RDB_table *upd_ops_tbp;
} RDB_dbroot;

typedef struct RDB_qresult {
    RDB_table *tbp;
    union {
        RDB_cursor *curp;
        struct {
            struct RDB_qresult *qrp;
            struct RDB_qresult *qr2p;
            
            /* only used for join */
            RDB_tuple tpl;
            RDB_bool tpl_valid;
        } virtual;
    } var;
    int endreached;
 
    /* needed for duplicate elimination */
    RDB_table *matp;
} RDB_qresult;

typedef int RDB_selector_func(RDB_object *, RDB_object *[],
        RDB_type *, const char *);

typedef int RDB_setter_func(RDB_object *, const RDB_object *,
        RDB_type *, const char *);

typedef int RDB_getter_func(const RDB_object *, RDB_object *,
        RDB_type *, const char *);

typedef struct {
    char *name;
    RDB_type *typ;
    RDB_object *defaultp;
    int options;
    RDB_getter_func *getterp;
    RDB_setter_func *setterp;
} RDB_icomp;

typedef struct RDB_ipossrep {
    char *name;
    int compc;
    RDB_icomp *compv;
    struct RDB_expression *constraintp;
    RDB_selector_func *selectorp;
} RDB_ipossrep;

typedef int RDB_ro_op_func(const char *name, int argc, RDB_object *argv[],
        RDB_object *retvalp, const char *iargp, RDB_transaction *txp);

typedef struct RDB_ro_op {
    char *name;
    int argc;
    RDB_type **argtv;
    RDB_type *rtyp;
    char *iargp;
    lt_dlhandle modhdl;
    RDB_ro_op_func *funcp;
    struct RDB_ro_op *nextp;
} RDB_ro_op;

typedef int RDB_upd_op_func(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const char *iargp, RDB_transaction *txp);

typedef struct RDB_upd_op {
    char *name;
    int argc;
    RDB_type **argtv;
    char *iargp;
    lt_dlhandle modhdl;
    RDB_upd_op_func *funcp;
    struct RDB_upd_op *nextp;
} RDB_upd_op;

/* Internal functions */

void
_RDB_init_builtin_types(void);

/* Iterator over the tuples of a RDB_table. Used internally.
 * Using it from an application is possible, but violates RM proscription 7.
 */
int
_RDB_table_qresult(RDB_table *, RDB_qresult **, RDB_transaction *);

int
_RDB_next_tuple(RDB_qresult *, RDB_tuple *, RDB_transaction *);

int
_RDB_qresult_contains(RDB_qresult *, const RDB_tuple *, RDB_transaction *);

int
_RDB_get_by_pindex(RDB_table *, RDB_object[], RDB_tuple *,
        RDB_transaction *);

int
_RDB_drop_qresult(RDB_qresult *, RDB_transaction *);

int
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_string_vec keyv[],
                RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_open_table(const char *name, RDB_bool persistent,
           int attrc, RDB_attr heading[],
           int keyc, RDB_string_vec keyv[], RDB_bool usr,
           RDB_bool create, RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_assign_table_db(RDB_table *tbp, RDB_database *dbp);

int
_RDB_drop_rtable(RDB_table *tbp, RDB_transaction *txp);

int
_RDB_drop_table(RDB_table *tbp, RDB_transaction *txp, RDB_bool rec);

RDB_attr *
_RDB_tuple_type_attr(const RDB_type *tuptyp, const char *attrname);

RDB_bool
_RDB_legal_name(const char *name);

int
RDB_evaluate_bool(RDB_expression *, const RDB_tuple *tup, RDB_transaction *,
                  RDB_bool *);

int
RDB_evaluate(RDB_expression *, const RDB_tuple *, RDB_transaction *, RDB_object *);

int
_RDB_find_rename_from(int renc, RDB_renaming renv[], const char *name);

RDB_expression *
_RDB_create_unexpr(RDB_expression *arg, enum _RDB_expr_kind kind);

RDB_expression *
_RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2,
                    enum _RDB_expr_kind kind);

RDB_bool
_RDB_expr_refers(RDB_expression *, RDB_table *);

RDB_ipossrep *
_RDB_get_possrep(RDB_type *typ, const char *repname);

RDB_icomp *
_RDB_get_icomp(RDB_type *, const char *compname);

void
_RDB_set_obj_type(RDB_object *, RDB_type *);

int
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_transaction *txp, RDB_ro_op **opp);

#define _RDB_pkey_len(tbp) ((tbp)->keyv[0].strc)

#endif
