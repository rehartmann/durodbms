#ifndef RDB_INTERNAL_H
#define RDB_INTERNAL_H

/* $Id$ */

#include <ltdl.h>

#define AVG_COUNT_SUFFIX "$C"

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
    
    /* needed to attach the qresults to the transaction */
    struct RDB_qresult *nextp;
} RDB_qresult;

typedef int RDB_selector_func(RDB_value *, RDB_value *[],
        RDB_type *, const char *);

typedef int RDB_setter_func(RDB_value *, const RDB_value *,
        RDB_type *, const char *);

typedef int RDB_getter_func(const RDB_value *, RDB_value *,
        RDB_type *, const char *);

typedef struct {
    char *name;
    RDB_type *typ;
    RDB_value *defaultp;
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
_RDB_drop_qresult(RDB_qresult *, RDB_transaction *);

int
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_key_attrs keyv[],
                RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_open_table(const char *name, RDB_bool persistent,
           int attrc, RDB_attr heading[],
           int keyc, RDB_key_attrs keyv[], RDB_bool usr,
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
RDB_evaluate(RDB_expression *, const RDB_tuple *, RDB_transaction *, RDB_value *);

int
_RDB_find_rename_from(int renc, RDB_renaming renv[], const char *name);

RDB_expression *
_RDB_create_unexpr(RDB_expression *arg, enum _RDB_expr_kind kind);

RDB_expression *
_RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2,
                    enum _RDB_expr_kind kind);

RDB_icomp *
_RDB_get_icomp(RDB_type *, const char *compname);

void
_RDB_set_value_type(RDB_value *, RDB_type *);

#define _RDB_pkey_len(tbp) ((tbp)->keyv[0].attrc)

#endif
