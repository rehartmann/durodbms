#ifndef RDB_INTERNAL_H
#define RDB_INTERNAL_H

/* $Id$ */

typedef struct RDB_qresult {
    RDB_table *tablep;
    union {
        RDB_cursor *curp;
        struct {
            struct RDB_qresult *qrp;
            
            /* only used for join */
            struct RDB_qresult *nestedp;
            RDB_tuple tpl;
            RDB_bool tpl_valid;
            
            /* table #, only used for union */
            int tbno;
        } virtual;
    } var;
    int endreached;
    RDB_transaction *txp;
 
    /* needed for duplicate elimination */
    RDB_table *matp;
    
    /* needed to attach the qresults to the transaction */
    struct RDB_qresult *nextp;
} RDB_qresult;

/* Internal functions */

void
_RDB_init_builtin_types(void);

/* Iterator over the tuples of a RDB_table. Used internally.
 * Using it from an application is possible, but violates RM proscription 7.
 */
int
_RDB_table_qresult(RDB_table *, RDB_qresult **, RDB_transaction *);

int
_RDB_next_tuple(RDB_qresult *, RDB_tuple *);

int
_RDB_drop_qresult(RDB_qresult *);

int
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_key_attrs keyv[],
                RDB_transaction *txp, RDB_table **tbpp);

int
_RDB_drop_rtable(RDB_table *tbp, RDB_transaction *txp);

int
_RDB_drop_table(RDB_table *tbp, RDB_transaction *txp, RDB_bool rec);

RDB_type *
_RDB_tuple_attr_type(const RDB_type *tuptyp, const char *attrname);

RDB_bool
_RDB_legal_name(const char *name);

int
RDB_evaluate_bool(RDB_expression *, const RDB_tuple *tup, RDB_transaction *,
                  RDB_bool *);

int
RDB_evaluate(RDB_expression *, const RDB_tuple *, RDB_transaction *, RDB_value *);

#define _RDB_pkey_len(tbp) (tbp->keyv[0].attrc)

#endif
