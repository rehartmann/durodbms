/* $Id$ */

#include <stdio.h>
#include "parse.h"
#include <rel/rdb.h>
#include <rel/internal.h>

int yyparse(void);
void yy_scan_string(const char *txt);

RDB_transaction *_RDB_parse_txp;
RDB_expression *_RDB_parse_resultp;
RDB_ltablefn *_RDB_parse_ltfp;
void *_RDB_parse_arg;
RDB_exec_context *_RDB_parse_ecp;

RDB_expression *
_RDB_parse_lookup_table(RDB_expression *);

int yywrap(void) {
    return 1;
}

void yyerror(char *errtxt) {
    RDB_raise_syntax(errtxt, _RDB_parse_ecp);
}

RDB_expression *
RDB_parse_expr(const char *txt, RDB_ltablefn *lt_fp, void *lt_arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int pret;
    RDB_expression *exp;

    _RDB_parse_txp = txp;
    _RDB_parse_ltfp = lt_fp;
    _RDB_parse_arg = lt_arg;
    _RDB_parse_ecp = ecp;

    yy_scan_string(txt);
    pret = yyparse();
    if (pret != 0) {
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("parse error", ecp);
        }
        return NULL;
    }

    /* If the expression represents an attribute, try to get table */
    if (_RDB_parse_resultp->kind == RDB_EX_VAR) {
        exp = _RDB_parse_lookup_table(_RDB_parse_resultp);
        if (exp == NULL) {
            RDB_drop_expr(_RDB_parse_resultp, ecp);
            RDB_raise_no_memory(ecp);
            return NULL;
        }
    } else {
        exp = _RDB_parse_resultp;
    }
    return exp;
}

RDB_object *
RDB_parse_table(const char *txt, RDB_ltablefn *lt_fp, void *lt_arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;
    RDB_expression *exp = RDB_parse_expr(txt, lt_fp, lt_arg, ecp, txp);
    if (exp == NULL)
        return NULL;

    tbp = RDB_expr_obj(exp);
    if (tbp == NULL) {
        tbp = RDB_expr_to_vtable(exp, ecp, txp);
        if (tbp == NULL) {
            RDB_drop_expr(exp, ecp);
            return NULL;
        }
        return tbp;
    }
    if (tbp->kind != RDB_OB_TABLE) {        
        RDB_raise_type_mismatch("no table", ecp);
        return NULL;
    }
    if (exp->kind == RDB_EX_OBJ) {
        /* Make a copy of the table */
        RDB_type *typ = _RDB_dup_nonscalar_type(tbp->typ, ecp);
        if (typ == NULL)
            return NULL;
        
        tbp = RDB_create_table_from_type(NULL, RDB_FALSE, typ,
                0, NULL, ecp, NULL);
        if (tbp == NULL) {
            RDB_drop_type(typ, ecp, NULL);
            return NULL;
        }
        if (_RDB_move_tuples(tbp, RDB_expr_obj(exp), ecp, NULL) != RDB_OK) {
            RDB_drop_table(tbp, ecp, NULL);
            return NULL;
        }
    }
    RDB_drop_expr(exp, ecp);
    return tbp;
}
