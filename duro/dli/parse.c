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

RDB_table *
RDB_parse_table(const char *txt, RDB_ltablefn *lt_fp, void *lt_arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_table *tbp;
    RDB_expression *exp = RDB_parse_expr(txt, lt_fp, lt_arg, ecp, txp);
    if (exp == NULL)
        return NULL;

    if (exp->kind != RDB_EX_OBJ || exp->var.obj.kind != RDB_OB_TABLE) {
        RDB_object obj;

        RDB_init_obj(&obj);
        ret = RDB_evaluate(exp, NULL, ecp, txp, &obj);
        RDB_drop_expr(exp, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&obj, ecp);
            return NULL;
        }
        if (obj.kind != RDB_OB_TABLE) {
            RDB_destroy_obj(&obj, ecp);
            RDB_raise_type_mismatch("no table", ecp);
            return NULL;
        }
        tbp = obj.var.tbp;
        obj.var.tbp = NULL;
        RDB_destroy_obj(&obj, ecp);
        return tbp;
    }

    /* !! drop exp? */
    return exp->var.obj.var.tbp;
}
