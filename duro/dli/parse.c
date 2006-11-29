/* $Id$ */

#include <stdio.h>
#include "parse.h"
#include <rel/rdb.h>
#include <rel/internal.h>

int yyparse(void);

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *txt);
void yy_delete_buffer(YY_BUFFER_STATE);

RDB_transaction *_RDB_parse_txp;
RDB_expression *_RDB_parse_resultp;
RDB_ltablefn *_RDB_parse_ltfp;
void *_RDB_parse_arg;
RDB_exec_context *_RDB_parse_ecp;

RDB_expression *
_RDB_parse_lookup_table(RDB_expression *);

int yywrap(void)
{
    return 1;
}

void yyerror(char *errtxt)
{
    RDB_raise_syntax(errtxt, _RDB_parse_ecp);
}

RDB_expression *
RDB_parse_expr(const char *txt, RDB_ltablefn *lt_fp, void *lt_arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int pret;
    RDB_expression *exp;
    YY_BUFFER_STATE buf;

    _RDB_parse_txp = txp;
    _RDB_parse_ltfp = lt_fp;
    _RDB_parse_arg = lt_arg;
    _RDB_parse_ecp = ecp;

    buf = yy_scan_string(txt);
    pret = yyparse();
    yy_delete_buffer(buf);
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
