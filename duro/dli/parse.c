/* $Id$ */

#include <stdio.h>
#include "parse.h"
#include <rel/rdb.h>
#include <rel/internal.h>

int yyparse(void);

void _RDB_flush_buf(void);

extern RDB_expression *yylval;
RDB_transaction *expr_txp;
const char *expr_in;
RDB_expression *resultp;
int expr_ret;

int yywrap(void) {
    return 1;
}

void yyerror(char *errtxt) {
    RDB_errmsg(expr_txp->dbp->dbrootp->envp, "%s", errtxt);
}

int RDB_parse_expr(const char *txt, RDB_transaction *txp, RDB_expression **exp)
{
    expr_txp = txp;
    expr_in = txt;
    expr_ret = RDB_OK;

    _RDB_flush_buf();
    if (yyparse() > 0) {
        if (expr_ret == RDB_OK) {
            /* syntax error */
            return RDB_INVALID_ARGUMENT;
        }
        return expr_ret;
    }

    *exp = resultp;
    return RDB_OK;
}

int RDB_parse_table(const char *txt, RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    RDB_expression *exp;

    ret = RDB_parse_expr(txt, txp, &exp);
    if (ret != RDB_OK)
        return ret;

    if (exp->kind != RDB_TABLE) {
        RDB_drop_expr(exp);
        return RDB_TYPE_MISMATCH;
    }

    *tbpp = exp->var.tbp;
    return RDB_OK;
}
