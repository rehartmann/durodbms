/* $Id$ */

#include <stdio.h>
#include "parse.h"
#include <rel/rdb.h>
#include <rel/internal.h>

int yyparse(void);
void yy_scan_string(const char *txt);

RDB_transaction *expr_txp;
RDB_expression *resultp;
int expr_ret;
RDB_ltablefn *expr_ltfp;
void *expr_arg;

int yywrap(void) {
    return 1;
}

void yyerror(char *errtxt) {
    RDB_errmsg(expr_txp->dbp->dbrootp->envp, "%s", errtxt);
}

int
RDB_parse_expr(const char *txt, RDB_ltablefn *lt_fp, void *lt_arg,
        RDB_transaction *txp, RDB_expression **exp)
{
    expr_txp = txp;
    expr_ret = RDB_OK;
    expr_ltfp = lt_fp;
    expr_arg = lt_arg;

    yy_scan_string(txt);
    if (yyparse() > 0) {
        if (expr_ret == RDB_OK) {
            /* syntax error */
            return RDB_SYNTAX;
        }
        return expr_ret;
    }

    *exp = resultp;
    return RDB_OK;
}

int
RDB_parse_table(const char *txt, RDB_ltablefn *lt_fp, void *lt_arg,
        RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    RDB_expression *exp;

    ret = RDB_parse_expr(txt, lt_fp, lt_arg, txp, &exp);
    if (ret != RDB_OK)
        return ret;

    if (exp->kind != RDB_EX_OBJ || exp->var.obj.kind != RDB_OB_TABLE) {
        RDB_drop_expr(exp);
        return RDB_TYPE_MISMATCH;
    }

    *tbpp = exp->var.obj.var.tbp;
    return RDB_OK;
}
