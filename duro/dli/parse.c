/* $Id$ */

#include <stdio.h>
#include "parse.h"
#include <rel/rdb.h>
#include <rel/internal.h>

int yyparse(void);

extern RDB_expression *yylval;
RDB_transaction *expr_txp;
const char *expr_in;
RDB_expression *resultp;

int yywrap(void) {
    return 1;
}

void yyerror(char *errtxt) {
    FILE *errfp = expr_txp->dbp->dbrootp->envp->errfilep;

    if (errfp != NULL)
        fputs(errtxt, errfp);
}

int RDB_parse_expr(const char *txt, RDB_transaction *txp, RDB_expression **exp)
{
    int ret;

    expr_txp = txp;
    expr_in = txt;

    ret = yyparse();
    if (ret > 0)
        return RDB_INVALID_ARGUMENT;

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
