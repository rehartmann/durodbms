/* $Id$ */

#include <stdio.h>
#include "parse.h"
#include <rel/rdb.h>
#include <rel/internal.h>

int yyparse(void);
void yy_scan_string(const char *txt);
void _RDB_parse_free(void);
void _RDB_parse_remove_exp(RDB_expression *);
RDB_table *_RDB_parse_expr_to_table(const RDB_expression *exp);

RDB_transaction *_RDB_parse_txp;
RDB_expression *_RDB_parse_resultp;
int _RDB_parse_ret;
RDB_ltablefn *_RDB_parse_ltfp;
void *_RDB_parse_arg;

int yywrap(void) {
    return 1;
}

void yyerror(char *errtxt) {
    RDB_errmsg(_RDB_parse_txp->dbp->dbrootp->envp, "%s", errtxt);
}

int
RDB_parse_expr(const char *txt, RDB_ltablefn *lt_fp, void *lt_arg,
        RDB_transaction *txp, RDB_expression **expp)
{
    int ret;

    _RDB_parse_txp = txp;
    _RDB_parse_ret = RDB_OK;
    _RDB_parse_ltfp = lt_fp;
    _RDB_parse_arg = lt_arg;

    yy_scan_string(txt);
    ret = yyparse();
    if (ret > 0) {
        _RDB_parse_free();
        if (_RDB_parse_ret == RDB_OK) {
            /* syntax error */
            return RDB_SYNTAX;
        }
        return _RDB_parse_ret;
    }

    /* If the expression represents an attribute, try to get table */
    if (_RDB_parse_resultp->kind == RDB_EX_ATTR) {
        RDB_table *tbp = _RDB_parse_expr_to_table(_RDB_parse_resultp);

        if (tbp != NULL) {
            *expp = RDB_table_to_expr(tbp);
        } else {
            *expp = _RDB_parse_resultp;
            _RDB_parse_remove_exp(_RDB_parse_resultp);
        }
    } else {
        *expp = _RDB_parse_resultp;
        _RDB_parse_remove_exp(_RDB_parse_resultp);
    }
    _RDB_parse_free();
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
        RDB_object obj;

        RDB_init_obj(&obj);
        ret = RDB_evaluate(exp, NULL, txp, &obj);
        RDB_drop_expr(exp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&obj);
            return ret;
        }
        if (obj.kind != RDB_OB_TABLE) {
            RDB_destroy_obj(&obj);
            return RDB_TYPE_MISMATCH;
        }
        *tbpp = obj.var.tbp;
        obj.var.tbp = NULL;
        RDB_destroy_obj(&obj);            
        return RDB_OK;
    }

    *tbpp = exp->var.obj.var.tbp;
    return RDB_OK;
}
