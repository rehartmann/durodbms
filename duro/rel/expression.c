/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>
#include <regex.h>

RDB_bool
RDB_expr_is_const(const RDB_expression *exprp)
{
    return exprp->kind == RDB_CONST;
}

RDB_type *
RDB_expr_type(const RDB_expression *exprp)
{
    switch (exprp->kind) {
        case RDB_CONST:
            return exprp->var.const_val.typ;
        case RDB_ATTR:
            return exprp->var.attr.typ;
        case RDB_OP_NOT:
        case RDB_OP_EQ:
        case RDB_OP_NEQ:
        case RDB_OP_LT:
        case RDB_OP_GT:
        case RDB_OP_LET:
        case RDB_OP_GET:
        case RDB_OP_AND:
        case RDB_OP_OR:
        case RDB_OP_REGMATCH:
        case RDB_OP_REL_IS_EMPTY:
            return &RDB_BOOLEAN;
        case RDB_OP_ADD:
        case RDB_OP_SUBTRACT:
            return RDB_expr_type(exprp->var.op.arg1);
        case RDB_OP_STRLEN:
            return &RDB_INTEGER;
        case RDB_TABLE:
            return exprp->var.tbp->typ;
    }
    abort();
}

RDB_expression *
RDB_bool_const(RDB_bool v)
{
    RDB_expression *exprp = malloc(sizeof (RDB_expression));
    
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_CONST;
    exprp->var.const_val.typ = &RDB_BOOLEAN;
    exprp->var.const_val.var.bool_val = v;

    return exprp;
}

RDB_expression *
RDB_int_const(RDB_int v)
{
    RDB_expression *exprp = malloc(sizeof (RDB_expression));
    
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_CONST;
    exprp->var.const_val.typ = &RDB_INTEGER;
    exprp->var.const_val.var.int_val = v;

    return exprp;
}

RDB_expression *
RDB_rational_const(RDB_rational v)
{
    RDB_expression *exprp = malloc(sizeof (RDB_expression));
    
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_CONST;
    exprp->var.const_val.typ = &RDB_RATIONAL;
    exprp->var.const_val.var.rational_val = v;

    return exprp;
}

RDB_expression *
RDB_string_const(const char *v)
{
    RDB_expression *exprp = malloc(sizeof (RDB_expression));
    
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_CONST;
    exprp->var.const_val.typ = &RDB_STRING;
    exprp->var.const_val.var.bin.datap = RDB_dup_str(v);
    exprp->var.const_val.var.bin.len = strlen(v)+1;

    return exprp;
}

RDB_expression *
RDB_value_const(const RDB_value *valp)
{
    int ret;
    RDB_expression *exprp = malloc(sizeof (RDB_expression));
    
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_CONST;
    RDB_init_value(&exprp->var.const_val);
    ret = RDB_copy_value(&exprp->var.const_val, valp);
    if (ret != RDB_OK) {
        free(exprp);
        return NULL;
    }
    return exprp;
}    

RDB_expression *
RDB_expr_attr(const char *attrname, RDB_type *typ)
{
    RDB_expression *exprp = malloc(sizeof (RDB_expression));
    
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_ATTR;
    exprp->var.attr.typ = typ;
    exprp->var.attr.name = RDB_dup_str(attrname);
    
    return exprp;
}

RDB_expression *
_RDB_create_unexpr(RDB_expression *arg, enum _RDB_expr_kind kind)
{
    RDB_expression *exprp;

    if (arg == NULL)
        return NULL;

    exprp = malloc(sizeof (RDB_expression));
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = kind;
    exprp->var.op.arg1 = arg;

    return exprp;
}

RDB_expression *
_RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2, enum _RDB_expr_kind kind)
{
    RDB_expression *exprp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = kind;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.arg2 = arg2;

    return exprp;
}

RDB_expression *
RDB_dup_expr(const RDB_expression *exprp)
{
    int ret;
    RDB_expression *newexprp = malloc(sizeof (RDB_expression));
    
    if (newexprp == NULL)
        return NULL;   
    newexprp->kind = exprp->kind;

    switch (exprp->kind) {
        case RDB_CONST:
            RDB_init_value(&newexprp->var.const_val);
            ret = RDB_copy_value(&newexprp->var.const_val, &exprp->var.const_val);
            if (ret != RDB_OK) {
                free(newexprp);
                return NULL;
            }
            return newexprp;
        case RDB_ATTR:
            return RDB_expr_attr(exprp->var.attr.name, RDB_expr_type(exprp));
        case RDB_OP_NOT:
        case RDB_OP_STRLEN:
        case RDB_OP_REL_IS_EMPTY:
            return _RDB_create_unexpr(RDB_dup_expr(exprp->var.op.arg1), exprp->kind);
        case RDB_OP_EQ:
        case RDB_OP_NEQ:
        case RDB_OP_LT:
        case RDB_OP_GT:
        case RDB_OP_LET:
        case RDB_OP_GET:
        case RDB_OP_AND:
        case RDB_OP_OR:
        case RDB_OP_ADD:
        case RDB_OP_SUBTRACT:
        case RDB_OP_REGMATCH:
            {
                RDB_expression *ex1p, *ex2p;
                
                ex1p = RDB_dup_expr(exprp->var.op.arg1);
                if (ex1p == NULL)
                    return NULL;
                ex2p = RDB_dup_expr(exprp->var.op.arg2);
                if (ex2p == NULL) {
                    RDB_drop_expr(ex1p);
                    return NULL;
                }
                return _RDB_create_binexpr(ex1p, ex2p, exprp->kind);
            }
        case RDB_TABLE:
            return RDB_rel_table(exprp->var.tbp);
    }
    abort();
}

RDB_expression *
RDB_eq(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_EQ);
}

RDB_expression *
RDB_neq(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_NEQ);
}

RDB_expression *
RDB_lt(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_LT);
}

RDB_expression *
RDB_gt(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_GT);
}

RDB_expression *
RDB_let(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_LET);
}

RDB_expression *
RDB_get(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_GET);
}

RDB_expression *
RDB_and(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_AND);
}

RDB_expression *
RDB_or(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_OR);
}    

RDB_expression *
RDB_not(RDB_expression *arg)
{
    RDB_expression *exprp;

    if (arg == NULL)
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_NOT;
    exprp->var.op.arg1 = arg;

    return exprp;
}    

RDB_expression *
RDB_add(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_ADD);
}

RDB_expression *
RDB_subtract(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_SUBTRACT);
}

RDB_expression *
RDB_strlen(RDB_expression *arg)
{
    return _RDB_create_unexpr(arg, RDB_OP_STRLEN);
}

RDB_expression *
RDB_regmatch(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_OP_REGMATCH);
}

RDB_expression *
RDB_rel_table(RDB_table *tbp) {
    RDB_expression *exprp;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_TABLE;
    exprp->var.tbp = tbp;
    
    return exprp;
}

RDB_expression *
RDB_rel_is_empty(RDB_expression *arg1)
{
    RDB_expression *exprp;

    if (arg1 == NULL)
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_REL_IS_EMPTY;
    exprp->var.op.arg1 = arg1;

    return exprp;
}

/* Destroy the expression and all subexpressions */
void 
RDB_drop_expr(RDB_expression *exprp)
{
    switch (exprp->kind) {
        case RDB_OP_EQ:
        case RDB_OP_NEQ:
        case RDB_OP_LT:
        case RDB_OP_GT:
        case RDB_OP_LET:
        case RDB_OP_GET:
        case RDB_OP_AND:
        case RDB_OP_OR:
        case RDB_OP_ADD:
        case RDB_OP_REGMATCH:
            RDB_drop_expr(exprp->var.op.arg2);
        case RDB_OP_NOT:
        case RDB_OP_REL_IS_EMPTY:
        case RDB_OP_STRLEN:
            RDB_drop_expr(exprp->var.op.arg1);
            break;
        case RDB_CONST:
            RDB_destroy_value(&exprp->var.const_val);
            break;
        default: ;
    }
    free(exprp);
}

static int
evaluate_string(RDB_expression *exprp, const RDB_tuple *tup,
                    RDB_transaction *txp, char **resp)
{
    char *strp = NULL;
    switch (exprp->kind) {
        case RDB_CONST:
            strp = exprp->var.const_val.var.bin.datap;
            break;
        case RDB_ATTR:
            strp = RDB_tuple_get_string(tup, exprp->var.attr.name);
            break;
        default: ;
    }
    *resp = RDB_dup_str(strp);
    if (resp == NULL)
        return RDB_NO_MEMORY;
    return RDB_OK;
}

static int
evaluate_int(RDB_expression *exprp, const RDB_tuple *tup,
                 RDB_transaction *txp, RDB_int *resp)
{
    switch (exprp->kind) {
        case RDB_CONST:
            *resp = exprp->var.const_val.var.int_val;
            break;
        case RDB_ATTR:
            *resp = RDB_tuple_get_int(tup, exprp->var.attr.name);
            break;
        case RDB_OP_ADD:
            {
                int err;
                RDB_int v1, v2;

                err = evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = v1 + v2;
                break;
            }
        case RDB_OP_STRLEN:
        {
           int err;
           char *str;

           err = evaluate_string(exprp->var.op.arg1, tup, txp, &str);
           if (err != RDB_OK)
               return err;
           *resp = strlen(str);
           free(str);
        }           
        default: ;
    }
    return RDB_OK;
}

static int
evaluate_rational(RDB_expression *exprp, const RDB_tuple *tup,
                 RDB_transaction *txp, RDB_rational *resp)
{
    switch (exprp->kind) {
        case RDB_CONST:
            *resp = exprp->var.const_val.var.rational_val;
            break;
        case RDB_ATTR:
            *resp = RDB_tuple_get_rational(tup, exprp->var.attr.name);
            break;
        case RDB_OP_ADD:
        {
            int err;
            RDB_rational v1, v2;

            err = evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
            if (err != RDB_OK)
                return err;

            err = evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
            if (err != RDB_OK)
                return err;

            *resp = v1 + v2;
            break;
        }
        default: ;
    }
    return RDB_OK;
}

int
RDB_evaluate_bool(RDB_expression *exprp, const RDB_tuple *tup,
                  RDB_transaction *txp, RDB_bool *resp)
{
    RDB_type *typ;
    int err;

    switch (exprp->kind) {
        case RDB_CONST:
            *resp = exprp->var.const_val.var.bool_val;
            return RDB_OK;
        case RDB_ATTR:
            *resp = RDB_tuple_get_bool(tup, exprp->var.attr.name);
            return RDB_OK;
        case RDB_OP_EQ:
            typ = RDB_expr_type(exprp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 == v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 == v2);
                return RDB_OK;
            }
            if (typ == &RDB_STRING) {
                char *s1, *s2;

                err = evaluate_string(exprp->var.op.arg1, tup, txp, &s1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_string(exprp->var.op.arg2, tup, txp, &s2);
                if (err != RDB_OK) {
                    free(s1);
                    return err;
                }
                    
                *resp = (RDB_bool) (strcmp (s1, s2) == 0);
                free(s1);
                free(s2);
                return RDB_OK;
            }
            return RDB_TYPE_MISMATCH;
            break;
        case RDB_OP_NEQ:
            err = RDB_evaluate_bool(exprp, tup, txp, resp);
            if (err != RDB_OK)
               return err;
            *resp = (RDB_bool) !*resp;
            return RDB_OK;
            break;
        case RDB_OP_LT:
            typ = RDB_expr_type(exprp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 < v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 < v2);
                return RDB_OK;
            }
        case RDB_OP_GT:
            typ = RDB_expr_type(exprp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 > v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 > v2);
                return RDB_OK;
            }
        case RDB_OP_LET:
            typ = RDB_expr_type(exprp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 <= v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 <= v2);
                return RDB_OK;
            }
        case RDB_OP_GET:
            typ = RDB_expr_type(exprp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 >= v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 >= v2);
                return RDB_OK;
            }
        case RDB_OP_AND:
            {
                RDB_bool v1, v2;
            
                err = RDB_evaluate_bool(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_bool(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 && v2);
            }
            return RDB_OK;                
        case RDB_OP_OR:
            {
                RDB_bool v1, v2;
            
                err = RDB_evaluate_bool(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_bool(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 && v2);
            }
            return RDB_OK;
        case RDB_OP_NOT:
            {
                RDB_bool v;
            
                err = RDB_evaluate_bool(exprp->var.op.arg1, tup, txp, &v);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)!v;
            }
            return RDB_OK;
        case RDB_OP_REGMATCH:
            {
                regex_t reg;
                char *s1, *s2;

                err = evaluate_string(exprp->var.op.arg1, tup, txp, &s1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_string(exprp->var.op.arg2, tup, txp, &s2);
                if (err != RDB_OK) {
                    free(s1);
                    return err;
                }

                err = regcomp(&reg, s2, REG_NOSUB);
                if (err != 0) {
                    free(s1);
                    free(s2);
                    return RDB_INVALID_ARGUMENT;
                }
                *resp = (RDB_bool)(regexec(&reg, s1, 0, NULL, 0) == 0);
                regfree(&reg);
                free(s1);
                free(s2);
            }
            return RDB_OK;
        case RDB_OP_REL_IS_EMPTY:
        {
            RDB_table *tbp = exprp->var.tbp;
        
            *resp = RDB_table_is_empty(tbp, txp, resp);
            return RDB_table_name(tbp) == NULL ?
                    RDB_drop_table(tbp, txp) : RDB_OK;
        }
        default: ;
    }
    abort();
}

int
RDB_evaluate(RDB_expression *exprp, const RDB_tuple *tup, RDB_transaction *txp,
            RDB_value *valp)
{
    RDB_type *typ = RDB_expr_type(exprp);

    if (typ != NULL) {
        valp->typ = typ;
        if (typ == &RDB_BOOLEAN)
            return RDB_evaluate_bool(exprp, tup, txp, &valp->var.bool_val);
        if (typ == &RDB_INTEGER)
            return evaluate_int(exprp, tup, txp, &valp->var.int_val);
        if (typ == &RDB_RATIONAL)
            return evaluate_rational(exprp, tup, txp, &valp->var.rational_val);
        if (typ == &RDB_STRING) {
            char *str;
            int err;
                
            err = evaluate_string(exprp, tup, txp, &str);
            if (err != RDB_OK)
                return err;

            valp->var.bin.datap = str;
            valp->var.bin.len = strlen(valp->var.bin.datap) + 1;

            return RDB_OK;
        }
        if (typ->kind == RDB_TP_RELATION) {
            valp->var.tbp = exprp->var.tbp;
            return RDB_OK;
        }
    }
    abort();
}
