/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>
#include <regex.h>

RDB_bool
RDB_expr_is_const(const RDB_expression *exp)
{
    return (RDB_bool) exp->kind == RDB_CONST;
}

RDB_type *
RDB_expr_type(const RDB_expression *exp)
{
    switch (exp->kind) {
        case RDB_CONST:
            return exp->var.const_val.typ;
        case RDB_ATTR:
            return exp->var.attr.typ;
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
            return RDB_expr_type(exp->var.op.arg1);
        case RDB_OP_STRLEN:
            return &RDB_INTEGER;
        case RDB_OP_GET_COMP:
            return _RDB_get_icomp(RDB_expr_type(exp->var.op.arg1),
                    exp->var.op.name)->typ;
        case RDB_TABLE:
            return exp->var.tbp->typ;
    }
    abort();
}

RDB_expression *
RDB_bool_const(RDB_bool v)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_CONST;
    exp->var.const_val.typ = &RDB_BOOLEAN;
    exp->var.const_val.kind = _RDB_BOOL;
    exp->var.const_val.var.bool_val = v;

    return exp;
}

RDB_expression *
RDB_int_const(RDB_int v)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_CONST;
    exp->var.const_val.typ = &RDB_INTEGER;
    exp->var.const_val.kind = _RDB_INT;
    exp->var.const_val.var.int_val = v;

    return exp;
}

RDB_expression *
RDB_rational_const(RDB_rational v)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_CONST;
    exp->var.const_val.typ = &RDB_RATIONAL;
    exp->var.const_val.kind = _RDB_RATIONAL;
    exp->var.const_val.var.rational_val = v;

    return exp;
}

RDB_expression *
RDB_string_const(const char *v)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_CONST;
    exp->var.const_val.typ = &RDB_STRING;
    exp->var.const_val.kind = _RDB_BIN;
    exp->var.const_val.var.bin.datap = RDB_dup_str(v);
    exp->var.const_val.var.bin.len = strlen(v)+1;

    return exp;
}

RDB_expression *
RDB_value_const(const RDB_value *valp)
{
    int ret;
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_CONST;
    RDB_init_value(&exp->var.const_val);
    ret = RDB_copy_value(&exp->var.const_val, valp);
    if (ret != RDB_OK) {
        free(exp);
        return NULL;
    }
    return exp;
}    

RDB_expression *
RDB_expr_attr(const char *attrname, RDB_type *typ)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_ATTR;
    exp->var.attr.typ = typ;
    exp->var.attr.name = RDB_dup_str(attrname);
    
    return exp;
}

RDB_expression *
_RDB_create_unexpr(RDB_expression *arg, enum _RDB_expr_kind kind)
{
    RDB_expression *exp;

    if (arg == NULL)
        return NULL;

    exp = malloc(sizeof (RDB_expression));
    if (exp == NULL)
        return NULL;
        
    exp->kind = kind;
    exp->var.op.arg1 = arg;

    return exp;
}

RDB_expression *
_RDB_create_binexpr(RDB_expression *arg1, RDB_expression *arg2, enum _RDB_expr_kind kind)
{
    RDB_expression *exp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exp = malloc(sizeof (RDB_expression));
    if (exp == NULL)
        return NULL;
        
    exp->kind = kind;
    exp->var.op.arg1 = arg1;
    exp->var.op.arg2 = arg2;

    return exp;
}

RDB_expression *
RDB_dup_expr(const RDB_expression *exp)
{
    int ret;
    RDB_expression *newexp = malloc(sizeof (RDB_expression));
    
    if (newexp == NULL)
        return NULL;   
    newexp->kind = exp->kind;

    switch (exp->kind) {
        case RDB_CONST:
            RDB_init_value(&newexp->var.const_val);
            ret = RDB_copy_value(&newexp->var.const_val, &exp->var.const_val);
            if (ret != RDB_OK) {
                free(newexp);
                return NULL;
            }
            return newexp;
        case RDB_ATTR:
            return RDB_expr_attr(exp->var.attr.name, RDB_expr_type(exp));
        case RDB_OP_NOT:
        case RDB_OP_STRLEN:
        case RDB_OP_REL_IS_EMPTY:
            return _RDB_create_unexpr(RDB_dup_expr(exp->var.op.arg1), exp->kind);
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
                
                ex1p = RDB_dup_expr(exp->var.op.arg1);
                if (ex1p == NULL)
                    return NULL;
                ex2p = RDB_dup_expr(exp->var.op.arg2);
                if (ex2p == NULL) {
                    RDB_drop_expr(ex1p);
                    return NULL;
                }
                return _RDB_create_binexpr(ex1p, ex2p, exp->kind);
            }
        case RDB_TABLE:
            return RDB_rel_table(exp->var.tbp);
        case RDB_OP_GET_COMP:
            return RDB_get_comp(exp->var.op.arg1, exp->var.op.name);
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
    RDB_expression *exp;

    if (arg == NULL)
        return NULL;

    exp = malloc(sizeof (RDB_expression));  
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_OP_NOT;
    exp->var.op.arg1 = arg;

    return exp;
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
    RDB_expression *exp;

    exp = malloc(sizeof (RDB_expression));  
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_TABLE;
    exp->var.tbp = tbp;
    
    return exp;
}

RDB_expression *
RDB_rel_is_empty(RDB_expression *arg1)
{
    return _RDB_create_unexpr(arg1, RDB_OP_REL_IS_EMPTY);
}

RDB_expression *
RDB_get_comp(RDB_expression *arg, const char *compname)
{
    RDB_expression *exp;

    /* check if compname is correct */
    if (_RDB_get_icomp(RDB_expr_type(arg), compname) == NULL)
        return NULL;
    
    exp = _RDB_create_unexpr(arg, RDB_OP_GET_COMP);
    if (exp == NULL)
        return NULL;

    exp->var.op.name = RDB_dup_str(compname);
    if (exp->var.op.name == NULL) {
        RDB_drop_expr(exp);
        return NULL;
    }
    return exp;
}

/* Destroy the expression and all subexpressions */
void 
RDB_drop_expr(RDB_expression *exp)
{
    switch (exp->kind) {
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
            RDB_drop_expr(exp->var.op.arg2);
        case RDB_OP_NOT:
        case RDB_OP_REL_IS_EMPTY:
        case RDB_OP_STRLEN:
            RDB_drop_expr(exp->var.op.arg1);
            break;
        case RDB_OP_GET_COMP:
            free(exp->var.op.name);
            RDB_drop_expr(exp->var.op.arg1);
            break;
        case RDB_CONST:
            RDB_destroy_value(&exp->var.const_val);
            break;
        default: ;
    }
    free(exp);
}

static int
evaluate_string(RDB_expression *exp, const RDB_tuple *tup,
                    RDB_transaction *txp, char **resp)
{
    RDB_value val;
    char *strp = NULL;

    switch (exp->kind) {
        case RDB_CONST:
            strp = exp->var.const_val.var.bin.datap;
            break;
        case RDB_ATTR:
            strp = RDB_tuple_get_string(tup, exp->var.attr.name);
            break;
        case RDB_OP_GET_COMP:
        {
            int ret;

            RDB_init_value(&val);
            ret = RDB_evaluate(exp, tup, txp, &val);
            if (ret != RDB_OK) {
                RDB_destroy_value(&val);
                return ret;
            }
            strp = val.var.bin.datap;
        }
        default: ;
    }
    *resp = RDB_dup_str(strp);
    if (resp == NULL)
        return RDB_NO_MEMORY;
    if (exp->kind == RDB_OP_GET_COMP)
        RDB_destroy_value(&val);
    return RDB_OK;
}

static int
evaluate_int(RDB_expression *exp, const RDB_tuple *tup,
                 RDB_transaction *txp, RDB_int *resp)
{
    switch (exp->kind) {
        case RDB_CONST:
            *resp = exp->var.const_val.var.int_val;
            break;
        case RDB_ATTR:
            *resp = RDB_tuple_get_int(tup, exp->var.attr.name);
            break;
        case RDB_OP_ADD:
            {
                int err;
                RDB_int v1, v2;

                err = evaluate_int(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = v1 + v2;
            }
            break;
        case RDB_OP_STRLEN:
        {
           int err;
           char *str;

           err = evaluate_string(exp->var.op.arg1, tup, txp, &str);
           if (err != RDB_OK)
               return err;
           *resp = strlen(str);
           free(str);
           break;
        }           
        case RDB_OP_GET_COMP:
        {
            RDB_value val;
            int ret;

            RDB_init_value(&val);
            ret = RDB_evaluate(exp, tup, txp, &val);
            if (ret != RDB_OK) {
                RDB_destroy_value(&val);
                return ret;
            }
            *resp = val.var.int_val;
            RDB_destroy_value(&val);
            return RDB_OK;
        }
        default: ;
    }
    return RDB_OK;
}

static int
evaluate_rational(RDB_expression *exp, const RDB_tuple *tup,
                 RDB_transaction *txp, RDB_rational *resp)
{
    switch (exp->kind) {
        case RDB_CONST:
            *resp = exp->var.const_val.var.rational_val;
            break;
        case RDB_ATTR:
            *resp = RDB_tuple_get_rational(tup, exp->var.attr.name);
            break;
        case RDB_OP_ADD:
        {
            int err;
            RDB_rational v1, v2;

            err = evaluate_rational(exp->var.op.arg1, tup, txp, &v1);
            if (err != RDB_OK)
                return err;

            err = evaluate_rational(exp->var.op.arg2, tup, txp, &v2);
            if (err != RDB_OK)
                return err;

            *resp = v1 + v2;
            break;
        }
        case RDB_OP_GET_COMP:
        {
            RDB_value val;
            int ret;

            RDB_init_value(&val);
            ret = RDB_evaluate(exp, tup, txp, &val);
            if (ret != RDB_OK) {
                RDB_destroy_value(&val);
                return ret;
            }
            *resp = val.var.rational_val;
            RDB_destroy_value(&val);
            return RDB_OK;
        }
        default: ;
    }
    return RDB_OK;
}

int
RDB_evaluate_bool(RDB_expression *exp, const RDB_tuple *tup,
                  RDB_transaction *txp, RDB_bool *resp)
{
    RDB_type *typ;
    int err;

    switch (exp->kind) {
        case RDB_CONST:
            *resp = exp->var.const_val.var.bool_val;
            return RDB_OK;
        case RDB_ATTR:
            *resp = RDB_tuple_get_bool(tup, exp->var.attr.name);
            return RDB_OK;
        case RDB_OP_EQ:
            typ = RDB_expr_type(exp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 == v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 == v2);
                return RDB_OK;
            }
            if (typ == &RDB_STRING) {
                char *s1, *s2;

                err = evaluate_string(exp->var.op.arg1, tup, txp, &s1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_string(exp->var.op.arg2, tup, txp, &s2);
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
            err = RDB_evaluate_bool(exp, tup, txp, resp);
            if (err != RDB_OK)
               return err;
            *resp = (RDB_bool) !*resp;
            return RDB_OK;
            break;
        case RDB_OP_LT:
            typ = RDB_expr_type(exp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 < v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 < v2);
                return RDB_OK;
            }
        case RDB_OP_GT:
            typ = RDB_expr_type(exp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 > v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 > v2);
                return RDB_OK;
            }
        case RDB_OP_LET:
            typ = RDB_expr_type(exp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 <= v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 <= v2);
                return RDB_OK;
            }
        case RDB_OP_GET:
            typ = RDB_expr_type(exp->var.op.arg1);
            if (typ == &RDB_INTEGER) {
                RDB_int v1, v2;
                
                err = evaluate_int(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 >= v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = evaluate_rational(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_rational(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 >= v2);
                return RDB_OK;
            }
        case RDB_OP_AND:
            {
                RDB_bool v1, v2;
            
                err = RDB_evaluate_bool(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_bool(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 && v2);
            }
            return RDB_OK;                
        case RDB_OP_OR:
            {
                RDB_bool v1, v2;
            
                err = RDB_evaluate_bool(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_bool(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 && v2);
            }
            return RDB_OK;
        case RDB_OP_NOT:
            {
                RDB_bool v;
            
                err = RDB_evaluate_bool(exp->var.op.arg1, tup, txp, &v);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)!v;
            }
            return RDB_OK;
        case RDB_OP_REGMATCH:
            {
                regex_t reg;
                char *s1, *s2;

                err = evaluate_string(exp->var.op.arg1, tup, txp, &s1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_string(exp->var.op.arg2, tup, txp, &s2);
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
            RDB_table *tbp = exp->var.tbp;
        
            *resp = RDB_table_is_empty(tbp, txp, resp);
            return RDB_table_name(tbp) == NULL ?
                    RDB_drop_table(tbp, txp) : RDB_OK;
        }
        case RDB_OP_GET_COMP:
        {
            RDB_value val;
            int ret;

            RDB_init_value(&val);
            ret = RDB_evaluate(exp, tup, txp, &val);
            if (ret != RDB_OK) {
                RDB_destroy_value(&val);
                return ret;
            }
            *resp = val.var.bool_val;
            RDB_destroy_value(&val);
            return RDB_OK;
        }
        default: ;
    }
    abort();
}

int
RDB_evaluate(RDB_expression *exp, const RDB_tuple *tup, RDB_transaction *txp,
            RDB_value *valp)
{
    RDB_type *typ = RDB_expr_type(exp);

    if (typ != NULL) {
        switch (exp->kind) {
            case RDB_OP_GET_COMP:
                {
                    int ret;
                    RDB_value val;

                    RDB_init_value(&val);
                    ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val);
                    if (ret != RDB_OK) {
                         RDB_destroy_value(&val);
                         return ret;
                    }
                    ret = RDB_value_get_comp(&val, exp->var.op.name, valp);
                    RDB_destroy_value(&val);
                    return ret;
                }
            case RDB_ATTR:
                return RDB_copy_value(valp, RDB_tuple_get(tup, exp->var.attr.name));
            default: ;
        }
        _RDB_set_value_type(valp, typ);
        if (typ == &RDB_BOOLEAN)
            return RDB_evaluate_bool(exp, tup, txp, &valp->var.bool_val);
        if (typ == &RDB_INTEGER)
            return evaluate_int(exp, tup, txp, &valp->var.int_val);
        if (typ == &RDB_RATIONAL)
            return evaluate_rational(exp, tup, txp, &valp->var.rational_val);
        if (typ == &RDB_STRING) {
            char *str;
            int ret;
                
            ret = evaluate_string(exp, tup, txp, &str);
            if (ret != RDB_OK)
                return ret;

            valp->var.bin.datap = str;
            valp->var.bin.len = strlen(valp->var.bin.datap) + 1;

            return RDB_OK;
        }
        if (typ->kind == RDB_TP_RELATION) {
            valp->var.tbp = exp->var.tbp;
            return RDB_OK;
        }
    }
    abort();
}
