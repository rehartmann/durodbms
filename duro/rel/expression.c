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
        case RDB_SELECTOR:
            return exp->var.selector.typ;
        case RDB_OP_AGGREGATE:
            switch (exp->var.aggr.op) {
                case RDB_COUNT:
                    return &RDB_INTEGER;
                case RDB_AVG:
                    return &RDB_RATIONAL;
                default:
                    return _RDB_tuple_type_attr(
                            exp->var.aggr.tbp->typ->var.basetyp,
                            exp->var.aggr.attrname)->typ;
            }
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
RDB_aggregate_expr(RDB_table *tbp, RDB_aggregate_op op, const char *attrname)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));

    if (exp == NULL)
        return NULL;
    exp->kind = RDB_OP_AGGREGATE;
    exp->var.aggr.attrname = RDB_dup_str(attrname);
    if (exp->var.aggr.attrname == NULL) {
        free(exp);
        return NULL;
    }
    exp->var.aggr.tbp = tbp;
    exp->var.aggr.op = op;

    return exp;
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

RDB_expression *
RDB_selector(RDB_type *typ, const char *repname, RDB_expression *argv[])
{
    RDB_expression *exp;
    RDB_ipossrep *prp = _RDB_get_possrep(typ, repname);
    int i;

    if (prp == NULL)
        return NULL;
    exp = malloc(sizeof (RDB_expression));
    if (exp == NULL)
        return NULL;   

    exp->kind = RDB_SELECTOR;
    exp->var.selector.typ = typ;
    exp->var.selector.argv = NULL;

    exp->var.selector.name = RDB_dup_str(repname);
    if (exp->var.selector.name == NULL)
        goto error;

    exp->var.selector.argv = malloc(prp->compc * sizeof(RDB_expression *));
    if (exp->var.selector.argv == NULL)
        goto error;

    for (i = 0; i < prp->compc; i++)
        exp->var.selector.argv[i] = argv[i];

    return exp;
error:
    free(exp->var.selector.name);
    free(exp->var.selector.argv);
    free(exp);

    return NULL;    
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
        case RDB_SELECTOR:
            {
                int compc = _RDB_get_possrep(exp->var.selector.typ,
                        exp->var.selector.name)->compc;
                int i;

                for (i = 0; i < compc; i++)
                    RDB_drop_expr(exp->var.selector.argv[i]);
                free(exp->var.selector.argv);
                free(exp->var.selector.name);
            }
            break;
        case RDB_OP_AGGREGATE:
            free(exp->var.aggr.attrname);
            break;
        case RDB_CONST:
            RDB_destroy_value(&exp->var.const_val);
            break;
        case RDB_ATTR:
            free(exp->var.attr.name);
            break;
        default:
            abort();
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
        case RDB_OP_SUBTRACT:
            {
                int err;
                RDB_int v1, v2;

                err = evaluate_int(exp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = evaluate_int(exp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = v1 - v2;
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
        case RDB_OP_AGGREGATE:
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
        default:
            abort();
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
        case RDB_OP_AGGREGATE:
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

static int
evaluate_eq(RDB_expression *exp, const RDB_tuple *tup,
                  RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_value val1, val2;

    RDB_init_value(&val1);
    RDB_init_value(&val2);

    /*
     * Evaluate arguments
     */

    ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val1);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_evaluate(exp->var.op.arg2, tup, txp, &val2);
    if (ret != RDB_OK)
        goto cleanup;

    /*
     * Compare values
     */

    *resp = RDB_value_equals(&val1, &val2);

    ret = RDB_OK;

cleanup:
    RDB_destroy_value(&val1);
    RDB_destroy_value(&val2);

    return ret;
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
            return evaluate_eq(exp, tup, txp, resp);
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
        case RDB_OP_AGGREGATE:
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

static int evaluate_selector(RDB_expression *exp, const RDB_tuple *tup, RDB_transaction *txp,
            RDB_value *valp)
{
    int ret;
    int i;
    int compc = _RDB_get_possrep(exp->var.selector.typ, exp->var.selector.name)->compc;
    RDB_value **valpv;
    RDB_value *valv;

    valpv = malloc(compc * sizeof (RDB_value *));
    if (valpv == NULL)
        goto error;
    valv = malloc(compc * sizeof (RDB_value));
    if (valv == NULL)
        goto error;
    for (i = 0; i < compc; i++) {
        valpv[i] = &valv[i];
        RDB_init_value(&valv[i]);
        ret = RDB_evaluate(exp->var.selector.argv[i], tup, txp, &valv[i]);
        /* !! */
    }
    ret = RDB_select_value(valp, exp->var.selector.typ, exp->var.selector.name, valpv);
    for (i = 0; i < compc; i++) {
        RDB_destroy_value(&valv[i]);
    }
    free(valv);
    free(valpv);
    return ret;
error:
    /* !! */
    return RDB_NO_MEMORY;
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
            case RDB_SELECTOR:
                return evaluate_selector(exp, tup, txp, valp);
            case RDB_OP_AGGREGATE:
                return RDB_aggregate(exp->var.aggr.tbp, exp->var.aggr.op,
                        exp->var.aggr.attrname, txp, valp);
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

RDB_bool
_RDB_expr_refers(RDB_expression *exp, RDB_table *tbp)
{
    switch (exp->kind) {
        case RDB_CONST:
        case RDB_ATTR:
            return RDB_FALSE;
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
            return (RDB_bool) (_RDB_expr_refers(exp->var.op.arg1, tbp)
                    || _RDB_expr_refers(exp->var.op.arg2, tbp));
        case RDB_OP_NOT:
        case RDB_OP_REL_IS_EMPTY:
        case RDB_OP_STRLEN:
        case RDB_OP_GET_COMP:
            return _RDB_expr_refers(exp->var.op.arg1, tbp);
        case RDB_TABLE:
            return (RDB_bool) (tbp == exp->var.tbp);
        case RDB_SELECTOR:
        {
            int i;
            RDB_bool res = RDB_FALSE;
            int compc = _RDB_get_possrep(exp->var.selector.typ,
                    exp->var.selector.name)->compc;

            for (i = 0; i < compc; i++)
                res |= _RDB_expr_refers(exp->var.selector.argv[i], tbp);
            
            return res;
        }
        case RDB_OP_AGGREGATE:
            return (RDB_bool) (exp->var.aggr.tbp == tbp);
    }
    abort();
}
