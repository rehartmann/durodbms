/* $Id$ */

#include "rdb.h"
#include <gen/strfns.h>
#include <string.h>
#include "internal.h"

RDB_bool
RDB_expr_is_const(const RDB_expression *exprp)
{
    return (exprp->kind == RDB_CONST);
}

RDB_type *
RDB_expr_type(const RDB_expression *exprp)
{
    switch(exprp->kind) {
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
            return &RDB_BOOLEAN;
        case RDB_OP_ADD:
            return RDB_expr_type(exprp->var.op.arg1);
        default:
            return NULL;
    }
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
    int res;
    RDB_expression *exprp = malloc(sizeof (RDB_expression));
    
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_CONST;
    RDB_init_value(&exprp->var.const_val);
    res = RDB_copy_value(&exprp->var.const_val, valp);
    if (res != RDB_OK) {
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
RDB_dup_expr(const RDB_expression *exprp)
{
    int res;
    RDB_expression *newexprp = malloc(sizeof (RDB_expression));
    
    if (newexprp == NULL)
        return NULL;   
    newexprp->kind = exprp->kind;

    switch(exprp->kind) {
        case RDB_CONST:
            RDB_init_value(&newexprp->var.const_val);
            res = RDB_copy_value(&newexprp->var.const_val, &exprp->var.const_val);
            if (res != RDB_OK) {
                free(newexprp);
                return NULL;
            }
            return newexprp;
        case RDB_ATTR:
            return RDB_expr_attr(exprp->var.attr.name, RDB_expr_type(exprp));
        case RDB_OP_NOT:
            return RDB_not(exprp->var.op.arg1);
        case RDB_OP_EQ:
        case RDB_OP_NEQ:
        case RDB_OP_LT:
        case RDB_OP_GT:
        case RDB_OP_LET:
        case RDB_OP_GET:
        case RDB_OP_AND:
        case RDB_OP_OR:
        case RDB_OP_ADD:
            newexprp->var.op.arg1 = RDB_dup_expr(exprp->var.op.arg1);
            newexprp->var.op.arg2 = RDB_dup_expr(exprp->var.op.arg2);
            if (newexprp->var.op.arg1 == NULL
                    || newexprp->var.op.arg2 == NULL) {
                free(newexprp->var.op.arg1);
                free(newexprp->var.op.arg1);
                free(newexprp);
                return NULL;
            }
            return newexprp;
        default:
            return NULL;
    }
}

static RDB_expression *
create_binexpr(RDB_expression *arg1, RDB_expression *arg2, int kind)
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
RDB_eq(RDB_expression *arg1, RDB_expression *arg2)
{
    return create_binexpr(arg1, arg2, RDB_OP_EQ);
}

RDB_expression *
RDB_neq(RDB_expression *arg1, RDB_expression *arg2) {
    return create_binexpr(arg1, arg2, RDB_OP_NEQ);
}

RDB_expression *
RDB_lt(RDB_expression *arg1, RDB_expression *arg2) {
    return create_binexpr(arg1, arg2, RDB_OP_LT);
}

RDB_expression *
RDB_gt(RDB_expression *arg1, RDB_expression *arg2) {
    return create_binexpr(arg1, arg2, RDB_OP_GT);
}

RDB_expression *
RDB_let(RDB_expression *arg1, RDB_expression *arg2) {
    return create_binexpr(arg1, arg2, RDB_OP_LET);
}

RDB_expression *
RDB_get(RDB_expression *arg1, RDB_expression *arg2) {
    return create_binexpr(arg1, arg2, RDB_OP_GET);
}

RDB_expression *
RDB_and(RDB_expression *arg1, RDB_expression *arg2)
{
    RDB_expression *exprp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_AND;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.arg2 = arg2;

    return exprp;
}    

RDB_expression *
RDB_or(RDB_expression *arg1, RDB_expression *arg2)
{
    RDB_expression *exprp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_OR;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.arg2 = arg2;

    return exprp;
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
    RDB_expression *exprp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_ADD;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.arg2 = arg2;

    return exprp;
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
RDB_rel_select(RDB_expression *tbexprp, RDB_expression *condp)
{
    RDB_expression *exprp;

    if ((tbexprp == NULL) || (condp == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;

    exprp->kind = RDB_OP_REL_SELECT;
    exprp->var.op.arg1 = tbexprp;
    exprp->var.op.arg2 = condp;

    return exprp;
}

RDB_expression *
RDB_rel_union(RDB_expression *arg1, RDB_expression *arg2)
{
    RDB_expression *exprp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_REL_UNION;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.arg2 = arg2;

    return exprp;
}

RDB_expression *
RDB_rel_minus(RDB_expression *arg1, RDB_expression *arg2)
{
    RDB_expression *exprp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_REL_MINUS;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.arg2 = arg2;

    return exprp;
}

RDB_expression *
RDB_rel_intersect(RDB_expression *arg1, RDB_expression *arg2)
{
    RDB_expression *exprp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_REL_INTERSECT;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.arg2 = arg2;

    return exprp;
}

/* Perform a natural join of the two tables. */
RDB_expression *
RDB_rel_join(RDB_expression *arg1, RDB_expression *arg2)
{
    RDB_expression *exprp;

    if ((arg1 == NULL) || (arg2 == NULL))
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_REL_JOIN;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.arg2 = arg2;

    return exprp;
}

RDB_expression *
RDB_rel_extend(RDB_expression *arg1, int attrc, RDB_virtual_attr attrv[])
{
    RDB_expression *exprp;
    int i;

    if (arg1 == NULL)
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_REL_EXTEND;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.attrc = attrc;
    exprp->var.op.attrv = malloc(sizeof (RDB_virtual_attr) * attrc);
    for (i = 0; i < attrc; i++) {
        exprp->var.op.attrv[i].name = RDB_dup_str(attrv[i].name);
        exprp->var.op.attrv[i].value = RDB_dup_expr(attrv[i].value);
    }
    return exprp;
}

RDB_expression *
RDB_rel_project(RDB_expression *arg1, int attrc, char *attrv[])
{
    RDB_expression *exprp;
    int i;

    if (arg1 == NULL)
        return NULL;

    exprp = malloc(sizeof (RDB_expression));  
    if (exprp == NULL)
        return NULL;
        
    exprp->kind = RDB_OP_REL_PROJECT;
    exprp->var.op.arg1 = arg1;
    exprp->var.op.attrc = attrc;
    exprp->var.op.attrv = malloc(sizeof (RDB_virtual_attr) * attrc);
    for (i = 0; i < attrc; i++) {
        exprp->var.op.attrv[i].name = RDB_dup_str(attrv[i]);
    }
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
    int i;

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
        case RDB_OP_REL_SELECT:
        case RDB_OP_REL_UNION:
        case RDB_OP_REL_MINUS:
        case RDB_OP_REL_INTERSECT:
            RDB_drop_expr(exprp->var.op.arg2);
        case RDB_OP_NOT:
        case RDB_OP_REL_IS_EMPTY:
            RDB_drop_expr(exprp->var.op.arg1);
            break;
        case RDB_OP_REL_JOIN:
            RDB_drop_expr(exprp->var.op.arg1);
            RDB_drop_expr(exprp->var.op.arg2);
            break;
        case RDB_OP_REL_EXTEND:
            RDB_drop_expr(exprp->var.op.arg1);
            for (i = 0; i < exprp->var.op.attrc; i++) {
                free(exprp->var.op.attrv[i].name);
                RDB_drop_expr(exprp->var.op.attrv[i].value);
            }
            break;
        case RDB_OP_REL_PROJECT:
            RDB_drop_expr(exprp->var.op.arg1);
            for (i = 0; i < exprp->var.op.attrc; i++) {
                free(exprp->var.op.attrv[i].name);
            }
            break;       
        case RDB_CONST:
            RDB_deinit_value(&exprp->var.const_val);
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

int
RDB_evaluate_bool(RDB_expression *exprp, const RDB_tuple *tup,
                  RDB_transaction *txp, RDB_bool *resp)
{
    RDB_type *typ = RDB_expr_type(exprp->var.op.arg1);
    int err;

    switch (exprp->kind) {
        case RDB_OP_EQ:
            if (typ == &RDB_INTEGER) {
                int v1, v2;
                
                err = RDB_evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 == v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = RDB_evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
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
            if (typ == &RDB_INTEGER) {
                int v1, v2;
                
                err = RDB_evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 < v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = RDB_evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 < v2);
                return RDB_OK;
            }
        case RDB_OP_GT:
            if (typ == &RDB_INTEGER) {
                int v1, v2;
                
                err = RDB_evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 > v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = RDB_evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 > v2);
                return RDB_OK;
            }
        case RDB_OP_LET:
            if (typ == &RDB_INTEGER) {
                int v1, v2;
                
                err = RDB_evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 <= v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = RDB_evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 <= v2);
                return RDB_OK;
            }
        case RDB_OP_GET:
            if (typ == &RDB_INTEGER) {
                int v1, v2;
                
                err = RDB_evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
                if (err != RDB_OK)
                    return err;

                *resp = (RDB_bool)(v1 >= v2);
                return RDB_OK;
            }
            if (typ == &RDB_RATIONAL) {
                RDB_rational v1, v2;
                
                err = RDB_evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
                if (err != RDB_OK)
                    return err;

                err = RDB_evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
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
        case RDB_CONST:
            *resp = exprp->var.const_val.var.bool_val;
            return RDB_OK;
        case RDB_ATTR:
            *resp = RDB_tuple_get_bool(tup, exprp->var.attr.name);
            return RDB_OK;
        case RDB_OP_REL_IS_EMPTY:
        {
            RDB_table *tbp;

            err = RDB_evaluate_table(exprp->var.op.arg1, tup, txp, &tbp);
            if (err != RDB_OK)
                return err;
            *resp = RDB_table_is_empty(tbp, txp, resp);
            return RDB_table_name(tbp) == NULL ?
                    RDB_drop_table(tbp, txp) : RDB_OK;
        }
        default: ;
    }
    abort();
}

int
RDB_evaluate_int(RDB_expression *exprp, const RDB_tuple *tup,
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

            err = RDB_evaluate_int(exprp->var.op.arg1, tup, txp, &v1);
            if (err != RDB_OK)
                return err;

            err = RDB_evaluate_int(exprp->var.op.arg2, tup, txp, &v2);
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
RDB_evaluate_rational(RDB_expression *exprp, const RDB_tuple *tup,
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

            err = RDB_evaluate_rational(exprp->var.op.arg1, tup, txp, &v1);
            if (err != RDB_OK)
                return err;

            err = RDB_evaluate_rational(exprp->var.op.arg2, tup, txp, &v2);
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
RDB_evaluate_table(RDB_expression *exprp, const RDB_tuple *tup,
                   RDB_transaction *txp, RDB_table **resultpp)
{
    int res;
    RDB_table *tbp1, *tbp2;

    switch (exprp->kind) {
        case RDB_TABLE:
            *resultpp = exprp->var.tbp;
            return RDB_OK;
        case RDB_OP_REL_SELECT:
            res = RDB_evaluate_table(exprp->var.op.arg1, tup, txp, &tbp1);
            if (res != RDB_OK)
                return res;
            return RDB_select(tbp1, RDB_dup_expr(exprp->var.op.arg2),
                                    resultpp);
        case RDB_OP_REL_UNION:
            res = RDB_evaluate_table(exprp->var.op.arg1, tup, txp, &tbp1);
            if (res != RDB_OK)
                return res;
            res = RDB_evaluate_table(exprp->var.op.arg2, tup, txp, &tbp2);
            if (res != RDB_OK)
                return res;
            return RDB_union(tbp1, tbp2, resultpp);
        case RDB_OP_REL_MINUS:
            res = RDB_evaluate_table(exprp->var.op.arg1, tup, txp, &tbp1);
            if (res != RDB_OK)
                return res;
            res = RDB_evaluate_table(exprp->var.op.arg2, tup, txp, &tbp2);
            if (res != RDB_OK)
                return res;
            return RDB_minus(tbp1, tbp2, resultpp);
        case RDB_OP_REL_INTERSECT:
            res = RDB_evaluate_table(exprp->var.op.arg1, tup, txp, &tbp1);
            if (res != RDB_OK)
                return res;
            res = RDB_evaluate_table(exprp->var.op.arg2, tup, txp, &tbp2);
            if (res != RDB_OK)
                return res;
            return RDB_intersect(tbp1, tbp2, resultpp);
        case RDB_OP_REL_JOIN:
            res = RDB_evaluate_table(exprp->var.op.arg1, tup, txp, &tbp1);
            if (res != RDB_OK)
                return res;
            res = RDB_evaluate_table(exprp->var.op.arg2, tup, txp, &tbp2);
            if (res != RDB_OK)
                return res;
            return RDB_join(tbp1, tbp2, resultpp);
        case RDB_OP_REL_EXTEND:
            res = RDB_evaluate_table(exprp->var.op.arg1, tup, txp, &tbp1);
            if (res != RDB_OK)
                return res;
            return RDB_extend(tbp1, exprp->var.op.attrc,
                                    exprp->var.op.attrv, resultpp);
        case RDB_OP_REL_PROJECT:
        {
            char **attrnamv;
            int attrc = exprp->var.op.attrc;
            int i;

            res = RDB_evaluate_table(exprp->var.op.arg1, tup, txp, &tbp1);
            if (res != RDB_OK)
                return res;
            
            attrnamv = malloc(attrc * sizeof(char *));
            for (i = 0; i < attrc; i++) {
                attrnamv[i] = exprp->var.op.attrv[i].name;
            }
            res = RDB_project(tbp1, attrc, attrnamv, resultpp);
            free(attrnamv);
            return res;
        }
        default:
             return RDB_ILLEGAL_ARG;
    }
}

int
RDB_evaluate(RDB_expression *exprp, const RDB_tuple *tup, RDB_transaction *txp,
            RDB_value *valp)
{
    RDB_type *typ = RDB_expr_type(exprp);

    if (typ != NULL) {
        valp->typ = typ;
        switch(typ->kind) {   
            case RDB_TP_BOOLEAN:
                return RDB_evaluate_bool(exprp, tup, txp, &valp->var.bool_val);
            case RDB_TP_INTEGER:
                return RDB_evaluate_int(exprp, tup, txp, &valp->var.int_val);
            case RDB_TP_RATIONAL:
                return RDB_evaluate_rational(exprp, tup, txp, &valp->var.rational_val);
            case RDB_TP_STRING:
            {
                char *str;
                int err;
                
                err = evaluate_string(exprp, tup, txp, &str);
                if (err != RDB_OK)
                    return err;
                        
                valp->var.bin.datap = str;
                valp->var.bin.len = strlen(valp->var.bin.datap) + 1;
                return RDB_OK;
            }
            default:
                abort();
        }
    } else {
        return RDB_evaluate_table(exprp, tup, txp, &valp->var.tbp);
    }
}
