/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"
#include <gen/strfns.h>
#include <string.h>
#include <regex.h>

RDB_bool
RDB_expr_is_const(const RDB_expression *exp)
{
    return (RDB_bool) exp->kind == RDB_EX_OBJ;
}

int
RDB_expr_type(const RDB_expression *exp, const RDB_type *tuptyp, RDB_type **typp)
{
    int ret;
    RDB_attr *attrp;
    RDB_type *typ;
    RDB_type *typ2;

    switch (exp->kind) {
        case RDB_EX_OBJ:
             *typp = RDB_obj_type(&exp->var.obj);
             if (*typp == NULL)
                 return RDB_NOT_FOUND;
             break;
        case RDB_EX_ATTR:
            attrp = _RDB_tuple_type_attr(
                    tuptyp, exp->var.attr.name);
            if (attrp == NULL)
                return RDB_NOT_FOUND;
            *typp = attrp->typ;
            break;
        case RDB_EX_NOT:
            /* Argument must be BOOLEAN */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            *typp = &RDB_BOOLEAN;
            break;
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
            /*
             * Operand types must be equal
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);

            /* Special treatment of tuple operands */
            if (ret != RDB_OK && ret != RDB_NOT_FOUND)
                return ret;
            if (ret == RDB_NOT_FOUND) {
                /* Operand must be tuple-type constant */
                if (exp->var.op.arg1->kind != RDB_EX_OBJ
                    || exp->var.op.arg1->var.obj.kind != RDB_OB_TUPLE) {
                        return RDB_NOT_FOUND;
                }
            } else if (RDB_type_is_scalar(typ)) {
                ret = RDB_expr_type(exp->var.op.arg2, tuptyp, &typ2);
                if (ret != RDB_OK)
                    return ret;
                if (!RDB_type_equals(typ, typ2))
                    return RDB_TYPE_MISMATCH;
            }
            *typp = &RDB_BOOLEAN;
            break;
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
            /*
             * Operand types must be equal and (numeric or STRING)
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (!RDB_type_is_numeric(typ) && typ != &RDB_STRING)
                return RDB_TYPE_MISMATCH;
            ret = RDB_expr_type(exp->var.op.arg2, tuptyp, &typ2);
            if (ret != RDB_OK)
                return ret;
            if (!RDB_type_is_numeric(typ2) && typ != &RDB_STRING)
                return RDB_TYPE_MISMATCH;
            if (!RDB_type_equals(typ, typ2))
                return RDB_TYPE_MISMATCH;
            *typp = &RDB_BOOLEAN;
            break;
        case RDB_EX_AND:
        case RDB_EX_OR:
            /*
             * Operand types must be BOOLEAN
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (typ != &RDB_BOOLEAN)
                return RDB_TYPE_MISMATCH;
            ret = RDB_expr_type(exp->var.op.arg2, tuptyp, &typ2);
            if (ret != RDB_OK)
                return ret;
            if (typ != &RDB_BOOLEAN)
                return RDB_TYPE_MISMATCH;
            *typp = &RDB_BOOLEAN;
            break;
        case RDB_EX_REGMATCH:
            /*
             * Operand type must be STRING
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (typ != &RDB_STRING)
                return RDB_TYPE_MISMATCH;
            *typp = &RDB_BOOLEAN;
            break;
        case RDB_EX_CONTAINS:
            /*
             * Operand #1 must be a relation
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (typ->kind != RDB_TP_RELATION)
                return RDB_TYPE_MISMATCH;
            *typp = &RDB_BOOLEAN;
            break;
        case RDB_EX_IS_EMPTY:
            /*
             * Operand must be a relation
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (typ->kind != RDB_TP_RELATION)
                return RDB_TYPE_MISMATCH;
            *typp = &RDB_BOOLEAN;
            break;
        case RDB_EX_SUBSET:
            *typp = &RDB_BOOLEAN;
            break;
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
            /*
             * Operand types must be equal and numeric
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (!RDB_type_is_numeric(typ))
                return RDB_TYPE_MISMATCH;
            ret = RDB_expr_type(exp->var.op.arg2, tuptyp, &typ2);
            if (ret != RDB_OK)
                return ret;
            if (!RDB_type_is_numeric(typ2))
                return RDB_TYPE_MISMATCH;
            if (!RDB_type_equals(typ, typ2))
                return RDB_TYPE_MISMATCH;
            *typp = typ;
            break;
        case RDB_EX_NEGATE:
            /*
             * Operand type must be numeric
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (!RDB_type_is_numeric(typ))
                return RDB_TYPE_MISMATCH;
            *typp = typ;
            break;
        case RDB_EX_STRLEN:
            /*
             * Operand type must be STRING
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (typ != &RDB_STRING)
                return RDB_TYPE_MISMATCH;
            *typp = &RDB_INTEGER;
            break;
        case RDB_EX_CONCAT:
            /*
             * Operand types must be STRING
             */
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            if (typ != &RDB_STRING)
                return RDB_TYPE_MISMATCH;
            ret = RDB_expr_type(exp->var.op.arg2, tuptyp, &typ2);
            if (ret != RDB_OK)
                return ret;
            if (typ != &RDB_STRING)
                return RDB_TYPE_MISMATCH;
            *typp = &RDB_STRING;
            break;
        case RDB_EX_TUPLE_ATTR:
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            *typp = RDB_type_attr_type(typ, exp->var.op.name);
            if (*typp == NULL)
                return RDB_NOT_FOUND;
            break;
        case RDB_EX_GET_COMP:
            ret = RDB_expr_type(exp->var.op.arg1, tuptyp, &typ);
            if (ret != RDB_OK)
                return ret;
            attrp = _RDB_get_icomp(typ, exp->var.op.name);
            if (attrp == NULL)
                return RDB_NOT_FOUND;
            *typp = attrp->typ;
            break;
        case RDB_EX_USER_OP:
            *typp = exp->var.user_op.rtyp;
            break;
        case RDB_EX_AGGREGATE:
            switch (exp->var.op.op) {
                case RDB_COUNT:
                    *typp = &RDB_INTEGER;
                    break;
                case RDB_AVG:
                    *typp = &RDB_RATIONAL;
                    break;
                default:
                    attrp = _RDB_tuple_type_attr(
                            exp->var.op.arg1->var.obj.var.tbp->typ->var.basetyp,
                            exp->var.op.name);
                    if (attrp == NULL)
                        return RDB_NOT_FOUND;
                    *typp = attrp->typ;
            }
            break;
    }
    return RDB_OK;
}

RDB_expression *
RDB_bool_to_expr(RDB_bool v)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_BOOLEAN;
    exp->var.obj.kind = RDB_OB_BOOL;
    exp->var.obj.var.bool_val = v;

    return exp;
}

RDB_expression *
RDB_int_to_expr(RDB_int v)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_INTEGER;
    exp->var.obj.kind = RDB_OB_INT;
    exp->var.obj.var.int_val = v;

    return exp;
}

RDB_expression *
RDB_rational_to_expr(RDB_rational v)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_RATIONAL;
    exp->var.obj.kind = RDB_OB_RATIONAL;
    exp->var.obj.var.rational_val = v;

    return exp;
}

RDB_expression *
RDB_string_to_expr(const char *v)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_OBJ;
    exp->var.obj.typ = &RDB_STRING;
    exp->var.obj.kind = RDB_OB_BIN;
    exp->var.obj.var.bin.datap = RDB_dup_str(v);
    exp->var.obj.var.bin.len = strlen(v) + 1;

    return exp;
}

RDB_expression *
RDB_obj_to_expr(const RDB_object *valp)
{
    int ret;
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_OBJ;
    RDB_init_obj(&exp->var.obj);
    ret = RDB_copy_obj(&exp->var.obj, valp);
    if (ret != RDB_OK) {
        free(exp);
        return NULL;
    }
    return exp;
}    

RDB_expression *
RDB_expr_attr(const char *attrname)
{
    RDB_expression *exp = malloc(sizeof (RDB_expression));
    
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_ATTR;
    exp->var.attr.name = RDB_dup_str(attrname);
    if (exp->var.attr.name == NULL) {
        free(exp);
        return NULL;
    }
    
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
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_EQ);
}

RDB_expression *
RDB_neq(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_NEQ);
}

RDB_expression *
RDB_lt(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_LT);
}

RDB_expression *
RDB_gt(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_GT);
}

RDB_expression *
RDB_let(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_LET);
}

RDB_expression *
RDB_get(RDB_expression *arg1, RDB_expression *arg2) {
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_GET);
}

RDB_expression *
RDB_and(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_AND);
}

RDB_expression *
RDB_or(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_OR);
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
        
    exp->kind = RDB_EX_NOT;
    exp->var.op.arg1 = arg;

    return exp;
}    

RDB_expression *
RDB_add(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_ADD);
}

RDB_expression *
RDB_subtract(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_SUBTRACT);
}

RDB_expression *
RDB_negate(RDB_expression *arg)
{
    return _RDB_create_unexpr(arg, RDB_EX_NEGATE);
}

RDB_expression *
RDB_multiply(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_MULTIPLY);
}

RDB_expression *
RDB_divide(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_DIVIDE);
}

RDB_expression *
RDB_strlen(RDB_expression *arg)
{
    return _RDB_create_unexpr(arg, RDB_EX_STRLEN);
}

RDB_expression *
RDB_regmatch(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_REGMATCH);
}

RDB_expression *
RDB_expr_contains(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_CONTAINS);
}

RDB_expression *
RDB_expr_subset(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_SUBSET);
}

RDB_expression *
RDB_concat(RDB_expression *arg1, RDB_expression *arg2)
{
    return _RDB_create_binexpr(arg1, arg2, RDB_EX_CONCAT);
}

RDB_expression *
RDB_table_to_expr(RDB_table *tbp)
{
    RDB_expression *exp;

    exp = malloc(sizeof (RDB_expression));  
    if (exp == NULL)
        return NULL;
        
    exp->kind = RDB_EX_OBJ;
    RDB_init_obj(&exp->var.obj);
    RDB_table_to_obj(&exp->var.obj, tbp);
    
    return exp;
}

RDB_expression *
RDB_expr_is_empty(RDB_expression *arg1)
{
    return _RDB_create_unexpr(arg1, RDB_EX_IS_EMPTY);
}

RDB_expression *
RDB_expr_aggregate(RDB_expression *arg, RDB_aggregate_op op,
        const char *attrname)
{
    RDB_expression *exp = _RDB_create_unexpr(arg, RDB_EX_AGGREGATE);

    if (exp == NULL)
        return NULL;
    if (attrname != NULL) {
        exp->var.op.name = RDB_dup_str(attrname);
        if (exp->var.op.name == NULL) {
            free(exp);
            return NULL;
        }
    } else {
        exp->var.op.name = NULL;
    }
    exp->var.op.op = op;

    return exp;
}

RDB_expression *
RDB_expr_sum(RDB_expression *arg, const char *attrname)
{
    return RDB_expr_aggregate(arg, RDB_SUM, attrname);
}

RDB_expression *
RDB_expr_avg(RDB_expression *arg, const char *attrname)
{
    return RDB_expr_aggregate(arg, RDB_AVG, attrname);
}

RDB_expression *
RDB_expr_max(RDB_expression *arg, const char *attrname)
{
    return RDB_expr_aggregate(arg, RDB_MAX, attrname);
}

RDB_expression *
RDB_expr_min(RDB_expression *arg, const char *attrname)
{
    return RDB_expr_aggregate(arg, RDB_MIN, attrname);
}

RDB_expression *
RDB_expr_all(RDB_expression *arg, const char *attrname) {
    return RDB_expr_aggregate(arg, RDB_ALL, attrname);
}

RDB_expression *
RDB_expr_any(RDB_expression *arg, const char *attrname) {
    return RDB_expr_aggregate(arg, RDB_ANY, attrname);
}

RDB_expression *
RDB_expr_cardinality(RDB_expression *arg)
{
    return RDB_expr_aggregate(arg, RDB_COUNT, NULL);
}

RDB_expression *
RDB_tuple_attr(RDB_expression *arg, const char *attrname)
{
    RDB_expression *exp;

    exp = _RDB_create_unexpr(arg, RDB_EX_TUPLE_ATTR);
    if (exp == NULL)
        return NULL;

    exp->var.op.name = RDB_dup_str(attrname);
    if (exp->var.op.name == NULL) {
        RDB_drop_expr(exp);
        return NULL;
    }
    return exp;
}

RDB_expression *
RDB_expr_comp(RDB_expression *arg, const char *compname)
{
    RDB_expression *exp;

    exp = _RDB_create_unexpr(arg, RDB_EX_GET_COMP);
    if (exp == NULL)
        return NULL;

    exp->var.op.name = RDB_dup_str(compname);
    if (exp->var.op.name == NULL) {
        RDB_drop_expr(exp);
        return NULL;
    }
    return exp;
}

static RDB_expression *
user_op(const char *opname, int argc, RDB_expression *argv[],
       RDB_type *rtyp)
{
    RDB_expression *exp;
    int i;

    exp = malloc(sizeof (RDB_expression));
    if (exp == NULL)
        return NULL;

    exp->kind = RDB_EX_USER_OP;
    
    exp->var.user_op.name = RDB_dup_str(opname);
    if (exp->var.user_op.name == NULL) {
        free(exp);
        return NULL;
    }

    exp->var.user_op.rtyp = rtyp;

    exp->var.user_op.argc = argc;
    exp->var.user_op.argv = malloc(argc * sizeof(RDB_expression *));
    if (exp->var.user_op.argv == NULL) {
        free(exp->var.user_op.name);
        free(exp);
        return NULL;
    }

    for (i = 0; i < argc; i++)
        exp->var.user_op.argv[i] = argv[i];

    return exp;
}

int
RDB_user_op(const char *opname, int argc, RDB_expression *argv[],
       RDB_transaction *txp, RDB_expression **expp)
{
    RDB_type *rtyp;
    int ret;

    ret = _RDB_get_cat_rtype(opname, txp, &rtyp);
    if (ret != RDB_OK) {
        return ret;
    }

    *expp = user_op(opname, argc, argv, rtyp);
    if (*expp == NULL)
        return RDB_NO_MEMORY;
    return RDB_OK;
}

/* Destroy the expression and all subexpressions */
void 
RDB_drop_expr(RDB_expression *exp)
{
    switch (exp->kind) {
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
        case RDB_EX_AND:
        case RDB_EX_OR:
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
        case RDB_EX_REGMATCH:
        case RDB_EX_CONTAINS:
        case RDB_EX_CONCAT:
        case RDB_EX_SUBSET:
            RDB_drop_expr(exp->var.op.arg2);
        case RDB_EX_NOT:
        case RDB_EX_NEGATE:
        case RDB_EX_IS_EMPTY:
        case RDB_EX_STRLEN:
            RDB_drop_expr(exp->var.op.arg1);
            break;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            free(exp->var.op.name);
            RDB_drop_expr(exp->var.op.arg1);
            break;
        case RDB_EX_USER_OP:
        {
            int i;

            for (i = 0; i < exp->var.user_op.argc; i++)
                RDB_drop_expr(exp->var.user_op.argv[i]);
            free(exp->var.user_op.argv);
            break;
        }
        case RDB_EX_AGGREGATE:
            free(exp->var.op.name);
            break;
        case RDB_EX_OBJ:
            RDB_destroy_obj(&exp->var.obj);
            break;
        case RDB_EX_ATTR:
            free(exp->var.attr.name);
            break;
    }
    free(exp);
}

static int
evaluate_arith(RDB_expression *exp, const RDB_object *tup, RDB_transaction *txp,
            RDB_object *valp, enum _RDB_expr_kind kind)
{
    int ret;
    RDB_object val1, val2;
    RDB_type *typ;

    RDB_init_obj(&val1);
    ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val1);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val1);
        return ret;
    }

    RDB_init_obj(&val2);
    ret = RDB_evaluate(exp->var.op.arg2, tup, txp, &val2);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val1);
        RDB_destroy_obj(&val2);
        return ret;
    }

    typ = RDB_obj_type(&val1);
    if (!RDB_type_equals(typ, RDB_obj_type(&val2))) {
        RDB_destroy_obj(&val1);
        RDB_destroy_obj(&val2);
        return RDB_TYPE_MISMATCH;
    }

    if (typ == &RDB_INTEGER) {
        RDB_destroy_obj(valp);
        RDB_init_obj(valp);
        _RDB_set_obj_type(valp, &RDB_INTEGER);

        switch (exp->kind) {
            case RDB_EX_ADD:
                valp->var.int_val = val1.var.int_val + val2.var.int_val;
                break;
            case RDB_EX_SUBTRACT:
                valp->var.int_val = val1.var.int_val - val2.var.int_val;
                break;
            case RDB_EX_NEGATE:
                valp->var.int_val = -val1.var.int_val;
                break;
            case RDB_EX_MULTIPLY:
                valp->var.int_val = val1.var.int_val * val2.var.int_val;
                break;
            case RDB_EX_DIVIDE:
                if (val2.var.int_val == 0) {
                    RDB_destroy_obj(&val1);
                    RDB_destroy_obj(&val2);
                    return RDB_INVALID_ARGUMENT;
                }
                valp->var.int_val = val1.var.int_val / val2.var.int_val;
                break;
           default: /* should never happen */
                return RDB_INVALID_ARGUMENT;
        }
    } else if (typ == &RDB_RATIONAL) {
        RDB_destroy_obj(valp);
        RDB_init_obj(valp);
        _RDB_set_obj_type(valp, &RDB_RATIONAL);
 
        switch (exp->kind) {
            case RDB_EX_ADD:
                valp->var.rational_val = val1.var.rational_val
                        + val2.var.rational_val;
                break;
            case RDB_EX_SUBTRACT:
                valp->var.rational_val = val1.var.rational_val
                        - val2.var.rational_val;
                break;
            case RDB_EX_NEGATE:
                valp->var.rational_val = -val1.var.rational_val;
                break;
            case RDB_EX_MULTIPLY:
                valp->var.rational_val = val1.var.rational_val
                        * val2.var.rational_val;
                break;
            case RDB_EX_DIVIDE:
                if (val2.var.rational_val == 0) {
                    RDB_destroy_obj(&val1);
                    RDB_destroy_obj(&val2);
                    return RDB_INVALID_ARGUMENT;
                }
                valp->var.rational_val = val1.var.rational_val
                        / val2.var.rational_val;
                break;
           default: /* should never happen */
                return RDB_INVALID_ARGUMENT;
        }
    } else {
        RDB_destroy_obj(&val1);
        RDB_destroy_obj(&val2);
        return RDB_INVALID_ARGUMENT;
    }

    RDB_destroy_obj(&val1);
    RDB_destroy_obj(&val2);        
    return RDB_OK;
}

static int
evaluate_eq(RDB_expression *exp, const RDB_object *tup,
                  RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_object val1, val2;

    RDB_init_obj(&val1);
    RDB_init_obj(&val2);

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
     * Check types
     */

    if (!RDB_type_equals(RDB_obj_type(&val1), RDB_obj_type(&val2)))
        return RDB_TYPE_MISMATCH;

    /*
     * Compare values
     */

    ret = RDB_obj_equals(&val1, &val2, resp);

cleanup:
    RDB_destroy_obj(&val1);
    RDB_destroy_obj(&val2);

    return ret;
}

static int evaluate_user_op(RDB_expression *exp, const RDB_object *tup,
        RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    int i;
    RDB_object **valpv;
    RDB_object *valv = NULL;
    int argc = exp->var.user_op.argc;

    valpv = malloc(argc * sizeof (RDB_object *));
    if (valpv == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    valv = malloc(argc * sizeof (RDB_object));
    if (valv == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    for (i = 0; i < argc; i++) {
        valpv[i] = &valv[i];
        RDB_init_obj(&valv[i]);
        ret = RDB_evaluate(exp->var.user_op.argv[i], tup, txp, &valv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }
    ret = RDB_call_ro_op(exp->var.user_op.name, argc, valpv, txp, valp);

cleanup:
    if (valv != NULL) {
        for (i = 0; i < argc; i++) {
            RDB_destroy_obj(&valv[i]);
        }
        free(valv);
    }
    free(valpv);
    return ret;
}

static int
evaluate_order(RDB_expression *exp, const RDB_object *tup, RDB_transaction *txp,
            RDB_object *valp, enum _RDB_expr_kind kind)
{
    int ret;
    RDB_object val1, val2;
    RDB_type *typ;

    RDB_init_obj(&val1);
    ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val1);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val1);
        return ret;
    }

    RDB_init_obj(&val2);
    ret = RDB_evaluate(exp->var.op.arg2, tup, txp, &val2);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val1);
        RDB_destroy_obj(&val2);
        return ret;
    }

    typ = RDB_obj_type(&val1);
    if (!RDB_type_equals(typ, RDB_obj_type(&val2))) {
        RDB_destroy_obj(&val1);
        RDB_destroy_obj(&val2);
        return RDB_TYPE_MISMATCH;
    }

    RDB_destroy_obj(valp);
    RDB_init_obj(valp);
    _RDB_set_obj_type(valp, &RDB_BOOLEAN);

    if (typ == &RDB_INTEGER) {
        switch (kind) {
            case RDB_EX_LT:
                valp->var.bool_val = (RDB_bool)
                        (val1.var.int_val < val2.var.int_val);
                break;
            case RDB_EX_GT:
                valp->var.bool_val = (RDB_bool)
                        (val1.var.int_val > val2.var.int_val);
                break;
            case RDB_EX_LET:
                valp->var.bool_val = (RDB_bool)
                        (val1.var.int_val <= val2.var.int_val);
                break;
            case RDB_EX_GET:
                valp->var.bool_val = (RDB_bool)
                        (val1.var.int_val >= val2.var.int_val);
                break;
            default: ;
        }
    } else if (typ == &RDB_RATIONAL) {
        switch (kind) {
            case RDB_EX_LT:
                valp->var.bool_val = (RDB_bool)
                        (val1.var.rational_val < val2.var.rational_val);
                break;
            case RDB_EX_GT:
                valp->var.bool_val = (RDB_bool)
                        (val1.var.rational_val > val2.var.rational_val);
                break;
            case RDB_EX_LET:
                valp->var.bool_val = (RDB_bool)
                        (val1.var.rational_val <= val2.var.rational_val);
                break;
            case RDB_EX_GET:
                valp->var.bool_val = (RDB_bool)
                        (val1.var.rational_val >= val2.var.rational_val);
                break;
            default: /* should never happen */
                return RDB_INVALID_ARGUMENT;
        }
    } else if (typ == &RDB_STRING) {
        switch (kind) {
            case RDB_EX_LT:
                valp->var.bool_val = (RDB_bool)
                        (strcmp(val1.var.bin.datap, val2.var.bin.datap) < 0);
                break;
            case RDB_EX_GT:
                valp->var.bool_val = (RDB_bool)
                        (strcmp(val1.var.bin.datap, val2.var.bin.datap) > 0);
                break;
            case RDB_EX_LET:
                valp->var.bool_val = (RDB_bool)
                        (strcmp(val1.var.bin.datap, val2.var.bin.datap) <= 0);
                break;
            case RDB_EX_GET:
                valp->var.bool_val = (RDB_bool)
                        (strcmp(val1.var.bin.datap, val2.var.bin.datap) >= 0);
                break;
            default: /* should never happen */
                return RDB_INVALID_ARGUMENT;
        }    
    } else {
        RDB_destroy_obj(&val1);
        RDB_destroy_obj(&val2);
        return RDB_INVALID_ARGUMENT;
    }

    RDB_destroy_obj(&val1);
    RDB_destroy_obj(&val2);        
    return RDB_OK;
}

static int
evaluate_logbin(RDB_expression *exp, const RDB_object *tup, RDB_transaction *txp,
            RDB_object *valp, enum _RDB_expr_kind kind)
{
    int ret;
    RDB_object val1, val2;

    RDB_init_obj(&val1);
    ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val1);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val1);
        return ret;
    }
    if (RDB_obj_type(&val1) != &RDB_BOOLEAN) {
        RDB_destroy_obj(&val1);
        return ret;
    }

    RDB_init_obj(&val2);
    ret = RDB_evaluate(exp->var.op.arg2, tup, txp, &val2);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val1);
        RDB_destroy_obj(&val2);
        return ret;
    }
    if (RDB_obj_type(&val2) != &RDB_BOOLEAN) {
        RDB_destroy_obj(&val1);
        RDB_destroy_obj(&val2);
        return ret;
    }

    RDB_destroy_obj(valp);
    RDB_init_obj(valp);
    _RDB_set_obj_type(valp, &RDB_BOOLEAN);

    switch (kind) {
        case RDB_EX_AND:
            valp->var.bool_val = (RDB_bool)
                    (val1.var.bool_val && val2.var.bool_val);
            break;
        case RDB_EX_OR:
            valp->var.bool_val = (RDB_bool)
                    (val1.var.bool_val || val2.var.bool_val);
            break;
        default: ;
    }

    RDB_destroy_obj(&val1);
    RDB_destroy_obj(&val2);        
    return RDB_OK;
}

static int
aggregate(RDB_table *tbp, RDB_aggregate_op op, const char *attrname,
          RDB_transaction *txp, RDB_object *resultp)
{
    switch(op) {
        case RDB_COUNT:
        {
            int ret = RDB_cardinality(tbp, txp);

            if (ret < 0)
                return ret;
            _RDB_set_obj_type(resultp, &RDB_INTEGER);
            ret = resultp->var.int_val = ret;
            return RDB_OK;
        }
        case RDB_SUM:
            return RDB_sum(tbp, attrname, txp, resultp);
        case RDB_AVG:
            _RDB_set_obj_type(resultp, &RDB_RATIONAL);
            return RDB_avg(tbp, attrname, txp, &resultp->var.rational_val);
        case RDB_MAX:
            return RDB_max(tbp, attrname, txp, resultp);
        case RDB_MIN:
            return RDB_min(tbp, attrname, txp, resultp);
        case RDB_ALL:
            _RDB_set_obj_type(resultp, &RDB_BOOLEAN);
            return RDB_all(tbp, attrname, txp, &resultp->var.bool_val);
        case RDB_ANY:
            _RDB_set_obj_type(resultp, &RDB_BOOLEAN);
            return RDB_any(tbp, attrname, txp, &resultp->var.bool_val);
        default: ;
    }
    abort();
}

int
RDB_evaluate(RDB_expression *exp, const RDB_object *tup, RDB_transaction *txp,
            RDB_object *valp)
{
    int ret;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        {
            int ret;
            RDB_object tpl;
            RDB_object *attrp;

            RDB_init_obj(&tpl);
            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &tpl);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }
            if (tpl.kind != RDB_OB_TUPLE) {
                RDB_destroy_obj(&tpl);
                return RDB_TYPE_MISMATCH;
            }
                
            attrp = RDB_tuple_get(&tpl, exp->var.op.name);
            if (attrp == NULL) {
                RDB_destroy_obj(&tpl);
                return RDB_INVALID_ARGUMENT;
            }
            ret = RDB_copy_obj(valp, attrp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }
            return RDB_destroy_obj(&tpl);
        }
        case RDB_EX_GET_COMP:
        {
            int ret;
            RDB_object obj;

            RDB_init_obj(&obj);
            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &obj);
            if (ret != RDB_OK) {
                 RDB_destroy_obj(&obj);
                 return ret;
            }
            ret = RDB_obj_comp(&obj, exp->var.op.name, valp, txp);
            RDB_destroy_obj(&obj);
            return ret;
        }
        case RDB_EX_USER_OP:
            return evaluate_user_op(exp, tup, txp, valp);
        case RDB_EX_ATTR:
        {
            if (tup != NULL) {
                RDB_object *srcp = RDB_tuple_get(tup, exp->var.attr.name);
                if (srcp != NULL)
                    return RDB_copy_obj(valp, srcp);
            }

            RDB_errmsg(RDB_db_env(RDB_tx_db(txp)), "attribute %s not found",
                    exp->var.attr.name);
            return RDB_INVALID_ARGUMENT;
        }
        case RDB_EX_OBJ:
            return RDB_copy_obj(valp, &exp->var.obj);
        case RDB_EX_EQ:
            RDB_destroy_obj(valp);
            _RDB_set_obj_type(valp, &RDB_BOOLEAN);
            return evaluate_eq(exp, tup, txp, &valp->var.bool_val);
        case RDB_EX_NEQ:
        {
            RDB_bool b;
        
            ret = evaluate_eq(exp, tup, txp, &b);
            if (ret != RDB_OK)
               return ret;
            _RDB_set_obj_type(valp, &RDB_BOOLEAN);
            valp->var.bool_val = !b;
            return RDB_OK;
        }
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
            return evaluate_order(exp, tup, txp, valp, exp->kind);
        case RDB_EX_AND:
        case RDB_EX_OR:
            return evaluate_logbin(exp, tup, txp, valp, exp->kind);
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
            return evaluate_arith(exp, tup, txp, valp, exp->kind);
        case RDB_EX_NEGATE:
        {
            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, valp);
            if (ret != RDB_OK)
                return ret;
            if (RDB_obj_type(valp) == &RDB_INTEGER)
                valp->var.int_val = -valp->var.int_val;
            else if (RDB_obj_type(valp) == &RDB_RATIONAL)
                valp->var.rational_val = -valp->var.rational_val;
            else
                return RDB_TYPE_MISMATCH;

            return RDB_OK;
        }
        case RDB_EX_NOT:
        {
            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, valp);
            if (ret != RDB_OK)
                return ret;
            if (RDB_obj_type(valp) != &RDB_BOOLEAN)
                return RDB_TYPE_MISMATCH;

            valp->var.bool_val = !valp->var.bool_val;
            return RDB_OK;
        }
        case RDB_EX_REGMATCH:
        {
            regex_t reg;
            RDB_object val1, val2;

            RDB_init_obj(&val1);
            RDB_init_obj(&val2);

            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val1);
            if (ret != RDB_OK)
                return ret;
            if (RDB_obj_type(&val1) != &RDB_STRING)
                return RDB_TYPE_MISMATCH;

            ret = RDB_evaluate(exp->var.op.arg2, tup, txp, &val2);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val1);
                return ret;
            }
            if (RDB_obj_type(&val2) != &RDB_STRING) {
                RDB_destroy_obj(&val1);
                return RDB_TYPE_MISMATCH;
            }

            ret = regcomp(&reg, val2.var.bin.datap, REG_NOSUB);
            if (ret != 0) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return RDB_INVALID_ARGUMENT;
            }
            RDB_destroy_obj(valp);
            RDB_init_obj(valp);
            _RDB_set_obj_type(valp, &RDB_BOOLEAN);
            valp->var.bool_val = (RDB_bool)
                    (regexec(&reg, val1.var.bin.datap, 0, NULL, 0) == 0);
            regfree(&reg);
            RDB_destroy_obj(&val1);
            return RDB_destroy_obj(&val2);
        }
        case RDB_EX_CONTAINS:
        {
            RDB_object val1, val2;

            RDB_init_obj(&val1);
            RDB_init_obj(&val2);
            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val1);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return ret;
            }
            if (val1.kind != RDB_OB_TABLE) {
                RDB_destroy_obj(&val1);
                return RDB_TYPE_MISMATCH;
            }
            ret = RDB_evaluate(exp->var.op.arg2, tup, txp, &val2);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return ret;
            }
            ret = RDB_table_contains(val1.var.tbp, &val2, txp);
            if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return ret;
            }
            RDB_destroy_obj(valp);
            RDB_init_obj(valp);
            _RDB_set_obj_type(valp, &RDB_BOOLEAN);
            valp->var.bool_val = ret == RDB_OK ? RDB_TRUE : RDB_FALSE;
            RDB_destroy_obj(&val1);
            return RDB_destroy_obj(&val2);
        }
        case RDB_EX_SUBSET:
        {
            RDB_object val1, val2;

            RDB_init_obj(&val1);
            RDB_init_obj(&val2);
            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val1);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return ret;
            }
            if (val1.kind != RDB_OB_TABLE) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return RDB_TYPE_MISMATCH;
            }
            ret = RDB_evaluate(exp->var.op.arg2, tup, txp, &val2);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return ret;
            }
            if (val2.kind != RDB_OB_TABLE) {
                RDB_destroy_obj(&val2);
                RDB_destroy_obj(&val2);
                return RDB_TYPE_MISMATCH;
            }
            RDB_destroy_obj(valp);
            RDB_init_obj(valp);
            _RDB_set_obj_type(valp, &RDB_BOOLEAN);
            ret = RDB_subset(val1.var.tbp, val2.var.tbp, txp,
                    &valp->var.bool_val);
            if (ret != RDB_OK && ret != RDB_NOT_FOUND) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return ret;
            }
            RDB_destroy_obj(&val1);
            return RDB_destroy_obj(&val2);
        }
        case RDB_EX_CONCAT:
        {
            int s1len;
            RDB_object val1, val2;

            RDB_init_obj(&val1);
            RDB_init_obj(&val2);

            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val1);
            if (ret != RDB_OK)
                return ret;
            if (RDB_obj_type(&val1) != &RDB_STRING)
                return RDB_TYPE_MISMATCH;

            ret = RDB_evaluate(exp->var.op.arg2, tup, txp, &val2);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val1);
                return ret;
            }
            if (RDB_obj_type(&val2) != &RDB_STRING) {
                RDB_destroy_obj(&val1);
                return RDB_TYPE_MISMATCH;
            }

            RDB_destroy_obj(valp);
            RDB_init_obj(valp);
            _RDB_set_obj_type(valp, &RDB_STRING);
            s1len = strlen(val1.var.bin.datap);
            valp->var.bin.len = s1len + strlen(val2.var.bin.datap) + 1;
            valp->var.bin.datap = malloc(valp->var.bin.len);
            if (valp->var.bin.datap == NULL) {
                RDB_destroy_obj(&val1);
                RDB_destroy_obj(&val2);
                return RDB_NO_MEMORY;
            }
            strcpy(valp->var.bin.datap, val1.var.bin.datap);
            strcpy(((RDB_byte *)valp->var.bin.datap) + s1len,
                    val2.var.bin.datap);

            RDB_destroy_obj(&val1);
            return RDB_destroy_obj(&val2);
        }
        case RDB_EX_STRLEN:
        {
            RDB_object val;

            RDB_init_obj(&val);

            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                return ret;
            }
            if (RDB_obj_type(&val) != &RDB_STRING) {
                RDB_destroy_obj(&val);
                return RDB_TYPE_MISMATCH;
            }

            RDB_destroy_obj(valp);
            RDB_init_obj(valp);
            _RDB_set_obj_type(valp, &RDB_INTEGER);
            valp->var.int_val = val.var.bin.len - 1;
            RDB_destroy_obj(&val);

            return RDB_OK;
        }           
        case RDB_EX_AGGREGATE:
        {
            RDB_object val;

            RDB_init_obj(&val);
            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                return ret;
            }
            if (val.kind != RDB_OB_TABLE) {
                RDB_destroy_obj(&val);
                return RDB_TYPE_MISMATCH;
            }
            ret = aggregate(val.var.tbp, exp->var.op.op,
                    exp->var.op.name, txp, valp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                return ret;
            }
            return RDB_destroy_obj(&val);
        }
        case RDB_EX_IS_EMPTY:
        {
            RDB_object val;

            RDB_init_obj(&val);

            ret = RDB_evaluate(exp->var.op.arg1, tup, txp, &val);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                return ret;
            }
            if (val.kind != RDB_OB_TABLE) {
                RDB_destroy_obj(&val);
                return RDB_TYPE_MISMATCH;
            }
            ret = RDB_destroy_obj(valp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                return ret;
            }
            RDB_init_obj(valp);
            _RDB_set_obj_type(valp, &RDB_BOOLEAN);
            ret = RDB_table_is_empty(val.var.tbp, txp,
                    &valp->var.bool_val);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&val);
                return ret;
            }
            return RDB_destroy_obj(&val);
        }
    }
    /* Should never be reached */
    abort();
}

int
RDB_evaluate_bool(RDB_expression *exp, const RDB_object *tup, RDB_transaction *txp,
                  RDB_bool *resp)
{
    int ret;
    RDB_object val;

    RDB_init_obj(&val);
    ret = RDB_evaluate(exp, tup, txp, &val);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&val);
        return ret;
    }
    if (RDB_obj_type(&val) != &RDB_BOOLEAN) {
        RDB_destroy_obj(&val);
        return RDB_TYPE_MISMATCH;
    }

    *resp = val.var.bool_val;
    RDB_destroy_obj(&val);
    return RDB_OK;
}

RDB_expression *
RDB_dup_expr(const RDB_expression *exp)
{
    switch (exp->kind) {
        case RDB_EX_EQ:
            return RDB_eq(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_NEQ:
            return RDB_neq(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_LT:
            return RDB_lt(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_GT:
            return RDB_gt(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_LET:
            return RDB_let(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_GET:
            return RDB_get(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_AND:
            return RDB_and(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_OR:
            return RDB_or(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_ADD:
            return RDB_add(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_SUBTRACT:
            return RDB_subtract(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_MULTIPLY:
            return RDB_multiply(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_DIVIDE:
            return RDB_divide(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_REGMATCH:
            return RDB_regmatch(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_CONTAINS:
            return RDB_expr_contains(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_CONCAT:
            return RDB_concat(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_SUBSET:
            return RDB_expr_subset(RDB_dup_expr(exp->var.op.arg1),
                    RDB_dup_expr(exp->var.op.arg2));
        case RDB_EX_NOT:
            return RDB_not(RDB_dup_expr(exp->var.op.arg1));
        case RDB_EX_NEGATE:
            return RDB_negate(RDB_dup_expr(exp->var.op.arg1));
        case RDB_EX_IS_EMPTY:
            return RDB_expr_is_empty(RDB_dup_expr(exp->var.op.arg1));
        case RDB_EX_STRLEN:
            return RDB_strlen(RDB_dup_expr(exp->var.op.arg1));
        case RDB_EX_TUPLE_ATTR:
            return RDB_tuple_attr(RDB_dup_expr(exp->var.op.arg1),
                    exp->var.op.name);
        case RDB_EX_GET_COMP:
            return RDB_expr_comp(RDB_dup_expr(exp->var.op.arg1),
                    exp->var.op.name);
        case RDB_EX_USER_OP:
        {
            int i;
            RDB_expression *newexp;
            RDB_expression **argexpv = (RDB_expression **)
                    malloc(sizeof (RDB_expression *) * exp->var.user_op.argc);

            if (argexpv == NULL)
                return NULL;

            for (i = 0; i < exp->var.user_op.argc; i++) {
                argexpv[i] = RDB_dup_expr(exp->var.user_op.argv[i]);
                if (argexpv[i] == NULL)
                    return NULL;
            }
            newexp = user_op(exp->var.user_op.name, exp->var.user_op.argc,
                    argexpv, exp->var.user_op.rtyp);
            free(argexpv);
            return newexp;
        }
        case RDB_EX_AGGREGATE:
            return RDB_expr_aggregate(RDB_dup_expr(exp->var.op.arg1), exp->var.op.op,
                    exp->var.op.name);
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj);
        case RDB_EX_ATTR:
            return RDB_expr_attr(exp->var.attr.name);
    }
    abort();
}

RDB_bool
_RDB_expr_refers(RDB_expression *exp, RDB_table *tbp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            if (exp->var.obj.kind == RDB_OB_TABLE)
                return (RDB_bool) (tbp == exp->var.obj.var.tbp);
            return RDB_FALSE;
        case RDB_EX_ATTR:
            return RDB_FALSE;
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
        case RDB_EX_AND:
        case RDB_EX_OR:
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
        case RDB_EX_REGMATCH:
        case RDB_EX_CONTAINS:
        case RDB_EX_CONCAT:
        case RDB_EX_SUBSET:
            return (RDB_bool) (_RDB_expr_refers(exp->var.op.arg1, tbp)
                    || _RDB_expr_refers(exp->var.op.arg2, tbp));
        case RDB_EX_NOT:
        case RDB_EX_NEGATE:
        case RDB_EX_IS_EMPTY:
        case RDB_EX_STRLEN:
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_expr_refers(exp->var.op.arg1, tbp);
        case RDB_EX_USER_OP:
        {
            int i;

            for (i = 0; i < exp->var.user_op.argc; i++)
                if (_RDB_expr_refers(exp->var.user_op.argv[i], tbp))
                    return RDB_TRUE;
            
            return RDB_FALSE;
        }
        case RDB_EX_AGGREGATE:
            return (RDB_bool) (exp->var.op.arg1->var.obj.var.tbp == tbp);
    }
    /* Should never be reached */
    abort();
}

RDB_object *
RDB_expr_obj(RDB_expression *exp)
{
    if (exp->kind != RDB_EX_OBJ)
        return NULL;
    return &exp->var.obj;
}

int
_RDB_invrename_expr(RDB_expression *exp, int renc, const RDB_renaming renv[])
{
    int ret;
    int i;

    switch (exp->kind) {
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
        case RDB_EX_AND:
        case RDB_EX_OR:
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
        case RDB_EX_REGMATCH:
        case RDB_EX_CONTAINS:
        case RDB_EX_CONCAT:
        case RDB_EX_SUBSET:
            ret = _RDB_invrename_expr(exp->var.op.arg2, renc, renv);
            if (ret != RDB_OK)
                return ret;
        case RDB_EX_NOT:
        case RDB_EX_NEGATE:
        case RDB_EX_IS_EMPTY:
        case RDB_EX_STRLEN:
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_invrename_expr(exp->var.op.arg1, renc, renv);
        case RDB_EX_USER_OP:
            for (i = 0; i < exp->var.user_op.argc; i++) {
                ret = _RDB_invrename_expr(exp->var.user_op.argv[i], renc, renv);
                if (ret != RDB_OK)
                    return ret;
            }
            return RDB_OK;
        case RDB_EX_AGGREGATE:
        case RDB_EX_OBJ:
            return RDB_OK;
        case RDB_EX_ATTR:
            /* Search attribute name in renv[].to */
            for (i = 0;
                    i < renc && strcmp(renv[i].to, exp->var.attr.name) != 0;
                    i++);

            /* If found, replace it */
            if (i < renc) {
                char *name = realloc(exp->var.attr.name,
                        strlen(renv[i].from) + 1);

                if (name == NULL)
                    return RDB_NO_MEMORY;

                strcpy(name, renv[i].from);
                exp->var.attr.name = name;
            }
            return RDB_OK;
    }
    abort();
}

int
_RDB_resolve_extend_expr(RDB_expression **expp, int attrc,
        const RDB_virtual_attr attrv[])
{
    int ret;
    int i;

    switch ((*expp)->kind) {
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
        case RDB_EX_AND:
        case RDB_EX_OR:
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
        case RDB_EX_REGMATCH:
        case RDB_EX_CONTAINS:
        case RDB_EX_CONCAT:
        case RDB_EX_SUBSET:
            ret = _RDB_resolve_extend_expr(&(*expp)->var.op.arg2, attrc, attrv);
            if (ret != RDB_OK)
                return ret;
        case RDB_EX_NOT:
        case RDB_EX_NEGATE:
        case RDB_EX_IS_EMPTY:
        case RDB_EX_STRLEN:
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_resolve_extend_expr(&(*expp)->var.op.arg1, attrc, attrv);
        case RDB_EX_USER_OP:
            for (i = 0; i < (*expp)->var.user_op.argc; i++) {
                ret = _RDB_resolve_extend_expr(&(*expp)->var.user_op.argv[i],
                        attrc, attrv);
                if (ret != RDB_OK)
                    return ret;
            }
            return RDB_OK;
        case RDB_EX_AGGREGATE:
        case RDB_EX_OBJ:
            return RDB_OK;
        case RDB_EX_ATTR:
            /* Search attribute name in attrv[].name */
            for (i = 0;
                    i < attrc && strcmp(attrv[i].name, (*expp)->var.attr.name) != 0;
                    i++);

            /* If found, replace attribute by expression */
            if (i < attrc) {
                RDB_expression *exp = RDB_dup_expr(attrv[i].exp);
                if (exp == NULL)
                    return RDB_NO_MEMORY;

                RDB_drop_expr(*expp);
                *expp = exp;
            }
            return RDB_OK;
    }
    abort();
}
