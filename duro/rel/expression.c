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

RDB_bool
RDB_expr_is_const(const RDB_expression *exp)
{
    /* !! Check for constant subexpressions missing */
    return (RDB_bool) exp->kind == RDB_EX_OBJ;
}

static int
expr_op_type(const RDB_expression *exp, const RDB_type *tuptyp,
        RDB_transaction *txp, RDB_type **typp)
{
    int i;
    int ret;
    RDB_ro_op_desc *op;

    if (strcmp(exp->var.op.name, "=") == 0
            || strcmp(exp->var.op.name, "<>") == 0
            || strcmp(exp->var.op.name, "IN") == 0
            || strcmp(exp->var.op.name, "SUBSET_OF") == 0
            || strcmp(exp->var.op.name, "IS_EMPTY") == 0) {
        *typp = &RDB_BOOLEAN;
        return RDB_OK;
    }

    RDB_type **argtv = malloc(sizeof (RDB_type *) * exp->var.op.argc);
    if (argtv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < exp->var.op.argc; i++) {
        ret = RDB_expr_type(exp->var.op.argv[i], tuptyp, txp, &argtv[i]);
        if (ret != RDB_OK) {
            free(argtv);
            return ret;
        }
    }
    ret = _RDB_get_ro_op(exp->var.op.name, exp->var.op.argc,
            argtv, txp, &op);
    free(argtv);
    if (ret != RDB_OK)
        return ret;
    *typp = op->rtyp;
    return RDB_OK;
}

int
RDB_expr_type(const RDB_expression *exp, const RDB_type *tuptyp,
        RDB_transaction *txp, RDB_type **typp)
{
    int ret;
    RDB_attr *attrp;
    RDB_type *typ;

    switch (exp->kind) {
        case RDB_EX_OBJ:
             *typp = RDB_obj_type(&exp->var.obj);
             if (*typp == NULL)
                 return RDB_NOT_FOUND;
             break;
        case RDB_EX_ATTR:
            attrp = _RDB_tuple_type_attr(
                    tuptyp, exp->var.attrname);
            if (attrp == NULL)
                return RDB_ATTRIBUTE_NOT_FOUND;
            *typp = attrp->typ;
            break;
        case RDB_EX_TUPLE_ATTR:
            ret = RDB_expr_type(exp->var.op.argv[0], tuptyp, txp, &typ);
            if (ret != RDB_OK)
                return ret;
            *typp = RDB_type_attr_type(typ, exp->var.op.name);
            if (*typp == NULL)
                return RDB_NOT_FOUND;
            break;
        case RDB_EX_GET_COMP:
            ret = RDB_expr_type(exp->var.op.argv[0], tuptyp, txp, &typ);
            if (ret != RDB_OK)
                return ret;
            attrp = _RDB_get_icomp(typ, exp->var.op.name);
            if (attrp == NULL)
                return RDB_NOT_FOUND;
            *typp = attrp->typ;
            break;
        case RDB_EX_RO_OP:
            ret = expr_op_type(exp, tuptyp, txp, typp);
            if (ret != RDB_OK)
                return ret;
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
                            exp->var.op.argv[0]->var.obj.var.tbp->typ->var.basetyp,
                            exp->var.op.name);
                    if (attrp == NULL)
                        return RDB_ATTRIBUTE_NOT_FOUND;
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
    exp->var.attrname = RDB_dup_str(attrname);
    if (exp->var.attrname == NULL) {
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

    exp->var.op.argv = malloc(sizeof (RDB_expression *));
    if (exp->var.op.argv == NULL)
        return NULL;
        
    exp->kind = kind;
    exp->var.op.argv[0] = arg;

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

    exp->var.op.argv = malloc(sizeof (RDB_expression *) * 2);
    if (exp->var.op.argv == NULL)
        return NULL;
        
    exp->kind = kind;
    exp->var.op.argv[0] = arg1;
    exp->var.op.argv[1] = arg2;

    return exp;
}

RDB_expression *
_RDB_ro_op(const char *opname, int argc, RDB_expression *argv[])
{
    RDB_expression *exp;
    int i;

    exp = malloc(sizeof (RDB_expression));
    if (exp == NULL)
        return NULL;

    exp->kind = RDB_EX_RO_OP;
    
    exp->var.op.name = RDB_dup_str(opname);
    if (exp->var.op.name == NULL) {
        free(exp);
        return NULL;
    }

    exp->var.op.argc = argc;
    exp->var.op.argv = malloc(argc * sizeof(RDB_expression *));
    if (exp->var.op.argv == NULL) {
        free(exp->var.op.name);
        free(exp);
        return NULL;
    }

    for (i = 0; i < argc; i++)
        exp->var.op.argv[i] = argv[i];

    return exp;
}

RDB_expression *
RDB_eq(RDB_expression *arg1, RDB_expression *arg2)
{
    RDB_expression *argv[2];

    argv[0] = arg1;
    argv[1] = arg2;
    return _RDB_ro_op("=", 2, argv);
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

int
RDB_ro_op(const char *opname, int argc, RDB_expression *argv[],
       RDB_transaction *txp, RDB_expression **expp)
{
    *expp = _RDB_ro_op(opname, argc, argv);
    if (*expp == NULL)
        return RDB_NO_MEMORY;
    return RDB_OK;
}

int
RDB_ro_op_1(const char *opname, RDB_expression *arg,
        RDB_transaction *txp, RDB_expression **expp)
{
    return RDB_ro_op(opname, 1, &arg, txp, expp);
}

int
RDB_ro_op_2(const char *opname, RDB_expression *arg1, RDB_expression *arg2,
        RDB_transaction *txp, RDB_expression **expp)
{
    RDB_expression *expv[2];

    expv[0] = arg1;
    expv[1] = arg2;
    return RDB_ro_op(opname, 2, expv, txp, expp);
}

/* Destroy the expression and all subexpressions */
void 
RDB_drop_expr(RDB_expression *exp)
{
    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            free(exp->var.op.name);
            RDB_drop_expr(exp->var.op.argv[0]);
            free(exp->var.op.argv);
            break;
        case RDB_EX_RO_OP:
        {
            int i;

            for (i = 0; i < exp->var.op.argc; i++)
                RDB_drop_expr(exp->var.op.argv[i]);
            free(exp->var.op.argv);
            break;
        }
        case RDB_EX_AGGREGATE:
            free(exp->var.op.name);
            break;
        case RDB_EX_OBJ:
            RDB_destroy_obj(&exp->var.obj);
            break;
        case RDB_EX_ATTR:
            free(exp->var.attrname);
            break;
    }
    free(exp);
}

static int
evaluate_ro_op(RDB_expression *exp, const RDB_object *tup,
        RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    int i;
    RDB_object **valpv;
    RDB_object *valv = NULL;
    int argc = exp->var.op.argc;

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
        ret = RDB_evaluate(exp->var.op.argv[i], tup, txp, &valv[i]);
        if (ret != RDB_OK)
            goto cleanup;
    }
    ret = RDB_call_ro_op(exp->var.op.name, argc, valpv, txp, valp);

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
            ret = RDB_evaluate(exp->var.op.argv[0], tup, txp, &tpl);
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
            ret = RDB_evaluate(exp->var.op.argv[0], tup, txp, &obj);
            if (ret != RDB_OK) {
                 RDB_destroy_obj(&obj);
                 return ret;
            }
            ret = RDB_obj_comp(&obj, exp->var.op.name, valp, txp);
            RDB_destroy_obj(&obj);
            return ret;
        }
        case RDB_EX_RO_OP:
            return evaluate_ro_op(exp, tup, txp, valp);
        case RDB_EX_ATTR:
        {
            if (tup != NULL) {
                RDB_object *srcp = RDB_tuple_get(tup, exp->var.attrname);
                if (srcp != NULL)
                    return RDB_copy_obj(valp, srcp);
            }

            RDB_errmsg(RDB_db_env(RDB_tx_db(txp)), "attribute %s not found",
                    exp->var.attrname);
            return RDB_INVALID_ARGUMENT;
        }
        case RDB_EX_OBJ:
            return RDB_copy_obj(valp, &exp->var.obj);
        case RDB_EX_AGGREGATE:
        {
            RDB_object val;

            RDB_init_obj(&val);
            ret = RDB_evaluate(exp->var.op.argv[0], tup, txp, &val);
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
        case RDB_EX_TUPLE_ATTR:
            return RDB_tuple_attr(RDB_dup_expr(exp->var.op.argv[0]),
                    exp->var.op.name);
        case RDB_EX_GET_COMP:
            return RDB_expr_comp(RDB_dup_expr(exp->var.op.argv[0]),
                    exp->var.op.name);
        case RDB_EX_RO_OP:
        {
            int i;
            RDB_expression *newexp;
            RDB_expression **argexpv = (RDB_expression **)
                    malloc(sizeof (RDB_expression *) * exp->var.op.argc);

            if (argexpv == NULL)
                return NULL;

            for (i = 0; i < exp->var.op.argc; i++) {
                argexpv[i] = RDB_dup_expr(exp->var.op.argv[i]);
                if (argexpv[i] == NULL)
                    return NULL;
            }
            newexp = _RDB_ro_op(exp->var.op.name, exp->var.op.argc,
                    argexpv);
            free(argexpv);
            return newexp;
        }
        case RDB_EX_AGGREGATE:
            return RDB_expr_aggregate(RDB_dup_expr(exp->var.op.argv[0]), exp->var.op.op,
                    exp->var.op.name);
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj);
        case RDB_EX_ATTR:
            return RDB_expr_attr(exp->var.attrname);
    }
    abort();
}

RDB_bool
_RDB_expr_refers(RDB_expression *exp, RDB_table *tbp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            if (exp->var.obj.kind == RDB_OB_TABLE)
                return RDB_table_refers(exp->var.obj.var.tbp, tbp);
            return RDB_FALSE;
        case RDB_EX_ATTR:
            return RDB_FALSE;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_expr_refers(exp->var.op.argv[0], tbp);
        case RDB_EX_RO_OP:
        {
            int i;

            for (i = 0; i < exp->var.op.argc; i++)
                if (_RDB_expr_refers(exp->var.op.argv[i], tbp))
                    return RDB_TRUE;
            
            return RDB_FALSE;
        }
        case RDB_EX_AGGREGATE:
            return (RDB_bool) (exp->var.op.argv[0]->var.obj.var.tbp == tbp);
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
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_invrename_expr(exp->var.op.argv[0], renc, renv);
        case RDB_EX_RO_OP:
            for (i = 0; i < exp->var.op.argc; i++) {
                ret = _RDB_invrename_expr(exp->var.op.argv[i], renc, renv);
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
                    i < renc && strcmp(renv[i].to, exp->var.attrname) != 0;
                    i++);

            /* If found, replace it */
            if (i < renc) {
                char *name = realloc(exp->var.attrname,
                        strlen(renv[i].from) + 1);

                if (name == NULL)
                    return RDB_NO_MEMORY;

                strcpy(name, renv[i].from);
                exp->var.attrname = name;
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
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_resolve_extend_expr(&(*expp)->var.op.argv[0],
                    attrc, attrv);
        case RDB_EX_RO_OP:
            for (i = 0; i < (*expp)->var.op.argc; i++) {
                ret = _RDB_resolve_extend_expr(&(*expp)->var.op.argv[i],
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
                    i < attrc && strcmp(attrv[i].name, (*expp)->var.attrname) != 0;
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

static RDB_bool
expr_attr(RDB_expression *exp, const char *attrname, char *opname)
{
    if (exp->kind == RDB_EX_RO_OP && strcmp(exp->var.op.name, opname) == 0) {
        if (exp->var.op.argv[0]->kind == RDB_EX_ATTR
                && strcmp(exp->var.op.argv[0]->var.attrname, attrname) == 0
                && exp->var.op.argv[1]->kind == RDB_EX_OBJ)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

RDB_expression *
_RDB_attr_node(RDB_expression *exp, const char *attrname, char *opname)
{
    while (exp->kind == RDB_EX_RO_OP
            && strcmp (exp->var.op.name, "AND") == 0) {
        if (expr_attr(exp->var.op.argv[1], attrname, opname))
            return exp;
        exp = exp->var.op.argv[0];
    }
    if (expr_attr(exp, attrname, opname))
        return exp;
    return NULL;
}
