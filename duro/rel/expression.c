/*
 * $Id$
 *
 * Copyright (C) 2004-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"
#include <gen/strfns.h>
#include <string.h>
#include <stdarg.h>

RDB_bool
RDB_expr_is_const(const RDB_expression *exp)
{
    int i;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            return RDB_TRUE;
        case RDB_EX_RO_OP:
            for (i = 0; i < exp->var.op.argc; i++) {
                if (!RDB_expr_is_const(exp->var.op.argv[i]))
                    return RDB_FALSE;
            }
            return RDB_TRUE;
        case RDB_EX_GET_COMP:
            return RDB_expr_is_const(exp->var.op.argv[0]);
        default: ;
    }
    return RDB_FALSE;
}

static int
wrap_type(const RDB_expression *exp, RDB_type **argtv, RDB_type **typp)
{
    int i, j;
    int ret;
    int wrapc;
    RDB_wrapping *wrapv;

    if (exp->var.op.argc < 1 || exp->var.op.argc %2 != 1)
        return RDB_INVALID_ARGUMENT;

    wrapc = exp->var.op.argc % 2;
    if (wrapc > 0) {
        wrapv = malloc(sizeof(RDB_wrapping) * wrapc);
        if (wrapv == NULL)
            return RDB_NO_MEMORY;
    }
    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrv = NULL;
    }

    for (i = 0; i < wrapc; i++) {
        RDB_object *objp;

        wrapv[i].attrc =  RDB_array_length(&exp->var.op.argv[i * 2 + 1]->var.obj);
        wrapv[i].attrv = malloc(sizeof (char *) * wrapv[i].attrc);
        for (j = 0; j < wrapv[i].attrc; j++) {
            ret = RDB_array_get(&exp->var.op.argv[i * 2 + 1]->var.obj, (RDB_int) j,
                                &objp);
            if (ret != RDB_OK) {
                goto cleanup;
            }
            wrapv[i].attrv[j] = RDB_obj_string(objp);
        }        
        wrapv[i].attrname = RDB_obj_string(&exp->var.op.argv[i * 2 + 2]->var.obj);
    }

    if (argtv[0]->kind == RDB_TP_RELATION) {
        ret = RDB_wrap_relation_type(argtv[0], wrapc, wrapv, typp);
    } else {
        ret = RDB_wrap_tuple_type(argtv[0], wrapc, wrapv, typp);
    }

cleanup:
    for (i = 0; i < wrapc; i++) {
        free(wrapv[i].attrv);
    }
    if (wrapc > 0)
        free(wrapv);
    return ret;
}

static RDB_expression *
expr_resolve_attrs(const RDB_expression *exp, const RDB_object *tplp)
{
    RDB_object *objp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            return RDB_tuple_attr(
                    expr_resolve_attrs(exp->var.op.argv[0], tplp),
                            exp->var.op.name);
        case RDB_EX_GET_COMP:
            return RDB_expr_comp(
                    expr_resolve_attrs(exp->var.op.argv[0], tplp),
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
                argexpv[i] = expr_resolve_attrs(exp->var.op.argv[i], tplp);
                if (argexpv[i] == NULL)
                    return NULL;
            }
            newexp = RDB_ro_op(exp->var.op.name, exp->var.op.argc, argexpv);
            free(argexpv);
            return newexp;
        }
        case RDB_EX_AGGREGATE:
            return RDB_expr_aggregate(
                    expr_resolve_attrs(exp->var.op.argv[0], tplp),
                            exp->var.op.op, exp->var.op.name);
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj);
        case RDB_EX_ATTR:
            if (tplp != NULL) {
                objp = RDB_tuple_get(tplp, exp->var.attrname);
                if (objp != NULL)
                    return RDB_obj_to_expr(objp);
            }
            return RDB_expr_attr(exp->var.attrname);
    }
    abort();
}

static RDB_summarize_add *
expv_to_addv(int addc, RDB_expression **expv, const RDB_object *tplp)
{
    int i;
    RDB_summarize_add *addv = malloc(sizeof (RDB_summarize_add) * addc);
    if (addv == NULL)
        return NULL;

    for (i = 0; i < addc; i++) {
        addv[i].op = expv[i]->var.op.op;
        if (addv[i].op != RDB_COUNT) {
            addv[i].exp = expr_resolve_attrs(expv[i]->var.op.argv[0],
                    tplp);
        } else {
            addv[i].exp = NULL;
        }
        addv[i].name = expv[i]->var.op.name;
    }
    return addv;
}

static int
expr_op_type(const RDB_expression *exp, const RDB_type *tuptyp,
        RDB_transaction *txp, RDB_type **typp)
{
    int i;
    int ret;
    RDB_ro_op_desc *op;
    RDB_type *tbtyp;
    RDB_type **argtv;

    /*
     * Handle built-in relational operators
     */
    if (exp->var.op.argc == 2
            && strcmp(exp->var.op.name, "WHERE") == 0) {
        ret = RDB_expr_type(exp->var.op.argv[0], tuptyp, txp, typp);
        if (ret != RDB_OK)
            return ret;
        return (*typp)->kind == RDB_TP_RELATION ? RDB_OK : RDB_TYPE_MISMATCH;
    }
    if (exp->var.op.argc >= 1 && strcmp(exp->var.op.name, "EXTEND") == 0) {
        int attrc = (exp->var.op.argc - 1) / 2;
        RDB_attr *attrv;

        ret = RDB_expr_type(exp->var.op.argv[0], tuptyp, txp, &tbtyp);
        if (ret != RDB_OK) {
            return ret;
        }

        attrv = malloc(sizeof (RDB_attr) * attrc);
        if (attrv == NULL) {
            RDB_drop_type(tbtyp, NULL);
            return RDB_NO_MEMORY;
        }
        for (i = 0; i < attrc; i++) {
            ret = RDB_expr_type(exp->var.op.argv[1 + i * 2], tuptyp, txp,
                    &attrv[i].typ);
            if (ret != RDB_OK) {
                free(attrv);
                RDB_drop_type(tbtyp, NULL);
                return ret;
            }
            if (exp->var.op.argv[2 + i * 2]->kind != RDB_EX_OBJ ||
                    exp->var.op.argv[2 + i * 2]->var.obj.typ != &RDB_STRING) {
                RDB_drop_type(tbtyp, NULL);
                return RDB_INVALID_ARGUMENT;
            }
            attrv[i].name = RDB_obj_string(&exp->var.op.argv[2 + i * 2]->var.obj);
        }
        switch (tbtyp->kind) {
            case RDB_TP_RELATION:
                ret = RDB_extend_relation_type(tbtyp, attrc, attrv, typp);
                break;
            case RDB_TP_TUPLE:
                ret = RDB_extend_tuple_type(tbtyp, attrc, attrv, typp);
                break;
            default:
                ret = RDB_TYPE_MISMATCH;
        }
        free(attrv);
        RDB_drop_type(tbtyp, NULL);
        return ret;
    }
    if (exp->var.op.argc >= 1 && exp->var.op.argc % 2 == 1
            && strcmp(exp->var.op.name, "RENAME") == 0) {
        int renc = (exp->var.op.argc - 1) / 2;
        RDB_renaming *renv = NULL;

        ret = RDB_expr_type(exp->var.op.argv[0], tuptyp, txp, &tbtyp);
        if (ret != RDB_OK) {
            return ret;
        }
        
        if (renc > 0) {
            renv = malloc(sizeof (RDB_renaming) * renc);
            if (renv == NULL) {
                RDB_drop_type(tbtyp, NULL);
                return RDB_NO_MEMORY;
            }
        }

        for (i = 0; i < renc; i++) {
            if (exp->var.op.argv[1 + i * 2]->kind != RDB_EX_OBJ ||
                    exp->var.op.argv[1 + i * 2]->var.obj.typ != &RDB_STRING) {
                free(renv);
                RDB_drop_type(tbtyp, NULL);
                return RDB_INVALID_ARGUMENT;
            }
            if (exp->var.op.argv[2 + i * 2]->kind != RDB_EX_OBJ ||
                    exp->var.op.argv[2 + i * 2]->var.obj.typ != &RDB_STRING) {
                RDB_drop_type(tbtyp, NULL);
                return RDB_INVALID_ARGUMENT;
            }
            renv[i].from = RDB_obj_string(&exp->var.op.argv[1 + i * 2]->var.obj);
            renv[i].to = RDB_obj_string(&exp->var.op.argv[1 + i * 2]->var.obj);
        }

        switch (tbtyp->kind) {
            case RDB_TP_RELATION:
                ret = RDB_rename_relation_type(tbtyp, renc, renv, typp);
                break;
            case RDB_TP_TUPLE:
                ret = RDB_rename_tuple_type(tbtyp, renc, renv, typp);
                break;
            default:
                ret = RDB_TYPE_MISMATCH;
        }
        free(renv);
        RDB_drop_type(tbtyp, NULL);
        return ret;
    }
    argtv = malloc(sizeof (RDB_type *) * exp->var.op.argc);
    if (argtv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < exp->var.op.argc; i++) {
        ret = RDB_expr_type(exp->var.op.argv[i], tuptyp, txp, &argtv[i]);
        if (ret == RDB_NOT_FOUND)
            argtv[i] = NULL;
        else if (ret != RDB_OK) {
            free(argtv);
            return ret;
        }
    }

    /*
     * Handle nonscalar comparison
     */
    if (exp->var.op.argc == 2
            && (argtv[0] == NULL || !RDB_type_is_scalar(argtv[0]))
            && (argtv[1] == NULL || !RDB_type_is_scalar(argtv[1]))
            && (strcmp(exp->var.op.name, "=") == 0
                    || strcmp(exp->var.op.name, "<>") == 0)) {
        *typp = &RDB_BOOLEAN;
        free(argtv);
        return RDB_OK;
    }

    /*
     * Handle IF-THEN-ELSE
     */
    if (strcmp(exp->var.op.name, "IF") == 0 && exp->var.op.argc == 3) {
        if (argtv[0] != &RDB_BOOLEAN || !RDB_type_equals(argtv[1], argtv[2])) {
            free(argtv);
            return RDB_TYPE_MISMATCH;
        }
        *typp = _RDB_dup_nonscalar_type(argtv[1]);
        free(argtv);
        return RDB_OK;
    }

    /*
     * Handle built-in scalar operators with relational arguments
     */
    if (exp->var.op.argc == 1
            && (argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION)
            && strcmp(exp->var.op.name, "IS_EMPTY") == 0) {
        *typp = &RDB_BOOLEAN;
        free(argtv);
        return RDB_OK;
    } else if (exp->var.op.argc == 1
            && (argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION)
            && strcmp(exp->var.op.name, "COUNT") == 0) {
        *typp = &RDB_INTEGER;
        free(argtv);
        return RDB_OK;
    } else if (exp->var.op.argc == 2
            && (argtv[1] != NULL && argtv[1]->kind == RDB_TP_RELATION)
            && (strcmp(exp->var.op.name, "IN") == 0
                    || strcmp(exp->var.op.name, "SUBSET_OF") == 0)) {
        *typp = &RDB_BOOLEAN;
        free(argtv);
        return RDB_OK;
    }

    /*
     * Handle relational operators
     */
    if (argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && strcmp(exp->var.op.name, "PROJECT") == 0) {
        char **attrv;

        for (i = 1; i < exp->var.op.argc; i++) {
            if (exp->var.op.argv[i]->kind != RDB_EX_OBJ
                    || exp->var.op.argv[i]->var.obj.typ != &RDB_STRING) {
                free(argtv);
                return RDB_TYPE_MISMATCH;
            }
        }
        attrv = malloc(sizeof (char *) * (exp->var.op.argc - 1));
        if (attrv == NULL) {
            free(argtv);
            return RDB_NO_MEMORY;
        }
        for (i = 1; i < exp->var.op.argc; i++) {
            attrv[i - 1] = RDB_obj_string(&exp->var.op.argv[i]->var.obj);
        }
        ret = RDB_project_relation_type(argtv[0], exp->var.op.argc - 1, attrv,
                typp);
        free(attrv);
        free(argtv);
        return ret;
    }
    if (argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && strcmp(exp->var.op.name, "REMOVE") == 0) {
        int attrc;
        int attri;
        char **attrv;

        for (i = 1; i < exp->var.op.argc; i++) {
            if (exp->var.op.argv[i]->kind != RDB_EX_OBJ
                    || exp->var.op.argv[i]->var.obj.typ != &RDB_STRING) {
                free(argtv);
                return RDB_TYPE_MISMATCH;
            }

            /* Check if attribute exists */
            if (RDB_type_attr_type(argtv[0],
                    RDB_obj_string(&exp->var.op.argv[i]->var.obj)) == NULL) {
                free(argtv);
                return RDB_ATTRIBUTE_NOT_FOUND;
            }
        }
        attrc = argtv[0]->var.basetyp->var.tuple.attrc - exp->var.op.argc + 1;
        attrv = malloc(sizeof (char *) * attrc);
        if (attrv == NULL) {
            free(argtv);
            return RDB_NO_MEMORY;
        }

        /* Collect attributes which are not removed */
        attri = 0;
        for (i = 0; i < argtv[0]->var.basetyp->var.tuple.attrc; i++) {
            int j = 1;
            char *attrname = argtv[0]->var.basetyp->var.tuple.attrv[i].name;

            while (j < exp->var.op.argc
                    && strcmp(RDB_obj_string(&exp->var.op.argv[j]->var.obj),
                              attrname) != 0)
                j++;
            if (j >= exp->var.op.argc) {
                /* Attribute does not appear in arguments - add */
                attrv[attri++] = attrname;
            }
        }
        if (attri < attrc) {
            free(argtv);
            free(attrv);
            return RDB_INVALID_ARGUMENT;
        }
        
        ret = RDB_project_relation_type(argtv[0], attrc, attrv, typp);
        free(attrv);
        free(argtv);
        return ret;
    }

    if (exp->var.op.argc >= 2
            && argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && argtv[1] != NULL && argtv[1]->kind == RDB_TP_RELATION
            && strcmp(exp->var.op.name, "SUMMARIZE") == 0) {
        int addc = exp->var.op.argc - 2;
        RDB_summarize_add *addv = expv_to_addv(addc, exp->var.op.argv + 2,
                NULL);
        if (addv == NULL) {
            free(argtv);
            return RDB_NO_MEMORY;
        }

        ret = RDB_summarize_type(argtv[0], argtv[1], addc, addv, 0, NULL,
                txp, typp);
        for (i = 0; i < addc; i++) {
            if (addv[i].exp != NULL)
                RDB_drop_expr(addv[i].exp);
        }
        free(addv);

        return ret;
    }
    if (argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && strcmp(exp->var.op.name, "WRAP") == 0) {
        ret = wrap_type(exp, argtv, typp);
        free(argtv);
        return ret;
    }
    if (exp->var.op.argc >= 1 && argtv[0] != NULL
            && (argtv[0]->kind == RDB_TP_RELATION
                || argtv[0]->kind == RDB_TP_TUPLE)
            && strcmp(exp->var.op.name, "UNWRAP") == 0) {
        char **attrv;
        int attrc = exp->var.op.argc - 1;

        for (i = 1; i < exp->var.op.argc; i++) {
            if (exp->var.op.argv[i]->kind != RDB_EX_OBJ
                    || exp->var.op.argv[i]->var.obj.typ != &RDB_STRING) {
                free(argtv);
                return RDB_TYPE_MISMATCH;
            }
        }

        attrv = malloc(attrc * sizeof (char *));
        if (attrv == NULL) {
            free(argtv);
            return RDB_NO_MEMORY;
        }
        for (i = 0; i < attrc; i++) {
            attrv[i] = RDB_obj_string(&exp->var.op.argv[i + 1]->var.obj);
        }

        if (argtv[0]->kind == RDB_TP_TUPLE) {
            ret = RDB_unwrap_tuple_type(argtv[0], attrc, attrv, typp);
        } else {
            ret = RDB_unwrap_relation_type(argtv[0], attrc, attrv, typp);
        }

        free(attrv);
        free(argtv);
        return ret;
    }
    if (argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && strcmp(exp->var.op.name, "GROUP") == 0) {
        char **attrv;

        if (exp->var.op.argc < 2) {
            free(argtv);
            return RDB_INVALID_ARGUMENT;
        }

        for (i = 1; i < exp->var.op.argc; i++) {
            if (exp->var.op.argv[i]->kind != RDB_EX_OBJ
                    || exp->var.op.argv[i]->var.obj.typ != &RDB_STRING) {
                free(argtv);
                return RDB_TYPE_MISMATCH;
            }
        }

        if (exp->var.op.argc > 2) {
            attrv = malloc(sizeof (char *) * (exp->var.op.argc - 2));
            if (attrv == NULL) {
                free(argtv);
                return RDB_NO_MEMORY;
            }
        } else {
            attrv = NULL;
        }

        for (i = 1; i < exp->var.op.argc - 1; i++) {
            attrv[i - 1] = RDB_obj_string(&exp->var.op.argv[i]->var.obj);
        }
        ret = RDB_group_type(argtv[0], exp->var.op.argc - 2, attrv,
                RDB_obj_string(&exp->var.op.argv[exp->var.op.argc - 1]->var.obj),
                typp);
        free(attrv);
        free(argtv);
        return ret;
    }
    if (exp->var.op.argc == 2
            && argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && strcmp(exp->var.op.name, "UNGROUP") == 0) {
        if (exp->var.op.argv[1]->kind != RDB_EX_OBJ
                || exp->var.op.argv[1]->var.obj.typ != &RDB_STRING) {
            free(argtv);
            return RDB_TYPE_MISMATCH;
        }

        ret = RDB_ungroup_type(argtv[0],
                RDB_obj_string(&exp->var.op.argv[1]->var.obj), typp);
        free(argtv);
        return ret;
    }
    if (exp->var.op.argc == 2
            && argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && argtv[1] != NULL && argtv[1]->kind == RDB_TP_RELATION
            && strcmp(exp->var.op.name, "JOIN") == 0) {
        ret = RDB_join_relation_types(argtv[0], argtv[1], typp);
        free(argtv);
        return ret;
    }
    if (argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && (strcmp(exp->var.op.name, "UNION") == 0
                || strcmp(exp->var.op.name, "MINUS") == 0
                || strcmp(exp->var.op.name, "INTERSECT") == 0
                || strcmp(exp->var.op.name, "DIVIDE_BY_PER") == 0)) {
        *typp = argtv[0];
        free(argtv);
        return RDB_OK;
    }
    if (argtv[0] != NULL && argtv[0]->kind == RDB_TP_RELATION
            && strcmp(exp->var.op.name, "TO_TUPLE") == 0) {
        *typp = argtv[0]->var.basetyp;
        free(argtv);
        return RDB_OK;
    }

    for (i = 0; i < exp->var.op.argc; i++) {
        if (argtv[i] == NULL) {
            free(argtv);
            return RDB_OPERATOR_NOT_FOUND;
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

            /*
             * Nonscalar types are managed by the caller, so
             * duplicate it
             */
            if (!RDB_type_is_scalar(*typp)) {
                *typp = _RDB_dup_nonscalar_type(*typp);
                if (*typp == NULL)
                    return RDB_NO_MEMORY;
            }
            break;
        case RDB_EX_ATTR:
            attrp = _RDB_tuple_type_attr(tuptyp, exp->var.attrname);
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

int
_RDB_check_expr_type(const RDB_expression *exp, const RDB_type *tuptyp,
        const RDB_type *checktyp, RDB_transaction *txp)
{
    RDB_type *typ;
    int ret = RDB_expr_type(exp, tuptyp, txp, &typ);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_type_equals(typ, checktyp) ? RDB_OK : RDB_TYPE_MISMATCH;
    if (!RDB_type_is_scalar(typ)) {
        ret = RDB_drop_type(typ, NULL);
        if (ret != RDB_OK)
            return ret;
    }
    return ret;
}

int
_RDB_expr_equals(const RDB_expression *ex1p, const RDB_expression *ex2p,
        RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    int i;

    if (ex1p->kind != ex2p->kind) {
        *resp = RDB_FALSE;
        return RDB_OK;
    }

    switch (ex1p->kind) {
        case RDB_EX_OBJ:
            return RDB_obj_equals(&ex1p->var.obj, &ex2p->var.obj, txp, resp);
        case RDB_EX_ATTR:
            *resp = (RDB_bool)
                    (strcmp (ex1p->var.attrname, ex2p->var.attrname) == 0);
            break;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            ret = _RDB_expr_equals(ex1p->var.op.argv[0], ex2p->var.op.argv[0],
                    txp, resp);
            if (ret != RDB_OK)
                return ret;
            *resp = (RDB_bool)
                    (strcmp (ex1p->var.op.name, ex2p->var.op.name) == 0);
            break;
        case RDB_EX_RO_OP:
            if (ex1p->var.op.argc != ex2p->var.op.argc
                    || strcmp(ex1p->var.op.name, ex2p->var.op.name) != 0) {
                *resp = RDB_FALSE;
                return RDB_OK;
            }
            for (i = 0; i < ex1p->var.op.argc; i++) {
                ret = _RDB_expr_equals(ex1p->var.op.argv[i],
                        ex2p->var.op.argv[i], txp, resp);
                if (ret != RDB_OK)
                    return ret;
                if (!*resp)
                    return RDB_OK;
            }
            *resp = RDB_TRUE;
            break;
        case RDB_EX_AGGREGATE:
            if (ex1p->var.op.op != ex2p->var.op.op) {
                *resp = RDB_FALSE;
                return RDB_OK;
            }
            if (!_RDB_table_def_equals(ex1p->var.op.argv[0]->var.obj.var.tbp,
                    ex2p->var.op.argv[0]->var.obj.var.tbp, txp)) {
                *resp = RDB_FALSE;
                return RDB_OK;
            }
            if (ex1p->var.op.op == RDB_COUNT) {
                *resp = RDB_TRUE;
            } else {
                *resp = (RDB_bool) (strcmp (ex1p->var.op.name,
                        ex2p->var.op.name) == 0);
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
RDB_ro_op(const char *opname, int argc, RDB_expression *argv[])
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

enum {
    EXPV_LEN = 64
};

RDB_expression *
RDB_ro_op_va(const char *opname, RDB_expression *arg, ...
        /* (RDB_expression *) NULL */ )
{
    va_list ap;
    RDB_expression *argv[EXPV_LEN];
    int argc = 0;

    va_start(ap, arg);
    while (arg != NULL) {
        if (argc >= EXPV_LEN)
            return NULL;
        argv[argc++] = arg;
        arg = va_arg(ap, RDB_expression *);
    }
    va_end(ap);
    return RDB_ro_op(opname, argc, argv);
}

RDB_expression *
RDB_eq(RDB_expression *arg1, RDB_expression *arg2)
{
    RDB_expression *argv[2];

    argv[0] = arg1;
    argv[1] = arg2;
    return RDB_ro_op("=", 2, argv);
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

            free(exp->var.op.name);
            for (i = 0; i < exp->var.op.argc; i++)
                RDB_drop_expr(exp->var.op.argv[i]);
            free(exp->var.op.argv);
            break;
        }
        case RDB_EX_AGGREGATE:
            free(exp->var.op.name);
            RDB_drop_expr(exp->var.op.argv[0]);
            free(exp->var.op.argv);
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
evaluate_where(RDB_expression *exp, const RDB_object *tplp,
        RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    RDB_object tobj;
    RDB_table *vtbp;
    RDB_expression *wherep;
    int argc = exp->var.op.argc;

    if (argc != 2)
        return RDB_OPERATOR_NOT_FOUND;

    RDB_init_obj(&tobj);
    ret = RDB_evaluate(exp->var.op.argv[0], tplp, txp, &tobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tobj);
        return ret;
    }
    if (tobj.kind != RDB_OB_TABLE) {
        RDB_destroy_obj(&tobj);
        return RDB_TYPE_MISMATCH;
    }

    wherep = expr_resolve_attrs(exp->var.op.argv[1], tplp);
    if (wherep == NULL) {
        RDB_destroy_obj(&tobj);
        return RDB_NO_MEMORY;
    }

    ret = RDB_select(RDB_obj_table(&tobj), wherep, txp, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        RDB_destroy_obj(&tobj);
        return ret;
    }

    RDB_table_to_obj(valp, vtbp);
    tobj.var.tbp = NULL;
    RDB_destroy_obj(&tobj);

    return RDB_OK;  
}

static int
evaluate_extend(RDB_expression *exp, const RDB_object *tplp,
        RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    int i;
    RDB_object tobj;
    RDB_table *tbp;
    RDB_table *vtbp;
    RDB_virtual_attr *attrv;
    int argc = exp->var.op.argc;
    int attrc = (argc - 1) / 2;

    if (argc < 1)
        return RDB_OPERATOR_NOT_FOUND;

    RDB_init_obj(&tobj);
    ret = RDB_evaluate(exp->var.op.argv[0], tplp, txp, &tobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tobj);
        return ret;
    }
    attrv = malloc(sizeof (RDB_virtual_attr) * attrc);
    if (attrv == NULL)
        return RDB_NO_MEMORY;
    for (i = 0; i < attrc; i++) {
        attrv[i].exp = expr_resolve_attrs(exp->var.op.argv[1 + i * 2], tplp);
        if (attrv[i].exp == NULL) {
            free(attrv);
            return RDB_NO_MEMORY;
        }
        if (exp->var.op.argv[2 + i * 2]->kind != RDB_EX_OBJ ||
                exp->var.op.argv[2 + i * 2]->var.obj.typ != &RDB_STRING)
            return RDB_INVALID_ARGUMENT;
        attrv[i].name = RDB_obj_string(&exp->var.op.argv[2 + i * 2]->var.obj);
    }

    tbp = RDB_obj_table(&tobj);
    if (tbp != NULL) {
        ret = RDB_extend(tbp, attrc, attrv, txp, &vtbp);
        if (ret != RDB_OK) {
            for (i = 0; i < attrc; i++) {
                RDB_drop_expr(attrv[i].exp);
            }
            free(attrv);
            RDB_destroy_obj(&tobj);
            return ret;
        }
        free(attrv);
        RDB_table_to_obj(valp, vtbp);
        tobj.var.tbp = NULL;
    } else {
        if (tobj.kind != RDB_OB_TUPLE) {
            RDB_destroy_obj(&tobj);
            free(attrv);
            return RDB_TYPE_MISMATCH;
        }

        ret = RDB_extend_tuple(&tobj, attrc, attrv, txp);
        for (i = 0; i < attrc; i++) {
            RDB_drop_expr(attrv[i].exp);
        }
        free(attrv);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tobj);
            return ret;
        }
        ret = RDB_copy_obj(valp, &tobj);
    }

    RDB_destroy_obj(&tobj);
    return ret;
}

static int
evaluate_summarize(RDB_expression *exp, const RDB_object *tplp,
        RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    int i;
    RDB_object t1obj, t2obj;
    RDB_table *tb1p, *tb2p;
    RDB_table *vtbp;
    RDB_summarize_add *addv;
    int argc = exp->var.op.argc;
    int addc = argc - 2;

    if (argc < 2)
        return RDB_OPERATOR_NOT_FOUND;

    RDB_init_obj(&t1obj);
    RDB_init_obj(&t2obj);
    ret = RDB_evaluate(exp->var.op.argv[0], tplp, txp, &t1obj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&t1obj);
        RDB_destroy_obj(&t2obj);
        return ret;
    }
    ret = RDB_evaluate(exp->var.op.argv[1], tplp, txp, &t2obj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&t1obj);
        RDB_destroy_obj(&t2obj);
        return ret;
    }

    tb1p = RDB_obj_table(&t1obj);
    if (tb1p == NULL)
        return RDB_INVALID_ARGUMENT;
    tb2p = RDB_obj_table(&t2obj);
    if (tb2p == NULL)
        return RDB_INVALID_ARGUMENT;
    addv = expv_to_addv(addc, exp->var.op.argv + 2, tplp);
    if (addv == NULL)
        return RDB_NO_MEMORY;
    ret = RDB_summarize(tb1p, tb2p, addc, addv, txp, &vtbp);
    if (ret != RDB_OK) {
        for (i = 0; i < addc; i++) {
            if (addv[i].exp != NULL)
                RDB_drop_expr(addv[i].exp);
        }
        free(addv);
        RDB_destroy_obj(&t1obj);
        RDB_destroy_obj(&t2obj);
        return ret;
    }
    free(addv);
    RDB_table_to_obj(valp, vtbp);
    t1obj.var.tbp = NULL;
    t2obj.var.tbp = NULL;

    RDB_destroy_obj(&t1obj);
    RDB_destroy_obj(&t2obj);
    return RDB_OK;
}

static int
evaluate_ro_op(RDB_expression *exp, const RDB_object *tplp,
        RDB_transaction *txp, RDB_object *valp)
{
    int ret;
    int i;
    RDB_object **valpv;
    RDB_object *valv = NULL;
    int argc = exp->var.op.argc;

    /*
     * Special treatment for WHERE and EXTEND, because the second argument
     * must be an expression and must therefore not be evaluated
     */
    if (strcmp(exp->var.op.name, "WHERE") == 0) {
        return evaluate_where(exp, tplp, txp, valp);
    }
    if (strcmp(exp->var.op.name, "EXTEND") == 0) {
        return evaluate_extend(exp, tplp, txp, valp);
    }
    if (strcmp(exp->var.op.name, "SUMMARIZE") == 0) {
        return evaluate_summarize(exp, tplp, txp, valp);
    }

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
        ret = RDB_evaluate(exp->var.op.argv[i], tplp, txp, &valv[i]);
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
    int ret;
    RDB_rational avg;
    RDB_bool b;

    switch(op) {
        case RDB_COUNT:
            ret = RDB_cardinality(tbp, txp);
            if (ret < 0)
                return ret;
            RDB_int_to_obj(resultp, ret);
            return RDB_OK;
        case RDB_SUM:
            return RDB_sum(tbp, attrname, txp, resultp);
        case RDB_AVG:
            ret = RDB_avg(tbp, attrname, txp, &avg);
            if (ret != RDB_OK)
                return ret;
            RDB_rational_to_obj(resultp, avg);
            return RDB_OK;
        case RDB_MAX:
            return RDB_max(tbp, attrname, txp, resultp);
        case RDB_MIN:
            return RDB_min(tbp, attrname, txp, resultp);
        case RDB_ALL:
            ret = RDB_all(tbp, attrname, txp, &b);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(resultp, b);
            return RDB_OK;
        case RDB_ANY:
            ret = RDB_any(tbp, attrname, txp, &b);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(resultp, b);
            return RDB_OK;
        default: ;
    }
    abort();
}

int
RDB_evaluate(RDB_expression *exp, const RDB_object *tplp, RDB_transaction *txp,
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
            ret = RDB_evaluate(exp->var.op.argv[0], tplp, txp, &tpl);
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
            ret = RDB_evaluate(exp->var.op.argv[0], tplp, txp, &obj);
            if (ret != RDB_OK) {
                 RDB_destroy_obj(&obj);
                 return ret;
            }
            ret = RDB_obj_comp(&obj, exp->var.op.name, valp, txp);
            RDB_destroy_obj(&obj);
            return ret;
        }
        case RDB_EX_RO_OP:
            return evaluate_ro_op(exp, tplp, txp, valp);
        case RDB_EX_ATTR:
            if (tplp != NULL) {
                RDB_object *srcp = RDB_tuple_get(tplp, exp->var.attrname);
                if (srcp != NULL)
                    return RDB_copy_obj(valp, srcp);
            }
            RDB_errmsg(RDB_db_env(RDB_tx_db(txp)), "attribute %s not found",
                    exp->var.attrname);
            return RDB_INVALID_ARGUMENT;
        case RDB_EX_OBJ:
            return RDB_copy_obj(valp, &exp->var.obj);
        case RDB_EX_AGGREGATE:
        {
            RDB_object val;

            RDB_init_obj(&val);
            ret = RDB_evaluate(exp->var.op.argv[0], tplp, txp, &val);
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
RDB_evaluate_bool(RDB_expression *exp, const RDB_object *tplp, RDB_transaction *txp,
                  RDB_bool *resp)
{
    int ret;
    RDB_object val;

    RDB_init_obj(&val);
    ret = RDB_evaluate(exp, tplp, txp, &val);
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
    RDB_expression *newexp;

    switch (exp->kind) {
        case RDB_EX_TUPLE_ATTR:
            newexp = RDB_dup_expr(exp->var.op.argv[0]);
            if (newexp == NULL)
                return NULL;
            return RDB_tuple_attr(newexp, exp->var.op.name);
        case RDB_EX_GET_COMP:
            newexp = RDB_dup_expr(exp->var.op.argv[0]);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_comp(newexp, exp->var.op.name);
        case RDB_EX_RO_OP:
        {
            int i;
            RDB_expression **argexpv = (RDB_expression **)
                    malloc(sizeof (RDB_expression *) * exp->var.op.argc);

            if (argexpv == NULL)
                return NULL;

            for (i = 0; i < exp->var.op.argc; i++) {
                argexpv[i] = RDB_dup_expr(exp->var.op.argv[i]);
                if (argexpv[i] == NULL)
                    return NULL;
            }
            newexp = RDB_ro_op(exp->var.op.name, exp->var.op.argc, argexpv);
            free(argexpv);
            return newexp;
        }
        case RDB_EX_AGGREGATE:
            newexp = RDB_dup_expr(exp->var.op.argv[0]);
            if (newexp == NULL)
                return NULL;
            return RDB_expr_aggregate(newexp, exp->var.op.op,
                    exp->var.op.name);
        case RDB_EX_OBJ:
            return RDB_obj_to_expr(&exp->var.obj);
        case RDB_EX_ATTR:
            return RDB_expr_attr(exp->var.attrname);
    }
    abort();
}

RDB_bool
_RDB_expr_expr_depend(const RDB_expression *ex1p, const RDB_expression *ex2p)
{
    switch (ex1p->kind) {
        case RDB_EX_OBJ:
            if (ex1p->var.obj.kind == RDB_OB_TABLE)
                return _RDB_expr_table_depend(ex2p, ex1p->var.obj.var.tbp);
            return RDB_FALSE;
        case RDB_EX_ATTR:
            return RDB_FALSE;
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_expr_expr_depend(ex1p->var.op.argv[0], ex2p);
        case RDB_EX_RO_OP:
        {
            int i;

            for (i = 0; i < ex1p->var.op.argc; i++)
                if (_RDB_expr_expr_depend(ex1p->var.op.argv[i], ex2p))
                    return RDB_TRUE;
            
            return RDB_FALSE;
        }
        case RDB_EX_AGGREGATE:
            if (ex2p->kind != RDB_EX_OBJ || ex2p->var.obj.kind != RDB_OB_TABLE)
                return RDB_FALSE;
            return (RDB_bool) (ex1p->var.op.argv[0]->var.obj.var.tbp ==
                    ex2p->var.obj.var.tbp);
    }
    /* Should never be reached */
    abort();
}

RDB_bool
_RDB_expr_refers(const RDB_expression *exp, RDB_table *tbp)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            if (exp->var.obj.kind == RDB_OB_TABLE)
                return _RDB_table_refers(exp->var.obj.var.tbp, tbp);
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

RDB_bool
_RDB_expr_refers_attr(const RDB_expression *exp, const char *attrname)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            return RDB_FALSE;
        case RDB_EX_ATTR:
            return (RDB_bool) (strcmp(exp->var.attrname, attrname) == 0);
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            return _RDB_expr_refers_attr(exp->var.op.argv[0], attrname);
        case RDB_EX_RO_OP:
        {
            int i;

            for (i = 0; i < exp->var.op.argc; i++) {
                if (_RDB_expr_refers_attr(exp->var.op.argv[i], attrname))
                    return RDB_TRUE;
            }            
            return RDB_FALSE;
        }
        case RDB_EX_AGGREGATE:
            return (RDB_bool) (strcmp(exp->var.op.name, attrname) == 0);
    }
    /* Should never be reached */
    abort();
}

/*
 * Check if there is some table which both exp and tbp depend on
 */
RDB_bool
_RDB_expr_table_depend(const RDB_expression *exp, RDB_table *tbp)
{
    int i;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            return _RDB_expr_refers(exp, tbp);
        case RDB_TB_SELECT:
            if (_RDB_expr_expr_depend(tbp->var.select.exp, exp))
                return RDB_TRUE;
            return _RDB_expr_table_depend(exp, tbp->var.select.tbp);
        case RDB_TB_UNION:
            if (_RDB_expr_table_depend(exp, tbp->var._union.tb1p))
                return RDB_TRUE;
            return _RDB_expr_table_depend(exp, tbp->var._union.tb2p);
        case RDB_TB_MINUS:
            if (_RDB_expr_table_depend(exp, tbp->var.minus.tb1p))
                return RDB_TRUE;
            return _RDB_expr_table_depend(exp, tbp->var.minus.tb2p);
        case RDB_TB_INTERSECT:
            if (_RDB_expr_table_depend(exp, tbp->var.intersect.tb1p))
                return RDB_TRUE;
            return _RDB_expr_table_depend(exp, tbp->var.intersect.tb2p);
        case RDB_TB_JOIN:
            if (_RDB_expr_table_depend(exp, tbp->var.join.tb1p))
                return RDB_TRUE;
            return _RDB_expr_table_depend(exp, tbp->var.join.tb2p);
        case RDB_TB_EXTEND:
            for (i = 0; i < tbp->var.extend.attrc; i++) {
                if (_RDB_expr_expr_depend(tbp->var.extend.attrv[i].exp, exp))
                    return RDB_TRUE;
            }
            return _RDB_expr_table_depend(exp, tbp->var.extend.tbp);
        case RDB_TB_PROJECT:
            return _RDB_expr_table_depend(exp, tbp->var.project.tbp);
        case RDB_TB_SUMMARIZE:
            for (i = 0; i < tbp->var.summarize.addc; i++) {
                if (_RDB_expr_expr_depend(tbp->var.summarize.addv[i].exp, exp))
                    return RDB_TRUE;
            }
            if (_RDB_expr_table_depend(exp, tbp->var.summarize.tb1p))
                return RDB_TRUE;
            return _RDB_expr_table_depend(exp, tbp->var.summarize.tb2p);
        case RDB_TB_RENAME:
            return _RDB_expr_table_depend(exp, tbp->var.rename.tbp);
        case RDB_TB_WRAP:
            return _RDB_expr_table_depend(exp, tbp->var.wrap.tbp);
        case RDB_TB_UNWRAP:
            return _RDB_expr_table_depend(exp, tbp->var.unwrap.tbp);
        case RDB_TB_GROUP:
            return _RDB_expr_table_depend(exp, tbp->var.group.tbp);
        case RDB_TB_UNGROUP:
            return _RDB_expr_table_depend(exp, tbp->var.ungroup.tbp);
        case RDB_TB_SDIVIDE:
            if (_RDB_expr_table_depend(exp, tbp->var.sdivide.tb1p))
                return RDB_TRUE;
            if (_RDB_expr_table_depend(exp, tbp->var.sdivide.tb2p))
                return RDB_TRUE;
            return _RDB_expr_table_depend(exp, tbp->var.sdivide.tb3p);
    }
    /* Must never be reached */
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
