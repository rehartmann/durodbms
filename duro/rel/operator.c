/*
 * $Id$
 *
 * Copyright (C) 2004, 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>
#include <stdlib.h>
#include <regex.h>

int
RDB_create_ro_op(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const void *iargp, size_t iarglen, 
                 RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object rtypobj;
    RDB_object typesobj;
    int ret;
    int i;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /*
     * Array types are not supported as argument or return types
     */
    for (i = 0; i < argc; i++) {
        if (argtv[i]->kind == RDB_TP_ARRAY)
            return RDB_NOT_SUPPORTED;
    }
    if (rtyp->kind == RDB_TP_ARRAY)
        return RDB_NOT_SUPPORTED;

    RDB_init_obj(&tpl);
    RDB_init_obj(&rtypobj);

    ret = RDB_tuple_set_string(&tpl, "NAME", name);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "LIB", libname);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "SYMBOL", symname);
    if (ret != RDB_OK)
        goto cleanup;

    if (iargp == NULL)
        iarglen = 0;

    RDB_init_obj(&iarg);
    ret = RDB_binary_set(&iarg, 0, iargp, iarglen);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&iarg);
        goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "IARG", &iarg);
    RDB_destroy_obj(&iarg);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized argument types */
    RDB_init_obj(&typesobj);
    ret = _RDB_make_typesobj(argc, argtv, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj);
        goto cleanup;
    }

    ret = RDB_tuple_set(&tpl, "ARGTYPES", &typesobj);
    RDB_destroy_obj(&typesobj);
    if (ret != RDB_OK)
        goto cleanup;

    ret = _RDB_type_to_obj(&rtypobj, rtyp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "RTYPE", &rtypobj);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->ro_ops_tbp, &tpl, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Check if it's a comparison operator */
    if (strcmp(name, "compare") == 0 && argc == 2
            && argtv[0] == argtv[1] && !RDB_type_is_builtin(argtv[0])) {
        RDB_ro_op_desc *cmpop;

        ret = _RDB_get_ro_op(name, argc, argtv, txp, &cmpop);
        if (ret != RDB_OK)
            goto cleanup;
        argtv[0]->comparep = cmpop->funcp;
        argtv[0]->compare_iarglen = cmpop->iarg.var.bin.len;
        argtv[0]->compare_iargp = cmpop->iarg.var.bin.datap;
        argtv[0]->tx_udata = txp->user_data;
    }

cleanup:
    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&rtypobj);
    return ret;
}

int
RDB_create_update_op(const char *name, int argc, RDB_type *argtv[],
                  RDB_bool updv[], const char *libname, const char *symname,
                  const void *iargp, size_t iarglen,
                  RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object updvobj;
    RDB_object updobj;
    RDB_object typesobj;
    int i;
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /*
     * Array types are not supported as argument types
     */
    for (i = 0; i < argc; i++) {
        if (argtv[i]->kind == RDB_TP_ARRAY)
            return RDB_NOT_SUPPORTED;
    }

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "NAME", name);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "LIB", libname);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "SYMBOL", symname);
    if (ret != RDB_OK)
        goto cleanup;

    if (iargp == NULL)
        iarglen = 0;

    RDB_init_obj(&iarg);
    ret = RDB_binary_set(&iarg, 0, iargp, iarglen);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&iarg);
        goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "IARG", &iarg);
    RDB_destroy_obj(&iarg);
    if (ret != RDB_OK)
        goto cleanup;

    RDB_init_obj(&updvobj);
    RDB_init_obj(&updobj);
    RDB_set_array_length(&updvobj, (RDB_int) argc);
    for (i = 0; i < argc; i++) {
        RDB_bool_to_obj(&updobj, updv[i]);
        ret = RDB_array_set(&updvobj, (RDB_int) i, &updobj);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&updvobj);
            RDB_destroy_obj(&updobj);
            goto cleanup;
        }
    }
    ret = RDB_tuple_set(&tpl, "UPDV", &updvobj);
    RDB_destroy_obj(&updvobj);
    RDB_destroy_obj(&updobj);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized arg types */
    RDB_init_obj(&typesobj);
    ret = _RDB_make_typesobj(argc, argtv, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj);
        goto cleanup;
    }
    
    ret = RDB_tuple_set(&tpl, "ARGTYPES", &typesobj);
    RDB_destroy_obj(&typesobj);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->upd_ops_tbp, &tpl, txp);

cleanup:
    RDB_destroy_obj(&tpl);
    return ret;
}

static void
free_ro_op(RDB_ro_op_desc *op) {
    int i;

    free(op->name);
    if (op->argtv != NULL) {
        for (i = 0; i < op->argc; i++) {
            if (RDB_type_name(op->argtv[i]) == NULL)
                RDB_drop_type(op->argtv[i], NULL);
        }
        free(op->argtv);
    }
    if (op->rtyp != NULL && RDB_type_name(op->rtyp) == NULL)
        RDB_drop_type(op->rtyp, NULL);
    if (op->modhdl != NULL) {
        /* Built-in operator */
        lt_dlclose(op->modhdl);
        RDB_destroy_obj(&op->iarg);
    }
    free(op);
}

void
_RDB_free_ro_ops(RDB_ro_op_desc *op)
{
    do {
        RDB_ro_op_desc *nextop = op->nextp;
        free_ro_op(op);
        op = nextop;
    } while (op != NULL);
}

static void
free_upd_op(RDB_upd_op *op) {
    int i;

    free(op->name);
    for (i = 0; i < op->argc; i++) {
        if (RDB_type_name(op->argtv[i]) == NULL)
            RDB_drop_type(op->argtv[i], NULL);
    }
    free(op->argtv);
    free(op->updv);
    lt_dlclose(op->modhdl);
    RDB_destroy_obj(&op->iarg);
    free(op);
}

void
_RDB_free_upd_ops(RDB_upd_op *op)
{
    do {
        RDB_upd_op *nextop = op->nextp;
        free_upd_op(op);
        op = nextop;
    } while (op != NULL);
}

static int
eq_bool(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val == argv[1]->var.bool_val));
    return RDB_OK;
}

static int
eq_binary(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    if (argv[0]->var.bin.len != argv[1]->var.bin.len)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else if (argv[0]->var.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->var.bin.datap,
            argv[1]->var.bin.datap, argv[0]->var.bin.len) == 0));
    return RDB_OK;
}

/* Default equality operator */
static int
obj_equals(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    RDB_bool res;
    RDB_type *arep = NULL;

    if (argv[0]->kind != argv[1]->kind)
        return RDB_TYPE_MISMATCH;

    /*
     * Check if there is a comparison function associated with the type
     */
    if (argv[0]->typ != NULL) {
        if (argv[0]->typ->comparep != NULL) {
            arep = argv[0]->typ;
        } else if (argv[0]->typ->kind == RDB_TP_SCALAR
                && argv[0]->typ->var.scalar.arep != NULL) {
            arep = argv[0]->typ->var.scalar.arep;
        }
    }

    /* If yes, call it */
    if (arep != NULL && arep->comparep != NULL) {
        RDB_object retval;

        RDB_init_obj(&retval);
        retval.typ = &RDB_INTEGER;
        ret = (*arep->comparep)("compare", 2, argv, arep->compare_iargp,
                arep->compare_iarglen, txp, &retval);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&retval);
            return ret;
        }
        RDB_bool_to_obj(retvalp, (RDB_bool) RDB_obj_int(&retval) == 0);
        RDB_destroy_obj(&retval);
        return RDB_OK;
    }

    switch (argv[0]->kind) {
        case RDB_OB_INITIAL:
            return RDB_INVALID_ARGUMENT;
        case RDB_OB_BOOL:
            return eq_bool("=", 2, argv, NULL, 0, txp, retvalp);
        case RDB_OB_INT:
        case RDB_OB_RATIONAL:
            /* Must not happen, because there must be a comparsion function */
            return RDB_INTERNAL;
        case RDB_OB_BIN:
            return eq_binary("=", 2, argv, NULL, 0, txp, retvalp);
        case RDB_OB_TUPLE:
            ret = _RDB_tuple_equals(argv[0], argv[1], txp, &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            break;
        case RDB_OB_TABLE:
            if (!RDB_type_equals(argv[0]->var.tbp->typ, argv[1]->var.tbp->typ))
                return RDB_TYPE_MISMATCH;
            ret = RDB_table_equals(argv[0]->var.tbp, argv[1]->var.tbp, NULL,
                    &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            break;
        case RDB_OB_ARRAY:
            ret = _RDB_array_equals(argv[0], argv[1], txp, &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            break;
    }
    return RDB_OK;
} 

int
obj_not_equals(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp) {
    int ret = obj_equals("=", 2, argv, NULL, 0, txp, retvalp);
    if (ret != RDB_OK)
        return ret;
    retvalp->var.bool_val = (RDB_bool) !retvalp->var.bool_val;
    return RDB_OK;
}

static RDB_ro_op_desc *
new_ro_op(const char *name, int argc, RDB_type *rtyp, RDB_ro_op_func *funcp)
{
    RDB_ro_op_desc *op = malloc(sizeof (RDB_ro_op_desc));
    if (op == NULL)
        return NULL;

    op->name = RDB_dup_str(name);
    if (op->name == NULL) {
        free(op);
        return NULL;
    }

    if (argc > 0) {
        op->argc = argc;
        op->argtv = malloc(sizeof (RDB_type *) * argc);
        if (op->argtv == NULL) {
            free(op->name);
            free(op);
            return NULL;
        }
    } else {
        op->argtv = NULL;
    }

    op->rtyp = rtyp;
    op->funcp = funcp;
    op->modhdl = NULL;    

    return op;
}

static int
put_ro_op(RDB_dbroot *dbrootp, RDB_ro_op_desc *op)
{
    int ret;
    RDB_ro_op_desc *fop = RDB_hashmap_get(&dbrootp->ro_opmap, op->name);

    if (fop == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&dbrootp->ro_opmap, op->name, op);
        if (ret != RDB_OK)
            return ret;
    } else {
        op->nextp = fop->nextp;
        fop->nextp = op;
    }
    return RDB_OK;
}

static int
get_ro_op(RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_type *argtv[], RDB_ro_op_desc **ropp)
{
    RDB_ro_op_desc *op = RDB_hashmap_get(&dbrootp->ro_opmap, name);
    RDB_bool pm = RDB_FALSE;

    if (op == NULL)
        return RDB_OPERATOR_NOT_FOUND;

    *ropp = op;

    /* Find an operator with same signature */
    do {
        if ((*ropp)->argtv == NULL) {
            /* Do not compare argument types */
            return RDB_OK;
        }

        if ((*ropp)->argc == argc) {
            int i;

            pm = RDB_TRUE;
            for (i = 0; (i < argc)
                    && RDB_type_equals((*ropp)->argtv[i], argtv[i]);
                 i++);
            if (i == argc) {
                /* Found */
                return RDB_OK;
            }
        }
        *ropp = (*ropp)->nextp;
    } while ((*ropp) != NULL);

    /*
     * Provide "=" and "<>" for user-defined operators
     */
    if (argc == 2 && RDB_type_equals(argtv[0], argtv[1])) {
        RDB_ro_op_desc *op;
        int ret;

        if (strcmp(name, "=") == 0) {
            op = new_ro_op("=", 2, &RDB_BOOLEAN, &obj_equals);
            if (op == NULL)
                return RDB_NO_MEMORY;
            op->argtv[0] = _RDB_dup_nonscalar_type(argtv[0]);
            op->argtv[1] = _RDB_dup_nonscalar_type(argtv[1]);
            ret = put_ro_op(dbrootp, op);
            if (ret != RDB_OK)
                return ret;
            *ropp = op;
            return RDB_OK;
        }
        if (strcmp(name, "<>") == 0) {
            op = new_ro_op("<>", 2, &RDB_BOOLEAN, &obj_not_equals);
            if (op == NULL)
                return RDB_NO_MEMORY;
            op->argtv[0] = _RDB_dup_nonscalar_type(argtv[0]);
            op->argtv[1] = _RDB_dup_nonscalar_type(argtv[1]);
            ret = put_ro_op(dbrootp, op);
            if (ret != RDB_OK)
                return ret;
            *ropp = op;
            return RDB_OK;
        }
    }

    return pm ? RDB_TYPE_MISMATCH : RDB_OPERATOR_NOT_FOUND;
}

static RDB_type **
valv_to_typev(int valc, RDB_object **valv)
{
    int i;
    RDB_type **typv = malloc(sizeof (RDB_type *) * valc);

    if (typv == NULL)
        return NULL;
    for (i = 0; i < valc; i++) {
        if (valv[i]->kind == RDB_OB_TUPLE)
            typv[i] = _RDB_tuple_type(valv[i]);
        else
            typv[i] = RDB_obj_type(valv[i]);
    }
    return typv;
}

int
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_transaction *txp, RDB_ro_op_desc **opp)
{
    int ret, ret2;

    /* Lookup operator in map */
    ret = get_ro_op(txp->dbp->dbrootp, name, argc, argtv, opp);

    if (ret == RDB_OPERATOR_NOT_FOUND || ret == RDB_TYPE_MISMATCH) {
        /* Not found in map, so read from catalog */
        ret2 = _RDB_cat_get_ro_op(name, argc, argtv, txp, opp);
        if (ret2 != RDB_OK)
            return ret2 == RDB_NOT_FOUND ? ret : ret2;
        
        /* Insert operator into map */
        ret = put_ro_op(txp->dbp->dbrootp, *opp);
        if (ret != RDB_OK) {
            free_ro_op(*opp);
            return ret;
        }
    }
    return RDB_OK;
}

int
_RDB_check_type_constraint(RDB_object *valp, RDB_transaction *txp)
{
    int ret;

    if (valp->typ->var.scalar.constraintp != NULL) {
        RDB_bool result;
        RDB_object tpl;

        RDB_init_obj(&tpl);

        /* Set tuple attribute */
        ret = RDB_tuple_set(&tpl, valp->typ->name, valp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }
        ret = RDB_evaluate_bool(valp->typ->var.scalar.constraintp, &tpl, txp,
                &result);
        RDB_destroy_obj(&tpl);
        if (ret != RDB_OK) {
            return ret;
        }
        if (!result) {
            return RDB_TYPE_CONSTRAINT_VIOLATION;
        }
    }
    return RDB_OK;
}

static int
neq_bool(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val != argv[1]->var.bool_val));
    return RDB_OK;
}

static int
neq_binary(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    if (argv[0]->var.bin.len != argv[1]->var.bin.len)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else if (argv[0]->var.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->var.bin.datap,
            argv[1]->var.bin.datap, argv[0]->var.bin.len) != 0));
    return RDB_OK;
}

static RDB_bool
obj_is_table(RDB_object *objp)
{
    /*
     * Check type first, as it could be a user-defined type
     * with a table representation
     */
    RDB_type *typ = RDB_obj_type(objp);
    if (typ == NULL)
        return (RDB_bool) (objp->kind == RDB_OB_TABLE);
    return (RDB_bool) (typ->kind == RDB_TP_RELATION);
}

static RDB_bool
obj_is_scalar(RDB_object *objp)
{
    return (RDB_bool) (objp->typ != NULL && RDB_type_is_scalar(objp->typ));
}

static int
op_rename(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *vtbp;
    int renc = (argc - 1) / 2;
    RDB_renaming *renv = malloc(sizeof(RDB_renaming) * renc);
    if (renv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < renc; i++) {
        if (argv[1 + i]->typ != &RDB_STRING
                || argv[2 + i]->typ != &RDB_STRING) {
            free(renv);
            return RDB_TYPE_MISMATCH;
        }
        renv[i].from = RDB_obj_string(argv[1 + i * 2]);
        renv[i].to = RDB_obj_string(argv[2 + i * 2]);
    }

    ret = RDB_rename(RDB_obj_table(argv[0]), renc, renv, &vtbp);
    free(renv);
    if (ret != RDB_OK)
        return ret;
    RDB_table_to_obj(retvalp, vtbp);

    /*
     * Since the table is consumed by RDB_rename, prevent it
     * from being deleted when argv[0] is destroyed.
     */
    argv[0]->var.tbp = NULL;

    return RDB_OK;
}

static int
op_project(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *tbp, *vtbp;
    char **attrv = malloc(sizeof(char *) * (argc - 1));
    if (attrv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {
        ret = RDB_project(tbp, argc - 1, attrv, &vtbp);
    } else {
        ret = RDB_project_tuple(argv[0], argc - 1, attrv, retvalp);
    }
    free(attrv);
    if (ret != RDB_OK)
        return ret;
    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp);
        argv[0]->var.tbp = NULL;
    }
    return RDB_OK;
}

static int
op_remove(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *tbp, *vtbp;
    char **attrv = malloc(sizeof(char *) * (argc - 1));
    if (attrv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {
        ret = RDB_remove(tbp, argc - 1, attrv, &vtbp);
    } else {
        ret = RDB_remove_tuple(argv[0], argc - 1, attrv, retvalp);
    }
    free(attrv);
    if (ret != RDB_OK)
        return ret;
    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp);
        argv[0]->var.tbp = NULL;
    }
    return RDB_OK;
}

static int
op_unwrap(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *tbp, *vtbp;
    char **attrv = malloc(sizeof(char *) * (argc - 1));
    if (attrv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {
        ret = RDB_unwrap(tbp, argc - 1, attrv, &vtbp);
    } else {
        ret = RDB_unwrap_tuple(argv[0], argc - 1, attrv, retvalp);
    }
    free(attrv);
    if (ret != RDB_OK)
        return ret;
    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp);
        argv[0]->var.tbp = NULL;
    }
    return RDB_OK;
}

static int
op_group(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *vtbp;
    char **attrv;
    int attrc;

    if (argc < 2)
        return RDB_INVALID_ARGUMENT;

    attrc = argc - 2;
    attrv = malloc(sizeof(char *) * attrc);
    if (attrv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < attrc; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    ret = RDB_group(RDB_obj_table(argv[0]), attrc, attrv,
            RDB_obj_string(argv[argc - 1]), &vtbp);
    free(attrv);
    if (ret != RDB_OK)
        return ret;
    RDB_table_to_obj(retvalp, vtbp);
    argv[0]->var.tbp = NULL;

    return RDB_OK;
}

static int
op_ungroup(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    RDB_table *vtbp;

    ret = RDB_ungroup(RDB_obj_table(argv[0]), RDB_obj_string(argv[1]), &vtbp);
    if (ret != RDB_OK)
        return ret;
    RDB_table_to_obj(retvalp, vtbp);
    argv[0]->var.tbp = NULL;

    return RDB_OK;
}

static int
op_union(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    RDB_table *vtbp;
    
    if (argc != 2)
        return RDB_INVALID_ARGUMENT;
    
    ret = RDB_union(RDB_obj_table(argv[0]), RDB_obj_table(argv[1]), &vtbp);
    if (ret != RDB_OK)
        return ret;

    RDB_table_to_obj(retvalp, vtbp);
    argv[0]->var.tbp = NULL;
    argv[1]->var.tbp = NULL;
    return RDB_OK;
}

static int
op_minus(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    RDB_table *vtbp;
    
    if (argc != 2)
        return RDB_INVALID_ARGUMENT;
    
    ret = RDB_minus(RDB_obj_table(argv[0]), RDB_obj_table(argv[1]), &vtbp);
    if (ret != RDB_OK)
        return ret;

    RDB_table_to_obj(retvalp, vtbp);
    argv[0]->var.tbp = NULL;
    argv[1]->var.tbp = NULL;
    return RDB_OK;
}

static int
op_intersect(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    RDB_table *vtbp;
    
    if (argc != 2)
        return RDB_INVALID_ARGUMENT;
    
    ret = RDB_intersect(RDB_obj_table(argv[0]), RDB_obj_table(argv[1]), &vtbp);
    if (ret != RDB_OK)
        return ret;

    RDB_table_to_obj(retvalp, vtbp);
    argv[0]->var.tbp = NULL;
    argv[1]->var.tbp = NULL;
    return RDB_OK;
}

static int
op_join(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    RDB_table *tbp;
    RDB_table *vtbp;
    
    if (argc != 2)
        return RDB_INVALID_ARGUMENT;

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {    
        ret = RDB_join(tbp, RDB_obj_table(argv[1]), &vtbp);
    } else {
        ret = RDB_join_tuples(argv[0], argv[1], txp, retvalp);
    }
    if (ret != RDB_OK)
        return ret;

    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp);
        argv[0]->var.tbp = NULL;
        argv[1]->var.tbp = NULL;
    }
    return RDB_OK;
}

static int
op_divide(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    RDB_table *vtbp;
    
    if (argc != 3)
        return RDB_INVALID_ARGUMENT;
    
    ret = RDB_sdivide(RDB_obj_table(argv[0]), RDB_obj_table(argv[1]),
            RDB_obj_table(argv[2]), &vtbp);
    if (ret != RDB_OK)
        return ret;

    RDB_table_to_obj(retvalp, vtbp);
    argv[0]->var.tbp = NULL;
    argv[1]->var.tbp = NULL;
    argv[2]->var.tbp = NULL;
    return RDB_OK;
}

int
RDB_call_ro_op(const char *name, int argc, RDB_object *argv[],
               RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_ro_op_desc *op;
    int ret;
    RDB_type **argtv;
    int i;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /*
     * Handle nonscalar comparison
     */
    if (argc == 2 && !obj_is_scalar(argv[0]) && !obj_is_scalar(argv[1])) {
        if (strcmp(name, "=") == 0)
            return obj_equals(name, 2, argv, NULL, 0, txp, retvalp);
        if (strcmp(name, "<>") == 0) {
            ret = obj_equals(name, 2, argv, NULL, 0, txp, retvalp);
            if (ret != RDB_OK)
                return ret;
            retvalp->var.bool_val = (RDB_bool) !retvalp->var.bool_val;
            return RDB_OK;
        }
    }

    /*
     * Handle built-in operators with relational arguments
     */
    if (argc == 1 && obj_is_table(argv[0]))  {
        if (strcmp(name, "IS_EMPTY") == 0) {
            RDB_bool res;

            ret = RDB_table_is_empty(argv[0]->var.tbp, txp, &res);
            if (ret != RDB_OK)
                return ret;

            RDB_bool_to_obj(retvalp, res);
            return RDB_OK;
        }
        if (strcmp(name, "COUNT") == 0) {
            ret = RDB_cardinality(argv[0]->var.tbp, txp);
            if (ret < 0)
                return ret;

            RDB_int_to_obj(retvalp, ret);
            return RDB_OK;
        }
    } else if (argc == 2 && obj_is_table(argv[1])) {
        if (strcmp(name, "IN") == 0) {
            ret = RDB_table_contains(argv[1]->var.tbp, argv[0], txp);
            if (ret == RDB_OK) {
                RDB_bool_to_obj(retvalp, RDB_TRUE);
                return RDB_OK;
            } else if (ret == RDB_NOT_FOUND) {
                RDB_bool_to_obj(retvalp, RDB_FALSE);
                return RDB_OK;
            } else {
                return ret;
            }
        } else if (strcmp(name, "SUBSET_OF") == 0) {
            RDB_bool res;

            ret = RDB_subset(argv[0]->var.tbp, argv[1]->var.tbp, txp, &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            return RDB_OK;
        }
    }
    if (obj_is_table(argv[0]) && strcmp(name, "TO_TUPLE") == 0
            && argc == 1) {
        return RDB_extract_tuple(RDB_obj_table(argv[0]), txp, retvalp);
    }

    argtv = valv_to_typev(argc, argv);
    if (argtv == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    ret = _RDB_get_ro_op(name, argc, argtv, txp, &op);
    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TUPLE)
            RDB_drop_type(argtv[i], NULL);
    }

    free(argtv);

    if (ret != RDB_OK) {
        if (ret == RDB_OPERATOR_NOT_FOUND)
            RDB_errmsg(RDB_tx_env(txp), "operator \"%s\" not found", name);
        goto error;
    }

    /* Set return type to make it available to the function */
    retvalp->typ = op->rtyp;

    ret = (*op->funcp)(name, argc, argv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, txp, retvalp);
    if (ret != RDB_OK)
        goto error;

    /* Check type constraint if the operator is a selector */
    if (retvalp->typ != NULL &&_RDB_get_possrep(retvalp->typ, name) != NULL) {
        ret = _RDB_check_type_constraint(retvalp, txp);
        if (ret != RDB_OK) {
            /* Destroy illegal value */
            RDB_destroy_obj(retvalp);
            RDB_init_obj(retvalp);
            return ret;
        }
    }

    return RDB_OK;
error:
    _RDB_handle_syserr(txp, ret);
    return ret;
}

static RDB_upd_op *
get_upd_op(const RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_type *argtv[])
{
    RDB_upd_op *op = RDB_hashmap_get(&dbrootp->upd_opmap, name);

    if (op == NULL)
        return NULL;
    
    /* Find a operation with same signature */
    while (op != NULL) {
        if (op->argc == argc) {
            int i;

            for (i = 0; (i < argc)
                    && RDB_type_equals(op->argtv[i], argtv[i]);
                 i++);
            if (i == argc) {
                /* Found */
                return op;
            }
        }
        op = op->nextp;
    }

    return NULL;
}

static int
put_upd_op(RDB_dbroot *dbrootp, RDB_upd_op *op)
{
    int ret;
    RDB_upd_op *fop = RDB_hashmap_get(&dbrootp->upd_opmap, op->name);

    if (fop == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&dbrootp->upd_opmap, op->name, op);
        if (ret != RDB_OK)
            return ret;
    } else {
        op->nextp = fop->nextp;
        fop->nextp = op;
    }
    return RDB_OK;
}

int
_RDB_get_upd_op(const char *name, int argc, RDB_type *argtv[],
               RDB_transaction *txp, RDB_upd_op **opp)
{
    int ret;

    *opp = get_upd_op(txp->dbp->dbrootp, name, argc, argtv);
    if (*opp == NULL) {
        ret = _RDB_cat_get_upd_op(name, argc, argtv, txp, opp);
        if (ret != RDB_OK)
            return ret == RDB_NOT_FOUND ? RDB_OPERATOR_NOT_FOUND : ret;
        ret = put_upd_op(txp->dbp->dbrootp, *opp);
        if (ret != RDB_OK) {
            free_upd_op(*opp);
            return ret;
        }
    }
    return RDB_OK;
}

int
RDB_call_update_op(const char *name, int argc, RDB_object *argv[],
                   RDB_transaction *txp)
{
    RDB_upd_op *op;
    RDB_type **argtv;
    int ret;
    int i;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    argtv = valv_to_typev(argc, argv);
    if (argtv == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }
    ret = _RDB_cat_get_upd_op(name, argc, argtv, txp, &op);
    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TUPLE)
            RDB_drop_type(argtv[i], NULL);
    }
    free(argtv);
    if (ret != RDB_OK) {
        if (ret == RDB_NOT_FOUND)
            ret = RDB_OPERATOR_NOT_FOUND;
        if (ret == RDB_OPERATOR_NOT_FOUND)
            RDB_errmsg(RDB_tx_env(txp), "operator \"%s\" not found", name);
        return ret;
    }

    return (*op->funcp)(name, argc, argv, op->updv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, txp);
}

int
RDB_drop_op(const char *name, RDB_transaction *txp)
{
    RDB_expression *exp;
    RDB_table *vtbp;
    int ret;
    RDB_bool isempty;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /*
     * Check if it's a read-only operator
     */
    exp = RDB_eq(RDB_expr_attr("NAME"), RDB_string_to_expr(name));
    if (exp == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }
    ret = RDB_select(txp->dbp->dbrootp->ro_ops_tbp, exp, txp, &vtbp);
    if (ret != RDB_OK) {
        return ret;
    }
    ret = RDB_table_is_empty(vtbp, txp, &isempty);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, txp);
        return ret;
    }
    ret = RDB_drop_table(vtbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    if (isempty) {
        /* It's an update operator */
        RDB_upd_op *oldop;
        RDB_upd_op *op = NULL;

        /* Delete all versions of update operator from hashmap */
        oldop = RDB_hashmap_get(&txp->dbp->dbrootp->ro_opmap, name);
        if (oldop != NULL)
            _RDB_free_upd_ops(oldop);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->upd_opmap, name, op);
        if (ret != RDB_OK) {
            RDB_rollback_all(txp);
            return ret;
        }
        
        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_expr_attr("NAME"), RDB_string_to_expr(name));
        if (exp == NULL) {
            RDB_rollback_all(txp);
            return RDB_NO_MEMORY;
        }
        ret = RDB_delete(txp->dbp->dbrootp->upd_ops_tbp, exp, txp);
        RDB_drop_expr(exp);
        if (ret != RDB_OK) {
            return ret;
        }        
    } else {
        /* It's a read-only operator */
        RDB_ro_op_desc *oldop;
        RDB_ro_op_desc *op = NULL;

        /* Delete all versions of readonly operator from hashmap */
        oldop = RDB_hashmap_get(&txp->dbp->dbrootp->ro_opmap, name);
        if (oldop != NULL)
            _RDB_free_ro_ops(oldop);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->ro_opmap, name, op);
        if (ret != RDB_OK) {
            RDB_rollback_all(txp);
            return ret;
        }

        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_expr_attr("NAME"), RDB_string_to_expr(name));
        if (exp == NULL) {
            RDB_rollback_all(txp);
            return RDB_NO_MEMORY;
        }
        ret = RDB_delete(txp->dbp->dbrootp->ro_ops_tbp, exp, txp);
        if (ret != RDB_OK) {
            RDB_drop_expr(exp);
            _RDB_handle_syserr(txp, ret);
            return ret;
        }
    }

    return RDB_OK;
}

/*
 * Built-in operators
 */

static int
integer_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (int) argv[0]->var.rational_val);
    return RDB_OK;
}

static int
integer_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    char *endp;

    RDB_int_to_obj(retvalp, (RDB_int)
            strtol(argv[0]->var.bin.datap, &endp, 10));
    if (*endp != '\0')
        return RDB_INVALID_ARGUMENT;
    return RDB_OK;
}

static int
rational_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp, (RDB_rational) argv[0]->var.int_val);
    return RDB_OK;
}

static int
rational_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    char *endp;

    RDB_rational_to_obj(retvalp, (RDB_rational)
            strtod(argv[0]->var.bin.datap, &endp));
    if (*endp != '\0')
        return RDB_INVALID_ARGUMENT;
    return RDB_OK;
}

static int
string_obj(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    return RDB_obj_to_string(retvalp, argv[0]);
}

static int
length_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    size_t len = mbstowcs(NULL, argv[0]->var.bin.datap, 0);
    if (len == -1)
        return RDB_INVALID_ARGUMENT;

    RDB_int_to_obj(retvalp, (RDB_int) len);
    return RDB_OK;
}

static int
substring(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int start = argv[1]->var.int_val;
    int len = argv[2]->var.int_val;
    int i;
    int cl;
    int bstart, blen;

    /* Operands must not be negative */
    if (len < 0 || start < 0)
        return RDB_INVALID_ARGUMENT;

    /* Find start of substring */
    bstart = 0;
    for (i = 0; i < start && bstart < argv[0]->var.bin.len - 1; i++) {
        cl = mblen(((char *) argv[0]->var.bin.datap) + bstart, 4);
        if (cl == -1)
            return RDB_INVALID_ARGUMENT;
        bstart += cl;
    }
    if (bstart >= argv[0]->var.bin.len - 1)
        return RDB_INVALID_ARGUMENT;

    /* Find end of substring */
    blen = 0;
    for (i = 0; i < len && bstart + blen < argv[0]->var.bin.len; i++) {
        cl = mblen(((char *) argv[0]->var.bin.datap) + bstart + blen, 4);
        if (cl == -1)
            return RDB_INVALID_ARGUMENT;
        blen += cl > 0 ? cl : 1;
    }
    if (bstart + blen >= argv[0]->var.bin.len)
        return RDB_INVALID_ARGUMENT;

    RDB_destroy_obj(retvalp);
    retvalp->typ = &RDB_STRING;
    retvalp->kind = RDB_OB_BIN;
    retvalp->var.bin.len = blen + 1;
    retvalp->var.bin.datap = malloc(retvalp->var.bin.len);
    if (retvalp->var.bin.datap == NULL)
        return RDB_NO_MEMORY;
    strncpy(retvalp->var.bin.datap, (char *) argv[0]->var.bin.datap
            + bstart, blen);
    ((char *) retvalp->var.bin.datap)[blen] = '\0';
    return RDB_OK;
}

static int
concat(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    size_t s1len = strlen(argv[0]->var.bin.datap);

    RDB_destroy_obj(retvalp);
    RDB_init_obj(retvalp);
    _RDB_set_obj_type(retvalp, &RDB_STRING);
    retvalp->var.bin.len = s1len + strlen(argv[1]->var.bin.datap) + 1;
    retvalp->var.bin.datap = malloc(retvalp->var.bin.len);
    if (retvalp->var.bin.datap == NULL) {
        return RDB_NO_MEMORY;
    }
    strcpy(retvalp->var.bin.datap, argv[0]->var.bin.datap);
    strcpy(((char *)retvalp->var.bin.datap) + s1len, argv[1]->var.bin.datap);
    return RDB_OK;
}

static int
matches(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    regex_t reg;
    int ret;

    ret = regcomp(&reg, argv[1]->var.bin.datap, REG_NOSUB);
    if (ret != 0) {
        return RDB_INVALID_ARGUMENT;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool)
            (regexec(&reg, argv[0]->var.bin.datap, 0, NULL, 0) == 0));
    regfree(&reg);

    return RDB_OK;
}

static int
and(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val && argv[1]->var.bool_val);
    return RDB_OK;
}

static int
or(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val || argv[1]->var.bool_val);
    return RDB_OK;
}

static int
not(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool) !argv[0]->var.bool_val);
    return RDB_OK;
}

static int
lt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) < 0);
    RDB_destroy_obj(&retval);
    return RDB_OK;
}

static int
let(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) <= 0);
    RDB_destroy_obj(&retval);
    return RDB_OK;
}

static int
gt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) > 0);
    RDB_destroy_obj(&retval);
    return RDB_OK;
}

static int
get(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) >= 0);
    RDB_destroy_obj(&retval);
    return RDB_OK;
}

static int
negate_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, -argv[0]->var.int_val);
    return RDB_OK;
}

static int
negate_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp, -argv[0]->var.rational_val);
    return RDB_OK;
}

static int
add_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val + argv[1]->var.int_val);
    return RDB_OK;
}

static int
add_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp,
            argv[0]->var.rational_val + argv[1]->var.rational_val);
    return RDB_OK;
}

static int
subtract_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val - argv[1]->var.int_val);
    return RDB_OK;
}

static int
subtract_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp,
            argv[0]->var.rational_val - argv[1]->var.rational_val);
    return RDB_OK;
}

static int
multiply_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val * argv[1]->var.int_val);
    return RDB_OK;
}

static int
multiply_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp,
            argv[0]->var.rational_val * argv[1]->var.rational_val);
    return RDB_OK;
}

static int
divide_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    if (argv[1]->var.int_val == 0)
        return RDB_INVALID_ARGUMENT;
    RDB_int_to_obj(retvalp, argv[0]->var.int_val / argv[1]->var.int_val);
    return RDB_OK;
}

static int
divide_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    if (argv[1]->var.rational_val == 0.0)
        return RDB_INVALID_ARGUMENT;
    RDB_rational_to_obj(retvalp,
            argv[0]->var.rational_val / argv[1]->var.rational_val);
    return RDB_OK;
}

int
_RDB_add_builtin_ops(RDB_dbroot *dbrootp)
{
    RDB_ro_op_desc *op;
    int ret;

    op = new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_rational);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_string);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("RATIONAL", 1, &RDB_RATIONAL, &rational_int);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("RATIONAL", 1, &RDB_RATIONAL, &rational_string);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("STRING", 1, &RDB_STRING, &string_obj);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("STRING", 1, &RDB_STRING, &string_obj);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("LENGTH", 1, &RDB_INTEGER, &length_string);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("SUBSTRING", 3, &RDB_STRING, &substring);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_INTEGER;
    op->argtv[2] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("||", 2, &RDB_STRING, &concat);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("MATCHES", 2, &RDB_BOOLEAN, &matches);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("AND", 2, &RDB_BOOLEAN, &and);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("OR", 2, &RDB_BOOLEAN, &or);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("NOT", 1, &RDB_BOOLEAN, &not);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<", 2, &RDB_BOOLEAN, &lt);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<", 2, &RDB_BOOLEAN, &lt);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<", 2, &RDB_BOOLEAN, &lt);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<=", 2, &RDB_BOOLEAN, &let);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<=", 2, &RDB_BOOLEAN, &let);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<=", 2, &RDB_BOOLEAN, &let);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">", 2, &RDB_BOOLEAN, &gt);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">", 2, &RDB_BOOLEAN, &gt);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">", 2, &RDB_BOOLEAN, &gt);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">=", 2, &RDB_BOOLEAN, &get);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">=", 2, &RDB_BOOLEAN, &get);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">=", 2, &RDB_BOOLEAN, &get);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, &eq_bool);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, obj_equals);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, obj_equals);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, obj_equals);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, &eq_binary);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BINARY;
    op->argtv[1] = &RDB_BINARY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &neq_bool);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &obj_not_equals);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &obj_not_equals);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &obj_not_equals);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &neq_binary);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BINARY;
    op->argtv[1] = &RDB_BINARY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("-", 1, &RDB_INTEGER, &negate_int);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("-", 1, &RDB_RATIONAL, &negate_rational);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("+", 2, &RDB_INTEGER, &add_int);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("+", 2, &RDB_RATIONAL, &add_rational);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("-", 2, &RDB_INTEGER, &subtract_int);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("-", 2, &RDB_RATIONAL, &subtract_rational);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("*", 2, &RDB_INTEGER, &multiply_int);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("*", 2, &RDB_RATIONAL, &multiply_rational);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("/", 2, &RDB_INTEGER, &divide_int);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("/", 2, &RDB_RATIONAL, &divide_rational);
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("RENAME", -1, NULL, &op_rename);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("PROJECT", -1, NULL, &op_project);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("REMOVE", -1, NULL, &op_remove);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("UNWRAP", -1, NULL, &op_unwrap);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("UNION", -1, NULL, &op_union);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("MINUS", -1, NULL, &op_minus);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("INTERSECT", -1, NULL, &op_intersect);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("JOIN", -1, NULL, &op_join);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("DIVIDE_BY_PER", -1, NULL, &op_divide);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("GROUP", -1, NULL, &op_group);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("UNGROUP", -1, NULL, &op_ungroup);
    if (op == NULL)
        return RDB_NO_MEMORY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}
