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
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object rtypobj;
    RDB_object typesobj;
    int ret;
    int i;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Array types are not supported as argument or return types
     */
    for (i = 0; i < argc; i++) {
        if (argtv[i]->kind == RDB_TP_ARRAY) {
            RDB_raise_not_supported(
                    "array type not supported as argument type", ecp);
            return RDB_ERROR;
        }
    }
    if (rtyp->kind == RDB_TP_ARRAY) {
        RDB_raise_not_supported("array type not supported as return type",
                ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&rtypobj);

    ret = RDB_tuple_set_string(&tpl, "NAME", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "LIB", libname, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "SYMBOL", symname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    if (iargp == NULL)
        iarglen = 0;

    RDB_init_obj(&iarg);
    ret = RDB_binary_set(&iarg, 0, iargp, iarglen, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&iarg, ecp);
        goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "IARG", &iarg, ecp);
    RDB_destroy_obj(&iarg, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized argument types */
    RDB_init_obj(&typesobj);
    ret = _RDB_make_typesobj(argc, argtv, ecp, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj, ecp);
        goto cleanup;
    }

    ret = RDB_tuple_set(&tpl, "ARGTYPES", &typesobj, ecp);
    RDB_destroy_obj(&typesobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = _RDB_type_to_obj(&rtypobj, rtyp, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "RTYPE", &rtypobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->ro_ops_tbp, &tpl, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Check if it's a comparison operator */
    if (strcmp(name, "compare") == 0 && argc == 2
            && argtv[0] == argtv[1] && !RDB_type_is_builtin(argtv[0])) {
        RDB_ro_op_desc *cmpop;

        ret = _RDB_get_ro_op(name, argc, argtv, ecp, txp, &cmpop);
        if (ret != RDB_OK)
            goto cleanup;
        argtv[0]->comparep = cmpop->funcp;
        argtv[0]->compare_iarglen = cmpop->iarg.var.bin.len;
        argtv[0]->compare_iargp = cmpop->iarg.var.bin.datap;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&rtypobj, ecp);
    return ret;
}

int
RDB_create_update_op(const char *name, int argc, RDB_type *argtv[],
                  RDB_bool updv[], const char *libname, const char *symname,
                  const void *iargp, size_t iarglen,
                  RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object updvobj;
    RDB_object updobj;
    RDB_object typesobj;
    int i;
    int ret;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Array types are not supported as argument types
     */
    for (i = 0; i < argc; i++) {
        if (argtv[i]->kind == RDB_TP_ARRAY) {
            RDB_raise_not_supported(
                    "array type not supported as argument type", ecp);
            return RDB_ERROR;
         }
    }

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "NAME", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "LIB", libname, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "SYMBOL", symname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    if (iargp == NULL)
        iarglen = 0;

    RDB_init_obj(&iarg);
    ret = RDB_binary_set(&iarg, 0, iargp, iarglen, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&iarg, ecp);
        goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "IARG", &iarg, ecp);
    RDB_destroy_obj(&iarg, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    RDB_init_obj(&updvobj);
    RDB_init_obj(&updobj);
    RDB_set_array_length(&updvobj, (RDB_int) argc, ecp);
    for (i = 0; i < argc; i++) {
        RDB_bool_to_obj(&updobj, updv[i]);
        ret = RDB_array_set(&updvobj, (RDB_int) i, &updobj, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&updvobj, ecp);
            RDB_destroy_obj(&updobj, ecp);
            goto cleanup;
        }
    }
    ret = RDB_tuple_set(&tpl, "UPDV", &updvobj, ecp);
    RDB_destroy_obj(&updvobj, ecp);
    RDB_destroy_obj(&updobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized arg types */
    RDB_init_obj(&typesobj);
    ret = _RDB_make_typesobj(argc, argtv, ecp, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj, ecp);
        goto cleanup;
    }
    
    ret = RDB_tuple_set(&tpl, "ARGTYPES", &typesobj, ecp);
    RDB_destroy_obj(&typesobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->upd_ops_tbp, &tpl, ecp, txp);

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static void
free_ro_op(RDB_ro_op_desc *op, RDB_exec_context *ecp) {
    int i;

    free(op->name);
    if (op->argtv != NULL) {
        for (i = 0; i < op->argc; i++) {
            if (RDB_type_name(op->argtv[i]) == NULL)
                RDB_drop_type(op->argtv[i], ecp, NULL);
        }
        free(op->argtv);
    }
    if (op->rtyp != NULL && RDB_type_name(op->rtyp) == NULL)
        RDB_drop_type(op->rtyp, ecp, NULL);
    if (op->modhdl != NULL) {
        /* Built-in operator */
        lt_dlclose(op->modhdl);
        RDB_destroy_obj(&op->iarg, ecp);
    }
    free(op);
}

void
_RDB_free_ro_ops(RDB_ro_op_desc *op, RDB_exec_context *ecp)
{
    do {
        RDB_ro_op_desc *nextop = op->nextp;
        free_ro_op(op, ecp);
        op = nextop;
    } while (op != NULL);
}

static void
free_upd_op(RDB_upd_op *op, RDB_exec_context *ecp) {
    int i;

    free(op->name);
    for (i = 0; i < op->argc; i++) {
        if (RDB_type_name(op->argtv[i]) == NULL)
            RDB_drop_type(op->argtv[i], ecp, NULL);
    }
    free(op->argtv);
    free(op->updv);
    lt_dlclose(op->modhdl);
    RDB_destroy_obj(&op->iarg, ecp);
    free(op);
}

void
_RDB_free_upd_ops(RDB_upd_op *op, RDB_exec_context *ecp)
{
    do {
        RDB_upd_op *nextop = op->nextp;
        free_upd_op(op, ecp);
        op = nextop;
    } while (op != NULL);
}

static int
eq_bool(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val == argv[1]->var.bool_val));
    return RDB_OK;
}

static int
eq_binary(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
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
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    RDB_bool res;
    RDB_type *arep = NULL;

    if (argv[0]->kind != argv[1]->kind) {
        RDB_raise_type_mismatch("", ecp);
        return RDB_ERROR;
    }

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
                arep->compare_iarglen, ecp, txp, &retval);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&retval, ecp);
            return ret;
        }
        RDB_bool_to_obj(retvalp, (RDB_bool) RDB_obj_int(&retval) == 0);
        RDB_destroy_obj(&retval, ecp);
        return RDB_OK;
    }

    switch (argv[0]->kind) {
        case RDB_OB_INITIAL:
            RDB_raise_invalid_argument("invalid argument to equality", ecp);
            return RDB_ERROR;
        case RDB_OB_BOOL:
            return eq_bool("=", 2, argv, NULL, 0, ecp, txp, retvalp);
        case RDB_OB_INT:
        case RDB_OB_RATIONAL:
            /* Must not happen, because there must be a comparsion function */
            RDB_raise_internal("missing comparison function", ecp);
            return RDB_ERROR;
        case RDB_OB_BIN:
            return eq_binary("=", 2, argv, NULL, 0, ecp, txp, retvalp);
        case RDB_OB_TUPLE:
            ret = _RDB_tuple_equals(argv[0], argv[1], ecp, txp, &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            break;
        case RDB_OB_TABLE:
            if (!RDB_type_equals(argv[0]->var.tbp->typ,
                    argv[1]->var.tbp->typ)) {
                RDB_raise_type_mismatch("", ecp);
                return RDB_ERROR;
            }
            ret = RDB_table_equals(argv[0]->var.tbp, argv[1]->var.tbp, ecp, NULL,
                    &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            break;
        case RDB_OB_ARRAY:
            ret = _RDB_array_equals(argv[0], argv[1], ecp, txp, &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            break;
    }
    return RDB_OK;
} 

int
obj_not_equals(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp) {
    int ret = obj_equals("=", 2, argv, NULL, 0, ecp, txp, retvalp);
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

static RDB_ro_op_desc *
get_ro_op(RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_type *argtv[], RDB_exec_context *ecp)
{
    RDB_ro_op_desc *rop;
    RDB_bool pm = RDB_FALSE;

    rop = RDB_hashmap_get(&dbrootp->ro_opmap, name);

    /* Search for an operator with same signature */
    while (rop != NULL) {
        if (rop->argc == argc) {
            int i;

            pm = RDB_TRUE;
            for (i = 0; (i < argc)
                    && RDB_type_equals(rop->argtv[i], argtv[i]);
                 i++);
            if (i == argc) {
                /* Found */
                return rop;
            }
        }
        rop = rop->nextp;
    }

    if (pm) {
        RDB_raise_type_mismatch("", ecp);
    } else {
        RDB_raise_operator_not_found(name, ecp);
    }
    return NULL;
}

static RDB_type **
valv_to_typev(int valc, RDB_object **valv, RDB_exec_context *ecp)
{
    int i;
    RDB_type **typv = malloc(sizeof (RDB_type *) * valc);

    if (typv == NULL)
        return NULL;
    for (i = 0; i < valc; i++) {
        if (valv[i]->kind == RDB_OB_TUPLE)
            typv[i] = _RDB_tuple_type(valv[i], ecp);
        else
            typv[i] = RDB_obj_type(valv[i]);
    }
    return typv;
}

int
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_exec_context *ecp, RDB_transaction *txp,
               RDB_ro_op_desc **opp)
{
    int i;
    int ret;
    RDB_bool typnull = RDB_FALSE;
    RDB_bool typmismatch = RDB_FALSE;

    /* Check one of the types if NULL */
    for (i = 0; i < argc && !typnull; i++) {
        if (argtv[i] == NULL) {
            typnull = RDB_TRUE;
        }
    }

    if (!typnull) {
        RDB_type *errtyp;

        /* Lookup operator in map */
        *opp = get_ro_op(txp->dbp->dbrootp, name, argc, argtv, ecp);
        if (*opp != NULL)
            return RDB_OK;

        errtyp = RDB_obj_type(RDB_get_err(ecp));        
        if (errtyp != &RDB_OPERATOR_NOT_FOUND_ERROR
                && errtyp != &RDB_TYPE_MISMATCH_ERROR) {
            return RDB_ERROR;
        }
        if (errtyp == &RDB_TYPE_MISMATCH_ERROR) {
            typmismatch = RDB_TRUE;
        }
        RDB_clear_err(ecp);
    }

    /*
     * Search for a generic operator
     */
    *opp = RDB_hashmap_get(&txp->dbp->dbrootp->ro_opmap, name);
    while ((*opp) != NULL) {
        if ((*opp)->argtv == NULL) {
            /* Generic operator found */
            return RDB_OK;
        }
        *opp = (*opp)->nextp;
    } 

    /*
     * If one of the argument types is NULL and a generic operator
     * was not found, return with failure
     */
    if (typnull) {
        RDB_raise_operator_not_found(name, ecp);
        return RDB_ERROR;
    }

    /*
     * Provide "=" and "<>" for user-defined types
     */
    if (argc == 2 && RDB_type_equals(argtv[0], argtv[1])) {
        RDB_ro_op_desc *op;
        int ret;

        if (strcmp(name, "=") == 0) {
            op = new_ro_op("=", 2, &RDB_BOOLEAN, &obj_equals);
            if (op == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            op->argtv[0] = _RDB_dup_nonscalar_type(argtv[0], ecp);
            op->argtv[1] = _RDB_dup_nonscalar_type(argtv[1], ecp);
            ret = put_ro_op(txp->dbp->dbrootp, op);
            if (ret != RDB_OK)
                return ret;
            *opp = op;
            return RDB_OK;
        }
        if (strcmp(name, "<>") == 0) {
            op = new_ro_op("<>", 2, &RDB_BOOLEAN, &obj_not_equals);
            if (op == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            op->argtv[0] = _RDB_dup_nonscalar_type(argtv[0], ecp);
            op->argtv[1] = _RDB_dup_nonscalar_type(argtv[1], ecp);
            ret = put_ro_op(txp->dbp->dbrootp, op);
            if (ret != RDB_OK)
                return ret;
            *opp = op;
            return RDB_OK;
        }
    }

    /*
     * Operator was not found in map, so read from catalog
     */
    if (_RDB_cat_get_ro_op(name, argc, argtv, ecp, txp, opp) != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            if (typmismatch) {
                RDB_raise_type_mismatch("", ecp);
            } else {
                RDB_raise_operator_not_found(name, ecp);
            }
        }
        return RDB_ERROR;
    }
    
    /* Insert operator into map */
    ret = put_ro_op(txp->dbp->dbrootp, *opp);
    if (ret != RDB_OK) {
        free_ro_op(*opp, ecp);
        return ret;
    }

    return RDB_OK;
}

int
_RDB_check_type_constraint(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (valp->typ->var.scalar.constraintp != NULL) {
        RDB_bool result;
        RDB_object tpl;

        RDB_init_obj(&tpl);

        /* Set tuple attribute */
        ret = RDB_tuple_set(&tpl, valp->typ->name, valp, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        ret = RDB_evaluate_bool(valp->typ->var.scalar.constraintp, &tpl,
                ecp, txp, &result);
        RDB_destroy_obj(&tpl, ecp);
        if (ret != RDB_OK) {
            return ret;
        }
        if (!result) {
            RDB_raise_type_constraint_violation("", ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
neq_bool(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val != argv[1]->var.bool_val));
    return RDB_OK;
}

static int
neq_binary(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
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
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *tbp, *vtbp;
    int renc = (argc - 1) / 2;
    RDB_renaming *renv = malloc(sizeof(RDB_renaming) * renc);
    if (renv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < renc; i++) {
        if (argv[1 + i]->typ != &RDB_STRING
                || argv[2 + i]->typ != &RDB_STRING) {
            free(renv);
            RDB_raise_type_mismatch("RENAME argument must be STRING", ecp);
            return RDB_ERROR;
        }
        renv[i].from = RDB_obj_string(argv[1 + i * 2]);
        renv[i].to = RDB_obj_string(argv[2 + i * 2]);
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {
        vtbp = RDB_rename(tbp, renc, renv, ecp);
        ret = vtbp == NULL ? RDB_ERROR : RDB_OK;
    } else {
        ret = RDB_rename_tuple(argv[0], renc, renv, ecp, retvalp);
    }
    free(renv);
    if (ret != RDB_OK)
        return ret;
    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp, ecp);

        /*
         * Since the table is consumed by RDB_rename, prevent it
         * from being deleted when argv[0] is destroyed.
         */
        argv[0]->var.tbp = NULL;
    }

    return RDB_OK;
}

static int
op_project(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *tbp, *vtbp;
    char **attrv = malloc(sizeof(char *) * (argc - 1));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {
        vtbp = RDB_project(tbp, argc - 1, attrv, ecp);
        ret = vtbp == NULL ? RDB_ERROR : RDB_OK;
    } else {
        ret = RDB_project_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    }
    free(attrv);
    if (ret != RDB_OK)
        return ret;
    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp, ecp);
        argv[0]->var.tbp = NULL;
    }
    return RDB_OK;
}

static int
op_remove(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *tbp, *vtbp;
    char **attrv = malloc(sizeof(char *) * (argc - 1));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {
        vtbp = RDB_remove(tbp, argc - 1, attrv, ecp);
        ret = vtbp == NULL ? RDB_ERROR : RDB_OK;
    } else {
        ret = RDB_remove_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    }
    free(attrv);
    if (ret != RDB_OK)
        return ret;
    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp, ecp);
        argv[0]->var.tbp = NULL;
    }
    return RDB_OK;
}

static int
op_wrap(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i, j;
    RDB_table *tbp, *vtbp;
    int wrapc;
    RDB_wrapping *wrapv;

    if (argc < 1 || argc %2 != 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return RDB_ERROR;
    }

    wrapc = argc % 2;
    if (wrapc > 0) {
        wrapv = malloc(sizeof(RDB_wrapping) * wrapc);
        if (wrapv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }
    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrv = NULL;
    }

    for (i = 0; i < wrapc; i++) {
        RDB_object *objp;

        wrapv[i].attrc =  RDB_array_length(argv[i * 2 + 1], ecp);
        wrapv[i].attrv = malloc(sizeof (char *) * wrapv[i].attrc);
        if (wrapv[i].attrv == NULL) {
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        for (j = 0; j < wrapv[i].attrc; j++) {
            objp = RDB_array_get(argv[i * 2 + 1], (RDB_int) j, ecp);
            if (objp == NULL) {
                ret = RDB_ERROR;
                goto cleanup;
            }
            wrapv[i].attrv[j] = RDB_obj_string(objp);
        }        
        wrapv[i].attrname = RDB_obj_string(argv[i * 2 + 2]);
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {
        vtbp = RDB_wrap(tbp, wrapc, wrapv, ecp);
        if (vtbp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    } else {
        ret = RDB_wrap_tuple(argv[0], wrapc, wrapv, ecp, retvalp);
        if (ret != RDB_OK)
            goto cleanup;
    }
    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp, ecp);
        argv[0]->var.tbp = NULL;
    }
    ret = RDB_OK;

cleanup:
    for (i = 0; i < wrapc; i++) {
        free(wrapv[i].attrv);
    }
    if (wrapc > 0)
        free(wrapv);

    return ret;
}

static int
op_unwrap(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i;
    RDB_table *tbp, *vtbp;
    char **attrv;

    if (argc < 1) {
        RDB_raise_invalid_argument("invalid argument to UNWRAP", ecp);
        return RDB_ERROR;
    }
    
    attrv = malloc(sizeof(char *) * (argc - 1));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {
        vtbp = RDB_unwrap(tbp, argc - 1, attrv, ecp);
        ret = vtbp == NULL ? RDB_ERROR : RDB_OK;
    } else {
        ret = RDB_unwrap_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    }
    free(attrv);
    if (ret != RDB_OK)
        return ret;
    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp, ecp);
        argv[0]->var.tbp = NULL;
    }
    return RDB_OK;
}

static int
op_group(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    RDB_table *vtbp;
    char **attrv;
    int attrc;

    if (argc < 2) {
        RDB_raise_invalid_argument("invalid argument to GROUP", ecp);
        return RDB_ERROR;
    }

    attrc = argc - 2;
    attrv = malloc(sizeof(char *) * attrc);
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < attrc; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    vtbp = RDB_group(RDB_obj_table(argv[0]), attrc, attrv,
            RDB_obj_string(argv[argc - 1]), ecp);
    free(attrv);
    if (vtbp == NULL)
        return RDB_ERROR;
    RDB_table_to_obj(retvalp, vtbp, ecp);
    argv[0]->var.tbp = NULL;

    return RDB_OK;
}

static int
op_ungroup(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_table *vtbp;

    vtbp = RDB_ungroup(RDB_obj_table(argv[0]), RDB_obj_string(argv[1]), ecp);
    if (vtbp == NULL)
        return RDB_ERROR;
    RDB_table_to_obj(retvalp, vtbp, ecp);
    argv[0]->var.tbp = NULL;

    return RDB_OK;
}

static int
op_union(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_table *vtbp;
    
    if (argc != 2) {
        RDB_raise_invalid_argument("invalid argument to UNION", ecp);
        return RDB_ERROR;
    }
    
    vtbp = RDB_union(RDB_obj_table(argv[0]), RDB_obj_table(argv[1]), ecp);
    if (vtbp == NULL)
        return RDB_ERROR;

    RDB_table_to_obj(retvalp, vtbp, ecp);
    argv[0]->var.tbp = NULL;
    argv[1]->var.tbp = NULL;
    return RDB_OK;
}

static int
op_minus(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_table *vtbp;
    
    if (argc != 2) {
        RDB_raise_invalid_argument("invalid argument to MINUS", ecp);
        return RDB_ERROR;
    }
    
    vtbp = RDB_minus(RDB_obj_table(argv[0]), RDB_obj_table(argv[1]), ecp);
    if (vtbp == NULL)
        return RDB_ERROR;

    RDB_table_to_obj(retvalp, vtbp, ecp);
    argv[0]->var.tbp = NULL;
    argv[1]->var.tbp = NULL;
    return RDB_OK;
}

static int
op_intersect(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_table *vtbp;
    
    if (argc != 2) {
        RDB_raise_invalid_argument("invalid argument to INTERSECT", ecp);
        return RDB_ERROR;
    }
    
    vtbp = RDB_intersect(RDB_obj_table(argv[0]), RDB_obj_table(argv[1]), ecp);
    if (vtbp == NULL)
        return RDB_ERROR;

    RDB_table_to_obj(retvalp, vtbp, ecp);
    argv[0]->var.tbp = NULL;
    argv[1]->var.tbp = NULL;
    return RDB_OK;
}

static int
op_join(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    RDB_table *tbp;
    RDB_table *vtbp;
    
    if (argc != 2) {
        RDB_raise_invalid_argument("invalid argument to JOIN", ecp);
        return RDB_ERROR;
    }

    tbp = RDB_obj_table(argv[0]);
    if (tbp != NULL) {    
        vtbp = RDB_join(tbp, RDB_obj_table(argv[1]), ecp);
        ret = vtbp == NULL ? RDB_ERROR : RDB_OK;
    } else {
        ret = RDB_join_tuples(argv[0], argv[1], ecp, txp, retvalp);
    }
    if (ret != RDB_OK)
        return ret;

    if (tbp != NULL) {
        RDB_table_to_obj(retvalp, vtbp, ecp);
        argv[0]->var.tbp = NULL;
        argv[1]->var.tbp = NULL;
    }
    return RDB_OK;
}

static int
op_divide(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_table *vtbp;
    
    if (argc != 3) {
        RDB_raise_invalid_argument("invalid argument to DIVIDE", ecp);
        return RDB_ERROR;
    }

    vtbp = RDB_sdivide(RDB_obj_table(argv[0]), RDB_obj_table(argv[1]),
            RDB_obj_table(argv[2]), ecp);
    if (vtbp == NULL)
        return RDB_ERROR;

    RDB_table_to_obj(retvalp, vtbp, ecp);
    argv[0]->var.tbp = NULL;
    argv[1]->var.tbp = NULL;
    argv[2]->var.tbp = NULL;
    return RDB_OK;
}

int
RDB_call_ro_op(const char *name, int argc, RDB_object *argv[],
               RDB_exec_context *ecp, RDB_transaction *txp,
               RDB_object *retvalp)
{
    RDB_ro_op_desc *op;
    int ret;
    RDB_type **argtv;
    int i;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Handle nonscalar comparison
     */
    if (argc == 2 && !obj_is_scalar(argv[0]) && !obj_is_scalar(argv[1])) {
        if (strcmp(name, "=") == 0)
            return obj_equals(name, 2, argv, NULL, 0, ecp, txp, retvalp);
        if (strcmp(name, "<>") == 0) {
            ret = obj_equals(name, 2, argv, NULL, 0, ecp, txp, retvalp);
            if (ret != RDB_OK)
                return ret;
            retvalp->var.bool_val = (RDB_bool) !retvalp->var.bool_val;
            return RDB_OK;
        }
    }

    /*
     * Handle IF-THEN-ELSE
     */
    if (strcmp(name, "IF") == 0 && argc == 3) {
        if (argv[0]->typ != &RDB_BOOLEAN) {
            RDB_raise_type_mismatch("IF argument must be BOOLEAN", ecp);
            return RDB_ERROR;
        }
        if (argv[0]->var.bool_val) {
            ret = RDB_copy_obj(retvalp, argv[1], ecp);
        } else {
            ret = RDB_copy_obj(retvalp, argv[2], ecp);
        }
        return ret;
    }

    /*
     * Handle built-in operators with relational arguments
     */
    if (argc == 1 && obj_is_table(argv[0]))  {
        if (strcmp(name, "IS_EMPTY") == 0) {
            RDB_bool res;

            ret = RDB_table_is_empty(argv[0]->var.tbp, ecp, txp, &res);
            if (ret != RDB_OK)
                return ret;

            RDB_bool_to_obj(retvalp, res);
            return RDB_OK;
        }
        if (strcmp(name, "COUNT") == 0) {
            ret = RDB_cardinality(argv[0]->var.tbp, ecp, txp);
            if (ret < 0)
                return ret;

            RDB_int_to_obj(retvalp, ret);
            return RDB_OK;
        }
    } else if (argc == 2 && obj_is_table(argv[1])) {
        if (strcmp(name, "IN") == 0) {
            RDB_bool b;

            ret = RDB_table_contains(argv[1]->var.tbp, argv[0], ecp, txp, &b);
            if (ret != RDB_OK)
                return ret;

            RDB_bool_to_obj(retvalp, b);
            return RDB_OK;
        } else if (strcmp(name, "SUBSET_OF") == 0) {
            RDB_bool res;

            ret = RDB_subset(argv[0]->var.tbp, argv[1]->var.tbp, ecp, txp,
                    &res);
            if (ret != RDB_OK)
                return ret;
            RDB_bool_to_obj(retvalp, res);
            return RDB_OK;
        }
    }
    if (argc >= 1 && obj_is_table(argv[0]) && strcmp(name, "TO_TUPLE") == 0
            && argc == 1) {
        return RDB_extract_tuple(RDB_obj_table(argv[0]), ecp, txp, retvalp);
    }

    argtv = valv_to_typev(argc, argv, ecp);
    if (argtv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_get_ro_op(name, argc, argtv, ecp, txp, &op);
    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TUPLE)
            RDB_drop_type(argtv[i], ecp, NULL);
    }

    free(argtv);

    if (ret != RDB_OK) {
        goto error;
    }

    /* Set return type to make it available to the function */
    retvalp->typ = op->rtyp;

    ret = (*op->funcp)(name, argc, argv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, ecp, txp, retvalp);
    if (ret != RDB_OK)
        goto error;

    /* Check type constraint if the operator is a selector */
    if (retvalp->typ != NULL &&_RDB_get_possrep(retvalp->typ, name) != NULL) {
        ret = _RDB_check_type_constraint(retvalp, ecp, txp);
        if (ret != RDB_OK) {
            /* Destroy illegal value */
            RDB_destroy_obj(retvalp, ecp);
            RDB_init_obj(retvalp);
            return ret;
        }
    }

    return RDB_OK;

error:
    return RDB_ERROR;
}

static RDB_upd_op *
get_upd_op(const RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_type *argtv[], RDB_exec_context *ecp)
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
                RDB_exec_context *ecp, RDB_transaction *txp, RDB_upd_op **opp)
{
    int ret;

    *opp = get_upd_op(txp->dbp->dbrootp, name, argc, argtv, ecp);
    if (*opp == NULL) {
        ret = _RDB_cat_get_upd_op(name, argc, argtv, ecp, txp, opp);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                RDB_clear_err(ecp);
                RDB_raise_operator_not_found(name, ecp);
            }
            return RDB_ERROR;
        }
        ret = put_upd_op(txp->dbp->dbrootp, *opp);
        if (ret != RDB_OK) {
            free_upd_op(*opp, ecp);
            return ret;
        }
    }
    return RDB_OK;
}

int
RDB_call_update_op(const char *name, int argc, RDB_object *argv[],
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_upd_op *op;
    RDB_type **argtv;
    int ret;
    int i;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    argtv = valv_to_typev(argc, argv, ecp);
    if (argtv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    ret = _RDB_cat_get_upd_op(name, argc, argtv, ecp, txp, &op);
    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TUPLE)
            RDB_drop_type(argtv[i], ecp, NULL);
    }
    free(argtv);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            RDB_raise_operator_not_found(name, ecp);
        }
        return RDB_ERROR;
    }

    return (*op->funcp)(name, argc, argv, op->updv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, ecp, txp);
}

int
RDB_drop_op(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp;
    RDB_table *vtbp;
    int ret;
    RDB_bool isempty;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Check if it's a read-only operator
     */
    exp = RDB_eq(RDB_expr_attr("NAME", ecp), RDB_string_to_expr(name, ecp), ecp);
    if (exp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    vtbp = RDB_select(txp->dbp->dbrootp->ro_ops_tbp, exp, ecp, txp);
    if (vtbp == NULL) {
        return RDB_ERROR;
    }
    ret = RDB_table_is_empty(vtbp, ecp, txp, &isempty);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, ecp, txp);
        return ret;
    }
    ret = RDB_drop_table(vtbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    if (isempty) {
        /* It's an update operator */
        RDB_upd_op *oldop;
        RDB_upd_op *op = NULL;

        /* Delete all versions of update operator from hashmap */
        oldop = RDB_hashmap_get(&txp->dbp->dbrootp->upd_opmap, name);
        if (oldop != NULL)
            _RDB_free_upd_ops(oldop, ecp);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->upd_opmap, name, op);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }
        
        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_expr_attr("NAME", ecp), RDB_string_to_expr(name, ecp), ecp);
        if (exp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->upd_ops_tbp, exp, ecp, txp);
        RDB_drop_expr(exp, ecp);
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
            _RDB_free_ro_ops(oldop, ecp);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->ro_opmap, name, op);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }

        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_expr_attr("NAME", ecp), RDB_string_to_expr(name, ecp),
               ecp);
        if (exp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        ret = RDB_delete(txp->dbp->dbrootp->ro_ops_tbp, exp, ecp, txp);
        if (ret != RDB_OK) {
            RDB_drop_expr(exp, ecp);
            _RDB_handle_errcode(ret, ecp, txp);
            return RDB_ERROR;
        }
    }

    return RDB_OK;
}

/*
 * Built-in operators
 */

static int
integer_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (int) argv[0]->var.rational_val);
    return RDB_OK;
}

static int
integer_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_int_to_obj(retvalp, (RDB_int)
            strtol(argv[0]->var.bin.datap, &endp, 10));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to INTEGER failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
rational_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp, (RDB_rational) argv[0]->var.int_val);
    return RDB_OK;
}

static int
rational_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_rational_to_obj(retvalp, (RDB_rational)
            strtod(argv[0]->var.bin.datap, &endp));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to RATIONAL failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
string_obj(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_obj_to_string(retvalp, argv[0], ecp);
}

static int
length_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    size_t len = mbstowcs(NULL, argv[0]->var.bin.datap, 0);
    if (len == -1) {
        RDB_raise_invalid_argument("", ecp);
        return RDB_ERROR;
    }

    RDB_int_to_obj(retvalp, (RDB_int) len);
    return RDB_OK;
}

static int
substring(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int start = argv[1]->var.int_val;
    int len = argv[2]->var.int_val;
    int i;
    int cl;
    int bstart, blen;

    /* Operands must not be negative */
    if (len < 0 || start < 0) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    /* Find start of substring */
    bstart = 0;
    for (i = 0; i < start && bstart < argv[0]->var.bin.len - 1; i++) {
        cl = mblen(((char *) argv[0]->var.bin.datap) + bstart, 4);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
            return RDB_ERROR;
        }
        bstart += cl;
    }
    if (bstart >= argv[0]->var.bin.len - 1) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    /* Find end of substring */
    blen = 0;
    for (i = 0; i < len && bstart + blen < argv[0]->var.bin.len; i++) {
        cl = mblen(((char *) argv[0]->var.bin.datap) + bstart + blen, 4);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
            return RDB_ERROR;
        }
        blen += cl > 0 ? cl : 1;
    }
    if (bstart + blen >= argv[0]->var.bin.len) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(retvalp, ecp);
    retvalp->typ = &RDB_STRING;
    retvalp->kind = RDB_OB_BIN;
    retvalp->var.bin.len = blen + 1;
    retvalp->var.bin.datap = malloc(retvalp->var.bin.len);
    if (retvalp->var.bin.datap == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    strncpy(retvalp->var.bin.datap, (char *) argv[0]->var.bin.datap
            + bstart, blen);
    ((char *) retvalp->var.bin.datap)[blen] = '\0';
    return RDB_OK;
}

static int
concat(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    size_t s1len = strlen(argv[0]->var.bin.datap);

    RDB_destroy_obj(retvalp, ecp);
    RDB_init_obj(retvalp);
    _RDB_set_obj_type(retvalp, &RDB_STRING);
    retvalp->var.bin.len = s1len + strlen(argv[1]->var.bin.datap) + 1;
    retvalp->var.bin.datap = malloc(retvalp->var.bin.len);
    if (retvalp->var.bin.datap == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    strcpy(retvalp->var.bin.datap, argv[0]->var.bin.datap);
    strcpy(((char *)retvalp->var.bin.datap) + s1len, argv[1]->var.bin.datap);
    return RDB_OK;
}

static int
matches(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    regex_t reg;
    int ret;

    ret = regcomp(&reg, argv[1]->var.bin.datap, REG_NOSUB);
    if (ret != 0) {
        RDB_raise_invalid_argument("invalid regular expression", ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool)
            (regexec(&reg, argv[0]->var.bin.datap, 0, NULL, 0) == 0));
    regfree(&reg);

    return RDB_OK;
}

static int
and(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val && argv[1]->var.bool_val);
    return RDB_OK;
}

static int
or(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val || argv[1]->var.bool_val);
    return RDB_OK;
}

static int
not(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool) !argv[0]->var.bool_val);
    return RDB_OK;
}

static int
lt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) < 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
let(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) <= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
gt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) > 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
get(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, ecp, txp,
            &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) >= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
negate_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, -argv[0]->var.int_val);
    return RDB_OK;
}

static int
negate_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp, -argv[0]->var.rational_val);
    return RDB_OK;
}

static int
add_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val + argv[1]->var.int_val);
    return RDB_OK;
}

static int
add_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp,
            argv[0]->var.rational_val + argv[1]->var.rational_val);
    return RDB_OK;
}

static int
subtract_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val - argv[1]->var.int_val);
    return RDB_OK;
}

static int
subtract_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp,
            argv[0]->var.rational_val - argv[1]->var.rational_val);
    return RDB_OK;
}

static int
multiply_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val * argv[1]->var.int_val);
    return RDB_OK;
}

static int
multiply_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_rational_to_obj(retvalp,
            argv[0]->var.rational_val * argv[1]->var.rational_val);
    return RDB_OK;
}

static int
divide_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->var.int_val == 0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(retvalp, argv[0]->var.int_val / argv[1]->var.int_val);
    return RDB_OK;
}

static int
divide_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->var.rational_val == 0.0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_rational_to_obj(retvalp,
            argv[0]->var.rational_val / argv[1]->var.rational_val);
    return RDB_OK;
}

/* !! */
int
_RDB_add_builtin_ops(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    RDB_ro_op_desc *op;
    int ret;

    op = new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_rational);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_string);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("RATIONAL", 1, &RDB_RATIONAL, &rational_int);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("RATIONAL", 1, &RDB_RATIONAL, &rational_string);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("STRING", 1, &RDB_STRING, &string_obj);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("STRING", 1, &RDB_STRING, &string_obj);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("LENGTH", 1, &RDB_INTEGER, &length_string);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("SUBSTRING", 3, &RDB_STRING, &substring);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_INTEGER;
    op->argtv[2] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("||", 2, &RDB_STRING, &concat);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("MATCHES", 2, &RDB_BOOLEAN, &matches);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("AND", 2, &RDB_BOOLEAN, &and);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("OR", 2, &RDB_BOOLEAN, &or);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("NOT", 1, &RDB_BOOLEAN, &not);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<", 2, &RDB_BOOLEAN, &lt);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<", 2, &RDB_BOOLEAN, &lt);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<", 2, &RDB_BOOLEAN, &lt);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<=", 2, &RDB_BOOLEAN, &let);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<=", 2, &RDB_BOOLEAN, &let);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<=", 2, &RDB_BOOLEAN, &let);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">", 2, &RDB_BOOLEAN, &gt);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">", 2, &RDB_BOOLEAN, &gt);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">", 2, &RDB_BOOLEAN, &gt);
    if (op == NULL) { 
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">=", 2, &RDB_BOOLEAN, &get);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">=", 2, &RDB_BOOLEAN, &get);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op(">=", 2, &RDB_BOOLEAN, &get);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, &eq_bool);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    op = new_ro_op("=", 2, &RDB_BOOLEAN, obj_equals);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, obj_equals);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, obj_equals);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("=", 2, &RDB_BOOLEAN, &eq_binary);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BINARY;
    op->argtv[1] = &RDB_BINARY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &neq_bool);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &obj_not_equals);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &obj_not_equals);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &obj_not_equals);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("<>", 2, &RDB_BOOLEAN, &neq_binary);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BINARY;
    op->argtv[1] = &RDB_BINARY;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("-", 1, &RDB_INTEGER, &negate_int);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("-", 1, &RDB_RATIONAL, &negate_rational);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("+", 2, &RDB_INTEGER, &add_int);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("+", 2, &RDB_RATIONAL, &add_rational);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("-", 2, &RDB_INTEGER, &subtract_int);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("-", 2, &RDB_RATIONAL, &subtract_rational);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("*", 2, &RDB_INTEGER, &multiply_int);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("*", 2, &RDB_RATIONAL, &multiply_rational);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("/", 2, &RDB_INTEGER, &divide_int);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    op = new_ro_op("/", 2, &RDB_RATIONAL, &divide_rational);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("RENAME", -1, NULL, &op_rename);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("PROJECT", -1, NULL, &op_project);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("REMOVE", -1, NULL, &op_remove);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("WRAP", -1, NULL, &op_wrap);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("UNWRAP", -1, NULL, &op_unwrap);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("UNION", -1, NULL, &op_union);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("MINUS", -1, NULL, &op_minus);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("INTERSECT", -1, NULL, &op_intersect);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("JOIN", -1, NULL, &op_join);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("DIVIDE_BY_PER", -1, NULL, &op_divide);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("GROUP", -1, NULL, &op_group);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("UNGROUP", -1, NULL, &op_ungroup);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = new_ro_op("NOT_FOUND_ERROR", 0, &RDB_NOT_FOUND_ERROR, &_RDB_sys_select);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    op = new_ro_op("OPERATOR_NOT_FOUND_ERROR", 0,
            &RDB_OPERATOR_NOT_FOUND_ERROR, &_RDB_sys_select);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    /* !! more selectors */

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}
