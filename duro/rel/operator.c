/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>
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

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

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

    ret = RDB_insert(txp->dbp->dbrootp->ro_ops_tbp, &tpl, txp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = _RDB_type_to_obj(&rtypobj, rtyp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "RTYPE", &rtypobj);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->ro_op_rtypes_tbp, &tpl, txp);
    /* Operator may be overloaded */
    if (ret == RDB_ELEMENT_EXISTS)
        ret = RDB_OK;

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
    for (i = 0; i < op->argc; i++) {
        if (RDB_type_name(op->argtv[i]) == NULL)
            RDB_drop_type(op->argtv[i], NULL);
    }
    free(op->argtv);
    if (RDB_type_name(op->rtyp) == NULL)
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

static RDB_ro_op_desc *
get_ro_op(const RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_type *argtv[])
{
    RDB_ro_op_desc **opp = RDB_hashmap_get(&dbrootp->ro_opmap, name, NULL);
    RDB_ro_op_desc *op;

    if (opp == NULL || *opp == NULL)
        return NULL;

    op = *opp;

    /* Find a operation with same signature */
    do {
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
    } while (op != NULL);

    return NULL;
}

static int
put_ro_op(RDB_dbroot *dbrootp, RDB_ro_op_desc *op)
{
    int ret;
    RDB_ro_op_desc **fopp = RDB_hashmap_get(&dbrootp->ro_opmap, op->name, NULL);

    if (fopp == NULL || *fopp == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&dbrootp->ro_opmap, op->name, &op, sizeof (op));
        if (ret != RDB_OK)
            return ret;
    } else {
        op->nextp = (*fopp)->nextp;
        (*fopp)->nextp = op;
    }
    return RDB_OK;
}

static RDB_type **
valv_to_typev(int valc, RDB_object **valv) {
    int i;
    RDB_type **typv = malloc(sizeof (RDB_type *) * valc);

    if (typv == NULL)
        return NULL;
    for (i = 0; i < valc; i++) {
        typv[i] = RDB_obj_type(valv[i]);
    }
    return typv;
}

int
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_transaction *txp, RDB_ro_op_desc **opp)
{
    int ret;

    /* Lookup operator in map */
    *opp = get_ro_op(txp->dbp->dbrootp, name, argc, argtv);

    if (*opp == NULL) {
        /* Not found in map, so read from catalog */
        ret = _RDB_get_cat_ro_op(name, argc, argtv, txp, opp);
        if (ret != RDB_OK)
            return ret;
        
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
    int i, j;
    int ret;
    RDB_bool result;

    /* Check constraint for each possrep */
    for (i = 0; i < valp->typ->var.scalar.repc; i++) {
        RDB_object tpl;

        if (valp->typ->var.scalar.repv[i].constraintp != NULL) {
            RDB_init_obj(&tpl);
            /* Set tuple attributes */
            for (j = 0; j < valp->typ->var.scalar.repv[i].compc; j++) {
                RDB_object comp;
                char *compname = valp->typ->var.scalar.repv[i].compv[j].name;

                RDB_init_obj(&comp);
                ret = RDB_obj_comp(valp, compname, &comp, txp);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&comp);
                    RDB_destroy_obj(&tpl);
                    return ret;
                }
                ret = RDB_tuple_set(&tpl, compname, &comp);
                RDB_destroy_obj(&comp);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&tpl);
                    return ret;
                }
            }
            RDB_evaluate_bool(valp->typ->var.scalar.repv[i].constraintp,
                    &tpl, txp, &result);
            RDB_destroy_obj(&tpl);
            if (!result) {
                return RDB_TYPE_CONSTRAINT_VIOLATION;
            }
        }
    }
    return RDB_OK;
}

int
RDB_call_ro_op(const char *name, int argc, RDB_object *argv[],
               RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_ro_op_desc *op;
    int ret;
    RDB_type **argtv;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    argtv = valv_to_typev(argc, argv);
    if (argtv == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }
    ret = _RDB_get_ro_op(name, argc, argtv, txp, &op);
    free(argtv);
    if (ret != RDB_OK)
        goto error;

    retvalp->typ = op->rtyp;
    ret = (*op->funcp)(name, argc, argv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, txp, retvalp);
    if (ret != RDB_OK)
        goto error;

    /* Check type constraint if the operator is a selector */
    if (_RDB_get_possrep(retvalp->typ, name) != NULL) {
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
    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}

static RDB_upd_op *
get_upd_op(const RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_type *argtv[])
{
    RDB_upd_op *op;
    RDB_upd_op **opp = RDB_hashmap_get(&dbrootp->upd_opmap, name, NULL);    

    if (opp == NULL)
        return NULL;
    op = *opp;
    
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
    RDB_upd_op **fopp = RDB_hashmap_get(&dbrootp->upd_opmap, op->name, NULL);

    if (fopp == NULL || *fopp == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&dbrootp->upd_opmap, op->name, &op, sizeof (op));
        if (ret != RDB_OK)
            return ret;
    } else {
        op->nextp = (*fopp)->nextp;
        (*fopp)->nextp = op;
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
        ret = _RDB_get_cat_upd_op(name, argc, argtv, txp, opp);
        if (ret != RDB_OK)
            return ret;
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

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    argtv = valv_to_typev(argc, argv);
    if (argtv == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }
    ret = _RDB_get_upd_op(name, argc, argtv, txp, &op);
    free(argtv);
    if (ret != RDB_OK)
        return ret;

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
    ret = RDB_select(txp->dbp->dbrootp->ro_ops_tbp, exp, &vtbp);
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
        RDB_upd_op **oldopp;
        RDB_upd_op *op = NULL;

        /* Delete all versions of update operator from hashmap */
        oldopp = (RDB_upd_op **)RDB_hashmap_get(&txp->dbp->dbrootp->ro_opmap,
                name, NULL);
        if (oldopp != NULL && *oldopp != NULL)
            _RDB_free_upd_ops(*oldopp);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->upd_opmap, name,
                &op, sizeof (op));
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
        RDB_ro_op_desc **oldopp;
        RDB_ro_op_desc *op = NULL;

        /* Delete all versions of readonly operator from hashmap */
        oldopp = (RDB_ro_op_desc **)RDB_hashmap_get(&txp->dbp->dbrootp->ro_opmap,
                name, NULL);
        if (oldopp != NULL && *oldopp != NULL)
            _RDB_free_ro_ops(*oldopp);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->ro_opmap, name,
                &op, sizeof (op));
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
            if (RDB_is_syserr(ret))
                RDB_rollback_all(txp);
            return ret;
        }
        ret = RDB_delete(txp->dbp->dbrootp->ro_op_rtypes_tbp, exp, txp);
        RDB_drop_expr(exp);
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret))
                RDB_rollback_all(txp);
            return ret;
        }
    }

    return RDB_OK;
}

static int
length_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.bin.len - 1);
    return RDB_OK;
}

static int
substring(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int start = argv[1]->var.int_val;
    int len = argv[2]->var.int_val;

    /* Operands must not be negative */
    if (len < 0 || start < 0)
        return RDB_INVALID_ARGUMENT;

    /* Check if substring exceeds source string */
    if (start + len + 1 > argv[0]->var.bin.len)
        return RDB_INVALID_ARGUMENT;

    RDB_destroy_obj(retvalp);
    retvalp->typ = &RDB_STRING;
    retvalp->kind = RDB_OB_BIN;
    retvalp->var.bin.len = len + 1;
    retvalp->var.bin.datap = malloc(retvalp->var.bin.len);
    if (retvalp->var.bin.datap == NULL)
        return RDB_NO_MEMORY;
    strncpy(retvalp->var.bin.datap, (char *) argv[0]->var.bin.datap
            + start, len);
    ((char *) retvalp->var.bin.datap)[len] = '\0';
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
    RDB_destroy_obj(retvalp);
    RDB_init_obj(retvalp);
    _RDB_set_obj_type(retvalp, &RDB_BOOLEAN);
    retvalp->var.bool_val = (RDB_bool)
            (regexec(&reg, argv[0]->var.bin.datap, 0, NULL, 0) == 0);
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
    RDB_bool_to_obj(retvalp, (RDB_bool)
            ((*argv[0]->typ->comparep)(argv[0], argv[1]) < 0));
    return RDB_OK;
}

static int
let(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            ((*argv[0]->typ->comparep)(argv[0], argv[1]) <= 0));
    return RDB_OK;
}

static int
gt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            ((*argv[0]->typ->comparep)(argv[0], argv[1]) > 0));
    return RDB_OK;
}

static int
get(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            ((*argv[0]->typ->comparep)(argv[0], argv[1]) >= 0));
    return RDB_OK;
}

int
_RDB_add_builtin_ops(RDB_dbroot *dbrootp)
{
    RDB_ro_op_desc *op;
    int ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("LENGTH");
    op->argc = 1;
    op->argtv = malloc(sizeof (RDB_type *));
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->rtyp = &RDB_INTEGER;
    op->funcp = &length_string;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("SUBSTRING");
    op->argc = 3;
    op->argtv = malloc(sizeof (RDB_type *) * 3);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_INTEGER;
    op->argtv[2] = &RDB_INTEGER;
    op->rtyp = &RDB_STRING;
    op->funcp = &substring;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("||");
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;
    op->rtyp = &RDB_STRING;
    op->funcp = &concat;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("MATCHES");
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &matches;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("AND");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &and;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("OR");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &or;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("NOT");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 1;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_BOOLEAN;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &not;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("<");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &lt;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("<");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &lt;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("<");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &lt;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("<=");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &let;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("<=");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &let;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str("<=");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &let;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str(">");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &gt;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str(">");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &gt;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str(">");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &gt;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str(">=");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &get;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str(">=");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_RATIONAL;
    op->argtv[1] = &RDB_RATIONAL;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &get;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    op = malloc(sizeof(RDB_dbroot));
    if (op == NULL)
        return RDB_NO_MEMORY;
    op->name = RDB_dup_str(">=");
    if (op->name == NULL)
        return RDB_NO_MEMORY;
    op->argc = 2;
    op->argtv = malloc(sizeof (RDB_type *) * 2);
    if (op->argtv == NULL)
        return RDB_NO_MEMORY;
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;
    op->rtyp = &RDB_BOOLEAN;
    op->funcp = &get;
    op->modhdl = NULL;

    ret = put_ro_op(dbrootp, op);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}
