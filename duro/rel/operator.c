/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"

static void
free_ro_op(RDB_ro_op *op) {
    int i;

    free(op->name);
    for (i = 0; i < op->argc; i++) {
        if (RDB_type_name(op->argtv[i]) == NULL)
            RDB_drop_type(op->argtv[i], NULL);
    }
    free(op->argtv);
    if (RDB_type_name(op->rtyp) == NULL)
        RDB_drop_type(op->rtyp, NULL);
    lt_dlclose(op->modhdl);
    RDB_destroy_obj(&op->iarg);
    free(op);
}

void
_RDB_free_ro_ops(RDB_ro_op *op)
{
    do {
        RDB_ro_op *nextop = op->nextp;
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

static RDB_ro_op *
get_ro_op(const RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_type *argtv[])
{
    RDB_ro_op **opp = RDB_hashmap_get(&dbrootp->ro_opmap, name, NULL);
    RDB_ro_op *op;

    if (opp == NULL || *opp == NULL)
        return NULL;
    
    op = *opp;

    /* Find a operation with same signature */
    do {
        if (op->argc == argc) {
            int i;

            for (i = 0; (i < argc)
                    && !RDB_type_equals(op->argtv[i], argtv[i]);
                 i++);
            if (i >= argc) {
                /* Found */
                return op;
            }
        }
        op = op->nextp;
    } while(op != NULL);

    return NULL;
}

static int
put_ro_op(RDB_dbroot *dbrootp, RDB_ro_op *op)
{
    int ret;
    RDB_ro_op **fopp = RDB_hashmap_get(&dbrootp->ro_opmap, op->name, NULL);

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
               RDB_transaction *txp, RDB_ro_op **opp)
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
check_type_constraint(RDB_object *valp, RDB_transaction *txp)
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
                    &tpl, NULL, &result);
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
               RDB_object *retvalp, RDB_transaction *txp)
{
    RDB_ro_op *op;
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
        ret = check_type_constraint(retvalp, txp);
        if (ret != RDB_OK)
            return ret;
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
                    && !RDB_type_equals(op->argtv[i], argtv[i]);
                 i++);
            if (i >= argc) {
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

    ret = (*op->funcp)(name, argc, argv, op->updv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, txp);
    return ret;
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
        RDB_ro_op **oldopp;
        RDB_ro_op *op = NULL;

        /* Delete all versions of readonly operator from hashmap */
        oldopp = (RDB_ro_op **)RDB_hashmap_get(&txp->dbp->dbrootp->ro_opmap,
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
        RDB_drop_expr(exp);
        if (ret != RDB_OK) {
            return ret;
        }
    }

    return RDB_OK;
}
