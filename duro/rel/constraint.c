/*
 * Copyright (C) 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

/*
 * Functions for declarative integrity constraints
 */

#include "rdb.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>

/*
 * Read constraints from catalog
 */
int
_RDB_read_constraints(RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object constrs;
    RDB_object *tplp;
    RDB_dbroot *dbrootp = RDB_tx_db(txp)->dbrootp;

    RDB_init_obj(&constrs);

    ret = RDB_table_to_array(&constrs, dbrootp->constraints_tbp, 0, NULL, txp);
    if (ret != RDB_OK)
        goto cleanup;

    for (i = 0; (ret = RDB_array_get(&constrs, i, &tplp)) == RDB_OK; i++) {
        RDB_constraint *constrp = malloc(sizeof(RDB_constraint));

        if (constrp == NULL) {
             ret = RDB_NO_MEMORY;
             goto cleanup;
        }
        constrp->name = RDB_dup_str(RDB_obj_string(RDB_tuple_get(tplp,
                "CONSTRAINTNAME")));
        if (constrp->name == NULL) {
            free(constrp);
            ret = RDB_NO_MEMORY;
            goto cleanup;
        }
        ret = _RDB_deserialize_expr(RDB_tuple_get(tplp, "I_EXPR"), txp,
                &constrp->exp);
        if (ret != RDB_OK) {
            free(constrp->name);
            free(constrp);
            goto cleanup;
        }
        constrp->empty_tbp = constrp->exp->var.op.argv[0]->var.obj.var.tbp;
        constrp->nextp = dbrootp->first_constrp;
        dbrootp->first_constrp = constrp;
    }
    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&constrs);
    return ret;
}

int
RDB_create_constraint(const char *name, RDB_expression *exp,
                      RDB_transaction *txp)
{
    int ret;
    RDB_dbroot *dbrootp;
    RDB_bool res;
    RDB_constraint *constrp;
    RDB_expression *argp;

    /* Only IS_EMPTY(table) is possible */
    if (exp->kind != RDB_EX_RO_OP
            || exp->var.op.argc != 1
            || strcmp(exp->var.op.name, "IS_EMPTY") != 0)
        return RDB_NOT_SUPPORTED;
    argp = exp->var.op.argv[0];
    if (argp->kind != RDB_EX_OBJ || argp->var.obj.kind != RDB_OB_TABLE)
        return RDB_NOT_SUPPORTED;

    /* Check constraint */
    ret = RDB_evaluate_bool(exp, NULL, txp, &res);
    if (ret != RDB_OK)
        return ret;
    if (!res) {
        return RDB_PREDICATE_VIOLATION;
    }

    if (!RDB_tx_db(txp)->dbrootp->constraints_read) {
        ret = _RDB_read_constraints(txp);
        if (ret != RDB_OK)
            return ret;
        RDB_tx_db(txp)->dbrootp->constraints_read = RDB_TRUE;
    }

    constrp = malloc(sizeof (RDB_constraint));
    if (constrp == NULL)
        return RDB_NO_MEMORY;

    constrp->exp = exp;
    constrp->empty_tbp = argp->var.obj.var.tbp;
    constrp->name = RDB_dup_str(name);
    if (constrp->name == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    /* Create constraint in catalog */
    ret = _RDB_cat_create_constraint(name, exp, txp);
    if (ret != RDB_OK)
        goto error;

    /* Insert constraint into list */
    dbrootp = RDB_tx_db(txp)->dbrootp;
    constrp->nextp = dbrootp->first_constrp;
    dbrootp->first_constrp = constrp;
    
    return RDB_OK;

error:
    free(constrp->name);
    free(constrp);

    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}

int
RDB_drop_constraint(const char *name, RDB_transaction *txp)
{
    int ret;
    RDB_expression *condp;
    RDB_dbroot *dbrootp = RDB_tx_db(txp)->dbrootp;

    if (dbrootp->constraints_read) {
        /* Delete constraint from list */
        RDB_constraint *constrp = dbrootp->first_constrp;
        if (constrp == NULL)
            return RDB_NOT_FOUND;

        if (strcmp(constrp->name, name) == 0) {
            dbrootp->first_constrp = constrp->nextp;
            RDB_drop_expr(constrp->exp);
            free(constrp->name);
            free(constrp);
        } else {
            RDB_constraint *hconstrp;

            while (constrp->nextp != NULL
                    && strcmp(constrp->nextp->name, name) !=0) {
                constrp = constrp->nextp;
            }
            if (constrp->nextp == NULL)
                return RDB_NOT_FOUND;
            hconstrp = constrp->nextp;
            constrp->nextp = constrp->nextp->nextp;
            RDB_drop_expr(hconstrp->exp);
            free(hconstrp->name);
            free(hconstrp);
        }
    }

    /* Delete constraint from catalog */
    condp = RDB_eq(RDB_expr_attr("CONSTRAINTNAME"), RDB_string_to_expr(name));
    if (condp == NULL)
        return RDB_NO_MEMORY;
    ret = RDB_delete(dbrootp->constraints_tbp, condp, txp);
    RDB_drop_expr(condp);

    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}
