/*
 * Public table functions.
 *
 *  Created on: 03.08.2013
 *
 * Copyright (C) 2013-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <rel/rdb.h>
#include "internal.h"
#include "catalog.h"
#include <obj/key.h>

/** @addtogroup table
 * @{
 */

int
RDB_create_public_table_from_type(const char *name,
                RDB_type *reltyp,
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_transaction tx;
    RDB_string_vec allkey;

    if (name != NULL && !RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid table name", ecp);
        return RDB_ERROR;
    }

    if (reltyp->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("relation type required", ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < reltyp->def.basetyp->def.tuple.attrc; i++) {
        if (!RDB_legal_name(reltyp->def.basetyp->def.tuple.attrv[i].name)) {
            RDB_object str;

            RDB_init_obj(&str);
            if (RDB_string_to_obj(&str, "invalid attribute name: ", ecp)
                    != RDB_OK) {
                RDB_destroy_obj(&str, ecp);
                return RDB_ERROR;
            }

            if (RDB_append_string(&str, reltyp->def.basetyp->def.tuple.attrv[i].name, ecp)
                    != RDB_OK) {
                RDB_destroy_obj(&str, ecp);
                return RDB_ERROR;
            }

            RDB_raise_invalid_argument(RDB_obj_string(&str), ecp);
            RDB_destroy_obj(&str, ecp);
            return RDB_ERROR;
        }
    }

    /* name may only be NULL if table is transient */
    if ((name == NULL)) {
        RDB_raise_invalid_argument("table must have a name", ecp);
        return RDB_ERROR;
    }

    allkey.strv = NULL;
    if (keyv != NULL) {
        if (RDB_check_keys(reltyp, keyc, keyv, ecp) != RDB_OK)
            return RDB_ERROR;
    } else {
        if (RDB_all_key(reltyp->def.basetyp->def.tuple.attrc,
                reltyp->def.basetyp->def.tuple.attrv, ecp, &allkey) != RDB_OK)
            return RDB_ERROR;

        keyc = 1;
        keyv = &allkey;
    }

    /* Create subtransaction */
    if (RDB_begin_tx(ecp, &tx, txp->dbp, txp) != RDB_OK)
        goto error;

    /* Insert table into catalog */
    if (RDB_cat_insert_ptable(name, reltyp->def.basetyp->def.tuple.attrc,
            reltyp->def.basetyp->def.tuple.attrv, keyc, keyv, ecp, &tx) != RDB_OK) {
        /* Don't destroy type */
        RDB_rollback(ecp, &tx);
        goto error;
    }

    if (RDB_commit(ecp, &tx) != RDB_OK) {
        goto error;
    }

    RDB_del_nonscalar_type(reltyp, ecp);
    RDB_free(allkey.strv);
    return RDB_OK;

error:
    RDB_del_nonscalar_type(reltyp, ecp);
    RDB_free(allkey.strv);
    return RDB_ERROR;
}

/**
Create a public table with name <var>name</var>.
The table must be mapped to a relational expression before it can be used.

@returns

RDB_OK on success, RDB_ERROR on failure.
*/
int
RDB_create_public_table(const char *name,
                int attrc, const RDB_attr attrv[],
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *tbtyp = RDB_new_relation_type(attrc, attrv, ecp);
    if (tbtyp == NULL) {
        return RDB_ERROR;
    }

    return RDB_create_public_table_from_type(name, tbtyp, keyc, keyv,
            ecp, txp);
}

/**
 * Map a public table to a relational expression.

@returns RDB_OK on success, RDB_ERROR on failure.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>not_found_error
<dd>A public table with name <var>name</var> has not been defined.
<dt>type_mismatch_error
<dd>The type of *<var>exp</var> does not match the type of the table.
<dt>invalid_argument_error
<dd>The inferred keys of *<var>exp</var> do not match the keys of the table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_map_public_table(const char *name, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *tbtyp, *exptyp;
    int keyc, ekeyc;
    RDB_string_vec *keyv, *ekeyv;
    RDB_bool isempty;
    RDB_bool eq;
    RDB_bool mustfree;
    RDB_object *tbp;

    /*
     * Check if table exists in the catalog
     */

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbp = RDB_cat_get_ptable_vt(name, ecp, txp);
    if (tbp == NULL)
        return RDB_ERROR;

    if (RDB_table_is_empty(tbp, ecp, txp, &isempty) != RDB_OK) {
        RDB_drop_table(tbp, ecp, txp);
        return RDB_ERROR;
    }
    RDB_drop_table(tbp, ecp, txp);
    if (isempty) {
        RDB_raise_not_found(name, ecp);
        return RDB_ERROR;
    }

    /*
     * Check type
     */

    exptyp = RDB_expr_type(exp, NULL, NULL, NULL, ecp, txp);
    if (exptyp == NULL)
        return RDB_ERROR;

    tbtyp = RDB_cat_get_table_type(name, ecp, txp);
    if (tbtyp == NULL)
        return RDB_ERROR;

    if (!RDB_type_equals(exptyp, tbtyp)) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        RDB_raise_type_mismatch(
                "Expression type does not match type of public table", ecp);
        return RDB_ERROR;
    }
    RDB_del_nonscalar_type(tbtyp, ecp);

    /*
     * Check if keys match
     */

    if (RDB_cat_get_keys(name, ecp, txp, &keyc, &keyv) != RDB_OK)
        return RDB_ERROR;

    ekeyc = RDB_infer_keys(exp, NULL, NULL, NULL, ecp, txp,
            &ekeyv, &mustfree);
    if (ekeyc == RDB_ERROR) {
        RDB_free_keys(keyc, keyv);
        return RDB_ERROR;
    }

    eq = RDB_keys_equal(keyc, keyv, ekeyc, ekeyv);

    RDB_free_keys(keyc, keyv);
    if (mustfree)
        RDB_free_keys(ekeyc, ekeyv);

    if (!eq) {
        RDB_raise_invalid_argument("keys do not match", ecp);
        return RDB_ERROR;
    }

    /*
     * Check if the table is already in memory.
     * If so, replace the expression.
     */
    tbp = RDB_hashmap_get(&txp->dbp->dbrootp->ptbmap, name);
    if (tbp != NULL) {
        /* Found - replace expression */
        exp = RDB_dup_expr(exp, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        RDB_del_expr(tbp->val.tb.exp, ecp);
        tbp->val.tb.exp = exp;
    }

    /* Write the mapping to the catalog */
    return RDB_cat_map_ptable(name, exp, ecp, txp);
}

/* @} */
