/*
 * interp_vardef.c
 *
 *  Created on: 16.03.2014
 *      Author: rene
 */

#include "interp_core.h"
#include "interp_eval.h"
#include "exparse.h"

int
Duro_exec_vardef(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_object *objp;
    RDB_type *typ = NULL;
    RDB_expression *initexp = NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_transaction *txp = interp->txnp != NULL ? &interp->txnp->tx : NULL;

    /* Init value without type? */
    if (nodep->nextp->kind == RDB_NODE_TOK && nodep->nextp->val.token == TOK_INIT) {
        /* No type - get INIT value */
        initexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txp);
        if (initexp == NULL)
            return RDB_ERROR;
    } else {
        typ = Duro_parse_node_to_type_retry(nodep->nextp, interp, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        if (RDB_type_is_relation(typ)) {
            RDB_raise_syntax("relation type not permitted", ecp);
            return RDB_ERROR;
        }
        if (!RDB_type_is_valid(typ)) {
            RDB_raise_invalid_argument("type is not implemented", ecp);
            return RDB_ERROR;
        }
        if (nodep->nextp->nextp->kind == RDB_NODE_INNER) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->val.children.firstp->nextp, ecp, txp);
            if (initexp == NULL)
                return RDB_ERROR;
        }
    }

    /*
     * Check if the variable already exists
     */
    if (RDB_hashmap_get(interp->current_varmapp != NULL ?
            &interp->current_varmapp->map : &interp->root_module.varmap, varname) != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return RDB_ERROR;
    }
    RDB_init_obj(objp);

    if (initexp == NULL) {
        if (Duro_init_obj(objp, typ, interp, ecp, txp) != RDB_OK) {
            goto error;
        }
    } else {
        if (Duro_evaluate_retry(initexp, interp, ecp, objp) != RDB_OK) {
            goto error;
        }
        if (RDB_obj_type(objp) != NULL) {
            /* Check type if type was given */
            if (typ != NULL &&
                    !RDB_type_equals(typ, RDB_obj_type(objp))) {
                RDB_raise_type_mismatch("", ecp);
                goto error;
            }
        } else {
            /* No type available (tuple or array) - set type */
            typ = Duro_expr_type_retry(initexp, interp, ecp);
            if (typ == NULL)
                goto error;
            typ = RDB_dup_nonscalar_type(typ, ecp);
            if (typ == NULL)
                goto error;
            RDB_obj_set_typeinfo(objp, typ);
        }
    }

    if (interp->current_varmapp != NULL) {
        /* We're in local scope */
        if (RDB_hashmap_put(&interp->current_varmapp->map, varname, objp) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
        /* Global scope */
        if (RDB_hashmap_put(&interp->root_module.varmap, varname, objp) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }
    return RDB_OK;

error:
    RDB_destroy_obj(objp, ecp);
    RDB_free(objp);
    return RDB_ERROR;
}

static int
parse_default(RDB_parse_node *defaultattrnodep,
        RDB_attr **attrvp,
        Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int attrc;
    int i;
    RDB_expression *exp;
    RDB_parse_node *nodep;

    attrc = (RDB_parse_nodelist_length(defaultattrnodep) + 1) / 2;
    if (attrc == 0)
        return 0;

    *attrvp = RDB_alloc (sizeof (RDB_attr) * attrc, ecp);
    if (*attrvp == NULL)
        return RDB_ERROR;

    for (i = 0; i < attrc; i++) {
        (*attrvp)[i].defaultp = NULL;
        (*attrvp)[i].options = 0;
    }

    nodep = defaultattrnodep->val.children.firstp;
    i = 0;
    for (;;) {
        (*attrvp)[i].name = (char *) RDB_expr_var_name(RDB_parse_node_expr(
                nodep, ecp, NULL));

        exp = RDB_parse_node_expr(nodep->nextp, ecp, NULL);
        if (exp == NULL)
            goto error;

        /* If the default is not serial(), evaluate it */
        if (RDB_expr_is_serial(exp)) {
            (*attrvp)[i].defaultp = RDB_dup_expr(exp, ecp);
        } else {
            (*attrvp)[i].defaultp = RDB_obj_to_expr(NULL, ecp);
            if ((*attrvp)[i].defaultp == NULL)
                goto error;

            if (Duro_evaluate_retry(exp, interp, ecp,
                    RDB_expr_obj((*attrvp)[i].defaultp)) != RDB_OK)
                goto error;
        }

        nodep = nodep->nextp->nextp;
        if (nodep == NULL)
            break;

        /* Skip comma */
        nodep = nodep->nextp;

        i++;
    }
    return attrc;

error:
    for (i = 0; i < attrc; i++) {
        if ((*attrvp)[i].defaultp != NULL) {
            RDB_del_expr((*attrvp)[i].defaultp, ecp);
        }
    }
    RDB_free(*attrvp);
    return RDB_ERROR;
}

static int
node_to_key(RDB_parse_node *nodep, RDB_exec_context *ecp, RDB_string_vec *vp)
{
    RDB_parse_node *np;

    vp->strc = (RDB_parse_nodelist_length(nodep) + 1) / 2;
    vp->strv = RDB_alloc(sizeof (char *) * vp->strc, ecp);
    if (vp->strv == NULL)
        return RDB_ERROR;

    np = nodep->val.children.firstp;
    if (np != NULL) {
        int i = 0;
        for(;;) {
            vp->strv[i++] = (char *) RDB_parse_node_ID(np);
            np = np->nextp;
            if (np == NULL)
                break;
            np = np->nextp;
        }
    }
    return RDB_OK;
}

static RDB_string_vec *
keylist_to_keyv(RDB_parse_node *nodep, int *keycp, RDB_exec_context *ecp)
{
    RDB_string_vec *keyv;
    RDB_parse_node *np;
    int i;

    *keycp = RDB_parse_nodelist_length(nodep) / 4;

    keyv = RDB_alloc(sizeof(RDB_string_vec) * (*keycp), ecp);
    if (keyv == NULL) {
        return NULL;
    }

    for (i = 0; i < *keycp; i++) {
        keyv[i].strv = NULL;
    }

    i = 0;
    np = nodep->val.children.firstp;
    while (i < *keycp) {
        if (node_to_key(np->nextp->nextp, ecp, &keyv[i]) != RDB_OK)
            goto error;
        np = np->nextp->nextp->nextp->nextp;
        i++;
    }

    return keyv;

error:
    for (i = 0; i < *keycp; i++) {
        RDB_free(keyv[i].strv);
    }
    RDB_free(keyv);
    return NULL;
}

int
Duro_exec_vardef_private(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_parse_node *keylistnodep;
    RDB_parse_node *defaultnodep;
    RDB_attr *default_attrv = NULL;
    int keyc;
    RDB_string_vec *keyv;
    int default_attrc = 0;
    RDB_bool freekeys = RDB_FALSE;
    RDB_object *tbp = NULL;
    RDB_expression *initexp = NULL;
    RDB_transaction *txp = interp->txnp != NULL ? &interp->txnp->tx : NULL;
    RDB_type *tbtyp = NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);

    /* Init value without type? */
    if (nodep->nextp->nextp->kind == RDB_NODE_TOK
            && nodep->nextp->nextp->val.token == TOK_INIT) {
        /* No type - get INIT value */
        initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp, ecp, txp);
        if (initexp == NULL)
            return RDB_ERROR;
        keylistnodep = nodep->nextp->nextp->nextp->nextp;
        tbtyp = Duro_expr_type_retry(initexp, interp, ecp);
        if (tbtyp == NULL) {
            return RDB_ERROR;
        }

        tbtyp = RDB_dup_nonscalar_type(tbtyp, ecp);
        if (tbtyp == NULL)
            return RDB_ERROR;
    } else {
        tbtyp = Duro_parse_node_to_type_retry(nodep->nextp->nextp, interp, ecp);
        if (tbtyp == NULL)
            return RDB_ERROR;
        if (nodep->nextp->nextp->nextp->kind == RDB_NODE_INNER
                && nodep->nextp->nextp->nextp->val.children.firstp != NULL
                && nodep->nextp->nextp->nextp->val.children.firstp->kind == RDB_NODE_TOK
                && nodep->nextp->nextp->nextp->val.children.firstp->val.token == TOK_INIT) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->val.children.firstp->nextp,
                    ecp, txp);
            if (initexp == NULL)
                return RDB_ERROR;

            keylistnodep = nodep->nextp->nextp->nextp->nextp;
        } else {
            keylistnodep = nodep->nextp->nextp->nextp;
        }
    }
    defaultnodep = keylistnodep->nextp;

    /*
     * Check if the variable already exists
     */
    if (RDB_hashmap_get(interp->current_varmapp != NULL ?
            &interp->current_varmapp->map : &interp->root_module.varmap, varname) != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }

    if (!RDB_type_is_relation(tbtyp)) {
        RDB_raise_type_mismatch("relation type required", ecp);
        goto error;
    }

    tbp = RDB_alloc(sizeof(RDB_object), ecp);
    if (tbp == NULL) {
        goto error;
    }
    RDB_init_obj(tbp);

    if (keylistnodep->kind == RDB_NODE_INNER
            && keylistnodep->val.children.firstp != NULL) {
        /* Get keys from KEY nodes */
        keyv = keylist_to_keyv(keylistnodep, &keyc, ecp);
        if (keyv == NULL)
            goto error;
        freekeys = RDB_TRUE;
    } else {
        /*
         * Key list is empty - if there is an INIT expression,
         * get the keys from it, otherwise pass
         * a keyv of NULL which means the table is all-key
         */
        if (initexp != NULL) {
            keyc = RDB_infer_keys(initexp, &Duro_get_var, interp,
                    interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL,
                    &keyv, &freekeys);
            if (keyc == RDB_ERROR) {
                keyv = NULL;
                goto error;
            }
        } else {
            keyv = NULL;
        }
    }

    if (defaultnodep->kind == RDB_NODE_INNER) {
        default_attrc = parse_default(
                defaultnodep->val.children.firstp->nextp->nextp,
                &default_attrv, interp, ecp);
        if (default_attrc == RDB_ERROR)
            goto error;
    }

    if (RDB_init_table_from_type(tbp, varname, tbtyp,
            keyc, keyv, default_attrc, default_attrv, ecp) != RDB_OK) {
        RDB_free(default_attrv);
        goto error;
    }
    RDB_free(default_attrv);

    if (initexp != NULL) {
        if (Duro_evaluate_retry(initexp, interp, ecp, tbp) != RDB_OK) {
            goto error;
        }
    }

    if (interp->current_varmapp != NULL) {
        if (RDB_hashmap_put(&interp->current_varmapp->map, varname, tbp) != RDB_OK) {
            RDB_destroy_obj(tbp, ecp);
            goto error;
        }
    } else {
        if (RDB_hashmap_put(&interp->root_module.varmap, varname, tbp) != RDB_OK) {
            RDB_destroy_obj(tbp, ecp);
            goto error;
        }
    }

    if (RDB_parse_get_interactive())
        printf("Local table %s created.\n", varname);

    if (freekeys) {
        int i;

        for (i = 0; i < keyc; i++) {
            RDB_free(keyv[i].strv);
        }
        RDB_free(keyv);
    }

    return RDB_OK;

error:
    if (tbp != NULL) {
        RDB_free_obj(tbp, ecp);
    } else if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp)) {
        RDB_del_nonscalar_type(tbtyp, ecp);
    }
    if (freekeys) {
        int i;

        for (i = 0; i < keyc; i++) {
            RDB_free(keyv[i].strv);
        }
        RDB_free(keyv);
    }
    return RDB_ERROR;
}

int
Duro_exec_vardef_real(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int keyc;
    RDB_string_vec *keyv;
    const char *varname;
    RDB_attr *default_attrv = NULL;
    RDB_bool freekeys = RDB_FALSE;
    int default_attrc = 0;
    RDB_parse_node *keylistnodep;
    RDB_parse_node *defaultnodep;
    RDB_type *tbtyp = NULL;
    RDB_object *tbp = NULL;
    RDB_expression *initexp = NULL;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    varname = RDB_expr_var_name(nodep->exp);

    /* Init value without type? */
    if (nodep->nextp->nextp->kind == RDB_NODE_TOK && nodep->nextp->nextp->val.token == TOK_INIT) {
        /* No type - get INIT value */
        initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp, ecp, &interp->txnp->tx);
        if (initexp == NULL)
            return RDB_ERROR;
        keylistnodep = nodep->nextp->nextp->nextp->nextp;
        tbtyp = RDB_expr_type(initexp, Duro_get_var_type,
                interp, interp->envp, ecp, &interp->txnp->tx);
        if (tbtyp == NULL) {
            return RDB_ERROR;
        }

        tbtyp = RDB_dup_nonscalar_type(tbtyp, ecp);
        if (tbtyp == NULL)
            return RDB_ERROR;
    } else {
        tbtyp = RDB_parse_node_to_type(nodep->nextp->nextp, &Duro_get_var_type,
                interp, ecp, &interp->txnp->tx);
        if (tbtyp == NULL)
            return RDB_ERROR;
        if (nodep->nextp->nextp->nextp->kind == RDB_NODE_INNER
                && nodep->nextp->nextp->nextp->val.children.firstp != NULL
                && nodep->nextp->nextp->nextp->val.children.firstp->kind == RDB_NODE_TOK
                && nodep->nextp->nextp->nextp->val.children.firstp->val.token == TOK_INIT) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->val.children.firstp->nextp,
                    ecp, &interp->txnp->tx);
            if (initexp == NULL)
                return RDB_ERROR;
            keylistnodep = nodep->nextp->nextp->nextp->nextp;
        } else {
            keylistnodep = nodep->nextp->nextp->nextp;
        }
    }
    defaultnodep = keylistnodep->nextp;

    if (keylistnodep->kind == RDB_NODE_INNER
            && keylistnodep->val.children.firstp != NULL) {
        /* Get keys from KEY node(s) */
        keyv = keylist_to_keyv(keylistnodep, &keyc, ecp);
        if (keyv == NULL)
            goto error;
        freekeys = RDB_TRUE;
    } else {
        /*
         * Key list is empty - if there is an INIT expression,
         * get the keys from it, otherwise pass
         * a keyv of NULL which means the table is all-key
         */
        if (initexp != NULL) {
            keyc = RDB_infer_keys(initexp, &Duro_get_var, interp,
                    interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL, &keyv, &freekeys);
            if (keyc == RDB_ERROR) {
                keyv = NULL;
                goto error;
            }
        } else {
            keyv = NULL;
        }
    }

    if (!RDB_type_is_relation(tbtyp)) {
        RDB_raise_type_mismatch("relation type required for table", ecp);
        goto error;
    }

    if (defaultnodep->kind == RDB_NODE_INNER) {
        default_attrc = parse_default(
                defaultnodep->val.children.firstp->nextp->nextp,
                &default_attrv, interp, ecp);
        if (default_attrc == RDB_ERROR)
            goto error;
    }

    tbp = RDB_create_table_from_type(varname, tbtyp, keyc, keyv,
            default_attrc, default_attrv, ecp, &interp->txnp->tx);
    RDB_free(default_attrv);
    if (tbp == NULL) {
        goto error;
    }

    if (initexp != NULL) {
        if (RDB_evaluate(initexp, &Duro_get_var, interp, interp->envp,
                ecp, &interp->txnp->tx, tbp) != RDB_OK) {
            goto error;
        }
    }

    if (RDB_parse_get_interactive())
        printf("Table %s created.\n", varname);

    if (freekeys) {
        int i;

        for (i = 0; i < keyc; i++) {
            RDB_free(keyv[i].strv);
        }
        RDB_free(keyv);
    }

    return RDB_OK;

error:
    {
        RDB_exec_context ec;

        RDB_init_exec_context(&ec);
        if (tbp != NULL) {
            RDB_drop_table(tbp, &ec, &interp->txnp->tx);
        } else if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp)) {
            RDB_del_nonscalar_type(tbtyp, &ec);
        }

        if (keyv != NULL && freekeys) {
            int i;

            for (i = 0; i < keyc; i++) {
                if (keyv[i].strv != NULL)
                    RDB_free(keyv[i].strv);
            }
            RDB_free(keyv);
        }

        RDB_destroy_exec_context(&ec);
    }
    return RDB_ERROR;
}

int
Duro_exec_vardef_public(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_parse_node *keylistnodep;
    int keyc;
    RDB_string_vec *keyv;
    RDB_bool freekeys = RDB_FALSE;
    RDB_type *tbtyp = NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbtyp = Duro_parse_node_to_type_retry(nodep->nextp->nextp, interp, ecp);
    if (tbtyp == NULL)
        return RDB_ERROR;
    keylistnodep = nodep->nextp->nextp->nextp;

    /*
     * Check if the variable already exists
     */
    if (RDB_hashmap_get(interp->current_varmapp != NULL ?
            &interp->current_varmapp->map : &interp->root_module.varmap, varname) != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }

    if (!RDB_type_is_relation(tbtyp)) {
        RDB_raise_type_mismatch("relation type required", ecp);
        goto error;
    }

    if (keylistnodep->kind == RDB_NODE_INNER
            && keylistnodep->val.children.firstp != NULL) {
        /* Get keys from KEY nodes */
        keyv = keylist_to_keyv(keylistnodep, &keyc, ecp);
        if (keyv == NULL)
            goto error;
        freekeys = RDB_TRUE;
    } else {
        /*
         * Key list is empty - pass a keyv of NULL which means the table is all-key
         */
        keyv = NULL;
    }

    if (RDB_create_public_table_from_type(varname, tbtyp, keyc, keyv, ecp, &interp->txnp->tx) != RDB_OK) {
        goto error;
    }

    if (RDB_parse_get_interactive())
        printf("Public table %s created.\n", varname);

    if (freekeys) {
        int i;

        for (i = 0; i < keyc; i++) {
            RDB_free(keyv[i].strv);
        }
        RDB_free(keyv);
    }

    return RDB_OK;

error:
    if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp)) {
        RDB_del_nonscalar_type(tbtyp, ecp);
    }
    if (freekeys) {
        int i;

        for (i = 0; i < keyc; i++) {
            RDB_free(keyv[i].strv);
        }
        RDB_free(keyv);
    }
    return RDB_ERROR;
}
