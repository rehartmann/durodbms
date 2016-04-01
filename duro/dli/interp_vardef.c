/*
 * Interpreter functions for variable definition.
 *
 * Copyright (C) 2014-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "interp_core.h"
#include <gen/strfns.h>
#include <obj/key.h>
#include "exparse.h"

int
Duro_exec_vardef(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    Duro_var_entry *entryp;
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
            RDB_del_nonscalar_type(typ, ecp);
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
    entryp = Duro_varmap_get(interp->current_varmapp != NULL ?
            &interp->current_varmapp->map : &interp->root_varmap, varname);
    if (entryp != NULL && entryp->varp != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return RDB_ERROR;
    }
    RDB_init_obj(objp);

    if (initexp == NULL) {
        if (RDB_type_is_union(typ)) {
            RDB_raise_invalid_argument("INIT value required for union type", ecp);
            goto error;
        }
        if (RDB_set_init_value(objp, typ, interp->envp, ecp) != RDB_OK) {
            goto error;
        }
    } else {
        if (RDB_evaluate(initexp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL,
                objp) != RDB_OK) {
            goto error;
        }
        if (RDB_obj_type(objp) != NULL) {
            /* Check type if type was given */
            if (typ != NULL) {
                RDB_type *objtyp = RDB_obj_type(objp);
                if (!RDB_is_subtype(objtyp, typ)) {
                    if (!RDB_type_is_scalar(typ)) {
                        RDB_del_nonscalar_type(typ, ecp);
                        typ = NULL;
                    }
                    RDB_raise_type_mismatch("", ecp);
                    goto error;
                }
                if (RDB_type_is_scalar(typ)) {
                    RDB_obj_set_typeinfo(objp, typ);
                } else {
                    RDB_del_nonscalar_type(typ, ecp);
                }
            }
        } else {
            /* No type available (tuple or array) - set type */
            if (typ == NULL) {
                typ = Duro_expr_type(initexp, interp, ecp);
                if (typ == NULL)
                    goto error;
                typ = RDB_dup_nonscalar_type(typ, ecp);
                if (typ == NULL)
                    goto error;
            }
            RDB_obj_set_typeinfo(objp, typ);
        }
    }

    if (interp->current_varmapp != NULL) {
        /* We're in local scope */
        if (Duro_varmap_put(&interp->current_varmapp->map, varname, objp,
                DURO_VAR_FREE, ecp) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
        /* Global scope */
        if (Duro_varmap_put(&interp->root_varmap, varname, objp,
                DURO_VAR_FREE, ecp) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }
    return RDB_OK;

error:
    RDB_destroy_obj(objp, ecp);
    RDB_free(objp);
    if (typ != NULL && !RDB_type_is_scalar(typ)) {
        RDB_del_nonscalar_type(typ, ecp);
    }
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

            if (RDB_evaluate(exp, &Duro_get_var, interp, interp->envp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL,
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
    int i;

    vp->strc = (RDB_parse_nodelist_length(nodep) + 1) / 2;
    vp->strv = RDB_alloc(sizeof (char *) * vp->strc, ecp);
    if (vp->strv == NULL)
        return RDB_ERROR;
    for (i = 0; i < vp->strc; i++)
        vp->strv[i] = NULL;

    np = nodep->val.children.firstp;
    if (np != NULL) {
        i = 0;
        for(;;) {
            vp->strv[i] = RDB_dup_str((char *) RDB_parse_node_ID(np));
            if (vp->strv[i] == NULL)
                return RDB_ERROR;
            i++;
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
    if (keyv == NULL)
        RDB_free_keys(*keycp, keyv);
    return NULL;
}

int
Duro_exec_vardef_private(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    Duro_var_entry *entryp;
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
        tbtyp = Duro_expr_type(initexp, interp, ecp);
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
    entryp = Duro_varmap_get(interp->current_varmapp != NULL ?
            &interp->current_varmapp->map : &interp->root_varmap, varname);
    if (entryp != NULL && entryp->varp != NULL) {
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
        if (RDB_evaluate(initexp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL,
                tbp) != RDB_OK) {
            goto error;
        }
    }

    if (interp->current_varmapp != NULL) {
        if (Duro_varmap_put(&interp->current_varmapp->map, varname, tbp,
                DURO_VAR_FREE, ecp) != RDB_OK) {
            RDB_destroy_obj(tbp, ecp);
            goto error;
        }
    } else {
        if (Duro_varmap_put(&interp->root_varmap, varname, tbp, DURO_VAR_FREE,
                ecp) != RDB_OK) {
            RDB_destroy_obj(tbp, ecp);
            goto error;
        }
    }

    if (RDB_parse_get_interactive())
        printf("Local table %s created.\n", varname);

    if (freekeys && keyv != NULL) {
        RDB_free_keys(keyc, keyv);
    }

    return RDB_OK;

error:
    if (tbp != NULL) {
        RDB_free_obj(tbp, ecp);
    } else if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp)) {
        RDB_del_nonscalar_type(tbtyp, ecp);
    }
    if (freekeys && keyv != NULL) {
        RDB_free_keys(keyc, keyv);
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
        if (RDB_type_is_generic(tbtyp)) {
            RDB_raise_syntax("generic type not permitted", ecp);
            if (!RDB_type_is_scalar(tbtyp)) {
                RDB_del_nonscalar_type(tbtyp, ecp);
            }
            return RDB_ERROR;
        }
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
        RDB_free_keys(keyc, keyv);
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
            RDB_free_keys(keyc, keyv);
        }

        RDB_destroy_exec_context(&ec);
    }
    return RDB_ERROR;
}

int
Duro_exec_vardef_public(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    Duro_var_entry *entryp;
    RDB_parse_node *keylistnodep;
    int keyc;
    RDB_string_vec *keyv;
    RDB_bool freekeys = RDB_FALSE;
    RDB_type *tbtyp = NULL;
    RDB_object idobj;
    const char *varname;

    RDB_init_obj(&idobj);

    if (*RDB_obj_string(&interp->pkg_name) != '\0') {
        if (Duro_package_q_id(&idobj, RDB_expr_var_name(nodep->exp), interp, ecp)
                != RDB_OK) {
            goto error;
        }
        varname = RDB_obj_string(&idobj);
    } else {
        varname = RDB_expr_var_name(nodep->exp);
    }

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        goto error;
    }

    tbtyp = Duro_parse_node_to_type_retry(nodep->nextp->nextp, interp, ecp);
    if (tbtyp == NULL)
        goto error;
    keylistnodep = nodep->nextp->nextp->nextp;

    /*
     * Check if the variable already exists
     */
    entryp = Duro_varmap_get(interp->current_varmapp != NULL ?
            &interp->current_varmapp->map : &interp->root_varmap, varname);
    if (entryp != NULL && entryp->varp != NULL) {
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
        RDB_free_keys(keyc, keyv);
    }

    RDB_destroy_obj(&idobj, ecp);
    return RDB_OK;

error:
    if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp)) {
        RDB_del_nonscalar_type(tbtyp, ecp);
    }
    if (freekeys) {
        RDB_free_keys(keyc, keyv);
    }
    RDB_destroy_obj(&idobj, ecp);
    return RDB_ERROR;
}

int
Duro_exec_constdef(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    Duro_var_entry *entryp;
    RDB_object *objp;
    RDB_expression *initexp = NULL;
    const char *constname = RDB_expr_var_name(nodep->exp);
    RDB_transaction *txp = interp->txnp != NULL ? &interp->txnp->tx : NULL;

    /* Get value */
    initexp = RDB_parse_node_expr(nodep->nextp, ecp, txp);
    if (initexp == NULL)
        return RDB_ERROR;

    /*
     * Check if the variable already exists
     */
    entryp = Duro_varmap_get(interp->current_varmapp != NULL ?
            &interp->current_varmapp->map : &interp->root_varmap, constname);
    if (entryp != NULL && entryp->varp != NULL) {
        RDB_raise_element_exists(constname, ecp);
        return RDB_ERROR;
    }

    objp = RDB_alloc(sizeof(RDB_object), ecp);
    if (objp == NULL) {
        return RDB_ERROR;
    }
    RDB_init_obj(objp);

    if (RDB_evaluate(initexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL,
            objp) != RDB_OK) {
        goto error;
    }
    if (RDB_obj_type(objp) == NULL) {
        /* No type available (tuple or array) - set type */
        RDB_type *typ = Duro_expr_type(initexp, interp, ecp);
        if (typ == NULL)
            goto error;
        typ = RDB_dup_nonscalar_type(typ, ecp);
        if (typ == NULL)
            goto error;
        RDB_obj_set_typeinfo(objp, typ);
    }

    if (interp->current_varmapp != NULL) {
        /* We're in local scope */
        if (Duro_varmap_put(&interp->current_varmapp->map, constname, objp,
                DURO_VAR_CONST | DURO_VAR_FREE, ecp) != RDB_OK) {
            goto error;
        }
    } else {
        /* Global scope */
        if (Duro_varmap_put(&interp->root_varmap, constname, objp,
                DURO_VAR_CONST | DURO_VAR_FREE, ecp) != RDB_OK) {
            goto error;
        }
    }
    return RDB_OK;

error:
    RDB_destroy_obj(objp, ecp);
    RDB_free(objp);
    return RDB_ERROR;
}
