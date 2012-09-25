/*
 * interp_core.c
 *
 *  Created on: 24.08.2012
 *      Author: Rene Hartmann
 */

#include "interp_core.h"
#include <gen/hashmapit.h>
#include "exparse.h"
#include <rel/internal.h>

#include <stddef.h>
#include <string.h>

RDB_environment *envp = NULL;

tx_node *txnp = NULL;

sig_atomic_t interrupted;

/* System Duro_module, contains STDIN, STDOUT etc. */
Duro_module sys_module;

/* Top-level Duro_module */
static Duro_module root_module;

/*
 * Points to the local variables in the current scope.
 * Linked list from inner to outer scope.
 */
static varmap_node *current_varmapp;

int
Duro_add_varmap(RDB_exec_context *ecp)
{
    varmap_node *nodep = RDB_alloc(sizeof(varmap_node), ecp);
    if (nodep == NULL) {
        return RDB_ERROR;
    }
    RDB_init_hashmap(&nodep->map, DEFAULT_VARMAP_SIZE);
    nodep->parentp = current_varmapp;
    current_varmapp = nodep;
    return RDB_OK;
}

static int
drop_local_var(RDB_object *objp, RDB_exec_context *ecp)
{
    RDB_type *typ = RDB_obj_type(objp);

    /* Array and tuple types must be destroyed */
    if (!RDB_type_is_scalar(typ) && !RDB_type_is_relation(typ)) {
        if (RDB_del_nonscalar_type(typ, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    if (RDB_destroy_obj(objp, ecp) != RDB_OK)
        return RDB_ERROR;

    RDB_free(objp);
    return RDB_OK;
}

void
Duro_destroy_varmap(RDB_hashmap *map)
{
    RDB_hashmap_iter it;
    char *name;
    RDB_object *objp;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    RDB_init_hashmap_iter(&it, map);
    for(;;) {
        objp = RDB_hashmap_next(&it, &name);
        if (name == NULL)
            break;
        if (objp != NULL) {
            drop_local_var(objp, &ec);
        }
    }

    RDB_destroy_hashmap(map);
    RDB_destroy_exec_context(&ec);
}

void
Duro_remove_varmap(void)
{
    varmap_node *parentp = current_varmapp->parentp;
    Duro_destroy_varmap(&current_varmapp->map);
    RDB_free(current_varmapp);
    current_varmapp = parentp;
}

void
Duro_init_vars(void)
{
    RDB_init_hashmap(&root_module.varmap, DEFAULT_VARMAP_SIZE);
    RDB_init_hashmap(&sys_module.varmap, DEFAULT_VARMAP_SIZE);
    current_varmapp = NULL;
}

void
Duro_destroy_vars(void)
{
    Duro_destroy_varmap(&root_module.varmap);

    /* Destroy only the varmap, not the variables as they are global */
    RDB_destroy_hashmap(&sys_module.varmap);
}

/*
 * Set current_varmapp and return the old value
 */
varmap_node *
Duro_set_current_varmap(varmap_node *newvarmapp)
{
    varmap_node *ovarmapp = current_varmapp;
    current_varmapp = newvarmapp;
    return ovarmapp;
}


static RDB_object *
lookup_transient_var(const char *name, varmap_node *varmapp)
{
    RDB_object *objp;
    varmap_node *nodep = varmapp;

    /* Search in local vars */
    while (nodep != NULL) {
        objp = RDB_hashmap_get(&nodep->map, name);
        if (objp != NULL)
            return objp;
        nodep = nodep->parentp;
    }

    /* Search in global transient vars */
    objp = RDB_hashmap_get(&root_module.varmap, name);
    if (objp != NULL)
        return objp;

    /* Search in system Duro_module */
    return RDB_hashmap_get(&sys_module.varmap, name);
}

RDB_object *
Duro_lookup_transient_var(const char *name)
{
    return lookup_transient_var(name, current_varmapp);
}

RDB_object *
Duro_lookup_var(const char *name, RDB_exec_context *ecp)
{
    RDB_object *objp = Duro_lookup_transient_var(name);
    if (objp != NULL)
        return objp;

    if (txnp != NULL) {
        /* Try to get table from DB */
        objp = RDB_get_table(name, ecp, &txnp->tx);
    }
    if (objp == NULL)
        RDB_raise_name(name, ecp);
    return objp;
}

RDB_type *
Duro_get_var_type(const char *name, void *arg)
{
    RDB_object *objp = Duro_lookup_transient_var(name);
    return objp != NULL ? RDB_obj_type(objp) : NULL;
}

static RDB_object *
get_var(const char *name, void *maparg)
{
    return lookup_transient_var(name, (varmap_node *) maparg);
}

/*
 * Evaluate expression.
 * If evaluation fails with OPERATOR_NOT_FOUND_ERROR ad no transaction is running
 * but a environment is available, start a transaction and try again.
 */
int
Duro_evaluate_retry(RDB_expression *exp, RDB_exec_context *ecp, RDB_object *resultp)
{
    RDB_transaction tx;
    RDB_database *dbp;
    int ret;

    ret = RDB_evaluate(exp, &get_var, current_varmapp, envp, ecp,
            txnp != NULL ? &txnp->tx : NULL, resultp);
    /*
     * Success or error different from OPERATOR_NOT_FOUND_ERROR
     * -> return
     */
    if (ret == RDB_OK
            || RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR)
        return ret;
    /*
     * If a transaction is already active or no environment is
     * available, give up
     */
    if (txnp != NULL || envp == NULL)
        return ret;
    /*
     * Start transaction and retry.
     * If this succeeds, the operator will be in memory next time
     * so no transaction will be needed.
     */
    dbp = Duro_get_db(ecp);
    if (dbp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
        return RDB_ERROR;
    ret = RDB_evaluate(exp, &get_var, current_varmapp, envp, ecp, &tx, resultp);
    if (ret != RDB_OK) {
        RDB_commit(ecp, &tx);
        return ret;
    }
    return RDB_commit(ecp, &tx);
}

int
Duro_exec_vardef(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_object *objp;
    RDB_type *typ = NULL;
    RDB_expression *initexp = NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_transaction *txp = txnp != NULL ? &txnp->tx : NULL;

    /* Init value without type? */
    if (nodep->nextp->kind == RDB_NODE_TOK && nodep->nextp->val.token == TOK_INIT) {
        /* No type - get INIT value */
        initexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txp);
        if (initexp == NULL)
            return RDB_ERROR;
    } else {
        typ = Duro_parse_node_to_type_retry(nodep->nextp, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        if (RDB_type_is_relation(typ)) {
            RDB_raise_syntax("relation type not permitted", ecp);
            return RDB_ERROR;
        }
        if (nodep->nextp->nextp->kind == RDB_NODE_TOK
                && nodep->nextp->nextp->val.token == TOK_INIT) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp, ecp, txp);
            if (initexp == NULL)
                return RDB_ERROR;
        }
    }

    /*
     * Check if the variable already exists
     */
    if (RDB_hashmap_get(current_varmapp != NULL ?
            &current_varmapp->map : &root_module.varmap, varname) != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return RDB_ERROR;
    }
    RDB_init_obj(objp);

    if (initexp == NULL) {
        if (Duro_init_obj(objp, typ, ecp, txp) != RDB_OK) {
            goto error;
        }
    } else {
        if (Duro_evaluate_retry(initexp, ecp, objp) != RDB_OK) {
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
            typ = RDB_expr_type(initexp, Duro_get_var_type,
                    NULL, ecp, txnp != NULL ? &txnp->tx : NULL);
            if (typ == NULL)
                goto error;
            typ = RDB_dup_nonscalar_type(typ, ecp);
            if (typ == NULL)
                goto error;
            RDB_obj_set_typeinfo(objp, typ);
        }
    }

    if (current_varmapp != NULL) {
        /* We're in local scope */
        if (RDB_hashmap_put(&current_varmapp->map, varname, objp) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
        /* Global scope */
        if (RDB_hashmap_put(&root_module.varmap, varname, objp) != RDB_OK) {
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

static int
parse_default(RDB_parse_node *defaultattrnodep,
        RDB_attr **attrvp,
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

        (*attrvp)[i].defaultp = RDB_alloc(sizeof (RDB_object), ecp);
        if ((*attrvp)[i].defaultp == NULL)
            goto error;
        RDB_init_obj((*attrvp)[i].defaultp);
        exp = RDB_parse_node_expr(nodep->nextp, ecp, NULL);
        if (exp == NULL)
            goto error;
        if (Duro_evaluate_retry(exp, ecp, (*attrvp)[i].defaultp) != RDB_OK)
            goto error;

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
            RDB_destroy_obj((*attrvp)[i].defaultp, ecp);
        }
    }
    RDB_free(*attrvp);
    return RDB_ERROR;
}

int
Duro_exec_vardef_private(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_parse_node *keylistnodep;
    RDB_parse_node *defaultnodep;
    RDB_attr *default_attrv;
    int default_attrc = 0;
    RDB_bool freekeys;
    int keyc;
    RDB_string_vec *keyv = NULL;
    RDB_object *tbp = NULL;
    RDB_expression *initexp = NULL;
    RDB_transaction *txp = txnp != NULL ? &txnp->tx : NULL;
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
        tbtyp = RDB_expr_type(initexp, Duro_get_var_type,
                NULL, ecp, txnp != NULL ? &txnp->tx : NULL);
        if (tbtyp == NULL) {
            return RDB_ERROR;
        }

        tbtyp = RDB_dup_nonscalar_type(tbtyp, ecp);
        if (tbtyp == NULL)
            return RDB_ERROR;
    } else {
        tbtyp = Duro_parse_node_to_type_retry(nodep->nextp->nextp, ecp);
        if (tbtyp == NULL)
            return RDB_ERROR;
        if (nodep->nextp->nextp->nextp->kind == RDB_NODE_TOK
                && nodep->nextp->nextp->nextp->val.token == TOK_INIT) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp, txp);
            if (initexp == NULL)
                return RDB_ERROR;

            keylistnodep = nodep->nextp->nextp->nextp->nextp->nextp;
        } else {
            keylistnodep = nodep->nextp->nextp->nextp;
        }
    }

    /*
     * Check if the variable already exists
     */
    if (RDB_hashmap_get(current_varmapp != NULL ?
            &current_varmapp->map : &root_module.varmap, varname) != NULL) {
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
            && keylistnodep->val.children.firstp->kind == RDB_NODE_TOK
            && keylistnodep->val.children.firstp->val.token == TOK_KEY) {
        keyv = keylist_to_keyv(keylistnodep, &keyc, ecp);
        if (keyv == NULL)
            goto error;
        freekeys = RDB_TRUE;

        /*
         * Make defaultnodep point to the node after the keylist
         */
        defaultnodep = keylistnodep->nextp;
    } else {
        /* Get keys from the INIT expression */
        keyc = RDB_infer_keys(initexp, &get_var, current_varmapp,
                ecp, txnp != NULL ? &txnp->tx : NULL, &keyv, &freekeys);
        if (keyc == RDB_ERROR) {
            keyv = NULL;
            goto error;
        }

        /*
         * No KEY node, so keylistnodep points either to the DEFAULT node
         * or to the trailing semicolon
         */
        defaultnodep = keylistnodep;
    }

    if (defaultnodep->kind == RDB_NODE_INNER) {
        default_attrc = parse_default(
                defaultnodep->val.children.firstp->nextp->nextp,
                &default_attrv, ecp);
        if (default_attrc == RDB_ERROR)
            goto error;
    }

    if (RDB_init_table_from_type(tbp, varname, tbtyp,
            keyc, keyv, default_attrc, default_attrv, ecp) != RDB_OK) {
        goto error;
    }

    if (initexp != NULL) {
        if (Duro_evaluate_retry(initexp, ecp, tbp) != RDB_OK) {
            goto error;
        }
    }

    if (current_varmapp != NULL) {
        if (RDB_hashmap_put(&current_varmapp->map, varname, tbp) != RDB_OK) {
            RDB_destroy_obj(tbp, ecp);
            goto error;
        }
    } else {
        if (RDB_hashmap_put(&root_module.varmap, varname, tbp) != RDB_OK) {
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
    /* !! Destroy keyv */
    return RDB_ERROR;
}

int
Duro_exec_vardef_real(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int keyc;
    RDB_string_vec *keyv;
    const char *varname;
    RDB_bool freekeys;
    RDB_attr *default_attrv;
    int default_attrc = 0;
    RDB_parse_node *keylistnodep;
    RDB_parse_node *defaultnodep;
    RDB_type *tbtyp = NULL;
    RDB_object *tbp = NULL;
    RDB_expression *initexp = NULL;

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    varname = RDB_expr_var_name(nodep->exp);

    /* Init value without type? */
    if (nodep->nextp->nextp->kind == RDB_NODE_TOK && nodep->nextp->nextp->val.token == TOK_INIT) {
        /* No type - get INIT value */
        initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp, ecp, &txnp->tx);
        if (initexp == NULL)
            return RDB_ERROR;
        keylistnodep = nodep->nextp->nextp->nextp->nextp;
        tbtyp = RDB_expr_type(initexp, Duro_get_var_type,
                NULL, ecp, &txnp->tx);
        if (tbtyp == NULL) {
            return RDB_ERROR;
        }

        tbtyp = RDB_dup_nonscalar_type(tbtyp, ecp);
        if (tbtyp == NULL)
            return RDB_ERROR;
    } else {
        tbtyp = RDB_parse_node_to_type(nodep->nextp->nextp, &Duro_get_var_type,
                NULL, ecp, &txnp->tx);
        if (tbtyp == NULL)
            return RDB_ERROR;
        if (nodep->nextp->nextp->nextp->kind == RDB_NODE_TOK
                && nodep->nextp->nextp->nextp->val.token == TOK_INIT) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp, &txnp->tx);
            if (initexp == NULL)
                return RDB_ERROR;
            keylistnodep = nodep->nextp->nextp->nextp->nextp->nextp;
        } else {
            keylistnodep = nodep->nextp->nextp->nextp;
        }
    }

    if (keylistnodep->kind == RDB_NODE_INNER
            && keylistnodep->val.children.firstp->kind == RDB_NODE_TOK
            && keylistnodep->val.children.firstp->val.token == TOK_KEY) {
        keyv = keylist_to_keyv(keylistnodep, &keyc, ecp);
        if (keyv == NULL)
            goto error;
        freekeys = RDB_TRUE;

        /*
         * Make defaultnodep point to the node after the last KEY node
         */
        defaultnodep = keylistnodep->nextp;
        while (defaultnodep->kind == RDB_NODE_INNER
                && defaultnodep->val.children.firstp->kind == RDB_NODE_TOK
                && defaultnodep->val.children.firstp->val.token == TOK_KEY)
            defaultnodep = defaultnodep->nextp->nextp->nextp->nextp;
    } else {
        /* Get keys from the INIT expression */
        keyc = RDB_infer_keys(initexp, &get_var, current_varmapp,
                ecp, &txnp->tx, &keyv, &freekeys);
        if (keyc == RDB_ERROR) {
            keyv = NULL;
            goto error;
        }

        /*
         * No KEY node, keylistnodep points either to the DEFAULT node
         * or to the trailing semicolon
         */
        defaultnodep = keylistnodep;
    }

    if (!RDB_type_is_relation(tbtyp)) {
        RDB_raise_type_mismatch("relation type required for table", ecp);
        goto error;
    }

    if (defaultnodep->kind == RDB_NODE_INNER) {
        default_attrc = parse_default(
                defaultnodep->val.children.firstp->nextp->nextp,
                &default_attrv, ecp);
        if (default_attrc == RDB_ERROR)
            goto error;
    }

    tbp = RDB_create_table_from_type(varname, tbtyp, keyc, keyv,
            default_attrc, default_attrv, ecp, &txnp->tx);
    if (tbp == NULL) {
        goto error;
    }

    if (initexp != NULL) {
        if (RDB_evaluate(initexp, &get_var, current_varmapp, envp,
                ecp, &txnp->tx, tbp) != RDB_OK) {
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
            RDB_drop_table(tbp, &ec, &txnp->tx);
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
Duro_exec_vardef_virtual(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_expression *defexp;
    RDB_expression *texp;
    const char *varname = RDB_expr_var_name(nodep->exp);

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    defexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, &txnp->tx);
    if (defexp == NULL)
        return RDB_ERROR;

    texp = RDB_dup_expr(defexp, ecp);
    if (texp == NULL)
        return RDB_ERROR;

    tbp = RDB_expr_to_vtable(texp, ecp, &txnp->tx);
    if (tbp == NULL) {
        RDB_del_expr(texp, ecp);
        return RDB_ERROR;
    }
    if (RDB_set_table_name(tbp, varname, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;
    if (RDB_add_table(tbp, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Table %s created.\n", varname);
    return RDB_OK;
}

int
Duro_exec_vardrop(const RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *objp = NULL;

    /* Try to look up local variable */
    if (current_varmapp != NULL)
        objp = RDB_hashmap_get(&current_varmapp->map, varname);
    if (objp == NULL)
        objp = RDB_hashmap_get(&root_module.varmap, varname);
    if (objp != NULL) {
        /* If transient variable found */
        if (current_varmapp != NULL) {
            if (RDB_hashmap_put(&current_varmapp->map, varname, NULL) != RDB_OK)
                return RDB_ERROR;
        } else {
            if (RDB_hashmap_put(&root_module.varmap, varname, NULL) != RDB_OK)
                return RDB_ERROR;
        }
        return drop_local_var(objp, ecp);
    }

    /*
     * Delete persistent table
     */

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    objp = RDB_get_table(varname, ecp, &txnp->tx);
    if (objp == NULL)
        return RDB_ERROR;

    if (RDB_drop_table(objp, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Table %s dropped.\n", varname);
    return RDB_OK;
}

int
Duro_put_var(const char *name, RDB_object *objp, RDB_exec_context *ecp)
{
    if (RDB_hashmap_put(&current_varmapp->map, name, objp)
            != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static const char *
var_of_type(RDB_hashmap *mapp, RDB_type *typ)
{
    RDB_hashmap_iter it;
    char *namp;
    RDB_object *objp;

    RDB_init_hashmap_iter(&it, mapp);
    for(;;) {
        objp = RDB_hashmap_next(&it, &namp);
        if (namp == NULL)
            break;
        if (objp != NULL) {
            RDB_type *vtyp = RDB_obj_type(objp);

            /* If the types are equal, return variable name */
            if (vtyp != NULL && RDB_type_equals(vtyp, typ))
                return namp;
        }
    }
    return NULL;
}

/*
 * Checks if *typ is in use by a transient variable.
 * If it is, returns the name of the variable, otherwise NULL.
 */
const char *
Duro_type_in_use(RDB_type *typ)
{
    const char *typenamp;
    if (current_varmapp != NULL) {
        varmap_node *varnodep = current_varmapp;
        do {
            typenamp = var_of_type(&varnodep->map, typ);
            if (typenamp != NULL)
                return typenamp;
            varnodep = varnodep->parentp;
        } while (varnodep != NULL);
    }
    return var_of_type(&root_module.varmap, typ);
}

RDB_database *
Duro_get_db(RDB_exec_context *ecp)
{
    char *dbname;
    RDB_object *dbnameobjp = RDB_hashmap_get(&sys_module.varmap, "current_db");
    if (dbnameobjp == NULL) {
        RDB_raise_resource_not_found("no database", ecp);
        return NULL;
    }
    dbname = RDB_obj_string(dbnameobjp);
    if (*dbname == '\0') {
        RDB_raise_not_found("no database", ecp);
        return NULL;
    }
    if (envp == NULL) {
        RDB_raise_resource_not_found("no connection", ecp);
        return NULL;
    }

    return RDB_get_db_from_env(dbname, envp, ecp);
}

/*
 * Convert parse node into type. If the type cannot be found and no
 * transaction was active, start a transaction and try again.
 */
RDB_type *
Duro_parse_node_to_type_retry(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_database *dbp;
    RDB_transaction tx;
    RDB_type *typ = RDB_parse_node_to_type(nodep, &Duro_get_var_type,
            NULL, ecp, txnp != NULL ? &txnp->tx : NULL);
    /*
     * Success or error different from NAME_ERROR
     * -> return
     */
    if (typ != NULL)
        return typ;
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NAME_ERROR)
        return NULL;
    /*
     * If a transaction is already active or no environment is
     * available, give up
     */
    if (txnp != NULL || envp == NULL)
        return NULL;
    /*
     * Start transaction and retry.
     */
    dbp = Duro_get_db(ecp);
    if (dbp == NULL) {
        return NULL;
    }

    if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
        return NULL;
    typ = RDB_parse_node_to_type(nodep, &Duro_get_var_type,
                NULL, ecp, &tx);
    if (typ == NULL) {
        RDB_commit(ecp, &tx);
        return NULL;
    }
    if (RDB_commit(ecp, &tx) != RDB_OK) {
        if (!RDB_type_is_scalar(typ)) {
            RDB_del_nonscalar_type(typ, ecp);
            return NULL;
        }
    }
    return typ;
}

static int
init_obj_by_selector(RDB_object *objp, RDB_possrep *rep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object *objv;
    RDB_object **objpv;

    objv = RDB_alloc(sizeof(RDB_object) * rep->compc, ecp);
    if (objv == NULL) {
        return RDB_ERROR;
    }
    for (i = 0; i < rep->compc; i++)
        RDB_init_obj(&objv[i]);
    objpv = RDB_alloc(sizeof(RDB_object *) * rep->compc, ecp);
    if (objpv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Get selector arguments */
    for (i = 0; i < rep->compc; i++) {
        ret = Duro_init_obj(&objv[i], rep->compv[i].typ, ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;
        objpv[i] = &objv[i];
    }

    /* Call selector */
    ret = RDB_call_ro_op_by_name_e(rep->name, rep->compc, objpv, envp, ecp,
            txp, objp);

cleanup:
    for (i = 0; i < rep->compc; i++)
        RDB_destroy_obj(&objv[i], ecp);
    RDB_free(objv);
    RDB_free(objpv);

    return ret;
}

int
Duro_init_obj(RDB_object *objp, RDB_type *typ, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;

    if (typ == &RDB_BOOLEAN) {
        RDB_bool_to_obj(objp, RDB_FALSE);
    } else if (typ == &RDB_INTEGER) {
        RDB_int_to_obj(objp, 0);
    } else if (typ == &RDB_FLOAT) {
        RDB_float_to_obj(objp, 0.0);
    } else if (typ == &RDB_STRING) {
        return RDB_string_to_obj(objp, "", ecp);
    } else if (typ == &RDB_BINARY) {
        return RDB_binary_set(objp, 0, NULL, (size_t) 0, ecp);
    } else if (RDB_type_is_tuple(typ)) {
        for (i = 0; i < typ->def.tuple.attrc; i++) {
            if (RDB_tuple_set(objp, typ->def.tuple.attrv[i].name,
                    NULL, ecp) != RDB_OK)
                return RDB_ERROR;
            if (Duro_init_obj(RDB_tuple_get(objp, typ->def.tuple.attrv[i].name),
                    typ->def.tuple.attrv[i].typ, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        }
        typ = RDB_dup_nonscalar_type(typ, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        RDB_obj_set_typeinfo(objp, typ);
    } else if (RDB_type_is_array(typ)) {
        typ = RDB_dup_nonscalar_type(typ, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        RDB_obj_set_typeinfo(objp, typ);
    } else if (RDB_type_is_relation(typ)) {
        if (RDB_init_table_from_type(objp, NULL, typ, 0, NULL,
                0, NULL, ecp) != RDB_OK)
            return RDB_ERROR;
    } else {
        /* Invoke selector */
        if (typ->def.scalar.repc > 0) {
            return init_obj_by_selector(objp, &typ->def.scalar.repv[0],
                    ecp, txp);
        }
    }
    return RDB_OK;
}
