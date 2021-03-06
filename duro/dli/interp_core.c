/*
 * Interpreter core functionality such as handling of variables
 *
 * Copyright (C) 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "interp_core.h"
#include "exparse.h"
#include <gen/hashmapit.h>

#include <stddef.h>
#include <string.h>

int
Duro_add_varmap(Duro_interp *interp, RDB_exec_context *ecp)
{
    varmap_node *nodep = RDB_alloc(sizeof(varmap_node), ecp);
    if (nodep == NULL) {
        return RDB_ERROR;
    }
    Duro_init_varmap(&nodep->map, DEFAULT_VARMAP_SIZE);
    nodep->parentp = interp->current_varmapp;
    interp->current_varmapp = nodep;
    return RDB_OK;
}

static int
del_local_var(RDB_object *objp, RDB_exec_context *ecp)
{
    RDB_type *typ = RDB_obj_type(objp);

    /* Array and tuple types must be destroyed */
    if (!RDB_type_is_scalar(typ) && !RDB_type_is_relation(typ)) {
        if (RDB_del_nonscalar_type(typ, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    return RDB_free_obj(objp, ecp);
}

void
Duro_remove_varmap(Duro_interp *interp)
{
    varmap_node *parentp = interp->current_varmapp->parentp;
    Duro_destroy_varmap(&interp->current_varmapp->map);
    RDB_free(interp->current_varmapp);
    interp->current_varmapp = parentp;
}

void
Duro_init_vars(Duro_interp *interp)
{
    Duro_init_varmap(&interp->root_varmap, DEFAULT_VARMAP_SIZE);
    interp->current_varmapp = NULL;
}

void
Duro_destroy_vars(Duro_interp *interp)
{
    Duro_destroy_varmap(&interp->root_varmap);
}

/*
 * Set interp->current_varmapp and return the old value
 */
varmap_node *
Duro_set_current_varmap(Duro_interp *interp, varmap_node *newvarmapp)
{
    varmap_node *ovarmapp = interp->current_varmapp;
    interp->current_varmapp = newvarmapp;
    return ovarmapp;
}

static Duro_var_entry *
lookup_transient_var_e(Duro_interp *interp, const char *name, varmap_node *varmapp)
{
    Duro_var_entry *varentryp;
    varmap_node *nodep = varmapp;

    /* Search in local vars */
    while (nodep != NULL) {
        varentryp = Duro_varmap_get(&nodep->map, name);
        if (varentryp != NULL && varentryp->varp != NULL)
            return varentryp;
        nodep = nodep->parentp;
    }

    /* Search in global transient vars */
    varentryp = Duro_varmap_get(&interp->root_varmap, name);
    if (varentryp == NULL)
        return NULL;
    return varentryp;
}

RDB_object *
Duro_lookup_transient_var(Duro_interp *interp, const char *name)
{
    Duro_var_entry *entryp = lookup_transient_var_e(interp, name,
            interp->current_varmapp);
    return entryp != NULL ? entryp->varp : NULL;
}

Duro_var_entry *
Duro_lookup_transient_var_e(Duro_interp *interp, const char *name)
{
    return lookup_transient_var_e(interp, name, interp->current_varmapp);
}

RDB_object *
Duro_get_var(const char *name, void *arg)
{
    Duro_interp *interp = arg;
    Duro_var_entry *entryp = lookup_transient_var_e(interp, name,
            interp->current_varmapp);
    return entryp != NULL ? entryp->varp : NULL;
}

RDB_type *
Duro_get_var_type(const char *name, void *arg)
{
    Duro_interp *interp = arg;
    Duro_var_entry *entryp = lookup_transient_var_e(interp, name,
            interp->current_varmapp);
    return entryp != NULL ? RDB_obj_type(entryp->varp) : NULL;
}

int
Duro_exec_vardef_virtual(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_expression *defexp;
    RDB_expression *texp;
    const char *varname = RDB_expr_var_name(nodep->exp);

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    defexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, &interp->txnp->tx);
    if (defexp == NULL)
        return RDB_ERROR;

    texp = RDB_dup_expr(defexp, ecp);
    if (texp == NULL)
        return RDB_ERROR;

    tbp = RDB_expr_to_vtable(texp, ecp, &interp->txnp->tx);
    if (tbp == NULL) {
        RDB_del_expr(texp, ecp);
        return RDB_ERROR;
    }
    if (RDB_set_table_name(tbp, varname, ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;
    if (RDB_add_table(tbp, NULL, ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Table %s created.\n", varname);
    return RDB_OK;
}

int
Duro_exec_vardrop(const RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_object nameobj;
    const char *varname;
    Duro_var_entry *varentryp = NULL;

    if (interp->current_foreachp != NULL) {
        RDB_raise_not_supported("Dropping variables not supported in FOR .. IN loops", ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&nameobj);

    /* If we're within PACKAGE, prepend package name */
    if (*RDB_obj_string(&interp->pkg_name) != '\0') {
        if (Duro_package_q_id(&nameobj, RDB_expr_var_name(nodep->exp), interp, ecp) != RDB_OK)
            goto error;
        varname = RDB_obj_string(&nameobj);
    } else {
        varname = RDB_expr_var_name(nodep->exp);
    }

    /* Try to look up local variable */
    if (interp->current_varmapp != NULL) {
        varentryp = Duro_varmap_get(&interp->current_varmapp->map, varname);
        if (varentryp != NULL && varentryp->varp != NULL
                && !(DURO_VAR_CONST & varentryp->flags)) {
            RDB_object *varp = varentryp->varp;
            /* Delete key by putting NULL value */
            if (Duro_varmap_put(&interp->current_varmapp->map, varname, NULL,
                    DURO_VAR_CONST, ecp) != RDB_OK) {
                goto error;
            }
            if (!(DURO_VAR_FREE & varentryp->flags))
                return RDB_OK;
            /* Destroy transient variable */
            return del_local_var(varp, ecp);
        }
    }
    if (varentryp == NULL || varentryp->varp == NULL) {
        varentryp = Duro_varmap_get(&interp->root_varmap, varname);
        if (varentryp != NULL && varentryp->varp != NULL
                && !(DURO_VAR_CONST & varentryp->flags)) {
            RDB_object *varp = varentryp->varp;
            int flags = varentryp->flags;

            if (Duro_varmap_put(&interp->root_varmap, varname, NULL,
                    DURO_VAR_CONST, ecp) != RDB_OK) {
                goto error;
            }
            if (!(DURO_VAR_FREE & flags))
                return RDB_OK;
            return del_local_var(varp, ecp);
        }
    }

    /*
     * Delete persistent table
     */

    if (interp->txnp == NULL) {
        RDB_raise_name(varname, ecp);
        goto error;
    }

    if (RDB_drop_table_by_name(varname, ecp, &interp->txnp->tx) != RDB_OK)
        goto error;

    if (RDB_parse_get_interactive())
        printf("Table %s dropped.\n", varname);

    RDB_destroy_obj(&nameobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

int
Duro_exec_rename(const RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_object *tbp;
    const char *srcname = RDB_expr_var_name(nodep->val.children.firstp->exp);
    const char *dstname = RDB_expr_var_name(
            nodep->val.children.firstp->nextp->nextp->exp);

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (interp->current_foreachp != NULL) {
        RDB_raise_not_supported("Dropping variables not supported in FOR .. IN loops", ecp);
        return RDB_ERROR;
    }

    /*
     * If a foreach loop is running, check if the table
     * depends on the foreach expression
     */

    tbp = RDB_get_table(srcname, ecp, &interp->txnp->tx);
    if (tbp == NULL)
        return RDB_ERROR;
    if (RDB_set_table_name(tbp, dstname, ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Table %s renamed.\n", srcname);
    return RDB_OK;
}

/* Find a variable which depends on a type */
static const char *
var_of_type(Duro_varmap *mapp, RDB_type *typ)
{
    RDB_hashtable_iter it;
    Duro_var_entry *entryp;

    RDB_init_hashtable_iter(&it, &mapp->hashtab);
    for(;;) {
        entryp = RDB_hashtable_next(&it);
        if (entryp == NULL)
            break;
        if (entryp->varp != NULL) {
            RDB_type *vtyp = RDB_obj_type((RDB_object *) entryp->varp);

            if (vtyp != NULL && RDB_type_depends_type(vtyp, typ))
                return entryp->name;
        }
    }
    return NULL;
}

/*
 * Checks if *typ is in use by a transient variable.
 * If it is, returns the name of the variable, otherwise NULL.
 */
const char *
Duro_type_in_use(Duro_interp *interp, RDB_type *typ)
{
    const char *typenamp;
    varmap_node *varnodep = interp->current_varmapp;
    if (varnodep != NULL) {
        do {
            typenamp = var_of_type(&varnodep->map, typ);
            if (typenamp != NULL)
                return typenamp;
            varnodep = varnodep->parentp;
        } while (varnodep != NULL);
    }
    return var_of_type(&interp->root_varmap, typ);
}

RDB_database *
Duro_get_db(Duro_interp *interp, RDB_exec_context *ecp)
{
    char *dbname;
    Duro_var_entry *varentryp = Duro_varmap_get(&interp->root_varmap, "current_db");
    if (varentryp == NULL || varentryp->varp == NULL) {
        RDB_raise_resource_not_found("no database", ecp);
        return NULL;
    }
    dbname = RDB_obj_string(varentryp->varp);
    if (*dbname == '\0') {
        RDB_raise_not_found("no database", ecp);
        return NULL;
    }
    if (interp->envp == NULL) {
        RDB_raise_resource_not_found("no connection", ecp);
        return NULL;
    }

    return RDB_get_db_from_env(dbname, interp->envp, ecp, NULL);
}

/*
 * Convert parse node into type. If the type cannot be found and no
 * transaction was active, start a transaction and try again.
 */
RDB_type *
Duro_parse_node_to_type_retry(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_database *dbp;
    RDB_transaction tx;
    RDB_exec_context ec;
    RDB_type *typ = RDB_parse_node_to_type(nodep, &Duro_get_var_type,
            interp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    /*
     * Success or error different from NAME_ERROR and OPERATOR_NOT_FOUND_ERROR
     * -> return
     */
    if (typ != NULL) {
        if (RDB_type_is_generic(typ)) {
            if (!RDB_type_is_scalar(typ))
                RDB_del_nonscalar_type(typ, ecp);
            RDB_raise_syntax("generic type not permitted", ecp);
            return NULL;
        }
        return typ;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_TYPE_NOT_FOUND_ERROR
            && RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR)
        return NULL;
    /*
     * If a transaction is already active or no environment is
     * available, give up
     */
    if (interp->txnp != NULL || interp->envp == NULL)
        return NULL;
    /*
     * Start transaction and retry.
     */
    RDB_init_exec_context(&ec);
    dbp = Duro_get_db(interp, &ec);
    RDB_destroy_exec_context(&ec);
    if (dbp == NULL) {
        return NULL;
    }

    if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
        return NULL;
    typ = RDB_parse_node_to_type(nodep, &Duro_get_var_type,
                interp, ecp, &tx);
    if (typ == NULL) {
        RDB_commit(ecp, &tx);
        return NULL;
    }
    if (RDB_type_is_generic(typ)) {
        RDB_raise_syntax("generic type not permitted", ecp);
        if (!RDB_type_is_scalar(typ)) {
            RDB_del_nonscalar_type(typ, ecp);
        }
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

/* Prepend id with package name */
int
Duro_package_q_id(RDB_object *dstobjp, const char *id, Duro_interp *interp, RDB_exec_context *ecp)
{
    if (RDB_string_to_obj(dstobjp, RDB_obj_string(&interp->pkg_name), ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(dstobjp, ".", ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(dstobjp, id, ecp);
}

int
Duro_nodes_to_seqitv(RDB_seq_item *seqitv, RDB_parse_node *nodep,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    int i = 0;

    if (nodep != NULL) {
        for (;;) {
            /* Get attribute name */
            exp = RDB_parse_node_expr(nodep->val.children.firstp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL);
            if (exp == NULL) {
                return RDB_ERROR;
            }
            seqitv[i].attrname = (char *) RDB_expr_var_name(exp);

            /* Get ascending/descending info */
            seqitv[i].asc = (RDB_bool)
                    (nodep->val.children.firstp->nextp->val.token == TOK_ASC);

            nodep = nodep->nextp;
            if (nodep == NULL)
                break;

            /* Skip comma */
            nodep = nodep->nextp;

            i++;
        }
    }
    return RDB_OK;
}
