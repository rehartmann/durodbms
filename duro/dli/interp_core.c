/*
 * interp_core.c
 *
 *  Created on: 24.08.2012
 *      Author: Rene Hartmann
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
    RDB_init_hashmap(&nodep->map, DEFAULT_VARMAP_SIZE);
    nodep->parentp = interp->current_varmapp;
    interp->current_varmapp = nodep;
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
    void *datap;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    RDB_init_hashmap_iter(&it, map);
    for(;;) {
        if (RDB_hashmap_next(&it, &datap) == NULL)
            break;
        if (datap != NULL) {
            drop_local_var(datap, &ec);
        }
    }

    RDB_destroy_hashmap(map);
    RDB_destroy_exec_context(&ec);
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
    RDB_init_hashmap(&interp->root_varmap, DEFAULT_VARMAP_SIZE);
    RDB_init_hashmap(&interp->sys_varmap, DEFAULT_VARMAP_SIZE);
    interp->current_varmapp = NULL;
}

void
Duro_destroy_vars(Duro_interp *interp)
{
    Duro_destroy_varmap(&interp->root_varmap);

    /* Destroy only the varmap, not the variables as they are global */
    RDB_destroy_hashmap(&interp->sys_varmap);
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

static RDB_object *
lookup_transient_var(Duro_interp *interp, const char *name, varmap_node *varmapp)
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
    objp = RDB_hashmap_get(&interp->root_varmap, name);
    if (objp != NULL)
        return objp;

    /* Search in system Duro_module */
    return RDB_hashmap_get(&interp->sys_varmap, name);
}

RDB_object *
Duro_lookup_transient_var(Duro_interp *interp, const char *name)
{
    return lookup_transient_var(interp, name, interp->current_varmapp);
}

RDB_object *
Duro_get_var(const char *name, void *arg)
{
    Duro_interp *interp = arg;
    return lookup_transient_var(interp, name, interp->current_varmapp);
}

RDB_type *
Duro_get_var_type(const char *name, void *arg)
{
    Duro_interp *interp = arg;
    RDB_object *objp = Duro_lookup_transient_var(interp, name);
    return objp != NULL ? RDB_obj_type(objp) : NULL;
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
    if (RDB_add_table(tbp, ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Table %s created.\n", varname);
    return RDB_OK;
}

static int
check_foreach_depends(const RDB_object *tbp, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    foreach_iter *itp = interp->current_foreachp;
    while (itp != NULL) {
        if (RDB_table_refers(itp->tbp, tbp)) {
            RDB_raise_in_use("FOREACH refers to table", ecp);
            return RDB_ERROR;
        }
        itp = itp->prevp;
    }
    return RDB_OK;
}

int
Duro_exec_vardrop(const RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *objp = NULL;

    /* Try to look up local variable */
    if (interp->current_varmapp != NULL) {
        objp = RDB_hashmap_get(&interp->current_varmapp->map, varname);
        if (objp != NULL) {
            if (check_foreach_depends(objp, interp, ecp) != RDB_OK)
                return RDB_ERROR;
            /* Delete key by putting NULL value */
            if (RDB_hashmap_put(&interp->current_varmapp->map, varname, NULL) != RDB_OK)
                return RDB_ERROR;
        }
    }
    if (objp == NULL) {
        objp = RDB_hashmap_get(&interp->root_varmap, varname);
        if (objp != NULL) {
            if (check_foreach_depends(objp, interp, ecp) != RDB_OK)
                return RDB_ERROR;
            if (RDB_hashmap_put(&interp->root_varmap, varname, NULL) != RDB_OK)
                return RDB_ERROR;
        }
    }

    if (objp != NULL) {
        /* Destroy transient variable */
        return drop_local_var(objp, ecp);
    }

    /*
     * Delete persistent table
     */

    if (interp->txnp == NULL) {
        RDB_raise_name(varname, ecp);
        return RDB_ERROR;
    }

    /*
     * If a foreach loop is running, check if the table
     * depends on the foreach expression
     */

    objp = RDB_get_table(varname, ecp, &interp->txnp->tx);
    if (objp == NULL)
        return RDB_ERROR;

    if (check_foreach_depends(objp, interp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_drop_table(objp, ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Table %s dropped.\n", varname);
    return RDB_OK;
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

    /*
     * If a foreach loop is running, check if the table
     * depends on the foreach expression
     */

    tbp = RDB_get_table(srcname, ecp, &interp->txnp->tx);
    if (tbp == NULL)
        return RDB_ERROR;

    if (check_foreach_depends(tbp, interp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_set_table_name(tbp, dstname, ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Table %s renamed.\n", srcname);
    return RDB_OK;
}

int
Duro_put_var(const char *name, RDB_object *objp, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    if (RDB_hashmap_put(&interp->current_varmapp->map, name, objp)
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
    const char *namp;
    void *datap;

    RDB_init_hashmap_iter(&it, mapp);
    for(;;) {
        namp = RDB_hashmap_next(&it, &datap);
        if (namp == NULL)
            break;
        if (datap != NULL) {
            RDB_type *vtyp = RDB_obj_type((RDB_object *) datap);

            /* If the types are equal, return variable name */
            if (vtyp != NULL && RDB_type_depends_type(vtyp, typ))
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
    RDB_object *dbnameobjp = RDB_hashmap_get(&interp->sys_varmap, "current_db");
    if (dbnameobjp == NULL) {
        RDB_raise_resource_not_found("no database", ecp);
        return NULL;
    }
    dbname = RDB_obj_string(dbnameobjp);
    if (*dbname == '\0') {
        RDB_raise_not_found("no database", ecp);
        return NULL;
    }
    if (interp->envp == NULL) {
        RDB_raise_resource_not_found("no connection", ecp);
        return NULL;
    }

    return RDB_get_db_from_env(dbname, interp->envp, ecp);
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
    if (typ != NULL)
        return typ;
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

/*
 * Initialize *objp and set type information, consuming typ on success.
 */
int
Duro_init_obj(RDB_object *objp, RDB_type *typ, Duro_interp *interp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    if (RDB_type_is_tuple(typ)) {
        RDB_type *attrtyp;

        for (i = 0; i < typ->def.tuple.attrc; i++) {
            if (RDB_tuple_set(objp, typ->def.tuple.attrv[i].name,
                    NULL, ecp) != RDB_OK)
                return RDB_ERROR;
            attrtyp = RDB_dup_nonscalar_type(typ->def.tuple.attrv[i].typ, ecp);
            if (attrtyp == NULL)
                return RDB_ERROR;

            if (Duro_init_obj(RDB_tuple_get(objp, typ->def.tuple.attrv[i].name),
                    attrtyp, interp, ecp, txp) != RDB_OK) {
                if (!RDB_type_is_scalar(attrtyp)) {
                    RDB_del_nonscalar_type(typ, ecp);
                }
                return RDB_ERROR;
            }
        }
        RDB_obj_set_typeinfo(objp, typ);
    } else if (RDB_type_is_array(typ)) {
        RDB_obj_set_typeinfo(objp, typ);
        if (RDB_set_array_length(objp, (RDB_int) 0, ecp) != RDB_OK)
            return RDB_ERROR;
    } else {
        return RDB_set_init_value(objp, typ, interp->envp, ecp);
    }
    return RDB_OK;
}
