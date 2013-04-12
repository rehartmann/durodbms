/*
 * cat_type.c
 *
 *  Created on: 02.09.2012
 *      Author: Rene Hartmann
 */

#include "cat_type.h"
#include "internal.h"
#include "serialize.h"
#include <gen/strfns.h>

#include <stddef.h>
#include <string.h>

static int
types_query(const char *name, RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object **tbpp)
{
    RDB_expression *exp, *argp;

    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        return RDB_ERROR;
    }

    argp = RDB_table_ref(txp->dbp->dbrootp->types_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("typename", ecp),
                    RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    *tbpp = RDB_expr_to_vtable(exp, ecp, txp);
    if (*tbpp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_possreps_query(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object **tbpp)
{
    RDB_expression *exp, *argp;

    exp = RDB_ro_op("project", ecp);
    if (exp == NULL)
        return RDB_ERROR;
    argp = RDB_table_ref(txp->dbp->dbrootp->possrepcomps_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_string_to_expr("typename", ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_string_to_expr("possrepname", ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = exp;
    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        RDB_del_expr(argp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("typename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    *tbpp = RDB_expr_to_vtable(exp, ecp, txp);
    if (*tbpp == NULL) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static RDB_object *
possrepcomps_query(const char *name, const char *possrepname,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;
    RDB_expression *argp, *wexp;
    RDB_expression *exp = RDB_ro_op("and", ecp);
    if (exp == NULL) {
        return NULL;
    }
    argp = RDB_eq(RDB_var_ref("typename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("possrepname", ecp),
            RDB_string_to_expr(possrepname, ecp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    wexp = RDB_ro_op("where", ecp);
    if (wexp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->possrepcomps_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(wexp, ecp);
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(wexp, argp);
    RDB_add_arg(wexp, exp);

    tbp = RDB_expr_to_vtable(wexp, ecp, txp);
    if (tbp == NULL) {
        RDB_del_expr(wexp, ecp);
        return NULL;
    }
    return tbp;
}

static int
get_possrepcomps(const char *typename, RDB_possrep *rep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object comps;
    RDB_object *tplp;
    RDB_object *tmptbp = NULL;

    RDB_init_obj(&comps);

    tmptbp = possrepcomps_query(typename, rep->name, ecp, txp);
    if (tmptbp == NULL) {
        goto error;
    }
    ret = RDB_table_to_array(&comps, tmptbp, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    ret = RDB_array_length(&comps, ecp);
    if (ret < 0) {
        goto error;
    }
    rep->compc = ret;
    if (ret > 0) {
        rep->compv = RDB_alloc(ret * sizeof (RDB_attr), ecp);
        if (rep->compv == NULL) {
            goto error;
        }
    } else {
        rep->compv = NULL;
    }

    /*
     * Read component data from array and store it in
     * rep->compv.
     */
    for (i = 0; i < rep->compc; i++) {
        RDB_int idx;

        tplp = RDB_array_get(&comps, (RDB_int) i, ecp);
        if (tplp == NULL)
            goto error;
        idx = RDB_tuple_get_int(tplp, "compno");
        rep->compv[idx].name = RDB_dup_str(
                RDB_tuple_get_string(tplp, "compname"));
        if (rep->compv[idx].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }

        rep->compv[idx].typ = RDB_binobj_to_type(
                RDB_tuple_get(tplp, "comptype"), ecp, txp);
        if (rep->compv[idx].typ == NULL)
            goto error;
    }
    RDB_drop_table(tmptbp, ecp, txp);
    RDB_destroy_obj(&comps, ecp);

    return RDB_OK;

error:
    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, ecp, txp);
    RDB_destroy_obj(&comps, ecp);

    return RDB_ERROR;
}

int
RDB_cat_get_type(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_type **typp)
{
    RDB_object *tmptb1p = NULL;
    RDB_object *tmptb2p = NULL;
    RDB_object tpl;
    RDB_object *tplp;
    RDB_object possreps;
    RDB_object *cvalp;
    RDB_object *ivalp;
    RDB_object *implvalp;
    RDB_type *typ = NULL;
    RDB_object *typedatap;
    int ret, tret;
    int i;

    RDB_init_obj(&tpl);
    RDB_init_obj(&possreps);

    /*
     * Get type info from sys_types
     */

    ret = types_query(name, ecp, txp, &tmptb1p);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_extract_tuple(tmptb1p, ecp, txp, &tpl);
    if (ret != RDB_OK)
        goto error;

    typ = RDB_alloc(sizeof (RDB_type), ecp);
    if (typ == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    typ->kind = RDB_TP_SCALAR;
    typ->compare_op = NULL;
    typ->def.scalar.repc = 0;

    typedatap = RDB_tuple_get(&tpl, "arep_type");
    if (RDB_binary_length(typedatap) != 0) {
        typ->def.scalar.arep = RDB_binobj_to_type(typedatap, ecp, txp);
        if (typ->def.scalar.arep == NULL)
            goto error;
    } else {
        typ->def.scalar.arep = NULL;
    }

    typ->name = RDB_dup_str(name);
    if (typ->name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    implvalp = RDB_tuple_get(&tpl, "implemented");
    typ->def.scalar.implemented = RDB_obj_bool(implvalp);

    cvalp = RDB_tuple_get(&tpl, "constraint");
    if (RDB_binary_length(cvalp) > 0) {
        typ->def.scalar.constraintp = RDB_binobj_to_expr(cvalp, ecp, txp);
        if (typ->def.scalar.constraintp == NULL)
            goto error;
    } else {
        typ->def.scalar.constraintp = NULL;
    }

    /*
     * Read init expression but do not evaluate it
     * as this may lead to an infinite recursion
     * because evaluation may result in an attempt to read the type
     */
    ivalp = RDB_tuple_get(&tpl, "init");
    if (RDB_binary_length(ivalp) > 0) {
        typ->def.scalar.initexp = RDB_binobj_to_expr(ivalp, ecp, txp);
        if (typ->def.scalar.initexp == NULL)
            goto error;
    } else {
        typ->def.scalar.initexp = NULL;
    }
    typ->def.scalar.init_val_is_valid = RDB_FALSE;

    typ->ireplen = RDB_tuple_get_int(&tpl, "arep_len");
    typ->def.scalar.sysimpl = RDB_tuple_get_bool(&tpl, "sysimpl");
    typ->def.scalar.repc = 0;
    typ->def.scalar.builtin = RDB_FALSE;

    /*
     * Get possrep info from sys_possreps
     */

    ret = RDB_possreps_query(name, ecp, txp, &tmptb2p);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_table_to_array(&possreps, tmptb2p, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    ret = RDB_array_length(&possreps, ecp);
    if (ret < 0) {
        goto error;
    }
    typ->def.scalar.repc = ret;
    if (ret > 0) {
        typ->def.scalar.repv = RDB_alloc(ret * sizeof (RDB_possrep), ecp);
        if (typ->def.scalar.repv == NULL) {
            goto error;
        }
    }
    for (i = 0; i < typ->def.scalar.repc; i++)
        typ->def.scalar.repv[i].compv = NULL;

    /*
     * Read possrep data from array and store it in typ->def.scalar.repv
     */
    for (i = 0; i < typ->def.scalar.repc; i++) {
        tplp = RDB_array_get(&possreps, (RDB_int) i, ecp);
        if (tplp == NULL)
            goto error;
        typ->def.scalar.repv[i].name = RDB_dup_str(
                RDB_tuple_get_string(tplp, "possrepname"));
        if (typ->def.scalar.repv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }

        ret = get_possrepcomps(name, &typ->def.scalar.repv[i], ecp, txp);
        if (ret != RDB_OK)
            goto error;
    }

    *typp = typ;

    ret = RDB_drop_table(tmptb1p, ecp, txp);
    tret = RDB_drop_table(tmptb2p, ecp, txp);
    if (ret == RDB_OK)
        ret = tret;

    RDB_destroy_obj(&tpl, ecp);
    tret = RDB_destroy_obj(&possreps, ecp);
    if (ret == RDB_OK)
        ret = tret;

    return ret;

error:
    if (tmptb1p != NULL)
        RDB_drop_table(tmptb1p, ecp, txp);
    if (tmptb2p != NULL)
        RDB_drop_table(tmptb2p, ecp, txp);
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&possreps, ecp);
    if (typ != NULL) {
        if (typ->def.scalar.repc != 0) {
            for (i = 0; i < typ->def.scalar.repc; i++)
                RDB_free(typ->def.scalar.repv[i].compv);
            RDB_free(typ->def.scalar.repv);
        }
        RDB_free(typ->name);
        RDB_free(typ);
    }
    return RDB_ERROR;
}

/*
 * Create expression which represents the type
 */
static RDB_expression *
scalar_type_to_expr(const char *name, RDB_exec_context *ecp)
{
    int pos = 0;
    RDB_expression *exp = RDB_obj_to_expr(NULL, ecp);
    if (exp == NULL)
        return NULL;
    /* Start with a buffer of 1 bytes */
    if (RDB_binary_set(RDB_expr_obj(exp), 0, "\0", (size_t) 1, ecp)
            != RDB_OK) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    if (RDB_serialize_scalar_type(RDB_expr_obj(exp), &pos, name, ecp)
            != RDB_OK) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    /* Set length to actual length */
    RDB_expr_obj(exp)->val.bin.len = pos;
    return exp;
}

static RDB_expression *
attr_type_query(const char *name, RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_expression *argp, *arg2p;

    /* Create expression sys_tableattrs WHERE type = <type given by name> */

    argp = RDB_var_ref("type", ecp);
    if (argp == NULL)
        goto error;

    exp = RDB_ro_op("=", ecp);
    if (exp == NULL) {
        RDB_del_expr(argp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    /*
     * Create expression which represents the type
     */
    argp = scalar_type_to_expr(name, ecp);
    if (argp == NULL)
        return NULL;

    RDB_add_arg(exp, argp);

    arg2p = exp;
    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        RDB_del_expr(arg2p, ecp);
        return NULL;
    }

    argp = RDB_table_ref(dbp->dbrootp->table_attr_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(arg2p, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    RDB_add_arg(exp, arg2p);

    return exp;

error:
    RDB_del_expr(exp, ecp);
    return NULL;
}

static RDB_expression *
ro_op_type_query(const char *name, RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_expression *exp, *ex2p;
    RDB_expression *argp, *arg2p;

    /*
     * Create expression sys_ro_ops
     * WHERE index_of(argtypes, <type given by name>) >= 0
     * OR rtype = <type given by name>
     */

    argp = RDB_var_ref("argtypes", ecp);
    if (argp == NULL)
        goto error;

    exp = RDB_ro_op("index_of", ecp);
    if (exp == NULL) {
        RDB_del_expr(argp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    /*
     * Create expression which represents the type
     */
    argp = scalar_type_to_expr(name, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(exp, argp);

    argp = exp;
    exp = RDB_ro_op(">=", ecp);
    RDB_add_arg(exp, argp);

    argp = RDB_int_to_expr(0, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(exp, argp);

    argp = RDB_var_ref("rtype", ecp);
    if (argp == NULL)
        goto error;

    ex2p = RDB_ro_op("=", ecp);
    if (ex2p == NULL) {
        RDB_del_expr(argp, NULL);
        goto error;
    }
    RDB_add_arg(ex2p, argp);

    argp = scalar_type_to_expr(name, ecp);
    if (argp == NULL) {
        RDB_del_expr(ex2p, NULL);
        goto error;
    }
    RDB_add_arg(ex2p, argp);

    arg2p = RDB_ro_op("or", ecp);
    if (arg2p == NULL) {
        RDB_del_expr(ex2p, NULL);
        goto error;
    }
    RDB_add_arg(arg2p, exp);
    RDB_add_arg(arg2p, ex2p);

    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        RDB_del_expr(arg2p, ecp);
        return NULL;
    }

    argp = RDB_table_ref(dbp->dbrootp->ro_ops_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(arg2p, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    RDB_add_arg(exp, arg2p);

    return exp;

error:
    RDB_del_expr(exp, ecp);
    return NULL;
}

static RDB_expression *
upd_op_type_query(const char *name, RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_expression *argp, *arg2p;

    /*
     * Create expression sys_upd_ops
     * WHERE index_of(argtypes, <type given by name>) >= 0
     */

    argp = RDB_var_ref("argtypes", ecp);
    if (argp == NULL)
        goto error;

    exp = RDB_ro_op("index_of", ecp);
    if (exp == NULL) {
        RDB_del_expr(argp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);

    /*
     * Create expression which represents the type
     */
    argp = scalar_type_to_expr(name, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(exp, argp);

    argp = exp;
    exp = RDB_ro_op(">=", ecp);
    RDB_add_arg(exp, argp);

    argp = RDB_int_to_expr(0, ecp);
    if (argp == NULL)
        goto error;
    RDB_add_arg(exp, argp);

    arg2p = exp;
    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        RDB_del_expr(arg2p, ecp);
        return NULL;
    }

    argp = RDB_table_ref(dbp->dbrootp->upd_ops_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(arg2p, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    RDB_add_arg(exp, arg2p);

    return exp;

error:
    RDB_del_expr(exp, ecp);
    return NULL;
}

/*
 * Raise in_use_error if there is a real table with an attribute of type <name>
 */
int
RDB_cat_check_type_used(RDB_type *typ, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *tmptbp;
    RDB_bool isempty;
    RDB_object tpl;
    RDB_expression *exp = attr_type_query(typ->name, RDB_tx_db(txp), ecp);

    if (exp == NULL)
        return RDB_ERROR;

    tmptbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptbp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }

    if (RDB_table_is_empty(tmptbp, ecp, txp, &isempty) != RDB_OK) {
        RDB_drop_table(tmptbp, ecp, txp);
        return RDB_ERROR;
    }
    if (RDB_drop_table(tmptbp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (!isempty) {
        /* There is at least one real table attribute with this type */
        RDB_raise_in_use("table refers to type", ecp);
        return RDB_ERROR;
    }

    exp = upd_op_type_query(typ->name, RDB_tx_db(txp), ecp);
    if (exp == NULL)
        return RDB_ERROR;

    tmptbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptbp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }

    if (RDB_table_is_empty(tmptbp, ecp, txp, &isempty) != RDB_OK) {
        RDB_drop_table(tmptbp, ecp, txp);
        return RDB_ERROR;
    }
    if (RDB_drop_table(tmptbp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (!isempty) {
        /* There is at least one update operator definition referencing this type */
        RDB_raise_in_use("update operator refers to type", ecp);
        return RDB_ERROR;
    }

    exp = ro_op_type_query(typ->name, RDB_tx_db(txp), ecp);
    if (exp == NULL)
        return RDB_ERROR;

    tmptbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptbp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    /* Only a selector is permitted */
    if (RDB_extract_tuple(tmptbp, ecp, txp, &tpl) != RDB_OK) {
        RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
        if (errtyp == &RDB_INVALID_ARGUMENT_ERROR) {
            /* More than 1 tuple */
            RDB_drop_table(tmptbp, ecp, txp);
            RDB_destroy_obj(&tpl, ecp);
            RDB_clear_err(ecp);
            RDB_raise_in_use("read-only operator refers to type", ecp);
            return RDB_ERROR;
        }
        if (errtyp != &RDB_NOT_FOUND_ERROR) {
            /* Unexpected error */
            RDB_drop_table(tmptbp, ecp, txp);
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);
    } else {
        if (!typ->def.scalar.sysimpl) {
            /*
             * No system-generated selector,
             * so a read-only operator is not acceptible
             */
            RDB_raise_in_use(RDB_tuple_get_string(&tpl, "name"), ecp);
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        if (strcmp(RDB_tuple_get_string(&tpl, "name"),
                typ->def.scalar.repv[0].name) != 0) {
            /* Not a selector */
            RDB_raise_in_use(RDB_tuple_get_string(&tpl, "name"), ecp);
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
    }
    if (RDB_drop_table(tmptbp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    return RDB_destroy_obj(&tpl, ecp);
}
