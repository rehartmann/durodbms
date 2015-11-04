/*
 * Catalog functions dealing with physical tables and indexes.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "cat_stored.h"
#include <gen/strfns.h>
#include <obj/objinternal.h>

int
RDB_string_to_id(RDB_object *id, const char *str, RDB_exec_context *ecp)
{
    RDB_object strobj;
    RDB_object *strobjp = &strobj;

    RDB_init_obj(&strobj);
    if (RDB_string_to_obj(&strobj, str, ecp) != RDB_OK)
        goto error;

    /* Call selector */
    if (RDB_call_ro_op_by_name("identifier", 1, &strobjp, ecp, NULL, id)
            != RDB_OK)
        goto error;
    RDB_destroy_obj(&strobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&strobj, ecp);
    if (RDB_obj_type(RDB_get_err(ecp))
            == &RDB_TYPE_CONSTRAINT_VIOLATION_ERROR) {
        RDB_raise_invalid_argument("invalid name", ecp);
    }
    return RDB_ERROR;
}

int
RDB_cat_insert_index(const char *name, int attrc, const RDB_seq_item attrv[],
        RDB_bool unique, RDB_bool ordered, const char *tbname,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl;
    RDB_object attrsarr;
    RDB_object attrtpl;
    RDB_object tbnameobj;
    RDB_object attrnameobj;

    RDB_init_obj(&tpl);
    RDB_init_obj(&attrsarr);
    RDB_init_obj(&attrtpl);
    RDB_init_obj(&tbnameobj);
    RDB_init_obj(&attrnameobj);

    ret = RDB_string_to_id(&tbnameobj, tbname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "idxname", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set(&tpl, "tablename", &tbnameobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_set_array_length(&attrsarr, (RDB_int) attrc, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    for (i = 0; i < attrc; i++) {
        ret = RDB_string_to_id(&attrnameobj, attrv[i].attrname, ecp);
        if (ret != RDB_OK)
            goto cleanup;
        ret = RDB_tuple_set(&attrtpl, "attrname", &attrnameobj, ecp);
        if (ret != RDB_OK)
            goto cleanup;
        ret = RDB_tuple_set_bool(&attrtpl, "asc", attrv[i].asc, ecp);
        if (ret != RDB_OK)
            goto cleanup;
        ret = RDB_array_set(&attrsarr, (RDB_int) i, &attrtpl, ecp);
        if (ret != RDB_OK)
            goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "attrs", &attrsarr, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_bool(&tpl, "unique", unique, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_bool(&tpl, "ordered", ordered, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->indexes_tbp, &tpl, ecp, txp);

cleanup:
    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&attrsarr, ecp);
    RDB_destroy_obj(&attrtpl, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);
    return ret;
}

RDB_expression *
RDB_tablename_id_eq_expr(const char *name, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_expression *arg2p;
    RDB_expression *argp = RDB_var_ref("tablename", ecp);
    if (argp == NULL) {
        return NULL;
    }
    arg2p = RDB_expr_property(argp, "name", ecp);
    if (arg2p == NULL) {
        RDB_del_expr(argp, ecp);
        return NULL;
    }
    argp = arg2p;

    arg2p = RDB_string_to_expr(name, ecp);
    if (arg2p == NULL) {
        RDB_del_expr(argp, ecp);
        return NULL;
    }
    exp = RDB_eq(argp, arg2p, ecp);
    if (exp == NULL) {
        RDB_del_expr(argp, ecp);
        RDB_del_expr(arg2p, ecp);
        return NULL;
    }

    /* Set transformed flag to avoid infinite recursion */
    exp->transformed = RDB_TRUE;
    return exp;
}

/*
 * Read table indexes from catalog
 */
int
RDB_cat_get_indexes(const char *tablename, RDB_dbroot *dbrootp,
        RDB_exec_context *ecp, RDB_transaction *txp, struct RDB_tbindex **indexvp)
{
    int ret;
    int i;
    int j;
    int indexc;
    RDB_object *vtbp;
    RDB_object arr;
    RDB_expression *argp;
    RDB_expression *exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        return RDB_ERROR;
    }
    argp = RDB_table_ref(dbrootp->indexes_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_tablename_id_eq_expr(tablename, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (vtbp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&arr);
    ret = RDB_table_to_array(&arr, vtbp, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    indexc = RDB_array_length(&arr, ecp);
    if (indexc > 0) {
        (*indexvp) = RDB_alloc(sizeof(RDB_tbindex) * indexc, ecp);
        if (*indexvp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        for (i = 0; i < indexc; i++) {
            RDB_object *tplp;
            RDB_object *attrarrp;
            char *idxname;
            RDB_tbindex *indexp = &(*indexvp)[i];

            tplp = RDB_array_get(&arr, (RDB_int) i, ecp);
            if (tplp == NULL) {
                ret = RDB_ERROR;
                goto cleanup;
            }

            idxname = RDB_tuple_get_string(tplp, "idxname");
            if (idxname[0] != '\0') {
                indexp->name = RDB_dup_str(idxname);
                if (indexp->name == NULL) {
                    RDB_raise_no_memory(ecp);
                    ret = RDB_ERROR;
                    goto cleanup;
                }
            }

            attrarrp = RDB_tuple_get(tplp, "attrs");
            indexp->attrc = RDB_array_length(attrarrp, ecp);
            if (indexp->attrc < 0) {
                ret = indexp->attrc;
                goto cleanup;
            }

            indexp->attrv = RDB_alloc(sizeof (RDB_seq_item) * indexp->attrc, ecp);
            if (indexp->attrv == NULL) {
                ret = RDB_ERROR;
                goto cleanup;
            }

            for (j = 0; j < indexp->attrc; j++) {
                RDB_object *attrtplp;

                attrtplp = RDB_array_get(attrarrp, (RDB_int) j, ecp);
                if (attrtplp == NULL) {
                    ret = RDB_ERROR;
                    goto cleanup;
                }

                indexp->attrv[j].attrname = RDB_dup_str(RDB_tuple_get_string(
                        attrtplp, "attrname"));
                if (indexp->attrv[j].attrname == NULL) {
                    RDB_raise_no_memory(ecp);
                    ret = RDB_ERROR;
                    goto cleanup;
                }
                indexp->attrv[j].asc = RDB_tuple_get_bool(attrtplp, "asc");
            }

            indexp->unique = RDB_tuple_get_bool(tplp, "unique");
            indexp->ordered = RDB_tuple_get_bool(tplp, "ordered");
        }
    }
    ret = indexc;

cleanup:
    RDB_destroy_obj(&arr, ecp);
    RDB_drop_table(vtbp, ecp, NULL);
    return ret;
}

int
RDB_cat_insert_table_recmap(RDB_object *tbp, const char *rmname,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_object tbnameobj;

    RDB_init_obj(&tbnameobj);

    /* Insert entry into sys_vtables */

    ret = RDB_string_to_id(&tbnameobj, RDB_table_name(tbp), ecp);
    if (ret != RDB_OK)
        goto cleanup;

    RDB_init_obj(&tpl);

    ret = RDB_tuple_set(&tpl, "tablename", &tbnameobj, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "recmap", rmname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->table_recmap_tbp, &tpl, ecp, txp);

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);
    return ret;
}

int
RDB_cat_recmap_name(RDB_object *tbp, RDB_object *rmnameobjp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_expression *argp;
    RDB_expression *exp;
    RDB_object *tmptbp = NULL;

    RDB_init_obj(&tpl);
    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        goto error;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->table_recmap_tbp, ecp);
    if (argp == NULL) {
        goto error;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_tablename_id_eq_expr(RDB_table_name(tbp), ecp);
    if (argp == NULL) {
        goto error;
    }
    RDB_add_arg(exp, argp);

    tmptbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptbp == NULL) {
        goto error;
    }
    exp = NULL;
    ret = RDB_extract_tuple(tmptbp, ecp, txp, &tpl);
    if (ret != RDB_OK) {
        goto error;
    }
    if (RDB_string_to_obj(rmnameobjp, RDB_tuple_get_string(&tpl, "recmap"),
            ecp) != RDB_OK)
        goto error;
    RDB_destroy_obj(&tpl, ecp);

    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, ecp, txp);
    return RDB_OK;

error:
    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, ecp, txp);
    if (exp != NULL)
        RDB_del_expr(exp, ecp);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_ERROR;
}
