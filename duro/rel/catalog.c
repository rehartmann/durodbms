/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "catalog.h"
#include "typeimpl.h"
#include "insert.h"
#include "internal.h"
#include "stable.h"
#include "serialize.h"
#include "qresult.h"

#include <gen/strfns.h>
#include <string.h>

enum {
    MAJOR_VERSION = 0,
    MINOR_VERSION = 16
};

/*
 * Definitions of the catalog tables.
 */

static RDB_attr table_attr_attrv[] = {
        { "attrname", &RDB_STRING, NULL, 0 },
        { "tablename", &RDB_STRING, NULL, 0 },
        { "type", &RDB_BINARY, NULL, 0 },
        { "i_fno", &RDB_INTEGER, NULL, 0 } };
static char *table_attr_keyattrv[] = { "attrname", "tablename" };
static RDB_string_vec table_attr_keyv[] = { { 2, table_attr_keyattrv } };

static RDB_attr table_attr_defvals_attrv[] = {
            { "attrname", &RDB_STRING, NULL, 0 },
            { "tablename", &RDB_STRING, NULL, 0 },
            { "default_value", &RDB_BINARY, NULL, 0 } };
static char *table_attr_defvals_keyattrv[] = { "attrname", "tablename" };
static RDB_string_vec table_attr_defvals_keyv[] = { { 2, table_attr_defvals_keyattrv } };

static RDB_attr rtables_attrv[] = {
    { "tablename", &RDB_STRING, NULL, 0 },
    { "is_user", &RDB_BOOLEAN, NULL, 0 },
};
static char *rtables_keyattrv[] = { "tablename" };
static RDB_string_vec rtables_keyv[] = { { 1, rtables_keyattrv } };

static RDB_attr vtables_attrv[] = {
    { "tablename", &RDB_STRING, NULL, 0 },
    { "is_user", &RDB_BOOLEAN, NULL, 0 },
    { "i_def", &RDB_BINARY, NULL, 0 }
};

static char *vtables_keyattrv[] = { "tablename" };
static RDB_string_vec vtables_keyv[] = { { 1, vtables_keyattrv } };

static RDB_attr table_recmap_attrv[] = {
    { "tablename", &RDB_STRING, NULL, 0 },
    { "recmap", &RDB_STRING, NULL, 0 },
};
static char *table_recmap_keyattrv1[] = { "tablename" };
static char *table_recmap_keyattrv2[] = { "recmap" };
static RDB_string_vec table_recmap_keyv[] = {
    { 1, table_recmap_keyattrv1 }, { 1, table_recmap_keyattrv2 } };

static RDB_attr dbtables_attrv[] = {
    { "tablename", &RDB_STRING },
    { "dbname", &RDB_STRING }
};
static char *dbtables_keyattrv[] = { "tablename", "dbname" };
static RDB_string_vec dbtables_keyv[] = { { 2, dbtables_keyattrv } };

static RDB_attr keys_attrv[] = {
    { "tablename", &RDB_STRING, NULL, 0 },
    { "keyno", &RDB_INTEGER, NULL, 0 },
    { "attrs", NULL, NULL, 0 } /* type is set to a relation type later */
};
static char *keys_keyattrv[] = { "tablename", "keyno" };
static RDB_string_vec keys_keyv[] = { { 2, keys_keyattrv } };

static RDB_attr types_attrv[] = {
    { "typename", &RDB_STRING, NULL, 0 },
    { "i_arep_len", &RDB_INTEGER, NULL, 0 },
    { "i_arep_type", &RDB_BINARY, NULL, 0 },
    { "i_sysimpl", &RDB_BOOLEAN, NULL, 0},
    { "i_constraint", &RDB_BINARY, NULL, 0 }
};
static char *types_keyattrv[] = { "typename" };
static RDB_string_vec types_keyv[] = { { 1, types_keyattrv } };

static RDB_attr possrepcomps_attrv[] = {
    { "typename", &RDB_STRING, NULL, 0 },
    { "possrepname", &RDB_STRING, NULL, 0 },
    { "compno", &RDB_INTEGER, NULL, 0 },
    { "compname", &RDB_STRING, NULL, 0 },
    { "comptype", &RDB_BINARY, NULL, 0 }
};
static char *possrepcomps_keyattrv1[] = { "typename", "possrepname", "compno" };
static char *possrepcomps_keyattrv2[] = { "typename", "possrepname", "compname" };
static RDB_string_vec possrepcomps_keyv[] = {
    { 3, possrepcomps_keyattrv1 },
    { 3, possrepcomps_keyattrv2 }
};

static RDB_attr ro_ops_attrv[] = {
    { "name", &RDB_STRING, NULL, 0 },
    { "argtypes", NULL, NULL, 0 }, /* type is set to array of BINARY later */
    { "lib", &RDB_STRING, NULL, 0 },
    { "symbol", &RDB_STRING, NULL, 0 },
    { "source", &RDB_STRING, NULL, 0 },
    { "rtype", &RDB_BINARY, NULL, 0 }
};

static char *ro_ops_keyattrv[] = { "name", "argtypes" };
static RDB_string_vec ro_ops_keyv[] = { { 2, ro_ops_keyattrv } };

static RDB_attr upd_ops_attrv[] = {
    { "name", &RDB_STRING, NULL, 0 },
    { "argtypes", NULL, NULL, 0 }, /* type is set to array of BINARY later */
    { "lib", &RDB_STRING, NULL, 0 },
    { "symbol", &RDB_STRING, NULL, 0 },
    { "source", &RDB_STRING, NULL, 0 },
    { "updv", NULL, NULL, 0 } /* type is set to array of BOOLEAN later */
};

static char *upd_ops_keyattrv[] = { "name", "argtypes" };
static RDB_string_vec upd_ops_keyv[] = { { 2, upd_ops_keyattrv } };

static RDB_attr indexes_attrv[] = {
    { "name", &RDB_STRING, NULL, 0 },
    { "tablename", &RDB_STRING, NULL, 0 },
    { "attrs", NULL, NULL, 0 }, /* type is set to an array type later */
    { "unique", &RDB_BOOLEAN, 0 },
    { "ordered", &RDB_BOOLEAN, 0 }
};

static char *indexes_keyattrv[] = { "name" };
static RDB_string_vec indexes_keyv[] = { { 1, indexes_keyattrv } };

static RDB_attr indexes_attrs_attrv[] = {
    { "name", &RDB_STRING, NULL, 0 },
    { "asc", &RDB_BOOLEAN, NULL, 0 }
};

static RDB_attr constraints_attrv[] = {
    { "constraintname", &RDB_STRING, NULL, 0 },
    { "i_expr", &RDB_BINARY, NULL, 0 }
};
static char *constraints_keyattrv[] = { "constraintname" };
static RDB_string_vec constraints_keyv[] = { { 1, constraints_keyattrv } };

static RDB_attr version_info_attrv[] = {
    { "major_version", &RDB_INTEGER, NULL, 0 },
    { "minor_version", &RDB_INTEGER, NULL, 0 },
    { "micro_version", &RDB_INTEGER, NULL, 0 }
};
static RDB_string_vec version_info_keyv[] = { { 0, NULL } };

static int
dbtables_insert(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    /* Insert (database, table) pair into sys_dbtables */
    RDB_init_obj(&tpl);

    if (RDB_tuple_set_string(&tpl, "tablename", RDB_table_name(tbp), ecp)
            != RDB_OK)
    {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    if (RDB_tuple_set_string(&tpl, "dbname", txp->dbp->name, ecp) != RDB_OK)
    {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    ret = RDB_insert(txp->dbp->dbrootp->dbtables_tbp, &tpl, ecp, txp);
    RDB_destroy_obj(&tpl, ecp);
    
    return ret;
}

static int
insert_key(RDB_string_vec *keyp, int i, const char *tablename,
        RDB_dbroot *dbrootp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int j;
    RDB_object tpl;
    RDB_object *keystbp;
    RDB_attr keysattr;

    RDB_init_obj(&tpl);
    if (RDB_tuple_set_int(&tpl, "keyno", i, ecp) != RDB_OK)
        goto error;

    if (RDB_tuple_set_string(&tpl, "tablename", tablename, ecp) != RDB_OK)
        goto error;

    if (RDB_tuple_set(&tpl, "attrs", NULL, ecp) != RDB_OK)
        goto error;

    keystbp = RDB_tuple_get(&tpl, "attrs");

    keysattr.name = "key";
    keysattr.typ = &RDB_STRING;
    keysattr.defaultp = NULL;
    keysattr.options = 0;

    if (RDB_init_table(keystbp, NULL, 1, &keysattr, 0, NULL, ecp)
            != RDB_OK) {
        goto error;
    }

    for (j = 0; j < keyp->strc; j++) {
        RDB_object ktpl;

        RDB_init_obj(&ktpl);
        if (RDB_tuple_set_string(&ktpl, "key", keyp->strv[j], ecp) != RDB_OK) {
            RDB_destroy_obj(&ktpl, ecp);
            goto error;
        }

        ret = RDB_insert(keystbp, &ktpl, ecp, NULL);
        RDB_destroy_obj(&ktpl, ecp);
        if (ret != RDB_OK) {
            goto error;
        }
    }

    if (RDB_insert(dbrootp->keys_tbp, &tpl, ecp, txp) != RDB_OK)
        goto error;

    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;

error:
    RDB_destroy_obj(&tpl, ecp);

    return RDB_ERROR;
}

/* Insert entries into table sys_tableattr_defvals */
static int
insert_defvals(RDB_object *tbp, RDB_dbroot *dbrootp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl;
    RDB_type *tuptyp = tbp->typ->def.basetyp;

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "tablename", RDB_table_name(tbp), ecp);
    if (ret != RDB_OK) {
        goto error;
    }

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        char *attrname = tuptyp->def.tuple.attrv[i].name;
        RDB_object *defaultp = RDB_tuple_get(tbp->val.tb.default_tplp,
                attrname);
        if (defaultp != NULL) {
            RDB_object binval;
            void *datap;
            size_t len;

            if (!RDB_type_equals(defaultp->typ,
                    tuptyp->def.tuple.attrv[i].typ)) {
                RDB_raise_type_mismatch(
                        "Type of default value does not match attribute type",
                        ecp);
                goto error;
            }

            ret = RDB_tuple_set_string(&tpl, "attrname", attrname, ecp);
            if (ret != RDB_OK) {
                goto error;
            }

            RDB_init_obj(&binval);
            datap = RDB_obj_irep(defaultp, &len);
            ret = RDB_binary_set(&binval, 0, datap, len, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&binval, ecp);
                goto error;
            }

            ret = RDB_tuple_set(&tpl, "default_value", &binval, ecp);
            RDB_destroy_obj(&binval, ecp);
            if (ret != RDB_OK) {
                goto error;
            }

            ret = RDB_insert(dbrootp->table_attr_defvals_tbp, &tpl, ecp, txp);
            if (ret != RDB_OK) {
                goto error;
            }
        }
    }
    RDB_destroy_obj(&tpl, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&tpl, ecp);
    return RDB_ERROR;
}

/* Insert the table pointed to by tbp into the catalog. */
static int
insert_rtable(RDB_object *tbp, RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_type *tuptyp = tbp->typ->def.basetyp;
    int ret;
    int i;

    /* insert entry into table sys_rtables */
    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "tablename", RDB_table_name(tbp), ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    ret = RDB_tuple_set_bool(&tpl, "is_user", tbp->val.tb.is_user, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    ret = RDB_insert(dbrootp->rtables_tbp, &tpl, ecp, txp);
    RDB_destroy_obj(&tpl, ecp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    /* insert entries into table sys_tableattrs */
    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "tablename", RDB_table_name(tbp), ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        RDB_object typedata;
        char *attrname = tuptyp->def.tuple.attrv[i].name;

        RDB_init_obj(&typedata);
        ret = RDB_type_to_binobj(&typedata, tuptyp->def.tuple.attrv[i].typ,
                ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typedata, ecp);
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        ret = RDB_tuple_set(&tpl, "type", &typedata, ecp);
        RDB_destroy_obj(&typedata, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }

        ret = RDB_tuple_set_string(&tpl, "attrname", attrname, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        ret = RDB_tuple_set_int(&tpl, "i_fno", i, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        ret = RDB_insert(dbrootp->table_attr_tbp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
    }
    RDB_destroy_obj(&tpl, ecp);

    if (tbp->val.tb.default_tplp != NULL) {
        if (insert_defvals(tbp, dbrootp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }

    /*
     * Insert keys into sys_keys
     */
    for (i = 0; i < tbp->val.tb.keyc; i++) {
        if (insert_key(&tbp->val.tb.keyv[i], i, RDB_table_name(tbp), dbrootp,
                ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
insert_vtable(RDB_object *tbp, RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_object defval;

    RDB_init_obj(&tpl);
    RDB_init_obj(&defval);

    /* Insert entry into sys_vtables */

    ret = RDB_tuple_set_string(&tpl, "tablename", RDB_table_name(tbp), ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_bool(&tpl, "is_user", RDB_TRUE, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_expr_to_binobj(&defval, RDB_vtable_expr(tbp), ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "i_def", &defval, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    if (RDB_env_trace(dbrootp->envp) > 0) {
        fputs("Writing virtual table definition:\n", stderr);
        RDB_dump(defval.val.bin.datap, RDB_binary_length(&defval), stderr);
    }

    ret = RDB_insert(dbrootp->vtables_tbp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR) {
            RDB_clear_err(ecp);
            RDB_raise_element_exists("table already exists", ecp);
        }
        goto cleanup;
    }

    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&defval, ecp);

    return ret;
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

    RDB_init_obj(&tpl);
    RDB_init_obj(&attrsarr);
    RDB_init_obj(&attrtpl);

    ret = RDB_tuple_set_string(&tpl, "name", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "tablename", tbname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_set_array_length(&attrsarr, (RDB_int) attrc, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    for (i = 0; i < attrc; i++) {
        ret = RDB_tuple_set_string(&attrtpl, "name", attrv[i].attrname, ecp);
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
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&attrsarr, ecp);
    RDB_destroy_obj(&attrtpl, ecp);
    return ret;
}

int
RDB_cat_insert_table_recmap(RDB_object *tbp, const char *rmname,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;

    RDB_init_obj(&tpl);

    ret = RDB_tuple_set_string(&tpl, "tablename", RDB_table_name(tbp), ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "recmap", rmname, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->table_recmap_tbp, &tpl, ecp, txp);

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

int
RDB_cat_index_tablename(const char *name, char **tbnamep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_object *vtbp;
    RDB_expression *argp;
    RDB_expression *exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        return RDB_ERROR;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->indexes_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("name", ecp), RDB_string_to_expr(name, ecp),
            ecp);
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

    RDB_init_obj(&tpl);
    ret = RDB_extract_tuple(vtbp, ecp, txp, &tpl);
    RDB_drop_table(vtbp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    *tbnamep = RDB_dup_str(RDB_tuple_get_string(&tpl, "tablename"));
    RDB_destroy_obj(&tpl, ecp);
    if (*tbnamep == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_cat_delete_index(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *wherep = RDB_eq(RDB_var_ref("name", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (wherep == NULL) {
        return RDB_ERROR;
    }

    ret = RDB_delete(txp->dbp->dbrootp->indexes_tbp, wherep, ecp, txp);
    RDB_del_expr(wherep, ecp);
    if (ret == RDB_ERROR)
        return RDB_ERROR;
    return RDB_OK;
}

int
RDB_cat_insert(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    /*
     * Create table in the catalog.
     */
    if (tbp->val.tb.exp == NULL) {
        ret = insert_rtable(tbp, txp->dbp->dbrootp, ecp, txp);
        /* If the table already exists in the catalog, proceed */
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_ELEMENT_EXISTS_ERROR) {
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        }

        /*
         * If it's a system table, insert indexes into the catalog.
         * For user tables, the indexes are inserted when the recmap is created.
         */
        if (ret == RDB_OK) {
            if (!tbp->val.tb.is_user) {
                int i;

                for (i = 0; i < tbp->val.tb.stp->indexc; i++) {
                    ret = RDB_cat_insert_index(tbp->val.tb.stp->indexv[i].name,
                            tbp->val.tb.stp->indexv[i].attrc,
                            tbp->val.tb.stp->indexv[i].attrv,
                            tbp->val.tb.stp->indexv[i].unique,
                            tbp->val.tb.stp->indexv[i].ordered,
                            RDB_table_name(tbp), ecp, txp);
                    if (ret != RDB_OK)
                        return ret;
                }
            }
        }
    } else {
        ret = insert_vtable(tbp, txp->dbp->dbrootp, ecp, txp);
        /* If the table already exists in the catalog, proceed */
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_ELEMENT_EXISTS_ERROR) {
                return RDB_ERROR;
            }
            RDB_clear_err(ecp);
        }
    }

    /*
     * Add the table into the database by inserting the table/DB pair
     * into sys_dbtables.
     */
    return dbtables_insert(tbp, ecp, txp);
}

/* Delete a real table from the catalog */
static int
delete_rtable(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exprp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(RDB_table_name(tbp), ecp), ecp);
    if (exprp == NULL) {
        return RDB_ERROR;
    }
    ret = RDB_delete(txp->dbp->dbrootp->rtables_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_recmap_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_attr_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_attr_defvals_tbp, exprp, ecp,
            txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->keys_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->indexes_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->dbtables_tbp, exprp, ecp, txp);

cleanup:
    RDB_del_expr(exprp, ecp);
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

/* Delete a virtual table from the catalog */
static int
delete_vtable(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exprp = RDB_eq(RDB_var_ref("tablename", ecp),
                   RDB_string_to_expr(RDB_table_name(tbp), ecp), ecp);
    if (exprp == NULL) {
        return RDB_ERROR;
    }
    ret = RDB_delete(txp->dbp->dbrootp->vtables_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->dbtables_tbp, exprp, ecp, txp);

cleanup:
    RDB_del_expr(exprp, ecp);

    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

/**
 * Delete table *tbp from the catalog.
 */
int
RDB_cat_delete(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (tbp->val.tb.exp == NULL)
        return delete_rtable(tbp, ecp, txp);
    else
        return delete_vtable(tbp, ecp, txp);
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
    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(tablename, ecp), ecp);
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

            idxname = RDB_tuple_get_string(tplp, "name");
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
                        attrtplp, "name"));
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

/*
 * Create or open a system table depending on the create argument
 */
static int
provide_systable(const char *name, int attrc, RDB_attr heading[],
           int keyc, RDB_string_vec keyv[], RDB_bool create,
           RDB_exec_context *ecp, RDB_transaction *txp, RDB_environment *envp,
           RDB_object **tbpp)
{
    int ret;
    RDB_type *tbtyp = RDB_new_relation_type(attrc, heading, ecp);
    if (tbtyp == NULL) {
        return RDB_ERROR;
    }

    *tbpp = RDB_new_rtable(name, RDB_TRUE, tbtyp, keyc, keyv, 0, NULL,
            RDB_FALSE, ecp);
    if (*tbpp == NULL) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    if (create) {
        ret = RDB_create_stored_table(*tbpp, txp->envp,
                NULL, ecp, txp);
    } else {
        ret = RDB_open_stored_table(*tbpp, txp->envp, name, -1, NULL,
                ecp, txp);
    }
    if (ret != RDB_OK) {
        RDB_free_obj(*tbpp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
insert_version_info(RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_int(&tpl, "major_version", MAJOR_VERSION, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_int(&tpl, "minor_version", MINOR_VERSION, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_int(&tpl, "micro_version", 0, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    /* bypass checks */
    ret = RDB_insert_real(dbrootp->version_info_tbp, &tpl, ecp, txp);

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static int
check_version_info(RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;

    RDB_init_obj(&tpl);
    ret = RDB_extract_tuple(dbrootp->version_info_tbp, ecp, txp, &tpl);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            RDB_raise_version_mismatch(ecp);
            ret = RDB_ERROR;
        }
        goto cleanup;
    }

    if (RDB_tuple_get_int(&tpl, "major_version") != MAJOR_VERSION
            || RDB_tuple_get_int(&tpl, "minor_version") != MINOR_VERSION) {
        RDB_raise_version_mismatch(ecp);
        ret = RDB_ERROR;
    } else {
        ret = RDB_OK;
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static int
open_indexes(RDB_object *tbp, RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_tbindex *indexv;
    int indexc = RDB_cat_get_indexes(RDB_table_name(tbp), dbrootp, ecp, txp, &indexv);
    if (indexc < 0)
        return indexc;

    tbp->val.tb.stp->indexc = indexc;
    tbp->val.tb.stp->indexv = indexv;

    /* Open secondary indexes */
    for (i = 0; i < indexc; i++) {
        char *p = strchr(indexv[i].name, '$');
        if (p == NULL || strcmp (p, "$0") != 0) {
            ret = RDB_open_table_index(tbp, &indexv[i], dbrootp->envp, ecp,
                    txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else {
            indexv[i].idxp = NULL;
        }
    }

    return RDB_OK;
}

/*
 * Create or open system tables.
 * If the tables are created, associate them with the database pointed to by
 * txp->dbp.
 */
int
RDB_open_systables(RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_attr keysattr;
    RDB_type *typ;
    RDB_bool create = RDB_FALSE;

    /* Create or open catalog tables */

    for(;;) {
        ret = provide_systable("sys_tableattrs", 4, table_attr_attrv,
                1, table_attr_keyv, create, ecp, txp, dbrootp->envp,
                &dbrootp->table_attr_tbp);
        if (!create && ret != RDB_OK
                && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            /* Table not found, so tables must be created */
            create = RDB_TRUE;
            RDB_clear_err(ecp);
        } else {
            break;
        }
    }       
    if (ret != RDB_OK) {
        return ret;
    }
        
    ret = provide_systable("sys_tableattr_defvals",
            3, table_attr_defvals_attrv, 1, table_attr_defvals_keyv,
            create, ecp, txp, dbrootp->envp, &dbrootp->table_attr_defvals_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = provide_systable("sys_rtables", 2, rtables_attrv,
            1, rtables_keyv, create, ecp,
            txp, dbrootp->envp, &dbrootp->rtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = provide_systable("sys_vtables", 3, vtables_attrv,
            1, vtables_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->vtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = provide_systable("sys_table_recmap", 2, table_recmap_attrv,
            2, table_recmap_keyv, create, ecp,
            txp, dbrootp->envp, &dbrootp->table_recmap_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = provide_systable("sys_dbtables", 2, dbtables_attrv, 1, dbtables_keyv,
            create, ecp, txp, dbrootp->envp, &dbrootp->dbtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    keysattr.name = "key";
    keysattr.typ = &RDB_STRING;
    keys_attrv[2].typ = RDB_new_relation_type(1, &keysattr, ecp);
    if (keys_attrv[2].typ == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = provide_systable("sys_keys", 3, keys_attrv, 1, keys_keyv,
            create, ecp, txp, dbrootp->envp, &dbrootp->keys_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = provide_systable("sys_types", 5, types_attrv, 1, types_keyv,
            create, ecp, txp, dbrootp->envp, &dbrootp->types_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = provide_systable("sys_possrepcomps", 5, possrepcomps_attrv,
            2, possrepcomps_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->possrepcomps_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ro_ops_attrv[1].typ = RDB_new_array_type(&RDB_BINARY, ecp);
    if (ro_ops_attrv[1].typ == NULL) {
        return RDB_ERROR;
    }

    ret = provide_systable("sys_ro_ops", 6, ro_ops_attrv,
            1, ro_ops_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->ro_ops_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    upd_ops_attrv[1].typ = RDB_new_array_type(&RDB_BINARY, ecp);
    if (upd_ops_attrv[1].typ == NULL) {
        return RDB_ERROR;
    }
    upd_ops_attrv[5].typ = RDB_new_array_type(&RDB_BOOLEAN, ecp);
    if (upd_ops_attrv[5].typ == NULL) {
        return RDB_ERROR;
    }

    ret = provide_systable("sys_upd_ops", 6, upd_ops_attrv,
            1, upd_ops_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->upd_ops_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = provide_systable("sys_version_info", 3, version_info_attrv,
            1, version_info_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->version_info_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    if (create) {
        /* Insert version info into catalog */
        ret = insert_version_info(dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
    } else {
        /* Check if catalog version matches software version */
        ret = check_version_info(dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
    }

    ret = provide_systable("sys_constraints", 2, constraints_attrv,
            1, constraints_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->constraints_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    typ = RDB_new_tuple_type(2, indexes_attrs_attrv, ecp);
    if (typ == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    indexes_attrv[2].typ = RDB_new_array_type(typ, ecp);
    if (indexes_attrv[2].typ == NULL) {
        return RDB_ERROR;
    }

    ret = provide_systable("sys_indexes", 5, indexes_attrv,
            1, indexes_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->indexes_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    if (!create && (dbrootp->rtables_tbp->val.tb.stp->indexc == -1)) {
        /*
         * Read indexes from the catalog 
         */

        /* Open indexes of sys_indexes first, because they are used often */
        ret = open_indexes(dbrootp->indexes_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->rtables_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->table_attr_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->table_attr_defvals_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->vtables_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->table_recmap_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->dbtables_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->keys_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->types_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->possrepcomps_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->ro_ops_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->upd_ops_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->constraints_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = open_indexes(dbrootp->version_info_tbp, dbrootp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}

int
RDB_cat_create_db(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    ret = RDB_cat_insert(txp->dbp->dbrootp->table_attr_tbp, ecp, txp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_ELEMENT_EXISTS_ERROR) {
            RDB_clear_err(ecp);
            /*
             * The catalog table already exists, but not in this database,
             * so associate it with this database too
             */
            ret = dbtables_insert(txp->dbp->dbrootp->table_attr_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->rtables_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->vtables_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->table_recmap_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->dbtables_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->keys_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->types_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->possrepcomps_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->ro_ops_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->upd_ops_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->indexes_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;

            ret = dbtables_insert(txp->dbp->dbrootp->constraints_tbp, ecp, txp);
            if (ret != RDB_OK) 
                return ret;

            ret = dbtables_insert(txp->dbp->dbrootp->version_info_tbp, ecp, txp);
        }
        return ret;
    }

    /*
     * Inserting sys_tableattrs into the catalog was successful,
     * this indicates that the other system tables must be inserted there too
     */

    ret = RDB_cat_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->rtables_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->vtables_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->table_recmap_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->dbtables_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->keys_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->types_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->possrepcomps_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->ro_ops_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->upd_ops_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->indexes_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->constraints_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_cat_insert(txp->dbp->dbrootp->version_info_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    return ret;
}

static int
get_key(RDB_object *tbp, RDB_string_vec *keyp, RDB_exec_context *ecp)
{
    int ret;
    int i;
    RDB_object attrarr;
    RDB_object *tplp;

    RDB_init_obj(&attrarr);
    ret = RDB_table_to_array(&attrarr, tbp, 0, NULL, 0, ecp, NULL);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&attrarr, ecp);
        return ret;
    }
    keyp->strc = RDB_array_length(&attrarr, ecp);
    if (keyp->strc < 0) {
        RDB_destroy_obj(&attrarr, ecp);
        ret = keyp->strc;
        return ret;
    }
    keyp->strv = RDB_alloc(sizeof (char *) * keyp->strc, ecp);
    if (keyp->strv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    for (i = 0; i < keyp->strc; i++)
        keyp->strv[i] = NULL;

    for (i = 0; i < keyp->strc; i++) {
        tplp = RDB_array_get(&attrarr, i, ecp);
        if (tplp == NULL) {
            RDB_destroy_obj(&attrarr, ecp);
            goto error;
        }
        keyp->strv[i] = RDB_dup_str(RDB_tuple_get_string(tplp, "key"));
        if (keyp->strv[i] == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }

    RDB_destroy_obj(&attrarr, ecp);
    return RDB_OK;

error:
    if (keyp->strv != NULL) {
        for (i = 0; i < keyp->strc; i++)
            RDB_free(keyp->strv[i]);
        RDB_free(keyp->strv);
    }
    RDB_destroy_obj(&attrarr, ecp);
    return RDB_ERROR;
}

static int
get_keys(const char *name, RDB_exec_context *ecp, RDB_transaction *txp,
         int *keycp, RDB_string_vec **keyvp)
{
    RDB_expression *exp, *argp;
    RDB_object *vtbp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    *keyvp = NULL;

    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        return RDB_ERROR;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->keys_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_string_to_expr(name, ecp),
            RDB_var_ref("tablename", ecp), ecp);
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
        goto error;

    ret = RDB_array_length(&arr, ecp);
    if (ret < 0)
        goto error;
    *keycp = ret;

    *keyvp = RDB_alloc(sizeof(RDB_string_vec) * *keycp, ecp);
    if (*keyvp == NULL) {
        goto error;
    }
    for (i = 0; i < *keycp; i++)
        (*keyvp)[i].strv = NULL;

    for (i = 0; i < *keycp; i++) {
        RDB_int kno;
        tplp = RDB_array_get(&arr, i, ecp);
        if (tplp == NULL) {
            goto error;
        }
        kno = RDB_tuple_get_int(tplp, "keyno");

        ret = get_key(RDB_tuple_get(tplp, "attrs"), &(*keyvp)[kno], ecp);
        if (ret != RDB_OK) {
            goto error;
        }
    }
    ret = RDB_destroy_obj(&arr, ecp);
    RDB_drop_table(vtbp, ecp, txp);

    return ret;

error:
    RDB_destroy_obj(&arr, ecp);
    RDB_drop_table(vtbp, ecp, txp);
    if (*keyvp != NULL) {
        int i, j;
    
        for (i = 0; i < *keycp; i++) {
            if ((*keyvp)[i].strv != NULL) {
                for (j = 0; j < (*keyvp)[i].strc; j++)
                    RDB_free((*keyvp)[i].strv[j]);
            }
        }
        RDB_free(*keyvp);
    }
    return RDB_ERROR;
}

RDB_object *
RDB_cat_get_rtable(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp, *argp;
    RDB_object *tbp = NULL;
    RDB_object *tmptb1p = NULL;
    RDB_object *tmptb2p = NULL;
    RDB_object *tmptb3p = NULL;
    RDB_object *tmptb4p = NULL;
    RDB_object arr;
    RDB_object tpl;
    RDB_object *tplp;
    RDB_bool usr;
    int ret;
    RDB_int i;
    int attrc;
    RDB_attr *attrv = NULL;
    int defvalc;
    int keyc;
    RDB_string_vec *keyv;
    RDB_type *tbtyp;
    int indexc;
    RDB_tbindex *indexv;
    char *recmapname = NULL;

    /* Read real table data from the catalog */

    RDB_init_obj(&arr);
    RDB_init_obj(&tpl);

    /* !! Should check if table is from txp->dbp ... */

    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        goto error;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->rtables_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }

    /* Set transformed flags to avoid infinite recursion */

    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    exp->transformed = RDB_TRUE;

    tmptb1p = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptb1p == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }

    ret = RDB_extract_tuple(tmptb1p, ecp, txp, &tpl);
    if (ret != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "is_user");

    /*
     * Read attribute names and types
     */

    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        goto error;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->table_attr_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);

    tmptb2p = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptb2p == NULL)
        goto error;
    ret = RDB_table_to_array(&arr, tmptb2p, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    attrc = RDB_array_length(&arr, ecp);
    if (attrc > 0) {
        attrv = RDB_alloc(sizeof(RDB_attr) * attrc, ecp);
        if (attrv == NULL) {
            goto error;
        }
    }
    for (i = 0; i < attrc; i++)
        attrv[i].name = NULL;

    for (i = 0; i < attrc; i++) {
        RDB_object *typedatap;
        RDB_int fno;

        tplp = RDB_array_get(&arr, i, ecp);
        if (tplp == NULL)
            goto error;
        fno = RDB_tuple_get_int(tplp, "i_fno");
        attrv[fno].name = RDB_dup_str(RDB_tuple_get_string(tplp, "attrname"));
        if (attrv[fno].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        typedatap = RDB_tuple_get(tplp, "type");

        attrv[fno].typ = RDB_binobj_to_type(typedatap, ecp, txp);
        if (attrv[fno].typ == NULL)
            goto error;
        
        attrv[fno].defaultp = NULL;
        attrv[fno].options = 0;
    }

    /*
     * Read default values
     */

    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        goto error;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->table_attr_defvals_tbp,
            ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);

    tmptb3p = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptb3p == NULL)
        goto error;
    ret = RDB_table_to_array(&arr, tmptb3p, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    defvalc = RDB_array_length(&arr, ecp);

    for (i = 0; i < defvalc; i++) {
        char *name;
        RDB_object *binvalp;

        tplp = RDB_array_get(&arr, i, ecp);
        if (tplp == NULL)
            goto error;
        name = RDB_tuple_get_string(tplp, "attrname");
        binvalp = RDB_tuple_get(tplp, "default_value");
        
        /* Find attrv entry and set default value */
        for (i = 0; i < attrc && (strcmp(attrv[i].name, name) != 0); i++);
        if (i >= attrc) {
            /* Not found */
            RDB_raise_internal(
                    "attribute not found while setting default value", ecp);
            goto error;
        }
        attrv[i].defaultp = RDB_alloc(sizeof (RDB_object), ecp);
        if (attrv[i].defaultp == NULL) {
            goto error;
        }
        RDB_init_obj(attrv[i].defaultp);
        ret = RDB_irep_to_obj(attrv[i].defaultp, attrv[i].typ,
                binvalp->val.bin.datap, RDB_binary_length(binvalp), ecp);
        if (ret != RDB_OK)
            goto error;            
    }

    ret = get_keys(name, ecp, txp, &keyc, &keyv);
    if (ret != RDB_OK)
        goto error;

    if (usr) {
        /*
         * Read recmap name from catalog, if it's user table.
         * For system tables, the recmap name is the table name.
         */
        exp = RDB_ro_op("where", ecp);
        if (exp == NULL) {
            goto error;
        }
        argp = RDB_table_ref(txp->dbp->dbrootp->table_recmap_tbp, ecp);
        if (argp == NULL) {
            RDB_del_expr(exp, ecp);
            goto error;
        }
        RDB_add_arg(exp, argp);
        argp = RDB_eq(RDB_var_ref("tablename", ecp),
                RDB_string_to_expr(name, ecp), ecp);
        if (argp == NULL) {
            RDB_del_expr(exp, ecp);
            goto error;
        }
        RDB_add_arg(exp, argp);

        tmptb4p = RDB_expr_to_vtable(exp, ecp, txp);
        if (tmptb4p == NULL) {
            RDB_del_expr(exp, ecp);
            goto error;
        }
        ret = RDB_extract_tuple(tmptb4p, ecp, txp, &tpl);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
                goto error;
            }
            RDB_clear_err(ecp);
        }
        if (ret == RDB_OK) {
            recmapname = RDB_tuple_get_string(&tpl, "recmap");
        }
    }

    tbtyp = RDB_new_relation_type(attrc, attrv, ecp);
    if (tbtyp == NULL) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        goto error;
    }

    tbp = RDB_new_rtable(name, RDB_TRUE, tbtyp, keyc, keyv, 0, NULL, usr, ecp);
    RDB_free_keys(keyc, keyv);
    if (tbp == NULL) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        goto error;
    }

    if (RDB_set_defvals(tbp, attrc, attrv, ecp) != RDB_OK) {
        goto error;
    }

    indexc = RDB_cat_get_indexes(name, txp->dbp->dbrootp, ecp, txp, &indexv);
    if (indexc < 0) {
        ret = indexc;
        goto error;
    }

    if (!usr) {
        recmapname = RDB_table_name(tbp);
    }
    if (recmapname != NULL) {
        ret = RDB_open_stored_table(tbp, txp->envp, recmapname,
                indexc, indexv, ecp, txp);
        if (ret != RDB_OK) {
            goto error;
        }
    }
    for (i = 0; i < attrc; i++) {
        RDB_free(attrv[i].name);
        if (attrv[i].defaultp != NULL) {
            RDB_destroy_obj(attrv[i].defaultp, ecp);
            RDB_free(attrv[i].defaultp);
        }
    }
    if (attrc > 0)
        RDB_free(attrv);

    if (RDB_assoc_table_db(tbp, txp->dbp, ecp) != RDB_OK)
        goto error;

    ret = RDB_destroy_obj(&arr, ecp);

    RDB_drop_table(tmptb1p, ecp, txp);
    RDB_drop_table(tmptb2p, ecp, txp);
    RDB_drop_table(tmptb3p, ecp, txp);
    if (tmptb4p != NULL)
        RDB_drop_table(tmptb4p, ecp, txp);

    RDB_destroy_obj(&tpl, ecp);

    return tbp;

error:
    if (attrv != NULL) {
        for (i = 0; i < attrc; i++)
            RDB_free(attrv[i].name);
        RDB_free(attrv);
    }

    RDB_destroy_obj(&arr, ecp);

    if (tmptb1p != NULL)
        RDB_drop_table(tmptb1p, ecp, txp);
    if (tmptb2p != NULL)
        RDB_drop_table(tmptb2p, ecp, txp);
    if (tmptb3p != NULL)
        RDB_drop_table(tmptb3p, ecp, txp);
    if (tmptb4p != NULL)
        RDB_drop_table(tmptb4p, ecp, txp);

    RDB_destroy_obj(&tpl, ecp);

    if (tbp != NULL)
        RDB_free_obj(tbp, ecp);

    return NULL;
} /* RDB_cat_get_rtable */

static RDB_object *
RDB_binobj_to_vtable(RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp, *texp;
    int pos = 0;
    int ret = RDB_deserialize_expr(valp, &pos, ecp, txp, &exp);
    if (ret != RDB_OK)
        return NULL;

    /*
     * Resolve table names so the underlying real table(s) can be
     * accessed without having to resolve a variable
     */
    texp = RDB_expr_resolve_varnames(exp, NULL, NULL, ecp, txp);
    RDB_del_expr(exp, ecp);
    if (texp == NULL)
        return NULL;

    return RDB_expr_to_vtable(texp, ecp, txp);
}

RDB_object *
RDB_cat_get_vtable(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *tbp;
    RDB_expression *exp, *argp;
    RDB_object *tmptbp = NULL;
    RDB_object tpl;
    RDB_object arr;
    RDB_object *valp;
    RDB_bool usr;
    int ret;

    /*
     * Read virtual table data from the catalog
     */

    RDB_init_obj(&arr);
    RDB_init_obj(&tpl);

    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        goto error;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->vtables_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    /* Set transformed flags to avoid infinite recursion */

    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    exp->transformed = RDB_TRUE;
    tmptbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptbp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    
    ret = RDB_extract_tuple(tmptbp, ecp, txp, &tpl);
    RDB_drop_table(tmptbp, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "is_user");

    valp = RDB_tuple_get(&tpl, "i_def");

    tbp = RDB_binobj_to_vtable(valp, ecp, txp);
    if (tbp == NULL)
        goto error;

    RDB_destroy_obj(&tpl, ecp);
    ret = RDB_destroy_obj(&arr, ecp);
    if (ret != RDB_OK)
        goto error;

    tbp->val.tb.is_persistent = RDB_TRUE;
    tbp->val.tb.is_user = usr;

    tbp->val.tb.name = RDB_dup_str(name);
    if (tbp->val.tb.name == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    if (RDB_assoc_table_db(tbp, txp->dbp, ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(RDB_db_env(RDB_tx_db(txp))) > 0) {
        fprintf(stderr,
                "Definition of virtual table %s read from the catalog\n",
                name);
    }
    return tbp;

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&arr, ecp);
    
    return NULL;
} /* RDB_cat_get_vtable */

int
RDB_cat_rename_table(RDB_object *tbp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_attr_update upd;
    RDB_expression *condp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(RDB_table_name(tbp), ecp), ecp);
    if (condp == NULL) {
        return RDB_ERROR;
    }

    upd.name = "tablename";
    upd.exp = RDB_string_to_expr(name, ecp);
    if (upd.exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (tbp->val.tb.exp == NULL) {
        ret = RDB_update(txp->dbp->dbrootp->rtables_tbp, condp, 1, &upd, ecp, txp);
        if (ret == RDB_ERROR)
            goto cleanup;
        ret = RDB_update(txp->dbp->dbrootp->table_attr_tbp, condp, 1, &upd, ecp, txp);
        if (ret == RDB_ERROR)
            goto cleanup;
        ret = RDB_update(txp->dbp->dbrootp->table_attr_defvals_tbp, condp,
                1, &upd, ecp, txp);
        if (ret == RDB_ERROR)
            goto cleanup;
        ret = RDB_update(txp->dbp->dbrootp->keys_tbp, condp, 1, &upd, ecp, txp);
        if (ret == RDB_ERROR)
            goto cleanup;
        ret = RDB_update(txp->dbp->dbrootp->indexes_tbp, condp, 1, &upd, ecp, txp);
        if (ret == RDB_ERROR)
            goto cleanup;
    } else {
        ret = RDB_update(txp->dbp->dbrootp->vtables_tbp, condp, 1, &upd, ecp, txp);
        if (ret == RDB_ERROR)
            goto cleanup;
    }
    ret = RDB_update(txp->dbp->dbrootp->table_recmap_tbp, condp, 1, &upd, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;
    ret = RDB_update(txp->dbp->dbrootp->dbtables_tbp, condp, 1, &upd, ecp, txp);

cleanup:
    RDB_del_expr(condp, ecp);
    if (upd.exp != NULL)
        RDB_del_expr(upd.exp, ecp);
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

int
RDB_cat_create_constraint(const char *name, RDB_expression *exp,
                      RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_object exprval;

    RDB_init_obj(&tpl);
    RDB_init_obj(&exprval);

    /*
     * Insert data into sys_constraints
     */

    ret = RDB_tuple_set_string(&tpl, "constraintname", name, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_expr_to_binobj(&exprval, exp, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "i_expr", &exprval, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(RDB_tx_db(txp)->dbrootp->constraints_tbp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR) {
            RDB_clear_err(ecp);
            RDB_raise_element_exists("constraint already exists", ecp);
        }
        ret = RDB_ERROR;
        goto cleanup;
    }

    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&exprval, ecp);

    return ret;
}

/*
 * Catalog version. A newer version can open db environments from
 * old versions only if the catalog version is the same.
 */

int
RDB_cat_major_version(void)
{
    return MAJOR_VERSION;
}

int
RDB_cat_minor_version(void)
{
    return MINOR_VERSION;
}
