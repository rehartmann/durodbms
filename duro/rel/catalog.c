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
    MINOR_VERSION = 15
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

        ret = RDB_insert(keystbp, &ktpl, ecp, txp);
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
        ret = _RDB_type_to_binobj(&typedata, tuptyp->def.tuple.attrv[i].typ,
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

    /* insert entries into table sys_tableattr_defvals */
    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "tablename", RDB_table_name(tbp), ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        if (tuptyp->def.tuple.attrv[i].defaultp != NULL) {
            char *attrname = tuptyp->def.tuple.attrv[i].name;
            RDB_object binval;
            void *datap;
            size_t len;

            if (!RDB_type_equals(tuptyp->def.tuple.attrv[i].defaultp->typ,
                    tuptyp->def.tuple.attrv[i].typ)) {
                RDB_raise_type_mismatch(
                        "Type of default value does not match attribute type",
                        ecp);
                return RDB_ERROR;
            }
            
            ret = RDB_tuple_set_string(&tpl, "attrname", attrname, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }

            RDB_init_obj(&binval);
            datap = RDB_obj_irep(tuptyp->def.tuple.attrv[i].defaultp, &len);
            ret = RDB_binary_set(&binval, 0, datap, len, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_destroy_obj(&binval, ecp);
                return RDB_ERROR;
            }

            ret = RDB_tuple_set(&tpl, "default_value", &binval, ecp);
            RDB_destroy_obj(&binval, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }

            ret = RDB_insert(dbrootp->table_attr_defvals_tbp, &tpl, ecp, txp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
        }
    }
    RDB_destroy_obj(&tpl, ecp);

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

    ret = _RDB_expr_to_binobj(&defval, tbp->val.tb.exp, ecp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "i_def", &defval, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    if (RDB_env_trace(dbrootp->envp) > 0) {
        fputs("Writing virtual table definition:\n", stderr);
        _RDB_dump(defval.val.bin.datap, RDB_binary_length(&defval), stderr);
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
_RDB_cat_insert_index(const char *name, int attrc, const RDB_seq_item attrv[],
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
_RDB_cat_insert_table_recmap(RDB_object *tbp, const char *rmname,
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
_RDB_cat_index_tablename(const char *name, char **tbnamep,
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
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("name", ecp), RDB_string_to_expr(name, ecp),
            ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
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
_RDB_cat_delete_index(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *wherep = RDB_eq(RDB_var_ref("name", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (wherep == NULL) {
        return RDB_ERROR;
    }

    ret = RDB_delete(txp->dbp->dbrootp->indexes_tbp, wherep, ecp, txp);
    RDB_drop_expr(wherep, ecp);
    if (ret == RDB_ERROR)
        return RDB_ERROR;
    return RDB_OK;
}

int
_RDB_cat_insert(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
                    ret = _RDB_cat_insert_index(tbp->val.tb.stp->indexv[i].name,
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
    RDB_drop_expr(exprp, ecp);
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
    RDB_drop_expr(exprp, ecp);

    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

int
_RDB_cat_delete(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
_RDB_cat_get_indexes(const char *tablename, RDB_dbroot *dbrootp,
        RDB_exec_context *ecp, RDB_transaction *txp, struct _RDB_tbindex **indexvp)
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
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(tablename, ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&arr);
    ret = RDB_table_to_array(&arr, vtbp, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    indexc = RDB_array_length(&arr, ecp);
    if (indexc > 0) {
        (*indexvp) = RDB_alloc(sizeof(_RDB_tbindex) * indexc, ecp);
        if (*indexvp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        for (i = 0; i < indexc; i++) {
            RDB_object *tplp;
            RDB_object *attrarrp;
            char *idxname;
            _RDB_tbindex *indexp = &(*indexvp)[i];

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

    *tbpp = _RDB_new_rtable(name, RDB_TRUE, tbtyp, keyc, keyv, RDB_FALSE, ecp);
    if (*tbpp == NULL) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    if (create) {
        ret = _RDB_create_stored_table(*tbpp, txp != NULL ? txp->envp : NULL,
                NULL, ecp, txp);
    } else {
        ret = _RDB_open_stored_table(*tbpp, txp != NULL ? txp->envp : NULL,
                name, -1, NULL, ecp, txp);
    }
    if (ret != RDB_OK) {
        _RDB_free_obj(*tbpp, ecp);
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
    ret = _RDB_insert_real(dbrootp->version_info_tbp, &tpl, ecp, txp);

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
    _RDB_tbindex *indexv;
    int indexc = _RDB_cat_get_indexes(RDB_table_name(tbp), dbrootp, ecp, txp, &indexv);
    if (indexc < 0)
        return indexc;

    tbp->val.tb.stp->indexc = indexc;
    tbp->val.tb.stp->indexv = indexv;

    /* Open secondary indexes */
    for (i = 0; i < indexc; i++) {
        char *p = strchr(indexv[i].name, '$');
        if (p == NULL || strcmp (p, "$0") != 0) {
            ret = _RDB_open_table_index(tbp, &indexv[i], dbrootp->envp, ecp,
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
_RDB_open_systables(RDB_dbroot *dbrootp, RDB_exec_context *ecp,
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
_RDB_cat_create_db(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    ret = _RDB_cat_insert(txp->dbp->dbrootp->table_attr_tbp, ecp, txp);
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

    ret = _RDB_cat_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->rtables_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->vtables_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->table_recmap_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->dbtables_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->keys_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->types_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->possrepcomps_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->ro_ops_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->upd_ops_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->indexes_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->constraints_tbp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->version_info_tbp, ecp, txp);
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
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_string_to_expr(name, ecp),
            RDB_var_ref("tablename", ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    vtbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (vtbp == NULL) {
        RDB_drop_expr(exp, ecp);
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
_RDB_cat_get_rtable(const char *name, RDB_exec_context *ecp,
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
    _RDB_tbindex *indexv;
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
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }

    /* Set transformed flags to avoid infinite recursion */

    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    exp->transformed = RDB_TRUE;

    tmptb1p = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptb1p == NULL) {
        RDB_drop_expr(exp, ecp);
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
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
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

        attrv[fno].typ = _RDB_binobj_to_type(typedatap, ecp, txp);
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
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
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
            RDB_drop_expr(exp, ecp);
            goto error;
        }
        RDB_add_arg(exp, argp);
        argp = RDB_eq(RDB_var_ref("tablename", ecp),
                RDB_string_to_expr(name, ecp), ecp);
        if (argp == NULL) {
            RDB_drop_expr(exp, ecp);
            goto error;
        }
        RDB_add_arg(exp, argp);

        tmptb4p = RDB_expr_to_vtable(exp, ecp, txp);
        if (tmptb4p == NULL) {
            RDB_drop_expr(exp, ecp);
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
    if (_RDB_set_defvals(tbtyp, attrc, attrv, ecp) != RDB_OK) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        return NULL;
    }

    tbp = _RDB_new_rtable(name, RDB_TRUE, tbtyp, keyc, keyv, usr, ecp);
    _RDB_free_keys(keyc, keyv);
    if (tbp == NULL) {
        RDB_del_nonscalar_type(tbtyp, ecp);
        goto error;
    }

    indexc = _RDB_cat_get_indexes(name, txp->dbp->dbrootp, ecp, txp, &indexv);
    if (indexc < 0) {
        ret = indexc;
        goto error;
    }

    if (!usr) {
        recmapname = RDB_table_name(tbp);
    }
    if (recmapname != NULL) {
        ret = _RDB_open_stored_table(tbp, txp->envp, recmapname,
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

    if (_RDB_assoc_table_db(tbp, txp->dbp, ecp) != RDB_OK)
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
        _RDB_free_obj(tbp, ecp);

    return NULL;
} /* _RDB_cat_get_rtable */

RDB_object *
_RDB_cat_get_vtable(const char *name, RDB_exec_context *ecp,
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
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    /* Set transformed flags to avoid infinite recursion */

    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    argp = RDB_eq(RDB_var_ref("tablename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    exp->transformed = RDB_TRUE;
    tmptbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptbp == NULL) {
        RDB_drop_expr(exp, ecp);
        goto error;
    }
    
    ret = RDB_extract_tuple(tmptbp, ecp, txp, &tpl);
    RDB_drop_table(tmptbp, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "is_user");

    valp = RDB_tuple_get(&tpl, "i_def");

    tbp = _RDB_binobj_to_vtable(valp, ecp, txp);
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

    if (_RDB_assoc_table_db(tbp, txp->dbp, ecp) != RDB_OK)
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
} /* _RDB_cat_get_vtable */

int
_RDB_cat_rename_table(RDB_object *tbp, const char *name, RDB_exec_context *ecp,
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
    RDB_drop_expr(condp, ecp);
    if (upd.exp != NULL)
        RDB_drop_expr(upd.exp, ecp);
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

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
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("typename", ecp),
                    RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    *tbpp = RDB_expr_to_vtable(exp, ecp, txp);
    if (*tbpp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
_RDB_possreps_query(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object **tbpp)
{
    RDB_expression *exp, *argp;

    exp = RDB_ro_op("project", ecp);
    if (exp == NULL)
    	return RDB_ERROR;
    argp = RDB_table_ref(txp->dbp->dbrootp->possrepcomps_tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_string_to_expr("typename", ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_string_to_expr("possrepname", ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    argp = exp;
    exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        RDB_drop_expr(argp, ecp);        
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("typename", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
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
        RDB_drop_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_eq(RDB_var_ref("possrepname", ecp),
            RDB_string_to_expr(possrepname, ecp), ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);        
        return NULL;
    }
    RDB_add_arg(exp, argp);

    wexp = RDB_ro_op("where", ecp);
    if (wexp == NULL) {
        RDB_drop_expr(exp, ecp);        
        return NULL;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->possrepcomps_tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(wexp, ecp);
        RDB_drop_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(wexp, argp);
    RDB_add_arg(wexp, exp);

    tbp = RDB_expr_to_vtable(wexp, ecp, txp);
    if (tbp == NULL) {
        RDB_drop_expr(wexp, ecp);
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

        rep->compv[idx].typ = _RDB_binobj_to_type(
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
_RDB_cat_get_type(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_type **typp)
{
    RDB_object *tmptb1p = NULL;
    RDB_object *tmptb2p = NULL;
    RDB_object tpl;
    RDB_object *tplp;
    RDB_object possreps;
    RDB_object *cvalp;
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

    typedatap = RDB_tuple_get(&tpl, "i_arep_type");
    if (RDB_binary_length(typedatap) != 0) {   
        typ->def.scalar.arep = _RDB_binobj_to_type(typedatap, ecp, txp);
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

    cvalp = RDB_tuple_get(&tpl, "i_constraint");
    if (RDB_binary_length(cvalp) > 0) {
        typ->def.scalar.constraintp = _RDB_binobj_to_expr(cvalp, ecp, txp);
        if (typ->def.scalar.constraintp == NULL)
            goto error;
    } else {
        typ->def.scalar.constraintp = NULL;
    }

    typ->ireplen = RDB_tuple_get_int(&tpl, "i_arep_len");
    typ->def.scalar.sysimpl = RDB_tuple_get_bool(&tpl, "i_sysimpl");
    typ->def.scalar.repc = 0;
    typ->def.scalar.builtin = RDB_FALSE;

    /*
     * Get possrep info from sys_possreps
     */

    ret = _RDB_possreps_query(name, ecp, txp, &tmptb2p);
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
 * Convert tuple to operator (without return type)
 */
static RDB_operator *
tuple_to_operator(const char *name, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp) {
    int i;
    const char *libname;
    RDB_object *typobjp;
    RDB_operator *op;
    RDB_int argc;
    RDB_type **argtv = NULL;
    RDB_object *typarrp = RDB_tuple_get(tplp, "argtypes");

    argc = RDB_array_length(typarrp, ecp);
    if (argc == (RDB_int) RDB_ERROR)
        return NULL;

    /* Read types from tuple */
    if (argc > 0) {
        argtv = RDB_alloc(sizeof(RDB_type *) * argc, ecp);
        if (argtv == NULL)
            return NULL;
        for (i = 0; i < argc; i++) {
            typobjp = RDB_array_get(typarrp, i, ecp);
            if (typobjp == NULL)
                goto error;
            argtv[i] = _RDB_binobj_to_type(typobjp, ecp, txp);
            if (argtv[i] == NULL)
                goto error;
        }
    }
    op = _RDB_new_operator(name, argc, argtv, NULL, ecp);
    if (op == NULL)
        goto error;

    RDB_init_obj(&op->source);
    if (RDB_copy_obj(&op->source, RDB_tuple_get(tplp, "source"), ecp) != RDB_OK)
        goto error;

    libname = RDB_tuple_get_string(tplp, "lib");
    if (libname[0] != '\0') {
        op->modhdl = lt_dlopenext(libname);
        if (op->modhdl == NULL) {
            RDB_raise_resource_not_found(libname, ecp);
            goto error;
        }
    } else {
        op->modhdl = NULL;
    }
    return op;

error:
    if (argtv != NULL) {
        for (i = 0; i < argc; i++) {
            if (argtv[i] != NULL && !RDB_type_is_scalar(argtv[i]))
                RDB_del_nonscalar_type(argtv[i], ecp);
        }
        RDB_free(argtv);
    }
    return NULL;
} /* tuple_to_operator */

/*
 * Read all read-only operators with specified name from database.
 * Return the number of operators loaded or RDB_ERROR if an error occured.
 */
RDB_int
_RDB_cat_load_ro_op(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp, *wexp, *argp;
    RDB_qresult *qrp;
    const char *symname;
    RDB_object *vtbp;
    RDB_object tpl;
    RDB_object typesobj;
    int ret;
    RDB_operator *op;
    RDB_int opcount = 0;

    /*
     * Create virtual table sys_ro_ops WHERE name=<name>
     */
    exp = RDB_eq(RDB_var_ref("name", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (exp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_destroy_obj(&typesobj, ecp);
        return RDB_ERROR;
    }

    wexp = RDB_ro_op("where", ecp);
    if (wexp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->ro_ops_tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(wexp, ecp);
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wexp, argp);
    RDB_add_arg(wexp, exp);

    vtbp = RDB_expr_to_vtable(wexp, ecp, txp);
    if (vtbp == NULL) {
        RDB_drop_expr(wexp, ecp);
        return RDB_ERROR;
    }
    qrp = _RDB_table_qresult(vtbp, ecp, txp);
    if (qrp == NULL) {
        RDB_drop_table(vtbp, ecp, txp);
        return RDB_ERROR;
    }
    RDB_init_obj(&tpl);

    /* Read tuples and convert them to operators */
    while (_RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        op = tuple_to_operator(name, &tpl, ecp, txp);
        if (op == NULL)
            goto error;

        /* Return type */
        op->rtyp = _RDB_binobj_to_type(RDB_tuple_get(&tpl, "rtype"), ecp, txp);
        if (op->rtyp == NULL) {
            RDB_free_op_data(op, ecp);
            goto error;
        }
        symname = RDB_tuple_get_string(&tpl, "symbol");
        if (strcmp(symname, "_RDB_sys_select") == 0) {
            op->opfn.ro_fp = &_RDB_sys_select;
        } else {
            op->opfn.ro_fp = (RDB_ro_op_func *) lt_dlsym(op->modhdl, symname);
            if (op->opfn.ro_fp == NULL) {
                RDB_raise_resource_not_found(symname, ecp);
                RDB_free_op_data(op, ecp);
                goto error;
            }
        }

        if (RDB_put_op(&txp->dbp->dbrootp->ro_opmap, op, ecp) != RDB_OK) {
            RDB_free_op_data(op, ecp);
            goto error;
        }
        if (RDB_env_trace(txp->envp) > 0) {
            fputs("Read-only operator ", stderr);
            fputs(name, stderr);
            fputs(" loaded from catalog\n", stderr);
        }
        opcount++;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    ret = RDB_drop_table(vtbp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    RDB_destroy_obj(&tpl, ecp);
    return opcount;

error:
    RDB_drop_table(vtbp, ecp, txp);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_ERROR;
} /* RDB_cat_get_ro_op */

/* Read all read-only operators with specified name from database */
RDB_int
_RDB_cat_load_upd_op(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp, *wexp, *argp;
    RDB_qresult *qrp;
    const char *symname;
    RDB_object *vtbp;
    RDB_object tpl;
    RDB_object typesobj;
    RDB_object *updobjp, *updvobjp;
    int ret;
    int i;
    RDB_operator *op;
    RDB_int opcount = 0;

    /*
     * Create virtual table sys_upd_ops WHERE name=<name>
     */
    exp = RDB_eq(RDB_var_ref("name", ecp),
            RDB_string_to_expr(name, ecp), ecp);
    if (exp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_destroy_obj(&typesobj, ecp);
        return RDB_ERROR;
    }

    wexp = RDB_ro_op("where", ecp);
    if (wexp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->upd_ops_tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(wexp, ecp);
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wexp, argp);
    RDB_add_arg(wexp, exp);

    vtbp = RDB_expr_to_vtable(wexp, ecp, txp);
    if (vtbp == NULL) {
        RDB_drop_expr(wexp, ecp);
        return RDB_ERROR;
    }
    qrp = _RDB_table_qresult(vtbp, ecp, txp);
    if (qrp == NULL) {
        RDB_drop_table(vtbp, ecp, txp);
        return RDB_ERROR;
    }
    RDB_init_obj(&tpl);

    /* Read tuples and convert them to operators */
    while (_RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        op = tuple_to_operator(name, &tpl, ecp, txp);
        if (op == NULL)
            goto error;

        updvobjp = RDB_tuple_get(&tpl, "updv");
        for (i = 0; i < op->paramc; i++) {
            updobjp = RDB_array_get(updvobjp, (RDB_int) i, ecp);
            if (updobjp == NULL)
                goto error;
            op->paramv[i].update = RDB_obj_bool(updobjp);
        }

        symname = RDB_tuple_get_string(&tpl, "symbol");
        op->opfn.upd_fp = (RDB_upd_op_func *) lt_dlsym(op->modhdl, symname);
        if (op->opfn.upd_fp == NULL) {
            RDB_raise_resource_not_found(symname, ecp);
            RDB_free_op_data(op, ecp);
            goto error;
        }

        if (RDB_put_op(&txp->dbp->dbrootp->upd_opmap, op, ecp) != RDB_OK) {
            RDB_free_op_data(op, ecp);
            goto error;
        }

        if (RDB_env_trace(txp->envp) > 0) {
            fputs("Update operator ", stderr);
            fputs(name, stderr);
            fputs(" loaded from catalog\n", stderr);
        }
        opcount++;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    ret = RDB_drop_table(vtbp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    RDB_destroy_obj(&tpl, ecp);
    return opcount;

error:
    RDB_drop_table(vtbp, ecp, txp);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_ERROR;
}

int
_RDB_cat_create_constraint(const char *name, RDB_expression *exp,
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

    ret = _RDB_expr_to_binobj(&exprval, exp, ecp);
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

int
RDB_major_version(void)
{
    return MAJOR_VERSION;
}

int
RDB_minor_version(void)
{
    return MINOR_VERSION;
}
