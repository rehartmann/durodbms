/*
 * Catalog functions and definitions.
 *
 * Copyright (C) 2003-2009, 2011-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "catalog.h"
#include "typeimpl.h"
#include "insert.h"
#include "internal.h"
#include "stable.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <gen/strdump.h>
#include <gen/hashmapit.h>
#include <obj/key.h>
#include <obj/objinternal.h>

#include <string.h>

enum {
    MAJOR_VERSION = 0,
    MINOR_VERSION = 26
};

/*
 * Definitions of the catalog tables.
 */

static RDB_attr table_attr_attrv[] = {
        { "attrname", &RDB_IDENTIFIER, NULL, 0 },
        { "tablename", &RDB_STRING, NULL, 0 },
        { "type", &RDB_BINARY, NULL, 0 },
        { "i_fno", &RDB_INTEGER, NULL, 0 } };
static char *table_attr_keyattrv[] = { "attrname", "tablename" };
static RDB_string_vec table_attr_keyv[] = { { 2, table_attr_keyattrv } };

static RDB_attr table_attr_defvals_attrv[] = {
            { "attrname", &RDB_IDENTIFIER, NULL, 0 },
            { "tablename", &RDB_IDENTIFIER, NULL, 0 },
            { "default_value", &RDB_BINARY, NULL, 0 } };
static char *table_attr_defvals_keyattrv[] = { "attrname", "tablename" };
static RDB_string_vec table_attr_defvals_keyv[] = { { 2, table_attr_defvals_keyattrv } };

static RDB_attr rtables_attrv[] = {
    { "tablename", &RDB_IDENTIFIER, NULL, 0 },
    { "is_user", &RDB_BOOLEAN, NULL, 0 },
};
static char *rtables_keyattrv[] = { "tablename" };
static RDB_string_vec rtables_keyv[] = { { 1, rtables_keyattrv } };

static RDB_attr vtables_attrv[] = {
    { "tablename", &RDB_IDENTIFIER, NULL, 0 },
    { "is_user", &RDB_BOOLEAN, NULL, 0 },
    { "i_def", &RDB_BINARY, NULL, 0 }
};

static char *vtables_keyattrv[] = { "tablename" };
static RDB_string_vec vtables_keyv[] = { { 1, vtables_keyattrv } };

static RDB_attr ptables_attrv[] = {
    { "tablename", &RDB_STRING, NULL, 0 },
    { "is_user", &RDB_BOOLEAN, NULL, 0 },
    { "i_def", &RDB_BINARY, NULL, 0 }
};

static char *ptables_keyattrv[] = { "tablename" };
static RDB_string_vec ptables_keyv[] = { { 1, ptables_keyattrv } };

static RDB_attr table_recmap_attrv[] = {
    { "tablename", &RDB_IDENTIFIER, NULL, 0 },
    { "recmap", &RDB_STRING, NULL, 0 },
};
static char *table_recmap_keyattrv1[] = { "tablename" };
static char *table_recmap_keyattrv2[] = { "recmap" };
static RDB_string_vec table_recmap_keyv[] = {
    { 1, table_recmap_keyattrv1 }, { 1, table_recmap_keyattrv2 } };

static RDB_attr dbtables_attrv[] = {
    { "tablename", &RDB_IDENTIFIER },
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
    { "arep_len", &RDB_INTEGER, NULL, 0 },
    { "arep_type", &RDB_BINARY, NULL, 0 },
    { "sysimpl", &RDB_BOOLEAN, NULL, 0},
    { "ordered", &RDB_BOOLEAN, NULL, 0 },
    { "constraint", &RDB_BINARY, NULL, 0 },
    { "init", &RDB_BINARY, NULL, 0 }
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
    { "opname", &RDB_STRING, NULL, 0 },
    { "argtypes", NULL, NULL, 0 }, /* type is set to array of BINARY later */
    { "lib", &RDB_STRING, NULL, 0 },
    { "symbol", &RDB_STRING, NULL, 0 },
    { "source", &RDB_STRING, NULL, 0 },
    { "rtype", &RDB_BINARY, NULL, 0 },
    { "creation_time", &RDB_DATETIME, NULL, 0 },
};

static char *ro_ops_keyattrv[] = { "opname", "argtypes" };
static RDB_string_vec ro_ops_keyv[] = { { 2, ro_ops_keyattrv } };

static RDB_attr upd_ops_attrv[] = {
    { "opname", &RDB_STRING, NULL, 0 },
    { "argtypes", NULL, NULL, 0 }, /* type is set to array of BINARY later */
    { "lib", &RDB_STRING, NULL, 0 },
    { "symbol", &RDB_STRING, NULL, 0 },
    { "source", &RDB_STRING, NULL, 0 },
    { "updv", NULL, NULL, 0 }, /* type is set to array of BOOLEAN later */
    { "creation_time", &RDB_DATETIME, NULL, 0 }
};

static char *upd_ops_keyattrv[] = { "opname", "argtypes" };
static RDB_string_vec upd_ops_keyv[] = { { 2, upd_ops_keyattrv } };

static RDB_attr indexes_attrv[] = {
    { "idxname", &RDB_STRING, NULL, 0 },
    { "tablename", &RDB_IDENTIFIER, NULL, 0 },
    { "attrs", NULL, NULL, 0 }, /* type is set to an array type later */
    { "unique", &RDB_BOOLEAN, 0 },
    { "ordered", &RDB_BOOLEAN, 0 }
};

static char *indexes_keyattrv[] = { "idxname" };
static RDB_string_vec indexes_keyv[] = { { 1, indexes_keyattrv } };

static RDB_attr indexes_attrs_attrv[] = {
    { "attrname", &RDB_IDENTIFIER, NULL, 0 },
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
string_to_id(RDB_object *id, const char *str, RDB_exec_context *ecp)
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
RDB_cat_dbtables_insert(RDB_object *tbp, RDB_database *dbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object tbnameobj;

    /* Insert (database, table) pair into sys_dbtables */
    RDB_init_obj(&tpl);
    RDB_init_obj(&tbnameobj);

    if (string_to_id(&tbnameobj, RDB_table_name(tbp), ecp) != RDB_OK)
        goto error;

    if (RDB_tuple_set(&tpl, "tablename", &tbnameobj, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_tuple_set_string(&tpl, "dbname", RDB_db_name(dbp), ecp) != RDB_OK) {
        goto error;
    }

    if (RDB_table_is_user(tbp)) {
        if (RDB_insert(txp->dbp->dbrootp->dbtables_tbp, &tpl, ecp, txp) != RDB_OK)
            goto error;
    } else {
        /*
         * If it's a system table bypass contraint checking because resolving
         * table names may not yet work
         */
        if (RDB_insert_real(txp->dbp->dbrootp->dbtables_tbp, &tpl, ecp, txp)
                != RDB_OK)
            goto error;
    }


    RDB_destroy_obj(&tbnameobj, ecp);
    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;

error:
    RDB_destroy_obj(&tbnameobj, ecp);
    RDB_destroy_obj(&tpl, ecp);

    return RDB_ERROR;
}

static int
insert_key(const RDB_string_vec *keyp, int i, const char *tablenamep,
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

    if (RDB_tuple_set_string(&tpl, "tablename", tablenamep, ecp) != RDB_OK)
        goto error;

    if (RDB_tuple_set(&tpl, "attrs", NULL, ecp) != RDB_OK)
        goto error;

    keystbp = RDB_tuple_get(&tpl, "attrs");

    keysattr.name = "keyattr";
    keysattr.typ = &RDB_IDENTIFIER;
    keysattr.defaultp = NULL;
    keysattr.options = 0;

    if (RDB_init_table(keystbp, NULL, 1, &keysattr, 0, NULL, ecp)
            != RDB_OK) {
        goto error;
    }

    for (j = 0; j < keyp->strc; j++) {
        RDB_object kaobj;
        RDB_object ktpl;

        RDB_init_obj(&ktpl);
        RDB_init_obj(&kaobj);

        if (string_to_id(&kaobj, keyp->strv[j], ecp) != RDB_OK) {
            RDB_destroy_obj(&ktpl, ecp);
            RDB_destroy_obj(&kaobj, ecp);
            goto error;
        }

        if (RDB_tuple_set(&ktpl, "keyattr", &kaobj, ecp) != RDB_OK) {
            RDB_destroy_obj(&ktpl, ecp);
            RDB_destroy_obj(&kaobj, ecp);
            goto error;
        }

        ret = RDB_insert(keystbp, &ktpl, ecp, NULL);
        RDB_destroy_obj(&kaobj, ecp);
        RDB_destroy_obj(&ktpl, ecp);
        if (ret != RDB_OK) {
            goto error;
        }
    }

    if (RDB_insert_real(dbrootp->keys_tbp, &tpl, ecp, txp) != RDB_OK)
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
    RDB_object tbnameobj;
    RDB_object attrnameobj;
    RDB_type *tuptyp = tbp->typ->def.basetyp;

    /* Insert (database, table) pair into sys_dbtables */
    RDB_init_obj(&tpl);
    RDB_init_obj(&tbnameobj);
    RDB_init_obj(&attrnameobj);

    if (string_to_id(&tbnameobj, RDB_table_name(tbp), ecp) != RDB_OK)
        goto error;

    if (RDB_tuple_set(&tpl, "tablename", &tbnameobj, ecp) != RDB_OK) {
        goto error;
    }

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        char *attrname = tuptyp->def.tuple.attrv[i].name;
        RDB_attr_default *defaultp = RDB_hashmap_get(tbp->val.tb.default_map,
                attrname);
        if (defaultp != NULL) {
            RDB_object binval;
            RDB_object *valp = RDB_expr_obj(defaultp->exp);

            if (valp == NULL && !RDB_expr_is_serial(defaultp->exp)) {
                RDB_raise_invalid_argument("Invalid default value", ecp);
                goto error;
            }

            if (valp != NULL && !RDB_type_equals(valp->typ,
                    tuptyp->def.tuple.attrv[i].typ)) {
                RDB_raise_type_mismatch(
                        "Type of default value does not match attribute type",
                        ecp);
                goto error;
            }

            if (string_to_id(&attrnameobj, attrname, ecp) != RDB_OK)
                goto error;
            if (RDB_tuple_set(&tpl, "attrname", &attrnameobj, ecp)
                    != RDB_OK) {
                goto error;
            }

            RDB_init_obj(&binval);
            ret = RDB_expr_to_binobj(&binval, defaultp->exp, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&binval, ecp);
                goto error;
            }

            ret = RDB_tuple_set(&tpl, "default_value", &binval, ecp);
            RDB_destroy_obj(&binval, ecp);
            if (ret != RDB_OK) {
                goto error;
            }

            if (RDB_table_is_user(tbp)) {
                ret = RDB_insert(dbrootp->table_attr_defvals_tbp, &tpl, ecp, txp);
            } else {
                ret = RDB_insert_real(dbrootp->table_attr_defvals_tbp, &tpl, ecp, txp);
            }
            if (ret != RDB_OK) {
                goto error;
            }
        }
    }
    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);
    RDB_destroy_obj(&tpl, ecp);
    return RDB_ERROR;
}

/* Insert the table pointed to by tbp into the catalog. */
static int
insert_rtable(RDB_object *tbp, RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object atpl;
    RDB_object tbnameobj;
    RDB_object attrnameobj;
    RDB_type *tuptyp = tbp->typ->def.basetyp;
    int ret;
    int i;

    /* insert entry into table sys_rtables */
    RDB_init_obj(&tpl);
    RDB_init_obj(&atpl);
    RDB_init_obj(&tbnameobj);
    RDB_init_obj(&attrnameobj);

    if (string_to_id(&tbnameobj, RDB_table_name(tbp), ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set(&tpl, "tablename", &tbnameobj, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set_bool(&tpl, "is_user", RDB_table_is_user(tbp), ecp)
            != RDB_OK) {
        goto error;
    }

    /*
     * When the catalog tables are inserted into the catalog, some tables
     * are not already present in the catalog. The can cause e.g. reading
     * the constraints to fail because table names cannot be resolved.
     * So insert system tables without constraint check.
     */
    if (RDB_table_is_user(tbp)) {
        if (RDB_insert(dbrootp->rtables_tbp, &tpl, ecp, txp) != RDB_OK) {
            goto error;
        }
    } else {
        if (RDB_insert_real(dbrootp->rtables_tbp, &tpl, ecp, txp) != RDB_OK) {
            goto error;
        }
    }

    /* Insert entries into table sys_tableattrs */

    if (RDB_tuple_set_string(&atpl, "tablename", RDB_table_name(tbp), ecp) != RDB_OK)
        goto error;

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        RDB_object typedata;
        char *attrname = tuptyp->def.tuple.attrv[i].name;

        RDB_init_obj(&typedata);
        if (RDB_type_to_binobj(&typedata, tuptyp->def.tuple.attrv[i].typ,
                ecp) != RDB_OK) {
            RDB_destroy_obj(&typedata, ecp);
            goto error;
        }
        ret = RDB_tuple_set(&atpl, "type", &typedata, ecp);
        RDB_destroy_obj(&typedata, ecp);
        if (ret != RDB_OK) {
            goto error;
        }

        if (string_to_id(&attrnameobj, attrname, ecp) != RDB_OK)
            goto error;
        if (RDB_tuple_set(&atpl, "attrname", &attrnameobj, ecp)
                != RDB_OK) {
            goto error;
        }
        if (RDB_tuple_set_int(&atpl, "i_fno", i, ecp) != RDB_OK) {
            goto error;
        }

        if (RDB_table_is_user(tbp)) {
            if (RDB_insert(dbrootp->table_attr_tbp, &atpl, ecp, txp) != RDB_OK) {
                goto error;
            }
        } else {
            if (RDB_insert_real(dbrootp->table_attr_tbp, &atpl, ecp, txp) != RDB_OK) {
                goto error;
            }
        }
    }

    if (tbp->val.tb.default_map != NULL) {
        if (insert_defvals(tbp, dbrootp, ecp, txp) != RDB_OK)
            goto error;
    }

    /*
     * Insert keys into sys_keys
     */
    for (i = 0; i < tbp->val.tb.keyc; i++) {
        if (insert_key(&tbp->val.tb.keyv[i], i, RDB_table_name(tbp), dbrootp,
                ecp, txp) != RDB_OK)
            goto error;
    }

    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&atpl, ecp);
    return RDB_destroy_obj(&tbnameobj, ecp);

error:
    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&atpl, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);
    return RDB_ERROR;
}

static int
insert_vtable(RDB_object *tbp, RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_object tbnameobj;
    RDB_object defval;

    RDB_init_obj(&tpl);
    RDB_init_obj(&defval);
    RDB_init_obj(&tbnameobj);

    /* Insert entry into sys_vtables */

    ret = string_to_id(&tbnameobj, RDB_table_name(tbp), ecp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set(&tpl, "tablename", &tbnameobj, ecp);
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
    RDB_destroy_obj(&tbnameobj, ecp);

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
    RDB_object tbnameobj;
    RDB_object attrnameobj;

    RDB_init_obj(&tpl);
    RDB_init_obj(&attrsarr);
    RDB_init_obj(&attrtpl);
    RDB_init_obj(&tbnameobj);
    RDB_init_obj(&attrnameobj);

    ret = string_to_id(&tbnameobj, tbname, ecp);
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
        ret = string_to_id(&attrnameobj, attrv[i].attrname, ecp);
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

int
RDB_cat_insert_table_recmap(RDB_object *tbp, const char *rmname,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_object tbnameobj;

    RDB_init_obj(&tbnameobj);

    /* Insert entry into sys_vtables */

    ret = string_to_id(&tbnameobj, RDB_table_name(tbp), ecp);
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
RDB_cat_index_tablename(const char *name, RDB_object *tbnamep,
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
    argp = RDB_attr_eq_strval("idxname", name, ecp);
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
        goto error;
    }

    if (RDB_copy_obj(tbnamep, RDB_tuple_get(&tpl, "tablename"), ecp) != RDB_OK)
        goto error;
    RDB_destroy_obj(&tpl, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&tpl, ecp);
    return RDB_ERROR;
}

int
RDB_cat_delete_index(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *wherep = RDB_attr_eq_strval("idxname", name, ecp);
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
         * For user tables, the key indexes are inserted when the recmap is created.
         */
        if (ret == RDB_OK) {
            if (!RDB_table_is_user(tbp)) {
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
    return RDB_cat_dbtables_insert(tbp, RDB_tx_db(txp), ecp, txp);
}

/* Insert a public table into the catalog */
int
RDB_cat_insert_ptable(const char *name,
        int attrc, const RDB_attr attrv[],
        int keyc, const RDB_string_vec keyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl;
    RDB_object tbnameobj;
    RDB_object defval;
    RDB_object attrnameobj;

    /*
     * Insert entry into table sys_ptables
     */

    RDB_init_obj(&tpl);
    RDB_init_obj(&tbnameobj);
    RDB_init_obj(&attrnameobj);
    RDB_init_obj(&defval);

    if (RDB_tuple_set_string(&tpl, "tablename", name, ecp) != RDB_OK)
        goto error;

    if (RDB_tuple_set_bool(&tpl, "is_user", RDB_TRUE, ecp) != RDB_OK) {
        goto error;
    }

    /* i_def is initially empty */
    if (RDB_binary_set(&defval, 0, NULL, 0, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_tuple_set(&tpl, "i_def", &defval, ecp) != RDB_OK)
        goto error;
    if (RDB_insert(txp->dbp->dbrootp->ptables_tbp, &tpl, ecp, txp) != RDB_OK) {
        goto error;
    }

    /*
     * Insert entries into table sys_tableattrs
     */

    for (i = 0; i < attrc; i++) {
        RDB_object typedata;
        char *attrname = attrv[i].name;

        RDB_init_obj(&typedata);
        ret = RDB_type_to_binobj(&typedata, attrv[i].typ,
                ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typedata, ecp);
            goto error;
        }
        ret = RDB_tuple_set(&tpl, "type", &typedata, ecp);
        RDB_destroy_obj(&typedata, ecp);
        if (ret != RDB_OK) {
            goto error;
        }

        if (string_to_id(&attrnameobj, attrname, ecp) != RDB_OK)
            goto error;
        if (RDB_tuple_set(&tpl, "attrname", &attrnameobj, ecp) != RDB_OK) {
            goto error;
        }
        ret = RDB_tuple_set_int(&tpl, "i_fno", -1, ecp);
        if (ret != RDB_OK) {
            goto error;
        }
        ret = RDB_insert(txp->dbp->dbrootp->table_attr_tbp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            goto error;
        }
    }

    /*
     * Insert keys into sys_keys
     *
    if (string_to_id(&tbnameobj, name, ecp) != RDB_OK)
        goto error;
     */
    for (i = 0; i < keyc; i++) {
        if (insert_key(&keyv[i], i, name, txp->dbp->dbrootp,
                ecp, txp) != RDB_OK)
            goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&defval, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);
    RDB_destroy_obj(&attrnameobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&defval, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);
    RDB_destroy_obj(&attrnameobj, ecp);
    return RDB_ERROR;
}

static RDB_expression *
tablename_id_eq_expr(const char *name, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_expression *arg2p;
    RDB_expression *argp = RDB_var_ref("tablename", ecp);
    if (argp == NULL) {
        return NULL;
    }
    arg2p = RDB_expr_comp(argp, "name", ecp);
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

static RDB_expression *
tablename_eq_expr(const char *name, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_expression *arg2p;
    RDB_expression *argp = RDB_var_ref("tablename", ecp);
    if (argp == NULL) {
        return NULL;
    }

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

int
RDB_cat_map_ptable(const char *name, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_attr_update upd;
    RDB_object defval;
    int count;
    RDB_expression *condp = tablename_eq_expr(name, ecp);
    if (condp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&defval);
    upd.exp = NULL;

    upd.name = "i_def";
    if (RDB_expr_to_binobj(&defval, exp, ecp) != RDB_OK)
        goto error;
    upd.exp = RDB_obj_to_expr(&defval, ecp);
    if (upd.exp == NULL)
        goto error;

    count = RDB_update(RDB_tx_db(txp)->dbrootp->ptables_tbp,
            condp, 1, &upd, ecp, txp);
    if (count == 0) {
        RDB_raise_not_found(name, ecp);
        goto error;
    }

    RDB_del_expr(upd.exp, ecp);
    RDB_del_expr(condp, ecp);
    RDB_destroy_obj(&defval, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&defval, ecp);
    if (upd.exp != NULL)
        RDB_del_expr(upd.exp, ecp);
    if (condp != NULL)
        RDB_del_expr(condp, ecp);
    return RDB_ERROR;
}

/* Delete a real table from the catalog */
static int
delete_rtable(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exprp = NULL;
    RDB_expression *idexprp = tablename_id_eq_expr(RDB_table_name(tbp), ecp);
    if (idexprp == NULL) {
        return RDB_ERROR;
    }

    exprp = tablename_eq_expr(RDB_table_name(tbp), ecp);
    if (exprp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    ret = RDB_delete(txp->dbp->dbrootp->indexes_tbp, idexprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->rtables_tbp, idexprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_recmap_tbp, idexprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_attr_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_attr_defvals_tbp, idexprp, ecp,
            txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->keys_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->dbtables_tbp, idexprp, ecp, txp);

cleanup:
    RDB_del_expr(idexprp, ecp);
    if (exprp != NULL) {
        RDB_del_expr(exprp, ecp);
    }
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

/* Delete a virtual table from the catalog */
static int
delete_vtable(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exprp = tablename_id_eq_expr(RDB_table_name(tbp), ecp);
    if (exprp == NULL) {
        return RDB_ERROR;
    }
    ret = (int) RDB_delete(txp->dbp->dbrootp->vtables_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;
    if (ret == 0) {
        RDB_raise_name(RDB_table_name(tbp), ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    ret = RDB_delete(txp->dbp->dbrootp->dbtables_tbp, exprp, ecp, txp);

cleanup:
    RDB_del_expr(exprp, ecp);

    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

/*
 * Delete a public table from the catalog.
 * Raise RDB_NAME_ERROR if the table does not exist.
s */
int
RDB_cat_delete_ptable(const char *tbname, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exprp;
    RDB_expression *idexprp;

    exprp = tablename_eq_expr(tbname, ecp);
    if (exprp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    idexprp = tablename_id_eq_expr(tbname, ecp);
    if (idexprp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    ret = (int) RDB_delete(txp->dbp->dbrootp->ptables_tbp, exprp, ecp, txp);
    if (ret == 0) {
        /* No table entry deleted */
        RDB_raise_name(tbname, ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->dbtables_tbp, idexprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_attr_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->keys_tbp, exprp, ecp, txp);
    if (ret == RDB_ERROR)
        goto cleanup;

cleanup:
    if (idexprp != NULL)
        RDB_del_expr(idexprp, ecp);
    if (exprp != NULL)
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

    /* Can be a virtual or public table - delete from both catalog tables */
    if (delete_vtable(tbp, ecp, txp) == RDB_OK)
        return RDB_OK;

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NAME_ERROR) {
        return RDB_ERROR;
    }

    /* name_error means it's probable a public table */
    return RDB_cat_delete_ptable(RDB_table_name(tbp), ecp, txp);
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
    argp = tablename_id_eq_expr(tablename, ecp);
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
        ret = RDB_open_stored_table(*tbpp, txp->envp, name, ecp, txp);
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
            ret = RDB_open_tbindex(tbp, &indexv[i], dbrootp->envp, ecp,
                    txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        } else {
            indexv[i].idxp = NULL;
        }
    }

    return RDB_OK;
}

static RDB_expression *
index_rtable_constraint(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    RDB_expression *exp, *arg1p, *arg2p;

    /* Create expression sys_indexes { tablename } subset_of sys_rtables { tablename } */
    exp = RDB_ro_op("project", ecp);
    if (exp == NULL)
        return NULL;

    arg1p = RDB_table_ref(dbrootp->indexes_tbp, ecp);
    if (arg1p == NULL)
        goto error;
    RDB_add_arg(exp, arg1p);

    arg1p = RDB_string_to_expr("tablename", ecp);
    if (arg1p == NULL)
        goto error;
    RDB_add_arg(exp, arg1p);

    arg2p = RDB_ro_op("project", ecp);
    if (arg2p == NULL)
        goto error;

    arg1p = RDB_table_ref(dbrootp->rtables_tbp, ecp);
    if (arg1p == NULL) {
        RDB_del_expr(arg1p, ecp);
        goto error;
    }
    RDB_add_arg(arg2p, arg1p);

    arg1p = RDB_string_to_expr("tablename", ecp);
    if (arg1p == NULL) {
        RDB_del_expr(arg1p, ecp);
        goto error;
    }
    RDB_add_arg(arg2p, arg1p);

    arg1p = exp;
    exp = RDB_ro_op("subset_of", ecp);
    if (exp == NULL) {
        RDB_del_expr(arg1p, ecp);
        RDB_del_expr(arg2p, ecp);
        goto error;
    }
    RDB_add_arg(exp, arg1p);
    RDB_add_arg(exp, arg2p);
    return exp;

error:
    if (exp != NULL)
        RDB_del_expr(exp, ecp);
    return NULL;
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
    static RDB_type *bin_array_typ = NULL;
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

    ret = provide_systable("sys_ptables", 3, ptables_attrv,
            1, ptables_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->ptables_tbp);
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

    if (keys_attrv[2].typ == NULL) {
        keysattr.name = "keyattr";
        keysattr.typ = &RDB_IDENTIFIER;
        keys_attrv[2].typ = RDB_new_relation_type(1, &keysattr, ecp);
        if (keys_attrv[2].typ == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    ret = provide_systable("sys_keys", 3, keys_attrv, 1, keys_keyv,
            create, ecp, txp, dbrootp->envp, &dbrootp->keys_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = provide_systable("sys_types", 7, types_attrv, 1, types_keyv,
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

    if (bin_array_typ == NULL) {
        bin_array_typ = RDB_new_array_type(&RDB_BINARY, ecp);
        if (bin_array_typ == NULL)
            return RDB_ERROR;
    }

    ro_ops_attrv[1].typ = bin_array_typ;

    ret = provide_systable("sys_ro_ops", 7, ro_ops_attrv,
            1, ro_ops_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->ro_ops_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    upd_ops_attrv[1].typ = bin_array_typ;
    if (upd_ops_attrv[5].typ == NULL) {
        upd_ops_attrv[5].typ = RDB_new_array_type(&RDB_BOOLEAN, ecp);
        if (upd_ops_attrv[5].typ == NULL) {
            return RDB_ERROR;
        }
    }

    ret = provide_systable("sys_upd_ops", 7, upd_ops_attrv,
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

    if (indexes_attrv[2].typ == NULL) {
        typ = RDB_new_tuple_type(2, indexes_attrs_attrv, ecp);
        if (typ == NULL) {
            return RDB_ERROR;
        }
        indexes_attrv[2].typ = RDB_new_array_type(typ, ecp);
        if (indexes_attrv[2].typ == NULL) {
            return RDB_ERROR;
        }
    }

    ret = provide_systable("sys_indexes", 5, indexes_attrv,
            1, indexes_keyv, create, ecp, txp, dbrootp->envp,
            &dbrootp->indexes_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    if (create) {
        /* Add referential constraint INDEX -> RTABLE */
        RDB_expression *exp = index_rtable_constraint(dbrootp, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        if (RDB_create_constraint("SYS_INDEXES_RTABLE", exp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
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
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->table_attr_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->rtables_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->vtables_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->ptables_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->table_recmap_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->dbtables_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->keys_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->types_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->possrepcomps_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->ro_ops_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->upd_ops_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->indexes_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;

            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->constraints_tbp, RDB_tx_db(txp), ecp, txp);
            if (ret != RDB_OK) 
                return ret;

            ret = RDB_cat_dbtables_insert(txp->dbp->dbrootp->version_info_tbp, RDB_tx_db(txp), ecp, txp);
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

    ret = RDB_cat_insert(txp->dbp->dbrootp->ptables_tbp, ecp, txp);
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
get_key(RDB_object *tbp, RDB_string_vec *keyp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object attrarr;
    RDB_object attrnameobj;
    RDB_object *tplp;

    RDB_init_obj(&attrarr);
    RDB_init_obj(&attrnameobj);
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
        if (RDB_obj_property(RDB_tuple_get(tplp, "keyattr"), "name",
                &attrnameobj, NULL, ecp, txp) != RDB_OK)
            goto error;

        keyp->strv[i] = RDB_dup_str(RDB_obj_string(&attrnameobj));
        if (keyp->strv[i] == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }

    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&attrarr, ecp);
    return RDB_OK;

error:
    if (keyp->strv != NULL) {
        for (i = 0; i < keyp->strc; i++)
            RDB_free(keyp->strv[i]);
        RDB_free(keyp->strv);
    }
    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&attrarr, ecp);
    return RDB_ERROR;
}

int
RDB_cat_get_keys(const char *name, RDB_exec_context *ecp, RDB_transaction *txp,
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
    argp = tablename_eq_expr(name, ecp);
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

        ret = get_key(RDB_tuple_get(tplp, "attrs"), &(*keyvp)[kno], ecp, txp);
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

/*
 * Get type of real or public table with name name.
 * If the table does not exist, RELATION { } is returned.
 */
RDB_type *
RDB_cat_get_table_type(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int attrc;
    RDB_expression *exp, *argp;
    RDB_attr *attrv = NULL;
    RDB_type *tbtyp;
    RDB_object arr;
    RDB_object attrnameobj;
    RDB_object *tplp;
    RDB_object *tmptbp = NULL;

    /*
     * Read attribute names and types
     */

    RDB_init_obj(&arr);
    RDB_init_obj(&attrnameobj);

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
    argp = tablename_eq_expr(name, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        goto error;
    }
    RDB_add_arg(exp, argp);

    tmptbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (tmptbp == NULL)
        goto error;
    if (RDB_table_to_array(&arr, tmptbp, 0, NULL, 0, ecp, txp)
            != RDB_OK) {
        goto error;
    }

    RDB_drop_table(tmptbp, ecp, txp);
    tmptbp = NULL;

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
        if (fno == -1)
            fno = i;
        if (RDB_obj_property(RDB_tuple_get(tplp, "attrname"), "name",
                &attrnameobj, NULL, ecp, txp) != RDB_OK)
            goto error;
        attrv[fno].name = RDB_dup_str(RDB_obj_string(&attrnameobj));
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

    tbtyp = RDB_new_relation_type(attrc, attrv, ecp);
    if (tbtyp == NULL) {
        goto error;
    }

    for (i = 0; i < attrc; i++) {
        RDB_free(attrv[i].name);
        RDB_del_nonscalar_type(attrv[i].typ, ecp);
    }
    if (attrc > 0)
        RDB_free(attrv);

    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&arr, ecp);

    return tbtyp;

error:
    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&arr, ecp);

    if (tmptbp != NULL)
        RDB_drop_table(tmptbp, ecp, txp);

    if (attrv != NULL) {
        for (i = 0; i < attrc; i++) {
            RDB_free(attrv[i].name);
            if (attrv[i].typ != NULL)
                RDB_del_nonscalar_type(attrv[i].typ, ecp);
        }
        RDB_free(attrv);
    }
    return NULL;
}

static RDB_expression *
db_query(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_expression *argp;
    RDB_expression *exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        return NULL;
    }
    argp = RDB_table_ref(dbp->dbrootp->dbtables_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);
    argp = RDB_attr_eq_strval("dbname", dbp->name, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);
    return exp;
}

static RDB_expression *
table_query(RDB_database *dbp, RDB_object *tables_tbp,
        const char *name, RDB_exec_context *ecp)
{
    RDB_expression *argp;
    RDB_expression *exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        return NULL;
    }
    argp = RDB_table_ref(tables_tbp, ecp);
    if (argp == NULL) {
        goto error;
    }
    RDB_add_arg(exp, argp);
    argp = tablename_id_eq_expr(name, ecp);
    if (argp == NULL) {
        goto error;
    }
    RDB_add_arg(exp, argp);

    argp = exp;
    exp = RDB_ro_op("join", ecp);
    if (exp == NULL) {
        RDB_del_expr(argp, ecp);
        goto error;
    }

    /* Set transformed flags to avoid infinite recursion */
    argp->transformed = RDB_TRUE;

    RDB_add_arg(exp, argp);
    argp = db_query(dbp, ecp);
    if (argp == NULL) {
        goto error;
    }
    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    exp->transformed = RDB_TRUE;

    return exp;

error:
    if (exp != NULL)
        RDB_del_expr(exp, NULL);
    return NULL;
}

static RDB_expression *
tb_dbs_query(RDB_object *tbp, RDB_exec_context *ecp, RDB_dbroot *dbrootp)
{
    RDB_expression *argp;
    RDB_expression *exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        return NULL;
    }
    argp = RDB_table_ref(dbrootp->dbtables_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);
    argp = tablename_id_eq_expr(RDB_table_name(tbp), ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    RDB_add_arg(exp, argp);
    return exp;
}

static int
get_table_dbs(RDB_object *tbp, RDB_object *arrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tb_dbs_tbp = NULL;
    RDB_expression *exp = tb_dbs_query(tbp, ecp, txp->dbp->dbrootp);

    if (exp == NULL)
        return RDB_ERROR;

    tb_dbs_tbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (tb_dbs_tbp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }

    ret = RDB_table_to_array(arrp, tb_dbs_tbp, 0, NULL, 0, ecp, txp);
    RDB_drop_table(tb_dbs_tbp, ecp, txp);
    return ret;
}

/* Adds a table to all databases in memory to which it belongs */
static int
add_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object arr;
    int i;
    RDB_int len;

    RDB_init_obj(&arr);

    if (get_table_dbs(tbp, &arr, ecp, txp) != RDB_OK)
        goto error;

    len = RDB_array_length(&arr, ecp);
    if (len == (RDB_int) RDB_ERROR)
        goto error;

    for (i = 0; i < len; i++) {
        RDB_database *dbp;
        RDB_object *tp = RDB_array_get(&arr, (RDB_int) i, ecp);
        if (tp == NULL)
            goto error;
        dbp = RDB_get_db_from_env(RDB_tuple_get_string(tp, "dbname"), txp->dbp->dbrootp->envp, ecp);
        if (dbp == NULL)
            goto error;
        if (RDB_assoc_table_db(tbp, dbp, ecp) != RDB_OK)
            goto error;
    }

    RDB_destroy_obj(&arr, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&arr, ecp);
    return RDB_ERROR;
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
    argp = tablename_id_eq_expr(RDB_table_name(tbp), ecp);
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

/* Reads a real table from the catalog */
static int
RDB_cat_get_rtable(RDB_object *tbp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp, *argp;
    RDB_object *tmptb1p = NULL;
    RDB_object *tmptb3p = NULL;
    RDB_object *tmptb4p = NULL;
    RDB_object arr;
    RDB_object tpl;
    RDB_object attrnameobj;
    RDB_object *tplp;
    RDB_bool usr;
    int ret;
    RDB_int i;
    int keyc;
    RDB_string_vec *keyv;
    RDB_type *tbtyp;
    int defvalc;
    RDB_hashmap *defvalmap = NULL;
    RDB_object recmapnameobj;
    const char *recmapname = NULL;

    RDB_init_obj(&arr);
    RDB_init_obj(&tpl);
    RDB_init_obj(&attrnameobj);
    RDB_init_obj(&recmapnameobj);

    exp = table_query(txp->dbp, txp->dbp->dbrootp->rtables_tbp, name, ecp);
    if (exp == NULL)
        goto error;

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

    tbtyp = RDB_cat_get_table_type(name, ecp, txp);
    if (tbtyp == NULL)
        goto error;

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
    argp = tablename_id_eq_expr(name, ecp);
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

    /* Insert default values into map */
    defvalc = RDB_array_length(&arr, ecp);
    if (defvalc > 0) {
        defvalmap = RDB_alloc(sizeof (RDB_hashmap), ecp);
        if (defvalmap == NULL)
            goto error;
        RDB_init_hashmap(defvalmap, 256);
        for (i = 0; i < defvalc; i++) {
            RDB_attr_default *entryp;
            int pos = 0;

            tplp = RDB_array_get(&arr, i, ecp);
            if (tplp == NULL)
                goto error;

            if (RDB_obj_property(RDB_tuple_get(tplp, "attrname"), "name",
                    &attrnameobj, NULL, ecp, txp) != RDB_OK)
                goto error;
            entryp = RDB_alloc(sizeof(RDB_attr_default), ecp);
            if (entryp == NULL)
                goto error;
            entryp->seqp = NULL;
            if (RDB_deserialize_expr(RDB_tuple_get(tplp, "default_value"), &pos,
                    ecp, txp, &entryp->exp) != RDB_OK) {
                goto error;
            }
            ret = RDB_hashmap_put(defvalmap, RDB_obj_string(&attrnameobj), entryp);
            if (ret != RDB_OK) {
                RDB_errcode_to_error(ret, ecp);
                goto error;
            }
        }
    }

    if (RDB_cat_get_keys(name, ecp, txp, &keyc, &keyv) != RDB_OK)
        goto error;

    if (RDB_init_table_i(tbp, name, RDB_TRUE, tbtyp, keyc, keyv,
            0, NULL, usr, NULL, ecp) != RDB_OK) {
        RDB_free_keys(keyc, keyv);
        RDB_del_nonscalar_type(tbtyp, ecp);
        goto error;
    }
    RDB_free_keys(keyc, keyv);

    /*
     * Read recmap name from catalog if it's user table.
     * For system tables, the recmap name is the table name.
     */
    if (usr) {
        if (RDB_cat_recmap_name(tbp, &recmapnameobj, ecp, txp) != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                goto error;
            recmapname = NULL;
        } else {
            recmapname = RDB_obj_string(&recmapnameobj);
        }
    } else {
        recmapname = RDB_table_name(tbp);
    }
    if (recmapname != NULL) {
        ret = RDB_open_stored_table(tbp, txp->envp, recmapname, ecp, txp);
        if (ret != RDB_OK) {
            goto error;
        }
    }

    if (add_table(tbp, ecp, txp) != RDB_OK)
        goto error;

    tbp->val.tb.default_map = defvalmap;

    RDB_destroy_obj(&arr, ecp);

    RDB_drop_table(tmptb1p, ecp, txp);
    RDB_drop_table(tmptb3p, ecp, txp);
    if (tmptb4p != NULL)
        RDB_drop_table(tmptb4p, ecp, txp);

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&recmapnameobj, ecp);

    return RDB_OK;

error:
    RDB_destroy_obj(&arr, ecp);

    if (tmptb1p != NULL)
        RDB_drop_table(tmptb1p, ecp, txp);
    if (tmptb3p != NULL)
        RDB_drop_table(tmptb3p, ecp, txp);
    if (tmptb4p != NULL)
        RDB_drop_table(tmptb4p, ecp, txp);

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&attrnameobj, ecp);
    RDB_destroy_obj(&recmapnameobj, ecp);

    if (defvalmap != NULL) {
        if (tbp->val.tb.default_map != NULL) {
            RDB_hashmap_iter hiter;
            void *valp;

            RDB_init_hashmap_iter(&hiter, tbp->val.tb.default_map);
            while (RDB_hashmap_next(&hiter, &valp) != NULL) {
                RDB_free(valp);
            }
            RDB_destroy_hashmap_iter(&hiter);
            RDB_destroy_hashmap(tbp->val.tb.default_map);
        }
        RDB_free(tbp->val.tb.default_map);
        tbp->val.tb.default_map = NULL;
    }

    return RDB_ERROR;
} /* RDB_cat_get_rtable */

static int
RDB_binobj_to_vtable(RDB_object *tbp, RDB_object *valp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp, *texp;
    int pos = 0;
    if (RDB_deserialize_expr(valp, &pos, ecp, txp, &exp) != RDB_OK)
        return RDB_ERROR;

    /*
     * Resolve table names so the underlying real table(s) can be
     * accessed without having to resolve a variable
     */
    texp = RDB_expr_resolve_varnames(exp, NULL, NULL, ecp, txp);
    RDB_del_expr(exp, ecp);
    if (texp == NULL)
        return RDB_ERROR;

    return RDB_vtexp_to_obj(texp, ecp, txp, tbp);
}

static int
RDB_cat_get_vtable(RDB_object *tbp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_expression *exp;
    RDB_object tpl;
    RDB_object arr;
    RDB_object *valp;
    RDB_bool usr;
    int ret;
    RDB_object *tmptbp = NULL;

    /*
     * Read virtual table data from the catalog
     */

    RDB_init_obj(&arr);
    RDB_init_obj(&tpl);

    exp = table_query(txp->dbp, txp->dbp->dbrootp->vtables_tbp, name, ecp);
    if (exp == NULL)
        goto error;

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

    if (RDB_binobj_to_vtable(tbp, valp, ecp, txp) != RDB_OK)
        goto error;
    tbp->val.tb.name = RDB_dup_str(name);
    if (tbp->val.tb.name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    if (RDB_destroy_obj(&arr, ecp) != RDB_OK)
        goto error;

    tbp->flags = RDB_TB_PERSISTENT;
    if (usr) {
        tbp->flags |= RDB_TB_USER;
    } else {
        tbp->flags &= ~RDB_TB_USER;
    }

    if (add_table(tbp, ecp, txp) != RDB_OK)
        goto error;

    if (RDB_env_trace(RDB_db_env(RDB_tx_db(txp))) > 0) {
        fprintf(stderr,
                "Definition of virtual table %s read from the catalog\n",
                name);
    }
    return RDB_OK;

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&arr, ecp);
    
    return RDB_ERROR;
} /* RDB_cat_get_vtable */

RDB_object *
RDB_cat_get_ptable_vt(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *restbp;
    RDB_expression *argp;
    RDB_expression *exp = RDB_ro_op("where", ecp);
    if (exp == NULL) {
        return NULL;
    }
    argp = RDB_table_ref(txp->dbp->dbrootp->ptables_tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }

    /* Set transformed flags to avoid infinite recursion */

    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    argp = tablename_eq_expr(name, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }

    argp->transformed = RDB_TRUE;
    RDB_add_arg(exp, argp);

    exp->transformed = RDB_TRUE;
    restbp = RDB_expr_to_vtable(exp, ecp, txp);
    if (restbp == NULL) {
        RDB_del_expr(exp, ecp);
        return NULL;
    }
    return restbp;
}

static int
RDB_cat_get_ptable(RDB_object *tbp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object arr;
    RDB_object *valp;
    RDB_bool usr;
    int ret;
    RDB_object *tmptbp = NULL;

    /*
     * Read virtual table data from the catalog
     */

    RDB_init_obj(&arr);
    RDB_init_obj(&tpl);

    tmptbp = RDB_cat_get_ptable_vt(name, ecp, txp);
    if (tmptbp == NULL)
        return RDB_ERROR;
    ret = RDB_extract_tuple(tmptbp, ecp, txp, &tpl);
    RDB_drop_table(tmptbp, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "is_user");

    valp = RDB_tuple_get(&tpl, "i_def");

    if (RDB_binary_length(valp) == 0) {
        /* No mapping defined */
        RDB_raise_not_found(name, ecp);
        goto error;
    }

    if (RDB_binobj_to_vtable(tbp, valp, ecp, txp) != RDB_OK)
        goto error;
    tbp->val.tb.name = RDB_dup_str(name);
    if (tbp->val.tb.name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    ret = RDB_destroy_obj(&arr, ecp);
    if (ret != RDB_OK)
        goto error;

    tbp->flags = RDB_TB_PERSISTENT;
    if (usr) {
        tbp->flags |= RDB_TB_USER;
    } else {
        tbp->flags &= ~RDB_TB_USER;
    }

    if (RDB_env_trace(RDB_db_env(RDB_tx_db(txp))) > 0) {
        fprintf(stderr,
                "Definition of virtual public %s read from the catalog\n",
                name);
    }

    ret = RDB_hashmap_put(&txp->dbp->dbrootp->ptbmap,
            name, tbp);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, NULL);
        return RDB_ERROR;
    }

    return RDB_OK;

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&arr, ecp);

    return RDB_ERROR;
}

int
RDB_cat_get_table(RDB_object *tbp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (RDB_cat_get_rtable(tbp, name, ecp, txp) == RDB_OK)
        return RDB_OK;

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        return RDB_ERROR;

    if (RDB_cat_get_vtable(tbp, name, ecp, txp) == RDB_OK)
        return RDB_OK;
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        return RDB_ERROR;

    return RDB_cat_get_ptable(tbp, name, ecp, txp);
}

int
RDB_cat_db_exists(const char *name, RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object tbnameobj;
    RDB_bool b;

    /*
     * Check if the database exists by checking if the DBTABLES contains
     * sys_rtables for this database.
     */

    RDB_init_obj(&tpl);
    RDB_init_obj(&tbnameobj);

    if (string_to_id(&tbnameobj, "sys_rtables", ecp) != RDB_OK)
        goto error;

    if (RDB_tuple_set(&tpl, "tablename", &tbnameobj, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_tuple_set_string(&tpl, "dbname", name, ecp) != RDB_OK) {
        goto error;
    }

    if (RDB_table_contains(dbrootp->dbtables_tbp, &tpl, ecp, txp, &b)
            != RDB_OK) {
        goto error;
    }

    if (!b) {
        RDB_raise_not_found("database not found", ecp);
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);

    return RDB_OK;

error:

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&tbnameobj, ecp);

    return RDB_ERROR;
}

static RDB_expression *
string_to_id_expr(const char *str, RDB_exec_context *ecp)
{
    RDB_object obj;
    RDB_expression *exp;

    RDB_init_obj(&obj);
    if (string_to_id(&obj, str, ecp) != RDB_OK)
        goto error;
    exp = RDB_obj_to_expr (&obj, ecp);
    if (exp == NULL)
        goto error;
    RDB_destroy_obj(&obj, ecp);
    return exp;

error:
    RDB_destroy_obj(&obj, ecp);
    return NULL;
}

int
RDB_cat_rename_table(RDB_object *tbp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_attr_update updid;
    RDB_attr_update upd;
    RDB_ma_update updv[5];
    RDB_transaction tx;
    RDB_expression *idcondp = NULL;
    RDB_expression *condp = NULL;

    /* Start subtx */
    if (RDB_begin_tx(ecp, &tx, RDB_tx_db(txp), txp) != RDB_OK)
        return RDB_ERROR;

    updid.exp = NULL;
    upd.exp = NULL;

    idcondp = tablename_id_eq_expr(RDB_table_name(tbp), ecp);
    if (idcondp == NULL) {
        goto error;
    }

    condp = tablename_eq_expr(RDB_table_name(tbp), ecp);
    if (condp == NULL) {
        goto error;
    }

    updid.name = "tablename";
    updid.exp = string_to_id_expr(name, ecp);
    if (updid.exp == NULL) {
        goto error;
    }

    upd.name = "tablename";
    upd.exp = RDB_string_to_expr(name, ecp);
    if (upd.exp == NULL) {
        goto error;
    }

    if (tbp->val.tb.exp == NULL) {
        /* Real table */
        updv[0].condp = idcondp;
        updv[0].tbp = txp->dbp->dbrootp->rtables_tbp;
        updv[0].updc = 1;
        updv[0].updv = &updid;

        updv[1].condp = condp;
        updv[1].tbp = txp->dbp->dbrootp->table_attr_tbp;
        updv[1].updc = 1;
        updv[1].updv = &upd;

        updv[2].condp = idcondp;
        updv[2].tbp = txp->dbp->dbrootp->table_attr_defvals_tbp;
        updv[2].updc = 1;
        updv[2].updv = &updid;

        updv[3].condp = condp;
        updv[3].tbp = txp->dbp->dbrootp->keys_tbp;
        updv[3].updc = 1;
        updv[3].updv = &upd;

        updv[4].condp = idcondp;
        updv[4].tbp = txp->dbp->dbrootp->indexes_tbp;
        updv[4].updc = 1;
        updv[4].updv = &updid;

        ret = RDB_multi_assign(0, NULL, 5, updv, 0, NULL, 0, NULL, 0, NULL,
                NULL, NULL, ecp, &tx);
        if (ret == RDB_ERROR)
            goto error;
    } else {
        ret = RDB_update(txp->dbp->dbrootp->vtables_tbp, idcondp, 1, &updid, ecp, &tx);
        if (ret == RDB_ERROR)
            goto error;
        if (ret == 0) {
            /* Rename public table */
            updv[0].condp = condp;
            updv[0].tbp = txp->dbp->dbrootp->ptables_tbp;
            updv[0].updc = 1;
            updv[0].updv = &upd;

            updv[1].condp = condp;
            updv[1].tbp = txp->dbp->dbrootp->table_attr_tbp;
            updv[1].updc = 1;
            updv[1].updv = &upd;

            updv[2].condp = condp;
            updv[2].tbp = txp->dbp->dbrootp->keys_tbp;
            updv[2].updc = 1;
            updv[2].updv = &upd;

            ret = RDB_multi_assign(0, NULL, 3, updv, 0, NULL, 0, NULL, 0, NULL,
                    NULL, NULL, ecp, &tx);
            if (ret == RDB_ERROR)
                goto error;
        }
    }
    if (ret == 0) {
        RDB_raise_not_found("table not found in catalog", ecp);
        goto error;
    }
    ret = RDB_update(txp->dbp->dbrootp->table_recmap_tbp, idcondp, 1, &updid, ecp, &tx);
    if (ret == RDB_ERROR)
        goto error;
    ret = RDB_update(txp->dbp->dbrootp->dbtables_tbp, idcondp, 1, &updid, ecp, &tx);

    RDB_del_expr(idcondp, ecp);
    RDB_del_expr(condp, ecp);
    if (updid.exp != NULL)
        RDB_del_expr(updid.exp, ecp);
    if (upd.exp != NULL)
        RDB_del_expr(upd.exp, ecp);
    return RDB_commit(ecp, &tx);

error:
    RDB_rollback(ecp, &tx);
    if (idcondp != NULL)
        RDB_del_expr(idcondp, ecp);
    if (condp != NULL)
        RDB_del_expr(condp, ecp);
    if (updid.exp != NULL)
        RDB_del_expr(updid.exp, ecp);
    if (upd.exp != NULL)
        RDB_del_expr(upd.exp, ecp);
    return RDB_ERROR;
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
