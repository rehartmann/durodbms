/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "catalog.h"
#include "typeimpl.h"
#include "internal.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>

/*
 * Definitions of the catalog tables.
 */

static RDB_attr table_attr_attrv[] = {
            { "ATTRNAME", &RDB_STRING, NULL, 0 },
            { "TABLENAME", &RDB_STRING, NULL, 0 },
            { "TYPE", &RDB_BINARY, NULL, 0 },
            { "I_FNO", &RDB_INTEGER, NULL, 0 } };
static char *table_attr_keyattrv[] = { "ATTRNAME", "TABLENAME" };
static RDB_string_vec table_attr_keyv[] = { { 2, table_attr_keyattrv } };

static RDB_attr table_attr_defvals_attrv[] = {
            { "ATTRNAME", &RDB_STRING, NULL, 0 },
            { "TABLENAME", &RDB_STRING, NULL, 0 },
            { "DEFAULT_VALUE", &RDB_BINARY, NULL, 0 } };
static char *table_attr_defvals_keyattrv[] = { "ATTRNAME", "TABLENAME" };
static RDB_string_vec table_attr_defvals_keyv[] = { { 2, table_attr_defvals_keyattrv } };

static RDB_attr rtables_attrv[] = {
    { "TABLENAME", &RDB_STRING, NULL, 0 },
    { "IS_USER", &RDB_BOOLEAN, NULL, 0 },
    { "I_RECMAP", &RDB_STRING, NULL, 0 }
};
static char *rtables_keyattrv[] = { "TABLENAME" };
static RDB_string_vec rtables_keyv[] = { { 1, rtables_keyattrv } };

static RDB_attr vtables_attrv[] = {
    { "TABLENAME", &RDB_STRING, NULL, 0 },
    { "IS_USER", &RDB_BOOLEAN, NULL, 0 },
    { "I_DEF", &RDB_BINARY, NULL, 0 }
};
static char *vtables_keyattrv[] = { "TABLENAME" };
static RDB_string_vec vtables_keyv[] = { { 1, vtables_keyattrv } };

static RDB_attr dbtables_attrv[] = {
    { "TABLENAME", &RDB_STRING },
    { "DBNAME", &RDB_STRING }
};
static char *dbtables_keyattrv[] = { "TABLENAME", "DBNAME" };
static RDB_string_vec dbtables_keyv[] = { { 2, dbtables_keyattrv } };

static RDB_attr keys_attrv[] = {
    { "TABLENAME", &RDB_STRING, NULL, 0 },
    { "KEYNO", &RDB_INTEGER, NULL, 0 },
    { "ATTRS", NULL, NULL, 0 } /* type is set to a relation type later */
};
static char *keys_keyattrv[] = { "TABLENAME", "KEYNO" };
static RDB_string_vec keys_keyv[] = { { 2, keys_keyattrv } };

static RDB_attr types_attrv[] = {
    { "TYPENAME", &RDB_STRING, NULL, 0 },
    { "I_AREP_LEN", &RDB_INTEGER, NULL, 0 },
    { "I_AREP_TYPE", &RDB_BINARY, NULL, 0 },
    { "I_SYSIMPL", &RDB_BOOLEAN, NULL, 0}
};
static char *types_keyattrv[] = { "TYPENAME" };
static RDB_string_vec types_keyv[] = { { 1, types_keyattrv } };

static RDB_attr possreps_attrv[] = {
    { "TYPENAME", &RDB_STRING, NULL, 0 },
    { "POSSREPNAME", &RDB_STRING, NULL, 0 },
    { "I_CONSTRAINT", &RDB_BINARY, NULL, 0 }
};
static char *possreps_keyattrv[] = { "TYPENAME", "POSSREPNAME" };
static RDB_string_vec possreps_keyv[] = { { 2, possreps_keyattrv } };

static RDB_attr possrepcomps_attrv[] = {
    { "TYPENAME", &RDB_STRING, NULL, 0 },
    { "POSSREPNAME", &RDB_STRING, NULL, 0 },
    { "COMPNO", &RDB_INTEGER, NULL, 0 },
    { "COMPNAME", &RDB_STRING, NULL, 0 },
    { "COMPTYPENAME", &RDB_STRING, NULL, 0 }
};
static char *possrepcomps_keyattrv1[] = { "TYPENAME", "POSSREPNAME", "COMPNO" };
static char *possrepcomps_keyattrv2[] = { "TYPENAME", "POSSREPNAME", "COMPNAME" };
static RDB_string_vec possrepcomps_keyv[] = {
    { 3, possrepcomps_keyattrv1 },
    { 3, possrepcomps_keyattrv2 }
};

static RDB_attr ro_ops_attrv[] = {
    { "NAME", &RDB_STRING, NULL, 0 },
    { "ARGTYPES", NULL, NULL, 0 }, /* type is set to array of BINARY later */
    { "LIB", &RDB_STRING, NULL, 0 },
    { "SYMBOL", &RDB_STRING, NULL, 0 },
    { "IARG", &RDB_BINARY, NULL, 0 }
};

static char *ro_ops_keyattrv[] = { "NAME", "ARGTYPES" };
static RDB_string_vec ro_ops_keyv[] = { { 2, ro_ops_keyattrv } };

static RDB_attr ro_op_rtypes_attrv[] = {
    { "NAME", &RDB_STRING, NULL, 0 },
    { "RTYPE", &RDB_BINARY, NULL, 0 }
};

static char *ro_op_rtypes_keyattrv[] = { "NAME" };
static RDB_string_vec ro_op_rtypes_keyv[] = { { 1, ro_op_rtypes_keyattrv } };

static RDB_attr upd_ops_attrv[] = {
    { "NAME", &RDB_STRING, NULL, 0 },
    { "ARGTYPES", NULL, NULL, 0 }, /* type is set to array of BINARY later */
    { "LIB", &RDB_STRING, NULL, 0 },
    { "SYMBOL", &RDB_STRING, NULL, 0 },
    { "IARG", &RDB_BINARY, NULL, 0 },
    { "UPDV", NULL, NULL, 0 } /* type is set to array of BOOLEAN later */
};

static char *upd_ops_keyattrv[] = { "NAME", "ARGTYPES" };
static RDB_string_vec upd_ops_keyv[] = { { 2, upd_ops_keyattrv } };

static RDB_attr indexes_attrv[] = {
    { "NAME", &RDB_STRING, NULL, 0 },
    { "TABLENAME", &RDB_STRING, NULL, 0 },
    { "ATTRS", NULL, NULL, 0 }, /* type is set to an array type later */
    { "UNIQUE", &RDB_BOOLEAN, 0 }
};

static char *indexes_keyattrv[] = { "NAME" };
static RDB_string_vec indexes_keyv[] = { { 1, indexes_keyattrv } };

static RDB_attr indexes_attrs_attrv[] = {
    { "NAME", &RDB_STRING, NULL, 0 },
    { "ASC", &RDB_BOOLEAN, NULL, 0 }
};

static int
dbtables_insert(RDB_table *tbp, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    /* Insert (database, table) pair into SYS_DBTABLES */
    RDB_init_obj(&tpl);

    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK)
    {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    ret = RDB_tuple_set_string(&tpl, "DBNAME", txp->dbp->name);
    if (ret != RDB_OK)
    {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    ret = RDB_insert(txp->dbp->dbrootp->dbtables_tbp, &tpl, txp);
    RDB_destroy_obj(&tpl);
    
    return ret;
}

/* Insert the table pointed to by tbp into the catalog. */
static int
insert_rtable(RDB_table *tbp, RDB_dbroot *dbrootp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int ret;
    int i, j;

    /* insert entry into table SYS_RTABLES */
    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    ret = RDB_tuple_set_bool(&tpl, "IS_USER", tbp->is_user);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    ret = RDB_tuple_set_string(&tpl, "I_RECMAP", tbp->name);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    ret = RDB_insert(dbrootp->rtables_tbp, &tpl, txp);
    RDB_destroy_obj(&tpl);
    if (ret != RDB_OK) {
        return ret;
    }

    /* insert entries into table SYS_TABLEATTRS */
    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        RDB_object typedata;
        char *attrname = tuptyp->var.tuple.attrv[i].name;

        RDB_init_obj(&typedata);
        ret = _RDB_type_to_obj(&typedata, tuptyp->var.tuple.attrv[i].typ);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typedata);
            RDB_destroy_obj(&tpl);
            return ret;
        }
        ret = RDB_tuple_set(&tpl, "TYPE", &typedata);
        RDB_destroy_obj(&typedata);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }

        ret = RDB_tuple_set_string(&tpl, "ATTRNAME", attrname);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }
        ret = RDB_tuple_set_int(&tpl, "I_FNO", i);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }
        ret = RDB_insert(dbrootp->table_attr_tbp, &tpl, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }
    }
    RDB_destroy_obj(&tpl);

    /* insert entries into table SYS_TABLEATTR_DEFVALS */
    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        if (tuptyp->var.tuple.attrv[i].defaultp != NULL) {
            char *attrname = tuptyp->var.tuple.attrv[i].name;
            RDB_object binval;
            void *datap;
            size_t len;

            if (!RDB_type_equals(tuptyp->var.tuple.attrv[i].defaultp->typ,
                    tuptyp->var.tuple.attrv[i].typ))
                return RDB_TYPE_MISMATCH;
            
            ret = RDB_tuple_set_string(&tpl, "ATTRNAME", attrname);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }

            RDB_init_obj(&binval);
            datap = RDB_obj_irep(tuptyp->var.tuple.attrv[i].defaultp, &len);
            ret = RDB_binary_set(&binval, 0, datap, len);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                RDB_destroy_obj(&binval);
                return ret;
            }

            ret = RDB_tuple_set(&tpl, "DEFAULT_VALUE", &binval);
            RDB_destroy_obj(&binval);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }

            ret = RDB_insert(dbrootp->table_attr_defvals_tbp, &tpl, txp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }
        }
    }
    RDB_destroy_obj(&tpl);

    /* insert keys into SYS_KEYS */
    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK)
        goto cleanup;

    for (i = 0; i < tbp->keyc; i++) {
        RDB_table *keystbp;
        RDB_attr keysattr;
        RDB_object keysobj;
        RDB_string_vec *kap = &tbp->keyv[i];

        ret = RDB_tuple_set_int(&tpl, "KEYNO", i);
        if (ret != RDB_OK)
            goto cleanup;

        keysattr.name = "KEY";
        keysattr.typ = &RDB_STRING;
        keysattr.defaultp = NULL;
        ret = RDB_create_table(NULL, RDB_FALSE, 1, &keysattr, 0, NULL,
                txp, &keystbp);
        if (ret != RDB_OK)
            goto cleanup;

        for (j = 0; j < kap->strc; j++) {
            RDB_object tpl;
            
            RDB_init_obj(&tpl);
            ret = RDB_tuple_set_string(&tpl, "KEY", kap->strv[j]);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                goto cleanup;
            }
            
            ret = RDB_insert(keystbp, &tpl, txp);
            RDB_destroy_obj(&tpl);
            if (ret != RDB_OK)
                goto cleanup;
        }

        RDB_init_obj(&keysobj);
        RDB_table_to_obj(&keysobj, keystbp);
        ret = RDB_tuple_set(&tpl, "ATTRS", &keysobj);
        RDB_destroy_obj(&keysobj);
        if (ret != RDB_OK)
            goto cleanup;

        ret = RDB_insert(dbrootp->keys_tbp, &tpl, txp);
        if (ret != RDB_OK)
            goto cleanup;
    }
    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);

    return ret;
}

static int
insert_vtable(RDB_table *tbp, RDB_dbroot *dbrootp, RDB_transaction *txp)
{
    int ret;
    RDB_object tpl;
    RDB_object defval;

    RDB_init_obj(&tpl);
    RDB_init_obj(&defval);

    /* Insert entry into SYS_VTABLES */

    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_bool(&tpl, "IS_USER", RDB_TRUE);
    if (ret != RDB_OK)
        goto cleanup;

    ret = _RDB_table_to_obj(&defval, tbp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "I_DEF", &defval);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(dbrootp->vtables_tbp, &tpl, txp);
    if (ret != RDB_OK) {
        if (ret == RDB_KEY_VIOLATION)
            ret = RDB_ELEMENT_EXISTS;
        goto cleanup;
    }

    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&defval);

    return ret;
}

int
_RDB_cat_insert_index(_RDB_tbindex *indexp, const char *tbname,
        RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object tpl;
    RDB_object attrsarr;
    RDB_object attrtpl;

    RDB_init_obj(&tpl);
    RDB_init_obj(&attrsarr);
    RDB_init_obj(&attrtpl);

    ret = RDB_tuple_set_string(&tpl, "NAME", indexp->name);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbname);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_set_array_length(&attrsarr, (RDB_int) indexp->attrc);
    if (ret != RDB_OK)
        goto cleanup;
    for (i = 0; i < indexp->attrc; i++) {
        ret = RDB_tuple_set_string(&attrtpl, "NAME", indexp->attrv[i].attrname);
        if (ret != RDB_OK)
            goto cleanup;
        ret = RDB_tuple_set_bool(&attrtpl, "ASC", indexp->attrv[i].asc);
        if (ret != RDB_OK)
            goto cleanup;
        ret = RDB_array_set(&attrsarr, (RDB_int) i, &attrtpl);
        if (ret != RDB_OK)
            goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "ATTRS", &attrsarr);
    if (ret != RDB_OK)
        goto cleanup;   

    ret = RDB_tuple_set_bool(&tpl, "UNIQUE", indexp->unique);
    if (ret != RDB_OK)
        goto cleanup;   

    ret = RDB_insert(txp->dbp->dbrootp->indexes_tbp, &tpl, txp);

cleanup:
    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&attrsarr);
    RDB_destroy_obj(&attrtpl);
    return ret;
}

int
_RDB_cat_insert(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    /*
     * Create table in the catalog.
     */
    if (tbp->kind == RDB_TB_STORED) {
        int i;

        ret = insert_rtable(tbp, txp->dbp->dbrootp, txp);
        /* If the table already exists in the catalog, proceed */
        if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
            return ret;
        }

        /*
         * Insert indexes into the catalog
         */

        if (ret == RDB_OK) {
            for (i = 0; i < tbp->var.stored.indexc; i++) {
                ret = _RDB_cat_insert_index(&tbp->var.stored.indexv[i],
                        tbp->name, txp);
                if (ret != RDB_OK)
                    return ret;
            }
        }
    } else {
        ret = insert_vtable(tbp, txp->dbp->dbrootp, txp);
        /* If the table already exists in the catalog, proceed */
        if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
            return ret;
        }
    }

    /*
     * Add the table into the database by inserting the table/DB pair
     * into SYS_DBTABLES.
     */
    return dbtables_insert(tbp, txp);
}

/* Delete a real table from the catalog */
static int
delete_rtable(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exprp = RDB_eq(RDB_expr_attr("TABLENAME"),
                   RDB_string_to_expr(tbp->name));
    if (exprp == NULL) {
        return RDB_NO_MEMORY;
    }
    ret = RDB_delete(txp->dbp->dbrootp->rtables_tbp, exprp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_attr_tbp, exprp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->table_attr_defvals_tbp, exprp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->keys_tbp, exprp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->indexes_tbp, exprp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_delete(txp->dbp->dbrootp->dbtables_tbp, exprp, txp);
    if (ret != RDB_OK)
        goto cleanup;

cleanup:
    RDB_drop_expr(exprp);
    return ret;
}

/* Delete a virtual table from the catalog */
static int
delete_vtable(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exprp = RDB_eq(RDB_expr_attr("TABLENAME"),
                   RDB_string_to_expr(tbp->name));
    if (exprp == NULL) {
        return RDB_NO_MEMORY;
    }
    ret = RDB_delete(txp->dbp->dbrootp->vtables_tbp, exprp, txp);
    RDB_drop_expr(exprp);

    return ret;
}

int
_RDB_cat_delete(RDB_table *tbp, RDB_transaction *txp)
{
    if (tbp->kind == RDB_TB_STORED)
        return delete_rtable(tbp, txp);
    else
        return delete_vtable(tbp, txp);
}

/*
 * Read table indexes from catalog
 */
static int
get_indexes(RDB_table *tbp, RDB_dbroot *dbrootp, RDB_transaction *txp)
{
    int ret;
    int i;
    int j;
    RDB_table *vtbp;
    RDB_object arr;
    RDB_expression *wherep = RDB_eq(RDB_expr_attr("TABLENAME"),
            RDB_string_to_expr(tbp->name));

    if (wherep == NULL)
        return RDB_NO_MEMORY;

    ret = RDB_select(dbrootp->indexes_tbp, wherep, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        return ret;
    }

    RDB_init_obj(&arr);
    ret = RDB_table_to_array(&arr, vtbp, 0, NULL, txp);
    if (ret != RDB_OK)
        goto cleanup;

    tbp->var.stored.indexc = RDB_array_length(&arr);
    if (tbp->var.stored.indexc > 0) {
        tbp->var.stored.indexv = malloc(sizeof(_RDB_tbindex)
                * tbp->var.stored.indexc);
        if (tbp->var.stored.indexv == NULL) {
            ret = RDB_NO_MEMORY;
            goto cleanup;
        }
        for (i = 0; i < tbp->var.stored.indexc; i++) {
            RDB_object *tplp;
            RDB_object *attrarrp;
            char *idxname;
            char *p;
            _RDB_tbindex *indexp = &tbp->var.stored.indexv[i];

            ret = RDB_array_get(&arr, (RDB_int) i, &tplp);
            if (ret != RDB_OK)
                goto cleanup;

            idxname = RDB_tuple_get_string(tplp, "NAME");
            if (idxname[0] != '\0') {
                indexp->name = RDB_dup_str(idxname);
                if (indexp->name == NULL) {
                    ret = RDB_NO_MEMORY;
                    goto cleanup;
                }
            }

            attrarrp = RDB_tuple_get(tplp, "ATTRS");
            indexp->attrc = RDB_array_length(attrarrp);
            if (indexp->attrc < 0) {
                ret = indexp->attrc;
                goto cleanup;
            }
            
            indexp->attrv = malloc(sizeof (RDB_seq_item) * indexp->attrc);
            if (indexp->attrv == NULL) {
                ret = RDB_NO_MEMORY;
                goto cleanup;
            }
            
            for (j = 0; j < indexp->attrc; j++) {
                RDB_object *attrtplp;                

                ret = RDB_array_get(attrarrp, (RDB_int) j, &attrtplp);
                if (ret != RDB_OK)
                    goto cleanup;

                indexp->attrv[j].attrname = RDB_dup_str(RDB_tuple_get_string(
                        attrtplp, "NAME"));
                if (indexp->attrv[j].attrname == NULL) {
                    ret = RDB_NO_MEMORY;
                    goto cleanup;
                }
                indexp->attrv[j].asc = RDB_tuple_get_bool(attrtplp, "ASC");
            }

            indexp->unique = RDB_tuple_get_bool(tplp, "UNIQUE");

            /* Don't open the index if it's a primary index */
            p = strchr(indexp->name, '$');
            if (p == NULL || strcmp (p, "$0") != 0) {
                ret = _RDB_open_table_index(tbp, indexp, dbrootp->envp, txp);
                if (ret != RDB_OK)
                    goto cleanup;
            } else {
                indexp->idxp = NULL;
            }
        }
    }

cleanup:
    RDB_destroy_obj(&arr);
    RDB_drop_table(vtbp, NULL);
    return ret;
}

/*
 * Create or open system tables.
 * If the tables are created, associate them with the database pointed to by
 * txp->dbp.
 */
int
_RDB_open_systables(RDB_dbroot *dbrootp, RDB_transaction *txp)
{
    int ret;
    RDB_attr keysattr;
    RDB_bool create = RDB_FALSE;

    /* create or open catalog tables */

    for(;;) {
        ret = _RDB_provide_table("SYS_TABLEATTRS", RDB_TRUE, 4, table_attr_attrv,
                1, table_attr_keyv, RDB_FALSE, create, txp, dbrootp->envp,
                &dbrootp->table_attr_tbp);
        if (!create && ret == RDB_NOT_FOUND) {
            /* Table not found, so create it */
            create = RDB_TRUE;
        } else {
            break;
        }
    }       
    if (ret != RDB_OK) {
        return ret;
    }
        
    ret = _RDB_provide_table("SYS_TABLEATTR_DEFVALS", RDB_TRUE,
            3, table_attr_defvals_attrv, 1, table_attr_defvals_keyv,
            RDB_FALSE, create, txp, dbrootp->envp,
            &dbrootp->table_attr_defvals_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_provide_table("SYS_RTABLES", RDB_TRUE, 3, rtables_attrv,
            1, rtables_keyv, RDB_FALSE, create,
            txp, dbrootp->envp, &dbrootp->rtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_provide_table("SYS_VTABLES", RDB_TRUE, 3, vtables_attrv,
            1, vtables_keyv, RDB_FALSE, create, txp, dbrootp->envp,
            &dbrootp->vtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_provide_table("SYS_DBTABLES", RDB_TRUE, 2, dbtables_attrv, 1, dbtables_keyv,
            RDB_FALSE, create, txp, dbrootp->envp, &dbrootp->dbtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    keysattr.name = "KEY";
    keysattr.typ = &RDB_STRING;
    keys_attrv[2].typ = RDB_create_relation_type(1, &keysattr);

    ret = _RDB_provide_table("SYS_KEYS", RDB_TRUE, 3, keys_attrv, 1, keys_keyv,
            RDB_FALSE, create, txp, dbrootp->envp, &dbrootp->keys_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_provide_table("SYS_TYPES", RDB_TRUE, 4, types_attrv, 1, types_keyv,
            RDB_FALSE, create, txp, dbrootp->envp, &dbrootp->types_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_provide_table("SYS_POSSREPS", RDB_TRUE, 3, possreps_attrv,
            1, possreps_keyv, RDB_FALSE, create, txp, dbrootp->envp,
            &dbrootp->possreps_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_provide_table("SYS_POSSREPCOMPS", RDB_TRUE, 5, possrepcomps_attrv,
            2, possrepcomps_keyv, RDB_FALSE, create, txp, dbrootp->envp,
            &dbrootp->possrepcomps_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ro_ops_attrv[1].typ = RDB_create_array_type(&RDB_BINARY);
    if (ro_ops_attrv[1].typ == NULL)
        return RDB_NO_MEMORY;

    ret = _RDB_provide_table("SYS_RO_OPS", RDB_TRUE, 5, ro_ops_attrv,
            1, ro_ops_keyv, RDB_FALSE, create, txp, dbrootp->envp,
            &dbrootp->ro_ops_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_provide_table("SYS_RO_OP_RTYPES", RDB_TRUE, 2, ro_op_rtypes_attrv,
            1, ro_op_rtypes_keyv, RDB_FALSE, create, txp, dbrootp->envp,
            &dbrootp->ro_op_rtypes_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    upd_ops_attrv[1].typ = RDB_create_array_type(&RDB_BINARY);
    if (upd_ops_attrv[1].typ == NULL)
        return RDB_NO_MEMORY;
    upd_ops_attrv[5].typ = RDB_create_array_type(&RDB_BOOLEAN);
    if (upd_ops_attrv[5].typ == NULL)
        return RDB_NO_MEMORY;

    ret = _RDB_provide_table("SYS_UPD_OPS", RDB_TRUE, 6, upd_ops_attrv,
            1, upd_ops_keyv, RDB_FALSE, create, txp, dbrootp->envp,
            &dbrootp->upd_ops_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    indexes_attrv[2].typ = RDB_create_array_type(
        RDB_create_tuple_type(2, indexes_attrs_attrv));

    ret = _RDB_provide_table("SYS_INDEXES", RDB_TRUE, 4, indexes_attrv,
            1, indexes_keyv, RDB_FALSE, create, txp, dbrootp->envp,
            &dbrootp->indexes_tbp);
    if (ret != RDB_OK) {
        return ret;
    }

    if (!create && (dbrootp->rtables_tbp->var.stored.indexc == -1)) {
        /*
         * Read indexes from the catalog 
         */

        ret = get_indexes(dbrootp->rtables_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->table_attr_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->table_attr_defvals_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->vtables_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->dbtables_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->keys_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->types_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->possreps_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->possrepcomps_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->ro_ops_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->ro_op_rtypes_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->upd_ops_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;

        ret = get_indexes(dbrootp->indexes_tbp, dbrootp, txp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}

int
_RDB_create_db_in_cat(RDB_transaction *txp)
{
    int ret;

    ret = _RDB_cat_insert(txp->dbp->dbrootp->table_attr_tbp, txp);
    if (ret != RDB_OK) {
        if (ret == RDB_ELEMENT_EXISTS) {
            /*
             * The catalog table already exists, but not in this database,
             * so associate it with this database too
             */
            ret = dbtables_insert(txp->dbp->dbrootp->table_attr_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->rtables_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->vtables_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->dbtables_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->keys_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->types_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->possreps_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->possrepcomps_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->ro_ops_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->ro_op_rtypes_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->upd_ops_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->indexes_tbp, txp);
        }
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->rtables_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->vtables_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->dbtables_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->keys_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->types_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->possreps_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->possrepcomps_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->ro_ops_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->ro_op_rtypes_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->upd_ops_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->indexes_tbp, txp);

    return ret;
}

static int
get_key(RDB_table *tbp, RDB_string_vec *keyp)
{
    int ret;
    int i;
    RDB_object attrarr;
    RDB_object *tplp;

    RDB_init_obj(&attrarr);
    ret = RDB_table_to_array(&attrarr, tbp, 0, NULL, NULL);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&attrarr);  
        return ret;
    }
    keyp->strc = RDB_array_length(&attrarr);
    if (keyp->strc < 0) {
        RDB_destroy_obj(&attrarr);  
        ret = keyp->strc;
        return ret;
    }
    keyp->strv = malloc(sizeof (char *) * keyp->strc);
    if (keyp->strv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    for (i = 0; i < keyp->strc; i++)
        keyp->strv[i] = NULL;

    for (i = 0; i < keyp->strc; i++) {
        ret = RDB_array_get(&attrarr, i, &tplp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&attrarr);
            goto error;
        }
        keyp->strv[i] = RDB_dup_str(RDB_tuple_get_string(tplp, "KEY"));
        if (keyp->strv[i] == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }

    RDB_destroy_obj(&attrarr);  
    return RDB_OK;

error:
    if (keyp->strv != NULL) {
        for (i = 0; i < keyp->strc; i++)
            free(keyp->strv[i]);
        free(keyp->strv);
    }
    RDB_destroy_obj(&attrarr);
    return ret;
}

static int
get_keys(const char *name, RDB_transaction *txp,
         int *keycp, RDB_string_vec **keyvp)
{
    RDB_expression *wherep;
    RDB_table *vtbp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    *keyvp = NULL;

    RDB_init_obj(&arr);
    
    wherep = RDB_eq(RDB_string_to_expr(name),
            RDB_expr_attr("TABLENAME"));
    if (wherep == NULL)
        return RDB_NO_MEMORY;

    ret = RDB_select(txp->dbp->dbrootp->keys_tbp, wherep, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        return ret;
    }

    ret = RDB_table_to_array(&arr, vtbp, 0, NULL, txp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_array_length(&arr);
    if (ret < 0)
        goto error;
    *keycp = ret;

    *keyvp = malloc(sizeof(RDB_string_vec) * *keycp);
    if (*keyvp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    for (i = 0; i < *keycp; i++)
        (*keyvp)[i].strv = NULL;

    for (i = 0; i < *keycp; i++) {
        RDB_int kno;
        ret = RDB_array_get(&arr, i, &tplp);
        if (ret != RDB_OK) {
            goto error;
        }
        kno = RDB_tuple_get_int(tplp, "KEYNO");

        ret = get_key(RDB_obj_table(RDB_tuple_get(tplp, "ATTRS")),
                &(*keyvp)[kno]);
        if (ret != RDB_OK) {
            goto error;
        }
    }
    ret = RDB_destroy_obj(&arr);
    RDB_drop_table(vtbp, txp);

    return ret;
error:
    RDB_destroy_obj(&arr);
    RDB_drop_table(vtbp, txp);
    if (*keyvp != NULL) {
        int i, j;
    
        for (i = 0; i < *keycp; i++) {
            if ((*keyvp)[i].strv != NULL) {
                for (j = 0; j < (*keyvp)[i].strc; j++)
                    free((*keyvp)[i].strv[j]);
            }
        }
        free(*keyvp);
    }
    return ret;
}

int
_RDB_get_cat_rtable(const char *name, RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_expression *exprp;
    RDB_table *tmptb1p = NULL;
    RDB_table *tmptb2p = NULL;
    RDB_table *tmptb3p = NULL;
    RDB_object arr;
    RDB_object tpl;
    RDB_object *tplp;
    RDB_bool usr;
    int ret;
    RDB_int i, j;
    int attrc;
    RDB_attr *attrv = NULL;
    int defvalc;
    int keyc;
    RDB_string_vec *keyv;
    RDB_transaction tx;

    /* Read real table data from the catalog */

    RDB_init_obj(&arr);
    RDB_init_obj(&tpl);

    exprp = RDB_eq(RDB_expr_attr("TABLENAME"),
            RDB_string_to_expr(name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    ret = RDB_select(txp->dbp->dbrootp->rtables_tbp, exprp, &tmptb1p);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    
    ret = RDB_extract_tuple(tmptb1p, txp, &tpl);

    if (ret != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "IS_USER");

    exprp = RDB_eq(RDB_expr_attr("TABLENAME"),
            RDB_string_to_expr(name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    /*
     * Read attribute names and types
     */

    ret = RDB_select(txp->dbp->dbrootp->table_attr_tbp, exprp, &tmptb2p);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_table_to_array(&arr, tmptb2p, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    attrc = RDB_array_length(&arr);
    if (attrc > 0)
        attrv = malloc(sizeof(RDB_attr) * attrc);

    for (i = 0; i < attrc; i++) {
        RDB_object *typedatap;
        RDB_int fno;

        ret = RDB_array_get(&arr, i, &tplp);
        if (ret != RDB_OK)
            goto error;
        fno = RDB_tuple_get_int(tplp, "I_FNO");
        attrv[fno].name = RDB_dup_str(RDB_tuple_get_string(tplp, "ATTRNAME"));
        typedatap = RDB_tuple_get(tplp, "TYPE");

        ret = _RDB_deserialize_type(typedatap, txp, &attrv[fno].typ);
        if (ret != RDB_OK)
            goto error;
        
        attrv[fno].defaultp = NULL;
    }

    /*
     * Read default values
     */

    exprp = RDB_eq(RDB_expr_attr("TABLENAME"), RDB_string_to_expr(name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    ret = RDB_select(txp->dbp->dbrootp->table_attr_defvals_tbp, exprp, &tmptb3p);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_table_to_array(&arr, tmptb3p, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    defvalc = RDB_array_length(&arr);

    for (i = 0; i < defvalc; i++) {
        char *name;
        RDB_object *binvalp;

        RDB_array_get(&arr, i, &tplp);
        name = RDB_tuple_get_string(tplp, "ATTRNAME");
        binvalp = RDB_tuple_get(tplp, "DEFAULT_VALUE");
        
        /* Find attrv entry and set default value */
        for (i = 0; i < attrc && (strcmp(attrv[i].name, name) != 0); i++);
        if (i >= attrc) {
            /* Not found */
            ret = RDB_INTERNAL;
            goto error;
        }
        attrv[i].defaultp = malloc(sizeof (RDB_object));
        RDB_init_obj(attrv[i].defaultp);
        ret = RDB_irep_to_obj(attrv[i].defaultp, attrv[i].typ,
                binvalp->var.bin.datap, binvalp->var.bin.len);
        if (ret != RDB_OK)
            goto error;            
    }

    /* Open the table */

    ret = get_keys(name, txp, &keyc, &keyv);
    if (ret != RDB_OK)
        goto error;

    /* Open table in a subtransaction (required by Berkeley DB) */
    ret = RDB_begin_tx(&tx, txp->dbp, txp);
    if (ret != RDB_OK)
        goto error;
    
    ret = _RDB_provide_table(name, RDB_TRUE, attrc, attrv, keyc, keyv, usr,
            RDB_FALSE, &tx, tx.dbp->dbrootp->envp, tbpp);
    for (i = 0; i < keyc; i++) {
        for (j = 0; j < keyv[i].strc; j++)
            free(keyv[i].strv[j]);
    }
    free(keyv);

    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }

    ret = get_indexes(*tbpp, tx.dbp->dbrootp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }
    
    ret = RDB_commit(&tx);
    if (ret != RDB_OK)
        goto error;

    for (i = 0; i < attrc; i++) {
        free(attrv[i].name);
        if (attrv[i].defaultp != NULL) {
            RDB_destroy_obj(attrv[i].defaultp);
            free(attrv[i].defaultp);
        }
    }
    if (attrc > 0)
        free(attrv);

    _RDB_assoc_table_db(*tbpp, txp->dbp);

    ret = RDB_destroy_obj(&arr);

    RDB_drop_table(tmptb1p, txp);
    RDB_drop_table(tmptb2p, txp);

    RDB_destroy_obj(&tpl);

    return ret;

error:
    if (attrv != NULL) {
        for (i = 0; i < attrc; i++)
            free(attrv[i].name);
        free(attrv);
    }

    RDB_destroy_obj(&arr);

    if (tmptb1p != NULL)
        RDB_drop_table(tmptb1p, txp);
    if (tmptb2p != NULL)
        RDB_drop_table(tmptb2p, txp);
    if (tmptb3p != NULL)
        RDB_drop_table(tmptb3p, txp);

    RDB_destroy_obj(&tpl);

    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);    
    return ret;
}

int
_RDB_get_cat_vtable(const char *name, RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_expression *exprp;
    RDB_table *tmptbp = NULL;
    RDB_object tpl;
    RDB_object arr;
    RDB_object *valp;
    RDB_bool usr;
    int ret;

    /* read virtual table data from the catalog */

    RDB_init_obj(&arr);

    exprp = RDB_eq(RDB_expr_attr("TABLENAME"), RDB_string_to_expr(name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    ret = RDB_select(txp->dbp->dbrootp->vtables_tbp, exprp, &tmptbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    
    RDB_init_obj(&tpl);
    ret = RDB_extract_tuple(tmptbp, txp, &tpl);

    RDB_drop_table(tmptbp, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "IS_USER");

    valp = RDB_tuple_get(&tpl, "I_DEF");
    ret = _RDB_deserialize_table(valp, txp, tbpp);
    if (ret != RDB_OK)
        goto error;
    
    ret = RDB_destroy_obj(&arr);

    (*tbpp)->is_persistent = RDB_TRUE;

    (*tbpp)->name = RDB_dup_str(name);
    if ((*tbpp)->name == NULL)
        return RDB_NO_MEMORY;

    _RDB_assoc_table_db(*tbpp, txp->dbp);
    
    return ret;
error:
    RDB_destroy_obj(&arr);
    
    return ret;
}

static int
types_query(const char *name, RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_expression *exp;
    RDB_expression *wherep;
    int ret;

    exp = RDB_expr_attr("TYPENAME");
    if (exp == NULL)
        return RDB_NO_MEMORY;
    wherep = RDB_eq(exp, RDB_string_to_expr(name));
    if (wherep == NULL) {
        RDB_drop_expr(exp);
        return RDB_NO_MEMORY;
    }

    ret = RDB_select(txp->dbp->dbrootp->types_tbp, wherep, tbpp);
    if (ret != RDB_OK) {
         RDB_drop_expr(wherep);
         return ret;
    }
    return RDB_OK;
}

int
_RDB_possreps_query(const char *name, RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_expression *exp;
    RDB_expression *wherep;
    int ret;

    exp = RDB_expr_attr("TYPENAME");
    if (exp == NULL) {
        return RDB_NO_MEMORY;
    }
    wherep = RDB_eq(exp, RDB_string_to_expr(name));
    if (wherep == NULL) {
        RDB_drop_expr(exp);
        return RDB_NO_MEMORY;
    }

    ret = RDB_select(txp->dbp->dbrootp->possreps_tbp, wherep, tbpp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        return ret;
    }
    return RDB_OK;
}

int
_RDB_possrepcomps_query(const char *name, const char *possrepname,
        RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_expression *exp, *ex2p;
    RDB_expression *wherep;
    int ret;

    exp = RDB_expr_attr("TYPENAME");
    if (exp == NULL) {
        ret = RDB_NO_MEMORY;
        return ret;
    }
    wherep = RDB_eq(exp, RDB_string_to_expr(name));
    if (wherep == NULL) {
        RDB_drop_expr(exp);
        ret = RDB_NO_MEMORY;
        return ret;
    }
    exp = RDB_expr_attr("POSSREPNAME");
    if (exp == NULL) {
        RDB_drop_expr(wherep);
        ret = RDB_NO_MEMORY;
        return ret;
    }
    ex2p = RDB_eq(exp, RDB_string_to_expr(possrepname));
    if (ex2p == NULL) {
        RDB_drop_expr(exp);
        RDB_drop_expr(wherep);
        ret = RDB_NO_MEMORY;
        return ret;
    }
    exp = wherep;
    wherep = RDB_and(exp, ex2p);
    if (wherep == NULL) {
        RDB_drop_expr(exp);
        RDB_drop_expr(ex2p);
        ret = RDB_NO_MEMORY;
        return ret;
    }
    ret = RDB_select(txp->dbp->dbrootp->possrepcomps_tbp, wherep, tbpp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        return ret;
    }
    return RDB_OK;
}

int
_RDB_get_cat_type(const char *name, RDB_transaction *txp, RDB_type **typp)
{
    RDB_table *tmptb1p = NULL;
    RDB_table *tmptb2p = NULL;
    RDB_table *tmptb3p = NULL;
    RDB_object tpl;
    RDB_object *tplp;
    RDB_object possreps;
    RDB_object comps;
    RDB_type *typ = NULL;
    RDB_object *typedatap;
    int ret, tret;
    int i;

    RDB_init_obj(&tpl);
    RDB_init_obj(&possreps);
    RDB_init_obj(&comps);

    /*
     * Get type info from SYS_TYPES
     */

    ret = types_query(name, txp, &tmptb1p);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_extract_tuple(tmptb1p, txp, &tpl);
    if (ret != RDB_OK)
        goto error;

    typ = malloc(sizeof (RDB_type));
    if (typ == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    typ->kind = RDB_TP_SCALAR;
    typ->comparep = NULL;

    typedatap = RDB_tuple_get(&tpl, "I_AREP_TYPE");
    if (RDB_binary_length(typedatap) != 0) {   
        ret = _RDB_deserialize_type(typedatap, txp, &typ->var.scalar.arep);
        if (ret != RDB_OK)
            goto error;
    } else {
        typ->var.scalar.arep = NULL;
    }

    typ->name = RDB_dup_str(name);
    if (typ->name == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    typ->ireplen = RDB_tuple_get_int(&tpl, "I_AREP_LEN");
    typ->var.scalar.sysimpl = RDB_tuple_get_bool(&tpl, "I_SYSIMPL");
    typ->var.scalar.repc = 0;

    /*
     * Get possrep info from SYS_POSSREPS
     */

    ret = _RDB_possreps_query(name, txp, &tmptb2p);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_table_to_array(&possreps, tmptb2p, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    ret = RDB_array_length(&possreps);
    if (ret < 0) {
        goto error;
    }
    typ->var.scalar.repc = ret;
    if (ret > 0)
        typ->var.scalar.repv = malloc(ret * sizeof (RDB_ipossrep));
    for (i = 0; i < typ->var.scalar.repc; i++)
        typ->var.scalar.repv[i].compv = NULL;
    /*
     * Read possrep data from array and store it in typ->var.scalar.repv
     */
    for (i = 0; i < typ->var.scalar.repc; i++) {
        int j;

        ret = RDB_array_get(&possreps, (RDB_int) i, &tplp);
        if (ret != RDB_OK)
            goto error;
        typ->var.scalar.repv[i].name = RDB_dup_str(
                RDB_tuple_get_string(tplp, "POSSREPNAME"));
        if (typ->var.scalar.repv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }

        ret = _RDB_deserialize_expr(RDB_tuple_get(tplp, "I_CONSTRAINT"), txp,
                &typ->var.scalar.repv[i].constraintp);
        if (ret != RDB_OK) {
            goto error;
        }

        ret = _RDB_possrepcomps_query(name, typ->var.scalar.repv[i].name, txp,
                &tmptb3p);
        if (ret != RDB_OK) {
            goto error;
        }
        ret = RDB_table_to_array(&comps, tmptb3p, 0, NULL, txp);
        if (ret != RDB_OK) {
            goto error;
        }
        ret = RDB_array_length(&comps);
        if (ret < 0) {
            goto error;
        }
        typ->var.scalar.repv[i].compc = ret;
        if (ret > 0)
            typ->var.scalar.repv[i].compv = malloc(ret * sizeof (RDB_attr));
        else
            typ->var.scalar.repv[i].compv = NULL;

        /*
         * Read component data from array and store it in
         * typ->var.scalar.repv[i].compv.
         */
        for (j = 0; j < typ->var.scalar.repv[i].compc; j++) {
            RDB_int idx;

            ret = RDB_array_get(&comps, (RDB_int) j, &tplp);
            if (ret != RDB_OK)
                goto error;
            idx = RDB_tuple_get_int(tplp, "COMPNO");
            typ->var.scalar.repv[i].compv[idx].name = RDB_dup_str(
                    RDB_tuple_get_string(tplp, "COMPNAME"));
            if (typ->var.scalar.repv[i].compv[idx].name == NULL) {
                ret = RDB_NO_MEMORY;
                goto error;
            }
            ret = RDB_get_type(RDB_tuple_get_string(tplp, "COMPTYPENAME"),
                    txp, &typ->var.scalar.repv[i].compv[idx].typ);
            if (ret != RDB_OK)
                goto error;
        }
    }

    *typp = typ;

    ret = RDB_drop_table(tmptb1p, txp);
    tret = RDB_drop_table(tmptb2p, txp);
    if (ret == RDB_OK)
        ret = tret;
    tret = RDB_drop_table(tmptb3p, txp);
    if (ret == RDB_OK)
        ret = tret;

    RDB_destroy_obj(&tpl);
    tret = RDB_destroy_obj(&possreps);
    if (ret == RDB_OK)
        ret = tret;
    tret = RDB_destroy_obj(&comps);
    if (ret == RDB_OK)
        ret = tret;

    return ret;

error:
    if (tmptb1p != NULL)
        RDB_drop_table(tmptb1p, txp);
    if (tmptb2p != NULL)
        RDB_drop_table(tmptb2p, txp);
    if (tmptb3p != NULL)
        RDB_drop_table(tmptb3p, txp);
    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&possreps);
    RDB_destroy_obj(&comps);
    if (typ != NULL) {
        if (typ->var.scalar.repc != 0) {
            for (i = 0; i < typ->var.scalar.repc; i++)
                free(typ->var.scalar.repv[i].compv);
            free(typ->var.scalar.repv);
        }
        free(typ->name);
        free(typ);
    }
    return ret;
}

int
_RDB_get_cat_rtype(const char *opname, RDB_transaction *txp, RDB_type **typp)
{
    int ret;
    RDB_expression *wherep;
    RDB_table *vtbp;
    RDB_object tpl;

    /*
     * Build query
     */

    wherep = RDB_eq(RDB_expr_attr("NAME"), RDB_string_to_expr(opname));
    if (wherep == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    ret = RDB_select(txp->dbp->dbrootp->ro_op_rtypes_tbp, wherep, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        RDB_rollback_all(txp);
        return ret;
    }

    /*
     * Read tuple
     */

    RDB_init_obj(&tpl);
    ret = RDB_extract_tuple(vtbp, txp, &tpl);
    RDB_drop_table(vtbp, txp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        RDB_destroy_obj(&tpl);
        return ret;
    }

    /*
     * Get type
     */

    ret = _RDB_deserialize_type(RDB_tuple_get(&tpl, "RTYPE"), txp, typp);
    RDB_destroy_obj(&tpl);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }

    return RDB_OK;
}

int
_RDB_make_typesobj(int argc, RDB_type *argtv[], RDB_object *objp)
{
    int i;
    int ret;
    RDB_object typeobj;

    RDB_set_array_length(objp, argc);
    RDB_init_obj(&typeobj);
    for (i = 0; i < argc; i++) {
        ret = _RDB_type_to_obj(&typeobj, argtv[i]);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typeobj);
            return ret;
        }
        ret = RDB_array_set(objp, (RDB_int) i, &typeobj);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typeobj);
            return ret;
        }
    }
    RDB_destroy_obj(&typeobj);
    return RDB_OK;
}

/* Read read-only operator from database */
int
_RDB_get_cat_ro_op(const char *name, int argc, RDB_type *argtv[],
        RDB_transaction *txp, RDB_ro_op **opp)
{
    RDB_expression *exp;
    RDB_table *vtbp;
    RDB_object tpl;
    RDB_object typesobj;
    int i;
    int ret;
    char *libname, *symname;
    RDB_ro_op *op = NULL;

    RDB_init_obj(&typesobj);
    ret = _RDB_make_typesobj(argc, argtv, &typesobj);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        RDB_destroy_obj(&typesobj);
        return ret;
    }

    exp = RDB_and(
            RDB_eq(RDB_expr_attr("NAME"),
                   RDB_string_to_expr(name)),
            RDB_eq(RDB_expr_attr("ARGTYPES"),
                   RDB_obj_to_expr(&typesobj)));
    RDB_destroy_obj(&typesobj);
    if (exp == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }
    ret = RDB_select(txp->dbp->dbrootp->ro_ops_tbp, exp, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exp);
        return ret;
    }
    RDB_init_obj(&tpl);
    ret = RDB_extract_tuple(vtbp, txp, &tpl);
    RDB_drop_table(vtbp, txp);
    if (ret != RDB_OK)
        goto error;

    op = malloc(sizeof (RDB_ro_op));
    if (op == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    RDB_init_obj(&op->iarg);

    op->argtv = NULL;
    op->name = RDB_dup_str(RDB_tuple_get_string(&tpl, "NAME"));
    if (op->name == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    op->argc = argc;        
    op->argtv = malloc(sizeof (RDB_type *) * op->argc);
    if (op->argtv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    for (i = 0; i < op->argc; i++) {
        op->argtv[i] = argtv[i];
    }

    ret = _RDB_get_cat_rtype(name, txp, &op->rtyp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_copy_obj(&op->iarg, RDB_tuple_get(&tpl, "IARG"));
    if (ret != RDB_OK)
        goto error;
    libname = RDB_tuple_get_string(&tpl, "LIB");
    op->modhdl = lt_dlopenext(libname);
    if (op->modhdl == NULL) {
        RDB_errmsg(txp->dbp->dbrootp->envp, "Cannot open library \"%s\"",
                libname);
        ret = RDB_RESOURCE_NOT_FOUND;
        goto error;
    }
    symname = RDB_tuple_get_string(&tpl, "SYMBOL");
    op->funcp = (RDB_ro_op_func *) lt_dlsym(op->modhdl, symname);
    if (op->funcp == NULL) {
        RDB_errmsg(txp->dbp->dbrootp->envp, "Symbol \"%s\" not found",
                symname);
        ret = RDB_RESOURCE_NOT_FOUND;
        goto error;
    }

    RDB_destroy_obj(&tpl);

    *opp = op;
    return RDB_OK;

error:
    if (op != NULL) {
        RDB_destroy_obj(&op->iarg);
        free(op->name);
        free(op->argtv);
        free(op);
    }

    RDB_destroy_obj(&tpl);
    return ret;
}

/* Read update operator from database */
int
_RDB_get_cat_upd_op(const char *name, int argc, RDB_type *argtv[],
        RDB_transaction *txp, RDB_upd_op **opp)
{
    RDB_expression *exp;
    RDB_table *vtbp;
    RDB_object tpl;
    RDB_object typesobj;
    int i;
    int ret;
    char *libname, *symname;
    RDB_upd_op *op = NULL;
    RDB_object *updvobjp, *updobjp;

    RDB_init_obj(&typesobj);
    ret = _RDB_make_typesobj(argc, argtv, &typesobj);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        RDB_destroy_obj(&typesobj);
        return ret;
    }
        
    exp = RDB_and(
            RDB_eq(RDB_expr_attr("NAME"),
                   RDB_string_to_expr(name)),
            RDB_eq(RDB_expr_attr("ARGTYPES"),
                   RDB_obj_to_expr(&typesobj)));
    RDB_destroy_obj(&typesobj);
    if (exp == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }
    ret = RDB_select(txp->dbp->dbrootp->upd_ops_tbp, exp, &vtbp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        RDB_drop_expr(exp);
        return ret;
    }
    RDB_init_obj(&tpl);
    ret = RDB_extract_tuple(vtbp, txp, &tpl);
    RDB_drop_table(vtbp, txp);
    if (ret != RDB_OK)
        goto error;

    op = malloc(sizeof (RDB_ro_op));
    if (op == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    RDB_init_obj(&op->iarg);

    op->argtv = NULL;
    op->updv = NULL;
    op->name = RDB_dup_str(RDB_tuple_get_string(&tpl, "NAME"));
    if (op->name == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    op->argc = argc;        
    op->argtv = malloc(sizeof(RDB_type *) * op->argc);
    if (op->argtv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    for (i = 0; i < op->argc; i++) {
        op->argtv[i] = argtv[i];
    }

    ret = RDB_copy_obj(&op->iarg, RDB_tuple_get(&tpl, "IARG"));
    if (ret != RDB_OK)
        goto error;

    op->updv = malloc(op->argc);
    if (op->updv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    updvobjp = RDB_tuple_get(&tpl, "UPDV");
    for (i = 0; i < op->argc; i++) {
        ret = RDB_array_get(updvobjp, (RDB_int) i, &updobjp);
        if (ret != RDB_OK)
            goto error;
        op->updv[i] = RDB_obj_bool(updobjp);
    }
        
    libname = RDB_tuple_get_string(&tpl, "LIB");
    op->modhdl = lt_dlopenext(libname);
    if (op->modhdl == NULL) {
        RDB_errmsg(txp->dbp->dbrootp->envp, "Cannot open library \"%s\"",
                libname);
        ret = RDB_RESOURCE_NOT_FOUND;
        goto error;
    }
    symname = RDB_tuple_get_string(&tpl, "SYMBOL");
    op->funcp = (RDB_upd_op_func *) lt_dlsym(op->modhdl, symname);
    if (op->funcp == NULL) {
        RDB_errmsg(txp->dbp->dbrootp->envp, "Symbol \"%s\" not found",
                symname);
        ret = RDB_RESOURCE_NOT_FOUND;
        goto error;
    }

    RDB_destroy_obj(&tpl);

    *opp = op;
    return RDB_OK;

error:
    if (op != NULL) {
        RDB_destroy_obj(&op->iarg);
        free(op->name);
        free(op->argtv);
        free(op->updv);
        free(op);
    }

    RDB_destroy_obj(&tpl);
    return ret;
}
