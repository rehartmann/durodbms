/*
 * Copyright (C) 2003 René Hartmann.
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
            { "TYPE", &RDB_STRING, NULL, 0 },
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
    { "ATTRS", &RDB_STRING, NULL, 0 }
};
static char *keys_keyattrv[] = { "TABLENAME", "KEYNO" };
static RDB_string_vec keys_keyv[] = { { 2, keys_keyattrv } };

static RDB_attr types_attrv[] = {
    { "TYPENAME", &RDB_STRING, NULL, 0 },
    { "I_LIBNAME", &RDB_STRING, NULL, 0 },
    { "I_AREP_LEN", &RDB_INTEGER, NULL, 0 },
    { "I_AREP_TYPE", &RDB_STRING, NULL, 0 }
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
    { "ARGTYPES", &RDB_STRING, NULL, 0 },
    { "RTYPE", &RDB_STRING, NULL, 0 },
    { "LIB", &RDB_STRING, NULL, 0 },
    { "SYMBOL", &RDB_STRING, NULL, 0 },
    { "IARG", &RDB_BINARY, NULL, 0 }
};

static char *ro_ops_keyattrv[] = { "NAME", "ARGTYPES" };
static RDB_string_vec ro_ops_keyv[] = { { 2, ro_ops_keyattrv } };

static RDB_attr upd_ops_attrv[] = {
    { "NAME", &RDB_STRING, NULL, 0 },
    { "ARGTYPES", &RDB_STRING, NULL, 0 },
    { "LIB", &RDB_STRING, NULL, 0 },
    { "SYMBOL", &RDB_STRING, NULL, 0 },
    { "IARG", &RDB_BINARY, NULL, 0 },
    { "UPDV", &RDB_BINARY, NULL, 0 }
};

static char *upd_ops_keyattrv[] = { "NAME", "ARGTYPES" };
static RDB_string_vec upd_ops_keyv[] = { { 2, upd_ops_keyattrv } };

static RDB_attr tuple_attrs_attrv[] = {
    { "TYPEKEY", &RDB_STRING, NULL, 0 },
    { "ATTRNAME", &RDB_STRING, NULL, 0 },
    { "TYPE", &RDB_STRING, NULL, 0 },
    { "I_ATTRNO", &RDB_INTEGER, NULL, 0 }
};

static char *tuple_attrs_keyattrv[] = { "TYPEKEY", "ATTRNAME" };
static RDB_string_vec tuple_attrs_keyv[] = { { 2, tuple_attrs_keyattrv } };

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

static char *
new_nstypekey(const char *tbname, const char *attrname)
{
    int tbnamelen = strlen(tbname);
    char *typename = malloc(tbnamelen + strlen(attrname) + 2);

    if (typename == NULL)
        return NULL;

    /* Type key is <tablename>$<attribute name> */
    strcpy(typename, tbname);
    typename[tbnamelen] = '$';
    strcpy(typename + tbnamelen + 1, attrname);
    return typename;
}

/*
 * Store the definition of a tuple type in the catalog.
 */
static int
insert_tuptype(RDB_type *tuptyp, const char *key, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_object tpl;

    RDB_init_obj(&tpl);
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        RDB_type *attrtyp = tuptyp->var.tuple.attrv[i].typ;
        char *attrname = tuptyp->var.tuple.attrv[i].name;

        ret = RDB_tuple_set_string(&tpl, "TYPEKEY", key);
        if (ret != RDB_OK)
            goto cleanup;

        ret = RDB_tuple_set_string(&tpl, "ATTRNAME", attrname);
        if (ret != RDB_OK)
            goto cleanup;

        if (RDB_type_is_scalar(attrtyp)) {
            ret = RDB_tuple_set_string(&tpl, "TYPE",
                    RDB_type_name(tuptyp->var.tuple.attrv[i].typ));
            if (ret != RDB_OK)
                 goto cleanup;
        } else if (attrtyp->kind == RDB_TP_TUPLE) {
            /* Attribute has tuple type */
            char *attrkey;

            attrkey = new_nstypekey(key, attrname);
            ret = RDB_tuple_set_string(&tpl, "TYPE", attrkey);
            if (ret != RDB_OK) {
                 free(attrkey);
                 goto cleanup;
            }
            ret = insert_tuptype(attrtyp, attrkey, txp);
            free(attrkey);
            if (ret != RDB_OK)
                goto cleanup;
        } else {
            ret = RDB_NOT_SUPPORTED;
            goto cleanup;
        }

        ret = RDB_tuple_set_int(&tpl, "I_ATTRNO", (RDB_int)i);
        if (ret != RDB_OK)
            goto cleanup;

        ret = RDB_insert(txp->dbp->dbrootp->tuple_attrs_tbp, &tpl, txp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);
    return ret;
}

static int
delete_tuptype(RDB_type *typ, const char *key, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_expression *exprp = RDB_eq(RDB_expr_attr("TYPEKEY"),
            RDB_string_const(key));

    if (exprp == NULL)
        return RDB_NO_MEMORY;

    ret = RDB_delete(txp->dbp->dbrootp->tuple_attrs_tbp, exprp, txp);
    RDB_drop_expr(exprp);
    if (ret != RDB_OK)
        return ret;

    /* Delete tuple attributes */
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        RDB_type *attrtyp = typ->var.tuple.attrv[i].typ;
        char *attrname = typ->var.tuple.attrv[i].name;

        if (attrtyp->kind == RDB_TP_TUPLE) {
            char *attrkey = new_nstypekey(key, attrname);

            ret = delete_tuptype(attrtyp, attrkey, txp);
            free(attrkey);
            if (ret != RDB_OK)
                return ret;
        }
    }
    return RDB_OK;
}

/* Insert the table pointed to by tbp into the catalog. */
static int
insert_rtable(RDB_table *tbp, RDB_transaction *txp)
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
    ret = RDB_insert(txp->dbp->dbrootp->rtables_tbp, &tpl, txp);
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
        char *attrname = tuptyp->var.tuple.attrv[i].name;
        RDB_type *attrtyp = tuptyp->var.tuple.attrv[i].typ;
        char *typename;

        if (!RDB_type_is_scalar(attrtyp)) {
            /*
             * Special handling for tuple-valued attributes
             */

            char *typekey = new_nstypekey(RDB_table_name(tbp), attrname);
            if (typekey == NULL)
                return RDB_NO_MEMORY;

            /* Insert tuple type definition into the cataog */
            if (attrtyp->kind == RDB_TP_TUPLE) {
                ret = insert_tuptype(tuptyp->var.tuple.attrv[i].typ, typekey, txp);
                free(typekey);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&tpl);
                    return ret;
                }
            } else {
                return RDB_NOT_SUPPORTED;
            }
            typename = "";
        } else {
            typename = RDB_type_name(tuptyp->var.tuple.attrv[i].typ);
        }

        ret = RDB_tuple_set_string(&tpl, "TYPE", typename);
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
        ret = RDB_insert(txp->dbp->dbrootp->table_attr_tbp, &tpl, txp);
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

            ret = RDB_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, &tpl, txp);
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
        RDB_string_vec *kap = &tbp->keyv[i];
        size_t buflen;
        char *buf;

        ret = RDB_tuple_set_int(&tpl, "KEYNO", i);
        if (ret != RDB_OK)
            goto cleanup;

        /* Allocate buffer */
        buflen = 1;
        if (kap->strc > 0) {
            buflen += strlen (kap->strv[0]);
            for (j = 1; j < kap->strc; j++) {
                buflen += strlen(kap->strv[j]) + 1;
            }
        }
        buf = malloc(buflen);
        if (buf == NULL) {
            ret = RDB_NO_MEMORY;
            goto cleanup;
        }

        /* Concatenate attribute names */
        buf[0] = '\0';
        if (kap->strc > 0) {
            strcpy(buf, kap->strv[0]);
            for (j = 1; j < kap->strc; j++) {
                strcat(buf, " ");
                strcat(buf, kap->strv[j]);
            }
        }

        ret = RDB_tuple_set_string(&tpl, "ATTRS", buf);
        free(buf);
        if (ret != RDB_OK)
            goto cleanup;

        ret = RDB_insert(txp->dbp->dbrootp->keys_tbp, &tpl, txp);
        if (ret != RDB_OK)
            goto cleanup;
    }
    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);

    return ret;
}

static int
insert_vtable(RDB_table *tbp, RDB_transaction *txp)
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

    ret = _RDB_table_to_obj(tbp, &defval);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "I_DEF", &defval);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->vtables_tbp, &tpl, txp);
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
_RDB_cat_insert(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    /*
     * Create table in the catalog.
     */
    if (tbp->kind == RDB_TB_STORED)
        ret = insert_rtable(tbp, txp);
    else
        ret = insert_vtable(tbp, txp);

    /* If the table already exists in the catalog, proceed */
    if (ret != RDB_OK && ret != RDB_ELEMENT_EXISTS) {
        return ret;
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
    int i;
    RDB_expression *exprp = RDB_eq(RDB_expr_attr("TABLENAME"),
                   RDB_string_const(tbp->name));
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

    /* Delete non-scalar types */
    for (i = 0; i < tbp->typ->var.basetyp->var.tuple.attrc; i++) {
        RDB_attr *attrp = &tbp->typ->var.basetyp->var.tuple.attrv[i];

        if (attrp->typ->kind == RDB_TP_TUPLE) {
            char *typekey = new_nstypekey(tbp->name, attrp->name);
            ret = delete_tuptype(attrp->typ, typekey, txp);
            free(typekey);
            if (ret != RDB_OK)
                goto cleanup;
        }
    }

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
                   RDB_string_const(tbp->name));
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
 * Create or open system tables, depending on the create argument.
 * If the tables are created, associate them with the database pointed to by
 * txp->dbp.
 */
int
_RDB_open_systables(RDB_transaction *txp)
{
    int ret;
    RDB_bool create = RDB_FALSE;

    /* create or open catalog tables */

    for(;;) {
        ret = _RDB_provide_table("SYS_TABLEATTRS", RDB_TRUE, 4, table_attr_attrv,
                1, table_attr_keyv, RDB_FALSE, create, txp,
                &txp->dbp->dbrootp->table_attr_tbp);
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
        
    _RDB_assign_table_db(txp->dbp->dbrootp->table_attr_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_TABLEATTR_DEFVALS", RDB_TRUE, 3, table_attr_defvals_attrv,
            1, table_attr_defvals_keyv, RDB_FALSE, create, txp,
            &txp->dbp->dbrootp->table_attr_defvals_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->table_attr_defvals_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_RTABLES", RDB_TRUE, 3, rtables_attrv, 1, rtables_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->rtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->rtables_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_VTABLES", RDB_TRUE, 3, vtables_attrv, 1, vtables_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->vtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->vtables_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_DBTABLES", RDB_TRUE, 2, dbtables_attrv, 1, dbtables_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->dbtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->dbtables_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_KEYS", RDB_TRUE, 3, keys_attrv, 1, keys_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->keys_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->keys_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_TYPES", RDB_TRUE, 4, types_attrv, 1, types_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->types_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->types_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_POSSREPS", RDB_TRUE, 3, possreps_attrv,
            1, possreps_keyv, RDB_FALSE, create, txp, &txp->dbp->dbrootp->possreps_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->possreps_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_POSSREPCOMPS", RDB_TRUE, 5, possrepcomps_attrv,
            2, possrepcomps_keyv, RDB_FALSE, create, txp,
            &txp->dbp->dbrootp->possrepcomps_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->possrepcomps_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_RO_OPS", RDB_TRUE, 6, ro_ops_attrv,
            1, ro_ops_keyv, RDB_FALSE, create, txp,
            &txp->dbp->dbrootp->ro_ops_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->ro_ops_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_UPD_OPS", RDB_TRUE, 6, upd_ops_attrv,
            1, upd_ops_keyv, RDB_FALSE, create, txp,
            &txp->dbp->dbrootp->upd_ops_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->upd_ops_tbp, txp->dbp);

    ret = _RDB_provide_table("SYS_TUPLE_ATTRS", RDB_TRUE, 4, tuple_attrs_attrv,
            1, tuple_attrs_keyv, RDB_FALSE, create, txp,
            &txp->dbp->dbrootp->tuple_attrs_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->tuple_attrs_tbp, txp->dbp);

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
             * so assign it to this database too
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
            ret = dbtables_insert(txp->dbp->dbrootp->upd_ops_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = dbtables_insert(txp->dbp->dbrootp->tuple_attrs_tbp, txp);
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

    ret = _RDB_cat_insert(txp->dbp->dbrootp->upd_ops_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_cat_insert(txp->dbp->dbrootp->tuple_attrs_tbp, txp);

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
    
    wherep = RDB_eq(RDB_string_const(name),
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
        int attrc;
    
        ret = RDB_array_get(&arr, i, &tplp);
        if (ret != RDB_OK) {
            goto error;
        }
        kno = RDB_tuple_get_int(tplp, "KEYNO");
        attrc = RDB_split_str(RDB_tuple_get_string(tplp, "ATTRS"),
                &(*keyvp)[kno].strv);
        if (attrc == -1) {
            (*keyvp)[kno].strv = NULL;
            ret = RDB_INTERNAL;
            goto error;
        }
        (*keyvp)[kno].strc = attrc;
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

static int
get_tuple_type(const char *typekey, RDB_transaction *txp, RDB_type **typp)
{
    int ret;
    int i;
    RDB_object arr;
    RDB_expression *wherep = RDB_eq(RDB_expr_attr("TYPEKEY"),
            RDB_string_const(typekey));
    RDB_table *vtbp;
    int attrc;
    RDB_attr *attrv;
    if (wherep == NULL)
        return RDB_NO_MEMORY;

    ret = RDB_select(txp->dbp->dbrootp->tuple_attrs_tbp, wherep, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        return ret;
    }

    attrv = NULL;
    RDB_init_obj(&arr);
    ret = RDB_table_to_array(&arr, vtbp, 0, NULL, txp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_array_length(&arr);
    if (ret < 0)
        goto cleanup;
    attrc = ret;
    if (attrc > 0)
        attrv = malloc(sizeof (RDB_attr) * attrc);

    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp;
        RDB_object *tplp;
        char *attrtypekey;

        ret = RDB_array_get(&arr, i, &tplp);
        if (ret != RDB_OK)
            goto cleanup;
        attrp = &attrv[RDB_tuple_get_int(tplp, "I_ATTRNO")];
        attrp->name = RDB_dup_str(RDB_tuple_get_string(tplp, "ATTRNAME"));
        if (attrp->name == NULL) {
            ret = RDB_NO_MEMORY;
            goto cleanup;
        }

        attrtypekey = RDB_tuple_get_string(tplp, "TYPE");
        if (strchr(attrtypekey, '$') == NULL)
            ret = RDB_get_type(attrtypekey, txp, &attrp->typ);
        else
            ret = get_tuple_type(attrtypekey, txp, &attrp->typ);
        if (ret != RDB_OK)
            goto cleanup;
    }
    *typp = RDB_create_tuple_type(attrc, attrv);
    if (*typp == NULL)
        ret = RDB_NO_MEMORY;
    else
        ret = RDB_OK;

cleanup:
    if (attrv != NULL) {
        for (i = 0; i < attrc; i++) {
            free(attrv[i].name);
        }
        free(attrv);
    }
    RDB_destroy_obj(&arr);
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
            RDB_string_const(name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    ret = RDB_select(txp->dbp->dbrootp->rtables_tbp, exprp, &tmptb1p);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    
    ret = RDB_extract_tuple(tmptb1p, &tpl, txp);

    if (ret != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "IS_USER");

    exprp = RDB_eq(RDB_expr_attr("TABLENAME"),
            RDB_string_const(name));
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
        char *typename;
        RDB_type *attrtyp;
        RDB_int fno;

        ret = RDB_array_get(&arr, i, &tplp);
        if (ret != RDB_OK)
            goto error;
        fno = RDB_tuple_get_int(tplp, "I_FNO");
        attrv[fno].name = RDB_dup_str(RDB_tuple_get_string(tplp, "ATTRNAME"));
        typename = RDB_tuple_get_string(tplp, "TYPE");
        if (typename[0] == '\0') {
            /* Get tuple type */
            typename = new_nstypekey(name, attrv[fno].name);
            ret = get_tuple_type(typename, txp, &attrtyp);
            free(typename);
        } else {        
            ret = RDB_get_type(typename, txp, &attrtyp);
        }
        if (ret != RDB_OK)
            goto error;
        attrv[fno].typ = attrtyp;
        attrv[fno].defaultp = NULL;
    }

    /*
     * Read default values
     */

    exprp = RDB_eq(RDB_expr_attr("TABLENAME"), RDB_string_const(name));
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
            RDB_FALSE, &tx, tbpp);
    for (i = 0; i < keyc; i++) {
        for (j = 0; j < keyv[i].strc; j++)
            free(keyv[i].strv[j]);
    }
    free(keyv);

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

    _RDB_assign_table_db(*tbpp, txp->dbp);

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

    exprp = RDB_eq(RDB_expr_attr("TABLENAME"), RDB_string_const(name));
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
    ret = RDB_extract_tuple(tmptbp, &tpl, txp);

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

    _RDB_assign_table_db(*tbpp, txp->dbp);
    
    return ret;
error:
    RDB_destroy_obj(&arr);
    
    return ret;
}

static RDB_selector_func *
get_selector(lt_dlhandle lhdl, const char *possrepname) {
    RDB_selector_func *fnp;
    char *fname = malloc(13 + strlen(possrepname));

    if (fname == NULL)
        return NULL;
    strcpy(fname, "RDBU_select_");
    strcat(fname, possrepname);

    fnp = (RDB_selector_func *) lt_dlsym(lhdl, fname);
    free(fname);
    return fnp;
}

static RDB_setter_func *
get_setter(lt_dlhandle lhdl, const char *compname) {
    RDB_setter_func *fnp;
    char *fname = malloc(10 + strlen(compname));

    if (fname == NULL)
        return NULL;
    strcpy(fname, "RDBU_set_");
    strcat(fname, compname);

    fnp = (RDB_setter_func *) lt_dlsym(lhdl, fname);
    free(fname);
    return fnp;
}

static RDB_getter_func *
get_getter(lt_dlhandle lhdl, const char *compname) {
    RDB_getter_func *fnp;
    char *fname = malloc(10 + strlen(compname));

    if (fname == NULL)
        return NULL;
    strcpy(fname, "RDBU_get_");
    strcat(fname, compname);

    fnp = (RDB_getter_func *) lt_dlsym(lhdl, fname);
    free(fname);
    return fnp;
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
    wherep = RDB_eq(exp, RDB_string_const(name));
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
    wherep = RDB_eq(exp, RDB_string_const(name));
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
    wherep = RDB_eq(exp, RDB_string_const(name));
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
    ex2p = RDB_eq(exp, RDB_string_const(possrepname));
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
    char *libname;
    char *typename;
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

    ret = RDB_extract_tuple(tmptb1p, &tpl, txp);
    if (ret != RDB_OK)
        goto error;

    typ = malloc(sizeof (RDB_type));
    if (typ == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    typ->kind = RDB_TP_SCALAR;
    typ->comparep = NULL;

    typename = RDB_tuple_get_string(&tpl, "I_AREP_TYPE");
    if (typename[0] != '\0') {
        ret = RDB_get_type(typename, txp, &typ->arep);
        if (ret != RDB_OK)
            goto error;
    } else {
        typ->arep = NULL;
    }

    typ->name = RDB_dup_str(name);
    if (typ->name == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    typ->ireplen = RDB_tuple_get_int(&tpl, "I_AREP_LEN");

    typ->var.scalar.repc = 0;

    libname = RDB_tuple_get_string(&tpl, "I_LIBNAME");
    if (libname[0] != '\0') {
        typ->var.scalar.modhdl = lt_dlopenext(libname);
        if (typ->var.scalar.modhdl == NULL) {
            RDB_errmsg(txp->dbp->dbrootp->envp, lt_dlerror());
            ret = RDB_RESOURCE_NOT_FOUND;
            RDB_rollback_all(txp);
            goto error;
        }
    } else {
        typ->var.scalar.modhdl = NULL;
    }

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
     * Read possrep data from array and store it in typ->var.scalar.repv.
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
            typ->var.scalar.repv[i].compv = malloc(ret * sizeof (RDB_icomp));
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
            
            if (typ->var.scalar.modhdl == NULL) {
                typ->var.scalar.repv[i].compv[idx].getterp = NULL;
                typ->var.scalar.repv[i].compv[idx].setterp = NULL;
            } else {
                typ->var.scalar.repv[i].compv[idx].getterp = get_getter(
                        typ->var.scalar.modhdl,
                        typ->var.scalar.repv[i].compv[idx].name);
                typ->var.scalar.repv[i].compv[idx].setterp = get_setter(
                        typ->var.scalar.modhdl,
                        typ->var.scalar.repv[i].compv[idx].name);
                if (typ->var.scalar.repv[i].compv[idx].getterp == NULL
                        || typ->var.scalar.repv[i].compv[idx].setterp == NULL) {
                    ret = RDB_RESOURCE_NOT_FOUND;
                    RDB_rollback_all(txp);
                    goto error;
                }
            }
        }
        if (typ->var.scalar.modhdl == NULL) {
            typ->var.scalar.repv[i].selectorp = NULL;
        } else {
            typ->var.scalar.repv[i].selectorp = get_selector(
                        typ->var.scalar.modhdl,
                        typ->var.scalar.repv[i].name);
            if (typ->var.scalar.repv[i].selectorp == NULL) {
                ret = RDB_RESOURCE_NOT_FOUND;
                RDB_rollback_all(txp);
                goto error;
            }
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
    RDB_table *hvtbp, *vtbp;
    RDB_object tpl;
    char *attrv[] = { "RTYPE" };

    /*
     * Build query
     */

    wherep = RDB_eq(RDB_expr_attr("NAME"), RDB_string_const(opname));
    if (wherep == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    ret = RDB_select(txp->dbp->dbrootp->ro_ops_tbp, wherep, &hvtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        RDB_rollback_all(txp);
        return ret;
    }

    ret = RDB_project(hvtbp, 1, attrv, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_table(hvtbp, txp);
        RDB_rollback_all(txp);
        return ret;
    }

    /*
     * Read tuple
     */

    RDB_init_obj(&tpl);
    ret = RDB_extract_tuple(vtbp, &tpl, txp);
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

    ret = RDB_get_type(RDB_tuple_get_string(&tpl, "RTYPE"), txp, typp);
    RDB_destroy_obj(&tpl);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }

    return RDB_OK;
}

char *
_RDB_make_typestr(int argc, RDB_type *argtv[])
{
    char *typesbuf;
    int i;
    size_t typeslen = 0;

    for (i = 0; i < argc; i++) {
        typeslen += strlen(RDB_type_name(argtv[i]));
        if (i > 0)
            typeslen++;
    }
    typesbuf = malloc(typeslen + 1);
    if (typesbuf == NULL)
        return NULL;
    typesbuf[0] = '\0';
    for (i = 0; i < argc; i++) {
        if (i > 0)
           strcat(typesbuf, " ");
        strcat(typesbuf, RDB_type_name(argtv[i]));
    }
    return typesbuf;
}

/* Read update operator from database */
int
_RDB_get_cat_ro_op(const char *name, int argc, RDB_type *argtv[],
        RDB_transaction *txp, RDB_ro_op **opp)
{
    RDB_expression *exp;
    RDB_table *vtbp;
    RDB_object tpl;
    int i;
    int ret;
    char *libname, *symname;
    RDB_ro_op *op = NULL;
    char *typestr = _RDB_make_typestr(argc, argtv);

    if (typestr == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }
        
    exp = RDB_and(
            RDB_eq(RDB_expr_attr("NAME"),
                   RDB_string_const(name)),
            RDB_eq(RDB_expr_attr("ARGTYPES"),
                   RDB_string_const(typestr)));
    free(typestr);
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
    ret = RDB_extract_tuple(vtbp, &tpl, txp);
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
    op->argtv = malloc(sizeof(RDB_type *) * op->argc);
    if (op->argtv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    for (i = 0; i < op->argc; i++) {
        op->argtv[i] = argtv[i];
    }

    ret = RDB_get_type(RDB_tuple_get_string(&tpl, "RTYPE"), txp, &op->rtyp);
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
    int i;
    int ret;
    char *libname, *symname;
    RDB_upd_op *op = NULL;
    char *typestr = _RDB_make_typestr(argc, argtv);

    if (typestr == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }
        
    exp = RDB_and(
            RDB_eq(RDB_expr_attr("NAME"),
                   RDB_string_const(name)),
            RDB_eq(RDB_expr_attr("ARGTYPES"),
                   RDB_string_const(typestr)));
    free(typestr);
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
    ret = RDB_extract_tuple(vtbp, &tpl, txp);
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

    ret = RDB_binary_get(RDB_tuple_get(&tpl, "UPDV"), 0, op->updv, op->argc);
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
