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
static RDB_key_attrs table_attr_keyv[] = { { 2, table_attr_keyattrv } };

static RDB_attr table_attr_defvals_attrv[] = {
            { "ATTRNAME", &RDB_STRING, NULL, 0 },
            { "TABLENAME", &RDB_STRING, NULL, 0 },
            { "DEFAULT_VALUE", &RDB_BINARY, NULL, 0 } };
static char *table_attr_defvals_keyattrv[] = { "ATTRNAME", "TABLENAME" };
static RDB_key_attrs table_attr_defvals_keyv[] = { { 2, table_attr_defvals_keyattrv } };

static RDB_attr rtables_attrv[] = {
    { "TABLENAME", &RDB_STRING, NULL, 0 },
    { "IS_USER", &RDB_BOOLEAN, NULL, 0 },
    { "I_RECMAP", &RDB_STRING, NULL, 0 }
};
static char *rtables_keyattrv[] = { "TABLENAME" };
static RDB_key_attrs rtables_keyv[] = { { 1, rtables_keyattrv } };

static RDB_attr vtables_attrv[] = {
    { "TABLENAME", &RDB_STRING, NULL, 0 },
    { "IS_USER", &RDB_BOOLEAN, NULL, 0 },
    { "I_DEF", &RDB_BINARY, NULL, 0 }
};
static char *vtables_keyattrv[] = { "TABLENAME" };
static RDB_key_attrs vtables_keyv[] = { { 1, vtables_keyattrv } };

static RDB_attr dbtables_attrv[] = {
    { "TABLENAME", &RDB_STRING },
    { "DBNAME", &RDB_STRING }
};
static char *dbtables_keyattrv[] = { "TABLENAME", "DBNAME" };
static RDB_key_attrs dbtables_keyv[] = { { 2, dbtables_keyattrv } };

static RDB_attr keys_attrv[] = {
    { "TABLENAME", &RDB_STRING, NULL, 0 },
    { "KEYNO", &RDB_INTEGER, NULL, 0 },
    { "ATTRS", &RDB_STRING, NULL, 0 }
};
static char *keys_keyattrv[] = { "TABLENAME", "KEYNO" };
static RDB_key_attrs keys_keyv[] = { { 2, keys_keyattrv } };

static RDB_attr types_attrv[] = {
    { "TYPENAME", &RDB_STRING, NULL, 0 },
    { "I_LIBNAME", &RDB_STRING, NULL, 0 },
    { "I_AREP_LEN", &RDB_INTEGER, NULL, 0 },
    { "I_AREP_TYPE", &RDB_STRING, NULL, 0 }
};
static char *types_keyattrv[] = { "TYPENAME" };
static RDB_key_attrs types_keyv[] = { { 1, types_keyattrv } };

static RDB_attr possreps_attrv[] = {
    { "TYPENAME", &RDB_STRING, NULL, 0 },
    { "POSSREPNAME", &RDB_STRING, NULL, 0 },
    { "I_CONSTRAINT", &RDB_BINARY, NULL, 0 }
};
static char *possreps_keyattrv[] = { "TYPENAME", "POSSREPNAME" };
static RDB_key_attrs possreps_keyv[] = { { 2, possreps_keyattrv } };

static RDB_attr possrepcomps_attrv[] = {
    { "TYPENAME", &RDB_STRING, NULL, 0 },
    { "POSSREPNAME", &RDB_STRING, NULL, 0 },
    { "COMPNO", &RDB_INTEGER, NULL, 0 },
    { "COMPNAME", &RDB_STRING, NULL, 0 },
    { "COMPTYPENAME", &RDB_STRING, NULL, 0 }
};
static char *possrepcomps_keyattrv1[] = { "TYPENAME", "POSSREPNAME", "COMPNO" };
static char *possrepcomps_keyattrv2[] = { "TYPENAME", "POSSREPNAME", "COMPNAME" };
static RDB_key_attrs possrepcomps_keyv[] = {
    { 3, possrepcomps_keyattrv1 },
    { 3, possrepcomps_keyattrv2 }
};

int
_RDB_dbtables_insert(RDB_table *tbp, RDB_transaction *txp)
{
    RDB_tuple tpl;
    int ret;

    /* Insert (database, table) pair into SYS_DBTABLES */
    RDB_init_tuple(&tpl);

    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK)
    {
        RDB_destroy_tuple(&tpl);
        return ret;
    }
    ret = RDB_tuple_set_string(&tpl, "DBNAME", txp->dbp->name);
    if (ret != RDB_OK)
    {
        RDB_destroy_tuple(&tpl);
        return ret;
    }
    ret = RDB_insert(txp->dbp->dbrootp->dbtables_tbp, &tpl, txp);
    RDB_destroy_tuple(&tpl);
    
    return ret;
}

/* Insert the table pointed to by tbp into the catalog. */
int
_RDB_catalog_insert(RDB_table *tbp, RDB_transaction *txp)
{
    RDB_tuple tpl;
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int ret;
    int i, j;

    /* insert entry into table SYS_RTABLES */
    RDB_init_tuple(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        return ret;
    }
    ret = RDB_tuple_set_bool(&tpl, "IS_USER", tbp->is_user);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        return ret;
    }
    ret = RDB_tuple_set_string(&tpl, "I_RECMAP", tbp->name);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        return ret;
    }
    ret = RDB_insert(txp->dbp->dbrootp->rtables_tbp, &tpl, txp);
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_dbtables_insert(tbp, txp);
    if (ret != RDB_OK)
        return ret;

    /* insert entries into table SYS_TABLEATTRS */
    RDB_init_tuple(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        return ret;
    }

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        char *attrname = tuptyp->var.tuple.attrv[i].name;
        char *typename = RDB_type_name(tuptyp->var.tuple.attrv[i].typ);

        ret = RDB_tuple_set_string(&tpl, "ATTRNAME", attrname);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret;
        }
        ret = RDB_tuple_set_string(&tpl, "TYPE", typename);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret;
        }
        ret = RDB_tuple_set_int(&tpl, "I_FNO", i);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret;
        }
        ret = RDB_insert(txp->dbp->dbrootp->table_attr_tbp, &tpl, txp);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret;
        }
    }
    RDB_destroy_tuple(&tpl);

    /* insert entries into table SYS_TABLEATTR_DEFVALS */
    RDB_init_tuple(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        return ret;
    }

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        if (tuptyp->var.tuple.attrv[i].defaultp != NULL) {
            char *attrname = tuptyp->var.tuple.attrv[i].name;
            RDB_value binval;
            void *datap;
            size_t len;

            if (!RDB_type_equals(tuptyp->var.tuple.attrv[i].defaultp->typ,
                    tuptyp->var.tuple.attrv[i].typ))
                return RDB_TYPE_MISMATCH;
            
            ret = RDB_tuple_set_string(&tpl, "ATTRNAME", attrname);
            if (ret != RDB_OK) {
                RDB_destroy_tuple(&tpl);
                return ret;
            }

            RDB_init_value(&binval);
            datap = RDB_value_irep(tuptyp->var.tuple.attrv[i].defaultp, &len);
            ret = RDB_binary_set(&binval, 0, datap, len);
            if (ret != RDB_OK) {
                RDB_destroy_tuple(&tpl);
                RDB_destroy_value(&binval);
                return ret;
            }

            ret = RDB_tuple_set(&tpl, "DEFAULT_VALUE", &binval);
            RDB_destroy_value(&binval);
            if (ret != RDB_OK) {
                RDB_destroy_tuple(&tpl);
                return ret;
            }

            ret = RDB_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, &tpl, txp);
            if (ret != RDB_OK) {
                RDB_destroy_tuple(&tpl);
                return ret;
            }
        }
    }
    RDB_destroy_tuple(&tpl);

    /* insert keys into SYS_KEYS */
    RDB_init_tuple(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK)
        return ret;
    for (i = 0; i < tbp->keyc; i++) {
        RDB_key_attrs *kap = &tbp->keyv[i];
        char buf[1024];

        ret = RDB_tuple_set_int(&tpl, "KEYNO", i);
        if (ret != RDB_OK)
            return ret;

        /* Concatenate attribute names */
        buf[0] = '\0';
        if (kap->attrc > 0) {
            strcpy(buf, kap->attrv[0]);
            for (j = 1; j < kap->attrc; j++) {
                strcat(buf, " ");
                strcat(buf, kap->attrv[j]);
            }
        }

        ret = RDB_tuple_set_string(&tpl, "ATTRS", buf);
        if (ret != RDB_OK)
            return ret;

        ret = RDB_insert(txp->dbp->dbrootp->keys_tbp, &tpl, txp);
        if (ret != RDB_OK)
            return ret;
    }
    RDB_destroy_tuple(&tpl);

    return RDB_OK;
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
        ret = _RDB_open_table("SYS_TABLEATTRS", RDB_TRUE, 4, table_attr_attrv,
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

    ret = _RDB_open_table("SYS_TABLEATTR_DEFVALS", RDB_TRUE, 3, table_attr_defvals_attrv,
            1, table_attr_defvals_keyv, RDB_FALSE, create, txp,
            &txp->dbp->dbrootp->table_attr_defvals_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->table_attr_defvals_tbp, txp->dbp);

    ret = _RDB_open_table("SYS_RTABLES", RDB_TRUE, 3, rtables_attrv, 1, rtables_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->rtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->rtables_tbp, txp->dbp);

    ret = _RDB_open_table("SYS_VTABLES", RDB_TRUE, 3, vtables_attrv, 1, vtables_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->vtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->vtables_tbp, txp->dbp);

    ret = _RDB_open_table("SYS_DBTABLES", RDB_TRUE, 2, dbtables_attrv, 1, dbtables_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->dbtables_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->dbtables_tbp, txp->dbp);

    ret = _RDB_open_table("SYS_KEYS", RDB_TRUE, 3, keys_attrv, 1, keys_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->keys_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->keys_tbp, txp->dbp);

    ret = _RDB_open_table("SYS_TYPES", RDB_TRUE, 4, types_attrv, 1, types_keyv,
            RDB_FALSE, create, txp, &txp->dbp->dbrootp->types_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->types_tbp, txp->dbp);

    ret = _RDB_open_table("SYS_POSSREPS", RDB_TRUE, 3, possreps_attrv,
            1, possreps_keyv, RDB_FALSE, create, txp, &txp->dbp->dbrootp->possreps_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->possreps_tbp, txp->dbp);

    ret = _RDB_open_table("SYS_POSSREPCOMPS", RDB_TRUE, 5, possrepcomps_attrv,
            2, possrepcomps_keyv, RDB_FALSE, create, txp,
            &txp->dbp->dbrootp->possrepcomps_tbp);
    if (ret != RDB_OK) {
        return ret;
    }
    _RDB_assign_table_db(txp->dbp->dbrootp->possrepcomps_tbp, txp->dbp);

    return RDB_OK;
}

int
_RDB_create_db_in_cat(RDB_transaction *txp)
{
    int ret;

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->table_attr_tbp, txp);
    if (ret != RDB_OK) {
        if (ret == RDB_ELEMENT_EXISTS) {
            /* the catalog table already exists, but not in this database */
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->table_attr_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->rtables_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->vtables_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->dbtables_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->keys_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->types_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->possreps_tbp, txp);
            if (ret != RDB_OK) 
                return ret;
            ret = _RDB_dbtables_insert(txp->dbp->dbrootp->possrepcomps_tbp, txp);
        }
        return ret;
    }

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->table_attr_defvals_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->rtables_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->vtables_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->dbtables_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->keys_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->types_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->possreps_tbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = _RDB_catalog_insert(txp->dbp->dbrootp->possrepcomps_tbp, txp);

    return ret;
}

static int
get_keyattrs(const char *attrstr, RDB_key_attrs *attrsp)
{
    int i;
    int slen = strlen(attrstr);

    if (attrstr[0] == '\0') {
        attrsp->attrc = 0;
    } else {
        const char *sp;
        const char *ep;
    
        attrsp->attrc = 1;
        for (i = 0; i < slen; i++) {
            if (attrstr[i] == ' ')
                attrsp->attrc++;
        }
        attrsp->attrv = malloc(sizeof(char *) * attrsp->attrc);
        if (attrsp->attrv == NULL)
            return RDB_NO_MEMORY;

        for (i = 0; i < attrsp->attrc; i++)
            attrsp->attrv[i] = NULL;

        sp = attrstr;
        for (i = 0, sp = attrstr; sp != NULL; i++) {
            ep = strchr(sp, ' ');
            if (ep != NULL) {
                attrsp->attrv[i] = malloc(ep - sp + 1);
                if (attrsp->attrv[i] == NULL)
                    goto error;
                strncpy(attrsp->attrv[i], sp, ep - sp);
                attrsp->attrv[i][ep - sp] = '\0';
            } else {
                attrsp->attrv[i] = RDB_dup_str(sp);
                if (attrsp->attrv[i] == NULL)
                    goto error;
            }
            sp = ep;
        }
    }
    return RDB_OK;
error:
    for (i = 0; i < attrsp->attrc; i++)
        free(attrsp->attrv[i]);
    free(attrsp->attrv);
    return RDB_NO_MEMORY;    
}

static int
get_keys(const char *name, RDB_transaction *txp,
         int *keycp, RDB_key_attrs **keyvp)
{
    RDB_expression *wherep;
    RDB_table *vtbp;
    RDB_array arr;
    RDB_tuple tpl;
    int ret;
    int i;
    
    *keyvp = NULL;

    RDB_init_array(&arr);
    
    wherep = RDB_eq(RDB_string_const(name),
            RDB_expr_attr("TABLENAME", &RDB_STRING));
    if (wherep == NULL)
        return RDB_NO_MEMORY;

    ret = RDB_select(txp->dbp->dbrootp->keys_tbp, wherep, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(wherep);
        return ret;
    }

    ret = RDB_table_to_array(vtbp, &arr, 0, NULL, txp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_array_length(&arr);
    if (ret < 0)
        goto error;
    *keycp = ret;

    *keyvp = malloc(sizeof(RDB_key_attrs) * *keycp);
    if (*keyvp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    for (i = 0; i < *keycp; i++)
        (*keyvp)[i].attrv = NULL;

    RDB_init_tuple(&tpl);

    for (i = 0; i < *keycp; i++) {
        RDB_int kno;
    
        ret = RDB_array_get_tuple(&arr, i, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            goto error;
        }
        kno = RDB_tuple_get_int(&tpl, "KEYNO");
        ret = get_keyattrs(RDB_tuple_get_string(&tpl, "ATTRS"), &(*keyvp)[kno]);
        if (ret != RDB_OK) {
            (*keyvp)[kno].attrv = NULL;
            RDB_destroy_tuple(&tpl);
            goto error;
        }
    }
    RDB_destroy_tuple(&tpl);
    ret = RDB_destroy_array(&arr);
    RDB_drop_table(vtbp, txp);

    return ret;
error:
    RDB_destroy_array(&arr);
    RDB_drop_table(vtbp, txp);
    if (*keyvp != NULL) {
        int i, j;
    
        for (i = 0; i < *keycp; i++) {
            if ((*keyvp)[i].attrv != NULL) {
                for (j = 0; j < (*keyvp)[i].attrc; j++)
                    free((*keyvp)[i].attrv[j]);
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
    RDB_array arr;
    RDB_tuple tpl;
    RDB_bool usr;
    int ret;
    RDB_int i, j;
    int attrc;
    RDB_attr *attrv = NULL;
    int defvalc;
    int keyc;
    RDB_key_attrs *keyv;

    /* Read real table data from the catalog */

    RDB_init_array(&arr);
    RDB_init_tuple(&tpl);

    exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
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

    exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
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
    ret = RDB_table_to_array(tmptb2p, &arr, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    attrc = RDB_array_length(&arr);
    if (attrc > 0)
        attrv = malloc(sizeof(RDB_attr) * attrc);

    for (i = 0; i < attrc; i++) {
        RDB_type *attrtyp;
        RDB_int fno;

        RDB_array_get_tuple(&arr, i, &tpl);
        fno = RDB_tuple_get_int(&tpl, "I_FNO");
        attrv[fno].name = RDB_dup_str(RDB_tuple_get_string(&tpl, "ATTRNAME"));
        ret = RDB_get_type(RDB_tuple_get_string(&tpl, "TYPE"), txp,
                           &attrtyp);
        if (ret != RDB_OK)
            goto error;
        attrv[fno].typ = attrtyp;
        attrv[fno].defaultp = NULL;
    }

    /*
     * Read default values
     */

    exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
            RDB_string_const(name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    ret = RDB_select(txp->dbp->dbrootp->table_attr_defvals_tbp, exprp, &tmptb3p);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_table_to_array(tmptb3p, &arr, 0, NULL, txp);
    if (ret != RDB_OK) {
        goto error;
    }

    defvalc = RDB_array_length(&arr);

    RDB_init_tuple(&tpl);

    for (i = 0; i < defvalc; i++) {
        char *name;
        RDB_value *binvalp;

        RDB_array_get_tuple(&arr, i, &tpl);
        name = RDB_tuple_get_string(&tpl, "ATTRNAME");
        binvalp = RDB_tuple_get(&tpl, "DEFAULT_VALUE");
        
        /* Find attrv entry and set default value */
        for (i = 0; i < attrc && (strcmp(attrv[i].name, name) != 0); i++);
        if (i >= attrc) {
            /* Not found */
            ret = RDB_INTERNAL;
            goto error;
        }
        attrv[i].defaultp = malloc(sizeof (RDB_value));
        RDB_init_value(attrv[i].defaultp);
        ret = RDB_irep_to_value(attrv[i].defaultp, attrv[i].typ,
                binvalp->var.bin.datap, binvalp->var.bin.len);
        if (ret != RDB_OK)
            goto error;            
    }

    /* Open the table */

    ret = get_keys(name, txp, &keyc, &keyv);
    if (ret != RDB_OK)
        goto error;

    ret = _RDB_open_table(name, RDB_TRUE, attrc, attrv, keyc, keyv, usr, RDB_FALSE,
                     txp, tbpp);
    for (i = 0; i < keyc; i++) {
        for (j = 0; j < keyv[i].attrc; j++)
            free(keyv[i].attrv[j]);
    }
    free(keyv);

    if (ret != RDB_OK)
        goto error;

    for (i = 0; i < attrc; i++) {
        free(attrv[i].name);
        if (attrv[i].defaultp != NULL) {
            RDB_destroy_value(attrv[i].defaultp);
            free(attrv[i].defaultp);
        }
    }
    if (attrc > 0)
        free(attrv);

    _RDB_assign_table_db(*tbpp, txp->dbp);

    ret = RDB_destroy_array(&arr);

    RDB_drop_table(tmptb1p, txp);
    RDB_drop_table(tmptb2p, txp);

    RDB_destroy_tuple(&tpl);

    return ret;
error:
    if (attrv != NULL) {
        for (i = 0; i < attrc; i++)
            free(attrv[i].name);
        free(attrv);
    }

    RDB_destroy_array(&arr);

    if (tmptb1p != NULL)
        RDB_drop_table(tmptb1p, txp);
    if (tmptb2p != NULL)
        RDB_drop_table(tmptb2p, txp);
    if (tmptb3p != NULL)
        RDB_drop_table(tmptb3p, txp);

    RDB_destroy_tuple(&tpl);
    
    return ret;
}

int
_RDB_get_cat_vtable(const char *name, RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_expression *exprp;
    RDB_table *tmptbp = NULL;
    RDB_tuple tpl;
    RDB_array arr;
    RDB_value *valp;
    RDB_bool usr;
    int ret;

    /* read real table data from the catalog */

    RDB_init_array(&arr);

    exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
            RDB_string_const(name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    ret = RDB_select(txp->dbp->dbrootp->vtables_tbp, exprp, &tmptbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    
    RDB_init_tuple(&tpl);
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
    
    ret = RDB_destroy_array(&arr);

    (*tbpp)->is_persistent = RDB_TRUE;
    (*tbpp)->name = RDB_dup_str(name);

    _RDB_assign_table_db(*tbpp, txp->dbp);
    
    return ret;
error:
    RDB_destroy_array(&arr);
    
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

    exp = RDB_expr_attr("TYPENAME", &RDB_STRING);
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

    exp = RDB_expr_attr("TYPENAME", &RDB_STRING);
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

    exp = RDB_expr_attr("TYPENAME", &RDB_STRING);
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
    exp = RDB_expr_attr("POSSREPNAME", &RDB_STRING);
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
    RDB_tuple tpl;
    RDB_array possreps;
    RDB_array comps;
    RDB_type *typ = NULL;
    char *libname;
    char *typename;
    int ret, tret;
    int i;

    RDB_init_tuple(&tpl);
    RDB_init_array(&possreps);
    RDB_init_array(&comps);

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
            ERRMSG(txp->dbp->dbrootp->envp, lt_dlerror());
            ret = RDB_RESOURCE_NOT_FOUND;
            RDB_rollback(txp);
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
    ret = RDB_table_to_array(tmptb2p, &possreps, 0, NULL, txp);
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

        ret = RDB_array_get_tuple(&possreps, (RDB_int) i, &tpl);
        if (ret != RDB_OK)
            goto error;
        typ->var.scalar.repv[i].name = RDB_dup_str(
                RDB_tuple_get_string(&tpl, "POSSREPNAME"));
        if (typ->var.scalar.repv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }

        ret = _RDB_deserialize_expr(RDB_tuple_get(&tpl, "I_CONSTRAINT"), txp,
                &typ->var.scalar.repv[i].constraintp);
        if (ret != RDB_OK) {
            goto error;
        }

        ret = _RDB_possrepcomps_query(name, typ->var.scalar.repv[i].name, txp,
                &tmptb3p);
        if (ret != RDB_OK) {
            goto error;
        }
        ret = RDB_table_to_array(tmptb3p, &comps, 0, NULL, txp);
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

            ret = RDB_array_get_tuple(&comps, (RDB_int) j, &tpl);
            if (ret != RDB_OK)
                goto error;
            idx = RDB_tuple_get_int(&tpl, "COMPNO");
            typ->var.scalar.repv[i].compv[idx].name = RDB_dup_str(
                    RDB_tuple_get_string(&tpl, "COMPNAME"));
            if (typ->var.scalar.repv[i].compv[idx].name == NULL) {
                ret = RDB_NO_MEMORY;
                goto error;
            }
            ret = RDB_get_type(RDB_tuple_get_string(&tpl, "COMPTYPENAME"),
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
                    RDB_rollback(txp);
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
                RDB_rollback(txp);
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

    RDB_destroy_tuple(&tpl);
    tret = RDB_destroy_array(&possreps);
    if (ret == RDB_OK)
        ret = tret;
    tret = RDB_destroy_array(&comps);
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
    RDB_destroy_tuple(&tpl);
    RDB_destroy_array(&possreps);
    RDB_destroy_array(&comps);
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
