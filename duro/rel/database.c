/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>
#include <stdio.h>

/* name of the file in which the tables are physically stored */
#define RDB_DATAFILE "rdata"

/* initial capacities of attribute map and table map */
enum {
    RDB_ATTRMAP_CAPACITY = 37,
    RDB_TABLEMAP_CAPACITY = 37,
    RDB_BUF_INITLEN = 256
};

/* Return the length (in bytes) of the internal representation
 * of the type pointed to by typ.
 */
static int replen(const RDB_type *typ) {
    switch (typ->kind) {
        case RDB_TP_BOOLEAN:
            return 1;
        case RDB_TP_INTEGER:
            return sizeof(RDB_int);
        case RDB_TP_RATIONAL:
            return sizeof(RDB_rational);
        case RDB_TP_STRING:
            return RDB_VARIABLE_LEN;
        default:
            return RDB_VARIABLE_LEN;
    }
    return 0;
}

/* Return RDB_TRUE if the attribute name is contained
 * in the attribute list pointed to by keyp.
 */
static RDB_bool
key_contains(const RDB_key_attrs *keyp, const char *name)
{
    int i;
    
    for (i = 0; i < keyp->attrc; i++) {
        if (strcmp(keyp->attrv[i], name) == 0)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

static int
open_key_index(RDB_table *tbp, int keyno, const RDB_key_attrs *keyattrsp,
                 RDB_bool create, RDB_transaction *txp, RDB_index **idxp)
{
    int res;
    int i;
    char *idx_name = malloc(strlen(tbp->name) + 4);
    int *fieldv = malloc(sizeof(int *) * keyattrsp->attrc);

    if (idx_name == NULL || fieldv == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }

    /* build index name */            
    sprintf(idx_name, "%s$%d", tbp->name, keyno);

    /* get index numbers */
    for (i = 0; i < keyattrsp->attrc; i++) {
        fieldv[i] = *(int *) RDB_hashmap_get(&tbp->var.stored.attrmap,
                        keyattrsp->attrv[i], NULL);
    }

    if (create) {
        /* create index */
        res = RDB_create_index(tbp->var.stored.recmapp,
                  tbp->is_persistent ? idx_name : NULL,
                  tbp->is_persistent ? RDB_DATAFILE : NULL,
                  txp->dbp->envp, keyattrsp->attrc, fieldv, RDB_TRUE, txp->txid, idxp);
    } else {
        /* open index */
        res = RDB_open_index(tbp->var.stored.recmapp, idx_name, RDB_DATAFILE,
                  txp->dbp->envp, keyattrsp->attrc, fieldv, RDB_TRUE, txp->txid, idxp);
    }

    if (res != RDB_OK)
        goto error;

    res = RDB_OK;
error:
    free(idx_name);
    free(fieldv);
    return res;
}


/* Open a table.
 *
 * Arguments:
 * name       the name of the table
 * persistent RDB_TRUE for a persistent table, RDB_FALSE for a transient table.
 *            May only be RDB_FALSE if create is RDB_TRUE (see below).
 * attrc      the number of attributes
 * keyc       number of keys
 * keyv       vector of keys
 * usr        if RDB_TRUE, the table is a user table, if RDB_FALSE,
 *            it's a system table.
 * envp        pointer to the database environment the table belongs to
 * tbpp       points to a location where a pointer to the newly created
 *            RDB_table structure is to be stored.
 * create     if RDB_TRUE, create a new table, if RDB_FALSE, open an
 *            existing table.
 */
static int
open_table(const char *name, RDB_bool persistent,
           int attrc, RDB_attr heading[],
           int keyc, RDB_key_attrs keyv[], RDB_bool usr,
           RDB_bool create, RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tbp = NULL;
    int *flens = NULL;
    int res, i, ki, di;

    /* choose key #0 as for the primary index */
    const RDB_key_attrs *prkeyattrs = &keyv[0];
    
    tbp = *tbpp = malloc(sizeof (RDB_table));
    if (tbp == NULL)
        return RDB_NO_MEMORY;
    tbp->is_user = usr;
    tbp->is_persistent = persistent;
    tbp->keyv = NULL;
    tbp->refcount = 1;

    tbp->kind = RDB_TB_STORED;
    if (name != NULL) {
        tbp->name = RDB_dup_str(name);
        if (tbp->name == NULL) {
            res = RDB_NO_MEMORY;
            goto error;
        }
    } else {
        tbp->name = NULL;
    }

    /* copy candidate keys */
    tbp->keyc = keyc;
    tbp->keyv = malloc(sizeof(RDB_attr) * keyc);
    for (i = 0; i < keyc; i++) {
        tbp->keyv[i].attrv = NULL;
    }
    for (i = 0; i < keyc; i++) {
        tbp->keyv[i].attrc = keyv[i].attrc;
        tbp->keyv[i].attrv = RDB_dup_strvec(keyv[i].attrc, keyv[i].attrv);
        if (tbp->keyv[i].attrv == NULL)
            goto error;
    }

    tbp->typ = RDB_create_relation_type(attrc, heading);
    if (tbp->typ == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }

    RDB_init_hashmap(&tbp->var.stored.attrmap, RDB_ATTRMAP_CAPACITY);

    flens = malloc(sizeof(int) * attrc);
    if (flens == NULL)
        goto error;
    ki = 0;
    di = prkeyattrs->attrc;
    for (i = 0; i < attrc; i++) {
        RDB_int val;
    
        if (key_contains(prkeyattrs, heading[i].name))
            val = ki++;
        else
            val = di++;
        res = RDB_hashmap_put(&tbp->var.stored.attrmap, heading[i].name,
                              &val, sizeof val);
        if (res != RDB_OK)
            goto error;
        /* Only built-in types supported by this version */
        if (!RDB_is_builtin_type(heading[i].type)) {
            res = RDB_NOT_SUPPORTED;
            goto error;
        }
        flens[i] = replen(heading[i].type);
        if (flens[i] == 0) {
            res = RDB_ILLEGAL_ARG;
            goto error;
        }
    }
    if (create) {
        res = RDB_create_recmap(persistent ? name : NULL,
                    persistent ? RDB_DATAFILE : NULL, txp->dbp->envp,
                    attrc, flens, prkeyattrs->attrc,
                    txp->txid, &tbp->var.stored.recmapp);
    } else {
        res = RDB_open_recmap(name, RDB_DATAFILE, txp->dbp->envp, attrc, flens,
                    prkeyattrs->attrc, txp->txid, &tbp->var.stored.recmapp);
    }

    /* Open/create indexes if there is more than one key */
    if (keyc > 1) {
        tbp->var.stored.keyidxv = malloc(sizeof(RDB_index *) * (keyc - 1));
        if (tbp->var.stored.keyidxv == NULL) {
            res = RDB_NO_MEMORY;
            goto error;
        }
        for (i = 1; i < keyc; i++) {    
            res = open_key_index(tbp, i, &keyv[i], create, txp,
                          &tbp->var.stored.keyidxv[i - 1]);
            if (res != RDB_OK)
                goto error;
        }
    }

    if (res != RDB_OK)
        goto error;

    free(flens);
    return RDB_OK;

error:
    /* clean up */
    free(flens);
    if (tbp != NULL) {
        free(tbp->name);
        for (i = 0; i < tbp->keyc; i++) {
            if (tbp->keyv[i].attrv != NULL) {
                RDB_free_strvec(tbp->keyv[i].attrc, tbp->keyv[i].attrv);
            }
        }
        free(tbp->keyv);
        if (tbp->typ != NULL) {
            RDB_drop_type(tbp->typ);
            RDB_deinit_hashmap(&tbp->var.stored.attrmap);
        }
        free(tbp);
    }
    return res;
}

/* Associate a RDB_table structure with a RDB_database structure. */
static int
assign_table_db(RDB_table *tbp, RDB_database *dbp)
{
    /* Insert table into table map */
    return RDB_hashmap_put(&dbp->tbmap, tbp->name, &tbp, sizeof (RDB_table *));
}

static int
dbtables_insert(RDB_table *tbp, RDB_transaction *txp)
{
    RDB_tuple tpl;
    int res;

    /* Insert (database, table) pair into SYSDBTABLES */
    RDB_init_tuple(&tpl);

    res = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (res != RDB_OK)
    {
        RDB_deinit_tuple(&tpl);
        return res;
    }
    res = RDB_tuple_set_string(&tpl, "DBNAME", txp->dbp->name);
    if (res != RDB_OK)
    {
        RDB_deinit_tuple(&tpl);
        return res;
    }
    res = RDB_insert(txp->dbp->dbtables_tbp, &tpl, txp);
    if (res != RDB_OK)
    {
        RDB_deinit_tuple(&tpl);
        return res;
    }
    RDB_deinit_tuple(&tpl);
    
    return RDB_OK;
}

/* Insert the table pointed to by tbp into the catalog. */
static int
catalog_insert(RDB_table *tbp, RDB_transaction *txp)
{
    RDB_tuple tpl;
    RDB_type *tuptyp = tbp->typ->complex.basetyp;
    int res;
    int i, j;

    res = dbtables_insert(tbp, txp);
    if (res != RDB_OK)
        return res;

    /* insert entry into table SYSRTABLES */
    RDB_init_tuple(&tpl);
    res = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (res != RDB_OK) {
        RDB_deinit_tuple(&tpl);
        return res;
    }
    res = RDB_tuple_set_bool(&tpl, "IS_USER", tbp->is_user);
    if (res != RDB_OK) {
        RDB_deinit_tuple(&tpl);
        return res;
    }
    res = RDB_tuple_set_string(&tpl, "I_RECMAP", tbp->name);
    if (res != RDB_OK) {
        RDB_deinit_tuple(&tpl);
        return res;
    }
    res = RDB_tuple_set_int(&tpl, "I_PXLEN", tbp->var.stored.recmapp->keyfieldcount);
    if (res != RDB_OK) {
        RDB_deinit_tuple(&tpl);
        return res;
    }
    res = RDB_insert(txp->dbp->rtables_tbp, &tpl, txp);
    RDB_deinit_tuple(&tpl);
    if (res != RDB_OK)
        return res;

    /* insert entries into table SYSTABLEATTRS */
    RDB_init_tuple(&tpl);
    res = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (res != RDB_OK) {
        RDB_deinit_tuple(&tpl);
        return res;
    }

    for (i = 0; i < tuptyp->complex.tuple.attrc; i++) {
        char *attrname = tuptyp->complex.tuple.attrv[i].name;
        char *typename = RDB_type_name(tuptyp->complex.tuple.attrv[i].type);

        res = RDB_tuple_set_string(&tpl, "ATTRNAME", attrname);
        if (res != RDB_OK) {
            RDB_deinit_tuple(&tpl);
            return res;
        }
        res = RDB_tuple_set_string(&tpl, "TYPE", typename);
        if (res != RDB_OK) {
            RDB_deinit_tuple(&tpl);
            return res;
        }
        res = RDB_tuple_set_int(&tpl, "I_FNO", i);
        if (res != RDB_OK) {
            RDB_deinit_tuple(&tpl);
            return res;
        }
        res = RDB_insert(txp->dbp->table_attr_tbp, &tpl, txp);
        if (res != RDB_OK) {
            RDB_deinit_tuple(&tpl);
            return res;
        }
    }
    RDB_deinit_tuple(&tpl);

    /* insert keys into SYSKEYS */
    RDB_init_tuple(&tpl);
    res = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (res != RDB_OK)
        return res;
    for (i = 0; i < tbp->keyc; i++) {
        RDB_key_attrs *kap = &tbp->keyv[i];
        char buf[1024];

        res = RDB_tuple_set_int(&tpl, "KEYNO", i);
        if (res != RDB_OK)
            return res;

        /* Concatenate attribute names */
        buf[0] = '\0';
        if (kap->attrc > 0) {
            strcpy(buf, kap->attrv[0]);
            for (j = 1; j < kap->attrc; j++) {
                strcat(buf, " ");
                strcat(buf, kap->attrv[j]);
            }
        }

        res = RDB_tuple_set_string(&tpl, "ATTRS", buf);
        if (res != RDB_OK)
            return res;

        res = RDB_insert(txp->dbp->keys_tbp, &tpl, txp);
        if (res != RDB_OK)
            return res;
    }
    RDB_deinit_tuple(&tpl);

    return RDB_OK;
}

/*
 * Definitions of the catalog tables.
 */

static RDB_attr table_attr_attrs[] = {
            { "ATTRNAME", &RDB_STRING },
            { "TABLENAME", &RDB_STRING },
            { "TYPE", &RDB_STRING },
            { "I_FNO", &RDB_INTEGER } };
static char *table_attr_keyattrs[] = { "ATTRNAME", "TABLENAME" };
static RDB_key_attrs table_attr_key[] = { { table_attr_keyattrs, 2 } };

static RDB_attr rtables_attrs[] = {
    { "TABLENAME", &RDB_STRING },
    { "IS_USER", &RDB_BOOLEAN },
    { "I_RECMAP", &RDB_STRING },
    { "I_PXLEN", &RDB_INTEGER }
};
static char *rtables_keyattrs[] = { "TABLENAME" };
static RDB_key_attrs rtables_key[] = { { rtables_keyattrs, 1 } };

static RDB_attr vtables_attrs[] = {
    { "TABLENAME", &RDB_STRING },
    { "IS_USER", &RDB_BOOLEAN },
    { "I_DEF", &RDB_BINARY }
};
static char *vtables_keyattrs[] = { "TABLENAME" };
static RDB_key_attrs vtables_key[] = { { vtables_keyattrs, 1 } };

static RDB_attr dbtables_attrs[] = {
    { "TABLENAME", &RDB_STRING },
    { "DBNAME", &RDB_STRING }
};
static char *dbtables_keyattrs[] = { "TABLENAME", "DBNAME" };
static RDB_key_attrs dbtables_key[] = { { dbtables_keyattrs, 2 } };

static RDB_attr keys_attrs[] = {
    { "TABLENAME", &RDB_STRING },
    { "KEYNO", &RDB_INTEGER },
    { "ATTRS", &RDB_STRING }
};
static char *keys_keyattrs[] = { "TABLENAME", "KEYNO" };
static RDB_key_attrs keys_key[] = { { keys_keyattrs, 2 } };

/* Create system tables if they do not already exist.
 * Associate system tables with the database pointed to by txp->dbp.
 */
static int
provide_systables(RDB_transaction *txp)
{
    int res;

    /* create or open catalog tables */

    res = open_table("SYSTABLEATTRS", RDB_TRUE, 4, table_attr_attrs,
            1, table_attr_key, RDB_FALSE, RDB_TRUE, txp,
            &txp->dbp->table_attr_tbp);
    if (res != RDB_OK) {
        return res;
    }
    assign_table_db(txp->dbp->table_attr_tbp, txp->dbp);

    res = open_table("SYSRTABLES", RDB_TRUE, 4, rtables_attrs, 1, rtables_key,
            RDB_FALSE, RDB_TRUE, txp, &txp->dbp->rtables_tbp);
    if (res != RDB_OK) {
        return res;
    }
    assign_table_db(txp->dbp->rtables_tbp, txp->dbp);

    res = open_table("SYSVTABLES", RDB_TRUE, 3, vtables_attrs, 1, vtables_key,
            RDB_FALSE, RDB_TRUE, txp, &txp->dbp->vtables_tbp);
    if (res != RDB_OK) {
        return res;
    }
    assign_table_db(txp->dbp->vtables_tbp, txp->dbp);

    res = open_table("SYSDBTABLES", RDB_TRUE, 2, dbtables_attrs, 1, dbtables_key,
            RDB_FALSE, RDB_TRUE, txp, &txp->dbp->dbtables_tbp);
    if (res != RDB_OK) {
        return res;
    }
    assign_table_db(txp->dbp->dbtables_tbp, txp->dbp);

    res = open_table("SYSKEYS", RDB_TRUE, 3, keys_attrs, 1, keys_key,
            RDB_FALSE, RDB_TRUE, txp, &txp->dbp->keys_tbp);
    if (res != RDB_OK) {
        return res;
    }
    assign_table_db(txp->dbp->keys_tbp, txp->dbp);

    /* insert catalog tables into catalog. catalog_insert() may return
       RDB_ELEMENT_EXISTS if the catalog table already existed, but not
       in this database. */
    res = catalog_insert(txp->dbp->table_attr_tbp, txp);
    if (res != RDB_OK && res != RDB_ELEMENT_EXISTS) {
        return res;
    }

    res = catalog_insert(txp->dbp->rtables_tbp, txp);
    if (res != RDB_OK && res != RDB_ELEMENT_EXISTS) {
        return res;
    }

    res = catalog_insert(txp->dbp->dbtables_tbp, txp);
    if (res != RDB_OK && res != RDB_ELEMENT_EXISTS) {
        return res;
    }

    res = catalog_insert(txp->dbp->keys_tbp, txp);
    if (res != RDB_OK && res != RDB_ELEMENT_EXISTS) {
        return res;
    }

    return RDB_OK;
}

/* cleanup function to close all DBs and tables */
static void
cleanup_env(RDB_environment *envp)
{
    RDB_database *dbp = (RDB_database *)RDB_env_private(envp);
    RDB_database *nextdbp;

    while (dbp != NULL) {
        nextdbp = dbp->nextdbp;
        RDB_release_db(dbp);
        dbp = nextdbp;
    }
}

static int
alloc_db(const char *name, RDB_environment *envp, RDB_database **dbpp)
{
    RDB_database *dbp;
    int res;

    /* Allocate structure */
    dbp = malloc(sizeof (RDB_database));
    if (dbp == NULL)
        return RDB_NO_MEMORY;

    /* Set name */
    dbp->name = RDB_dup_str(name);
    if (dbp->name == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }

    /* Set cleanup function */
    RDB_set_env_closefn(envp, cleanup_env);

    /* Initialize structure */

    dbp->envp = envp;
    dbp->refcount = 1;

    RDB_init_hashmap(&dbp->tbmap, RDB_TABLEMAP_CAPACITY);

    *dbpp = dbp;
    
    return RDB_OK;
error:
    free(dbp->name);
    free(dbp);
    return res;
}

static void
free_db(RDB_database *dbp) {
    RDB_deinit_hashmap(&dbp->tbmap);
    free(dbp->name);
    free(dbp);
}

int
RDB_create_db(const char *name, RDB_environment *envp, RDB_database **dbpp)
{
    RDB_transaction tx;
    int res;
    RDB_database *dbp;

    res = alloc_db(name, envp, &dbp);
    if (res != RDB_OK)
        return res;

    _RDB_init_builtin_types();

    res = RDB_begin_tx(&tx, dbp, NULL);
    if (res != RDB_OK) {
        free_db(dbp);
        return res;
    }

    res = provide_systables(&tx);
    if (res != RDB_OK) {
        goto error;
    }
    
    res = RDB_commit(&tx);
    if (res != RDB_OK)
        goto error;

    /* Insert database into list */
    dbp->nextdbp = RDB_env_private(envp);
    RDB_env_private(envp) = dbp;

    *dbpp = dbp;
    return RDB_OK;

error:
    RDB_rollback(&tx);
    free_db(dbp);
    return res;
}

int
RDB_get_db(const char *name, RDB_environment *envp, RDB_database **dbpp)
{
    int res;
    RDB_database *dbp;
    RDB_transaction tx;
    RDB_tuple tpl;

    /* search the DB list for the database */
    for (dbp = RDB_env_private(envp); dbp != NULL; dbp = dbp->nextdbp) {
        if (strcmp(dbp->name, name) == 0) {
            *dbpp = dbp;
            dbp->refcount++;
            return RDB_OK;
        }
    }

    /* Not found, create DB structure */

    res = alloc_db(name, envp, &dbp);
    if (res != RDB_OK)
        return res;

    _RDB_init_builtin_types();
   
    res = RDB_begin_tx(&tx, dbp, NULL);
    if (res != RDB_OK) {
        free_db(dbp);
        return res;
    }

    /* open catalog tables */

    res = open_table("SYSRTABLES", RDB_TRUE, 4, rtables_attrs, 1, rtables_key,
            RDB_FALSE, RDB_FALSE, &tx, &dbp->rtables_tbp);
    if (res != RDB_OK) {
        goto error;
    }
    assign_table_db(dbp->rtables_tbp, dbp);

    res = open_table("SYSVTABLES", RDB_TRUE, 3, vtables_attrs, 1, vtables_key,
            RDB_FALSE, RDB_FALSE, &tx, &dbp->vtables_tbp);
    if (res != RDB_OK) {
        goto error;
    }
    assign_table_db(dbp->vtables_tbp, dbp);

    res = open_table("SYSTABLEATTRS", RDB_TRUE, 4, table_attr_attrs,
            1, table_attr_key, RDB_FALSE, RDB_FALSE, &tx,
            &dbp->table_attr_tbp);
    if (res != RDB_OK) {
        goto error;
    }
    assign_table_db(dbp->table_attr_tbp, dbp);

    res = open_table("SYSDBTABLES", RDB_TRUE, 2, dbtables_attrs, 1, dbtables_key,
            RDB_FALSE, RDB_FALSE, &tx, &dbp->dbtables_tbp);
    if (res != RDB_OK) {
        goto error;
    }
    assign_table_db(dbp->dbtables_tbp, dbp);

    res = open_table("SYSKEYS", RDB_TRUE, 3, keys_attrs, 1, keys_key,
            RDB_FALSE, RDB_FALSE, &tx, &dbp->keys_tbp);
    assign_table_db(dbp->keys_tbp, dbp);

    /* Check if the database exists by checking if the DBTABLES contains
     * SYSRTABLES for this database.
     */
    RDB_init_tuple(&tpl);
    res = RDB_tuple_set_string(&tpl, "TABLENAME", "SYSRTABLES");
    if (res != RDB_OK) {
        goto error;
    }
    res = RDB_tuple_set_string(&tpl, "DBNAME", name);
    if (res != RDB_OK) {
        goto error;
    }

    res = RDB_table_contains(dbp->dbtables_tbp, &tpl, &tx);
    if (res != RDB_OK) {
        RDB_deinit_tuple(&tpl);
        goto error;
    }
    RDB_deinit_tuple(&tpl);
    
    res = RDB_commit(&tx);
    if (res != RDB_OK)
        return res;
    
    /* Insert database into list */
    dbp->nextdbp = RDB_env_private(envp);
    RDB_env_private(envp) = dbp;

    *dbpp = dbp;

    return RDB_OK;

error:
    RDB_rollback(&tx);
    RDB_deinit_hashmap(&dbp->tbmap);
    free(dbp->name);
    free(dbp);
    return res;
}

static void
free_table(RDB_table *tbp, RDB_environment *envp)
{
    int i;

    if (tbp->is_persistent) {
        RDB_database *dbp;
    
        /* Remove table from all RDB_databases in list */
        for (dbp = RDB_env_private(envp); dbp != NULL;
             dbp = dbp->nextdbp) {
            RDB_table **foundtbpp = (RDB_table **)RDB_hashmap_get(
                    &dbp->tbmap, tbp->name, NULL);
            if (foundtbpp != NULL) {
                void *nullp = NULL;
                RDB_hashmap_put(&dbp->tbmap, tbp->name, &nullp, sizeof nullp);
            }
        }
    }

    if (tbp->kind == RDB_TB_STORED) {
        RDB_drop_type(tbp->typ);
        RDB_deinit_hashmap(&tbp->var.stored.attrmap);
    }

    /* Delete candidate keys */
    for (i = 0; i < tbp->keyc; i++) {
        RDB_free_strvec(tbp->keyv[i].attrc, tbp->keyv[i].attrv);
    }
    free(tbp->keyv);

    free(tbp->name);
    free(tbp);
}

static int close_table(RDB_table *tbp, RDB_environment *envp)
{
    int i;
    int res;

    if (tbp->kind == RDB_TB_STORED) {
        /* close secondary indexes */
        for (i = 0; i < tbp->keyc - 1; i++) {
            RDB_close_index(tbp->var.stored.keyidxv[i]);
        }
   
        /* close recmap */
        res = RDB_close_recmap(tbp->var.stored.recmapp);
        free_table(tbp, envp);
        return res;
    }
    if (tbp->name == NULL) {
        return RDB_drop_table(tbp, NULL);
    }
    return RDB_OK;
}

static int
rm_db(RDB_database *dbp)
{
    /* Remove database from list */
    if (RDB_env_private(dbp->envp) == dbp) {
        RDB_env_private(dbp->envp) = dbp->nextdbp;
    } else {
        RDB_database *hdbp = RDB_env_private(dbp->envp);
        while (hdbp != NULL && hdbp->nextdbp != dbp) {
            hdbp = hdbp->nextdbp;
        }
        if (hdbp == NULL)
            return RDB_ILLEGAL_ARG;
        hdbp->nextdbp = dbp->nextdbp;
    }
    return RDB_OK;
}

int
RDB_release_db(RDB_database *dbp) 
{
    int res;
    int i;
    RDB_table **tbpp;
    int tbcount;
    char **tbnames;

    /* Close all tables which belong to only this database */
    tbcount = RDB_hashmap_size(&dbp->tbmap);
    tbnames = malloc(sizeof(char *) * tbcount);
    RDB_hashmap_keys(&dbp->tbmap, tbnames);
    for (i = 0; i < tbcount; i++) {
        tbpp = (RDB_table **)RDB_hashmap_get(&dbp->tbmap, tbnames[i], NULL);
        if (*tbpp != NULL) {
            if (--(*tbpp)->refcount == 0) {
                res = close_table(*tbpp, dbp->envp);
                if (res != RDB_OK)
                    return res;
            }
        }
    }

    res = rm_db(dbp);
    if (res != RDB_OK)
        return res;
    
    if (--dbp->refcount == 0)
        free_db(dbp);
    return res;
}

int
RDB_drop_db(RDB_database *dbp)
{
    int res;
    RDB_transaction tx;
    RDB_expression *exprp;
    RDB_expression *rexprp;
    RDB_table *vtbp;
    RDB_bool empty;

    res = RDB_begin_tx(&tx, dbp, NULL);
    if (res != RDB_OK)
        return res;

    /* Check if the database contains user tables */
    rexprp = RDB_rel_select(RDB_rel_table(dbp->rtables_tbp),
                            RDB_expr_attr("IS_USER", &RDB_BOOLEAN));
    rexprp = RDB_rel_join(rexprp,
            RDB_rel_select(RDB_rel_table(dbp->dbtables_tbp),
                           RDB_eq(RDB_expr_attr("DBNAME", &RDB_STRING),
                                  RDB_string_const(dbp->name))));
    if (rexprp == NULL) {
        res = RDB_NO_MEMORY;    
        goto error;
    }

    res = RDB_evaluate_table(rexprp, NULL, &tx, &vtbp);
    if (res != RDB_OK) {
        RDB_drop_expr(rexprp);
        goto error;
    }

    res = RDB_table_is_empty(vtbp, &tx, &empty);
    RDB_drop_table(vtbp, &tx);
    RDB_drop_expr(rexprp);
    if (res != RDB_OK) {
        goto error;
    }
    if (!empty) {
        res = RDB_ELEMENT_EXISTS;
        goto error;
    }

    /* Check if the database exists */

    exprp = RDB_eq(RDB_expr_attr("DBNAME", &RDB_STRING),
                  RDB_string_const(dbp->name));
    if (exprp == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }

    res = RDB_select(dbp->dbtables_tbp, RDB_dup_expr(exprp), &vtbp);
    if (res != RDB_OK) {
        goto error;
    }

    res = RDB_table_is_empty(vtbp, &tx, &empty);
    RDB_drop_table(vtbp, &tx);
    if (empty) {
        res = RDB_NOT_FOUND;
        goto error;
    }

    /* Disassociate all tables from database */

    res = RDB_delete(dbp->dbtables_tbp, exprp, &tx);
    if (res != RDB_OK) {
        goto error;
    }

    res = RDB_commit(&tx);
    if (res != RDB_OK)
        return res;

    /* Set refcount to 1 so RDB_release_db will remove it */
    dbp->refcount = 1;

    res = RDB_release_db(dbp);

    return RDB_OK;

error:
    RDB_rollback(&tx);
    return res;
}

int
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_key_attrs keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    /* At least one key is required */
    if (keyc < 1)
        return RDB_ILLEGAL_ARG;

    /* name may only be NULL if table is transient */
    if ((name == NULL) && persistent)
        return RDB_ILLEGAL_ARG;

    return open_table(name, persistent, attrc, heading, keyc, keyv, RDB_TRUE,
                      RDB_TRUE, txp, tbpp);
}

int
RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_key_attrs keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    int res;

    res = _RDB_create_table(name, persistent, attrc, heading, keyc, keyv,
                            txp, tbpp);
    if (res != RDB_OK)
        return res;

    if (persistent) {
        assign_table_db(*tbpp, txp->dbp);

        /* Insert table into catalog */
        return catalog_insert(*tbpp, txp);
    }

    return RDB_OK;
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
                attrsp->attrv[i] = malloc(strlen(sp));
                if (attrsp->attrv[i] == NULL)
                    goto error;
                strcpy(attrsp->attrv[i], sp);
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
    int res;
    int i;
    
    *keyvp = NULL;

    RDB_init_array(&arr);
    
    wherep = RDB_eq(RDB_string_const(name),
            RDB_expr_attr("TABLENAME", &RDB_STRING));
    if (wherep == NULL)
        return RDB_NO_MEMORY;

    res = RDB_select(txp->dbp->keys_tbp, wherep, &vtbp);
    if (res != RDB_OK) {
        RDB_drop_expr(wherep);
        return res;
    }

    res = RDB_table_to_array(vtbp, &arr, 0, NULL, txp);
    if (res != RDB_OK)
        goto error;

    res = RDB_array_length(&arr);
    if (res < 0)
        goto error;
    *keycp = res;

    *keyvp = malloc(sizeof(RDB_key_attrs) * *keycp);
    if (*keyvp == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    for (i = 0; i < *keycp; i++)
        (*keyvp)[i].attrv = NULL;

    RDB_init_tuple(&tpl);

    for (i = 0; i < *keycp; i++) {
        RDB_int kno;
    
        res = RDB_array_get_tuple(&arr, i, &tpl);
        if (res != RDB_OK) {
            RDB_deinit_tuple(&tpl);
            goto error;
        }
        kno = RDB_tuple_get_int(&tpl, "KEYNO");
        res = get_keyattrs(RDB_tuple_get_string(&tpl, "ATTRS"), &(*keyvp)[kno]);
        if (res != RDB_OK) {
            (*keyvp)[kno].attrv = NULL;
            RDB_deinit_tuple(&tpl);
            goto error;
        }
    }
    RDB_deinit_tuple(&tpl);
    RDB_deinit_array(&arr);
    RDB_drop_table(vtbp, txp);

    return RDB_OK;
error:
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
    RDB_deinit_array(&arr);
    return res;
}

static int
get_cat_rtable(RDB_database *dbp, const char *name, RDB_table **tbpp)
{
    RDB_expression *exprp;
    RDB_table *rexprp = NULL;
    RDB_transaction tx;
    RDB_array arr;
    RDB_tuple tpl;
    RDB_bool usr;
    int res;
    RDB_int i, j;
    int attrc;
    RDB_attr *attrv = NULL;
    int keyc;
    RDB_key_attrs *keyv;

    /* read real table data from the catalog */

    RDB_init_array(&arr);

    res = RDB_begin_tx(&tx, dbp, NULL);
    if (res != RDB_OK) {
        RDB_deinit_array(&arr);
        return res;
    }

    exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
            RDB_string_const(name));
    if (exprp == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    res = RDB_select(dbp->rtables_tbp, exprp, &rexprp);
    if (res != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    res = RDB_table_to_array(rexprp, &arr, 0, NULL, &tx);
    if (res != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);
    res = RDB_array_get_tuple(&arr, 0, &tpl);

    RDB_drop_table(rexprp, &tx);
    if (res != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "IS_USER");

    exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
            RDB_string_const(name));
    if (exprp == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    res = RDB_select(dbp->table_attr_tbp, exprp, &rexprp);
    if (res != RDB_OK)
        goto error;
    res = RDB_table_to_array(rexprp, &arr, 0, NULL, &tx);
    if (res != RDB_OK) {
        goto error;
    }

    attrc = RDB_array_length(&arr);
    attrv = malloc(sizeof(RDB_attr) * attrc);

    RDB_init_tuple(&tpl);

    for (i = 0; i < attrc; i++) {
        RDB_type *attrtyp;
        RDB_int val;
        
        RDB_array_get_tuple(&arr, i, &tpl);
        val = RDB_tuple_get_int(&tpl, "I_FNO");
        attrv[RDB_tuple_get_int(&tpl, "I_FNO")].name =
                RDB_dup_str(RDB_tuple_get_string(&tpl, "ATTRNAME"));
        res = RDB_get_type(dbp, RDB_tuple_get_string(&tpl, "TYPE"),
                           &attrtyp);
        if (res != RDB_OK)
            goto error;
        attrv[RDB_tuple_get_int(&tpl, "I_FNO")].type = attrtyp;
    }
    RDB_deinit_tuple(&tpl);

    /* Open the table */

    res = get_keys(name, &tx, &keyc, &keyv);
    if (res != RDB_OK)
        goto error;

    res = open_table(name, RDB_TRUE, attrc, attrv, keyc, keyv, usr, RDB_FALSE,
                     &tx, tbpp);
    for (i = 0; i < keyc; i++) {
        for (j = 0; j < keyv[i].attrc; j++)
            free(keyv[i].attrv[j]);
    }
    free(keyv);

    if (res != RDB_OK)
        goto error;

    assign_table_db(*tbpp, dbp);

    RDB_deinit_array(&arr);

    return RDB_commit(&tx);
error:
    if (attrv != NULL) {
        for (i = 0; i < attrc; i++)
            free(attrv[i].name);
        free(attrv);
    }

    RDB_deinit_array(&arr);
    
    RDB_rollback(&tx);
    return res;
}

static int
get_cat_vtable(RDB_database *dbp, const char *name, RDB_table **tbpp)
{
    RDB_expression *exprp;
    RDB_table *rexprp = NULL;
    RDB_tuple tpl;
    RDB_transaction tx;
    RDB_array arr;
    RDB_value *valp;
    RDB_bool usr;
    int res;
    int pos;

    /* read real table data from the catalog */

    RDB_init_array(&arr);

    res = RDB_begin_tx(&tx, dbp, NULL);
    if (res != RDB_OK) {
        RDB_deinit_array(&arr);
        return res;
    }

    exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
            RDB_string_const(name));
    if (exprp == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    res = RDB_select(dbp->vtables_tbp, exprp, &rexprp);
    if (res != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    res = RDB_table_to_array(rexprp, &arr, 0, NULL, &tx);
    if (res != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);
    res = RDB_array_get_tuple(&arr, 0, &tpl);

    RDB_drop_table(rexprp, &tx);
    if (res != RDB_OK) {
        goto error;
    }

    usr = RDB_tuple_get_bool(&tpl, "IS_USER");

    valp = RDB_tuple_get(&tpl, "I_DEF");
    pos = 0;
    res = _RDB_deserialize_table(valp, &pos, dbp, tbpp);
    if (res != RDB_OK)
        goto error;
    
    RDB_deinit_array(&arr);

    (*tbpp)->is_persistent = RDB_TRUE;
    (*tbpp)->name = RDB_dup_str(name);
    
    RDB_commit(&tx);
    return RDB_OK;
error:
    RDB_deinit_array(&arr);
    
    RDB_rollback(&tx);
    return res;
}

int
RDB_get_table(RDB_database *dbp, const char *name, RDB_table **tbpp)
{
    int res;
    RDB_table **foundtbpp;

    foundtbpp = (RDB_table **)RDB_hashmap_get(&dbp->tbmap, name, NULL);
    if (foundtbpp != NULL) {
        *tbpp = *foundtbpp;
        return RDB_OK;
    }

    res = get_cat_rtable(dbp, name, tbpp);
    if (res == RDB_OK)
        return RDB_OK;
    if (res != RDB_NOT_FOUND)
        return res;

    return get_cat_vtable(dbp, name, tbpp);
}

/*
 * Delete a real table, but not from the catalog
 */
int
_RDB_drop_rtable(RDB_table *tbp, RDB_transaction *txp)
{
    int i;

    /* Delete secondary indexes */
    for (i = 0; i < tbp->keyc - 1; i++) {
        RDB_delete_index(tbp->var.stored.keyidxv[i], txp->dbp->envp);
    }

    /* Delete recmap */
    return RDB_delete_recmap(tbp->var.stored.recmapp, txp->dbp->envp,
               txp->txid);
}

/* Drop the qresults in the list which belong to a table */
static int
drop_table_qresults(RDB_table *tbp, RDB_transaction *txp)
{
    RDB_qresult *prevqrp = NULL;
    RDB_qresult *qrp;
    RDB_qresult *nextqrp;
    int res = RDB_OK;
    int hres;

    qrp = txp->first_qrp;
    while (qrp != NULL) {
        nextqrp = qrp->nextp;
        if (qrp->tablep == tbp) {
            hres = _RDB_drop_qresult(qrp);
            if (prevqrp == NULL)
                txp->first_qrp = nextqrp;
            else
                prevqrp->nextp = nextqrp;
            if (hres != RDB_OK)
                res = hres;
        }
        qrp = nextqrp;
    }
    return res;
}

static int
drop_anon_table(RDB_table *tbp, RDB_transaction *txp)
{
    if (tbp->name == NULL)
        return RDB_drop_table(tbp, txp);
    return RDB_OK;
}

int
_RDB_drop_table(RDB_table *tbp, RDB_transaction *txp, RDB_bool rec)
{
    int res;

    /* !! must check if there is some table which depends on this table
       ... */

    /* Drop the qresults created from this table */
    res = drop_table_qresults(tbp, txp);
    if (res != RDB_OK)
        return res;

    res = RDB_OK;
    switch (tbp->kind) {
        case RDB_TB_STORED:
        {
            RDB_expression *exprp;

            if (tbp->is_persistent) {
                /* Delete table from catalog */
                        
                exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
                               RDB_string_const(tbp->name));
                if (exprp == NULL) {
                    return RDB_NO_MEMORY;
                }
                res = RDB_delete(txp->dbp->rtables_tbp, exprp, txp);
                if (res != RDB_OK)
                    return res;
                RDB_drop_expr(exprp);

                exprp = RDB_eq(RDB_expr_attr("TABLENAME", &RDB_STRING),
                       RDB_string_const(tbp->name));
                if (exprp == NULL) {
                    res = RDB_NO_MEMORY;
                    return res;
                }
                res = RDB_delete(txp->dbp->table_attr_tbp, exprp, txp);
                if (res != RDB_OK)
                    return res;

                res = RDB_delete(txp->dbp->keys_tbp, exprp, txp);
                if (res != RDB_OK)
                    return res;

                RDB_drop_expr(exprp);
            }

            res = _RDB_drop_rtable(tbp, txp);
            break;
        }
        case RDB_TB_SELECT_PINDEX:
            RDB_deinit_value(&tbp->var.select.val);
        case RDB_TB_SELECT:
            RDB_drop_expr(tbp->var.select.exprp);
            if (rec) {
                res = drop_anon_table(tbp->var.select.tbp, txp);
                if (res != RDB_OK)
                    return res;
            }
            break;
        case RDB_TB_JOIN:
            if (rec) {
                res = drop_anon_table(tbp->var.join.tbp1, txp);
                if (res != RDB_OK)
                    return res;
                res = drop_anon_table(tbp->var.join.tbp2, txp);
                if (res != RDB_OK)
                    return res;
            }
            RDB_drop_type(tbp->typ);
            free(tbp->var.join.common_attrv);
            break;
        case RDB_TB_EXTEND:
            if (rec) {
                res = drop_anon_table(tbp->var.extend.tbp, txp);
                if (res != RDB_OK)
                    return res;
            }
            RDB_drop_type(tbp->typ);
            break;
        case RDB_TB_PROJECT:
            if (rec) {
                res = drop_anon_table(tbp->var.project.tbp, txp);
                if (res != RDB_OK)
                    return res;
            }
            RDB_drop_type(tbp->typ);
            break;
        default: ;
    }

    free_table(tbp, txp->dbp->envp);
    return res;
}

int
RDB_drop_table(RDB_table *tbp, RDB_transaction *txp)
{
    return _RDB_drop_table(tbp, txp, RDB_TRUE);
}

int
RDB_set_table_name(RDB_table *tbp, const char *name, RDB_transaction *txp)
{
    if (tbp->is_persistent)
        return RDB_NOT_SUPPORTED;
    
    if (tbp->name != NULL)
        free(tbp->name);
    tbp->name = RDB_dup_str(name);
    if (tbp->name == NULL)
        return RDB_NO_MEMORY;

    return RDB_OK;
}

int
RDB_make_persistent(RDB_table *tbp, RDB_transaction *txp)
{
    RDB_tuple tpl;
    RDB_value defval;
    int res;
    int pos;

    if (tbp->is_persistent)
        return RDB_OK;

    if (tbp->name == NULL)
        return RDB_ILLEGAL_ARG;

    if (tbp->kind == RDB_TB_STORED)
        return RDB_NOT_SUPPORTED;

    RDB_init_tuple(&tpl);

    res = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (res != RDB_OK)
        goto error;

    res = RDB_tuple_set_bool(&tpl, "IS_USER", RDB_TRUE);
    if (res != RDB_OK)
        goto error;

    defval.typ = &RDB_BINARY;
    defval.var.bin.len = RDB_BUF_INITLEN;
    defval.var.bin.datap = malloc(RDB_BUF_INITLEN);
    if (defval.var.bin.datap == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    pos = 0;
    res = _RDB_serialize_table(&defval, &pos, tbp);
    if (res != RDB_OK)
        goto error;

    defval.var.bin.len = pos; /* Only store actual length */
    res = RDB_tuple_set(&tpl, "I_DEF", &defval);
    free(defval.var.bin.datap);
    if (res != RDB_OK)
        goto error;

    res = RDB_insert(txp->dbp->vtables_tbp, &tpl, txp);
    if (res != RDB_OK) {
        if (res == RDB_KEY_VIOLATION)
            res = RDB_ELEMENT_EXISTS;
        goto error;
    }

    res = dbtables_insert(tbp, txp);
    if (res != RDB_OK)
        goto error;

    RDB_deinit_tuple(&tpl);

    return RDB_OK;
error:
    RDB_deinit_tuple(&tpl);
    return res;
}

int
RDB_get_type(RDB_database *dbp, const char *name, RDB_type **typp)
{
    if (strcmp(name, "BOOLEAN") == 0) {
        *typp = &RDB_BOOLEAN;
        return RDB_OK;
    }
    if (strcmp(name, "INTEGER") == 0) {
        *typp = &RDB_INTEGER;
        return RDB_OK;
    }
    if (strcmp(name, "RATIONAL") == 0) {
        *typp = &RDB_RATIONAL;
        return RDB_OK;
    }
    if (strcmp(name, "STRING") == 0) {
        *typp = &RDB_STRING;
        return RDB_OK;
    }
    if (strcmp(name, "BINARY") == 0) {
        *typp = &RDB_BINARY;
        return RDB_OK;
    }
    /* search for user defined type 
     * ... */
    return RDB_NOT_FOUND;
}
