/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "typeimpl.h"
#include "serialize.h"
#include "catalog.h"
#include <gen/strfns.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

/* name of the file in which the tables are physically stored */
#define RDB_DATAFILE "rdata"

/* initial capacities of attribute map and table map */
enum {
    RDB_DFL_MAP_CAPACITY = 37,
};

/* Return the length (in bytes) of the internal representation
 * of the type pointed to by typ.
 */
static int replen(const RDB_type *typ) {
    return typ->ireplen;
}

/* Return RDB_TRUE if the attribute name is contained
 * in the attribute list pointed to by keyp.
 */
static RDB_bool
key_contains(const RDB_str_vec *keyp, const char *name)
{
    int i;
    
    for (i = 0; i < keyp->attrc; i++) {
        if (strcmp(keyp->attrv[i], name) == 0)
            return RDB_TRUE;
    }
    return RDB_FALSE;
}

static int
open_key_index(RDB_table *tbp, int keyno, const RDB_str_vec *keyattrsp,
                 RDB_bool create, RDB_transaction *txp, RDB_index **idxp)
{
    int ret;
    int i;
    char *idx_name = malloc(strlen(tbp->name) + 4);
    int *fieldv = malloc(sizeof(int *) * keyattrsp->attrc);

    if (idx_name == NULL || fieldv == NULL) {
        ret = RDB_NO_MEMORY;
        RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
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
        ret = RDB_create_index(tbp->var.stored.recmapp,
                  tbp->is_persistent ? idx_name : NULL,
                  tbp->is_persistent ? RDB_DATAFILE : NULL,
                  txp->dbp->dbrootp->envp, keyattrsp->attrc, fieldv, RDB_TRUE, txp->txid, idxp);
    } else {
        /* open index */
        ret = RDB_open_index(tbp->var.stored.recmapp, idx_name, RDB_DATAFILE,
                  txp->dbp->dbrootp->envp, keyattrsp->attrc, fieldv, RDB_TRUE, txp->txid, idxp);
    }

    if (ret != RDB_OK)
        goto error;

    ret = RDB_OK;
error:
    free(idx_name);
    free(fieldv);
    return ret;
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
int
_RDB_open_table(const char *name, RDB_bool persistent,
           int attrc, RDB_attr heading[],
           int keyc, RDB_str_vec keyv[], RDB_bool usr,
           RDB_bool create, RDB_transaction *txp, RDB_table **tbpp)
{
    RDB_table *tbp = NULL;
    int *flens = NULL;
    int ret, i, ki, di;

    /* choose key #0 as for the primary index */
    const RDB_str_vec *prkeyattrs = &keyv[0];
    
    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    tbp = *tbpp = malloc(sizeof (RDB_table));
    if (tbp == NULL) {
        RDB_rollback(txp);
        return RDB_NO_MEMORY;
    }
    tbp->is_user = usr;
    tbp->is_persistent = persistent;
    tbp->keyv = NULL;
    tbp->refcount = 1;
    RDB_init_hashmap(&tbp->var.stored.attrmap, RDB_DFL_MAP_CAPACITY);

    tbp->kind = RDB_TB_STORED;
    if (name != NULL) {
        tbp->name = RDB_dup_str(name);
        if (tbp->name == NULL) {
            ret = RDB_NO_MEMORY;
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
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
        ret = RDB_NO_MEMORY;
        RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
        goto error;
    }

    for (i = 0; i < keyc; i++) {
        int j;

        /* check if all the key attributes appear in the heading */
        for (j = 0; j < keyv[i].attrc; j++) {
            if (_RDB_tuple_type_attr(tbp->typ->var.basetyp, keyv[i].attrv[j])
                    == NULL) {
                ret = RDB_INVALID_ARGUMENT;
                goto error;
            }
        }
    }

    flens = malloc(sizeof(int) * attrc);
    if (flens == NULL)
        goto error;
    ki = 0;
    di = prkeyattrs->attrc;
    for (i = 0; i < attrc; i++) {
        RDB_int fno;
    
        if (key_contains(prkeyattrs, heading[i].name))
            fno = ki++;
        else
            fno = di++;
        ret = RDB_hashmap_put(&tbp->var.stored.attrmap, heading[i].name,
                              &fno, sizeof fno);
        if (ret != RDB_OK)
            goto error;

        /* Only scalar types supported by this version */
        if (!RDB_type_is_scalar(heading[i].typ)) {
            ret = RDB_NOT_SUPPORTED;
            goto error;
        }

        flens[fno] = replen(heading[i].typ);
    }
    if (create) {
        ret = RDB_create_recmap(persistent ? name : NULL,
                    persistent ? RDB_DATAFILE : NULL, txp->dbp->dbrootp->envp,
                    attrc, flens, prkeyattrs->attrc,
                    txp->txid, &tbp->var.stored.recmapp);
    } else {
        ret = RDB_open_recmap(name, RDB_DATAFILE, txp->dbp->dbrootp->envp, attrc, flens,
                    prkeyattrs->attrc, txp->txid, &tbp->var.stored.recmapp);
    }

    if (ret != RDB_OK)
        goto error;

    /* Open/create indexes if there is more than one key */
    if (keyc > 1) {
        tbp->var.stored.keyidxv = malloc(sizeof(RDB_index *) * (keyc - 1));
        if (tbp->var.stored.keyidxv == NULL) {
            ret = RDB_NO_MEMORY;
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            goto error;
        }
        for (i = 1; i < keyc; i++) {    
            ret = open_key_index(tbp, i, &keyv[i], create, txp,
                          &tbp->var.stored.keyidxv[i - 1]);
            if (ret != RDB_OK)
                goto error;
        }
    }

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
        }
        RDB_destroy_hashmap(&tbp->var.stored.attrmap);
        free(tbp);
    }
    if (RDB_is_syserr(ret))
        RDB_rollback(txp);
    return ret;
}

/* Associate a RDB_table structure with a RDB_database structure. */
int
_RDB_assign_table_db(RDB_table *tbp, RDB_database *dbp)
{
    /* Insert table into table map */
    return RDB_hashmap_put(&dbp->tbmap, tbp->name, &tbp, sizeof (RDB_table *));
}

static void
free_dbroot(RDB_dbroot *dbrootp)
{
    RDB_destroy_hashmap(&dbrootp->typemap);
    
    /* !! free RDB_upd_op and RDB_ro_op structures */
    RDB_destroy_hashmap(&dbrootp->ro_opmap);
    RDB_destroy_hashmap(&dbrootp->upd_opmap);
    free(dbrootp);
}

/* cleanup function to close all DBs and tables */
static void
cleanup_env(RDB_environment *envp)
{
    RDB_dbroot *dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    RDB_database *dbp = dbrootp->firstdbp;
    RDB_database *nextdbp;

    while (dbp != NULL) {
        nextdbp = dbp->nextdbp;
        RDB_release_db(dbp);
        dbp = nextdbp;
    }
    free_dbroot(dbrootp);
    lt_dlexit();
}

static RDB_dbroot *
new_dbroot(RDB_environment *envp)
{
    RDB_dbroot *dbrootp = malloc(sizeof (RDB_dbroot));

    if (dbrootp == NULL)
        return NULL;
    
    dbrootp->envp = envp;
    RDB_init_hashmap(&dbrootp->typemap, RDB_DFL_MAP_CAPACITY);
    RDB_init_hashmap(&dbrootp->ro_opmap, RDB_DFL_MAP_CAPACITY);
    RDB_init_hashmap(&dbrootp->upd_opmap, RDB_DFL_MAP_CAPACITY);
    dbrootp->firstdbp = NULL;

    return dbrootp;
}

static RDB_database *
new_db(const char *name)
{
    RDB_database *dbp;

    /* Allocate structure */
    dbp = malloc(sizeof (RDB_database));
    if (dbp == NULL)
        return NULL;

    /* Set name */
    dbp->name = RDB_dup_str(name);
    if (dbp->name == NULL) {
        goto error;
    }

    /* Initialize structure */

    dbp->refcount = 1;

    RDB_init_hashmap(&dbp->tbmap, RDB_DFL_MAP_CAPACITY);

    return dbp;

error:
    free(dbp->name);
    free(dbp);
    return NULL;
}

static void
free_db(RDB_database *dbp) {
    RDB_destroy_hashmap(&dbp->tbmap);
    free(dbp->name);
    free(dbp);
}

/* check if the name is legal */
RDB_bool
_RDB_legal_name(const char *name)
{
    int i;

    for (i = 0; name[i] != '\0'; i++) {
        if (!isprint(name[i]) || isspace(name[i]) || (name[i] == '$'))
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/*
 * Create and initialize RDB_dbroot structure. Use dbp to create the transaction.
 * If newdb is true, create the database in the catalog.
 */
static int
create_dbroot(const char *dbname, RDB_environment *envp, RDB_bool newdb,
              RDB_database *dbp, RDB_dbroot **dbrootpp)
{
    RDB_transaction tx;
    RDB_dbroot *dbrootp;
    int ret;

    /* Set cleanup function */
    RDB_set_env_closefn(envp, cleanup_env);

    dbrootp = new_dbroot(envp);
    if (dbrootp == NULL)
        return RDB_NO_MEMORY;

    dbp->dbrootp = dbrootp;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = _RDB_open_systables(&tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }

    if (newdb) {
        ret = _RDB_create_db_in_cat(&tx);
        if (ret != RDB_OK) {
            RDB_rollback(&tx);
            goto error;
        }
    }

    ret = RDB_commit(&tx);
    if (ret != RDB_OK)
        goto error;

    RDB_env_private(envp) = dbrootp;
    
    *dbrootpp = dbrootp;
    return RDB_OK;
error:
    free_dbroot(dbrootp);
    return ret;
}

int
RDB_create_db_from_env(const char *name, RDB_environment *envp,
                       RDB_database **dbpp)
{
    int ret;
    RDB_database *dbp;
    RDB_dbroot *dbrootp;

    if (!_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    dbp = new_db(name);
    if (dbp == NULL)
        return RDB_NO_MEMORY;

    if (dbrootp != NULL) {
        /*
         * dbroot found, so create DB in catalog
         */
        RDB_transaction tx;

        dbp->dbrootp = dbrootp;

        ret = RDB_begin_tx(&tx, dbp, NULL);
        if (ret != RDB_OK) {
            goto error;
        }

        ret = _RDB_create_db_in_cat(&tx);
        if (ret != RDB_OK) {
            RDB_rollback(&tx);
            goto error;
        }

        ret = RDB_commit(&tx);
        if (ret != RDB_OK) {
            goto error;
        }
    } else {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure, which also creates
         * the DB in the catalog (and ceates the catalog if necessary).
         */
        _RDB_init_builtin_types();
        ret = create_dbroot(name, envp, RDB_TRUE, dbp, &dbrootp);
        if (ret != RDB_OK) {
            goto error;
        }
        lt_dlinit();
    }

    /* Insert database into list */
    dbp->nextdbp = dbrootp->firstdbp;
    dbrootp->firstdbp = dbp;

    *dbpp = dbp;
    return RDB_OK;

error:
    free_db(dbp);
    return ret;
}

int
RDB_get_db_from_env(const char *name, RDB_environment *envp,
                    RDB_database **dbpp)
{
    int ret;
    RDB_database *dbp, *idbp;
    RDB_transaction tx;
    RDB_tuple tpl;
    RDB_dbroot *dbrootp = (RDB_dbroot *)RDB_env_private(envp);

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */

        /* Create db structure required by create_dbroot() */
        dbp = new_db(name);
        if (dbp == NULL)
            return RDB_NO_MEMORY;

        _RDB_init_builtin_types();
        lt_dlinit();
        ret = create_dbroot(name, envp, RDB_FALSE, dbp, &dbrootp);
        if (ret != RDB_OK) {
            goto error;
        }
    }

    /* search the DB list for the database */
    for (idbp = dbrootp->firstdbp; idbp != NULL; idbp = dbp->nextdbp) {
        if (strcmp(dbp->name, name) == 0) {
        
            /* dbp is not used */
            free_db(dbp);

            *dbpp = idbp;
            idbp->refcount++;
            return RDB_OK;
        }
    }

    /* Not found */
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    /* Check if the database exists by checking if the DBTABLES contains
     * SYS_RTABLES for this database.
     */
    RDB_init_tuple(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", "SYS_RTABLES");
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }
    ret = RDB_tuple_set_string(&tpl, "DBNAME", name);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }

    ret = RDB_table_contains(dbp->dbrootp->dbtables_tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        RDB_rollback(&tx);
        goto error;
    }
    RDB_destroy_tuple(&tpl);
    
    ret = RDB_commit(&tx);
    if (ret != RDB_OK)
        return ret;
    
    /* Insert database into list */
    dbp->nextdbp = dbrootp->firstdbp;
    dbrootp->firstdbp = dbp;

    *dbpp = dbp;

    return RDB_OK;

error:
    free_db(dbp);
    lt_dlexit();
    return ret;
}

static void
free_table(RDB_table *tbp, RDB_environment *envp)
{
    int i;
    RDB_dbroot *dbrootp = (RDB_dbroot *)RDB_env_private(envp);

    if (tbp->is_persistent) {
        RDB_database *dbp;
    
        /* Remove table from all RDB_databases in list */
        for (dbp = dbrootp->firstdbp; dbp != NULL; dbp = dbp->nextdbp) {
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
        RDB_destroy_hashmap(&tbp->var.stored.attrmap);
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
    int ret;

    if (tbp->kind == RDB_TB_STORED) {
        /* close secondary indexes */
        for (i = 0; i < tbp->keyc - 1; i++) {
            RDB_close_index(tbp->var.stored.keyidxv[i]);
        }
   
        /* close recmap */
        ret = RDB_close_recmap(tbp->var.stored.recmapp);
        free_table(tbp, envp);
        return ret;
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
    if (dbp->dbrootp->firstdbp == dbp) {
        dbp->dbrootp->firstdbp = dbp->nextdbp;
    } else {
        RDB_database *hdbp = dbp->dbrootp->firstdbp;
        while (hdbp != NULL && hdbp->nextdbp != dbp) {
            hdbp = hdbp->nextdbp;
        }
        if (hdbp == NULL)
            return RDB_INVALID_ARGUMENT;
        hdbp->nextdbp = dbp->nextdbp;
    }
    return RDB_OK;
}

int
RDB_release_db(RDB_database *dbp) 
{
    int ret;
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
                ret = close_table(*tbpp, dbp->dbrootp->envp);
                if (ret != RDB_OK)
                    return ret;
            }
        }
    }

    ret = rm_db(dbp);
    if (ret != RDB_OK)
        return ret;
    
    if (--dbp->refcount == 0)
        free_db(dbp);
    return ret;
}

int
RDB_drop_db(RDB_database *dbp)
{
    int ret;
    RDB_transaction tx;
    RDB_expression *exprp;
    RDB_table *vtbp, *vtb2p;
    RDB_bool empty;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK)
        return ret;

    /* Check if the database contains user tables */
    exprp = RDB_expr_attr("IS_USER", &RDB_BOOLEAN);
    ret = RDB_select(dbp->dbrootp->rtables_tbp, exprp, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    exprp = RDB_eq(RDB_expr_attr("DBNAME", &RDB_STRING),
                                 RDB_string_const(dbp->name));
    ret = RDB_select(dbp->dbrootp->dbtables_tbp, exprp, &vtb2p);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        RDB_drop_table(vtbp, &tx);
        goto error;
    }
    
    ret = RDB_join(vtbp, vtb2p, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, &tx);
        RDB_drop_table(vtb2p, &tx);
        goto error;
    }
    
    ret = RDB_table_is_empty(vtbp, &tx, &empty);
    RDB_drop_table(vtbp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    if (!empty) {
        ret = RDB_ELEMENT_EXISTS;
        goto error;
    }

    /* Check if the database exists */

    exprp = RDB_eq(RDB_expr_attr("DBNAME", &RDB_STRING),
                  RDB_string_const(dbp->name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    ret = RDB_select(dbp->dbrootp->dbtables_tbp, exprp, &vtbp);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_table_is_empty(vtbp, &tx, &empty);
    RDB_drop_table(vtbp, &tx);
    if (empty) {
        ret = RDB_NOT_FOUND;
        goto error;
    }

    /* Disassociate all tables from database */

    exprp = RDB_eq(RDB_expr_attr("DBNAME", &RDB_STRING),
                  RDB_string_const(dbp->name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    ret = RDB_delete(dbp->dbrootp->dbtables_tbp, exprp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_commit(&tx);
    if (ret != RDB_OK)
        return ret;

    /* Set refcount to 1 so RDB_release_db will remove it */
    dbp->refcount = 1;

    ret = RDB_release_db(dbp);

    return RDB_OK;

error:
    RDB_rollback(&tx);
    return ret;
}

int
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_str_vec keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    /* At least one key is required */
    if (keyc < 1)
        return RDB_INVALID_ARGUMENT;

    /* name may only be NULL if table is transient */
    if ((name == NULL) && persistent)
        return RDB_INVALID_ARGUMENT;

    return _RDB_open_table(name, persistent, attrc, heading, keyc, keyv,
                           RDB_TRUE, RDB_TRUE, txp, tbpp);
}

int
RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_str_vec keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    int i;

    if (!_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    for (i = 0; i < attrc; i++) {
        if (!_RDB_legal_name(heading[i].name))
            return RDB_INVALID_ARGUMENT;
    }

    ret = _RDB_create_table(name, persistent, attrc, heading, keyc, keyv,
                            txp, tbpp);
    if (ret != RDB_OK)
        return ret;

    if (persistent) {
        _RDB_assign_table_db(*tbpp, txp->dbp);

        /* Insert table into catalog */
        ret = _RDB_catalog_insert(*tbpp, txp);
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret))
                RDB_rollback(txp);
            return ret;
        }
    }

    return RDB_OK;
}

int
RDB_get_table(const char *name, RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    RDB_table **foundtbpp;

    foundtbpp = (RDB_table **)RDB_hashmap_get(&txp->dbp->tbmap, name, NULL);
    if (foundtbpp != NULL) {
        *tbpp = *foundtbpp;
        return RDB_OK;
    }

    ret = _RDB_get_cat_rtable(name, txp, tbpp);
    if (ret == RDB_OK)
        return RDB_OK;
    if (ret != RDB_NOT_FOUND)
        return ret;

    return _RDB_get_cat_vtable(name, txp, tbpp);
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
        RDB_delete_index(tbp->var.stored.keyidxv[i], txp->dbp->dbrootp->envp);
    }

    /* Delete recmap */
    return RDB_delete_recmap(tbp->var.stored.recmapp, txp->dbp->dbrootp->envp,
               txp->txid);
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
    int ret;
    int i;

    /* !! should check if there is some table which depends on this table
       ... */

    ret = RDB_OK;
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
                ret = RDB_delete(txp->dbp->dbrootp->rtables_tbp, exprp, txp);
                if (ret != RDB_OK)
                    return ret;

                ret = RDB_delete(txp->dbp->dbrootp->table_attr_tbp, exprp, txp);
                if (ret != RDB_OK)
                    return ret;

                ret = RDB_delete(txp->dbp->dbrootp->table_attr_defvals_tbp, exprp, txp);
                if (ret != RDB_OK)
                    return ret;

                ret = RDB_delete(txp->dbp->dbrootp->keys_tbp, exprp, txp);
                if (ret != RDB_OK)
                    return ret;

                RDB_drop_expr(exprp);
            }

            ret = _RDB_drop_rtable(tbp, txp);
            break;
        }
        case RDB_TB_SELECT_PINDEX:
            RDB_destroy_value(&tbp->var.select.val);
        case RDB_TB_SELECT:
            RDB_drop_expr(tbp->var.select.exprp);
            if (rec) {
                ret = drop_anon_table(tbp->var.select.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_JOIN:
            if (rec) {
                ret = drop_anon_table(tbp->var.join.tbp1, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.join.tbp2, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_drop_type(tbp->typ);
            free(tbp->var.join.common_attrv);
            break;
        case RDB_TB_EXTEND:
            if (rec) {
                ret = drop_anon_table(tbp->var.extend.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.extend.attrc; i++)
                free(tbp->var.extend.attrv[i].name);
            RDB_drop_type(tbp->typ);
            break;
        case RDB_TB_PROJECT:
            if (rec) {
                ret = drop_anon_table(tbp->var.project.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_drop_type(tbp->typ);
            break;
        default: ;
    }

    free_table(tbp, txp->dbp->dbrootp->envp);
    return ret;
}

int
RDB_drop_table(RDB_table *tbp, RDB_transaction *txp)
{
    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;
    return _RDB_drop_table(tbp, txp, RDB_TRUE);
}

int
RDB_set_table_name(RDB_table *tbp, const char *name, RDB_transaction *txp)
{
    if (!_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

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
    int ret;

    if (tbp->is_persistent)
        return RDB_OK;

    if (tbp->name == NULL)
        return RDB_INVALID_ARGUMENT;

    if (tbp->kind == RDB_TB_STORED)
        return RDB_NOT_SUPPORTED;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    RDB_init_tuple(&tpl);
    RDB_init_value(&defval);

    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_tuple_set_bool(&tpl, "IS_USER", RDB_TRUE);
    if (ret != RDB_OK)
        goto error;

    ret = _RDB_table_to_value(tbp, &defval);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set(&tpl, "I_DEF", &defval);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(txp->dbp->dbrootp->vtables_tbp, &tpl, txp);
    if (ret != RDB_OK) {
        if (ret == RDB_KEY_VIOLATION)
            ret = RDB_ELEMENT_EXISTS;
        goto error;
    }

    ret = _RDB_dbtables_insert(tbp, txp);
    if (ret != RDB_OK)
        goto error;

    RDB_destroy_tuple(&tpl);
    RDB_destroy_value(&defval);

    return RDB_OK;
error:
    RDB_destroy_tuple(&tpl);
    RDB_destroy_value(&defval);
    return ret;
}


int
RDB_get_type(const char *name, RDB_transaction *txp, RDB_type **typp)
{
    RDB_type **foundtypp;

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
    /* search for user defined type */

    foundtypp = (RDB_type **)RDB_hashmap_get(&txp->dbp->dbrootp->typemap,
            name, NULL);
    if (foundtypp != NULL) {
        *typp = *foundtypp;
        return RDB_OK;
    }
    
    return _RDB_get_cat_type(name, txp, typp);
}

int
RDB_define_type(const char *name, int repc, RDB_possrep repv[],
                RDB_transaction *txp)
{
    RDB_tuple tpl;
    RDB_value conval;
    int ret;
    int i, j;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    RDB_init_tuple(&tpl);
    RDB_init_value(&conval);

    /*
     * Insert tuple into SYS_TYPES
     */

    ret = RDB_tuple_set_string(&tpl, "TYPENAME", name);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "I_LIBNAME", "");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "I_AREP_TYPE", "");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "I_AREP_LEN", -2);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(txp->dbp->dbrootp->types_tbp, &tpl, txp);
    if (ret != RDB_OK)
        goto error;

    /*
     * Insert tuple into SYS_POSSREPS
     */   

    for (i = 0; i < repc; i++) {
        char *prname = repv[i].name;
        RDB_expression *exp = repv[i].constraintp;

        if (prname == NULL) {
            /* possrep name may be NULL if there's only 1 possrep */
            if (repc > 1) {
                ret = RDB_INVALID_ARGUMENT;
                goto error;
            }
            prname = (char *)name;
        }
        ret = RDB_tuple_set_string(&tpl, "POSSREPNAME", prname);
        if (ret != RDB_OK)
            goto error;
        
        /* Check if type of constraint is RDB_BOOLEAN */
        if (exp != NULL && RDB_expr_type(exp) != &RDB_BOOLEAN) {
            ret = RDB_TYPE_MISMATCH;
            goto error;
        }

        /* Store constraint in tuple */
        ret = _RDB_expr_to_value(exp, &conval);
        if (ret != RDB_OK)
            goto error;
        ret = RDB_tuple_set(&tpl, "I_CONSTRAINT", &conval);
        if (ret != RDB_OK)
            goto error;

        /* Store tuple */
        ret = RDB_insert(txp->dbp->dbrootp->possreps_tbp, &tpl, txp);
        if (ret != RDB_OK)
            goto error;

        for (j = 0; j < repv[i].compc; j++) {
            char *cname = repv[i].compv[j].name;

            if (cname == NULL) {
                if (repv[i].compc > 1) {
                    ret = RDB_INVALID_ARGUMENT;
                    goto error;
                }
                cname = prname;
            }
            ret = RDB_tuple_set_int(&tpl, "COMPNO", (RDB_int)j);
            if (ret != RDB_OK)
                goto error;
            ret = RDB_tuple_set_string(&tpl, "COMPNAME", cname);
            if (ret != RDB_OK)
                goto error;
            ret = RDB_tuple_set_string(&tpl, "COMPTYPENAME",
                    repv[i].compv[j].typ->name);
            if (ret != RDB_OK)
                goto error;

            ret = RDB_insert(txp->dbp->dbrootp->possrepcomps_tbp, &tpl, txp);
            if (ret != RDB_OK)
                goto error;
        }
    }

    RDB_destroy_value(&conval);    
    RDB_destroy_tuple(&tpl);
    
    return RDB_OK;
    
error:
    RDB_destroy_value(&conval);    
    RDB_destroy_tuple(&tpl);

    return ret;
}

int
RDB_implement_type(const char *name, const char *libname, RDB_type *arep,
                   int options, size_t areplen, RDB_transaction *txp)
{
    RDB_expression *exp, *wherep;
    RDB_attr_update upd[3];
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    if (libname != NULL) {
        if (arep == NULL) {
            if (!(RDB_FIXED_BINARY & options))
                return RDB_INVALID_ARGUMENT;
        } else {
            if (!RDB_type_is_builtin(arep))
                return RDB_NOT_SUPPORTED;
        }
    } else {
        /*
         * No libname given, so selector and getters/setters must be provided
         * by the system
         */
        RDB_table *tmptb1p;
        RDB_table *tmptb2p;
        RDB_tuple tpl;
        char *possrepname;

        if (arep != NULL)
            return RDB_INVALID_ARGUMENT;

        /* # of possreps must be one */
        ret = _RDB_possreps_query(name, txp, &tmptb1p);
        if (ret != RDB_OK)
            return ret;
        RDB_init_tuple(&tpl);
        ret = RDB_extract_tuple(tmptb1p, &tpl, txp);
        RDB_drop_table(tmptb1p, txp);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret;
        }
        possrepname = RDB_tuple_get_string(&tpl, "POSSREPNAME");

        /* only 1 possrep component currently supported */
        ret = _RDB_possrepcomps_query(name, possrepname, txp, &tmptb2p);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret;
        }
        ret = RDB_extract_tuple(tmptb2p, &tpl, txp);
        RDB_drop_table(tmptb2p, txp);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret == RDB_INVALID_ARGUMENT ? RDB_NOT_SUPPORTED : ret;
        }
        ret = RDB_get_type(RDB_tuple_get_string(&tpl, "COMPTYPENAME"), txp, &arep);
        RDB_destroy_tuple(&tpl);
        if (ret != RDB_OK) {
            return ret;
        }
        /* Empty string indicates that there is no library */
        libname = "";
    }

    exp = RDB_expr_attr("TYPENAME", &RDB_STRING);
    if (exp == NULL) {
        return RDB_NO_MEMORY;
    }
    wherep = RDB_eq(exp, RDB_string_const(name));
    if (wherep == NULL) {
        RDB_drop_expr(exp);
        return RDB_NO_MEMORY;
    }    

    upd[0].exp = upd[1].exp = NULL;

    upd[0].name = "I_AREP_TYPE";
    upd[0].exp = RDB_string_const(arep != NULL ? arep->name : "");
    if (upd[0].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    upd[1].name = "I_AREP_LEN";
    upd[1].exp = RDB_int_const(options & RDB_FIXED_BINARY ? areplen : arep->ireplen);
    if (upd[1].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    upd[2].name = "I_LIBNAME";
    upd[2].exp = RDB_string_const(libname);
    if (upd[2].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    ret = RDB_update(txp->dbp->dbrootp->types_tbp, wherep, 3, upd, txp);

cleanup:    
    if (upd[0].exp != NULL)
        RDB_drop_expr(upd[0].exp);
    if (upd[1].exp != NULL)
        RDB_drop_expr(upd[1].exp);
    if (upd[2].exp != NULL)
        RDB_drop_expr(upd[2].exp);
    RDB_drop_expr(wherep);

    return ret;
}

int
RDB_define_ro_op(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
                 const char *libname, const char *symname, const char *iarg,
                 RDB_transaction *txp)
{
    RDB_tuple tpl;
    char *typesbuf;
    int ret;

    if (!RDB_type_is_scalar(rtyp))
        return RDB_NOT_SUPPORTED;

    RDB_init_tuple(&tpl);
    ret = RDB_tuple_set_string(&tpl, "NAME", name);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "RTYPE", RDB_type_name(rtyp));
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "LIB", libname);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "SYMBOL", symname);
    if (ret != RDB_OK)
        goto cleanup;
    if (iarg == NULL)
        iarg = "";
    RDB_tuple_set_string(&tpl, "IARG", iarg);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to concatenation of arg type names */
    typesbuf = _RDB_make_typestr(argc, argtv);
    if (typesbuf == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    ret = RDB_tuple_set_string(&tpl, "ARGTYPES", typesbuf);
    free(typesbuf);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->ro_ops_tbp, &tpl, txp);

cleanup:
    RDB_destroy_tuple(&tpl);
    return ret;
}

int
RDB_define_update_op(const char *name, int argc, RDB_type *argtv[],
                  int updargc, int updargv[],
                  const char *libname, const char *symname, const char *iarg,
                  RDB_transaction *txp)
{
    RDB_tuple tpl;
    char *typesbuf;
    int i;
    int ret;

    for (i = 0; i < argc; i++) {
        if (!RDB_type_is_scalar(argtv[i]))
            return RDB_NOT_SUPPORTED;
    }

    RDB_init_tuple(&tpl);
    ret = RDB_tuple_set_string(&tpl, "NAME", name);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "LIB", libname);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "SYMBOL", symname);
    if (ret != RDB_OK)
        goto cleanup;
    if (iarg == NULL)
        iarg = "";
    RDB_tuple_set_string(&tpl, "IARG", iarg);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to concatenation of arg type names */
    typesbuf = _RDB_make_typestr(argc, argtv);
    if (typesbuf == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    
    ret = RDB_tuple_set_string(&tpl, "ARGTYPES", typesbuf);
    free(typesbuf);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->upd_ops_tbp, &tpl, txp);

cleanup:
    RDB_destroy_tuple(&tpl);
    return ret;
}

static RDB_ro_op *
get_ro_op(const RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_value *argv[])
{
    RDB_ro_op *op;
    RDB_ro_op **opp = RDB_hashmap_get(&dbrootp->ro_opmap, name, NULL);    

    if (opp == NULL)
        return NULL;
    op = *opp;
    
    /* Find a operation with same signature */
    do {
        if (op->argc == argc) {
            int i;

            for (i = 0; (i < argc)
                    && !RDB_type_equals(op->argtv[i], argv[i]->typ);
                 i++);
            if (i >= argc) {
                /* Found */
                return op;
            }
        }
        op = op->nextp;
    } while (op != NULL);

    return NULL;
}

int
RDB_call_ro_op(const char *name, int argc, RDB_value *argv[],
               RDB_value *retvalp, RDB_transaction *txp)
{
    RDB_ro_op *op;
    int ret;

    op = get_ro_op(txp->dbp->dbrootp, name, argc, argv);
    if (op == NULL) {
        ret = _RDB_get_cat_ro_op(name, argc, argv, txp, &op);
        if (ret != RDB_OK)
            return ret;
    }

    return (*op->funcp) (name, argc, argv, retvalp, op->iargp, txp);
}

static RDB_upd_op *
get_upd_op(const RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_value *argv[])
{
    RDB_upd_op *op;
    RDB_upd_op **opp = RDB_hashmap_get(&dbrootp->upd_opmap, name, NULL);    

    if (opp == NULL)
        return NULL;
    op = *opp;
    
    /* Find a operation with same signature */
    do {
        if (op->argc == argc) {
            int i;

            for (i = 0; (i < argc)
                    && !RDB_type_equals(op->argtv[i], argv[i]->typ);
                 i++);
            if (i >= argc) {
                /* Found */
                return op;
            }
        }
        op = op->nextp;
    } while (op != NULL);

    return NULL;
}

int
RDB_call_update_op(const char *name, int argc, RDB_value *argv[],
                RDB_transaction *txp)
{
    RDB_upd_op *op;
    int ret;

    op = get_upd_op(txp->dbp->dbrootp, name, argc, argv);
    if (op == NULL) {
        ret = _RDB_get_cat_upd_op(name, argc, argv, txp, &op);
        if (ret != RDB_OK)
            return ret;
    }

    return (*op->funcp) (name, argc, argv, 0, NULL, op->iargp, txp);
}

int
RDB_drop_op(const char *name, RDB_transaction *txp)
{
    return RDB_NOT_SUPPORTED;
}
