/*
 * Copyright (C) 2003, 2004 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include "typeimpl.h"
#include "serialize.h"
#include "catalog.h"
#include <gen/hashmapit.h>
#include <gen/strfns.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

RDB_environment *
RDB_db_env(RDB_database *dbp) {
    return dbp->dbrootp->envp;
}

/* Associate a RDB_table structure with a RDB_database structure. */
int
_RDB_assoc_table_db(RDB_table *tbp, RDB_database *dbp)
{
    /* Insert table into table map */
    return RDB_hashmap_put(&dbp->tbmap, tbp->name, &tbp, sizeof (RDB_table *));
}

static void
free_dbroot(RDB_dbroot *dbrootp)
{
    RDB_hashmap_iter it;
    char *keyp;
    void *datap;

    RDB_destroy_hashmap(&dbrootp->typemap);

    /*
     * destroy user-defined operators in memory
     */
    RDB_init_hashmap_iter(&it, &dbrootp->ro_opmap);
    while ((datap = RDB_hashmap_next(&it, &keyp, NULL)) != NULL) {
        RDB_ro_op *op = *(RDB_ro_op **)datap;

        if (op != NULL)
            _RDB_free_ro_ops(op);
    }
    RDB_destroy_hashmap_iter(&it);

    RDB_init_hashmap_iter(&it, &dbrootp->upd_opmap);
    while ((datap = RDB_hashmap_next(&it, &keyp, NULL)) != NULL) {
        RDB_upd_op *op = *(RDB_upd_op **) datap;

        if (op != NULL)
            _RDB_free_upd_ops(op);
    }
    RDB_destroy_hashmap_iter(&it);

    RDB_destroy_hashmap(&dbrootp->ro_opmap);
    RDB_destroy_hashmap(&dbrootp->upd_opmap);
    free(dbrootp);
}

static int
close_table(RDB_table *tbp, RDB_environment *envp)
{
    int i;
    int ret;
    RDB_dbroot *dbrootp;
    RDB_database *dbp;

    if (tbp->kind == RDB_TB_STORED) {
        if (tbp->var.stored.indexc > 0) {
            /* close secondary indexes */
            for (i = 0; i < tbp->var.stored.indexc; i++) {
                if (tbp->var.stored.indexv[i].idxp != NULL)
                    RDB_close_index(tbp->var.stored.indexv[i].idxp);
            }
        }

        /* close recmap */
        ret = RDB_close_recmap(tbp->var.stored.recmapp);
        if (ret != RDB_OK)
            return ret;

        /*
         * Remove table from all RDB_databases in list
         */
        dbrootp = (RDB_dbroot *)RDB_env_private(envp);
        for (dbp = dbrootp->firstdbp; dbp != NULL; dbp = dbp->nextdbp) {
            RDB_table **foundtbpp = (RDB_table **)RDB_hashmap_get(
                    &dbp->tbmap, tbp->name, NULL);
            if (foundtbpp != NULL) {
                void *nullp = NULL;
                RDB_hashmap_put(&dbp->tbmap, tbp->name, &nullp, sizeof nullp);
            }
        }

        return _RDB_drop_table(tbp, RDB_TRUE);
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

static void
free_db(RDB_database *dbp) {
    RDB_destroy_hashmap(&dbp->tbmap);
    free(dbp->name);
    free(dbp);
}

static int
release_db(RDB_database *dbp) 
{
    int ret;
    int i;
    RDB_table **tbpp;
    int tbcount;
    char **tbnames;

    /* Close all user tables */
    tbcount = RDB_hashmap_size(&dbp->tbmap);
    tbnames = malloc(sizeof(char *) * tbcount);
    RDB_hashmap_keys(&dbp->tbmap, tbnames);
    for (i = 0; i < tbcount; i++) {
        tbpp = (RDB_table **)RDB_hashmap_get(&dbp->tbmap, tbnames[i], NULL);
        if (*tbpp != NULL && (*tbpp)->is_user) {
            ret = close_table(*tbpp, dbp->dbrootp->envp);
            if (ret != RDB_OK)
                return ret;

        }
    }

    ret = rm_db(dbp);
    if (ret != RDB_OK)
        return ret;
    
    free_db(dbp);
    return ret;
}

static void
close_systables(RDB_dbroot *dbrootp)
{
    close_table(dbrootp->rtables_tbp, dbrootp->envp);
    close_table(dbrootp->table_attr_tbp, dbrootp->envp);
    close_table(dbrootp->table_attr_defvals_tbp, dbrootp->envp);
    close_table(dbrootp->vtables_tbp, dbrootp->envp);
    close_table(dbrootp->dbtables_tbp, dbrootp->envp);
    close_table(dbrootp->keys_tbp, dbrootp->envp);
    close_table(dbrootp->types_tbp, dbrootp->envp);
    close_table(dbrootp->possreps_tbp, dbrootp->envp);
    close_table(dbrootp->possrepcomps_tbp, dbrootp->envp);
    close_table(dbrootp->ro_ops_tbp, dbrootp->envp);
    close_table(dbrootp->ro_op_rtypes_tbp, dbrootp->envp);
    close_table(dbrootp->upd_ops_tbp, dbrootp->envp);
    close_table(dbrootp->indexes_tbp, dbrootp->envp);
}

/* cleanup function to close all DBs and tables */
static void
cleanup_env(RDB_environment *envp)
{
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_private(envp);
    RDB_database *dbp;
    RDB_database *nextdbp;

    if (dbrootp == NULL)
        return;

    dbp = dbrootp->firstdbp;

    while (dbp != NULL) {
        nextdbp = dbp->nextdbp;
        release_db(dbp);
        dbp = nextdbp;
    }
    close_systables(dbrootp);
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

    RDB_init_hashmap(&dbp->tbmap, RDB_DFL_MAP_CAPACITY);

    return dbp;

error:
    free(dbp->name);
    free(dbp);
    return NULL;
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

static void
assoc_systables(RDB_dbroot *dbrootp, RDB_database *dbp)
{
    _RDB_assoc_table_db(dbrootp->table_attr_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->table_attr_defvals_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->rtables_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->vtables_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->dbtables_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->keys_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->types_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->possreps_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->possrepcomps_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->ro_ops_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->ro_op_rtypes_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->upd_ops_tbp, dbp);

    _RDB_assoc_table_db(dbrootp->indexes_tbp, dbp);
}

/*
 * Create and initialize RDB_dbroot structure. Use dbp to create the transaction.
 * If newdb is true, create the database in the catalog.
 */
static int
create_dbroot(RDB_environment *envp, RDB_bool newdb,
              RDB_dbroot **dbrootpp)
{
    RDB_transaction tx;
    RDB_dbroot *dbrootp;
    int ret;

    /* Set cleanup function */
    RDB_set_env_closefn(envp, cleanup_env);

    dbrootp = new_dbroot(envp);
    if (dbrootp == NULL)
        return RDB_NO_MEMORY;

    ret = _RDB_begin_tx(&tx, envp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = _RDB_open_systables(dbrootp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
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
    RDB_transaction tx;

    if (!_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    dbp = new_db(name);
    if (dbp == NULL)
        return RDB_NO_MEMORY;

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */
        _RDB_init_builtin_types();
        ret = create_dbroot(envp, RDB_TRUE, &dbrootp);
        if (ret != RDB_OK) {
            goto error;
        }
        lt_dlinit();
    }

    /*
     * Create DB in catalog
     */

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

    assoc_systables(dbrootp, dbp);

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
    RDB_database *dbp;
    RDB_transaction tx;
    RDB_object tpl;
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_private(envp);
    RDB_bool crdbroot = RDB_FALSE;

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */

        crdbroot = RDB_TRUE;
        _RDB_init_builtin_types();
        lt_dlinit();
        ret = create_dbroot(envp, RDB_FALSE, &dbrootp);
        if (ret != RDB_OK) {
            goto error;
        }
    }

    /* search the DB list for the database */
    for (dbp = dbrootp->firstdbp; dbp != NULL; dbp = dbp->nextdbp) {
        if (strcmp(dbp->name, name) == 0) {
            *dbpp = dbp;
            return RDB_OK;
        }
    }

    /*
     * Not found, read database from catalog
     */

    ret = _RDB_begin_tx(&tx, envp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    /* Check if the database exists by checking if the DBTABLES contains
     * SYS_RTABLES for this database.
     */
    RDB_init_obj(&tpl);
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

    dbp = new_db(name);
    if (dbp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    ret = RDB_table_contains(dbrootp->dbtables_tbp, &tpl, &tx);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        RDB_rollback(&tx);
        goto error;
    }
    RDB_destroy_obj(&tpl);
    
    ret = RDB_commit(&tx);
    if (ret != RDB_OK)
        return ret;
    
    assoc_systables(dbrootp, dbp);
    dbp->dbrootp = dbrootp;
    
    /* Insert database into list */
    dbp->nextdbp = dbrootp->firstdbp;
    dbrootp->firstdbp = dbp;

    *dbpp = dbp;

    return RDB_OK;

error:
    if (crdbroot) {
        free_dbroot(dbrootp);
        RDB_env_private(envp) = NULL;
    }

    free_db(dbp);
    lt_dlexit();
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
    exprp = RDB_expr_attr("IS_USER");
    ret = RDB_select(dbp->dbrootp->rtables_tbp, exprp, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    exprp = RDB_eq(RDB_expr_attr("DBNAME"),
                   RDB_string_to_expr(dbp->name));
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

    exprp = RDB_eq(RDB_expr_attr("DBNAME"),
                  RDB_string_to_expr(dbp->name));
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

    exprp = RDB_eq(RDB_expr_attr("DBNAME"),
                  RDB_string_to_expr(dbp->name));
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

    return release_db(dbp);

error:
    RDB_rollback(&tx);
    return ret;
}

RDB_bool
strvec_is_subset(const RDB_string_vec *v1p, const RDB_string_vec *v2p)
{
    int i;

    for (i = 0; i < v1p->strc; i++) {
        if (RDB_find_str(v2p->strc, v2p->strv, v1p->strv[i]) == -1)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

int
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_string_vec keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    int i;
    RDB_transaction tx;
    RDB_string_vec allkey; /* Used if keyv is NULL */

    if (keyv == NULL) {
        /* Create key for all-key table */
        allkey.strc = attrc;
        allkey.strv = malloc(sizeof (char *) * attrc);
        if (allkey.strv == NULL) {
            if (txp != NULL)
                RDB_rollback_all(txp);
            return RDB_NO_MEMORY;
        }
        for (i = 0; i < attrc; i++)
            allkey.strv[i] = heading[i].name;
    }

    if (txp != NULL) {
        /* Create subtransaction */
        ret = RDB_begin_tx(&tx, txp->dbp, txp);
        if (ret != RDB_OK)
            return ret;
    }

    ret = _RDB_new_stored_table(name, persistent, attrc, heading,
                keyv != NULL ? keyc : 1, keyv != NULL ? keyv : &allkey,
                RDB_TRUE, tbpp);
    if (keyv == NULL)
        free(allkey.strv);
    if (ret != RDB_OK)
        return ret;

    if (persistent) {
        /* Insert table into catalog */
        ret = _RDB_cat_insert(*tbpp, &tx);
        if (ret != RDB_OK) {
            RDB_rollback(&tx);
            _RDB_free_table(*tbpp);
            if (RDB_is_syserr(ret))
                RDB_rollback_all(txp);
            return ret;
        }

        _RDB_assoc_table_db(*tbpp, txp->dbp);
    }

    ret = _RDB_create_table_storage(*tbpp, txp != NULL ? txp->envp : NULL,
            NULL, &tx);
    if (ret != RDB_OK) {
        RDB_drop_table(*tbpp, &tx);
        return ret;
    }

    return txp != NULL ? RDB_commit(&tx) : RDB_OK;
}

int
RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_string_vec keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    int i;

    if (name != NULL && !_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    for (i = 0; i < attrc; i++) {
        if (!_RDB_legal_name(heading[i].name))
            return RDB_INVALID_ARGUMENT;
    }

    if (keyv != NULL) {
        int j, k;

        /* At least one key is required */
        if (keyc < 1)
            return RDB_INVALID_ARGUMENT;

        /*
         * Check all keys
         */
        for (i = 0; i < keyc; i++) {
            /* Check if all the key attributes appear in the heading */
            for (j = 0; j < keyv[i].strc; j++) {
                for (k = 0; k < attrc
                        && strcmp(keyv[i].strv[j], heading[k].name) != 0;
                        k++);
                if (k >= attrc)
                    return RDB_INVALID_ARGUMENT;
            }

            /* Check if an attribute appears twice in a key */
            for (j = 0; j < keyv[i].strc - 1; j++) {
                /* Search attribute name in the remaining key */
                if (RDB_find_str(keyv[i].strc - j - 1, keyv[i].strv + j + 1,
                        keyv[i].strv[j]) != -1)
                    return RDB_INVALID_ARGUMENT;
            }
        }

        /* Check if a key is a subset of another */
        for (i = 0; i < keyc - 1; i++) {
            for (j = i + 1; j < keyc; j++) {
                if (keyv[i].strc <= keyv[j].strc) {
                    if (strvec_is_subset(&keyv[i], &keyv[j]))
                        return RDB_INVALID_ARGUMENT;
                } else {
                    if (strvec_is_subset(&keyv[j], &keyv[i]))
                        return RDB_INVALID_ARGUMENT;
                }
            }
        }
    }

    /* name may only be NULL if table is transient */
    if ((name == NULL) && persistent)
        return RDB_INVALID_ARGUMENT;

    return _RDB_create_table(name, persistent, attrc, heading,
                keyc, keyv, txp, tbpp);
}

int
RDB_get_table(const char *name, RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    RDB_table **foundtbpp;
    RDB_database *dbp;

    /* Search table in all databases */
    dbp = txp->dbp->dbrootp->firstdbp;
    while (dbp != NULL) {
        foundtbpp = (RDB_table **)RDB_hashmap_get(&dbp->tbmap, name, NULL);
        if (foundtbpp != NULL) {
            /* Found */
            *tbpp = *foundtbpp;
            return RDB_OK;
        }
        dbp = dbp->nextdbp;
    }

    ret = _RDB_get_cat_rtable(name, txp, tbpp);
    if (ret == RDB_OK)
        return RDB_OK;
    if (ret != RDB_NOT_FOUND)
        return ret;

    return _RDB_get_cat_vtable(name, txp, tbpp);
}

int
RDB_drop_table(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    if (tbp->is_persistent) {
        RDB_database *dbp;
        RDB_dbroot *dbrootp;

        if (!RDB_tx_is_running(txp))
            return RDB_INVALID_TRANSACTION;
    
        /*
         * Remove table from all RDB_databases in list
         */
        dbrootp = (RDB_dbroot *)RDB_env_private(txp->envp);
        for (dbp = dbrootp->firstdbp; dbp != NULL; dbp = dbp->nextdbp) {
            RDB_table **foundtbpp = (RDB_table **)RDB_hashmap_get(
                    &dbp->tbmap, tbp->name, NULL);
            if (foundtbpp != NULL) {
                void *nullp = NULL;
                RDB_hashmap_put(&dbp->tbmap, tbp->name, &nullp, sizeof nullp);
            }
        }

        /*
         * Remove table from catalog
         */
        ret = _RDB_cat_delete(tbp, txp);
        if (ret != RDB_OK)
            return ret;
    }

    /*
     * Delete recmap
     */
    if (tbp->kind == RDB_TB_STORED && tbp->var.stored.recmapp != NULL) {
        int i;
        int ret;

        /* Schedule secondary indexes for deletion */
        for (i = 0; i < tbp->var.stored.indexc; i++) {
            if (tbp->var.stored.indexv[i].idxp != NULL)
                _RDB_del_index(txp, tbp->var.stored.indexv[i].idxp);
        }

        if (txp != NULL) {
            /* Schedule recmap for deletion */
            ret = _RDB_del_recmap(txp, tbp->var.stored.recmapp);
        } else {
            ret = RDB_delete_recmap(tbp->var.stored.recmapp, NULL, NULL);
        }
        if (ret != RDB_OK)
            return ret;
    }

    return _RDB_drop_table(tbp, RDB_TRUE);
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
    if (tbp->name == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    return RDB_OK;
}

int
RDB_add_table(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;

    if (tbp->name == NULL)
        return RDB_INVALID_ARGUMENT;

    /* Turning a local real table into a persistent table is not supported */
    if (!tbp->is_persistent && tbp->kind == RDB_TB_STORED)
        return RDB_NOT_SUPPORTED;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_assoc_table_db(tbp, txp->dbp);
    if (ret != RDB_OK)
        return ret;

    tbp->is_persistent = RDB_TRUE;

    return _RDB_cat_insert(tbp, txp);
}

int
RDB_create_ro_op(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const void *iargp, size_t iarglen, 
                 RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object rtypobj;
    RDB_object typesobj;
    int ret;
    int i;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    for (i = 0; i < argc; i++) {
        if (!RDB_type_is_scalar(argtv[i]))
            return RDB_NOT_SUPPORTED;
    }

    if (!RDB_type_is_scalar(rtyp))
        return RDB_NOT_SUPPORTED;

    RDB_init_obj(&tpl);
    RDB_init_obj(&rtypobj);

    ret = RDB_tuple_set_string(&tpl, "NAME", name);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_tuple_set_string(&tpl, "LIB", libname);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set_string(&tpl, "SYMBOL", symname);
    if (ret != RDB_OK)
        goto cleanup;

    if (iargp == NULL)
        iarglen = 0;

    RDB_init_obj(&iarg);
    ret = RDB_binary_set(&iarg, 0, iargp, iarglen);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&iarg);
        goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "IARG", &iarg);
    RDB_destroy_obj(&iarg);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized arg types */
    RDB_init_obj(&typesobj);
    ret = _RDB_make_typesobj(argc, argtv, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj);
        goto cleanup;
    }

    ret = RDB_tuple_set(&tpl, "ARGTYPES", &typesobj);
    RDB_destroy_obj(&typesobj);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->ro_ops_tbp, &tpl, txp);
    if (ret != RDB_OK)
        goto cleanup;

    ret = _RDB_type_to_obj(&rtypobj, rtyp);
    if (ret != RDB_OK)
        goto cleanup;
    ret = RDB_tuple_set(&tpl, "RTYPE", &rtypobj);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->ro_op_rtypes_tbp, &tpl, txp);
    /* Operator may be overloaded */
    if (ret == RDB_ELEMENT_EXISTS)
        ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);
    RDB_destroy_obj(&rtypobj);
    return ret;
}

int
RDB_create_update_op(const char *name, int argc, RDB_type *argtv[],
                  RDB_bool updv[], const char *libname, const char *symname,
                  const void *iargp, size_t iarglen,
                  RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object iarg;
    RDB_object updvobj;
    RDB_object updobj;
    RDB_object typesobj;
    int i;
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    for (i = 0; i < argc; i++) {
        if (!RDB_type_is_scalar(argtv[i]))
            return RDB_NOT_SUPPORTED;
    }

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "NAME", name);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "LIB", libname);
    if (ret != RDB_OK)
        goto cleanup;
    RDB_tuple_set_string(&tpl, "SYMBOL", symname);
    if (ret != RDB_OK)
        goto cleanup;

    if (iargp == NULL)
        iarglen = 0;

    RDB_init_obj(&iarg);
    ret = RDB_binary_set(&iarg, 0, iargp, iarglen);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&iarg);
        goto cleanup;
    }
    ret = RDB_tuple_set(&tpl, "IARG", &iarg);
    RDB_destroy_obj(&iarg);
    if (ret != RDB_OK)
        goto cleanup;

    RDB_init_obj(&updvobj);
    RDB_init_obj(&updobj);
    RDB_set_array_length(&updvobj, (RDB_int) argc);
    for (i = 0; i < argc; i++) {
        RDB_bool_to_obj(&updobj, updv[i]);
        ret = RDB_array_set(&updvobj, (RDB_int) i, &updobj);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&updvobj);
            RDB_destroy_obj(&updobj);
            goto cleanup;
        }
    }
    ret = RDB_tuple_set(&tpl, "UPDV", &updvobj);
    RDB_destroy_obj(&updvobj);
    RDB_destroy_obj(&updobj);
    if (ret != RDB_OK)
        goto cleanup;

    /* Set ARGTYPES to array of serialized arg types */
    RDB_init_obj(&typesobj);
    ret = _RDB_make_typesobj(argc, argtv, &typesobj);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&typesobj);
        goto cleanup;
    }
    
    ret = RDB_tuple_set(&tpl, "ARGTYPES", &typesobj);
    RDB_destroy_obj(&typesobj);
    if (ret != RDB_OK)
        goto cleanup;

    ret = RDB_insert(txp->dbp->dbrootp->upd_ops_tbp, &tpl, txp);

cleanup:
    RDB_destroy_obj(&tpl);
    return ret;
}

int
RDB_get_dbs(RDB_environment *envp, RDB_object *arrp)
{
    static char *attrname[] = { "DBNAME" };
    int ret;
    int i;
    RDB_int len;
    RDB_table *vtbp;
    RDB_object resarr;
    RDB_transaction tx;
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_private(envp);

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */
        _RDB_init_builtin_types();
        ret = create_dbroot(envp, RDB_TRUE, &dbrootp);
        if (ret != RDB_OK) {
            return ret;
        }
        lt_dlinit();
    }

    ret = _RDB_begin_tx(&tx, envp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_project(dbrootp->dbtables_tbp, 1, attrname, &vtbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_init_obj(&resarr);

    ret = RDB_table_to_array(&resarr, vtbp, 0, NULL, &tx);
    if (ret != RDB_OK)
        goto cleanup;

    len = RDB_array_length(&resarr);
    if (len < 0) {
        ret = len;
        goto cleanup;
    }

    ret = RDB_set_array_length(arrp, len);
    if (ret != RDB_OK)
        goto cleanup;

    for (i = 0; i < len; i++) {
        RDB_object *tplp;

        ret = RDB_array_get(&resarr, (RDB_int) i, &tplp);
        if (ret != RDB_OK)
            goto cleanup;

        ret = RDB_array_set(arrp, (RDB_int) i, RDB_tuple_get(tplp, "DBNAME"));
        if (ret != RDB_OK)
            goto cleanup;
    }

cleanup:
    RDB_drop_table(vtbp, &tx);
    RDB_destroy_obj(&resarr);

    if (ret == RDB_OK)
        return RDB_commit(&tx);

    RDB_rollback(&tx);
    return ret;
}
