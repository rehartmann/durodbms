/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "typeimpl.h"
#include "serialize.h"
#include "catalog.h"
#include <gen/hashmapit.h>
#include <gen/hashtabit.h>
#include <gen/strfns.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

RDB_environment *
RDB_db_env(RDB_database *dbp)
{
    return dbp->dbrootp->envp;
}

/* Associate a RDB_table structure with a RDB_database structure. */
int
_RDB_assoc_table_db(RDB_table *tbp, RDB_database *dbp)
{
    /* Insert table into table map */
    return RDB_hashmap_put(&dbp->tbmap, tbp->name, tbp);
}

static void
free_dbroot(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    RDB_hashmap_iter it;
    char *keyp;
    void *datap;
    RDB_constraint *constrp, *nextconstrp;

    RDB_destroy_hashmap(&dbrootp->typemap);

    /*
     * destroy user-defined operators in memory
     */
    RDB_init_hashmap_iter(&it, &dbrootp->ro_opmap);
    while ((datap = RDB_hashmap_next(&it, &keyp)) != NULL) {
        RDB_ro_op_desc *op = datap;

        if (op != NULL)
            _RDB_free_ro_ops(op, ecp);
    }
    RDB_destroy_hashmap_iter(&it);

    /*
     * Destroy constraints
     */
    constrp = dbrootp->first_constrp;
    while (constrp != NULL) {
        nextconstrp = constrp->nextp;
        free(constrp->name);
        RDB_drop_expr(constrp->exp, ecp);
        free(constrp);
        constrp = nextconstrp;
    }

    RDB_init_hashmap_iter(&it, &dbrootp->upd_opmap);
    while ((datap = RDB_hashmap_next(&it, &keyp)) != NULL) {
        RDB_upd_op *op = datap;

        if (op != NULL)
            _RDB_free_upd_ops(op, ecp);
    }
    RDB_destroy_hashmap_iter(&it);

    RDB_destroy_hashmap(&dbrootp->ro_opmap);
    RDB_destroy_hashmap(&dbrootp->upd_opmap);

    RDB_destroy_hashtable(&dbrootp->empty_tbtab);
    free(dbrootp);
}

static int
close_table(RDB_table *tbp, RDB_environment *envp, RDB_exec_context *ecp)
{
    int ret;
    RDB_dbroot *dbrootp;
    RDB_database *dbp;

    if (tbp->stp != NULL) {
        ret = _RDB_close_stored_table(tbp->stp);
        if (ret != RDB_OK)
            return ret;
    }

    /*
     * Remove table from all RDB_databases in list
     */
    dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
        RDB_table *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->name);
        if (foundtbp != NULL) {
            RDB_hashmap_put(&dbp->tbmap, tbp->name, NULL);
        }
    }

    return _RDB_drop_table(tbp, RDB_TRUE, ecp);
}

static int
rm_db(RDB_database *dbp)
{
    /* Remove database from list */
    if (dbp->dbrootp->first_dbp == dbp) {
        dbp->dbrootp->first_dbp = dbp->nextdbp;
    } else {
        RDB_database *hdbp = dbp->dbrootp->first_dbp;
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
release_db(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    int i;
    RDB_table *tbp;
    int tbcount;
    char **tbnames;

    /* Close all user tables */
    tbcount = RDB_hashmap_size(&dbp->tbmap);
    tbnames = malloc(sizeof(char *) * tbcount);
    RDB_hashmap_keys(&dbp->tbmap, tbnames);
    for (i = 0; i < tbcount; i++) {
        tbp = RDB_hashmap_get(&dbp->tbmap, tbnames[i]);
        if (tbp != NULL && tbp->is_user) {
            ret = close_table(tbp, dbp->dbrootp->envp, ecp);
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
close_systables(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    close_table(dbrootp->rtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_attr_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_attr_defvals_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->vtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_recmap_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->dbtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->keys_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->types_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->possrepcomps_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->ro_ops_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->upd_ops_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->indexes_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->constraints_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->version_info_tbp, dbrootp->envp, ecp);
}

/* cleanup function to close all DBs and tables */
static void
cleanup_env(RDB_environment *envp)
{
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_private(envp);
    RDB_database *dbp;
    RDB_database *nextdbp;
    RDB_hashtable_iter tit;
    RDB_table *etbp;
    RDB_exec_context ec;

    if (dbrootp == NULL)
        return;

    RDB_init_exec_context(&ec);

    /* Destroy tables used in IS_EMPTY constraints */
    RDB_init_hashtable_iter(&tit, &dbrootp->empty_tbtab);
    while ((etbp = RDB_hashtable_next(&tit)) != NULL) {
        if (RDB_table_name(etbp) == NULL) {
            RDB_drop_table(etbp, &ec, NULL);
        }
    }
    RDB_destroy_hashtable_iter(&tit);

    dbp = dbrootp->first_dbp;

    while (dbp != NULL) {
        nextdbp = dbp->nextdbp;
        release_db(dbp, &ec);
        dbp = nextdbp;
    }
    close_systables(dbrootp, &ec);
    free_dbroot(dbrootp, &ec);

    RDB_destroy_exec_context(&ec);

    lt_dlexit();
}

static unsigned
hash_tb(const void *tbp, void *arg) {
    return ((RDB_table *) tbp)->kind;
}

static RDB_bool
tb_equals(const void *tb1p, const void *tb2p, void *txp) {
    int ret;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec); /* !! pass ec through struct arg */
    ret = _RDB_table_def_equals((RDB_table *) tb1p, (RDB_table *) tb2p,
            &ec, (RDB_transaction *) txp);
    RDB_destroy_exec_context(&ec);
    return ret;
}

static RDB_dbroot *
new_dbroot(RDB_environment *envp)
{
    int ret;
    RDB_dbroot *dbrootp = malloc(sizeof (RDB_dbroot));

    if (dbrootp == NULL)
        return NULL;
    
    dbrootp->envp = envp;
    RDB_init_hashmap(&dbrootp->typemap, RDB_DFL_MAP_CAPACITY);
    RDB_init_hashmap(&dbrootp->ro_opmap, RDB_DFL_MAP_CAPACITY);
    RDB_init_hashmap(&dbrootp->upd_opmap, RDB_DFL_MAP_CAPACITY);
    RDB_init_hashtable(&dbrootp->empty_tbtab, RDB_DFL_MAP_CAPACITY,
            &hash_tb, &tb_equals);

    ret = _RDB_add_builtin_ops(dbrootp);
    if (ret != RDB_OK)
        return NULL;

    dbrootp->first_dbp = NULL;
    dbrootp->first_constrp = NULL;
    dbrootp->constraints_read = RDB_FALSE;

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

    if (*name == '\0')
        return RDB_FALSE;

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
    _RDB_assoc_table_db(dbrootp->possrepcomps_tbp, dbp);
    _RDB_assoc_table_db(dbrootp->ro_ops_tbp, dbp);
    _RDB_assoc_table_db(dbrootp->upd_ops_tbp, dbp);
    _RDB_assoc_table_db(dbrootp->indexes_tbp, dbp);
    _RDB_assoc_table_db(dbrootp->constraints_tbp, dbp);
    _RDB_assoc_table_db(dbrootp->version_info_tbp, dbp);
}

/*
 * Create and initialize RDB_dbroot structure. Use dbp to create the transaction.
 * If newdb is true, create the database in the catalog.
 */
static int
create_dbroot(RDB_environment *envp, RDB_bool newdb,
              RDB_exec_context *ecp, RDB_dbroot **dbrootpp)
{
    RDB_transaction tx;
    RDB_dbroot *dbrootp;
    RDB_database sdb; /* Incomplete database, needed for built-in ops */
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

    tx.dbp = &sdb;
    sdb.dbrootp = dbrootp;
    ret = _RDB_open_systables(dbrootp, ecp, &tx);
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
    free_dbroot(dbrootp, ecp);
    return ret;
}

RDB_database *
RDB_create_db_from_env(const char *name, RDB_environment *envp,
                       RDB_exec_context *ecp)
{
    int ret;
    RDB_database *dbp;
    RDB_dbroot *dbrootp;
    RDB_transaction tx;

    if (!_RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid database name", ecp);
        return NULL;
    }

    dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    dbp = new_db(name);
    if (dbp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */
        _RDB_init_builtin_types();
        ret = create_dbroot(envp, RDB_TRUE, ecp, &dbrootp);
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

    ret = _RDB_cat_create_db(ecp, &tx);
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
    dbp->nextdbp = dbrootp->first_dbp;
    dbrootp->first_dbp = dbp;

    return dbp;

error:
    free_db(dbp);
    return NULL;
}

RDB_database *
RDB_get_db_from_env(const char *name, RDB_environment *envp,
                    RDB_exec_context *ecp)
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
        ret = create_dbroot(envp, RDB_FALSE, ecp, &dbrootp);
        if (ret != RDB_OK) {
            goto error;
        }
    }

    /* search the DB list for the database */
    for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
        if (strcmp(dbp->name, name) == 0) {
            return dbp;
        }
    }

    /*
     * Not found, read database from catalog
     */

    ret = _RDB_begin_tx(&tx, envp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    /*
     * Check if the database exists by checking if the DBTABLES contains
     * SYS_RTABLES for this database.
     */

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", "SYS_RTABLES", ecp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }
    ret = RDB_tuple_set_string(&tpl, "DBNAME", name, ecp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        goto error;
    }

    dbp = new_db(name);
    if (dbp == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    ret = RDB_table_contains(dbrootp->dbtables_tbp, &tpl, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        RDB_rollback(&tx);
        goto error;
    }
    RDB_destroy_obj(&tpl, ecp);

    ret = RDB_commit(&tx);
    if (ret != RDB_OK)
        return NULL;
    
    assoc_systables(dbrootp, dbp);
    dbp->dbrootp = dbrootp;
    
    /* Insert database into list */
    dbp->nextdbp = dbrootp->first_dbp;
    dbrootp->first_dbp = dbp;

    return dbp;

error:
    if (crdbroot) {
        free_dbroot(dbrootp, ecp);
        RDB_env_private(envp) = NULL;
    }

    free_db(dbp);
    lt_dlexit();
    return NULL;
}

int
RDB_drop_db(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_expression *exprp;
    RDB_table *vtbp, *vtb2p;
    RDB_bool empty;

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK)
        return ret;

    /*
     * Check if the database contains user tables
     */
    exprp = RDB_expr_attr("IS_USER");
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    vtbp = RDB_select(dbp->dbrootp->rtables_tbp, exprp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_drop_expr(exprp, ecp);
        goto error;
    }
    exprp = RDB_eq(RDB_expr_attr("DBNAME"),
                   RDB_string_to_expr(dbp->name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    vtb2p = RDB_select(dbp->dbrootp->dbtables_tbp, exprp, ecp, &tx);
    if (vtb2p == NULL) {
        RDB_drop_expr(exprp, ecp);
        RDB_drop_table(vtbp, ecp, &tx);
        goto error;
    }
    
    vtbp = RDB_join(vtbp, vtb2p, ecp);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, ecp, &tx);
        RDB_drop_table(vtb2p, ecp, &tx);
        /* !! ret */
        goto error;
    }
    
    ret = RDB_table_is_empty(vtbp, ecp, &tx, &empty);
    RDB_drop_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    if (!empty) {
        ret = RDB_ELEMENT_EXISTS;
        goto error;
    }

    /*
     * Check if the database exists
     */

    exprp = RDB_eq(RDB_expr_attr("DBNAME"),
                  RDB_string_to_expr(dbp->name));
    if (exprp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    vtbp = RDB_select(dbp->dbrootp->dbtables_tbp, exprp, ecp, &tx);
    if (vtbp == NULL) {
        goto error;
    }

    ret = RDB_table_is_empty(vtbp, ecp, &tx, &empty);
    RDB_drop_table(vtbp, ecp, &tx);
    if (ret != RDB_OK)
        goto error;
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

    ret = RDB_delete(dbp->dbrootp->dbtables_tbp, exprp, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_commit(&tx);
    if (ret != RDB_OK)
        return ret;

    return release_db(dbp, ecp);

error:
    RDB_rollback(&tx);
    return RDB_ERROR;
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

RDB_table *
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, const RDB_attr attrv[],
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_table *tbp;
    int ret;
    int i;
    RDB_transaction tx;
    RDB_string_vec allkey; /* Used if keyv is NULL */

    if (keyv == NULL) {
        /* Create key for all-key table */
        allkey.strc = attrc;
        allkey.strv = malloc(sizeof (char *) * attrc);
        if (allkey.strv == NULL) {
            RDB_raise_no_memory(ecp);
            return NULL;
        }
        for (i = 0; i < attrc; i++)
            allkey.strv[i] = attrv[i].name;
    }

    if (txp != NULL) {
        /* Create subtransaction */
        ret = RDB_begin_tx(&tx, txp->dbp, txp);
        if (ret != RDB_OK)
            return NULL;
    }

    tbp = _RDB_new_rtable(name, persistent, attrc, attrv,
                keyv != NULL ? keyc : 1, keyv != NULL ? keyv : &allkey,
                RDB_TRUE, ecp);
    if (keyv == NULL)
        free(allkey.strv);
    if (tbp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    if (persistent) {
        /* Insert table into catalog */
        ret = _RDB_cat_insert(tbp, ecp, &tx);
        if (ret != RDB_OK) {
            RDB_rollback(&tx);
            _RDB_free_table(tbp, ecp);
            _RDB_handle_syserr(txp, ret);
            return NULL;
        }

        _RDB_assoc_table_db(tbp, txp->dbp);
    }

    if (txp != NULL) {
         RDB_commit(&tx);
    }
    return tbp;
}

RDB_table *
RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, const RDB_attr attrv[],
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    if (name != NULL && !_RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid table name", ecp);
        return NULL;
    }

    for (i = 0; i < attrc; i++) {
        if (!_RDB_legal_name(attrv[i].name)) {
            RDB_raise_invalid_argument("invalid attribute name", ecp);
            return NULL;
        }
    }

    if (keyv != NULL) {
        int j, k;

        /* At least one key is required */
        if (keyc < 1) {
            RDB_raise_invalid_argument("key is required", ecp);
            return NULL;
        }

        /*
         * Check all keys
         */
        for (i = 0; i < keyc; i++) {
            /* Check if all the key attributes appear in the attrv */
            for (j = 0; j < keyv[i].strc; j++) {
                for (k = 0; k < attrc
                        && strcmp(keyv[i].strv[j], attrv[k].name) != 0;
                        k++);
                if (k >= attrc) {
                    RDB_raise_invalid_argument("invalid key", ecp);
                    return NULL;
                }
            }

            /* Check if an attribute appears twice in a key */
            for (j = 0; j < keyv[i].strc - 1; j++) {
                /* Search attribute name in the remaining key */
                if (RDB_find_str(keyv[i].strc - j - 1, keyv[i].strv + j + 1,
                        keyv[i].strv[j]) != -1) {
                    RDB_raise_invalid_argument("invalid key", ecp);
                    return NULL;
                }
            }
        }

        /* Check if a key is a subset of another */
        for (i = 0; i < keyc - 1; i++) {
            for (j = i + 1; j < keyc; j++) {
                if (keyv[i].strc <= keyv[j].strc) {
                    if (strvec_is_subset(&keyv[i], &keyv[j])) {
                        RDB_raise_invalid_argument("invalid key", ecp);
                        return NULL;
                    }
                } else {
                    if (strvec_is_subset(&keyv[j], &keyv[i])) {
                            RDB_raise_invalid_argument("invalid key", ecp);
                            return NULL;
                        }
                }
            }
        }
    }

    /* name may only be NULL if table is transient */
    if ((name == NULL) && persistent) {
        RDB_raise_invalid_argument("persistent table must have a name", ecp);
        return NULL;
    }

    return _RDB_create_table(name, persistent, attrc, attrv,
                keyc, keyv, ecp, txp);
}

RDB_table *
RDB_create_table_from_type(const char *name, RDB_bool persistent,
                RDB_type *reltyp,
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    return RDB_create_table(name, persistent,
            reltyp->var.basetyp->var.tuple.attrc,
            reltyp->var.basetyp->var.tuple.attrv, keyc, keyv, ecp, txp);
}

RDB_table *
RDB_get_table(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_table *tbp;
    RDB_database *dbp;

    /* Search table in all databases */
    dbp = txp->dbp->dbrootp->first_dbp;
    while (dbp != NULL) {
        tbp = RDB_hashmap_get(&dbp->tbmap, name);
        if (tbp != NULL) {
            /* Found */
            return tbp;
        }
        dbp = dbp->nextdbp;
    }

    tbp = _RDB_cat_get_rtable(name, ecp, txp);
    if (tbp != NULL)
        return tbp;
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        return NULL;

    return _RDB_cat_get_vtable(name, ecp, txp);
}

int
RDB_drop_table(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
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
        for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
            RDB_table *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->name);
            if (foundtbp != NULL) {
                RDB_hashmap_put(&dbp->tbmap, tbp->name, NULL);
            }
        }

        /*
         * Remove table from catalog
         */
        ret = _RDB_cat_delete(tbp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
    }

    /*
     * Delete recmap
     */
    if (tbp->stp != NULL) {
        ret = _RDB_delete_stored_table(tbp->stp, txp);
        if (ret != RDB_OK)
            return ret;
    }

    return _RDB_drop_table(tbp, RDB_TRUE, ecp);
}

int
RDB_set_table_name(RDB_table *tbp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (!_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    /* !! should check if virtual tables depend on this table */

    if (tbp->is_persistent) {
        RDB_database *dbp;

        /* Delete and reinsert tables from/to table maps */
        for (dbp = txp->dbp->dbrootp->first_dbp; dbp != NULL;
                dbp = dbp->nextdbp) {
            RDB_table *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->name);
            if (foundtbp != NULL) {
                RDB_hashmap_put(&dbp->tbmap, tbp->name, NULL);
                RDB_hashmap_put(&dbp->tbmap, name, tbp);
            }
        }

        /* Update catalog */
        ret = _RDB_cat_rename_table(tbp, name, ecp, txp);
        if (ret != RDB_OK) {
            RDB_errmsg(RDB_db_env(RDB_tx_db(txp)),
                    "cannot rename table in catalog: %s", RDB_strerror(ret));
            if (!RDB_is_syserr(ret)) {
                /* Should not happen */
                ret = RDB_INTERNAL;
            }
            _RDB_handle_syserr(txp, ret);
            return ret;
        }
    }
    
    if (tbp->name != NULL)
        free(tbp->name);
    tbp->name = RDB_dup_str(name);
    if (tbp->name == NULL) {
        return RDB_NO_MEMORY;
    }

    return RDB_OK;
}

int
RDB_add_table(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (tbp->name == NULL)
        return RDB_INVALID_ARGUMENT;

    /* Turning a local real table into a persistent table is not supported */
    if (!tbp->is_persistent && tbp->kind == RDB_TB_REAL)
        return RDB_NOT_SUPPORTED;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_assoc_table_db(tbp, txp->dbp);
    if (ret != RDB_OK)
        return ret;

    tbp->is_persistent = RDB_TRUE;

    return _RDB_cat_insert(tbp, ecp, txp);
}

int
RDB_get_dbs(RDB_environment *envp, RDB_object *arrp, RDB_exec_context *ecp)
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
        ret = create_dbroot(envp, RDB_TRUE, ecp, &dbrootp);
        if (ret != RDB_OK) {
            return ret;
        }
        lt_dlinit();
    }

    ret = _RDB_begin_tx(&tx, envp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    vtbp = RDB_project(dbrootp->dbtables_tbp, 1, attrname, ecp);
    if (vtbp == NULL) {
        RDB_rollback(&tx);
        return RDB_NO_MEMORY /* !! */;
    }

    RDB_init_obj(&resarr);

    ret = RDB_table_to_array(&resarr, vtbp, 0, NULL, ecp, &tx);
    if (ret != RDB_OK)
        goto cleanup;

    len = RDB_array_length(&resarr, ecp);
    if (len < 0) {
        ret = len;
        goto cleanup;
    }

    ret = RDB_set_array_length(arrp, len, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    for (i = 0; i < len; i++) {
        RDB_object *tplp;

        tplp = RDB_array_get(&resarr, (RDB_int) i, ecp);
        if (tplp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }

        ret = RDB_array_set(arrp, (RDB_int) i, RDB_tuple_get(tplp, "DBNAME"),
                ecp);
        if (ret != RDB_OK)
            goto cleanup;
    }

cleanup:
    RDB_drop_table(vtbp, ecp, &tx);
    RDB_destroy_obj(&resarr, ecp);

    if (ret == RDB_OK)
        return RDB_commit(&tx);

    RDB_rollback(&tx);
    return ret;
}
