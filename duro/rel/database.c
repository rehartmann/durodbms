/*
 * Copyright (C) 2003, 2004 René Hartmann.
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

/* name of the file in which the tables are physically stored */
#define RDB_DATAFILE "rdata"

RDB_environment *
RDB_db_env(RDB_database *dbp) {
    return dbp->dbrootp->envp;
}

/*
 * Return the length (in bytes) of the internal representation
 * of the type pointed to by typ.
 */
static int replen(const RDB_type *typ) {
    switch(typ->kind) {
        case RDB_TP_TUPLE:
        {
            int i;
            size_t len;
            size_t tlen = 0;

            /*
             * Add lengths of attribute types. If one of the attributes is
             * of variable length, the tuple type is of variable length.
             */
            for (i = 0; i < typ->var.tuple.attrc; i++) {
                len = replen(typ->var.tuple.attrv[i].typ);
                if (len == RDB_VARIABLE_LEN)
                    return RDB_VARIABLE_LEN;
                tlen += len;
            }
            return tlen;
        }
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
        case RDB_TP_SCALAR:
            return typ->ireplen;
    }
    abort();
}

int
_RDB_open_table_index(RDB_table *tbp, _RDB_tbindex *indexp,
        RDB_environment *envp, RDB_transaction *txp)
{
    int ret;
    int i;
    int *fieldv = malloc(sizeof(int *) * indexp->attrc);

    if (fieldv == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    /* get index numbers */
    for (i = 0; i < indexp->attrc; i++) {
        fieldv[i] = *(int *) RDB_hashmap_get(&tbp->var.stored.attrmap,
                        indexp->attrv[i].attrname, NULL);
    }

    /* open index */
    ret = RDB_open_index(tbp->var.stored.recmapp,
                  tbp->is_persistent ? indexp->name : NULL,
                  tbp->is_persistent ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, indexp->unique,
                  txp != NULL ? txp->txid : NULL, &indexp->idxp);

cleanup:
    free(fieldv);
    return ret;
}

static int
create_index(RDB_table *tbp, RDB_environment *envp, RDB_transaction *txp,
             _RDB_tbindex *indexp)
{
    int ret;
    int i;
    int *fieldv = malloc(sizeof(int *) * indexp->attrc);

    if (fieldv == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    /* Get index numbers */
    for (i = 0; i < indexp->attrc; i++) {
        void *np = RDB_hashmap_get(&tbp->var.stored.attrmap,
                indexp->attrv[i].attrname, NULL);
        if (np == NULL)
            return RDB_INVALID_ARGUMENT;
        fieldv[i] = *(int *) np;
    }

    /* Create record-layer index */
    ret = RDB_create_index(tbp->var.stored.recmapp,
                  tbp->is_persistent ? indexp->name : NULL,
                  tbp->is_persistent ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, indexp->unique,
                  txp != NULL ? txp->txid : NULL, &indexp->idxp);

cleanup:
    free(fieldv);
    return ret;
}

int
RDB_create_table_index(const char *name, RDB_table *tbp, int idxcompc,
        RDB_seq_item idxcompv[], int flags, RDB_transaction *txp)
{
    int i;
    int ret;
    _RDB_tbindex *indexp;

    if (!_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    tbp->var.stored.indexv = realloc(tbp->var.stored.indexv,
            (tbp->var.stored.indexc + 1) * sizeof (_RDB_tbindex));
    if (tbp->var.stored.indexv == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    indexp = &tbp->var.stored.indexv[tbp->var.stored.indexc++];

    indexp->name = RDB_dup_str(name);
    if (indexp->name == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    indexp->unique = RDB_FALSE;

    for (i = 0; i < idxcompc; i++) {
        indexp->attrv[i].asc = idxcompv[i].asc;
        indexp->attrv[i].attrname = RDB_dup_str(idxcompv[i].attrname);
        if (indexp->attrv[i].attrname == NULL) {
            RDB_rollback_all(txp);
            return RDB_NO_MEMORY;
        }
    }

    /* Create index in catalog */
    ret = _RDB_cat_insert_index(indexp, tbp->name, txp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }

    /* Create index */
    ret = create_index(tbp, RDB_db_env(RDB_tx_db(txp)), txp,
            &tbp->var.stored.indexv[i]);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }

    return RDB_OK;
}

int
RDB_drop_table_index(const char *name, RDB_table *tbp, RDB_transaction *txp)
{
    return RDB_NOT_SUPPORTED;
}

static int
compare_field(const void *data1p, size_t len1,
              const void *data2p, size_t len2, void *arg)
{
    RDB_object val1, val2;
    int res;
    RDB_type *typ = (RDB_type *)arg;

    RDB_init_obj(&val1);
    RDB_init_obj(&val2);

    RDB_irep_to_obj(&val1, typ, data1p, len1);
    RDB_irep_to_obj(&val2, typ, data2p, len2);

    res = (*typ->comparep)(&val1, &val2);

    RDB_destroy_obj(&val1);
    RDB_destroy_obj(&val2);

    return res;
}

/*
 * Create secondary indexes
 */
static int
create_key_indexes(RDB_table *tbp, RDB_environment *envp, RDB_transaction *txp)
{
    int i;
    int j;
    int ret;
    tbp->var.stored.indexc = tbp->keyc;
    tbp->var.stored.indexv = malloc(sizeof (_RDB_tbindex)
            * tbp->var.stored.indexc);
    if (tbp->var.stored.indexv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < tbp->var.stored.indexc; i++) {
        tbp->var.stored.indexv[i].attrc = tbp->keyv[i].strc;
        tbp->var.stored.indexv[i].attrv = malloc (sizeof(RDB_seq_item)
                * tbp->keyv[i].strc);
        if (tbp->var.stored.indexv[i].attrv == NULL)
            return RDB_NO_MEMORY;
        for (j = 0; j < tbp->keyv[i].strc; j++) {
            tbp->var.stored.indexv[i].attrv[j].asc = RDB_TRUE;
            tbp->var.stored.indexv[i].attrv[j].attrname =
                    RDB_dup_str(tbp->keyv[i].strv[j]);
            if (tbp->var.stored.indexv[i].attrv[j].attrname == NULL)
                return RDB_NO_MEMORY;
        }

        /* A primary index has no name */
        if (tbp->is_persistent) {
            tbp->var.stored.indexv[i].name =
                    malloc(strlen(RDB_table_name(tbp)) + 4);
            if (tbp->var.stored.indexv[i].name == NULL) {
                return RDB_NO_MEMORY;
            }
            /* build index name */            
            sprintf(tbp->var.stored.indexv[i].name, "%s$%d", tbp->name, i);
        } else {
            tbp->var.stored.indexv[i].name = NULL;
        }

        tbp->var.stored.indexv[i].unique = RDB_TRUE;

        if (i > 0) {
            /* Create index, if it's not the primary index */
            ret = create_index(tbp, envp, txp, &tbp->var.stored.indexv[i]);
            if (ret != RDB_OK)
                return ret;
        } else {
            tbp->var.stored.indexv[i].idxp = NULL;
        }
    }
    return RDB_OK;
}

/*
 * Open/create the physical representation of a table.
 * (The recmap and the indexes)
 *
 * Arguments:
 * tbp        the table
 * pilen      # of primary index attributes
 * piattrc    primary index attributes
 * create     if RDB_TRUE, create a new table, if RDB_FALSE, open an
 *            existing table
 * ascv       the sort order of the primary index, or NULL if unordered
 * txp        the transaction under which the operation is performed
 */
int
_RDB_open_table(RDB_table *tbp,
           int piattrc, char *piattrv[], RDB_bool create,
           RDB_transaction *txp, RDB_environment *envp, RDB_bool ascv[])
{
    int ret, i, di;
    int *flenv = NULL;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    RDB_attr *heading = tbp->typ->var.basetyp->var.tuple.attrv;

    if (!tbp->is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    flenv = malloc(sizeof(int) * attrc);
    if (flenv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    /* Allocate comparison vector, if needed */
    if (ascv != NULL) {
        cmpv = malloc(sizeof (RDB_compare_field) * piattrc);
        if (cmpv == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }

    di = piattrc;
    for (i = 0; i < attrc; i++) {
        RDB_int fno;

        if (piattrc == attrc) {
            fno = i;
        } else {            
            /* Search attribute in key */
            fno = (RDB_int) RDB_find_str(piattrc, piattrv, heading[i].name);
        }

        /* If it's not found in the key, give it a non-key field number */
        if (fno == -1)
            fno = di++;
        else if (ascv != NULL) {
            /* Set comparison field */
            if (heading[i].typ->comparep != NULL) {
                cmpv[fno].comparep = &compare_field;
                cmpv[fno].arg = heading[i].typ;
            } else {
                cmpv[fno].comparep = NULL;
            }
            cmpv[fno].asc = ascv[fno];
        }

        /* Put the field number into the attrmap */
        ret = RDB_hashmap_put(&tbp->var.stored.attrmap,
                tbp->typ->var.basetyp->var.tuple.attrv[i].name,
                &fno, sizeof fno);
        if (ret != RDB_OK)
            goto error;

        flenv[fno] = replen(heading[i].typ);
    }

    if (create) {
        if (ascv == NULL)
            ret = RDB_create_recmap(tbp->is_persistent ? tbp->name : NULL,
                    tbp->is_persistent ? RDB_DATAFILE : NULL,
                    envp, attrc, flenv, piattrc, txp != NULL ? txp->txid : NULL,
                    &tbp->var.stored.recmapp);
        else {
            ret = RDB_create_sorted_recmap(tbp->is_persistent ? tbp->name : NULL,
                    tbp->is_persistent ? RDB_DATAFILE : NULL,
                    envp, attrc, flenv, piattrc, cmpv,
                    RDB_TRUE, txp != NULL ? txp->txid : NULL,
                    &tbp->var.stored.recmapp);
        }
    } else {
        if (ascv != NULL) {
            ret = RDB_INVALID_ARGUMENT;
            goto error;
        }
        ret = RDB_open_recmap(tbp->name, RDB_DATAFILE, envp,
                attrc, flenv, piattrc, txp != NULL ? txp->txid : NULL,
                &tbp->var.stored.recmapp);
    }

    if (ret != RDB_OK)
        goto error;

    /* Open/create indexes if there is more than one key */
    if (create) {
        ret = create_key_indexes(tbp, envp, txp);
        if (ret != RDB_OK) {
            goto error;
        }
    } else {
        /* Flag that the indexes have not been opened */
        tbp->var.stored.indexc = -1;
    }

    free(flenv);
    free(cmpv);
    return RDB_OK;

error:
    /* clean up */
    free(flenv);
    free(cmpv);
    if (tbp != NULL) {
        RDB_destroy_hashmap(&tbp->var.stored.attrmap);
    }
    if (RDB_is_syserr(ret) && txp != NULL)
        RDB_rollback_all(txp);
    return ret;
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
        _RDB_free_table(tbp, envp);
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

void
_RDB_free_table(RDB_table *tbp, RDB_environment *envp)
{
    int i;

    if (envp != NULL && tbp->is_persistent) {
        RDB_database *dbp;
        RDB_dbroot *dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    
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
        int j;

        RDB_drop_type(tbp->typ, NULL);
        RDB_destroy_hashmap(&tbp->var.stored.attrmap);

        if (tbp->var.stored.indexc > 0) {
            for (i = 0; i < tbp->var.stored.indexc; i++) {
                for (j = 0; j < tbp->var.stored.indexv[i].attrc; j++)
                    free(tbp->var.stored.indexv[i].attrv[j].attrname);
                free(tbp->var.stored.indexv[i].attrv);
            }
            free(tbp->var.stored.indexv);
        }
    }

    /* Delete candidate keys */
    for (i = 0; i < tbp->keyc; i++) {
        RDB_free_strvec(tbp->keyv[i].strc, tbp->keyv[i].strv);
    }
    free(tbp->keyv);

    free(tbp->name);
    free(tbp);
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

/*
 * Creates a table, if it does not already exist,
 * or opens an existing table, depending on the create
 * argument.
 * Does not insert the table into the catalog.
 * If keyv is NULL, the table will be all-key.
 */
int
_RDB_provide_table(const char *name, RDB_bool persistent,
           int attrc, RDB_attr heading[],
           int keyc, RDB_string_vec keyv[], RDB_bool usr,
           RDB_bool create, RDB_transaction *txp, RDB_environment *envp,
           RDB_table **tbpp)
{
    RDB_type *reltyp;
    int ret;
    int i;

    /* At least one key is required */
    if (keyc < 1)
        return RDB_INVALID_ARGUMENT;

    /* name may only be NULL if table is transient */
    if ((name == NULL) && persistent)
        return RDB_INVALID_ARGUMENT;

    reltyp = RDB_create_relation_type(attrc, heading);
    if (reltyp == NULL) {
        return RDB_NO_MEMORY;
    }
    for (i = 0; i < attrc; i++) {
        if (heading[i].defaultp != NULL) {
            RDB_type *tuptyp = reltyp->var.basetyp;

            tuptyp->var.tuple.attrv[i].defaultp = malloc(sizeof (RDB_object));
            if (tuptyp->var.tuple.attrv[i].defaultp == NULL)
                return RDB_NO_MEMORY;
            RDB_init_obj(tuptyp->var.tuple.attrv[i].defaultp);
            RDB_copy_obj(tuptyp->var.tuple.attrv[i].defaultp,
                    heading[i].defaultp);
        }
    }

    ret = _RDB_new_stored_table(name, persistent, reltyp, keyc, keyv, usr,
            tbpp);
    if (ret != RDB_OK) {
        RDB_drop_type(reltyp, NULL);
        return ret;
    }

    ret = _RDB_open_table(*tbpp, keyv[0].strc, keyv[0].strv, create, txp,
            envp, NULL);
    if (ret != RDB_OK) {
        _RDB_free_table(*tbpp, NULL);
        return ret;
    }
    return RDB_OK;
}

int
_RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_string_vec keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    return _RDB_provide_table(name, persistent,
           attrc, heading, keyc, keyv, RDB_TRUE, RDB_TRUE,
           txp, txp != NULL ? txp->envp : NULL, tbpp);
}

int
RDB_create_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_string_vec keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    int i;
    RDB_transaction tx;
    RDB_string_vec allkey; /* Used if keyv is NULL */

    if (txp != NULL) {
        /* Create subtransaction */
        ret = RDB_begin_tx(&tx, txp->dbp, txp);
        if (ret != RDB_OK)
            return ret;
    }

    if (name != NULL && !_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    for (i = 0; i < attrc; i++) {
        if (!_RDB_legal_name(heading[i].name))
            return RDB_INVALID_ARGUMENT;
    }

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

    ret = _RDB_create_table(name, persistent, attrc, heading,
            keyv != NULL ? keyc : 1, keyv != NULL ? keyv : &allkey,
            txp != NULL ? &tx : NULL, tbpp);
    if (keyv == NULL)
        free(allkey.strv);
    if (ret != RDB_OK) {
        if (txp != NULL)
            RDB_rollback(&tx);
        return ret;
    }

    if (persistent) {
        /* Insert table into catalog */
        ret = _RDB_cat_insert(*tbpp, &tx);
        if (ret != RDB_OK) {
            RDB_rollback(&tx);
            _RDB_drop_rtable(*tbpp, txp);
            if (RDB_is_syserr(ret))
                RDB_rollback_all(txp);
            return ret;
        }

        _RDB_assoc_table_db(*tbpp, txp->dbp);
    }

    return txp != NULL ? RDB_commit(&tx) : RDB_OK;
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

/*
 * Delete a real table, but not from the catalog
 */
int
_RDB_drop_rtable(RDB_table *tbp, RDB_transaction *txp)
{
    int i;

    /* Schedule secondary indexes for deletion */
    for (i = 0; i < tbp->var.stored.indexc; i++) {
        if (tbp->var.stored.indexv[i].idxp != NULL)
            _RDB_del_index(txp, tbp->var.stored.indexv[i].idxp);
    }

    if (txp != NULL) {
        /* Schedule recmap for deletion */
        return _RDB_del_recmap(txp, tbp->var.stored.recmapp);
    } else {
        return RDB_delete_recmap(tbp->var.stored.recmapp, NULL, NULL);
    }
}

static int
drop_anon_table(RDB_table *tbp, RDB_transaction *txp)
{
    if (tbp->name == NULL)
        return _RDB_drop_table(tbp, txp, RDB_TRUE);
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

            ret = _RDB_drop_rtable(tbp, txp);
            break;
        }
        case RDB_TB_SELECT_INDEX:
        case RDB_TB_SELECT:
            RDB_drop_expr(tbp->var.select.exp);
            if (rec) {
                ret = drop_anon_table(tbp->var.select.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_UNION:
            if (rec) {
                ret = drop_anon_table(tbp->var._union.tb1p, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var._union.tb2p, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_MINUS:
            if (rec) {
                ret = drop_anon_table(tbp->var.minus.tb1p, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.minus.tb2p, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_INTERSECT:
            if (rec) {
                ret = drop_anon_table(tbp->var.intersect.tb1p, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.intersect.tb2p, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_JOIN:
            if (rec) {
                ret = drop_anon_table(tbp->var.join.tb1p, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.join.tb2p, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_drop_type(tbp->typ, NULL);
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
            RDB_drop_type(tbp->typ, NULL);
            break;
        case RDB_TB_PROJECT:
            if (rec) {
                ret = drop_anon_table(tbp->var.project.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_drop_type(tbp->typ, NULL);
            break;
        case RDB_TB_RENAME:
            if (rec) {
                ret = drop_anon_table(tbp->var.rename.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.rename.renc; i++) {
                free(tbp->var.rename.renv[i].from);
                free(tbp->var.rename.renv[i].to);
            }
            break;
        case RDB_TB_SUMMARIZE:
            if (rec) {
                ret = drop_anon_table(tbp->var.summarize.tb1p, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.summarize.tb2p, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.summarize.addc; i++) {
                if (tbp->var.summarize.addv[i].op != RDB_COUNT
                        && tbp->var.summarize.addv[i].op != RDB_COUNTD)
                    RDB_drop_expr(tbp->var.summarize.addv[i].exp);
                free(tbp->var.summarize.addv[i].name);
            }
            break;
        case RDB_TB_WRAP:
            if (rec) {
                ret = drop_anon_table(tbp->var.wrap.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.wrap.wrapc; i++) {
                free(tbp->var.wrap.wrapv[i].attrname);
                RDB_free_strvec(tbp->var.wrap.wrapv[i].attrc,
                        tbp->var.wrap.wrapv[i].attrv);
            }
            break;
        case RDB_TB_UNWRAP:
            if (rec) {
                ret = drop_anon_table(tbp->var.unwrap.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_free_strvec(tbp->var.unwrap.attrc, tbp->var.unwrap.attrv);
            break;
        case RDB_TB_SDIVIDE:
            if (rec) {
                ret = drop_anon_table(tbp->var.sdivide.tb1p, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.sdivide.tb2p, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.sdivide.tb3p, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
    }

    if (tbp->is_persistent) {
        /* Delete table from catalog */
        ret = _RDB_cat_delete(tbp, txp);
    }

    _RDB_free_table(tbp, txp != NULL ? txp->envp : NULL);
    return ret;
}

int
RDB_drop_table(RDB_table *tbp, RDB_transaction *txp)
{
    if (tbp->is_persistent && !RDB_tx_is_running(txp))
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
