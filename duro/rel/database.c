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

RDB_environment *
RDB_db_env(RDB_database *dbp) {
    return dbp->dbrootp->envp;
}

#define RDB_db_env(dbp) ((dbp)->dbrootp->envp)

/*
 * Return the length (in bytes) of the internal representation
 * of the type pointed to by typ.
 */
static int replen(const RDB_type *typ) {
    if (RDB_type_is_scalar(typ))
        return typ->ireplen;

    if (typ->kind == RDB_TP_TUPLE) {
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
    abort();
}

static int
open_key_index(RDB_table *tbp, int keyno, const RDB_string_vec *keyattrsp,
                 RDB_bool create, RDB_transaction *txp, RDB_index **idxp)
{
    int ret;
    int i;
    char *idx_name = NULL;
    int *fieldv = malloc(sizeof(int *) * keyattrsp->strc);

    if (fieldv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    if (tbp->is_persistent) {
        idx_name = malloc(strlen(RDB_table_name(tbp)) + 4);
        if (idx_name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }

        /* build index name */            
        sprintf(idx_name, "%s$%d", tbp->name, keyno);
    }

    /* get index numbers */
    for (i = 0; i < keyattrsp->strc; i++) {
        fieldv[i] = *(int *) RDB_hashmap_get(&tbp->var.stored.attrmap,
                        keyattrsp->strv[i], NULL);
    }

    if (create) {
        /* create index */
        ret = RDB_create_index(tbp->var.stored.recmapp,
                  tbp->is_persistent ? idx_name : NULL,
                  tbp->is_persistent ? RDB_DATAFILE : NULL,
                  txp->dbp->dbrootp->envp, keyattrsp->strc, fieldv, RDB_TRUE,
                  txp->txid, idxp);
    } else {
        /* open index */
        ret = RDB_open_index(tbp->var.stored.recmapp, idx_name, RDB_DATAFILE,
                  txp->dbp->dbrootp->envp, keyattrsp->strc, fieldv, RDB_TRUE,
                  txp->txid, idxp);
    }

    if (ret != RDB_OK)
        goto error;

    ret = RDB_OK;
error:
    free(idx_name);
    free(fieldv);
    return ret;
}

/*
 * Creates a stored table, but not the recmap and the indexes
 * and does not insert the table into the catalog.
 * reltyp is consumed on success (must not be freed by caller).
 */
int
_RDB_new_stored_table(const char *name, RDB_bool persistent,
                RDB_type *reltyp,
                int keyc, RDB_string_vec keyv[], RDB_bool usr,
                RDB_table **tbpp)
{
    RDB_table *tbp = NULL;
    int ret, i;

    for (i = 0; i < keyc; i++) {
        int j;

        /* check if all the key attributes appear in the heading */
        for (j = 0; j < keyv[i].strc; j++) {
            if (_RDB_tuple_type_attr(reltyp->var.basetyp, keyv[i].strv[j])
                    == NULL) {
                ret = RDB_INVALID_ARGUMENT;
                goto error;
            }
        }
    }

    tbp = *tbpp = malloc(sizeof (RDB_table));
    if (tbp == NULL) {
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
            goto error;
        }
    } else {
        tbp->name = NULL;
    }

    /* copy candidate keys */
    tbp->keyc = keyc;
    tbp->keyv = malloc(sizeof(RDB_attr) * keyc);
    for (i = 0; i < keyc; i++) {
        tbp->keyv[i].strv = NULL;
    }
    for (i = 0; i < keyc; i++) {
        tbp->keyv[i].strc = keyv[i].strc;
        tbp->keyv[i].strv = RDB_dup_strvec(keyv[i].strc, keyv[i].strv);
        if (tbp->keyv[i].strv == NULL)
            goto error;
    }

    tbp->typ = reltyp;

    return RDB_OK;

error:
    /* clean up */
    if (tbp != NULL) {
        free(tbp->name);
        for (i = 0; i < tbp->keyc; i++) {
            if (tbp->keyv[i].strv != NULL) {
                RDB_free_strvec(tbp->keyv[i].strc, tbp->keyv[i].strv);
            }
        }
        free(tbp->keyv);
        RDB_destroy_hashmap(&tbp->var.stored.attrmap);
        free(tbp);
    }
    return ret;
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
           RDB_transaction *txp, RDB_bool ascv[])
{
    int ret, i, di;
    int *flenv = NULL;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    RDB_attr *heading = tbp->typ->var.basetyp->var.tuple.attrv;

    if (!RDB_tx_is_running(txp))
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
        /* Search attribute in key */
        RDB_int fno = (RDB_int) RDB_find_str(piattrc, piattrv, heading[i].name);

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

        /* Relation types are supported by this version */
        if (heading[i].typ->kind == RDB_TP_RELATION) {
            ret = RDB_NOT_SUPPORTED;
            goto error;
        }

        flenv[fno] = replen(heading[i].typ);
    }
    if (create) {
        if (ascv == NULL)
            ret = RDB_create_recmap(tbp->is_persistent ? tbp->name : NULL,
                    tbp->is_persistent ? RDB_DATAFILE : NULL,
                    txp->dbp->dbrootp->envp,
                    attrc, flenv, piattrc, txp->txid,
                    &tbp->var.stored.recmapp);
        else {
            ret = RDB_create_sorted_recmap(tbp->is_persistent ? tbp->name : NULL,
                    tbp->is_persistent ? RDB_DATAFILE : NULL,
                    txp->dbp->dbrootp->envp, attrc, flenv,
                    piattrc, cmpv, RDB_TRUE, txp->txid,
                    &tbp->var.stored.recmapp);
        }
    } else {
        if (ascv != NULL) {
            ret = RDB_INVALID_ARGUMENT;
            goto error;
        }
        ret = RDB_open_recmap(tbp->name, RDB_DATAFILE, txp->dbp->dbrootp->envp,
                attrc, flenv, piattrc, txp->txid,
                &tbp->var.stored.recmapp);
    }

    if (ret != RDB_OK)
        goto error;

    /* Open/create indexes if there is more than one key */
    if (tbp->keyc > 1) {
        tbp->var.stored.keyidxv = malloc(sizeof(RDB_index *) * (tbp->keyc - 1));
        if (tbp->var.stored.keyidxv == NULL) {
            ret = RDB_NO_MEMORY;
            RDB_errmsg(txp->dbp->dbrootp->envp, RDB_strerror(ret));
            goto error;
        }
        for (i = 1; i < tbp->keyc; i++) {    
            ret = open_key_index(tbp, i, &tbp->keyv[i], create, txp,
                          &tbp->var.stored.keyidxv[i - 1]);
            if (ret != RDB_OK)
                goto error;
        }
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
free_ro_op(RDB_ro_op *op) {
    int i;

    free(op->name);
    for (i = 0; i < op->argc; i++) {
        if (RDB_type_name(op->argtv[i]) == NULL)
            RDB_drop_type(op->argtv[i], NULL);
    }
    free(op->argtv);
    if (RDB_type_name(op->rtyp) == NULL)
        RDB_drop_type(op->rtyp, NULL);
    lt_dlclose(op->modhdl);
    RDB_destroy_obj(&op->iarg);
    free(op);
}

static void
free_ro_ops(RDB_hashmap *hp, const char *key, void *argp)
{
    RDB_ro_op **opp = RDB_hashmap_get(hp, key, NULL);
    RDB_ro_op *op;

    if (opp == NULL || *opp == NULL)
        return;
    
    op = *opp;

    do {
        RDB_ro_op *nextop = op->nextp;
        free_ro_op(op);
        op = nextop;
    } while (op != NULL);
}

static void
free_upd_op(RDB_upd_op *op) {
    int i;

    free(op->name);
    for (i = 0; i < op->argc; i++) {
        if (RDB_type_name(op->argtv[i]) == NULL)
            RDB_drop_type(op->argtv[i], NULL);
    }
    free(op->argtv);
    lt_dlclose(op->modhdl);
    RDB_destroy_obj(&op->iarg);
    free(op);
}

static void
free_upd_ops(RDB_hashmap *hp, const char *key, void *argp)
{
    RDB_upd_op **opp = RDB_hashmap_get(hp, key, NULL);
    RDB_upd_op *op;

    if (opp == NULL || *opp == NULL)
        return;
    
    op = *opp;

    do {
        RDB_upd_op *nextop = op->nextp;
        free_upd_op(op);
        op = nextop;
    } while (op != NULL);
}

static void
free_dbroot(RDB_dbroot *dbrootp)
{
    RDB_destroy_hashmap(&dbrootp->typemap);

    /*
     * destroy user-defined operators in memory
     */
    RDB_hashmap_apply(&dbrootp->ro_opmap, &free_ro_ops, &dbrootp->ro_opmap);
    RDB_hashmap_apply(&dbrootp->upd_opmap, &free_upd_ops, &dbrootp->upd_opmap);

    RDB_destroy_hashmap(&dbrootp->ro_opmap);
    RDB_destroy_hashmap(&dbrootp->upd_opmap);
    free(dbrootp);
}

/* cleanup function to close all DBs and tables */
static void
cleanup_env(RDB_environment *envp)
{
    RDB_dbroot *dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    RDB_database *dbp;
    RDB_database *nextdbp;

    if (dbrootp == NULL)
        return;

    dbp = dbrootp->firstdbp;

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
    RDB_bool crdbroot = RDB_FALSE;

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */

        /* Create db structure */
        dbp = new_db(name);
        if (dbp == NULL)
            return RDB_NO_MEMORY;

        crdbroot = RDB_TRUE;
        _RDB_init_builtin_types();
        lt_dlinit();
        ret = create_dbroot(name, envp, RDB_FALSE, dbp, &dbrootp);
        if (ret != RDB_OK) {
            goto error;
        }
    } else {
        /* Get first db in list */
        dbp = dbrootp->firstdbp;
    }

    /* search the DB list for the database */
    for (idbp = dbrootp->firstdbp; idbp != NULL; idbp = idbp->nextdbp) {
        if (strcmp(dbp->name, name) == 0) {
            if (crdbroot) {
                /* dbp is not used */
                free_db(dbp);
            }

            *dbpp = idbp;
            idbp->refcount++;
            return RDB_OK;
        }
    }

    /*
     * Not found, read database from catalog
     */

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
        RDB_drop_type(tbp->typ, NULL);
        RDB_destroy_hashmap(&tbp->var.stored.attrmap);
    }

    /* Delete candidate keys */
    for (i = 0; i < tbp->keyc; i++) {
        RDB_free_strvec(tbp->keyv[i].strc, tbp->keyv[i].strv);
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
    exprp = RDB_expr_attr("IS_USER");
    ret = RDB_select(dbp->dbrootp->rtables_tbp, exprp, &vtbp);
    if (ret != RDB_OK) {
        RDB_drop_expr(exprp);
        goto error;
    }
    exprp = RDB_eq(RDB_expr_attr("DBNAME"),
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

    exprp = RDB_eq(RDB_expr_attr("DBNAME"),
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

    exprp = RDB_eq(RDB_expr_attr("DBNAME"),
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

/*
 * Creates a table, if it does not already exist,
 * or opens an existing table, depending on the create
 * argument.
 * Does not insert the table into the catalog.
 */
int
_RDB_provide_table(const char *name, RDB_bool persistent,
           int attrc, RDB_attr heading[],
           int keyc, RDB_string_vec keyv[], RDB_bool usr,
           RDB_bool create, RDB_transaction *txp, RDB_table **tbpp)
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
            RDB_init_obj(tuptyp->var.tuple.attrv[i].defaultp);
            RDB_copy_obj(tuptyp->var.tuple.attrv[i].defaultp,
                    heading[i].defaultp);
        }
    }

    ret = _RDB_new_stored_table(name, persistent, reltyp,
                keyc, keyv, usr, tbpp);
    if (ret != RDB_OK) {
        RDB_drop_type(reltyp, NULL);
        return ret;
    }

    ret = _RDB_open_table(*tbpp, keyv[0].strc, keyv[0].strv, create, txp, NULL);
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
           attrc, heading, keyc, keyv, RDB_TRUE,
           RDB_TRUE, txp, tbpp);
}

int
RDB_create_table(const char *name, RDB_bool persistent,
                int strc, RDB_attr heading[],
                int keyc, RDB_string_vec keyv[],
                RDB_transaction *txp, RDB_table **tbpp)
{
    int ret;
    int i;

    if (!_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    for (i = 0; i < strc; i++) {
        if (!_RDB_legal_name(heading[i].name))
            return RDB_INVALID_ARGUMENT;
    }

    ret = _RDB_create_table(name, persistent, strc, heading, keyc, keyv,
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

    /* Schedule secondary indexes for deletion */
    for (i = 0; i < tbp->keyc - 1; i++) {
        _RDB_del_index(txp, tbp->var.stored.keyidxv[i]);
    }

    /* Schedule recmap for deletion */
    return _RDB_del_recmap(txp, tbp->var.stored.recmapp);
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
            if (tbp->is_persistent) {
                /* Delete table from catalog */
                ret = _RDB_catalog_delete(tbp, txp);
            }

            ret = _RDB_drop_rtable(tbp, txp);
            break;
        }
        case RDB_TB_SELECT_PINDEX:
            RDB_destroy_obj(&tbp->var.select.val);
        case RDB_TB_SELECT:
            RDB_drop_expr(tbp->var.select.exprp);
            if (rec) {
                ret = drop_anon_table(tbp->var.select.tbp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_UNION:
            if (rec) {
                ret = drop_anon_table(tbp->var._union.tbp1, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var._union.tbp2, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_MINUS:
            if (rec) {
                ret = drop_anon_table(tbp->var.minus.tbp1, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.minus.tbp2, txp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_INTERSECT:
            if (rec) {
                ret = drop_anon_table(tbp->var.intersect.tbp1, txp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.intersect.tbp2, txp);
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
    }

    _RDB_free_table(tbp, txp != NULL ? txp->dbp->dbrootp->envp : NULL);
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
        RDB_rollback(txp);
        return RDB_NO_MEMORY;
    }

    return RDB_OK;
}

int
RDB_make_persistent(RDB_table *tbp, RDB_transaction *txp)
{
    RDB_tuple tpl;
    RDB_object defval;
    int ret;

    if (tbp->is_persistent)
        return RDB_OK;

    if (tbp->name == NULL)
        return RDB_INVALID_ARGUMENT;

    /* Turning a transient real table into a persistent table is not supported */
    if (tbp->kind == RDB_TB_STORED)
        return RDB_NOT_SUPPORTED;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    RDB_init_tuple(&tpl);
    RDB_init_obj(&defval);

    ret = RDB_tuple_set_string(&tpl, "TABLENAME", tbp->name);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_tuple_set_bool(&tpl, "IS_USER", RDB_TRUE);
    if (ret != RDB_OK)
        goto error;

    ret = _RDB_table_to_obj(tbp, &defval);
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
    RDB_destroy_obj(&defval);

    return RDB_OK;
error:
    RDB_destroy_tuple(&tpl);
    RDB_destroy_obj(&defval);
    return ret;
}

int
RDB_create_ro_op(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const void *iargp, size_t iarglen, 
                 RDB_transaction *txp)
{
    RDB_tuple tpl;
    RDB_object iarg;
    char *typesbuf;
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
RDB_create_update_op(const char *name, int argc, RDB_type *argtv[],
                  RDB_bool upd[], const char *libname, const char *symname,
                  const void *iargp, size_t iarglen,
                  RDB_transaction *txp)
{
    RDB_tuple tpl;
    RDB_object iarg;
    char *typesbuf;
    int i;
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

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
        int argc, RDB_type *argtv[])
{
    RDB_ro_op **opp = RDB_hashmap_get(&dbrootp->ro_opmap, name, NULL);
    RDB_ro_op *op;

    if (opp == NULL || *opp == NULL)
        return NULL;
    
    op = *opp;

    /* Find a operation with same signature */
    do {
        if (op->argc == argc) {
            int i;

            for (i = 0; (i < argc)
                    && !RDB_type_equals(op->argtv[i], argtv[i]);
                 i++);
            if (i >= argc) {
                /* Found */
                return op;
            }
        }
        op = op->nextp;
    } while(op != NULL);

    return NULL;
}

static int
put_ro_op(RDB_dbroot *dbrootp, RDB_ro_op *op)
{
    int ret;
    RDB_ro_op **fopp = RDB_hashmap_get(&dbrootp->ro_opmap, op->name, NULL);

    if (fopp == NULL || *fopp == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&dbrootp->ro_opmap, op->name, &op, sizeof (op));
        if (ret != RDB_OK)
            return ret;
    } else {
        op->nextp = (*fopp)->nextp;
        (*fopp)->nextp = op;
    }
    return RDB_OK;
}

static RDB_type **
valv_to_typev(int valc, RDB_object **valv) {
    int i;
    RDB_type **typv = malloc(sizeof (RDB_type *) * valc);

    if (typv == NULL)
        return NULL;
    for (i = 0; i < valc; i++) {
        typv[i] = RDB_obj_type(valv[i]);
    }
    return typv;
}

int
_RDB_get_ro_op(const char *name, int argc, RDB_type *argtv[],
               RDB_transaction *txp, RDB_ro_op **opp)
{
    int ret;

    /* Lookup operator in map */
    *opp = get_ro_op(txp->dbp->dbrootp, name, argc, argtv);

    if (*opp == NULL) {
        /* Not found in map, so read from catalog */
        ret = _RDB_get_cat_ro_op(name, argc, argtv, txp, opp);
        if (ret != RDB_OK)
            return ret;
        
        /* Insert operator into map */
        ret = put_ro_op(txp->dbp->dbrootp, *opp);
        if (ret != RDB_OK) {
            free_ro_op(*opp);
            return ret;
        }
    }
    return RDB_OK;
}

int
RDB_call_ro_op(const char *name, int argc, RDB_object *argv[],
               RDB_object *retvalp, RDB_transaction *txp)
{
    RDB_ro_op *op;
    int ret;
    RDB_type **argtv;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    argtv = valv_to_typev(argc, argv);
    if (argtv == NULL) {
        RDB_rollback(txp);
        return RDB_NO_MEMORY;
    }
    ret = _RDB_get_ro_op(name, argc, argtv, txp, &op);
    free(argtv);
    if (ret != RDB_OK)
        goto error;

    ret = (*op->funcp)(name, argc, argv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, txp, retvalp);
    if (ret != RDB_OK)
        goto error;
    return RDB_OK;
error:
    if (RDB_is_syserr(ret))
        RDB_rollback(txp);
    return ret;
}

static RDB_upd_op *
get_upd_op(const RDB_dbroot *dbrootp, const char *name,
        int argc, RDB_object *argv[])
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

static int
put_upd_op(RDB_dbroot *dbrootp, RDB_upd_op *op)
{
    int ret;
    RDB_upd_op *fop = RDB_hashmap_get(&dbrootp->upd_opmap, op->name, NULL);

    if (fop == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&dbrootp->upd_opmap, op->name, &op, sizeof (op));
        if (ret != RDB_OK)
            return ret;
    } else {
        op->nextp = fop->nextp;
        fop->nextp = op;
    }
    return RDB_OK;
}

int
RDB_call_update_op(const char *name, int argc, RDB_object *argv[],
                RDB_transaction *txp)
{
    RDB_upd_op *op;
    RDB_bool *updv;
    int i;
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    op = get_upd_op(txp->dbp->dbrootp, name, argc, argv);
    if (op == NULL) {
        ret = _RDB_get_cat_upd_op(name, argc, argv, txp, &op);
        if (ret != RDB_OK)
            return ret;
        ret = put_upd_op(txp->dbp->dbrootp, op);
        if (ret != RDB_OK) {
            free_upd_op(op);
            return ret;
        }
    }

    updv = malloc(sizeof(RDB_bool) * argc);
    if (updv == NULL)
        return RDB_NO_MEMORY;
    for (i = 0; i < argc; i++)
        updv[i] = RDB_FALSE;
    ret = (*op->funcp)(name, argc, argv, updv, op->iarg.var.bin.datap,
            op->iarg.var.bin.len, txp);
    free(updv);
    return ret;
}

int
RDB_drop_op(const char *name, RDB_transaction *txp)
{
    RDB_expression *exp;
    RDB_table *vtbp;
    int ret;
    RDB_bool isempty;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /*
     * Check if it's a read-only operator
     */
    exp = RDB_eq(RDB_expr_attr("NAME"), RDB_string_const(name));
    if (exp == NULL) {
        RDB_rollback(txp);
        return RDB_NO_MEMORY;
    }
    ret = RDB_select(txp->dbp->dbrootp->ro_ops_tbp, exp, &vtbp);
    if (ret != RDB_OK) {
        return ret;
    }
    ret = RDB_table_is_empty(vtbp, txp, &isempty);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, txp);
        return ret;
    }
    ret = RDB_drop_table(vtbp, txp);
    if (ret != RDB_OK) {
        return ret;
    }

    if (isempty) {
        /* It's an update operator */
        RDB_upd_op *op = NULL;

        /* Delete all versions of update operator from hashmap */
        free_upd_ops(&txp->dbp->dbrootp->upd_opmap, name, NULL);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->upd_opmap, name,
                &op, sizeof (op));
        if (ret != RDB_OK) {
            RDB_rollback(txp);
            return ret;
        }
        
        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_expr_attr("NAME"), RDB_string_const(name));
        if (exp == NULL) {
            RDB_rollback(txp);
            return RDB_NO_MEMORY;
        }
        ret = RDB_delete(txp->dbp->dbrootp->upd_ops_tbp, exp, txp);
        RDB_drop_expr(exp);
        if (ret != RDB_OK) {
            return ret;
        }        
    } else {
        /* It's a read-only operator */
        RDB_ro_op *op = NULL;

        /* Delete all versions of readonly operator from hashmap */
        free_ro_ops(&txp->dbp->dbrootp->ro_opmap, name, NULL);
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->ro_opmap, name,
                &op, sizeof (op));
        if (ret != RDB_OK) {
            RDB_rollback(txp);
            return ret;
        }

        /* Delete all versions of update operator from the database */
        exp = RDB_eq(RDB_expr_attr("NAME"), RDB_string_const(name));
        if (exp == NULL) {
            RDB_rollback(txp);
            return RDB_NO_MEMORY;
        }
        ret = RDB_delete(txp->dbp->dbrootp->ro_ops_tbp, exp, txp);
        RDB_drop_expr(exp);
        if (ret != RDB_OK) {
            return ret;
        }
    }

    return RDB_OK;
}
