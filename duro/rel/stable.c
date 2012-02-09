/*
 * $Id$
 *
 * Copyright (C) 2005-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Functions for physical storage of tables
 */

#include "stable.h"
#include "typeimpl.h"
#include "catalog.h"
#include "internal.h"
#include <gen/strfns.h>
#include <gen/hashmapit.h>
#include <string.h>
#include <errno.h>

/* name of the file in which the tables are physically stored */
#define RDB_DATAFILE "rdata"

void
_RDB_free_tbindex(_RDB_tbindex *idxp)
{
    int i;

    RDB_free(idxp->name);
    for (i = 0; i < idxp->attrc; i++)
        RDB_free(idxp->attrv[i].attrname);
    RDB_free(idxp->attrv);
}

static void
free_stored_table(RDB_stored_table *stp)
{
    int i;
    RDB_hashtable_iter hiter;
    _RDB_attrmap_entry *entryp;

    RDB_init_hashtable_iter(&hiter, &stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        RDB_free(entryp->key);
        RDB_free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&stp->attrmap);

    if (stp->indexc > 0) {
        for (i = 0; i < stp->indexc; i++) {
            _RDB_free_tbindex(&stp->indexv[i]);
        }
        RDB_free(stp->indexv);
    }
    RDB_free(stp);
}

int
_RDB_close_stored_table(RDB_stored_table *stp, RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (stp->indexc > 0) {
        /* Close secondary indexes */
        for (i = 0; i < stp->indexc; i++) {
            if (stp->indexv[i].idxp != NULL)
                RDB_close_index(stp->indexv[i].idxp);
        }
    }

    /* Close recmap */
    ret = RDB_close_recmap(stp->recmapp);
    free_stored_table(stp);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp, NULL);
        return RDB_ERROR;
    }
    return RDB_OK;
}

RDB_int *
_RDB_field_no(RDB_stored_table *stp, const char *attrname)
{
    _RDB_attrmap_entry entry;
    _RDB_attrmap_entry *entryp;

    entry.key = (char *) attrname;
    entryp = RDB_hashtable_get(&stp->attrmap, &entry,
            NULL);
    return entryp != NULL ? &entryp->fno : NULL;
}

static int
_RDB_put_field_no(RDB_stored_table *stp, const char *attrname,
        RDB_int fno, RDB_exec_context *ecp)
{
    int ret;
    _RDB_attrmap_entry *entryp = RDB_alloc(sizeof(_RDB_attrmap_entry), ecp);
    if (entryp == NULL) {
        return RDB_ERROR;
    }

    entryp->key = RDB_dup_str(attrname);
    if (entryp->key == NULL) {
        RDB_free(entryp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    entryp->fno = fno;
    ret = RDB_hashtable_put(&stp->attrmap, entryp, NULL);
    if (ret != RDB_OK) {
        RDB_free(entryp->key);
        RDB_free(entryp);
    }
    return ret;
}

RDB_exec_context *_RDB_cmp_ecp;

static int
compare_field(const void *data1p, size_t len1, const void *data2p, size_t len2,
              RDB_environment *envp, void *arg)
{
    RDB_object val1, val2, retval;
    RDB_object *valv[2];
    int ret;
    RDB_type *typ = (RDB_type *)arg;
    RDB_transaction tx;

    RDB_init_obj(&val1);
    RDB_init_obj(&val2);
    RDB_init_obj(&retval);

    RDB_irep_to_obj(&val1, typ, data1p, len1, _RDB_cmp_ecp);
    RDB_irep_to_obj(&val2, typ, data2p, len2, _RDB_cmp_ecp);

    valv[0] = &val1;
    valv[1] = &val2;
    tx.txid = NULL;
    tx.envp = envp;
    (*typ->compare_op->opfn.ro_fp) (2, valv, typ->compare_op, _RDB_cmp_ecp, &tx, &retval);
    ret = RDB_obj_int(&retval);

    RDB_destroy_obj(&val1, _RDB_cmp_ecp);
    RDB_destroy_obj(&val2, _RDB_cmp_ecp);
    RDB_destroy_obj(&retval, _RDB_cmp_ecp);

    return ret;
}

int
_RDB_create_tbindex(RDB_object *tbp, RDB_environment *envp, RDB_exec_context *ecp,
        RDB_transaction *txp, _RDB_tbindex *indexp, int flags)
{
    int ret;
    int i;
    RDB_compare_field *cmpv = 0;
    int *fieldv = RDB_alloc(sizeof(int *) * indexp->attrc, ecp);

    if (fieldv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (RDB_ORDERED & flags) {
        cmpv = RDB_alloc(sizeof (RDB_compare_field) * indexp->attrc, ecp);
        if (cmpv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        for (i = 0; i < indexp->attrc; i++) {
            RDB_type *attrtyp = RDB_type_attr_type(tbp->typ,
                    indexp->attrv[i].attrname);

            if (attrtyp->compare_op != NULL) {
                cmpv[i].comparep = &compare_field;
                cmpv[i].arg = attrtyp;
            } else {
                cmpv[i].comparep = NULL;
            }
            cmpv[i].asc = indexp->attrv[i].asc;
        }
    }

    /* Get index numbers */
    for (i = 0; i < indexp->attrc; i++) {
        RDB_int *np = _RDB_field_no(tbp->val.tb.stp, indexp->attrv[i].attrname);
        if (np == NULL) {
            RDB_raise_name(indexp->attrv[i].attrname, ecp);
            return RDB_ERROR;
        }
        fieldv[i] = *np;
    }

    /* Create record-layer index */
    ret = RDB_create_index(tbp->val.tb.stp->recmapp,
                  tbp->val.tb.is_persistent ? indexp->name : NULL,
                  tbp->val.tb.is_persistent ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, cmpv, flags,
                  txp != NULL ? txp->txid : NULL, &indexp->idxp);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        indexp->idxp = NULL;
        ret = RDB_ERROR;
    }

cleanup:
    RDB_free(fieldv);
    RDB_free(cmpv);
    return ret;
}

/*
 * Convert keys to indexes
 */
static int
keys_to_indexes(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int oindexc;
    int ret;
    
    oindexc = tbp->val.tb.stp->indexc;
    if (oindexc == 0)
        tbp->val.tb.stp->indexv = NULL;

    tbp->val.tb.stp->indexc += tbp->val.tb.keyc;
    tbp->val.tb.stp->indexv = RDB_realloc(tbp->val.tb.stp->indexv,
            sizeof (_RDB_tbindex) * tbp->val.tb.stp->indexc, ecp);
    if (tbp->val.tb.stp->indexv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < tbp->val.tb.keyc; i++) {
        tbp->val.tb.stp->indexv[oindexc + i].unique = RDB_TRUE;
        tbp->val.tb.stp->indexv[oindexc + i].ordered = RDB_FALSE;
        tbp->val.tb.stp->indexv[oindexc + i].attrc = tbp->val.tb.keyv[i].strc;
        tbp->val.tb.stp->indexv[oindexc + i].attrv = RDB_alloc(sizeof(RDB_seq_item)
                * tbp->val.tb.keyv[i].strc, ecp);
        if (tbp->val.tb.stp->indexv[oindexc + i].attrv == NULL) {
            return RDB_ERROR;
        }
        for (j = 0; j < tbp->val.tb.keyv[i].strc; j++) {
            tbp->val.tb.stp->indexv[oindexc + i].attrv[j].attrname =
                    RDB_dup_str(tbp->val.tb.keyv[i].strv[j]);
            if (tbp->val.tb.stp->indexv[oindexc + i].attrv[j].attrname == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
        }

        if (tbp->val.tb.is_persistent) {
            tbp->val.tb.stp->indexv[oindexc + i].name = RDB_alloc(strlen(RDB_table_name(tbp)) + 4, ecp);
            if (tbp->val.tb.stp->indexv[oindexc + i].name == NULL) {
                return RDB_ERROR;
            }
            /* build index name */            
            sprintf(tbp->val.tb.stp->indexv[oindexc + i].name, "%s$%d", RDB_table_name(tbp), i);

            if (tbp->val.tb.is_user) {
                ret = _RDB_cat_insert_index(tbp->val.tb.stp->indexv[oindexc + i].name,
                        tbp->val.tb.stp->indexv[oindexc + i].attrc,
                        tbp->val.tb.stp->indexv[oindexc + i].attrv,
                        RDB_TRUE, RDB_FALSE, RDB_table_name(tbp), ecp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
        } else {
            tbp->val.tb.stp->indexv[oindexc + i].name = NULL;
        }

        tbp->val.tb.stp->indexv[oindexc + i].idxp = NULL;
    }
    return RDB_OK;
}

static RDB_bool
index_is_primary(const char *name)
{
    char *p = strchr(name, '$');
    return (RDB_bool) (p != NULL && strcmp (p, "$0") == 0);
}

static int
create_indexes(RDB_object *tbp, RDB_environment *envp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;

    /*
     * Create secondary indexes
     */

    for (i = 0; i < tbp->val.tb.stp->indexc; i++) {
        /* Create a BDB secondary index if it's not the primary index */
        if (!index_is_primary(tbp->val.tb.stp->indexv[i].name)) {
            int flags = 0;

            if (tbp->val.tb.stp->indexv[i].unique)
                flags = RDB_UNIQUE;
            if (tbp->val.tb.stp->indexv[i].ordered)
                flags |= RDB_ORDERED;

            if (_RDB_create_tbindex(tbp, envp, ecp, txp,
                    &tbp->val.tb.stp->indexv[i], flags) != RDB_OK)
                return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
replen(const RDB_type *typ)
{
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
            for (i = 0; i < typ->def.tuple.attrc; i++) {
                len = replen(typ->def.tuple.attrv[i].typ);
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

static int
key_fnos(RDB_object *tbp, int **flenvp, const RDB_bool ascv[],
         RDB_compare_field *cmpv, RDB_exec_context *ecp)
{
    int ret;
    int i, j, di;
    _RDB_tbindex *pindexp;
    int attrc = tbp->typ->def.basetyp->def.tuple.attrc;
    RDB_attr *heading = tbp->typ->def.basetyp->def.tuple.attrv;
    int piattrc = tbp->val.tb.keyv[0].strc;
    char **piattrv = tbp->val.tb.keyv[0].strv;

    *flenvp = RDB_alloc(sizeof(int) * attrc, ecp);
    if (*flenvp == NULL) {
        return RDB_ERROR;
    }

    /*
     * Try to get primary index (not available for new tables or
     * newly opened system tables)
     */
    if (tbp->val.tb.is_persistent) {
        pindexp = NULL;
        for (i = 0; i < tbp->val.tb.stp->indexc; i++) {
            if (index_is_primary(tbp->val.tb.stp->indexv[i].name)) {
                pindexp = &tbp->val.tb.stp->indexv[i];
                break;
            }
        }
    } else {
        /* Transient tables don't have secondary indexes */
        pindexp = &tbp->val.tb.stp->indexv[0];
    }

    di = piattrc;
    for (i = 0; i < attrc; i++) {
        RDB_int fno;

        if (pindexp != NULL) {
            /*
             * Search attribute in index (necessary if there is user-defined
             * order of key attributes, e.g. when the primary index
             * is ordered)
             */
            for (j = 0; j < pindexp->attrc
                    && strcmp(pindexp->attrv[j].attrname, heading[i].name) != 0;
                    j++);
            fno = (j < pindexp->attrc) ? j : -1;
        } else {
            if (piattrc == attrc) {
                fno = i;
            } else {
                /* Search attribute in key */
                fno = (RDB_int) RDB_find_str(piattrc, piattrv, heading[i].name);
            }
        }

        /* If it's not found in the key, give it a non-key field number */
        if (fno == -1)
            fno = di++;
        else if (ascv != NULL) {
            /* Set comparison field */
            if (heading[i].typ->compare_op != NULL) {
                cmpv[fno].comparep = &compare_field;
                cmpv[fno].arg = heading[i].typ;
            } else {
                cmpv[fno].comparep = NULL;
            }
            cmpv[fno].asc = ascv[fno];
        }

        /* Put the field number into the attrmap */
        ret = _RDB_put_field_no(tbp->val.tb.stp,
                tbp->typ->def.basetyp->def.tuple.attrv[i].name, fno, ecp);
        if (ret != RDB_OK) {
            RDB_free(*flenvp);
            return ret;
        }

        (*flenvp)[fno] = replen(heading[i].typ);
    }
    return RDB_OK;
}

static unsigned
hash_str(const void *entryp, void *arg)
{
    return RDB_hash_str(((_RDB_attrmap_entry *) entryp)->key);
}

static RDB_bool
str_equals(const void *e1p, const void *e2p, void *arg)
{
    return (RDB_bool) strcmp(((_RDB_attrmap_entry *) e1p)->key,
            ((_RDB_attrmap_entry *) e2p)->key) == 0;
}

/*
 * Create the physical representation of a table.
 * (The recmap and the indexes)
 *
 * Arguments:
 * tbp        the table
 * envp       the database environment
 * txp        the transaction under which the operation is performed
 * ascv       the sorting order if it's a sorter, or NULL
 */
int
_RDB_create_stored_table(RDB_object *tbp, RDB_environment *envp,
        const RDB_bool ascv[], RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int flags;
    RDB_hashtable_iter hiter;
    _RDB_attrmap_entry *entryp;
    int *flenv = NULL;
    char *rmname = NULL;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->def.basetyp->def.tuple.attrc;
    int piattrc = tbp->val.tb.keyv[0].strc;

    if (!tbp->val.tb.is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbp->val.tb.stp = RDB_alloc(sizeof(RDB_stored_table), ecp);
    if (tbp->val.tb.stp == NULL) {
        return RDB_ERROR;
    }

    tbp->val.tb.stp->recmapp = NULL;
    RDB_init_hashtable(&tbp->val.tb.stp->attrmap, RDB_DFL_MAP_CAPACITY, &hash_str,
            &str_equals);
    tbp->val.tb.stp->est_cardinality = 0;

    /* Allocate comparison vector, if needed */
    if (ascv != NULL) {
        cmpv = RDB_alloc(sizeof (RDB_compare_field) * piattrc, ecp);
        if (cmpv == NULL) {
            goto error;
        }
    }

    if (tbp->val.tb.is_persistent && tbp->val.tb.is_user) {
        /* Get indexes from catalog */
        tbp->val.tb.stp->indexc = _RDB_cat_get_indexes(RDB_table_name(tbp), txp->dbp->dbrootp,
                ecp, txp, &tbp->val.tb.stp->indexv);
        if (tbp->val.tb.stp->indexc < 0) {
            goto error;
        }
    } else {
        tbp->val.tb.stp->indexc = 0;
    }

    ret = keys_to_indexes(tbp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    ret = key_fnos(tbp, &flenv, ascv, cmpv, ecp);
    if (ret != RDB_OK)
        goto error;

    /*
     * If the table is a persistent user table, insert recmap into SYS_TABLE_RECMAP
     */
    if (tbp->val.tb.is_persistent && tbp->val.tb.is_user) {
        ret = _RDB_cat_insert_table_recmap(tbp, RDB_table_name(tbp), ecp, txp);
        if (ret == RDB_ERROR
                && RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR) {
            int n = 0;

            RDB_clear_err(ecp);
            /* Choose a different recmap name */
            rmname = RDB_alloc(strlen(RDB_table_name(tbp)) + 4, ecp);
            if (rmname == NULL) {
                goto error;
            }
            do {
                sprintf(rmname, "%s%d", RDB_table_name(tbp), ++n);
                RDB_clear_err(ecp);
                ret = _RDB_cat_insert_table_recmap(tbp, rmname, ecp, txp);
            } while (ret != RDB_OK
                    && RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR
                    && n <= 999);
        }
        if (ret != RDB_OK)
            goto error;
    }

    /*
     * Use a sorted recmap for local tables, so the order of the tuples
     * is always the same if the table is stored as an attribute in a table.
     */
    flags = 0;
    if (ascv != NULL || !tbp->val.tb.is_persistent)
        flags |= RDB_ORDERED;
    if (ascv == NULL)
        flags |= RDB_UNIQUE;

    ret = RDB_create_recmap(tbp->val.tb.is_persistent ?
            (rmname == NULL ? RDB_table_name(tbp) : rmname) : NULL,
            tbp->val.tb.is_persistent ? RDB_DATAFILE : NULL,
            envp,
            attrc, flenv, piattrc, cmpv, flags,
            txp != NULL ? txp->txid : NULL,
            &tbp->val.tb.stp->recmapp);
    if (ret != RDB_OK) {
        tbp->val.tb.stp->recmapp = NULL;
        RDB_errcode_to_error(ret, ecp, txp);
        goto error;
    }

    /* Create non-primary indexes */
    if (tbp->val.tb.stp->indexc > 1) {
        ret = create_indexes(tbp, envp, ecp, txp);
        if (ret != RDB_OK)
            goto error;
    }

    RDB_free(flenv);
    RDB_free(cmpv);
    RDB_free(rmname);
    return RDB_OK;

error:
    /* clean up */
    if (flenv != NULL) {
        RDB_free(flenv);
    }
    RDB_free(cmpv);
    RDB_free(rmname);

    RDB_init_hashtable_iter(&hiter, &tbp->val.tb.stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        RDB_free(entryp->key);
        RDB_free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&tbp->val.tb.stp->attrmap);

    if (tbp->val.tb.stp->recmapp != NULL && txp == NULL) {
        /*
         * Delete recmap if there is no transaction, otherwise this must happen
         * through the rollback
         * (If the transaction has been created under the control of a transaction,
         * the transaction must have been committed before the DB handle can be destroyed)
         */
        RDB_delete_recmap(tbp->val.tb.stp->recmapp, NULL);
    }
    RDB_free(tbp->val.tb.stp);
    tbp->val.tb.stp = NULL;

    return RDB_ERROR;
}

/*
 * Open the physical representation of a table.
 * (The recmap and the indexes)
 *
 * Arguments:
 * tbp        the table
 * envp       the database environment
 * rmname     the recmap name
 * indexc/indexv the indexes
 * txp        the transaction under which the operation is performed
 */
int
_RDB_open_stored_table(RDB_object *tbp, RDB_environment *envp,
        const char *rmname, int indexc, _RDB_tbindex *indexv,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    int *flenv;
    RDB_hashtable_iter hiter;
    _RDB_attrmap_entry *entryp;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->def.basetyp->def.tuple.attrc;
    int piattrc = tbp->val.tb.keyv[0].strc;

    if (!tbp->val.tb.is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbp->val.tb.stp = RDB_alloc(sizeof(RDB_stored_table), ecp);
    if (tbp->val.tb.stp == NULL) {
        return RDB_ERROR;
    }

    RDB_init_hashtable(&tbp->val.tb.stp->attrmap, RDB_DFL_MAP_CAPACITY, &hash_str,
            &str_equals);
    tbp->val.tb.stp->est_cardinality = 1000;

    tbp->val.tb.stp->indexc = indexc;
    tbp->val.tb.stp->indexv = indexv;

    ret = key_fnos(tbp, &flenv, NULL, NULL, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = RDB_open_recmap(rmname, RDB_DATAFILE, envp,
            attrc, flenv, piattrc, txp != NULL ? txp->txid : NULL,
            &tbp->val.tb.stp->recmapp);
    if (ret != RDB_OK) {
        if (ret == ENOENT) {
            RDB_raise_not_found("table not found", ecp);
        } else {
            RDB_errcode_to_error(ret, ecp, txp);
        }
        goto error;
    }

    /* Open secondary indexes */
    for (i = 0; i < indexc; i++) {
        char *p = strchr(indexv[i].name, '$');
        if (p == NULL || strcmp (p, "$0") != 0) {
            ret = _RDB_open_table_index(tbp, &indexv[i], envp, ecp, txp);
            if (ret != RDB_OK)
                goto error;
        } else {
            indexv[i].idxp = NULL;
        }
    }

    RDB_free(flenv);
    RDB_free(cmpv);
    return RDB_OK;

error:
    /* clean up */
    RDB_free(flenv);
    RDB_free(cmpv);

    RDB_init_hashtable_iter(&hiter, &tbp->val.tb.stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        RDB_free(entryp->key);
        RDB_free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&tbp->val.tb.stp->attrmap);
    RDB_free(tbp->val.tb.stp);
    tbp->val.tb.stp = NULL;

    return RDB_ERROR;
}

int
_RDB_open_table_index(RDB_object *tbp, _RDB_tbindex *indexp,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    int *fieldv = RDB_alloc(sizeof(int *) * indexp->attrc, ecp);

    if (fieldv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* get index numbers */
    for (i = 0; i < indexp->attrc; i++) {
        fieldv[i] = *_RDB_field_no(tbp->val.tb.stp, indexp->attrv[i].attrname);
    }

    /* open index */
    ret = RDB_open_index(tbp->val.tb.stp->recmapp,
                  tbp->val.tb.is_persistent ? indexp->name : NULL,
                  tbp->val.tb.is_persistent ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, indexp->unique ? RDB_UNIQUE : 0,
                  txp != NULL ? txp->txid : NULL, &indexp->idxp);

cleanup:
    RDB_free(fieldv);
    return ret;
}

int
_RDB_delete_stored_table(RDB_stored_table *stp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;

    /* Schedule secondary indexes for deletion */
    for (i = 0; i < stp->indexc; i++) {
        if (stp->indexv[i].idxp != NULL) {
            ret = _RDB_del_index(txp, stp->indexv[i].idxp, ecp);
            if (ret != RDB_OK)
                return ret;
        }
    }

    if (txp != NULL) {
        /*
         * Schedule recmap for deletion.
         * The recmap cannot be deleted immediately, because
         * this means closing the DB handle, which must not be closed
         * before transaction commit.
         */
        ret = _RDB_del_recmap(txp, stp->recmapp, ecp);
    } else {
        ret = RDB_delete_recmap(stp->recmapp, NULL);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp, txp);
            ret = RDB_ERROR;
        }
    }
    free_stored_table(stp);
    return ret;
}
