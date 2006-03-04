/*
 * $Id$
 *
 * Copyright (C) 2005-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Functions for physical storage of tables
 */

#include "rdb.h"
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

    free(idxp->name);
    for (i = 0; i < idxp->attrc; i++)
        free(idxp->attrv[i].attrname);
    free(idxp->attrv);
}

void
free_stored_table(RDB_stored_table *stp)
{
    int i;
    RDB_hashtable_iter hiter;
    _RDB_attrmap_entry *entryp;

    RDB_init_hashtable_iter(&hiter, &stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        free(entryp->key);
        free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&stp->attrmap);

    if (stp->indexc > 0) {
        for (i = 0; i < stp->indexc; i++) {
            _RDB_free_tbindex(&stp->indexv[i]);
        }
        free(stp->indexv);
    }
    free(stp);
}

int
_RDB_close_stored_table(RDB_stored_table *stp)
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
    if (ret != RDB_OK)
        return ret;

    free_stored_table(stp);
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

int
_RDB_put_field_no(RDB_stored_table *stp, const char *attrname,
        RDB_int fno, RDB_exec_context *ecp)
{
    int ret;
    _RDB_attrmap_entry *entryp = malloc(sizeof(_RDB_attrmap_entry));
    if (entryp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    entryp->key = RDB_dup_str(attrname);
    if (entryp->key == NULL) {
        free(entryp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    entryp->fno = fno;
    ret = RDB_hashtable_put(&stp->attrmap, entryp, NULL);
    if (ret != RDB_OK) {
        free(entryp->key);
        free(entryp);
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
    retval.typ = &RDB_INTEGER;
    (*typ->comparep)("compare", 2, valv, typ->compare_iargp,
            typ->compare_iarglen, _RDB_cmp_ecp, &tx, &retval);
    ret = RDB_obj_int(&retval);

    RDB_destroy_obj(&val1, _RDB_cmp_ecp);
    RDB_destroy_obj(&val2, _RDB_cmp_ecp);
    RDB_destroy_obj(&retval, _RDB_cmp_ecp);

    return ret;
}

static int
create_index(RDB_table *tbp, RDB_environment *envp, RDB_exec_context *ecp,
        RDB_transaction *txp, _RDB_tbindex *indexp, int flags)
{
    int ret;
    int i;
    RDB_compare_field *cmpv = 0;
    int *fieldv = malloc(sizeof(int *) * indexp->attrc);

    if (fieldv == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (RDB_ORDERED & flags) {
        cmpv = malloc(sizeof (RDB_compare_field) * indexp->attrc);
        if (cmpv == NULL) {
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        for (i = 0; i < indexp->attrc; i++) {
            RDB_type *attrtyp = RDB_type_attr_type(tbp->typ,
                    indexp->attrv[i].attrname);

            if (attrtyp->comparep != NULL) {
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
        RDB_int *np = _RDB_field_no(tbp->stp, indexp->attrv[i].attrname);
        if (np == NULL) {
            RDB_raise_attribute_not_found(indexp->attrv[i].attrname, ecp);
            return RDB_ERROR;
        }
        fieldv[i] = *np;
    }

    /* Create record-layer index */
    ret = RDB_create_index(tbp->stp->recmapp,
                  tbp->is_persistent ? indexp->name : NULL,
                  tbp->is_persistent ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, cmpv, flags,
                  txp != NULL ? txp->txid : NULL, &indexp->idxp);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        indexp->idxp = NULL;
        ret = RDB_ERROR;
    }

cleanup:
    free(fieldv);
    free(cmpv);
    return ret;
}

/*
 * Convert keys to indexes
 */
static int
keys_to_indexes(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int oindexc;
    int ret;
    
    oindexc = tbp->stp->indexc;
    if (oindexc == 0)
        tbp->stp->indexv = NULL;

    tbp->stp->indexc += tbp->keyc;
    tbp->stp->indexv = realloc(tbp->stp->indexv,
            sizeof (_RDB_tbindex) * tbp->stp->indexc);
    if (tbp->stp->indexv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < tbp->keyc; i++) {
        tbp->stp->indexv[oindexc + i].unique = RDB_TRUE;
        tbp->stp->indexv[oindexc + i].ordered = RDB_FALSE;
        tbp->stp->indexv[oindexc + i].attrc = tbp->keyv[i].strc;
        tbp->stp->indexv[oindexc + i].attrv = malloc(sizeof(RDB_seq_item)
                * tbp->keyv[i].strc);
        if (tbp->stp->indexv[oindexc + i].attrv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        for (j = 0; j < tbp->keyv[i].strc; j++) {
            tbp->stp->indexv[oindexc + i].attrv[j].attrname =
                    RDB_dup_str(tbp->keyv[i].strv[j]);
            if (tbp->stp->indexv[oindexc + i].attrv[j].attrname == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
        }

        if (tbp->is_persistent) {
            tbp->stp->indexv[oindexc + i].name = malloc(strlen(tbp->name) + 4);
            if (tbp->stp->indexv[oindexc + i].name == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            /* build index name */            
            sprintf(tbp->stp->indexv[oindexc + i].name, "%s$%d", tbp->name, i);

            if (tbp->is_user) {
                ret = _RDB_cat_insert_index(tbp->stp->indexv[oindexc + i].name,
                        tbp->stp->indexv[oindexc + i].attrc,
                        tbp->stp->indexv[oindexc + i].attrv,
                        RDB_TRUE, RDB_FALSE, tbp->name, ecp, txp);
                if (ret != RDB_OK)
                    return ret;
            }
        } else {
            tbp->stp->indexv[oindexc + i].name = NULL;
        }

        tbp->stp->indexv[oindexc + i].idxp = NULL;
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
create_indexes(RDB_table *tbp, RDB_environment *envp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;

    /*
     * Create secondary indexes
     */

    for (i = 0; i < tbp->stp->indexc; i++) {
        /* Create a BDB secondary index if it's not the primary index */
        if (!index_is_primary(tbp->stp->indexv[i].name)) {
            int flags = 0;

            if (tbp->stp->indexv[i].unique)
                flags = RDB_UNIQUE;
            if (tbp->stp->indexv[i].ordered)
                flags |= RDB_ORDERED;

            ret = create_index(tbp, envp, ecp, txp, &tbp->stp->indexv[i], flags);
            if (ret != RDB_OK)
                return ret;
        }
    }
    return RDB_OK;
}

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

static int
key_fnos(RDB_table *tbp, int **flenvp, const RDB_bool ascv[],
         RDB_compare_field *cmpv, RDB_exec_context *ecp)
{
    int ret;
    int i, j, di;
    _RDB_tbindex *pindexp;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    RDB_attr *heading = tbp->typ->var.basetyp->var.tuple.attrv;
    int piattrc = tbp->keyv[0].strc;
    char **piattrv = tbp->keyv[0].strv;

    *flenvp = malloc(sizeof(int) * attrc);
    if (*flenvp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /*
     * Try to get primary index (not available for new tables or
     * newly opened system tables)
     */
    if (tbp->is_persistent) {
        pindexp = NULL;
        for (i = 0; i < tbp->stp->indexc; i++) {
            if (index_is_primary(tbp->stp->indexv[i].name)) {
                pindexp = &tbp->stp->indexv[i];
                break;
            }
        }
    } else {
        /* Transient tables don't have secondary indexes */
        pindexp = &tbp->stp->indexv[0];
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
            if (heading[i].typ->comparep != NULL) {
                cmpv[fno].comparep = &compare_field;
                cmpv[fno].arg = heading[i].typ;
            } else {
                cmpv[fno].comparep = NULL;
            }
            cmpv[fno].asc = ascv[fno];
        }

        /* Put the field number into the attrmap */
        ret = _RDB_put_field_no(tbp->stp,
                tbp->typ->var.basetyp->var.tuple.attrv[i].name, fno, ecp);
        if (ret != RDB_OK) {
            free(*flenvp);
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
_RDB_create_stored_table(RDB_table *tbp, RDB_environment *envp,
        const RDB_bool ascv[], RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int flags;
    RDB_hashtable_iter hiter;
    _RDB_attrmap_entry *entryp;
    int *flenv = NULL;
    char *rmname = NULL;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    int piattrc = tbp->keyv[0].strc;

    if (!tbp->is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    tbp->stp = malloc(sizeof(RDB_stored_table));
    if (tbp->stp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    tbp->stp->recmapp = NULL;
    RDB_init_hashtable(&tbp->stp->attrmap, RDB_DFL_MAP_CAPACITY, &hash_str,
            &str_equals);
    tbp->stp->est_cardinality = 0;

    /* Allocate comparison vector, if needed */
    if (ascv != NULL) {
        cmpv = malloc(sizeof (RDB_compare_field) * piattrc);
        if (cmpv == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }

    if (tbp->is_persistent && tbp->is_user) {
        /* Get indexes from catalog */
        tbp->stp->indexc = _RDB_cat_get_indexes(tbp->name, txp->dbp->dbrootp,
                ecp, txp, &tbp->stp->indexv);
        if (tbp->stp->indexc < 0) {
            goto error;
        }
    } else {
        tbp->stp->indexc = 0;
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
    if (tbp->is_persistent && tbp->is_user) {
        ret = _RDB_cat_insert_table_recmap(tbp, tbp->name, ecp, txp);
        if (ret == RDB_ERROR
                && RDB_obj_type(RDB_get_err(ecp)) == &RDB_KEY_VIOLATION_ERROR) {
            int n = 0;

            RDB_clear_err(ecp);
            /* Choose a different recmap name */
            rmname = malloc(strlen(tbp->name) + 4);
            if (rmname == NULL) {
                RDB_raise_no_memory(ecp);
                goto error;
            }
            do {
                sprintf(rmname, "%s%d", tbp->name, ++n);
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
    if (ascv != NULL || !tbp->is_persistent)
        flags |= RDB_ORDERED;
    if (ascv == NULL)
        flags |= RDB_UNIQUE;

    ret = RDB_create_recmap(tbp->is_persistent ?
            (rmname == NULL ? tbp->name : rmname) : NULL,
            tbp->is_persistent ? RDB_DATAFILE : NULL,
            envp, attrc, flenv, piattrc, cmpv, flags,
            txp != NULL ? txp->txid : NULL,
            &tbp->stp->recmapp);
    if (ret != RDB_OK) {
        goto error;
    }

    /* Create non-primary indexes */
    if (tbp->stp->indexc > 1) {
        ret = create_indexes(tbp, envp, ecp, txp);
        if (ret != RDB_OK)
            goto error;
    }

    free(flenv);
    free(cmpv);
    free(rmname);
    return RDB_OK;

error:
    /* clean up */
    if (flenv != NULL) {
        free(flenv);
    }
    free(cmpv);
    free(rmname);

    RDB_init_hashtable_iter(&hiter, &tbp->stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        free(entryp->key);
        free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&tbp->stp->attrmap);

    if (tbp->stp->recmapp != NULL) {
        RDB_delete_recmap(tbp->stp->recmapp, txp != NULL ? txp->txid : NULL);
    }
    free(tbp->stp);
    tbp->stp = NULL;

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
_RDB_open_stored_table(RDB_table *tbp, RDB_environment *envp,
        const char *rmname, int indexc, _RDB_tbindex *indexv,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    int *flenv;
    RDB_hashtable_iter hiter;
    _RDB_attrmap_entry *entryp;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    int piattrc = tbp->keyv[0].strc;

    if (!tbp->is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    tbp->stp = malloc(sizeof(RDB_stored_table));
    if (tbp->stp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_init_hashtable(&tbp->stp->attrmap, RDB_DFL_MAP_CAPACITY, &hash_str,
            &str_equals);
    tbp->stp->est_cardinality = 1000;

    tbp->stp->indexc = indexc;
    tbp->stp->indexv = indexv;

    ret = key_fnos(tbp, &flenv, NULL, NULL, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = RDB_open_recmap(rmname, RDB_DATAFILE, envp,
            attrc, flenv, piattrc, txp != NULL ? txp->txid : NULL,
            &tbp->stp->recmapp);
    if (ret != RDB_OK) {
        if (ret == ENOENT) {
            RDB_raise_not_found("table not found", ecp);
        } else {
            _RDB_handle_errcode(ret, ecp, txp);
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

    free(flenv);
    free(cmpv);
    return RDB_OK;

error:
    /* clean up */
    free(flenv);
    free(cmpv);

    RDB_init_hashtable_iter(&hiter, &tbp->stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        free(entryp->key);
        free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&tbp->stp->attrmap);
    free(tbp->stp);

    return RDB_ERROR;
}

int
_RDB_open_table_index(RDB_table *tbp, _RDB_tbindex *indexp,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    int *fieldv = malloc(sizeof(int *) * indexp->attrc);

    if (fieldv == NULL) {
        RDB_raise_no_memory(ecp);
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* get index numbers */
    for (i = 0; i < indexp->attrc; i++) {
        fieldv[i] = *_RDB_field_no(tbp->stp, indexp->attrv[i].attrname);
    }

    /* open index */
    ret = RDB_open_index(tbp->stp->recmapp,
                  tbp->is_persistent ? indexp->name : NULL,
                  tbp->is_persistent ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, indexp->unique ? RDB_UNIQUE : 0,
                  txp != NULL ? txp->txid : NULL, &indexp->idxp);

cleanup:
    free(fieldv);
    return ret;
}

int
RDB_create_table_index(const char *name, RDB_table *tbp, int idxcompc,
        const RDB_seq_item idxcompv[], int flags, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;
    _RDB_tbindex *indexp;

    if (!_RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid index name", ecp);
        return RDB_ERROR;
    }

    if (tbp->is_persistent) {
        /* Insert index into catalog */
        ret = _RDB_cat_insert_index(name, idxcompc, idxcompv,
                (RDB_bool) (RDB_UNIQUE & flags),
                (RDB_bool) (RDB_ORDERED & flags), tbp->name, ecp, txp);
        if (ret != RDB_OK)
            goto error;
    }

    if (tbp->stp != NULL) {
        tbp->stp->indexv = realloc(tbp->stp->indexv,
                (tbp->stp->indexc + 1) * sizeof (_RDB_tbindex));
        if (tbp->stp->indexv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        indexp = &tbp->stp->indexv[tbp->stp->indexc++];

        indexp->name = RDB_dup_str(name);
        if (indexp->name == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        indexp->attrc = idxcompc;
        indexp->attrv = malloc(sizeof (RDB_seq_item) * idxcompc);
        if (indexp->attrv == NULL) {
            free(indexp->name);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        for (i = 0; i < idxcompc; i++) {
            indexp->attrv[i].attrname = RDB_dup_str(idxcompv[i].attrname);
            if (indexp->attrv[i].attrname == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            indexp->attrv[i].asc = idxcompv[i].asc;
        }
        indexp->unique = (RDB_bool) (RDB_UNIQUE & flags);
        indexp->ordered = (RDB_bool) (RDB_ORDERED & flags);

        /* Create index */
        ret = create_index(tbp, RDB_db_env(RDB_tx_db(txp)), ecp, txp, indexp,
                flags);
        if (ret != RDB_OK) {
            goto error;
        }
    }

    return RDB_OK;

error:
    if (tbp->stp != NULL) {
        /* Remove index entry */
        void *ivp = realloc(tbp->stp->indexv,
                (--tbp->stp->indexc) * sizeof (_RDB_tbindex));
        if (ivp != NULL)
            tbp->stp->indexv = ivp;
    }
    return RDB_ERROR;
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
        /* Schedule recmap for deletion */
        ret = _RDB_del_recmap(txp, stp->recmapp, ecp);
    } else {
        ret = RDB_delete_recmap(stp->recmapp, NULL);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, txp);
            ret = RDB_ERROR;
        }
    }
    free_stored_table(stp);
    return ret;
}
