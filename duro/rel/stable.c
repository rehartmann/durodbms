/*
 * $Id$
 *
 * Copyright (C) 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Functions for physical storage of tables
 */

#include "rdb.h"
#include "typeimpl.h"
#include "catalog.h"
#include "internal.h"
#include <gen/strfns.h>
#include <gen/errors.h>
#include <string.h>

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

    RDB_destroy_hashmap(&stp->attrmap);
    if (stp->indexc > 0) {
        for (i = 0; i < stp->indexc; i++) {
            _RDB_free_tbindex(&stp->indexv[i]);
        }
        free(stp->indexv);
    }
}

int
_RDB_close_stored_table(RDB_stored_table *stp)
{
    int i;
    int ret;

    if (stp->indexc > 0) {
        /* close secondary indexes */
        for (i = 0; i < stp->indexc; i++) {
            if (stp->indexv[i].idxp != NULL)
                RDB_close_index(stp->indexv[i].idxp);
        }
    }

    /* close recmap */
    ret = RDB_close_recmap(stp->recmapp);
    if (ret != RDB_OK)
        return ret;

    free_stored_table(stp);
    return RDB_OK;
}

static int
compare_field(const void *data1p, size_t len1, const void *data2p, size_t len2,
              RDB_environment *envp, void *arg)
{
    RDB_object val1, val2, retval;
    RDB_object *valv[2];
    int res;
    RDB_type *typ = (RDB_type *)arg;
    RDB_transaction tx;

    RDB_init_obj(&val1);
    RDB_init_obj(&val2);
    RDB_init_obj(&retval);

    RDB_irep_to_obj(&val1, typ, data1p, len1);
    RDB_irep_to_obj(&val2, typ, data2p, len2);

    valv[0] = &val1;
    valv[1] = &val2;
    tx.txid = NULL;
    tx.envp = envp;
    tx.user_data = typ->tx_udata;
    retval.typ = &RDB_INTEGER;
    (*typ->comparep)("compare", 2, valv, typ->compare_iargp,
            typ->compare_iarglen, &tx, &retval);
    res = RDB_obj_int(&retval);

    RDB_destroy_obj(&val1);
    RDB_destroy_obj(&val2);
    RDB_destroy_obj(&retval);

    return res;
}

static int
create_index(RDB_table *tbp, RDB_environment *envp, RDB_transaction *txp,
             _RDB_tbindex *indexp, int flags)
{
    int ret;
    int i;
    RDB_compare_field *cmpv = 0;
    int *fieldv = malloc(sizeof(int *) * indexp->attrc);

    if (fieldv == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    if (RDB_ORDERED & flags) {
        cmpv = malloc(sizeof (RDB_compare_field) * indexp->attrc);
        if (cmpv == NULL) {
            ret = RDB_NO_MEMORY;
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
        void *np = RDB_hashmap_get(&tbp->stp->attrmap,
                indexp->attrv[i].attrname, NULL);
        if (np == NULL)
            return RDB_ATTRIBUTE_NOT_FOUND;
        fieldv[i] = *(int *) np;
    }

    /* Create record-layer index */
    ret = RDB_create_index(tbp->stp->recmapp,
                  tbp->is_persistent ? indexp->name : NULL,
                  tbp->is_persistent ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, cmpv, flags,
                  txp != NULL ? txp->txid : NULL, &indexp->idxp);

cleanup:
    free(fieldv);
    free(cmpv);
    return ret;
}

/*
 * Create indexc/indexv and create secondary indexes
 */
static int
key_to_indexes(RDB_table *tbp, RDB_transaction *txp)
{
    int i, j;

    tbp->stp->indexc = tbp->keyc;
    tbp->stp->indexv = malloc(sizeof (_RDB_tbindex)
            * tbp->stp->indexc);
    if (tbp->stp->indexv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < tbp->stp->indexc; i++) {
        tbp->stp->indexv[i].attrc = tbp->keyv[i].strc;
        tbp->stp->indexv[i].attrv = malloc(sizeof(RDB_seq_item)
                * tbp->keyv[i].strc);
        if (tbp->stp->indexv[i].attrv == NULL)
            return RDB_NO_MEMORY;
        for (j = 0; j < tbp->keyv[i].strc; j++) {
            tbp->stp->indexv[i].attrv[j].attrname =
                    RDB_dup_str(tbp->keyv[i].strv[j]);
            if (tbp->stp->indexv[i].attrv[j].attrname == NULL)
                return RDB_NO_MEMORY;
        }

        if (tbp->is_persistent) {
            tbp->stp->indexv[i].name = malloc(strlen(tbp->name) + 4);
            if (tbp->stp->indexv[i].name == NULL) {
                return RDB_NO_MEMORY;
            }
            /* build index name */            
            sprintf(tbp->stp->indexv[i].name, "%s$%d", tbp->name, i);
        } else {
            tbp->stp->indexv[i].name = NULL;
        }

        tbp->stp->indexv[i].unique = RDB_TRUE;
        tbp->stp->indexv[i].idxp = NULL;
    }
    return RDB_OK;
}

static int
create_key_indexes(RDB_table *tbp, RDB_environment *envp, RDB_transaction *txp)
{
    int i;
    int ret;

    /*
     * Create secondary indexes
     */

    /* If it's not a system table, create index in catalog */
    if (tbp->is_user && tbp->is_persistent) {
        for (i = 0; i < tbp->stp->indexc; i++) {
            ret = _RDB_cat_insert_index(&tbp->stp->indexv[i], tbp->name,
                    txp);
            if (ret != RDB_OK) {
                if (ret == RDB_KEY_VIOLATION)
                    ret = RDB_ELEMENT_EXISTS;
                return ret;
            }
        }
    }

    /* Create a BDB secondary index for the indexes except the first */
    for (i = 1; i < tbp->stp->indexc; i++) {
        ret = create_index(tbp, envp, txp, &tbp->stp->indexv[i], RDB_UNIQUE);
        if (ret != RDB_OK)
            return ret;
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
         RDB_compare_field *cmpv)
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
        return RDB_NO_MEMORY;
    }

    /*
     * Try to get primary index (not available for new tables are
     * newly opened system tables)
     */
    if (tbp->is_persistent) {
        pindexp = NULL;
        for (i = 0; i < tbp->stp->indexc; i++) {
            char *p = strchr(tbp->stp->indexv[i].name, '$');
            if (p != NULL && strcmp (p, "$0") == 0) {
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
                if (piattrc == attrc && piattrc > 1 && fno != i)
                    fprintf(stderr, "fno=%d, i=%d\n", fno, i);
            }
        }

#ifdef OLD
/*        if (piattrc == attrc) {
            fno = i;
        } else { */
            /* Search attribute in key */
            fno = (RDB_int) RDB_find_str(piattrc, piattrv, heading[i].name);
            if (piattrc == attrc && piattrc > 1 && fno != i)
                fprintf(stderr, "fno=%d, i=%d\n", fno, i);
/*        } */
#endif

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
        ret = RDB_hashmap_put(&tbp->stp->attrmap,
                tbp->typ->var.basetyp->var.tuple.attrv[i].name,
                &fno, sizeof fno);
        if (ret != RDB_OK) {
            free(*flenvp);
            return ret;
        }

        (*flenvp)[fno] = replen(heading[i].typ);
    }
    return RDB_OK;
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
        const RDB_bool ascv[], RDB_transaction *txp)
{
    int ret;
    int *flenv;
    int flags;
    char *rmname = NULL;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    int piattrc = tbp->keyv[0].strc;

    if (!tbp->is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    tbp->stp = malloc(sizeof(RDB_stored_table));
    if (tbp->stp == NULL)
        return RDB_NO_MEMORY;

    RDB_init_hashmap(&tbp->stp->attrmap, RDB_DFL_MAP_CAPACITY);
    tbp->stp->est_cardinality = 1000;

    /* Allocate comparison vector, if needed */
    if (ascv != NULL) {
        cmpv = malloc(sizeof (RDB_compare_field) * piattrc);
        if (cmpv == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }

    ret = key_to_indexes(tbp, txp);
    if (ret != RDB_OK)
        return ret;

    ret = key_fnos(tbp, &flenv, ascv, cmpv);
    if (ret != RDB_OK)
        return ret;

    /*
     * If the table is a persistent user table, insert recmap into SYS_TABLE_RECMAP
     */
    if (tbp->is_persistent && tbp->is_user) {
        ret = _RDB_cat_insert_table_recmap(tbp, tbp->name, txp);
        if (ret == RDB_KEY_VIOLATION) {
            /* Choose a different recmap name */
            int n = 0;
            rmname = malloc(strlen(tbp->name) + 4);
            if (rmname == NULL) {
                ret = RDB_NO_MEMORY;
                goto error;
            }
            do {
                sprintf(rmname, "%s%d", tbp->name, ++n);
                ret = _RDB_cat_insert_table_recmap(tbp, rmname, txp);
            } while (ret == RDB_KEY_VIOLATION && n <= 999);
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
    if (ret != RDB_OK)
        goto error;

    /* Open/create indexes if there is more than one key */
    ret = create_key_indexes(tbp, envp, txp);
    if (ret != RDB_OK)
        goto error;

    free(flenv);
    free(cmpv);
    free(rmname);
    return RDB_OK;

error:
    /* clean up */
    free(flenv);
    free(cmpv);
    free(rmname);

    RDB_destroy_hashmap(&tbp->stp->attrmap);
    free(tbp->stp);

    if (txp != NULL)
        _RDB_handle_syserr(txp, ret);
    return ret;
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
        RDB_transaction *txp)
{
    int ret;
    int i;
    int *flenv;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    int piattrc = tbp->keyv[0].strc;

    if (!tbp->is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    tbp->stp = malloc(sizeof(RDB_stored_table));
    if (tbp->stp == NULL)
        return RDB_NO_MEMORY;

    RDB_init_hashmap(&tbp->stp->attrmap, RDB_DFL_MAP_CAPACITY);
    tbp->stp->est_cardinality = 1000;

    tbp->stp->indexc = indexc;
    tbp->stp->indexv = indexv;

    ret = key_fnos(tbp, &flenv, NULL, NULL);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_open_recmap(rmname, RDB_DATAFILE, envp,
            attrc, flenv, piattrc, txp != NULL ? txp->txid : NULL,
            &tbp->stp->recmapp);
    if (ret != RDB_OK)
        goto error;

    /* Open secondary indexes */
    for (i = 0; i < indexc; i++) {
        char *p = strchr(indexv[i].name, '$');
        if (p == NULL || strcmp (p, "$0") != 0) {
            ret = _RDB_open_table_index(tbp, &indexv[i], envp, txp);
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

    RDB_destroy_hashmap(&tbp->stp->attrmap);
    free(tbp->stp);

    if (txp != NULL)
        _RDB_handle_syserr(txp, ret);
    return ret;
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
        fieldv[i] = *(int *) RDB_hashmap_get(&tbp->stp->attrmap,
                        indexp->attrv[i].attrname, NULL);
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
        const RDB_seq_item idxcompv[], int flags, RDB_transaction *txp)
{
    int i;
    int ret;
    _RDB_tbindex *indexp;
    RDB_transaction tx;

    if (!_RDB_legal_name(name))
        return RDB_INVALID_ARGUMENT;

    tbp->stp->indexv = realloc(tbp->stp->indexv,
            (tbp->stp->indexc + 1) * sizeof (_RDB_tbindex));
    if (tbp->stp->indexv == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    indexp = &tbp->stp->indexv[tbp->stp->indexc++];

    indexp->name = RDB_dup_str(name);
    if (indexp->name == NULL) {
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    indexp->attrc = idxcompc;
    indexp->attrv = malloc(sizeof (RDB_seq_item) * idxcompc);
    if (indexp->attrv == NULL) {
        free(indexp->name);
        RDB_rollback_all(txp);
        return RDB_NO_MEMORY;
    }

    for (i = 0; i < idxcompc; i++) {
        indexp->attrv[i].attrname = RDB_dup_str(idxcompv[i].attrname);
        if (indexp->attrv[i].attrname == NULL) {
            RDB_rollback_all(txp);
            return RDB_NO_MEMORY;
        }
        indexp->attrv[i].asc = idxcompv[i].asc;
    }
    indexp->unique = (RDB_bool) (RDB_UNIQUE & flags);

    ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK) {
        _RDB_handle_syserr(txp, ret);
        return ret;
    }

    if (tbp->is_persistent) {
        /* Insert index into catalog */
        ret = _RDB_cat_insert_index(indexp, tbp->name, &tx);
        if (ret != RDB_OK)
            goto error;
    }

    /* Create index */
    ret = create_index(tbp, RDB_db_env(RDB_tx_db(txp)), &tx, indexp, flags);
    if (ret != RDB_OK) {
        goto error;
    }

    return RDB_commit(&tx);

error:
    _RDB_handle_syserr(txp, ret);
    RDB_rollback(&tx);
    tbp->stp->indexv = realloc(tbp->stp->indexv,
            (--tbp->stp->indexc) * sizeof (_RDB_tbindex)); /* !! */
    return ret;
}

int
_RDB_delete_stored_table(RDB_stored_table *stp, RDB_transaction *txp)
{
    int i;
    int ret;

    /* Schedule secondary indexes for deletion */
    for (i = 0; i < stp->indexc; i++) {
        if (stp->indexv[i].idxp != NULL) {
            ret = _RDB_del_index(txp, stp->indexv[i].idxp);
            if (ret != RDB_OK)
                return ret;
        }
    }

    if (txp != NULL) {
        /* Schedule recmap for deletion */
        ret = _RDB_del_recmap(txp, stp->recmapp);
    } else {
        ret = RDB_delete_recmap(stp->recmapp, NULL);
    }
    free_stored_table(stp);
    return ret;
}
