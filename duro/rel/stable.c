/*
 * Stored table functions.
 *
 * Copyright (C) 2005-2009, 2012-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Functions for physical storage of tables
 */

#include "stable.h"
#include "typeimpl.h"
#include "cat_stored.h"
#include "internal.h"
#include <obj/excontext.h>
#include <obj/type.h>
#include <obj/objinternal.h>
#include <gen/strfns.h>
#include <gen/hashmapit.h>
#include <db.h>
#include <string.h>

void
RDB_free_tbindex(RDB_tbindex *idxp)
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
    RDB_attrmap_entry *entryp;

    RDB_init_hashtable_iter(&hiter, &stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        RDB_free(entryp->key);
        RDB_free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&stp->attrmap);

    if (stp->indexc > 0) {
        for (i = 0; i < stp->indexc; i++) {
            RDB_free_tbindex(&stp->indexv[i]);
        }
        RDB_free(stp->indexv);
    }
    RDB_free(stp);
}

int
RDB_close_stored_table(RDB_stored_table *stp, RDB_exec_context *ecp)
{
    int i;
    int ret;

    /* Close secondary indexes */
    for (i = 0; i < stp->indexc; i++) {
        if (stp->indexv[i].idxp != NULL) {
            RDB_close_index(stp->indexv[i].idxp, ecp);
        }
    }

    /* Close recmap */
    if (stp->recmapp != NULL) {
        ret = RDB_close_recmap(stp->recmapp, ecp);
        free_stored_table(stp);
        if (ret != RDB_OK) {
            RDB_handle_err(ecp, NULL);
            return RDB_ERROR;
        }
    } else {
        free_stored_table(stp);
    }
    return RDB_OK;
}

RDB_int *
RDB_field_no(RDB_stored_table *stp, const char *attrname)
{
    RDB_attrmap_entry entry;
    RDB_attrmap_entry *entryp;

    entry.key = (char *) attrname;
    entryp = RDB_hashtable_get(&stp->attrmap, &entry,
            NULL);
    return entryp != NULL ? &entryp->fno : NULL;
}

static int
RDB_put_field_no(RDB_stored_table *stp, const char *attrname,
        RDB_int fno, RDB_exec_context *ecp)
{
    int ret;
    RDB_attrmap_entry *entryp = RDB_alloc(sizeof(RDB_attrmap_entry), ecp);
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

RDB_exec_context *RDB_cmp_ecp;

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

    RDB_irep_to_obj(&val1, typ, data1p, len1, RDB_cmp_ecp);
    RDB_irep_to_obj(&val2, typ, data2p, len2, RDB_cmp_ecp);

    valv[0] = &val1;
    valv[1] = &val2;
    tx.tx = NULL;
    tx.envp = envp;
    (*typ->compare_op->opfn.ro_fp) (2, valv, typ->compare_op, RDB_cmp_ecp, &tx, &retval);
    ret = RDB_obj_int(&retval);

    RDB_destroy_obj(&val1, RDB_cmp_ecp);
    RDB_destroy_obj(&val2, RDB_cmp_ecp);
    RDB_destroy_obj(&retval, RDB_cmp_ecp);

    return ret;
}

static RDB_compare_field *
cmpvec(RDB_object *tbp, RDB_tbindex *indexp, RDB_exec_context *ecp)
{
    int i;
    RDB_compare_field *cmpv = RDB_alloc(sizeof (RDB_compare_field) * indexp->attrc, ecp);
    if (cmpv == NULL)
        return NULL;
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
    return cmpv;
}

int
RDB_create_tbindex(RDB_object *tbp, RDB_tbindex *indexp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    int flags;
    RDB_compare_field *cmpv = NULL;
    RDB_field_descriptor *fieldv = RDB_alloc(sizeof(RDB_field_descriptor) * indexp->attrc,
            ecp);

    if (fieldv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (indexp->ordered) {
        cmpv = cmpvec(tbp, indexp, ecp);
        if (cmpv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

    /* Get index numbers */
    for (i = 0; i < indexp->attrc; i++) {
        RDB_int *np = RDB_field_no(tbp->val.tbp->stp, indexp->attrv[i].attrname);
        if (np == NULL) {
            RDB_raise_name(indexp->attrv[i].attrname, ecp);
            return RDB_ERROR;
        }
        fieldv[i].no = *np;
        fieldv[i].attrname = indexp->attrv[i].attrname;
    }

    flags = 0;
    if (indexp->unique)
        flags = RDB_UNIQUE;
    if (indexp->ordered)
        flags |= RDB_ORDERED;

    /* Create record-layer index */
    indexp->idxp = RDB_create_index(tbp->val.tbp->stp->recmapp,
                  RDB_table_is_persistent(tbp) ? indexp->name : NULL,
                  RDB_table_is_persistent(tbp) ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, cmpv, flags,
                  txp != NULL ? txp->tx : NULL, ecp);
    if (indexp->idxp == NULL) {
        RDB_handle_err(ecp, txp);
        ret = RDB_ERROR;
    } else {
        ret = RDB_OK;
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

    oindexc = tbp->val.tbp->stp->indexc;
    if (oindexc == 0)
        tbp->val.tbp->stp->indexv = NULL;

    tbp->val.tbp->stp->indexc += tbp->val.tbp->keyc;
    tbp->val.tbp->stp->indexv = RDB_realloc(tbp->val.tbp->stp->indexv,
            sizeof (RDB_tbindex) * tbp->val.tbp->stp->indexc, ecp);
    if (tbp->val.tbp->stp->indexv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < tbp->val.tbp->keyc; i++) {
        const char *tbname;

        tbp->val.tbp->stp->indexv[oindexc + i].unique = RDB_TRUE;
        tbp->val.tbp->stp->indexv[oindexc + i].ordered = RDB_FALSE;
        tbp->val.tbp->stp->indexv[oindexc + i].attrc = tbp->val.tbp->keyv[i].strc;
        tbp->val.tbp->stp->indexv[oindexc + i].attrv = RDB_alloc(sizeof(RDB_seq_item)
                * tbp->val.tbp->keyv[i].strc, ecp);
        if (tbp->val.tbp->stp->indexv[oindexc + i].attrv == NULL) {
            return RDB_ERROR;
        }
        for (j = 0; j < tbp->val.tbp->keyv[i].strc; j++) {
            tbp->val.tbp->stp->indexv[oindexc + i].attrv[j].attrname =
                    RDB_dup_str(tbp->val.tbp->keyv[i].strv[j]);
            if (tbp->val.tbp->stp->indexv[oindexc + i].attrv[j].attrname == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
        }

        tbname = RDB_table_name(tbp);
        tbp->val.tbp->stp->indexv[oindexc + i].name = RDB_alloc((tbname != NULL ? strlen(tbname) : 0) + 4, ecp);
        if (tbp->val.tbp->stp->indexv[oindexc + i].name == NULL) {
            return RDB_ERROR;
        }
        /* build index name */
        sprintf(tbp->val.tbp->stp->indexv[oindexc + i].name, "%s$%d",
                tbname != NULL ? tbname : "", i);

        if (RDB_table_is_persistent(tbp) && RDB_table_is_user(tbp)) {
            ret = RDB_cat_insert_index(tbp->val.tbp->stp->indexv[oindexc + i].name,
                    tbp->val.tbp->stp->indexv[oindexc + i].attrc,
                    tbp->val.tbp->stp->indexv[oindexc + i].attrv,
                    RDB_TRUE, RDB_FALSE, RDB_table_name(tbp), ecp, txp);
            if (ret != RDB_OK)
                return ret;
        }

        tbp->val.tbp->stp->indexv[oindexc + i].idxp = NULL;
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

    for (i = 0; i < tbp->val.tbp->stp->indexc; i++) {
        /* Create a BDB secondary index if it's not the primary index */
        if ((!RDB_table_is_persistent(tbp) && i > 0)
                || (RDB_table_is_persistent(tbp)
                    && !index_is_primary(tbp->val.tbp->stp->indexv[i].name))) {
            if (RDB_create_tbindex(tbp, &tbp->val.tbp->stp->indexv[i], envp,
                    ecp, txp) != RDB_OK)
                return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
replen(const RDB_type *typ)
{
    if (RDB_type_is_dummy(typ)) {
        return RDB_VARIABLE_LEN;
    }
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
key_fnos(RDB_object *tbp, RDB_field_info **finfovp, const RDB_bool ascv[],
         RDB_compare_field *cmpv, RDB_exec_context *ecp)
{
    int ret;
    int i, j, di;
    RDB_tbindex *pindexp;
    int attrc = tbp->typ->def.basetyp->def.tuple.attrc;
    RDB_attr *heading = tbp->typ->def.basetyp->def.tuple.attrv;
    int piattrc = tbp->val.tbp->keyv[0].strc;
    char **piattrv = tbp->val.tbp->keyv[0].strv;

    *finfovp = RDB_alloc(sizeof(RDB_field_info) * attrc, ecp);
    if (*finfovp == NULL) {
        return RDB_ERROR;
    }

    /*
     * Try to get primary index (not available for new tables or
     * newly opened system tables)
     */
    if (RDB_table_is_persistent(tbp)) {
        pindexp = NULL;
        for (i = 0; i < tbp->val.tbp->stp->indexc; i++) {
            if (index_is_primary(tbp->val.tbp->stp->indexv[i].name)) {
                pindexp = &tbp->val.tbp->stp->indexv[i];
                break;
            }
        }
    } else {
        /* Transient tables don't have secondary indexes */
        pindexp = &tbp->val.tbp->stp->indexv[0];
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
        ret = RDB_put_field_no(tbp->val.tbp->stp,
                tbp->typ->def.basetyp->def.tuple.attrv[i].name, fno, ecp);
        if (ret != RDB_OK) {
            RDB_free(*finfovp);
            return ret;
        }

        (*finfovp)[fno].len = replen(heading[i].typ);
        (*finfovp)[fno].attrname = heading[i].name;

        (*finfovp)[fno].flags = 0;
        if (heading[i].typ == &RDB_STRING) {
            (*finfovp)[fno].flags |= RDB_FTYPE_CHAR;
        } else if (heading[i].typ == &RDB_BOOLEAN) {
            (*finfovp)[fno].flags |= RDB_FTYPE_BOOLEAN;
        } else if (heading[i].typ == &RDB_INTEGER) {
            (*finfovp)[fno].flags |= RDB_FTYPE_INTEGER;
        } else if (heading[i].typ == &RDB_FLOAT) {
            (*finfovp)[fno].flags |= RDB_FTYPE_FLOAT;
        }
    }
    return RDB_OK;
}

static unsigned
hash_str(const void *entryp, void *arg)
{
    return RDB_hash_str(((RDB_attrmap_entry *) entryp)->key);
}

static RDB_bool
str_equals(const void *e1p, const void *e2p, void *arg)
{
    return (RDB_bool) strcmp(((RDB_attrmap_entry *) e1p)->key,
            ((RDB_attrmap_entry *) e2p)->key) == 0;
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
RDB_create_stored_table(RDB_object *tbp, RDB_environment *envp,
        const RDB_bool ascv[], RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int flags;
    RDB_hashtable_iter hiter;
    RDB_attrmap_entry *entryp;
    RDB_field_info *finfov = NULL;
    char *rmname = NULL;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->def.basetyp->def.tuple.attrc;
    int piattrc = tbp->val.tbp->keyv[0].strc;

    /* Do not use a transaction if the table is transient */
    if (!RDB_table_is_persistent(tbp))
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbp->val.tbp->stp = RDB_alloc(sizeof(RDB_stored_table), ecp);
    if (tbp->val.tbp->stp == NULL) {
        return RDB_ERROR;
    }

    tbp->val.tbp->stp->recmapp = NULL;
    RDB_init_hashtable(&tbp->val.tbp->stp->attrmap, RDB_DFL_MAP_CAPACITY, &hash_str,
            &str_equals);
    tbp->val.tbp->stp->est_cardinality = 0;

    /* Allocate comparison vector, if needed */
    if (ascv != NULL) {
        cmpv = RDB_alloc(sizeof (RDB_compare_field) * piattrc, ecp);
        if (cmpv == NULL) {
            goto error;
        }
    }

    if (RDB_table_is_persistent(tbp) && RDB_table_is_user(tbp)) {
        /* Get indexes from catalog */
        tbp->val.tbp->stp->indexc = RDB_cat_get_indexes(RDB_table_name(tbp), txp->dbp->dbrootp,
                ecp, txp, &tbp->val.tbp->stp->indexv);
        if (tbp->val.tbp->stp->indexc < 0) {
            goto error;
        }
    } else {
        tbp->val.tbp->stp->indexc = 0;
    }

    ret = keys_to_indexes(tbp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    ret = key_fnos(tbp, &finfov, ascv, cmpv, ecp);
    if (ret != RDB_OK)
        goto error;

    /*
     * If the table is a persistent user table, insert recmap into sys_table_recmap
     */
    if (RDB_table_is_persistent(tbp) && RDB_table_is_user(tbp)) {
        ret = RDB_cat_insert_table_recmap(tbp, RDB_table_name(tbp), ecp, txp);
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
                ret = RDB_cat_insert_table_recmap(tbp, rmname, ecp, txp);
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
    if (ascv != NULL || !RDB_table_is_persistent(tbp))
        flags |= RDB_ORDERED;
    if (ascv == NULL)
        flags |= RDB_UNIQUE;

    if (RDB_table_is_persistent(tbp) && RDB_env_trace(envp)) {
        fprintf(stderr, "Creating physical storage for table %s\n",
                RDB_table_name(tbp));
    }
    tbp->val.tbp->stp->recmapp = RDB_create_recmap(RDB_table_is_persistent(tbp) ?
            (rmname == NULL ? RDB_table_name(tbp) : rmname) : NULL,
            RDB_table_is_persistent(tbp) ? RDB_DATAFILE : NULL,
            RDB_table_is_persistent(tbp) ? envp : NULL,
            attrc, finfov, piattrc, cmpv, flags,
            txp != NULL ? txp->tx : NULL, ecp);
    if (tbp->val.tbp->stp->recmapp == NULL) {
        RDB_handle_err(ecp, txp);
        goto error;
    }

    /* Create non-primary indexes */
    if (tbp->val.tbp->stp->indexc > 1) {
        ret = create_indexes(tbp, envp, ecp, txp);
        if (ret != RDB_OK)
            goto error;
    }

    RDB_free(finfov);
    RDB_free(cmpv);
    RDB_free(rmname);
    return RDB_OK;

error:
    /* clean up */
    if (finfov != NULL) {
        RDB_free(finfov);
    }
    RDB_free(cmpv);
    RDB_free(rmname);

    RDB_init_hashtable_iter(&hiter, &tbp->val.tbp->stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        RDB_free(entryp->key);
        RDB_free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&tbp->val.tbp->stp->attrmap);

    if (tbp->val.tbp->stp->recmapp != NULL && txp == NULL) {
        /*
         * Delete recmap if there is no transaction, otherwise this must happen
         * through the rollback
         * (If the transaction has been created under the control of a transaction,
         * the transaction must have been committed before the DB handle can be destroyed)
         */
        RDB_delete_recmap(tbp->val.tbp->stp->recmapp, NULL, ecp);
    }
    RDB_free(tbp->val.tbp->stp);
    tbp->val.tbp->stp = NULL;

    return RDB_ERROR;
}

int
RDB_provide_stored_table(RDB_object *tbp, RDB_bool create, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object rmnameobj;

    RDB_init_obj(&rmnameobj);

    if (RDB_table_is_persistent(tbp)) {
        if (txp == NULL || !RDB_tx_is_running(txp)) {
            RDB_raise_no_running_tx(ecp);
            return RDB_ERROR;
        }

        /*
         * Try to get the recmap name from the catalog.
         * If it is found, open it.
         */
        if (RDB_cat_recmap_name(tbp, &rmnameobj, ecp, txp) == RDB_OK) {
            if (RDB_open_stored_table(tbp, txp->envp, RDB_obj_string(&rmnameobj),
                        ecp, txp) != RDB_OK) {
                goto error;
            }
            RDB_destroy_obj(&rmnameobj, ecp);
            return RDB_OK;
        }
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
             goto error;
    }

    if (create) {
        if (RDB_create_stored_table(tbp, txp != NULL ? txp->envp : NULL,
                NULL, ecp, txp) != RDB_OK) {
            goto error;
        }
    }

    RDB_destroy_obj(&rmnameobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&rmnameobj, ecp);
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
RDB_open_stored_table(RDB_object *tbp, RDB_environment *envp,
        const char *rmname, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_field_info *finfov = NULL;
    RDB_hashtable_iter hiter;
    RDB_attrmap_entry *entryp;
    int attrc = tbp->typ->def.basetyp->def.tuple.attrc;
    int piattrc = tbp->val.tbp->keyv[0].strc;

    if (!RDB_table_is_persistent(tbp))
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbp->val.tbp->stp = RDB_alloc(sizeof(RDB_stored_table), ecp);
    if (tbp->val.tbp->stp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_table_is_persistent(tbp) && RDB_table_is_user(tbp)) {
        /* Get indexes from catalog */
        tbp->val.tbp->stp->indexc = RDB_cat_get_indexes(RDB_table_name(tbp), txp->dbp->dbrootp,
                ecp, txp, &tbp->val.tbp->stp->indexv);
        if (tbp->val.tbp->stp->indexc < 0) {
            goto error;
        }
    } else {
        tbp->val.tbp->stp->indexc = 0;
    }

    RDB_init_hashtable(&tbp->val.tbp->stp->attrmap, RDB_DFL_MAP_CAPACITY, &hash_str,
            &str_equals);

    ret = key_fnos(tbp, &finfov, NULL, NULL, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    tbp->val.tbp->stp->recmapp = RDB_open_recmap(rmname, RDB_DATAFILE, envp,
            attrc, finfov, piattrc, txp != NULL ? txp->tx : NULL, ecp);
    if (tbp->val.tbp->stp->recmapp == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_RESOURCE_NOT_FOUND_ERROR) {
            RDB_raise_not_found("table not found", ecp);
        } else {
            RDB_handle_err(ecp, txp);
        }
        goto error;
    }

    /*
     * Get estimated number of records
     */
    ret = RDB_recmap_est_size(tbp->val.tbp->stp->recmapp,
            txp != NULL ? txp->tx : NULL,
            &tbp->val.tbp->stp->est_cardinality, ecp);
    if (ret != RDB_OK) {
        RDB_handle_err(ecp, txp);
        goto error;
    }
    if (tbp->val.tbp->stp->est_cardinality == 0) {
        /*
         * Assume that the size has never been calculated
         * and set it to a default
         */
        tbp->val.tbp->stp->est_cardinality = 100;
    }

    /* Open secondary indexes */
    for (i = 0; i < tbp->val.tbp->stp->indexc; i++) {
        char *p = strchr(tbp->val.tbp->stp->indexv[i].name, '$');
        if (p == NULL || strcmp (p, "$0") != 0) {
            ret = RDB_open_tbindex(tbp, &tbp->val.tbp->stp->indexv[i], envp, ecp, txp);
            if (ret != RDB_OK)
                goto error;
        } else {
            tbp->val.tbp->stp->indexv[i].idxp = NULL;
        }
    }

    RDB_free(finfov);
    return RDB_OK;

error:
    /* clean up */
    RDB_free(finfov);

    RDB_init_hashtable_iter(&hiter, &tbp->val.tbp->stp->attrmap);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        RDB_free(entryp->key);
        RDB_free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&tbp->val.tbp->stp->attrmap);
    RDB_free(tbp->val.tbp->stp);
    tbp->val.tbp->stp = NULL;

    return RDB_ERROR;
}

int
RDB_open_tbindex(RDB_object *tbp, RDB_tbindex *indexp,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_compare_field *cmpv = NULL;
    int *fieldv = RDB_alloc(sizeof(int *) * indexp->attrc, ecp);

    if (fieldv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    if (indexp->ordered) {
        cmpv = cmpvec(tbp, indexp, ecp);
        if (cmpv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

    /* get index numbers */
    for (i = 0; i < indexp->attrc; i++) {
        fieldv[i] = *RDB_field_no(tbp->val.tbp->stp, indexp->attrv[i].attrname);
    }

    /* open index */
    indexp->idxp = RDB_open_index(tbp->val.tbp->stp->recmapp,
                  RDB_table_is_persistent(tbp) ? indexp->name : NULL,
                  RDB_table_is_persistent(tbp) ? RDB_DATAFILE : NULL,
                  envp, indexp->attrc, fieldv, cmpv, indexp->unique ? RDB_UNIQUE : 0,
                  txp != NULL ? txp->tx : NULL, ecp);
    if (indexp->idxp == NULL) {
        ret = RDB_ERROR;
    } else {
        ret = RDB_OK;
    }

cleanup:
    RDB_free(fieldv);
    RDB_free(cmpv);
    return ret;
}

int
RDB_delete_stored_table(RDB_stored_table *stp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;

    /* Schedule secondary indexes for deletion */
    for (i = 0; i < stp->indexc; i++) {
        if (stp->indexv[i].idxp != NULL) {
            if (txp != NULL) {
                ret = RDB_add_del_index(txp, stp->indexv[i].idxp, ecp);
                if (ret != RDB_OK)
                    return ret;
            } else {
                ret = RDB_delete_index(stp->indexv[i].idxp, NULL, NULL, ecp);
            }
        }
    }

    if (txp != NULL) {
        /*
         * Schedule recmap for deletion.
         * The recmap cannot be deleted immediately, because
         * this means closing the DB handle, which must not be closed
         * before transaction commit.
         */
        ret = RDB_add_del_recmap(txp, stp->recmapp, ecp);
    } else {
        ret = RDB_delete_recmap(stp->recmapp, NULL, ecp);
        if (ret != RDB_OK) {
            RDB_handle_err(ecp, txp);
            ret = RDB_ERROR;
        }
    }
    free_stored_table(stp);
    return ret;
}

/**
 * If txp is not NULL and the error is a deadlock, abort the transaction
 */
void
RDB_handle_err(RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_DEADLOCK_ERROR && txp != NULL) {
        RDB_rollback_all(ecp, txp);
    }
}
