/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "catalog.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

/* name of the file in which the tables are physically stored */
#define RDB_DATAFILE "rdata"

RDB_table *
_RDB_new_table(void)
{
    RDB_table *tbp = malloc(sizeof (RDB_table));
    if (tbp == NULL) {
        return NULL;
    }
    tbp->name = NULL;
    tbp->refcount = 0;
    tbp->optimized = RDB_FALSE;
    tbp->keyv = NULL;
    return tbp;
}

/*
 * Creates a stored table, but not the recmap and the indexes
 * and does not insert the table into the catalog.
 * reltyp is consumed on success (must not be freed by caller).
 */
static int
new_stored_table(const char *name, RDB_bool persistent,
                RDB_type *reltyp,
                int keyc, RDB_string_vec keyv[], RDB_bool usr,
                RDB_table **tbpp)
{
    int ret;
    int i, j;
    RDB_table *tbp = _RDB_new_table();

    if (tbp == NULL)
        return RDB_NO_MEMORY;
    *tbpp = tbp;
    tbp->is_user = usr;
    tbp->is_persistent = persistent;

    RDB_init_hashmap(&tbp->var.stored.attrmap, RDB_DFL_MAP_CAPACITY);

    tbp->kind = RDB_TB_STORED;
    if (name != NULL) {
        tbp->name = RDB_dup_str(name);
        if (tbp->name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }
    tbp->var.stored.recmapp = NULL;

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
        tbp->var.stored.indexv[i].idxp = NULL;
    }

    tbp->typ = reltyp;

    return RDB_OK;

error:
    /* clean up */
    if (tbp != NULL) {
        free(tbp->name);
        for (i = 0; i < keyc; i++) {
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

/*
 * Like _RDB_new_stored_table(), but uses attrc and heading instead of reltype.
 */
int
_RDB_new_stored_table(const char *name, RDB_bool persistent,
                int attrc, RDB_attr heading[],
                int keyc, RDB_string_vec keyv[], RDB_bool usr,
                RDB_table **tbpp)
{
    RDB_type *reltyp;
    int i;
    int ret = RDB_create_relation_type(attrc, heading, &reltyp);

    if (ret != RDB_OK) {
        return ret;
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

    ret = new_stored_table(name, persistent, reltyp,
            keyc, keyv, usr, tbpp);
    if (ret != RDB_OK)
        RDB_drop_type(reltyp, NULL);
    return ret;
}

static int
drop_anon_table(RDB_table *tbp)
{
    if (RDB_table_name(tbp) == NULL)
        return RDB_drop_table(tbp, NULL);
    return RDB_OK;
}

void
_RDB_free_table(RDB_table *tbp)
{
    int i;

    if (tbp->keyv != NULL) {
        /* Delete candidate keys */
        for (i = 0; i < tbp->keyc; i++) {
            RDB_free_strvec(tbp->keyv[i].strc, tbp->keyv[i].strv);
        }
        free(tbp->keyv);
    }

    RDB_drop_type(tbp->typ, NULL);
    free(tbp->name);
    free(tbp);
}

int
RDB_table_keys(RDB_table *tbp, RDB_string_vec **keyvp)
{
    int ret;

    if (tbp->keyv == NULL) {
        ret = _RDB_infer_keys(tbp);
        if (ret != RDB_OK)
            return ret;
    }

    if (keyvp != NULL)
        *keyvp = tbp->keyv;
        
    return tbp->keyc;
}

int
_RDB_drop_table(RDB_table *tbp, RDB_bool rec)
{
    int i, j;
    int ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
        {
            RDB_destroy_hashmap(&tbp->var.stored.attrmap);
            if (tbp->var.stored.indexc > 0) {
                for (i = 0; i < tbp->var.stored.indexc; i++) {
                    for (j = 0; j < tbp->var.stored.indexv[i].attrc; j++)
                        free(tbp->var.stored.indexv[i].attrv[j].attrname);
                    free(tbp->var.stored.indexv[i].attrv);
                }
                free(tbp->var.stored.indexv);
            }
            break;
        }
        case RDB_TB_SELECT_INDEX:
        case RDB_TB_SELECT:
            RDB_drop_expr(tbp->var.select.exp);
            if (rec) {
                ret = drop_anon_table(tbp->var.select.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_UNION:
            if (rec) {
                ret = drop_anon_table(tbp->var._union.tb1p);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var._union.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_MINUS:
            if (rec) {
                ret = drop_anon_table(tbp->var.minus.tb1p);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.minus.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_INTERSECT:
            if (rec) {
                ret = drop_anon_table(tbp->var.intersect.tb1p);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.intersect.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_JOIN:
            if (rec) {
                ret = drop_anon_table(tbp->var.join.tb1p);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.join.tb2p);
                if (ret != RDB_OK)
                    return ret;
            }
            free(tbp->var.join.common_attrv);
            break;
        case RDB_TB_EXTEND:
            if (rec) {
                ret = drop_anon_table(tbp->var.extend.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.extend.attrc; i++)
                free(tbp->var.extend.attrv[i].name);
            break;
        case RDB_TB_PROJECT:
            if (rec) {
                ret = drop_anon_table(tbp->var.project.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_RENAME:
            if (rec) {
                ret = drop_anon_table(tbp->var.rename.tbp);
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
                ret = drop_anon_table(tbp->var.summarize.tb1p);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.summarize.tb2p);
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
                ret = drop_anon_table(tbp->var.wrap.tbp);
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
                ret = drop_anon_table(tbp->var.unwrap.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_free_strvec(tbp->var.unwrap.attrc, tbp->var.unwrap.attrv);
            break;
        case RDB_TB_SDIVIDE:
            if (rec) {
                ret = drop_anon_table(tbp->var.sdivide.tb1p);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.sdivide.tb2p);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.sdivide.tb3p);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_GROUP:
            if (rec) {
                ret = drop_anon_table(tbp->var.group.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.group.attrc; i++) {
                free(tbp->var.group.attrv[i]);
            }
            free(tbp->var.group.gattr);
            break;
        case RDB_TB_UNGROUP:
            if (rec) {
                ret = drop_anon_table(tbp->var.ungroup.tbp);
                if (ret != RDB_OK)
                    return ret;
            }
            free(tbp->var.ungroup.attr);
            break;
    }
    _RDB_free_table(tbp);
    return RDB_OK;
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

/*
 * Create secondary indexes
 */
static int
create_key_indexes(RDB_table *tbp, RDB_environment *envp, RDB_transaction *txp)
{
    int i;
    int ret;

    /* Create secondary indexes */
    for (i = 1; i < tbp->var.stored.indexc; i++) {
        ret = create_index(tbp, envp, txp, &tbp->var.stored.indexv[i]);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
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
key_fnos(RDB_table *tbp, int **flenvp, RDB_bool ascv[], RDB_compare_field *cmpv)
{
    int ret;
    int i, di;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    RDB_attr *heading = tbp->typ->var.basetyp->var.tuple.attrv;
    int piattrc = tbp->keyv[0].strc;
    char **piattrv = tbp->keyv[0].strv;

    *flenvp = malloc(sizeof(int) * attrc);
    if (*flenvp == NULL) {
        return RDB_NO_MEMORY;
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
 * ascv       the sort order of the primary index, or NULL if unordered
 */
int
_RDB_create_table_storage(RDB_table *tbp, RDB_environment *envp, RDB_bool ascv[],
           RDB_transaction *txp)
{
    int ret;
    int *flenv;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    int piattrc = tbp->keyv[0].strc;

    if (!tbp->is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /* Allocate comparison vector, if needed */
    if (ascv != NULL) {
        cmpv = malloc(sizeof (RDB_compare_field) * piattrc);
        if (cmpv == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }

    ret = key_fnos(tbp, &flenv, ascv, cmpv);
    if (ret != RDB_OK)
        return ret;

    /*
     * Use a sorted recmap for local tables, so the order of the tuples
     * is always the same if the table is stored as an attribute in a table.
     */
    if (ascv != NULL || !tbp->is_persistent)
        ret = RDB_create_sorted_recmap(tbp->is_persistent ? tbp->name : NULL,
                tbp->is_persistent ? RDB_DATAFILE : NULL,
                envp, attrc, flenv, piattrc, ascv != NULL ? cmpv : NULL,
                (RDB_bool) (ascv != NULL), txp != NULL ? txp->txid : NULL,
                &tbp->var.stored.recmapp);
    else {
        ret = RDB_create_recmap(tbp->is_persistent ? tbp->name : NULL,
                tbp->is_persistent ? RDB_DATAFILE : NULL,
                envp, attrc, flenv, piattrc, txp != NULL ? txp->txid : NULL,
                &tbp->var.stored.recmapp);
    }
    if (ret != RDB_OK)
        goto error;

    /* Open/create indexes if there is more than one key */
    ret = create_key_indexes(tbp, envp, txp);
    if (ret != RDB_OK)
        goto error;

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

/*
 * Open the physical representation of a table.
 * (The recmap and the indexes)
 *
 * Arguments:
 * tbp        the table
 * envp       the database environment
 * txp        the transaction under which the operation is performed
 */
int
_RDB_open_table_storage(RDB_table *tbp, RDB_environment *envp,
           RDB_transaction *txp)
{
    int ret;
    int *flenv;
    RDB_compare_field *cmpv = NULL;
    int attrc = tbp->typ->var.basetyp->var.tuple.attrc;
    int piattrc = tbp->keyv[0].strc;

    if (!tbp->is_persistent)
       txp = NULL;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = key_fnos(tbp, &flenv, NULL, NULL);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_open_recmap(tbp->name, RDB_DATAFILE, envp,
            attrc, flenv, piattrc, txp != NULL ? txp->txid : NULL,
            &tbp->var.stored.recmapp);
    if (ret != RDB_OK)
        goto error;

    /* Flag that the indexes have not been opened */
    tbp->var.stored.indexc = -1;

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

int
RDB_create_table_index(const char *name, RDB_table *tbp, int idxcompc,
        RDB_seq_item idxcompv[], int flags, RDB_transaction *txp)
{
    int i;
    int ret;
    _RDB_tbindex *indexp;
    RDB_transaction tx;

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

    indexp->attrc = idxcompc;
    indexp->attrv = malloc(sizeof (RDB_seq_item) * idxcompc);
    if (indexp->attrv == NULL) {
        free(indexp->name);
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

    ret = RDB_begin_tx(&tx, RDB_tx_db(txp), txp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }

    /* Create index in catalog */
    ret = _RDB_cat_insert_index(indexp, tbp->name, &tx);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(&tx);
        return ret;
    }

    /* Create index */
    ret = create_index(tbp, RDB_db_env(RDB_tx_db(txp)), &tx, indexp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(&tx);
        return ret;
    }

    return RDB_commit(&tx);
}

int
RDB_drop_table_index(const char *name, RDB_transaction *txp)
{
    return RDB_NOT_SUPPORTED;
}

RDB_type *
RDB_table_type(const RDB_table *tbp)
{
    return tbp->typ;
}

int
_RDB_move_tuples(RDB_table *dstp, RDB_table *srcp, RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    RDB_object tpl;
    int ret;

    /*
     * Copy all tuples from source table to destination table
     */
    ret = _RDB_table_qresult(srcp, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (dstp->kind == RDB_TB_STORED && !dstp->is_persistent)
            ret = RDB_insert(dstp, &tpl, NULL);
        else
            ret = RDB_insert(dstp, &tpl, txp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
    }
    if (ret == RDB_NOT_FOUND)
        ret = RDB_OK;
cleanup:
    _RDB_drop_qresult(qrp, txp);
    RDB_destroy_obj(&tpl);
    return ret;
}

int
RDB_copy_table(RDB_table *dstp, RDB_table *srcp, RDB_transaction *txp)
{
    RDB_transaction tx;
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    /* check if types of the two tables match */
    if (!RDB_type_equals(dstp->typ, srcp->typ))
        return RDB_TYPE_MISMATCH;

    /* start subtransaction */
    ret = RDB_begin_tx(&tx, txp->dbp, txp);
    if (ret != RDB_OK)
        return ret;

    /* Delete all tuples from destination table */
    ret = RDB_delete(dstp, NULL, &tx);
    if (ret != RDB_OK)
        goto error;

    ret = _RDB_move_tuples(dstp, srcp, &tx);
    if (ret != RDB_OK)
        goto error;

    return RDB_commit(&tx);

error:
    RDB_rollback(&tx);
    return ret;
}

int
RDB_all(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    /* initialize result */
    *resultp = RDB_TRUE;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (!RDB_tuple_get_bool(&tpl, attrname))
            *resultp = RDB_FALSE;
    }

    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_any(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    /* initialize result */
    *resultp = RDB_FALSE;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (RDB_tuple_get_bool(&tpl, attrname))
            *resultp = RDB_TRUE;
    }

    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_max(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = RDB_INT_MIN;
    else if (attrtyp == &RDB_RATIONAL)
        resultp->var.rational_val = RDB_RATIONAL_MIN;
    else
        return RDB_TYPE_MISMATCH;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(&tpl, attrname);
             
            if (val > resultp->var.int_val)
                 resultp->var.int_val = val;
        } else {
            RDB_rational val = RDB_tuple_get_rational(&tpl, attrname);
             
            if (val > resultp->var.rational_val)
                resultp->var.rational_val = val;
        }
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_min(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = RDB_INT_MAX;
    else if (attrtyp == &RDB_RATIONAL)
        resultp->var.rational_val = RDB_RATIONAL_MAX;
    else
        return RDB_TYPE_MISMATCH;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(&tpl, attrname);
             
            if (val < resultp->var.int_val)
                 resultp->var.int_val = val;
        } else {
            RDB_rational val = RDB_tuple_get_rational(&tpl, attrname);
             
            if (val < resultp->var.rational_val)
                resultp->var.rational_val = val;
        }
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_sum(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    /* initialize result */
    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = 0;
    else if (attrtyp == &RDB_RATIONAL)
        resultp->var.rational_val = 0.0;
    else
       return RDB_TYPE_MISMATCH;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        if (attrtyp == &RDB_INTEGER)
            resultp->var.int_val += RDB_tuple_get_int(&tpl, attrname);
        else
            resultp->var.rational_val
                            += RDB_tuple_get_rational(&tpl, attrname);
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_avg(RDB_table *tbp, const char *attrname, RDB_transaction *txp,
        RDB_rational *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;
    int count;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1)
            return RDB_INVALID_ARGUMENT;
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    if (attrname != NULL) {
        attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
        if (attrtyp == NULL)
            return RDB_INVALID_ARGUMENT;
    }

    if (!RDB_type_is_numeric(attrtyp))
        return RDB_TYPE_MISMATCH;
    count = 0;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&tpl);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        count++;
        if (attrtyp == &RDB_INTEGER)
            *resultp += RDB_tuple_get_int(&tpl, attrname);
        else
            *resultp += RDB_tuple_get_rational(&tpl, attrname);
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    if (count == 0)
        return RDB_AGGREGATE_UNDEFINED;
    *resultp /= count;

    _RDB_drop_qresult(qrp, txp);
    return RDB_OK;
}

int
RDB_extract_tuple(RDB_table *tbp, RDB_transaction *txp, RDB_object *tplp)
{
    int ret, ret2;
    RDB_qresult *qrp;
    RDB_object tpl;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    RDB_init_obj(&tpl);

    /* Get tuple */
    ret = _RDB_next_tuple(qrp, tplp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Check if there are more tuples */
    ret = _RDB_next_tuple(qrp, &tpl, txp);
    if (ret != RDB_NOT_FOUND) {
        if (ret == RDB_OK)
            ret = RDB_INVALID_ARGUMENT;
        goto cleanup;
    }

    ret = RDB_OK;

cleanup:
    RDB_destroy_obj(&tpl);

    ret2 = _RDB_drop_qresult(qrp, txp);
    if (ret == RDB_OK)
        ret = ret2;
    if (RDB_is_syserr(ret) || RDB_is_syserr(ret2)) {
        RDB_rollback_all(txp);
    }
    return ret;
}

int
RDB_table_is_empty(RDB_table *tbp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    RDB_init_obj(&tpl);

    ret = _RDB_next_tuple(qrp, &tpl, txp);
    if (ret == RDB_OK)
        *resultp = RDB_FALSE;
    else if (ret == RDB_NOT_FOUND)
        *resultp = RDB_TRUE;
    else {
         RDB_destroy_obj(&tpl);
        _RDB_drop_qresult(qrp, txp);
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        return ret;
    }
    RDB_destroy_obj(&tpl);
    return _RDB_drop_qresult(qrp, txp);
}

int
RDB_cardinality(RDB_table *tbp, RDB_transaction *txp)
{
    int ret;
    int count;
    RDB_qresult *qrp;
    RDB_object tpl;

    if (txp != NULL && !RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    RDB_init_obj(&tpl);

    count = 0;
    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        count++;
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, txp);
        goto error;
    }

    ret = _RDB_drop_qresult(qrp, txp);
    if (ret != RDB_OK)
        goto error;

    if (tbp->kind == RDB_TB_STORED)
        tbp->var.stored.est_cardinality = count;

    return count;

error:
    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}

int
RDB_subset(RDB_table *tb1p, RDB_table *tb2p, RDB_transaction *txp,
           RDB_bool *resultp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    if (!RDB_type_equals(tb1p->typ, tb2p->typ))
        return RDB_TYPE_MISMATCH;

    ret = _RDB_table_qresult(tb1p, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        return ret;
    }

    RDB_init_obj(&tpl);

    *resultp = RDB_TRUE;
    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, txp);
        if (ret == RDB_NOT_FOUND) {
            *resultp = RDB_FALSE;
            break;
        }
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret)) {
                RDB_rollback_all(txp);
            }
            RDB_destroy_obj(&tpl);
            _RDB_drop_qresult(qrp, txp);
            goto error;
        }
    }

    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND && ret != RDB_OK) {
        _RDB_drop_qresult(qrp, txp);
        goto error;
    }
    ret = _RDB_drop_qresult(qrp, txp);
    if (ret != RDB_OK)
        goto error;
    return RDB_OK;

error:
    if (RDB_is_syserr(ret))
        RDB_rollback_all(txp);
    return ret;
}

int
RDB_table_equals(RDB_table *tb1p, RDB_table *tb2p, RDB_transaction *txp,
        RDB_bool *resp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;
    int cnt = RDB_cardinality(tb1p, txp);

    /*
     * Check if both tables have same cardinality
     */
    if (cnt < 0)
        return cnt;
    ret =  RDB_cardinality(tb2p, txp);
    if (ret < 0)
        return ret;
    if (ret != cnt) {
        *resp = RDB_FALSE;
        return RDB_OK;
    }

    /*
     * Check if all tuples from table #1 are in table #2
     * (The implementation is quite inefficient if table #2
     * is a SUMMARIZE PER or GROUP table)
     */
    ret = _RDB_table_qresult(tb1p, txp, &qrp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);
    while ((ret = _RDB_next_tuple(qrp, &tpl, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, txp);
        if (ret == RDB_NOT_FOUND) {
            *resp = RDB_FALSE;
            RDB_destroy_obj(&tpl);
            return _RDB_drop_qresult(qrp, txp);
        } else if (ret != RDB_OK) {
            goto error;
        }
    }

    *resp = RDB_TRUE;
    RDB_destroy_obj(&tpl);
    return _RDB_drop_qresult(qrp, txp);

error:
    RDB_destroy_obj(&tpl);
    _RDB_drop_qresult(qrp, txp);
    return ret;
}
