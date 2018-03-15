/*
 * Record map functions implemented using Berkeley DB
 * 
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "bdbrecmap.h"
#include <rec/recmapimpl.h>
#include <rec/envimpl.h>
#include <rec/dbdefs.h>
#include <obj/excontext.h>
#include <gen/strfns.h>
#include <bdbrec/bdbcursor.h>

#include <stdlib.h>
#include <string.h>
#include <errno.h>

enum {
    RECLEN_SIZE = 4
};

/*
 * Allocate a RDB_recmap structure and initialize its fields.
 * The underlying BDB database is created using db_create(), but not opened.
 */
static RDB_recmap *
new_recmap(const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
        int keyfieldc, int flags, RDB_exec_context *ecp)
{
    int ret;
    RDB_recmap *rmp = RDB_new_recmap(namp, filenamp, envp, fieldc, fieldinfov,
            keyfieldc, flags, ecp);
    if (rmp == NULL)
        return NULL;

    rmp->close_recmap_fn = RDB_close_bdb_recmap;
    rmp->delete_recmap_fn = &RDB_delete_bdb_recmap;
    rmp->insert_rec_fn = &RDB_insert_bdb_rec;
    rmp->update_rec_fn = &RDB_update_bdb_rec;
    rmp->delete_rec_fn = &RDB_delete_bdb_rec;
    rmp->get_fields_fn = &RDB_get_bdb_fields;
    rmp->contains_rec_fn = &RDB_contains_bdb_rec;
    rmp->recmap_est_size_fn = &RDB_bdb_recmap_est_size;
    rmp->cursor_fn = &RDB_bdb_recmap_cursor;

    ret = db_create(&rmp->dbp, envp != NULL ? envp->env.envp : NULL, 0);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        goto error;
    }

    return rmp;

error:
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldinfos);
    free(rmp);
    return NULL;
}

/*
 * Comparison function for b-trees.
 * Compares records by comparing the fields.
 */
static int
compare_key(DB *dbp, const DBT *dbt1p, const DBT *dbt2p, size_t *locp)
{
    int i;
    int res;
    RDB_recmap *rmp = (RDB_recmap *) dbp->app_private;

    for (i = 0; i < rmp->cmpc; i++) {
        int offs1, offs2;
        size_t len1, len2;
        void *data1p, *data2p;

        offs1 = RDB_get_field(rmp, rmp->cmpv[i].fno, dbt1p->data, dbt1p->size, &len1, NULL);
        if (offs1 < 0)
            return offs1;
        offs2 = RDB_get_field(rmp, rmp->cmpv[i].fno, dbt2p->data, dbt2p->size, &len2, NULL);
        if (offs2 < 0)
            return offs2;
        data1p = ((RDB_byte *) dbt1p->data) + offs1;
        data2p = ((RDB_byte *) dbt2p->data) + offs2;

        /* Compare field */
        res = (*rmp->cmpv[i].comparep)(data1p, len1, data2p, len2,
                rmp->envp, rmp->cmpv[i].arg);

        /* If the fields are different, we're done */
        if (res != 0) {
            /* If order is descending, revert result */
            if (!rmp->cmpv[i].asc)
                res = -res;
            return res;
        }
    }
    if (rmp->cmpc == rmp->fieldcount)
        return 0;

    res = memcmp(dbt1p->data, dbt2p->data, dbt1p->size <= dbt2p->size ? dbt1p->size : dbt2p->size);
    if (res != 0)
        return res;

    return abs(dbt1p->size - dbt2p->size);
}

RDB_recmap *
RDB_create_bdb_recmap(const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
        int keyfieldc, int cmpc, const RDB_compare_field cmpv[], int flags,
        int keyc, const RDB_string_vec *keyv,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_recmap *rmp;
    int i;
    int ret;

    /* Allocate and initialize RDB_recmap structure */

    rmp = new_recmap(name, filename, envp,
            fieldc, fieldinfov, keyfieldc, flags, ecp);
    if (rmp == NULL) {
        return NULL;
    }

    if (cmpc > 0) {
        rmp->cmpc = cmpc;
        rmp->cmpv = malloc(sizeof (RDB_compare_field) * cmpc);
        if (rmp->cmpv == NULL) {
            ret = ENOMEM;
            goto error;
        }
        for (i = 0; i < cmpc; i++) {
            rmp->cmpv[i].fno = cmpv[i].fno;
            rmp->cmpv[i].comparep = cmpv[i].comparep;
            rmp->cmpv[i].arg = cmpv[i].arg;
            rmp->cmpv[i].asc = cmpv[i].asc;
        }

        /* Set comparison function */
        rmp->dbp->app_private = rmp;
        rmp->dbp->set_bt_compare(rmp->dbp, &compare_key);
    }

    if (!(RDB_UNIQUE & flags)) {
        /* Allow duplicate keys */
        rmp->dbp->set_flags(rmp->dbp, DB_DUPSORT);
    }

    if (envp == NULL) {
        ret = rmp->dbp->set_alloc(rmp->dbp, malloc, realloc, free);
        if (ret != 0) {
            goto error;
        }
    }

    /* Suppress error output */
    rmp->dbp->set_errfile(rmp->dbp, NULL);

    /* Create BDB database */
    ret = rmp->dbp->open(rmp->dbp, (DB_TXN *) rtxp, filename, name,
            RDB_ORDERED & flags ? DB_BTREE : DB_HASH,
            DB_CREATE | DB_EXCL, 0664);
    if (ret == EEXIST && envp != NULL) {
        /* BDB database exists - remove it and try again */
        envp->env.envp->dbremove(envp->env.envp, (DB_TXN *) rtxp, filename, name, 0);
        ret = rmp->dbp->open(rmp->dbp, (DB_TXN *) rtxp, filename, name,
                RDB_ORDERED & flags ? DB_BTREE : DB_HASH,
                DB_CREATE | DB_EXCL, 0664);
    }
    if (ret != 0) {
        goto error;
    }

    return rmp;

error:
    RDB_close_recmap(rmp, ecp);
    RDB_errcode_to_error(ret, ecp);
    return NULL;
}

RDB_recmap *
RDB_open_bdb_recmap(const char *name, const char *filename,
       RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[], int keyfieldc,
       RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_recmap *rmp;
    int ret;
    rmp = new_recmap(name, filename, envp, fieldc, fieldinfov, keyfieldc,
            RDB_UNIQUE, ecp);
    if (rmp == NULL) {
        return NULL;
    }

    if (envp == NULL) {
        ret = rmp->dbp->set_alloc(rmp->dbp, malloc, realloc, free);
        if (ret != 0) {
            goto error;
        }
    }

    /* Open database */
    ret = rmp->dbp->open(rmp->dbp, (DB_TXN *) rtxp, filename, name, DB_UNKNOWN,
            0, 0664);
    if (ret != 0) {
        goto error;
    }
    return rmp;

error:
RDB_close_recmap(rmp, ecp);
    RDB_errcode_to_error(ret, ecp);
    return NULL;
}

int
RDB_close_bdb_recmap(RDB_recmap *rmp, RDB_exec_context *ecp)
{
    int ret = rmp->dbp->close(rmp->dbp, 0);
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldinfos);
    free(rmp->cmpv);
    free(rmp);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_delete_bdb_recmap(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    /* The DB handle must be closed before calling DB_ENV->dbremove() */
    int ret = rmp->dbp->close(rmp->dbp, DB_NOSYNC);
    if (ret != 0)
        goto cleanup;

    /* Call DB_ENV->dbremove() only if the recmap is persistent */
    if (rmp->envp != NULL && rmp->namp != NULL) {
        if (rmp->envp->trace > 0) {
            fprintf(stderr, "deleting recmap %s\n", rmp->namp);
        }
        ret = rmp->envp->env.envp->dbremove(rmp->envp->env.envp, (DB_TXN *) rtxp,
                rmp->filenamp, rmp->namp, 0);
    }

cleanup:
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldinfos);
    free(rmp->cmpv);
    free(rmp);

    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static size_t
get_len(const RDB_byte *databp)
{
    size_t len;

    len = databp[0];
    len += databp[1] << 8;
    len += databp[2] << 16;
    len += databp[3] << 24;

    return len;
}

size_t
RDB_get_vflen(RDB_byte *databp, size_t len, int vfcnt, int vpos)
{
    return get_len(&databp[len - vfcnt * RECLEN_SIZE + vpos * RECLEN_SIZE]);
}

/*
 *
 * 
 * vposp	for variable-length fields: location for position in the
 *              var-len field table
 */
int
RDB_get_field(RDB_recmap *rmp, int fno, void *datap, size_t len, size_t *lenp,
              int *vposp)
{
    int i, vpos;
    int offs = 0;
    RDB_byte *databp = (RDB_byte *) datap;

    if (fno < rmp->keyfieldcount) {
        /*
         * compute offset and length for key
         */
        if (rmp->fieldinfos[fno].len != RDB_VARIABLE_LEN) {
            /* offset is sum of lengths of previous fields */
            for (i = 0; i < fno; i++) {
                if (rmp->fieldinfos[i].len != RDB_VARIABLE_LEN) {
                    offs += rmp->fieldinfos[i].len;
                }
            }

            *lenp = (size_t) rmp->fieldinfos[fno].len;
        } else {
            /* offset is sum of lengths of fixed-length fields
             * plus lengths of previous variable-length fields 
             */
             
            /* add fixed-length fields */
            for (i = 0; i < rmp->keyfieldcount; i++) {
                if (rmp->fieldinfos[i].len != RDB_VARIABLE_LEN) {
                    offs += rmp->fieldinfos[i].len;
                }
            }
            
            /* compute position within var-length fields */
            vpos = 0;
            for (i = 0; i < fno; i++) {
                if (rmp->fieldinfos[i].len == RDB_VARIABLE_LEN) {
                    vpos++;
                }
            }

            /* add previous variable-length fields */
            for (i = 0; i < vpos; i++) {
                offs += RDB_get_vflen(databp, len, rmp->varkeyfieldcount, i);
            }
            
            *lenp = RDB_get_vflen(databp, len, rmp->varkeyfieldcount, vpos);
        }
    } else {
        /*
         * compute offset and length for data
         */
        if (rmp->fieldinfos[fno].len != RDB_VARIABLE_LEN) {
            /* offset is sum of lengths of previous fields */
            for (i = rmp->keyfieldcount; i < fno; i++) {
                if (rmp->fieldinfos[i].len != RDB_VARIABLE_LEN) {
                    offs += rmp->fieldinfos[i].len;
                }
            }

            *lenp = (size_t) rmp->fieldinfos[fno].len;
        } else {
            /* offset is sum of lengths of fixed-length fields
             * plus lengths of previous variable-length fields 
             */
             
            /* add fixed-length fields */
            for (i = rmp->keyfieldcount; i < rmp->fieldcount; i++) {
                if (rmp->fieldinfos[i].len != RDB_VARIABLE_LEN) {
                    offs += rmp->fieldinfos[i].len;
                }
            }
            
            /* compute position within var-length fields */
            vpos = 0;
            for (i = rmp->keyfieldcount; i < fno; i++) {
                if (rmp->fieldinfos[i].len == RDB_VARIABLE_LEN) {
                    vpos++;
                }
            }

            /* add previous variable-length fields */
            for (i = 0; i < vpos; i++) {
                offs += RDB_get_vflen(databp, len, rmp->vardatafieldcount, i);
            }
            
            *lenp = RDB_get_vflen(databp, len, rmp->vardatafieldcount, vpos);
        }
    }
    /* Integrity check */
    if (*lenp > len)
        return RDB_RECORD_CORRUPTED;
    if (vposp != NULL)
        *vposp = vpos;
    return offs;
}

static void
set_len(RDB_byte *databp, size_t len)
{
    databp[0] = (RDB_byte)(len & 0xff);
    databp[1] = (RDB_byte)(len >> 8);
    databp[2] = (RDB_byte)(len >> 16);
    databp[3] = (RDB_byte)(len >> 24);
}

int
RDB_fields_to_DBT(RDB_recmap *rmp, int fldc, const RDB_field fldv[],
                   DBT *dbtp)
{
    RDB_byte *databp;
    int vfldc;
    int fi;
    int vfi;
    int offs;
    int i;
    int ret;
    int *fno = NULL;	/* fixed field no in DBT -> field no */
    int *vfno = NULL;	/* var field no in DBT -> field no */

    memset(dbtp, 0, sizeof(DBT));

    /* calculate key and data length, and # of variable size fields */
    dbtp->size = 0;
    vfldc = 0;
    for (i = 0; i < fldc; i++) {
        dbtp->size += fldv[i].len;
        if (rmp->fieldinfos[fldv[i].no].len == RDB_VARIABLE_LEN) {
            /* RECLEN_SIZE bytes extra for length */
            dbtp->size += RECLEN_SIZE;

            vfldc++;
        }
    }

    if (dbtp->size > 0) {  
        dbtp->data = malloc(dbtp->size);
        if (dbtp->data == NULL) {
            ret = ENOMEM;
            goto error;
        }
    } else {
        dbtp->data = NULL;
    }
    if (fldc - vfldc > 0) {
        fno = malloc((fldc - vfldc) * sizeof(int));
        if (fno == NULL) {
            ret = ENOMEM;
            goto error;
        }
    }
    if (vfldc > 0) {
        vfno = malloc(vfldc *  sizeof(int));
        if (vfno == NULL) {
            ret = ENOMEM;
            goto error;
        }
    }

    vfi = fi = 0;   
    for (i = 0; i < fldc; i++) {
        if (rmp->fieldinfos[fldv[i].no].len == RDB_VARIABLE_LEN) {
            vfno[vfi++] = i;
        } else {
            fno[fi++] = i;
        }
    }

    /*
     * Fill DBT
     */

    offs = 0;
    databp = dbtp->data;

    /* fixed-length fields */
    for (i = 0; i < fldc - vfldc; i++) {
        int fn = fno[i];
        (*fldv[fn].copyfp)(databp + offs, fldv[fn].datap, fldv[fn].len);
        offs += fldv[fn].len;
    }
    /* variable-length fields */
    for (i = 0; i < vfldc; i++) {
        int fn = vfno[i];
        (*fldv[fn].copyfp)(databp + offs, fldv[fn].datap, fldv[fn].len);
        offs += fldv[fn].len;

        /* field length */
        set_len(&databp[dbtp->size - vfldc * RECLEN_SIZE + i * RECLEN_SIZE],
                fldv[fn].len);
    }

    free(fno);
    free(vfno);
    return RDB_OK;
    
error:
    free(dbtp->data);
    free(fno);
    free(vfno);

    return ret;
}

/*
 * Convert the field data in flds to key DBT.
 */
static int
key_to_DBT(RDB_recmap *rmp, RDB_field fldv[], DBT *keyp)
{
    int i;

    for (i = 0; i < rmp->keyfieldcount; i++) {
        fldv[i].no = i;
    }

    return RDB_fields_to_DBT(rmp, rmp->keyfieldcount, fldv, keyp);
}

/*
 * Convert the field data in flds to data DBT.
 */
static int
data_to_DBT(RDB_recmap *rmp, RDB_field fldv[], DBT *datap)
{
    int i;

    for (i = rmp->keyfieldcount; i < rmp->fieldcount; i++) {
        fldv[i].no = i;
    }

    return RDB_fields_to_DBT(rmp, rmp->fieldcount - rmp->keyfieldcount,
                             fldv + rmp->keyfieldcount, datap);
}

int
RDB_insert_bdb_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    DBT key, data;
    int ret;

    ret = key_to_DBT(rmp, flds, &key);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    ret = data_to_DBT(rmp, flds, &data);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    ret = rmp->dbp->put(rmp->dbp, (DB_TXN *) rtxp, &key, &data,
            rmp->dup_keys ? 0 : DB_NOOVERWRITE);
    if (ret == EINVAL) {
        /* Assume duplicate secondary index */
        RDB_raise_key_violation("", ecp);
        free(key.data);
        free(data.data);
        return RDB_ERROR;
    }
    free(key.data);
    free(data.data);

    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_set_field(RDB_recmap *recmapp, DBT *recpartp, const RDB_field *fieldp, 
               int varfieldc)
{
    size_t oldlen;
    int vpos;
    RDB_byte *databp = (RDB_byte *) recpartp->data;
    int offs = RDB_get_field(recmapp, fieldp->no,
                    recpartp->data, recpartp->size, &oldlen, &vpos);
    if (offs < 0)
        return offs;

    if (oldlen != fieldp->len) {
        /*
         * change field length
         */

        /* If the field shrinks, move the data following the field before realloc */
        if (fieldp->len < oldlen)
            memmove(databp + offs + fieldp->len,
                databp + offs + oldlen,
                recpartp->size - offs - oldlen);

        /* change memory block size */
        recpartp->data = realloc(recpartp->data,
                                 recpartp->size + fieldp->len - oldlen);
        databp = (RDB_byte *) recpartp->data;

        /* If the field grows, move the data following the field after realloc */
        if (fieldp->len > oldlen)
            memmove(databp + offs + fieldp->len,
                    databp + offs + oldlen,
                    recpartp->size - offs - oldlen);
        recpartp->size = recpartp->size + fieldp->len - oldlen;
        set_len(&databp[recpartp->size - varfieldc * RECLEN_SIZE
                        + vpos * RECLEN_SIZE], fieldp->len);
    }
    /* copy data into field */
    (*(fieldp->copyfp))(databp + offs, fieldp->datap, fieldp->len);
    return RDB_OK;
}

int
RDB_update_DBT_rec(RDB_recmap *rmp, DBT *keyp, DBT *datap,
        int fieldc, const RDB_field fieldv[], DB_TXN *txid)
{
    int i;
    int ret;
    RDB_bool del = RDB_FALSE;

    /* Check if the key is to be modified */
    for (i = 0; i < fieldc; i++) {
        if (fieldv[i].no < rmp->keyfieldcount) {
            /* Key is to be modified, so delete record first */
            ret = rmp->dbp->del(rmp->dbp, txid, keyp, 0);
            if (ret != 0) {
                return ret;
            }
            del = RDB_TRUE;
            break;
        }
    }

    for (i = 0; i < fieldc; i++) {
        if (fieldv[i].no < rmp->keyfieldcount) {
            ret = RDB_set_field(rmp, keyp, &fieldv[i],
                           rmp->varkeyfieldcount);
        } else {
            ret = RDB_set_field(rmp, datap, &fieldv[i],
                           rmp->vardatafieldcount);
        }
        if (ret != RDB_OK)
            return ret;
    }

    /* Write record back */
    ret = rmp->dbp->put(rmp->dbp, txid, keyp, datap,
            del ? DB_NOOVERWRITE : 0);
    if (ret == EINVAL) {
        /* Assume duplicate secondary index */
        return DB_KEYEXIST;
    }
    if (ret == DB_KEYEXIST) {
        /* Possible key violation - check if the record already exists */
        ret = rmp->dbp->get(rmp->dbp, txid, keyp, datap, DB_GET_BOTH);
        if (ret == 0)
            return RDB_ELEMENT_EXISTS;
        if (ret == DB_NOTFOUND)
            return DB_KEYEXIST;
    }
    return ret;
}

int
RDB_update_bdb_rec(RDB_recmap *rmp, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], RDB_rec_transaction *rtxp,
               RDB_exec_context *ecp)
{
    DBT key, data;
    int ret;

    ret = key_to_DBT(rmp, keyv, &key);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    memset(&data, 0, sizeof (data));
    data.flags = DB_DBT_REALLOC;

    ret = rmp->dbp->get(rmp->dbp, (DB_TXN *) rtxp, &key, &data, 0);
    if (ret != 0)
        goto cleanup;

    ret = RDB_update_DBT_rec(rmp, &key, &data, fieldc, fieldv, (DB_TXN *) rtxp);

cleanup:
    free(key.data);
    free(data.data);

    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_delete_bdb_rec(RDB_recmap *rmp, int fieldc, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    DBT key;
    int ret;

    if (fieldc != rmp->keyfieldcount) {
        RDB_raise_not_supported("only key fields are supported", ecp);
        return RDB_ERROR;
    }
    ret = key_to_DBT(rmp, keyv, &key);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    ret = rmp->dbp->del(rmp->dbp, (DB_TXN *) rtxp, &key, 0);
    free(key.data);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_get_DBT_fields(RDB_recmap *rmp, const DBT *keyp, const DBT *datap, int fieldc,
           RDB_field retfieldv[])
{
    int i;

    for (i = 0; i < fieldc; i++) {
        int offs;

        if (retfieldv[i].no < rmp->keyfieldcount) {
            offs = RDB_get_field(rmp, retfieldv[i].no,
                    keyp->data, keyp->size, &retfieldv[i].len, NULL);
            if (offs < 0)
                return offs;
            retfieldv[i].datap = ((RDB_byte *)keyp->data) + offs;
        } else {
            offs = RDB_get_field(rmp, retfieldv[i].no,
                    datap->data, datap->size, &retfieldv[i].len, NULL);
            if (offs < 0)
                return offs;
            retfieldv[i].datap = ((RDB_byte *)datap->data) + offs;
        }
    }

    return RDB_OK;
}

int
RDB_get_bdb_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    DBT key, data;
    int ret;

    ret = key_to_DBT(rmp, keyv, &key);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    memset(&data, 0, sizeof (data));

    ret = rmp->dbp->get(rmp->dbp, (DB_TXN *) rtxp, &key, &data, 0);
    if (ret != 0) {
        free(key.data);
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    ret = RDB_get_DBT_fields(rmp, &key, &data, fieldc, retfieldv);
    free(key.data);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_contains_bdb_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    DBT key, data;
    int ret;

    ret = key_to_DBT(rmp, flds, &key);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    ret = data_to_DBT(rmp, flds, &data);
    if (ret != RDB_OK) {
        free(key.data);
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    key.flags = DB_DBT_REALLOC;
    data.flags = DB_DBT_REALLOC;

    ret = rmp->dbp->get(rmp->dbp, (DB_TXN *) rtxp, &key, &data, DB_GET_BOTH);
    free(key.data);
    free(data.data);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_recmap_est_size(RDB_recmap *rmp, RDB_rec_transaction *rtxp, unsigned *sz,
        RDB_exec_context *ecp)
{
    DBTYPE dbtype;
    int ret = rmp->dbp->get_type(rmp->dbp, &dbtype);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    if (dbtype == DB_BTREE) {
        DB_BTREE_STAT *btstatp;

        ret = rmp->dbp->stat(rmp->dbp, (DB_TXN *) rtxp, &btstatp, DB_FAST_STAT);
        if (ret != 0) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }

        *sz = (unsigned) btstatp->bt_ndata;
        free(btstatp);
    } else {
        /* Hashtable */
        DB_HASH_STAT *hstatp;

        ret = rmp->dbp->stat(rmp->dbp, (DB_TXN *) rtxp, &hstatp, DB_FAST_STAT);
        if (ret != 0) {
            RDB_errcode_to_error(ret, ecp);
            return RDB_ERROR;
        }

        *sz = (unsigned) hstatp->hash_ndata;
        free(hstatp);
    }
    return RDB_OK;
}
