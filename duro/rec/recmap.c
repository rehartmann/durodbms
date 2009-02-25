/*
 * $Id$
 * 
 * Copyright (C) 2003-2009 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "recmap.h"
#include <gen/strfns.h>
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
static int
new_recmap(RDB_recmap **rmpp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const int fieldlenv[], int keyfieldc,
        int flags)
{
    int i, ret;
    RDB_recmap *rmp = malloc(sizeof(RDB_recmap));

    if (rmp == NULL)
        return ENOMEM;

    rmp->envp = envp;
    rmp->filenamp = NULL;
    rmp->fieldlens = NULL;
    if (namp != NULL) {
        rmp->namp = RDB_dup_str(namp);
        if (rmp->namp == NULL) {
            ret = ENOMEM;
            goto error;
        }
    } else {
        rmp->namp = NULL;
    }

    if (filenamp != NULL) {
        rmp->filenamp = RDB_dup_str(filenamp);
        if (rmp->filenamp == NULL) {
            ret = ENOMEM;
            goto error;
        }
    } else {
        rmp->filenamp = NULL;
    }

    rmp->fieldlens = malloc(sizeof(int) * fieldc);
    if (rmp->fieldlens == NULL) {
        ret = ENOMEM;
        goto error;
    }

    rmp->fieldcount = fieldc;
    rmp->keyfieldcount = keyfieldc;
    rmp->cmpv = NULL;    
    rmp->dup_keys = (RDB_bool) !(RDB_UNIQUE & flags);

    rmp->varkeyfieldcount = rmp->vardatafieldcount = 0;
    for (i = 0; i < fieldc; i++) {
        rmp->fieldlens[i] = fieldlenv[i];
        if (fieldlenv[i] == RDB_VARIABLE_LEN) {
            if (i < rmp->keyfieldcount) {
                /* It's a key field */
                rmp->varkeyfieldcount++;
            } else {
                /* It's a data field */
                rmp->vardatafieldcount++;
            }
        }
    }
    ret = db_create(&rmp->dbp, envp != NULL ? envp->envp : NULL, 0);
    if (ret != 0) {
        goto error;
    }

    *rmpp = rmp;
    return RDB_OK;

error:
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldlens);
    free(rmp);
    return ret;
}

/*
 * Comparison function for b-trees.
 * Compares records by comparing the fields.
 */
static int
compare_key(DB *dbp, const DBT *dbt1p, const DBT *dbt2p)
{
    int i;
    RDB_recmap *rmp = (RDB_recmap *) dbp->app_private;

    for (i = 0; i < rmp->keyfieldcount; i++) {
        int offs1, offs2;
        size_t len1, len2;
        void *data1p, *data2p;
        int res;

        offs1 = _RDB_get_field(rmp, i, dbt1p->data, dbt1p->size, &len1, NULL);
        if (offs1 < 0)
            return offs1;
        offs2 = _RDB_get_field(rmp, i, dbt2p->data, dbt2p->size, &len2, NULL);
        if (offs2 < 0)
            return offs2;
        data1p = ((RDB_byte *) dbt1p->data) + offs1;
        data2p = ((RDB_byte *) dbt2p->data) + offs2;

        /* Compare field */
        if (rmp->cmpv[i].comparep != NULL) {
            /* Comparison function is available, so call it */
            res = (*rmp->cmpv[i].comparep)(data1p, len1, data2p, len2,
                    rmp->envp, rmp->cmpv[i].arg);
        } else {
            /* Compare memory */
            res = memcmp(data1p, data2p, len1 < len2 ? len1 : len2);
            if (res == 0)
                res = len1 - len2;
        }

        /* If the fields are different, we're done */
        if (res != 0) {
            /* If order is descending, revert result */
            if (!rmp->cmpv[i].asc)
                res = -res;
            return res;
        }
    }
    /* All fields equal */
    return 0;
}

int
RDB_create_recmap(const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const int fieldlenv[], int keyfieldc,
        const RDB_compare_field cmpv[], int flags,
        DB_TXN *txid, RDB_recmap **rmpp)
{
    int i;
    /* Allocate and initialize RDB_recmap structure */
    int ret = new_recmap(rmpp, name, filename, envp,
            fieldc, fieldlenv, keyfieldc, flags);
    if (ret != RDB_OK)
        return ret;

    if (cmpv != NULL) {
        (*rmpp)->cmpv = malloc(sizeof (RDB_compare_field) * keyfieldc);
        if ((*rmpp)->cmpv == NULL) {
            ret = ENOMEM;
            goto error;
        }
        for (i = 0; i < keyfieldc; i++) {
            (*rmpp)->cmpv[i].comparep = cmpv[i].comparep;
            (*rmpp)->cmpv[i].arg = cmpv[i].arg;
            (*rmpp)->cmpv[i].asc = cmpv[i].asc;
        }

        /* Set comparison function */
        (*rmpp)->dbp->app_private = *rmpp;
        (*rmpp)->dbp->set_bt_compare((*rmpp)->dbp, &compare_key);
    }

    if (!(RDB_UNIQUE & flags)) {
        /* Allow duplicate keys */
        (*rmpp)->dbp->set_flags((*rmpp)->dbp, DB_DUPSORT);
    }

    if (envp == NULL) {
        ret = (*rmpp)->dbp->set_alloc((*rmpp)->dbp, malloc, realloc, free);
        if (ret != 0) {
            goto error;
        }
    }

    /* Create BDB database under transaction control */
    ret = (*rmpp)->dbp->open((*rmpp)->dbp, txid, filename, name,
            RDB_ORDERED & flags ? DB_BTREE : DB_HASH,
            DB_CREATE, 0664);
    if (ret != 0) {
        goto error;
    }

    return RDB_OK;

error:
    RDB_close_recmap(*rmpp);
    return ret;
}

int
RDB_open_recmap(const char *name, const char *filename,
       RDB_environment *envp, int fieldc, const int fieldlenv[], int keyfieldc,
       DB_TXN *txid, RDB_recmap **rmpp)
{
    int ret = new_recmap(rmpp, name, filename, envp,
            fieldc, fieldlenv, keyfieldc, RDB_UNIQUE);
    if (ret != RDB_OK)
       return ret;

    if (envp == NULL) {
        ret = (*rmpp)->dbp->set_alloc((*rmpp)->dbp, malloc, realloc, free);
        if (ret != 0) {
            goto error;
        }
    }

    /* Open database */
    ret = (*rmpp)->dbp->open((*rmpp)->dbp, txid, filename, name, DB_UNKNOWN,
            0, 0664);
    if (ret != 0) {
        goto error;
    }
    return RDB_OK;

error:
    RDB_close_recmap(*rmpp);
    return ret;
}

int
RDB_close_recmap(RDB_recmap *rmp)
{
    int ret = rmp->dbp->close(rmp->dbp, 0);
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldlens);
    free(rmp->cmpv);
    free(rmp);
    return ret;
}

int
RDB_delete_recmap(RDB_recmap *rmp, DB_TXN *txid)
{
    int ret;
    ret = rmp->dbp->close(rmp->dbp, DB_NOSYNC);
    if (ret != 0)
        goto cleanup;

    if (rmp->envp != NULL && rmp->namp != NULL) {
        ret = rmp->envp->envp->dbremove(rmp->envp->envp, txid, rmp->filenamp,
                rmp->namp, 0);
    }

cleanup:
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldlens);
    free(rmp->cmpv);
    free(rmp);

    return ret;
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
_RDB_get_vflen(RDB_byte *databp, size_t len, int vfcnt, int vpos)
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
_RDB_get_field(RDB_recmap *rmp, int fno, void *datap, size_t len, size_t *lenp,
              int *vposp)
{
    int i, vpos;
    int offs = 0;
    RDB_byte *databp = (RDB_byte *) datap;

    if (fno < rmp->keyfieldcount) {
        /*
         * compute offset and length for key
         */
        if (rmp->fieldlens[fno] != RDB_VARIABLE_LEN) {
            /* offset is sum of lengths of previous fields */
            for (i = 0; i < fno; i++) {
                if (rmp->fieldlens[i] != RDB_VARIABLE_LEN) {
                    offs += rmp->fieldlens[i];
                }
            }

            *lenp = (size_t) rmp->fieldlens[fno];
        } else {
            /* offset is sum of lengths of fixed-length fields
             * plus lengths of previous variable-length fields 
             */
             
            /* add fixed-length fields */
            for (i = 0; i < rmp->keyfieldcount; i++) {
                if (rmp->fieldlens[i] != RDB_VARIABLE_LEN) {
                    offs += rmp->fieldlens[i];
                }
            }
            
            /* compute position within var-length fields */
            vpos = 0;
            for (i = 0; i < fno; i++) {
                if (rmp->fieldlens[i] == RDB_VARIABLE_LEN) {
                    vpos++;
                }
            }

            /* add previous variable-length fields */
            for (i = 0; i < vpos; i++) {
                offs += _RDB_get_vflen(databp, len, rmp->varkeyfieldcount, i);
            }
            
            *lenp = _RDB_get_vflen(databp, len, rmp->varkeyfieldcount, vpos);
        }
    } else {
        /*
         * compute offset and length for data
         */
        if (rmp->fieldlens[fno] != RDB_VARIABLE_LEN) {
            /* offset is sum of lengths of previous fields */
            for (i = rmp->keyfieldcount; i < fno; i++) {
                if (rmp->fieldlens[i] != RDB_VARIABLE_LEN) {
                    offs += rmp->fieldlens[i];
                }
            }

            *lenp = (size_t) rmp->fieldlens[fno];
        } else {
            /* offset is sum of lengths of fixed-length fields
             * plus lengths of previous variable-length fields 
             */
             
            /* add fixed-length fields */
            for (i = rmp->keyfieldcount; i < rmp->fieldcount; i++) {
                if (rmp->fieldlens[i] != RDB_VARIABLE_LEN) {
                    offs += rmp->fieldlens[i];
                }
            }
            
            /* compute position within var-length fields */
            vpos = 0;
            for (i = rmp->keyfieldcount; i < fno; i++) {
                if (rmp->fieldlens[i] == RDB_VARIABLE_LEN) {
                    vpos++;
                }
            }

            /* add previous variable-length fields */
            for (i = 0; i < vpos; i++) {
                offs += _RDB_get_vflen(databp, len, rmp->vardatafieldcount, i);
            }
            
            *lenp = _RDB_get_vflen(databp, len, rmp->vardatafieldcount, vpos);
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
_RDB_fields_to_DBT(RDB_recmap *rmp, int fldc, const RDB_field fldv[],
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
        if (rmp->fieldlens[fldv[i].no] == RDB_VARIABLE_LEN) {
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
        if (rmp->fieldlens[fldv[i].no] == RDB_VARIABLE_LEN) {
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
        if (fldv[fn].len != (size_t) RDB_VARIABLE_LEN) {
            (*fldv[fn].copyfp)(databp + offs, fldv[fn].datap, fldv[fn].len);
            offs += fldv[fn].len;
        }
        
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

    return _RDB_fields_to_DBT(rmp, rmp->keyfieldcount, fldv, keyp);
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

    return _RDB_fields_to_DBT(rmp, rmp->fieldcount - rmp->keyfieldcount,
                             fldv + rmp->keyfieldcount, datap);
}

int
RDB_insert_rec(RDB_recmap *rmp, RDB_field flds[], DB_TXN *txid)
{
    DBT key, data;
    int ret;

    ret = key_to_DBT(rmp, flds, &key);
    if (ret != RDB_OK)
        return ret;
    ret = data_to_DBT(rmp, flds, &data);
    if (ret != RDB_OK)
        return ret;

    ret = rmp->dbp->put(rmp->dbp, txid, &key, &data,
            rmp->dup_keys ? 0 : DB_NOOVERWRITE);
    if (ret == EINVAL) {
        /* Assume duplicate secondary index */
        ret = DB_KEYEXIST;
    }
    free(key.data);
    free(data.data);

    return ret;
}

int
_RDB_set_field(RDB_recmap *recmapp, DBT *recpartp, const RDB_field *fieldp, 
               int varfieldc)
{
    size_t oldlen;
    int vpos;
    RDB_byte *databp = (RDB_byte *) recpartp->data;
    int offs = _RDB_get_field(recmapp, fieldp->no,
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
_RDB_update_rec(RDB_recmap *rmp, DBT *keyp, DBT *datap,
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
            ret = _RDB_set_field(rmp, keyp, &fieldv[i],
                           rmp->varkeyfieldcount);
        } else {
            ret = _RDB_set_field(rmp, datap, &fieldv[i],
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
RDB_update_rec(RDB_recmap *rmp, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], DB_TXN *txid)
{
    DBT key, data;
    int ret;

    ret = key_to_DBT(rmp, keyv, &key);
    if (ret != RDB_OK)
        return ret;
    memset(&data, 0, sizeof (data));
    data.flags = DB_DBT_REALLOC;

    ret = rmp->dbp->get(rmp->dbp, txid, &key, &data, 0);
    if (ret != 0)
        goto cleanup;

    ret = _RDB_update_rec(rmp, &key, &data, fieldc, fieldv, txid);

cleanup:
    free(key.data);
    free(data.data);

    return ret;
}

int
RDB_delete_rec(RDB_recmap *rmp, RDB_field keyv[], DB_TXN *txid)
{
    DBT key;
    int ret;

    ret = key_to_DBT(rmp, keyv, &key);
    if (ret != RDB_OK)
        return ret;

    ret = rmp->dbp->del(rmp->dbp, txid, &key, 0);
    free(key.data);
    return ret;
}

int
_RDB_get_fields(RDB_recmap *rmp, const DBT *keyp, const DBT *datap, int fieldc,
           RDB_field retfieldv[])
{
    int i;

    for (i = 0; i < fieldc; i++) {
        int offs;

        if (retfieldv[i].no < rmp->keyfieldcount) {
            offs = _RDB_get_field(rmp, retfieldv[i].no,
                    keyp->data, keyp->size, &retfieldv[i].len, NULL);
            if (offs < 0)
                return offs;
            retfieldv[i].datap = ((RDB_byte *)keyp->data) + offs;
        } else {
            offs = _RDB_get_field(rmp, retfieldv[i].no,
                    datap->data, datap->size, &retfieldv[i].len, NULL);
            if (offs < 0)
                return offs;
            retfieldv[i].datap = ((RDB_byte *)datap->data) + offs;
        }
    }

    return RDB_OK;
}

int
RDB_get_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc, DB_TXN *txid,
           RDB_field retfieldv[])
{
    DBT key, data;
    int ret;

    ret = key_to_DBT(rmp, keyv, &key);
    if (ret != RDB_OK)
        return ret;

    memset(&data, 0, sizeof (data));

    ret = rmp->dbp->get(rmp->dbp, txid, &key, &data, 0);
    if (ret != 0) {
        free(key.data);
        return ret;
    }

    ret = _RDB_get_fields(rmp, &key, &data, fieldc, retfieldv);
    free(key.data);
    return ret;
}

int
RDB_contains_rec(RDB_recmap *rmp, RDB_field flds[], DB_TXN *txid)
{
    DBT key, data;
    int ret;

    ret = key_to_DBT(rmp, flds, &key);
    if (ret != RDB_OK)
        return ret;
    ret = data_to_DBT(rmp, flds, &data);
    if (ret != RDB_OK) {
        free(key.data);
        return ret;
    }
    key.flags = DB_DBT_REALLOC;
    data.flags = DB_DBT_REALLOC;

    ret = rmp->dbp->get(rmp->dbp, txid, &key, &data, DB_GET_BOTH);
    free(key.data);
    free(data.data);
    return ret;
}
