/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "recmap.h"

#include <gen/errors.h>
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
        RDB_bool dup)
{
    int i, ret;
    RDB_recmap *rmp = malloc(sizeof(RDB_recmap));

    if (rmp == NULL)
        return RDB_NO_MEMORY;

    rmp->filenamp = NULL;
    rmp->fieldlens = NULL;
    if (namp != NULL) {
        rmp->namp = RDB_dup_str(namp);
        if (rmp->namp == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    } else {
        rmp->namp = NULL;
    }

    if (filenamp != NULL) {
        rmp->filenamp = RDB_dup_str(filenamp);
        if (rmp->filenamp == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    } else {
        rmp->filenamp = NULL;
    }

    rmp->fieldlens = malloc(sizeof(int) * fieldc);
    if (rmp->fieldlens == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }

    rmp->fieldcount = fieldc;
    rmp->keyfieldcount = keyfieldc;
    rmp->cmpv = NULL;    
    rmp->dup_keys = dup;

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
    return RDB_convert_err(ret);
}

int
RDB_create_recmap(const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const int fieldlenv[], int keyfieldc,
        DB_TXN *txid, RDB_recmap **rmpp)
{
    /* Allocate and initialize RDB_recmap structure */
    int ret = new_recmap(rmpp, name, filename, envp,
            fieldc, fieldlenv, keyfieldc, RDB_FALSE); 
    if (ret != RDB_OK)
        return ret;
       
    /* Create BDB database */
    if ((ret = (*rmpp)->dbp->open((*rmpp)->dbp, txid, filename, name, DB_HASH,
            DB_CREATE, 0664)) != 0)
        goto error;

    return RDB_OK;

error:
    RDB_close_recmap(*rmpp);
    return RDB_convert_err(ret);
}

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
        offs2 = _RDB_get_field(rmp, i, dbt2p->data, dbt2p->size, &len2, NULL);
        data1p = ((RDB_byte *) dbt1p->data) + offs1;
        data2p = ((RDB_byte *) dbt2p->data) + offs2;    

        /* Compare fields */
        if (rmp->cmpv[i].comparep != NULL) {
            /* Comparison function is available, so call it */
            res = (*rmp->cmpv[i].comparep)(data1p, len1, data2p, len2,
                    rmp->cmpv[i].arg);
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
RDB_create_sorted_recmap(const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const int fieldlenv[], int keyfieldc,
        const RDB_compare_field cmpv[],
        RDB_bool dup, DB_TXN *txid, RDB_recmap **rmpp)
{
    int i;
    /* Allocate and initialize RDB_recmap structure */
    int ret = new_recmap(rmpp, name, filename, envp,
            fieldc, fieldlenv, keyfieldc, dup); 

    if (ret != RDB_OK)
        return ret;

    (*rmpp)->cmpv = malloc(sizeof (RDB_compare_field) * fieldc);
    if ((*rmpp)->cmpv == NULL)
        goto error;
    for (i = 0; i < fieldc; i++) {
        (*rmpp)->cmpv[i].comparep = cmpv[i].comparep;
        (*rmpp)->cmpv[i].arg = cmpv[i].arg;
        (*rmpp)->cmpv[i].asc = cmpv[i].asc;
    }

    /* Set comparison function */
    (*rmpp)->dbp->app_private = *rmpp;
    (*rmpp)->dbp->set_bt_compare((*rmpp)->dbp, &compare_key);

    if (dup) {
        /* Allow duplicate keys */
        (*rmpp)->dbp->set_flags((*rmpp)->dbp, DB_DUP);
    }    
   
    /* Create BDB database */
    if ((ret = (*rmpp)->dbp->open((*rmpp)->dbp, txid, filename, name, DB_BTREE,
            DB_CREATE, 0664)) != 0)
        goto error;
   
   return RDB_OK;

error:
    RDB_close_recmap(*rmpp);
    return RDB_convert_err(ret);
}

int
RDB_open_recmap(const char *name, const char *filename,
       RDB_environment *envp, int fieldc, const int fieldlenv[], int keyfieldc,
       DB_TXN *txid, RDB_recmap **rmpp)
{
    int ret = new_recmap(rmpp, name, filename, envp,
            fieldc, fieldlenv, keyfieldc, RDB_FALSE);
    if (ret != RDB_OK)
       return ret;

    ret = (*rmpp)->dbp->open((*rmpp)->dbp, txid, filename, name, DB_UNKNOWN,
            0, 0664);
    if (ret != 0) {
        if (ret == ENOENT)
            ret = RDB_NOT_FOUND;
        goto error;
    }
    return RDB_OK;

error:
    RDB_close_recmap(*rmpp);
    return RDB_convert_err(ret);
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
    return RDB_convert_err(ret);
}

int
RDB_delete_recmap(RDB_recmap *rmp, RDB_environment *envp, DB_TXN *txid)
{
    int ret;
    ret = rmp->dbp->close(rmp->dbp, DB_NOSYNC);
    if (ret != 0)
        goto cleanup;

    if (envp != NULL && rmp->namp != NULL) {
        ret = envp->envp->dbremove(envp->envp, txid, rmp->filenamp, rmp->namp, 0);
    }

cleanup:
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldlens);
    free(rmp);
    if (ret != 0)
        RDB_errmsg(envp, "Error deleting recmap: %s", RDB_strerror(ret));

    return RDB_convert_err(ret);
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
                offs += get_len(&databp[len - rmp->varkeyfieldcount * RECLEN_SIZE
                                        + i * RECLEN_SIZE]);
            }
            
            *lenp = get_len(&databp[len - rmp->varkeyfieldcount * RECLEN_SIZE
                                    + vpos * RECLEN_SIZE]);
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
                offs += get_len(&databp[len - rmp->vardatafieldcount * RECLEN_SIZE
                                        + i * RECLEN_SIZE]);
            }
            
            *lenp = get_len(&databp[len - rmp->vardatafieldcount * RECLEN_SIZE
                                    + vpos * RECLEN_SIZE]);
        }
    }
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
    int *fno;	/* fixed field no in DBT -> field no */
    int *vfno;	/* var field no in DBT -> field no */

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
  
    dbtp->data = malloc(dbtp->size);
    if (fldc - vfldc > 0) {
        fno = malloc((fldc - vfldc) * sizeof(int));
        if (fno == NULL || dbtp->data == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }
    if (vfldc > 0) {
        vfno = malloc(vfldc *  sizeof(int));
        if (vfno == NULL) {
            ret = RDB_NO_MEMORY;
            if (fldc - vfldc > 0)
                free(fno);
            free(dbtp->data);
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
        (*fldv[fn].copyfp)(databp + offs, fldv[fn].datap, fldv[fn].len);
        offs += fldv[fn].len;
        
        /* field length */
        set_len(&databp[dbtp->size - vfldc * RECLEN_SIZE + i * RECLEN_SIZE],
                fldv[fn].len);
    }

    if (fldc - vfldc > 0)
        free(fno);
    if (vfldc > 0)
        free(vfno);
    return RDB_OK;
    
error:
    free(dbtp->data);
    free(fno);
    free(vfno);

    return ret;
}

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

#ifdef DEBUG
    printf("storing key:\n");
    _RDB_dump(key.data, key.size);
    printf("storing data:\n");
    _RDB_dump(data.data, data.size);
#endif

    ret = rmp->dbp->put(rmp->dbp, txid, &key, &data,
            rmp->dup_keys ? 0 : DB_NOOVERWRITE);
    if (ret == EINVAL) {
        /* Assume duplicate secondary index */
        ret = RDB_KEY_VIOLATION;
    }
    free(key.data);
    free(data.data);

    return RDB_convert_err(ret);
}

void
_RDB_set_field(RDB_recmap *recmapp, DBT *recpartp, const RDB_field *fieldp, 
               int varfieldc)
{
    RDB_byte *databp;
    size_t oldlen;
    int vpos;
    int offs = _RDB_get_field(recmapp, fieldp->no,
                    recpartp->data, recpartp->size, &oldlen, &vpos);

    if (oldlen == fieldp->len) {
        databp = ((RDB_byte *)recpartp->data);
    } else {
        /* change field length */
        recpartp->data = realloc(recpartp->data,
                                 recpartp->size + fieldp->len - oldlen);
        databp = ((RDB_byte *)recpartp->data);
        memmove(databp + offs + fieldp->len,
                databp + offs + oldlen,
                recpartp->size - offs - oldlen);
        recpartp->size = recpartp->size + fieldp->len - oldlen;
        set_len(&databp[recpartp->size - varfieldc * RECLEN_SIZE
                        + vpos * RECLEN_SIZE], fieldp->len);
    }
    /* copy data */
    (*(fieldp->copyfp))(databp + offs, fieldp->datap, fieldp->len);
}

int
RDB_update_rec(RDB_recmap *recmapp, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], DB_TXN *txid)
{
    DBT key, data;
    int ret;
    int i;

    ret = key_to_DBT(recmapp, keyv, &key);
    if (ret != RDB_OK)
        return RDB_convert_err(ret);
    memset(&data, 0, sizeof (data));
    data.flags = DB_DBT_REALLOC;

    ret = recmapp->dbp->get(recmapp->dbp, txid, &key, &data, 0);
    if (ret != 0) {
        goto error;
    }

    /* Check if the key is to be modified */
    for (i = 0; i < fieldc; i++) {
        if (fieldv[i].no < recmapp->keyfieldcount) {
            /* Key is to be modified, so delete record first */
            ret = recmapp->dbp->del(recmapp->dbp, txid, &key, 0);
            if (ret != 0) {
                goto error;
            }
            break;
        }
    }

    for (i = 0; i < fieldc; i++) {
        if (fieldv[i].no < recmapp->keyfieldcount) {
            _RDB_set_field(recmapp, &key, &fieldv[i],
                           recmapp->varkeyfieldcount);
        } else {
            _RDB_set_field(recmapp, &data, &fieldv[i],
                           recmapp->vardatafieldcount);
        }
    }

    /* Write record back */
    ret = recmapp->dbp->put(recmapp->dbp, txid, &key, &data, 0);
    if (ret == EINVAL) {
        /* Assume duplicate secondary index */
        ret = RDB_KEY_VIOLATION;
    }

error:
    free(key.data);
    free(data.data);
    
    return RDB_convert_err(ret);
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
    if (ret != 0) {
        return RDB_convert_err(ret);
    }
    return RDB_OK;
}

RDB_bool
RDB_field_is_pindex(RDB_recmap *rmp, int fieldno)
{
   return (RDB_bool) (fieldno < rmp->keyfieldcount);
}

int
_RDB_get_fields(RDB_recmap *rmp, const DBT *keyp, const DBT *datap, int fieldc,
           RDB_field retfieldv[])
{
    int i;

    for (i = 0; i < fieldc; i++) {
        int offs = _RDB_get_field(rmp, retfieldv[i].no,
                                 datap->data, datap->size, &retfieldv[i].len, NULL);
        retfieldv[i].datap = ((RDB_byte *)datap->data) + offs;
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
        return RDB_convert_err(ret);
    }

    return _RDB_get_fields(rmp, &key, &data, fieldc, retfieldv);
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
    if (ret != RDB_OK)
        return ret;

    return RDB_convert_err(rmp->dbp->get(rmp->dbp, txid, &key, &data, DB_GET_BOTH));
}
