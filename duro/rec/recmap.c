#include "recmap.h"

/* $Id$ */

#include <gen/errors.h>
#include <gen/strfns.h>
#include <malloc.h>
#include <string.h>
#include <errno.h>

enum {
    RECLEN_SIZE = 4
};

/* Allocate a RDB_recmap structure and initialize its fields.
 * The underlying BDB database is created using db_create(), but not opened.
 */
static int
create_recmap(RDB_recmap **rmpp, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldlenv[], int keyfieldc)
{
    int i, res;
    RDB_recmap *rmp = malloc(sizeof(RDB_recmap));

    if (rmp == NULL)
        return RDB_NO_MEMORY;

    rmp->filenamp = NULL;
    rmp->fieldlens = NULL;
    if (namp != NULL) {
        rmp->namp = RDB_dup_str(namp);
        if (rmp->namp == NULL) {
            res = RDB_NO_MEMORY;
            goto error;
        }
    } else {
        rmp->namp = NULL;
    }

    if (filenamp != NULL) {
        rmp->filenamp = RDB_dup_str(filenamp);
        if (rmp->filenamp == NULL) {
            res = RDB_NO_MEMORY;
            goto error;
        }
    } else {
        rmp->filenamp = NULL;
    }

    rmp->fieldlens = malloc(sizeof(int) * fieldc);
    if (rmp->fieldlens == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    
    *rmpp = rmp;
    rmp->fieldcount = fieldc;
    rmp->keyfieldcount = keyfieldc;

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
    res = db_create(&rmp->dbp, dsp->envp, 0);
    if (res != 0) {
        goto error;
    }
    return RDB_OK;
error:
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldlens);
    free(rmp);
    return res;
}

int
RDB_create_recmap(const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldlenv[], int keyfieldc,
        DB_TXN *txid, RDB_recmap **rmpp)
{
    int res = create_recmap(rmpp, namp, filenamp, dsp,
            fieldc, fieldlenv, keyfieldc);
   
    if (res != RDB_OK)
        return res;
       
    if ((res = (*rmpp)->dbp->open((*rmpp)->dbp, txid, filenamp, namp, DB_HASH,
            DB_CREATE, 0664)) != 0)
        goto error; 
   return RDB_OK;

error:
    RDB_close_recmap(*rmpp);
    return res;
}

int
RDB_open_recmap(const char *namp, const char *filenamp,
       RDB_environment *dsp, int fieldc, const int fieldlenv[], int keyfieldc,
       DB_TXN *txid, RDB_recmap **rmpp)
{
    int res = create_recmap(rmpp, namp, filenamp, dsp,
            fieldc, fieldlenv, keyfieldc);
   
    if (res != RDB_OK)
       return res;

    res = (*rmpp)->dbp->open((*rmpp)->dbp, txid, filenamp, namp, DB_UNKNOWN, 0, 0664);
    if (res != 0)
        goto error;
    return RDB_OK;

error:
    RDB_close_recmap(*rmpp);
    return res;
}

int
RDB_close_recmap(RDB_recmap *rmp)
{
    int res = rmp->dbp->close(rmp->dbp, 0);
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldlens);
    free(rmp);
    return res;
}

int
RDB_delete_recmap(RDB_recmap *rmp, RDB_environment *envp, DB_TXN *txid)
{
    int res;

    res = rmp->dbp->close(rmp->dbp, DB_NOSYNC);
    if (res != 0)
        goto error;

    if (rmp->namp != NULL) {
        res = envp->envp->dbremove(envp->envp, txid, rmp->filenamp, rmp->namp, 0);
    }

error:
    free(rmp->namp);
    free(rmp->filenamp);
    free(rmp->fieldlens);
    free(rmp);
    return res;
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
    int offs = 0;
    int i, vpos;
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

            *lenp = rmp->fieldlens[fno];
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

            *lenp = rmp->fieldlens[fno];
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
    int res;
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
    fno = malloc((fldc - vfldc) * sizeof(int));
    vfno = malloc(vfldc *  sizeof(int));

    if (fno == NULL || vfno == NULL || dbtp->data == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
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
        memcpy(databp + offs, fldv[fn].datap, fldv[fn].len);
        offs += fldv[fn].len;
    }
    /* variable-length fields */
    for (i = 0; i < vfldc; i++) {
        int fn = vfno[i];
        memcpy(databp + offs, fldv[fn].datap, fldv[fn].len);
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

    return res;
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
    int res;

    res = key_to_DBT(rmp, flds, &key);
    if (res != RDB_OK)
        return res;
    res = data_to_DBT(rmp, flds, &data);
    if (res != RDB_OK)
        return res;

#ifdef DEBUG
    printf("storing key:\n");
    _RDB_dump(key.data, key.size);
    printf("storing data:\n");
    _RDB_dump(data.data, data.size);
#endif

    res = rmp->dbp->put(rmp->dbp, txid, &key, &data, DB_NOOVERWRITE);
    if (res == DB_KEYEXIST)
        res = RDB_KEY_VIOLATION;
    else if (res == DB_LOCK_DEADLOCK)
        res = RDB_DEADLOCK;
    else if (res == EINVAL)
        /* Assume duplicate secondary index */
        res = RDB_KEY_VIOLATION;
    free(key.data);
    free(data.data);

    return res;
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
        memmove(recpartp->data + offs + fieldp->len,
                        recpartp->data + offs + oldlen,
                        recpartp->size - offs - oldlen);
        recpartp->size = recpartp->size + fieldp->len - oldlen;
        set_len(&databp[recpartp->size - varfieldc * RECLEN_SIZE
                        + vpos * RECLEN_SIZE], fieldp->len);
    }
    /* copy data */
    memcpy(databp + offs, fieldp->datap, fieldp->len);
}

int
RDB_update_rec(RDB_recmap *recmapp, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], DB_TXN *txid)
{
    DBT key, data;
    int res;
    int i;

    res = key_to_DBT(recmapp, keyv, &key);
    if (res != RDB_OK)
        return res;
    memset(&data, 0, sizeof (data));
    data.flags = DB_DBT_REALLOC;

    res = recmapp->dbp->get(recmapp->dbp, txid, &key, &data, 0);
    if (res != 0) {
         if (res == DB_NOTFOUND)
            res = RDB_NOT_FOUND;
        else if (res == DB_LOCK_DEADLOCK)
            res = RDB_DEADLOCK;

        goto error;
    }

    /* Check if the key is to be modified */
    for (i = 0; i < fieldc; i++) {
        if (fieldv[i].no < recmapp->keyfieldcount) {
            /* Key is to be modified, so delete record first */
            res = recmapp->dbp->del(recmapp->dbp, txid, &key, 0);
            if (res != 0) {
                if (res == DB_LOCK_DEADLOCK)
                    res = RDB_DEADLOCK;
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
    res = recmapp->dbp->put(recmapp->dbp, txid, &key, &data, 0);
    if (res == DB_LOCK_DEADLOCK)
        res = RDB_DEADLOCK;

error:
    free(key.data);
    free(data.data);
    
    return res;
}

int
RDB_delete_rec(RDB_recmap *rmp, RDB_field keyv[], DB_TXN *txid)
{
    DBT key;
    int res;

    res = key_to_DBT(rmp, keyv, &key);
    if (res != RDB_OK)
        return res;

    res = rmp->dbp->del(rmp->dbp, txid, &key, 0);
    if (res != 0) {
        if (res == DB_NOTFOUND)
            res = RDB_NOT_FOUND;
        else if (res == DB_LOCK_DEADLOCK)
            res = RDB_DEADLOCK;
        return res;
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
           RDB_field resfieldv[])
{
    int i;

    for (i = 0; i < fieldc; i++) {
        int offs = _RDB_get_field(rmp, resfieldv[i].no,
                                 datap->data, datap->size, &resfieldv[i].len, NULL);
        resfieldv[i].datap = datap->data + offs;
    }

    return RDB_OK;
}

int
RDB_get_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc, DB_TXN *txid,
           RDB_field resfieldv[])
{
    DBT key, data;
    int res;

    res = key_to_DBT(rmp, keyv, &key);
    if (res != RDB_OK)
        return res;

    memset(&data, 0, sizeof (data));

    res = rmp->dbp->get(rmp->dbp, txid, &key, &data, 0);
    if (res != 0) {
         if (res == DB_NOTFOUND)
            res = RDB_NOT_FOUND;
        else if (res == DB_LOCK_DEADLOCK)
            res = RDB_DEADLOCK;

        return res;
    }

    return _RDB_get_fields(rmp, &key, &data, fieldc, resfieldv);
}

int
RDB_contains_rec(RDB_recmap *rmp, RDB_field flds[], DB_TXN *txid)
{
    DBT key, data;
    int res;

    res = key_to_DBT(rmp, flds, &key);
    if (res != RDB_OK)
        return res;
    res = data_to_DBT(rmp, flds, &data);
    if (res != RDB_OK)
        return res;

    res = rmp->dbp->get(rmp->dbp, txid, &key, &data, DB_GET_BOTH);
    if (res == DB_NOTFOUND)
        res = RDB_NOT_FOUND;
    else if (res == DB_LOCK_DEADLOCK)
        res = RDB_DEADLOCK;

    return res;
}
