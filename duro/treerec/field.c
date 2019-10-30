/*
 * Recmap field functions shared by tree and BDB recmaps.
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "field.h"
#include <rec/recmapimpl.h>

#include <errno.h>
#include <string.h>

/*
 *
 *
 * vposp    for variable-length fields: location for position in the
 *              var-len field table
 */
int
RDB_get_field(RDB_recmap *rmp, int fno, const void *datap, size_t len, size_t *lenp,
              int *vposp)
{
    int i, vpos;
    int offs = 0;
    uint8_t *databp = (uint8_t *) datap;

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

static size_t
get_len(const uint8_t *databp)
{
    size_t len;

    len = databp[0];
    len += databp[1] << 8;
    len += databp[2] << 16;
    len += databp[3] << 24;

    return len;
}

static void
set_len(uint8_t *databp, size_t len)
{
    databp[0] = (uint8_t)(len & 0xff);
    databp[1] = (uint8_t)(len >> 8);
    databp[2] = (uint8_t)(len >> 16);
    databp[3] = (uint8_t)(len >> 24);
}

int
RDB_fields_to_mem(RDB_recmap *rmp, int fldc, const RDB_field fldv[],
        void **datap, size_t *sizep)
{
    uint8_t *databp;
    int vfldc;
    int fi;
    int vfi;
    int offs;
    int i;
    int ret;
    int *fno = NULL;    /* fixed field no in DBT -> field no */
    int *vfno = NULL;   /* var field no in DBT -> field no */

    /* calculate key and data length, and # of variable size fields */
    *sizep = 0;
    vfldc = 0;
    for (i = 0; i < fldc; i++) {
        *sizep += fldv[i].len;
        if (rmp->fieldinfos[fldv[i].no].len == RDB_VARIABLE_LEN) {
            /* RECLEN_SIZE bytes extra for length */
            *sizep += RECLEN_SIZE;

            vfldc++;
        }
    }

    if (*sizep > 0) {
        *datap = malloc(*sizep);
        if (*datap == NULL) {
            ret = ENOMEM;
            goto error;
        }
    } else {
        *datap = NULL;
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
     * Fill destination buffer
     */

    offs = 0;
    databp = *datap;

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
        set_len(&databp[*sizep - vfldc * RECLEN_SIZE + i * RECLEN_SIZE],
                fldv[fn].len);
    }

    free(fno);
    free(vfno);
    return RDB_OK;

error:
    free(*datap);
    free(fno);
    free(vfno);

    return ret;
}

int
RDB_get_mem_fields(RDB_recmap *rmp, const void *key, size_t keylen,
        const void *value, size_t valuelen, int fieldc, RDB_field retfieldv[])
{
    int i;

    for (i = 0; i < fieldc; i++) {
        int offs;

        if (retfieldv[i].no < rmp->keyfieldcount) {
            offs = RDB_get_field(rmp, retfieldv[i].no,
                    key, keylen, &retfieldv[i].len, NULL);
            if (offs < 0)
                return offs;
            retfieldv[i].datap = ((uint8_t *)key) + offs;
        } else {
            offs = RDB_get_field(rmp, retfieldv[i].no,
                    value, valuelen, &retfieldv[i].len, NULL);
            if (offs < 0)
                return offs;
            retfieldv[i].datap = ((uint8_t *)value) + offs;
        }
    }

    return RDB_OK;
}

int
RDB_set_field_mem(RDB_recmap *recmapp, void **datap, size_t *sizep,
        const RDB_field *fieldp, int varfieldc)
{
    size_t oldlen;
    int vpos;
    void *ndata;
    uint8_t *databp = *datap;
    int offs = RDB_get_field(recmapp, fieldp->no,
                    *datap, *sizep, &oldlen, &vpos);
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
                *sizep - offs - oldlen);

        /* change memory block size */
        ndata = realloc(*datap, *sizep + fieldp->len - oldlen);
        if (ndata == NULL)
            return ENOMEM;
        *datap = ndata;
        databp = (uint8_t *) *datap;

        /* If the field grows, move the data following the field after realloc */
        if (fieldp->len > oldlen)
            memmove(databp + offs + fieldp->len,
                    databp + offs + oldlen,
                    *sizep - offs - oldlen);
        *sizep = *sizep + fieldp->len - oldlen;
        set_len(&databp[*sizep - varfieldc * RECLEN_SIZE
                        + vpos * RECLEN_SIZE], fieldp->len);
    }
    /* copy data into field */
    (*(fieldp->copyfp))(databp + offs, fieldp->datap, fieldp->len);
    return RDB_OK;
}

size_t
RDB_get_vflen(uint8_t *databp, size_t len, int vfcnt, int vpos)
{
    return get_len(&databp[len - vfcnt * RECLEN_SIZE + vpos * RECLEN_SIZE]);
}
