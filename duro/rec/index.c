/* $Id$ */

#include "index.h"
#include <gen/errors.h>
#include <gen/strfns.h>
#include <string.h>

static int
create_index(RDB_recmap *rmp, const char *name, const char *filename,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        RDB_bool unique, RDB_index **ixpp)
{
    int ret;
    int i;
    RDB_index *ixp = malloc(sizeof (RDB_index));

    if (ixp == NULL) {
        return RDB_NO_MEMORY;
    }
    ixp->fieldv = NULL;
    ixp->rmp = rmp;

    ixp->namp = ixp->filenamp = NULL;
    if (name != NULL) {  
        ixp->namp = RDB_dup_str(name);
        if (ixp->namp == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }
    if (filename != NULL) {
        ixp->filenamp = RDB_dup_str(filename);
        if (ixp->filenamp == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
    }

    ixp->fieldc = fieldc;
    ixp->fieldv = malloc(fieldc * sizeof(int));
    if (ixp->fieldv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    for (i = 0; i < fieldc; i++)
        ixp->fieldv[i] = fieldv[i];

    ret = db_create(&ixp->dbp, dsp->envp, 0);
    if (ret != 0) {
        ret = RDB_convert_err(ret);
        goto error;
    }
    ixp->unique = unique;

    *ixpp = ixp;
    return RDB_OK;

error:
    free(ixp->namp);
    free(ixp->filenamp);
    free(ixp->fieldv);
    free(ixp);
    return ret;
}

/* Fill the DBT for the secondary key */
static int
make_skey(DB *dbp, const DBT *pkeyp, const DBT *pdatap, DBT *skeyp)
{
    RDB_index *ixp = dbp->app_private;
    RDB_field *fieldv;
    int ret;
    int i;
    
    fieldv = malloc (sizeof(RDB_field) * ixp->fieldc);
    if (fieldv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < ixp->fieldc; i++) {
        fieldv[i].no = ixp->fieldv[i];
        fieldv[i].copyfp = &memcpy;
    }
    ret = _RDB_get_fields(ixp->rmp, pkeyp, pdatap, ixp->fieldc, fieldv);
    if (ret != RDB_OK) {
        free(fieldv);
        return ret;
    }

    ret = _RDB_fields_to_DBT(ixp->rmp, ixp->fieldc, fieldv, skeyp);
    skeyp->flags = DB_DBT_APPMALLOC;
    free(fieldv);
    return ret;
}

int
RDB_create_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const int fieldv[],
        RDB_bool unique, DB_TXN *txid, RDB_index **ixpp)
{
    RDB_index *ixp;
    int ret;
   
    ret = create_index(rmp, namp, filenamp, envp, fieldc, fieldv, unique, &ixp);
    if (ret != RDB_OK)
        return ret;

    if (!unique)
        ixp->dbp->set_flags(ixp->dbp, DB_DUPSORT);

    ret = ixp->dbp->open(ixp->dbp, txid, filenamp, namp, DB_HASH, DB_CREATE, 0664);
    if (ret != 0) {
        ret = RDB_convert_err(ret);
        RDB_errmsg(envp, "cannot open index: %s", RDB_strerror(ret));
        goto error;
    }

    /* attach index to BDB database (for the callback) */
    ixp->dbp->app_private = ixp;

    /* associate the index DB with the recmap DB */
    ret = ixp->dbp->associate(rmp->dbp, txid, ixp->dbp, make_skey, DB_CREATE);
    if (ret != 0) {
        ret = RDB_convert_err(ret);
        goto error;
    }
   
    *ixpp = ixp;
    return RDB_OK;

error:
    RDB_delete_index(ixp, envp, txid);
    return ret;
}

int
RDB_open_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const int fieldv[], RDB_bool unique,
        DB_TXN *txid, RDB_index **ixpp)
{
    RDB_index *ixp;
    int ret;

    ret = create_index(rmp, namp, filenamp, envp, fieldc, fieldv, unique, &ixp);
    if (ret != RDB_OK)
        return RDB_convert_err(ret);

    if (!unique)
        ixp->dbp->set_flags(ixp->dbp, DB_DUPSORT);

    ret = ixp->dbp->open(ixp->dbp, txid, filenamp, namp, DB_UNKNOWN, 0, 0664);
    if (ret != 0) {
        ret = RDB_convert_err(ret);
        goto error;
    }

    /* attach index to BDB database (for the callback) */
    ixp->dbp->app_private = ixp;

    /* associate the index DB with the recmap DB */
    ret = ixp->dbp->associate(rmp->dbp, txid, ixp->dbp, make_skey, 0);
    if (ret != 0) {
        ret = RDB_convert_err(ret);
        goto error;
    }

    *ixpp = ixp;
    return RDB_OK;
error:
    RDB_close_index(ixp);
    return ret;
}

int
RDB_close_index(RDB_index *ixp)
{
    int ret = ixp->dbp->close(ixp->dbp, 0);
    free(ixp->namp);
    free(ixp->filenamp);
    free(ixp->fieldv);
    free(ixp);
    return RDB_convert_err(ret);
}

/* Delete an index. */
int
RDB_delete_index(RDB_index *ixp, RDB_environment *envp, DB_TXN *txid)
{
    int ret = ixp->dbp->close(ixp->dbp, 0);
    if (ret != 0)
        goto cleanup;

    if (ixp->namp != NULL)
        ret = envp->envp->dbremove(envp->envp, txid, ixp->filenamp, ixp->namp, 0);

cleanup:
    free(ixp->namp);
    free(ixp->filenamp);
    free(ixp->fieldv);
    free(ixp);
    if (ret != 0)
        RDB_errmsg(envp, "cannot delete index: %s", RDB_strerror(ret));
    return RDB_convert_err(ret);
}

int
RDB_index_get_fields(RDB_index *ixp, RDB_field keyv[], int fieldc, DB_TXN *txid,
           RDB_field retfieldv[])
{
    DBT key, pkey, data;
    int ret;
    int i;

    for (i = 0; i < ixp->fieldc; i++) {
        keyv[i].no = ixp->fieldv[i];
    }

    /* Fill key DBT */
    ret = _RDB_fields_to_DBT(ixp->rmp, ixp->fieldc, keyv, &key);
    if (ret != RDB_OK)
        return ret;

    memset(&pkey, 0, sizeof (DBT));
    memset(&data, 0, sizeof (DBT));

    /* Get primary key and data */
    ret = ixp->dbp->pget(ixp->dbp, txid, &key, &pkey, &data, 0);
    if (ret != 0) {
        return RDB_convert_err(ret);
    }

    /* Get field values */
    for (i = 0; i < fieldc; i++) {
        int offs;
        int fno = retfieldv[i].no;

        if (fno < ixp->rmp->keyfieldcount) {
            offs = _RDB_get_field(ixp->rmp, fno, pkey.data, pkey.size,
                    &retfieldv[i].len, NULL);
            retfieldv[i].datap = ((RDB_byte *)pkey.data) + offs;
        } else {
            offs = _RDB_get_field(ixp->rmp, fno,
                    data.data, data.size, &retfieldv[i].len, NULL);
            retfieldv[i].datap = ((RDB_byte *)data.data) + offs;
        }
    }
    free(key.data);
    return RDB_OK;
}

int
RDB_index_delete_rec(RDB_index *ixp, RDB_field keyv[], DB_TXN *txid)
{
    DBT key;
    int ret;
    int i;

    for (i = 0; i < ixp->fieldc; i++) {
        keyv[i].no = ixp->fieldv[i];
    }

    /* Fill key DBT */
    ret = _RDB_fields_to_DBT(ixp->rmp, ixp->fieldc, keyv, &key);
    if (ret != RDB_OK)
        return ret;

    /* Delete record */
    ret = ixp->dbp->del(ixp->dbp, txid, &key, 0);
    if (ret != 0) {
        return RDB_convert_err(ret);
    }

    return RDB_OK;
}
