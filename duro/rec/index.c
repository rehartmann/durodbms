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
    int res;
    int i;
    RDB_index *ixp = malloc(sizeof (RDB_index));

    if (ixp == NULL) {
        return RDB_NO_MEMORY;
    }
    ixp->fieldv = NULL;
    ixp->rmp = rmp;
    
    ixp->namp = RDB_dup_str(name);
    ixp->filenamp = RDB_dup_str(name);
    if (ixp->namp == NULL || ixp->filenamp == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }

    ixp->fieldc = fieldc;
    ixp->fieldv = malloc(fieldc * sizeof(int));
    if (ixp->fieldv == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    for (i = 0; i < fieldc; i++)
        ixp->fieldv[i] = fieldv[i];

    res = db_create(&ixp->dbp, dsp->envp, 0);
    if (res != 0) {
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
    return res;
}

/* Fill the DBT for the secondary key */
static int
make_skey(DB *dbp, const DBT *pkeyp, const DBT *pdatap, DBT *skeyp)
{
    RDB_index *ixp = dbp->app_private;
    RDB_field *fieldv;
    int res;
    int i;
    
    fieldv = malloc (sizeof(RDB_field) * ixp->fieldc);
    if (fieldv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < ixp->fieldc; i++)
        fieldv[i].no = ixp->fieldv[i];
    res = _RDB_get_fields(ixp->rmp, pkeyp, pdatap, ixp->fieldc, fieldv);
    if (res != RDB_OK) {
        free(fieldv);
        return res;
    }

    memset(skeyp, 0, sizeof (DBT));
    res = _RDB_fields_to_DBT(ixp->rmp, ixp->fieldc, fieldv, skeyp);
    skeyp->flags = DB_DBT_APPMALLOC;
    free(fieldv);
    return res;
}

int
RDB_create_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        RDB_bool unique, DB_TXN *txid, RDB_index **ixpp)
{
    RDB_index *ixp;
    int res;
   
    res = create_index(rmp, namp, filenamp, dsp, fieldc, fieldv, unique, &ixp);
    if (res != RDB_OK)
        return res;

    res = ixp->dbp->open(ixp->dbp, txid, filenamp, namp, DB_HASH, DB_CREATE, 0664);
    if (res != 0)
        goto error;
        
    /* attach index to BDB database (for the callback) */
    ixp->dbp->app_private = ixp;

    /* associate the index DB with the recmap DB */
    res = ixp->dbp->associate(rmp->dbp, txid, ixp->dbp, make_skey, DB_CREATE);
    if (res != 0)
        goto error;    
   
    *ixpp = ixp;
    return RDB_OK;
error:
    RDB_delete_index(ixp, dsp);
    return res;
}

int
RDB_open_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[], RDB_bool unique,
        DB_TXN *txid, RDB_index **ixpp)
{
    RDB_index *ixp;
    int res;

    res = create_index(rmp, namp, filenamp, dsp, fieldc, fieldv, unique, &ixp);
    if (res != RDB_OK)
        return res;

    res = ixp->dbp->open(ixp->dbp, txid, filenamp, namp, DB_UNKNOWN, 0, 0664);
    if (res != 0)
        goto error; 

    /* attach index to BDB database (for the callback) */
    ixp->dbp->app_private = ixp;

    /* associate the index DB with the recmap DB */
    res = ixp->dbp->associate(rmp->dbp, txid, ixp->dbp, make_skey, 0);
    if (res != 0)
        goto error;    

    *ixpp = ixp;
    return RDB_OK;
error:
    RDB_close_index(ixp);
    return res;
}

int
RDB_close_index(RDB_index *ixp)
{
    int res = ixp->dbp->close(ixp->dbp, 0);
    free(ixp->namp);
    free(ixp->filenamp);
    free(ixp->fieldv);
    free(ixp);
    return res;
}

/* Delete an index. */
int
RDB_delete_index(RDB_index *ixp, RDB_environment *envp)
{
    int res = ixp->dbp->close(ixp->dbp, 0);
    free(ixp->namp);
    free(ixp->filenamp);
    free(ixp->fieldv);
    free(ixp);
    /* ... */
    return res;
}
