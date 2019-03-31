/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "indeximpl.h"
#include "envimpl.h"
#include "recmapimpl.h"
#include <obj/excontext.h>
#include <gen/strfns.h>

RDB_index *
RDB_create_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const RDB_field_descriptor fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*rmp->create_index_fn)(rmp, namp, filenamp, envp, fieldc, fieldv,
            cmpv, flags, rtxp, ecp);
}

RDB_index *
RDB_open_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    if (rmp->open_index_fn == NULL) {
        RDB_raise_not_supported("", ecp);
        return NULL;
    }
    return (*rmp->open_index_fn)(rmp, namp, filenamp, envp, fieldc, fieldv,
                cmpv, flags, rtxp, ecp);
}

int
RDB_close_index(RDB_index *ixp, RDB_exec_context *ecp)
{
    return (*ixp->close_index_fn)(ixp, ecp);
}

/* Delete an index. */
int
RDB_delete_index(RDB_index *ixp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*ixp->delete_index_fn)(ixp, rtxp, ecp);
}

int
RDB_index_get_fields(RDB_index *ixp, RDB_field keyv[], int fieldc, RDB_rec_transaction *rtxp,
           RDB_field retfieldv[], RDB_exec_context *ecp)
{
    return (*ixp->index_get_fields_fn)(ixp, keyv, fieldc, rtxp, retfieldv, ecp);
}

int
RDB_index_delete_rec(RDB_index *ixp, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*ixp->index_delete_rec_fn)(ixp, keyv, rtxp, ecp);
}

RDB_index *
RDB_new_index(RDB_recmap *rmp, const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_exec_context *ecp)
{
    int i;
    RDB_index *ixp = RDB_alloc(sizeof (RDB_index), ecp);

    if (ixp == NULL) {
        return NULL;
    }
    ixp->fieldv = NULL;
    ixp->rmp = rmp;
    ixp->flags = flags;

    ixp->namp = ixp->filenamp = NULL;
    if (name != NULL) {
        ixp->namp = RDB_dup_str(name);
        if (ixp->namp == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }
    if (filename != NULL) {
        ixp->filenamp = RDB_dup_str(filename);
        if (ixp->filenamp == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }

    ixp->fieldc = fieldc;
    ixp->fieldv = malloc(fieldc * sizeof(int));
    if (ixp->fieldv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    for (i = 0; i < fieldc; i++)
        ixp->fieldv[i] = fieldv[i];
    ixp->cmpv = 0;

    if (cmpv != NULL) {
        ixp->cmpv = RDB_alloc(sizeof (RDB_compare_field) * fieldc, ecp);
        if (ixp->cmpv == NULL)
            goto error;
        for (i = 0; i < fieldc; i++) {
            ixp->cmpv[i].comparep = cmpv[i].comparep;
            ixp->cmpv[i].arg = cmpv[i].arg;
            ixp->cmpv[i].asc = cmpv[i].asc;
        }
    }

    return ixp;

error:
    free(ixp->namp);
    free(ixp->filenamp);
    free(ixp->fieldv);
    free(ixp);
    return NULL;
}
