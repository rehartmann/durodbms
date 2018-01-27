/*
 * Record map functions
 * 
 * Copyright (C) 2003-2012, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "recmapimpl.h"
#include "envimpl.h"
#include "dbdefs.h"
#include <bdbrec/bdbrecmap.h>

/*
 * Create a recmap with the <var>name</var> specified by name in the DB environment
 * pointed to by envp in the file specified by filename.
 * If filename is NULL, a transient recmap is created.
 * The recmap will have fieldc fields, the length of field i
 * is given by fieldlenv[i].
 * The first keyfcnt fields constitute the primary index.
 * If a recmap with name <var>name</var> already exists and envp is NULL,
 * the existing recmap is deleted.
 */
RDB_recmap *
RDB_create_recmap(const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const int fieldlenv[], int keyfieldc,
        const RDB_compare_field cmpv[], int flags,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    if (envp == NULL)
        return RDB_create_bdb_recmap(name, filename, envp, fieldc, fieldlenv,
            keyfieldc, cmpv, flags, rtxp, ecp);
    return (*envp->create_recmap_fn)(name, filename, envp, fieldc, fieldlenv,
            keyfieldc, cmpv, flags, rtxp, ecp);
}

/* Open a recmap. For a description of the arguments, see RDB_create_recmap(). */
RDB_recmap *
RDB_open_recmap(const char *name, const char *filename,
       RDB_environment *envp, int fieldc, const int fieldlenv[], int keyfieldc,
       RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    if (envp == NULL)
        return RDB_open_recmap(name, filename, envp, fieldc, fieldlenv,
            keyfieldc, rtxp, ecp);
    return (*envp->open_recmap_fn)(name, filename, envp, fieldc, fieldlenv,
            keyfieldc, rtxp, ecp);
}

/* Close a recmap. */
int
RDB_close_recmap(RDB_recmap *rmp, RDB_exec_context *ecp)
{
    return (*rmp->close_recmap_fn)(rmp, ecp);
}

/* Delete a recmap. */
int
RDB_delete_recmap(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    return (*rmp->delete_recmap_fn)(rmp, rtxp, ecp);
}

/*
 * Insert a record into a recmap. The values are given by valv.
 * valv[..].no is ignored.
 */
int
RDB_insert_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*rmp->insert_rec_fn)(rmp, flds, rtxp, ecp);
}

/* Update the record whose key values are given by keyv.
 * The new values are given by fieldv.
 * keyv[..].no is ignored.
 */
int
RDB_update_rec(RDB_recmap *rmp, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], RDB_rec_transaction *rtxp,
               RDB_exec_context *ecp)
{
    return (*rmp->update_rec_fn)(rmp, keyv, fieldc, fieldv, rtxp, ecp);
}

/*
 * Delete from a recmap the record whose key values are given by keyv.
 * keyv[..].no is ignored.
 */
int
RDB_delete_rec(RDB_recmap *rmp, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*rmp->delete_rec_fn)(rmp, keyv, rtxp, ecp);
}

/*
 * Read field values of the record from a recmap using the primary index.
 * Arguments:
 * rm       pointer to the recmap.
 * keyv     array of the key fields specifying the record to read.
 * txid     pointer to a Berkeley DB transaction
 * fieldc   how many fields are to be read
 * resfieldv    the fields to read.
 *              no must be set by the caller,
 *              size and datap are set by RDB_get_fields().
 *              Must not contain key fields.
 */
int
RDB_get_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    return (*rmp->get_fields_fn)(rmp, keyv, fieldc, rtxp, retfieldv, ecp);
}

/*
 * Check if the recmap contains the record whose values are given by valv.
 * Return RDB_OK if yes, RDB_ERROR with RDB_NOT_FOUND_FOUND_ERROR in *ecp if no.
 * valv[..].no is ignored.
 */
int
RDB_contains_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*rmp->contains_rec_fn)(rmp, flds, rtxp, ecp);
}

/*
 * Get the number of records in the recmap and store it in *sz,
 * using the 'fast' method (not traversing the BDB database),
 * so the result may not be accurate.
 */
int
RDB_recmap_est_size(RDB_recmap *rmp, RDB_rec_transaction *rtxp, unsigned *sz,
        RDB_exec_context *ecp)
{
    return (*rmp->recmap_est_size_fn)(rmp, rtxp, sz, ecp);
}
