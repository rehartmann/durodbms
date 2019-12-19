/*
 * Record map functions
 * 
 * Copyright (C) 2003-2012, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "recmapimpl.h"
#include "envimpl.h"
#include "dbdefs.h"
#include <gen/strfns.h>
#include <obj/object.h>
#include <obj/excontext.h>
#include <treerec/treerecmap.h>

#include <string.h>
#include <math.h>

#ifdef _WIN32
#include <winsock.h>
#else
#include <arpa/inet.h>
#endif

/*
 * Create a recmap with the <var>name</var> specified by name in the DB environment
 * pointed to by envp in the file specified by filename.
 * If envp is NULL, or if the environment is a Berkeley DB environment and filename is NULL,
 * a transient recmap is created.
 * The recmap will have fieldc fields, the length of field i
 * is given by fieldlenv[i].
 * The first keyfcnt fields constitute the primary index.
 * If a recmap with name <var>name</var> already exists and envp is NULL,
 * the existing recmap is deleted.
 */
RDB_recmap *
RDB_create_recmap(const char *name, RDB_environment *envp,
		int fieldc, const RDB_field_info fieldinfov[], int keyfieldc,
        int cmpc, const RDB_compare_field cmpv[], int flags,
        int keyc, const RDB_string_vec *keyv,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    if (envp == NULL)
        return RDB_create_tree_recmap(fieldc, fieldinfov,
            keyfieldc, cmpc, cmpv, flags, keyc, keyv, NULL, ecp);
    return (*envp->create_recmap_fn)(name, envp, fieldc, fieldinfov,
            keyfieldc, cmpc, cmpv, flags, keyc, keyv, rtxp, ecp);
}

/*
 * Open a recmap. For a description of the arguments, see RDB_create_recmap().
 * envp may not be NULL.
 */
RDB_recmap *
RDB_open_recmap(const char *name, RDB_environment *envp,
       int fieldc, const RDB_field_info fieldinfov[], int keyfieldc,
       RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    return (*envp->open_recmap_fn)(name, envp, fieldc, fieldinfov,
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
 * Delete from a recmap the record whose values are given by fieldv.
 * It is implementation-dependent if only delete by key fields or only value
 * fields may be passed.
 * fieldv[..].no is ignored.
 */
int
RDB_delete_rec(RDB_recmap *rmp, int fieldc, RDB_field fieldv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*rmp->delete_rec_fn)(rmp, fieldc, fieldv, rtxp, ecp);
}

/*
 * Read field values of the record from a recmap using the primary index.
 * Arguments:
 * rm       pointer to the recmap.
 * keyv     array of the key fields specifying the record to read.
 * txid     pointer to a Berkeley DB transaction
 * fieldc   how many fields are to be read
 * retfieldv    the fields to read.
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

RDB_bool
RDB_recmap_delayed_deletion(RDB_recmap *rmp)
{
    return rmp->delayed_deletion;
}

/*
 * Allocate a RDB_recmap structure and initialize its storage-independent fields.
 */
RDB_recmap *
RDB_new_recmap(const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
        int keyfieldc, int flags, RDB_exec_context *ecp)
{
    int i;
    RDB_recmap *rmp = RDB_alloc(sizeof(RDB_recmap), ecp);

    if (rmp == NULL)
        return NULL;

    rmp->envp = envp;
    rmp->filenamp = NULL;
    rmp->fieldinfos = NULL;
    if (namp != NULL) {
        rmp->namp = RDB_dup_str(namp);
        if (rmp->namp == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
        rmp->namp = NULL;
    }

    if (filenamp != NULL) {
        rmp->filenamp = RDB_dup_str(filenamp);
        if (rmp->filenamp == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
        rmp->filenamp = NULL;
    }

    rmp->fieldinfos = RDB_alloc(sizeof(RDB_field_info) * fieldc, ecp);
    if (rmp->fieldinfos == NULL) {
        goto error;
    }

    rmp->fieldcount = fieldc;
    rmp->keyfieldcount = keyfieldc;
    rmp->cmpv = NULL;
    rmp->dup_keys = (RDB_bool) !(RDB_UNIQUE & flags);

    rmp->varkeyfieldcount = rmp->vardatafieldcount = 0;
    for (i = 0; i < fieldc; i++) {
        rmp->fieldinfos[i].len = fieldinfov[i].len;
        if (fieldinfov[i].len == RDB_VARIABLE_LEN) {
            if (i < rmp->keyfieldcount) {
                /* It's a key field */
                rmp->varkeyfieldcount++;
            } else {
                /* It's a data field */
                rmp->vardatafieldcount++;
            }
        }
        rmp->fieldinfos[i].flags = fieldinfov[i].flags;
        rmp->fieldinfos[i].attrname = fieldinfov[i].attrname;
    }
    rmp->delayed_deletion = RDB_FALSE;

    return rmp;

error:
    RDB_free(rmp->namp);
    RDB_free(rmp->filenamp);
    RDB_free(rmp->fieldinfos);
    RDB_free(rmp);
    return NULL;
}

int
RDB_fdb_transform_fields(int fieldc, RDB_field dstv[], const RDB_field srcv[],
		RDB_field_info finfov[], RDB_exec_context *ecp)
{
	int i;

	for (i = 0; i < fieldc; i++) {
		if (RDB_FTYPE_CHAR & finfov[srcv[i].no].flags) {
			size_t dstlen;

			void *tdst = RDB_alloc(srcv[i].len, ecp);
			if (tdst == NULL) {
				return RDB_ERROR;
			}
			(*srcv[i].copyfp)(tdst, srcv[i].datap, srcv[i].len);
			dstlen = strxfrm(NULL, tdst, 0) + 1;
			dstv[i].datap = malloc(dstlen);
			if (dstv[i].datap == NULL) {
				RDB_free(tdst);
				RDB_raise_no_memory(ecp);
				return RDB_ERROR;
			}
			dstv[i].len = dstlen;
			if (strxfrm(dstv[i].datap, tdst, dstlen) >= dstlen) {
				RDB_raise_internal("strxfrm() failed", ecp);
				return RDB_ERROR;
			}
			RDB_free(tdst);
		} else if (RDB_FTYPE_INTEGER & finfov[srcv[i].no].flags) {
			RDB_int val;

			dstv[i].datap = malloc(sizeof(RDB_int));
			if (dstv[i].datap == NULL) {
				RDB_raise_no_memory(ecp);
				return RDB_ERROR;
			}
			(*srcv[i].copyfp)(&val, srcv[i].datap, sizeof(RDB_int));
			*((uint32_t *) dstv[i].datap) = htonl((uint32_t) val);
			dstv[i].len = sizeof(RDB_int);
		} else if (RDB_FTYPE_FLOAT & finfov[srcv[i].no].flags) {
			static const RDB_float MINUS_ONE = -1.0;
			RDB_float val;
			int j;
			uint8_t *dstdatap = malloc(sizeof(RDB_float));
			if (dstdatap == NULL) {
				RDB_raise_no_memory(ecp);
				return RDB_ERROR;
			}

			(*srcv[i].copyfp)(&val, srcv[i].datap, sizeof(RDB_float));

			if (isnan(val)) {
				/* Use canonical NaN */
				val = nan("");
			}

			/* If little endian, invert byte order */
			if (((uint8_t*) &MINUS_ONE)[0] & 1) {
				*((RDB_float *) dstdatap) = val;
			} else {
				uint8_t *srcp = (uint8_t *)&val;

				for (j = 0; j < sizeof(RDB_float); j++) {
					dstdatap[j] = srcp[sizeof(RDB_float) - 1 - j];
				}
			}
			if (dstdatap[0] & 128) {
				/* Number is negative - invert all bits */
				for (j = 0; j < sizeof(RDB_float); j++) {
				    dstdatap[j] = ~dstdatap[j];
				}
			} else {
				/* Number is positive - invert sign bit */
				dstdatap[0] ^= 128;
			}
			dstv[i].datap = dstdatap;
			dstv[i].len = sizeof(RDB_float);
		} else {
			dstv[i].datap = malloc(srcv[i].len);
			if (dstv[i].datap == NULL) {
				RDB_raise_no_memory(ecp);
				return RDB_ERROR;
			}
			(*srcv[i].copyfp)(dstv[i].datap, srcv[i].datap, srcv[i].len);
			dstv[i].len = srcv[i].len;
		}
		dstv[i].copyfp = &memcpy;
		dstv[i].no = srcv[i].no;
	}
	return RDB_OK;
}
