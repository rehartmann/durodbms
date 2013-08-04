/*
 * key.c
 *
 *  Created on: 02.08.2013
 *      Author: Rene Hartmann
 *
 * Key functions
 */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>

static RDB_bool
strvec_is_subset(const RDB_string_vec *v1p, const RDB_string_vec *v2p)
{
    int i;

    for (i = 0; i < v1p->strc; i++) {
        if (RDB_find_str(v2p->strc, v2p->strv, v1p->strv[i]) == -1)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

int
RDB_check_keys(const RDB_type *reltyp, int keyc, const RDB_string_vec keyv[],
        RDB_exec_context *ecp)
{
    int i, j;

    /* At least one key is required */
    if (keyc < 1) {
        RDB_raise_invalid_argument("key is required", ecp);
        return RDB_ERROR;
    }

    /*
     * Check all keys
     */
    for (i = 0; i < keyc; i++) {
        /* Check if all the key attributes appear in the type */
        for (j = 0; j < keyv[i].strc; j++) {
            if (RDB_tuple_type_attr(reltyp->def.basetyp, keyv[i].strv[j])
                    == NULL) {
                RDB_raise_invalid_argument("invalid key", ecp);
                return RDB_ERROR;
            }
        }

        /* Check if an attribute appears twice in a key */
        for (j = 0; j < keyv[i].strc - 1; j++) {
            /* Search attribute name in the remaining key */
            if (RDB_find_str(keyv[i].strc - j - 1, keyv[i].strv + j + 1,
                    keyv[i].strv[j]) != -1) {
                RDB_raise_invalid_argument("invalid key", ecp);
                return RDB_ERROR;
            }
        }
    }

    /* Check if a key is a subset of another */
    for (i = 0; i < keyc - 1; i++) {
        for (j = i + 1; j < keyc; j++) {
            if (keyv[i].strc <= keyv[j].strc) {
                if (strvec_is_subset(&keyv[i], &keyv[j])) {
                    RDB_raise_invalid_argument("invalid key", ecp);
                    return RDB_ERROR;
                }
            } else {
                if (strvec_is_subset(&keyv[j], &keyv[i])) {
                    RDB_raise_invalid_argument("invalid key", ecp);
                    return RDB_ERROR;
                }
            }
        }
    }
    return RDB_OK;
}

/*
 * Create a key over all attributes and store it in *keyp.
 */
int
RDB_all_key(int attrc, const RDB_attr *attrv, RDB_exec_context *ecp,
        RDB_string_vec *keyp)
{
    int i;

    keyp->strc = attrc;
    keyp->strv = RDB_alloc(sizeof (char *) * attrc, ecp);
    if (keyp->strv == NULL) {
        return RDB_ERROR;
    }
    for (i = 0; i < attrc; i++)
        keyp->strv[i] = attrv[i].name;
    return RDB_OK;
}

static int
find_key(const RDB_string_vec *keyp, int keyc,
        const RDB_string_vec keyv[])
{
    int i;

    for (i = 0; i < keyc; i++) {
        if (keyp->strc == keyv[i].strc
                && strvec_is_subset(keyp, &keyv[i])) {
            return i;
        }
    }
    return -1;
}

/*
 * Check if sets of keys are equal
 */
RDB_bool
RDB_keys_equal(int keyc1, const RDB_string_vec keyv1[],
        int keyc2, const RDB_string_vec keyv2[])
{
    int i;

    /*
     * It is assumed that both key sets are well-formed
     * (No keys that are a subset of another, no duplicate attributes)
     */

    if (keyc1 != keyc2)
        return RDB_FALSE;

    /* Check if each key appears in the other set of keys */
    for (i = 0; i < keyc1; i++) {
        if (find_key(&keyv1[i], keyc2, keyv2) == -1)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/**
 * Delete candidate keys.
 */
void
RDB_free_keys(int keyc, RDB_string_vec *keyv)
{
    int i;

    for (i = 0; i < keyc; i++) {
        RDB_free_strvec(keyv[i].strc, keyv[i].strv);
    }
    RDB_free(keyv);
}
