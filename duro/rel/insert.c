/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

int
_RDB_insert_real(RDB_table *tbp, const RDB_object *tplp,
                   RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_field *fvp;
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int attrcount = tuptyp->var.tuple.attrc;

    if (tbp->stp == NULL) {
        /* Create physical table */
        ret = _RDB_create_stored_table(tbp, txp != NULL ? txp->envp : NULL,
                NULL, txp);
        if (ret != RDB_OK) {
            return ret;
        }
    }

    fvp = malloc(sizeof(RDB_field) * attrcount);
    if (fvp == NULL) {
        if (txp != NULL) {
            RDB_rollback_all(txp);
        }
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    for (i = 0; i < attrcount; i++) {
        int *fnop;
        RDB_object *valp;
        
        fnop = RDB_hashmap_get(&tbp->stp->attrmap,
                tuptyp->var.tuple.attrv[i].name, NULL);
        valp = RDB_tuple_get(tplp, tuptyp->var.tuple.attrv[i].name);

        /* If there is no value, check if there is a default */
        if (valp == NULL) {
            valp = tuptyp->var.tuple.attrv[i].defaultp;
            if (valp == NULL) {
                ret = RDB_INVALID_ARGUMENT;
                goto cleanup;
            }
        }
        
        /* Typecheck */
        if (valp->typ == NULL) {
            RDB_type *attrtyp = tuptyp->var.tuple.attrv[i].typ;

            switch (valp->kind) {
                case RDB_OB_BOOL:
                case RDB_OB_INT:
                case RDB_OB_RATIONAL:
                case RDB_OB_BIN:
                    ret = RDB_INTERNAL;
                    goto cleanup;
                case RDB_OB_INITIAL:
                    if (!RDB_type_is_scalar(attrtyp)) {
                        ret = RDB_TYPE_MISMATCH;
                        goto cleanup;
                    }
                    break;
                case RDB_OB_TUPLE:
                    if (attrtyp->kind != RDB_TP_TUPLE) {
                        ret = RDB_TYPE_MISMATCH;
                        goto cleanup;
                    }
                    break;
                case RDB_OB_TABLE:
                    if (attrtyp->kind != RDB_TP_RELATION) {
                        ret = RDB_TYPE_MISMATCH;
                        goto cleanup;
                     }
                     break;
                case RDB_OB_ARRAY:
                    if (attrtyp->kind != RDB_TP_ARRAY) {
                        ret = RDB_TYPE_MISMATCH;
                        goto cleanup;
                    }
                    break;
            }
        } else {
            if (!RDB_type_equals(valp->typ, tuptyp->var.tuple.attrv[i].typ))
                return RDB_TYPE_MISMATCH;
        }

        /* Set type - needed for tuple and array attributes */
        if (valp->typ == NULL
                && (valp->kind == RDB_OB_TUPLE
                 || valp->kind == RDB_OB_ARRAY))
            valp->typ =  tuptyp->var.tuple.attrv[i].typ;
            /* !! check object kind against type? */

        ret = _RDB_obj_to_field(&fvp[*fnop], valp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_insert_rec(tbp->stp->recmapp, fvp,
            tbp->is_persistent ? txp->txid : NULL);
    if (RDB_is_syserr(ret)) {
        if (txp != NULL) {
            RDB_errmsg(txp->dbp->dbrootp->envp, "cannot insert record: %s",
                    RDB_strerror(ret));
            _RDB_handle_syserr(txp, ret);
        }
    } else if (ret == RDB_KEY_VIOLATION) {
        /* check if the tuple is an element of the table */
        if (RDB_contains_rec(tbp->stp->recmapp, fvp,
                tbp->is_persistent ? txp->txid : NULL) == RDB_OK)
            ret = RDB_ELEMENT_EXISTS;
    }
    tbp->stp->est_cardinality++;

cleanup:
    free(fvp);
    return ret;
}

int
RDB_insert(RDB_table *tbp, const RDB_object *tplp, RDB_transaction *txp)
{
    RDB_ma_insert ins;

    ins.tbp = tbp;
    ins.tplp = (RDB_object *) tplp;
    return RDB_multi_assign(1, &ins, 0, NULL, 0, NULL, 0, NULL, txp);
}
