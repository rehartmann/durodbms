/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "insert.h"
#include "typeimpl.h"
#include "internal.h"
#include "stable.h"
#include <gen/strfns.h>
#include <string.h>

int
_RDB_insert_real(RDB_object *tbp, const RDB_object *tplp,
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_field *fvp;
    RDB_type *tuptyp = tbp->typ->var.basetyp;
    int attrcount = tuptyp->var.tuple.attrc;

    if (tbp->var.tb.stp == NULL) {
        /* Create physical table */
        if (_RDB_create_stored_table(tbp, txp != NULL ? txp->envp : NULL,
                NULL, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    fvp = RDB_alloc(sizeof(RDB_field) * attrcount, ecp);
    if (fvp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    for (i = 0; i < attrcount; i++) {
        int *fnop;
        RDB_object *valp;
        
        fnop = _RDB_field_no(tbp->var.tb.stp, tuptyp->var.tuple.attrv[i].name);
        valp = RDB_tuple_get(tplp, tuptyp->var.tuple.attrv[i].name);

        /* If there is no value, check if there is a default */
        if (valp == NULL) {
            valp = tuptyp->var.tuple.attrv[i].defaultp;
            if (valp == NULL) {
                RDB_raise_invalid_argument("missing default value", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }

        /* Typecheck */
        if (valp->typ == NULL) {
            RDB_type *attrtyp = tuptyp->var.tuple.attrv[i].typ;

            switch (valp->kind) {
                case RDB_OB_BOOL:
                case RDB_OB_INT:
                case RDB_OB_FLOAT:
                case RDB_OB_BIN:
                    RDB_raise_internal("invalid tuple attribute", ecp);
                    ret = RDB_ERROR;
                    goto cleanup;
                case RDB_OB_INITIAL:
                    if (!RDB_type_is_scalar(attrtyp)) {
                        RDB_raise_type_mismatch("attribute type must be scalar",
                                ecp);
                        ret = RDB_ERROR;
                        goto cleanup;
                    }
                    break;
                case RDB_OB_TUPLE:
                    if (attrtyp->kind != RDB_TP_TUPLE) {
                        RDB_raise_type_mismatch("attribute must be tuple",
                                ecp);
                        ret = RDB_ERROR;
                        goto cleanup;
                    }
                    break;
                case RDB_OB_TABLE:
                    if (attrtyp->kind != RDB_TP_RELATION) {
                        RDB_raise_type_mismatch("attribute must be table",
                                ecp);
                        ret = RDB_ERROR;
                        goto cleanup;
                     }
                     break;
                case RDB_OB_ARRAY:
                    if (attrtyp->kind != RDB_TP_ARRAY) {
                        RDB_raise_type_mismatch("attribute must be array",
                                ecp);
                        ret = RDB_ERROR;
                        goto cleanup;
                    }
                    break;
            }
        } else {
            if (!RDB_type_equals(valp->typ, tuptyp->var.tuple.attrv[i].typ)) {
                RDB_raise_type_mismatch(
                        "tuple attribute type does not match table attribute type",
                        ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }

        /* Set type - needed for tuple and array attributes */
        if (valp->typ == NULL) {
            RDB_obj_set_type(valp, tuptyp->var.tuple.attrv[i].typ);
            /* !! check object kind against type? */
        }

        ret = _RDB_obj_to_field(&fvp[*fnop], valp, ecp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
    }

    _RDB_cmp_ecp = ecp;
    ret = RDB_insert_rec(tbp->var.tb.stp->recmapp, fvp,
            tbp->var.tb.is_persistent ? txp->txid : NULL);
    if (ret == DB_KEYEXIST) {
        /* check if the tuple is an element of the table */
        if (RDB_contains_rec(tbp->var.tb.stp->recmapp, fvp,
                tbp->var.tb.is_persistent ? txp->txid : NULL) == RDB_OK) {
            RDB_raise_element_exists("tuple is already in table", ecp);
        } else {
            _RDB_handle_errcode(ret, ecp, txp);
        }
        ret = RDB_ERROR;
    } else if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        ret = RDB_ERROR;
    }
    if (ret == RDB_OK) {
        tbp->var.tb.stp->est_cardinality++;
    }

cleanup:
    RDB_free(fvp);
    return ret;
}
