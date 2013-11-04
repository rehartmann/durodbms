/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "insert.h"
#include "typeimpl.h"
#include "internal.h"
#include "stable.h"
#include <gen/strfns.h>
#include <string.h>

int
RDB_insert_real(RDB_object *tbp, const RDB_object *tplp,
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_field *fvp;
    RDB_type *tuptyp = tbp->typ->def.basetyp;
    int attrcount = tuptyp->def.tuple.attrc;

    if (tbp->val.tb.stp == NULL) {
        /* Create physical table */
        if (RDB_create_stored_table(tbp, txp != NULL ? txp->envp : NULL,
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
        RDB_expression *dflexp;
        RDB_object *valp;
        RDB_type *attrtyp = tuptyp->def.tuple.attrv[i].typ;

        fnop = RDB_field_no(tbp->val.tb.stp, tuptyp->def.tuple.attrv[i].name);
        valp = RDB_tuple_get(tplp, tuptyp->def.tuple.attrv[i].name);

        /* If there is no value, check if there is a default */
        if (valp == NULL) {
            if (tbp->val.tb.default_map == NULL) {
                RDB_raise_invalid_argument("missing value", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
            dflexp = RDB_hashmap_get(tbp->val.tb.default_map,
                    tuptyp->def.tuple.attrv[i].name);
            if (dflexp == NULL) {
                RDB_raise_invalid_argument("missing value", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
            valp = RDB_expr_obj(dflexp);
            if (valp == NULL) {
                RDB_raise_internal("invalid default value", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }

        /* Typecheck */
        if (valp->typ == NULL) {
            if (valp->kind != RDB_OB_TUPLE && valp->kind != RDB_OB_ARRAY) {
                RDB_raise_invalid_argument("missing type information", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        } else {
            if (!RDB_type_equals(valp->typ, attrtyp)) {
                RDB_raise_type_mismatch(
                        "tuple attribute type does not match table attribute type",
                        ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }

        /* Set type information for storage */
        valp->store_typ = attrtyp;

        ret = RDB_obj_to_field(&fvp[*fnop], valp, ecp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
    }

    RDB_cmp_ecp = ecp;
    ret = RDB_insert_rec(tbp->val.tb.stp->recmapp, fvp,
            RDB_table_is_persistent(tbp) ? txp->txid : NULL);
    if (ret == DB_KEYEXIST) {
        /* check if the tuple is an element of the table */
        if (RDB_contains_rec(tbp->val.tb.stp->recmapp, fvp,
                RDB_table_is_persistent(tbp) ? txp->txid : NULL) == RDB_OK) {
            RDB_raise_element_exists("tuple is already in table", ecp);
        } else {
            RDB_handle_errcode(ret, ecp, txp);
        }
        ret = RDB_ERROR;
    } else if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        ret = RDB_ERROR;
    }
    if (ret == RDB_OK) {
        tbp->val.tb.stp->est_cardinality++;
    }

cleanup:
    RDB_free(fvp);
    return ret;
}
