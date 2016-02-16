/*
 * Copyright (C) 2003-2008, 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "insert.h"
#include "typeimpl.h"
#include "internal.h"
#include "stable.h"
#include <gen/strfns.h>
#include <rec/sequence.h>

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
    RDB_object serial_val;

    /*
     * If the tuple has type information, check if all attributes are present in the
     * destination table
     */
    if (tplp->typ != NULL) {
        int i;
        int attrc;
        RDB_attr *attrv = RDB_type_attrs(tplp->typ, &attrc);
        if (attrv != NULL) {
            for (i = 0; i < attrc; i++) {
                if (RDB_tuple_type_attr(tuptyp, attrv[i].name) == NULL) {
                    RDB_raise_type_mismatch(
                            "tuple attribute not found in destination table",
                            ecp);
                    return RDB_ERROR;
                }
            }
        }
    }

    if (tbp->val.tbp->stp == NULL) {
        /* Create physical table */
        if (RDB_provide_stored_table(tbp, RDB_TRUE, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    RDB_init_obj(&serial_val);

    fvp = RDB_alloc(sizeof(RDB_field) * attrcount, ecp);
    if (fvp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    for (i = 0; i < attrcount; i++) {
        RDB_int nextval;
        int *fnop = RDB_field_no(tbp->val.tbp->stp, tuptyp->def.tuple.attrv[i].name);
        RDB_attr_default *dflp = NULL;
        RDB_object *valp = RDB_tuple_get(tplp, tuptyp->def.tuple.attrv[i].name);
        RDB_type *attrtyp = tuptyp->def.tuple.attrv[i].typ;

        if (tbp->val.tbp->default_map != NULL) {
            dflp = RDB_hashmap_get(tbp->val.tbp->default_map,
                    tuptyp->def.tuple.attrv[i].name);
        }

        /* If there is no value, check if there is a default */
        if (valp == NULL) {
            if (dflp == NULL) {
                RDB_raise_invalid_argument("missing value", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
            if (RDB_expr_is_serial(dflp->exp)) {
                if (!RDB_table_is_persistent(tbp)) {
                    RDB_raise_internal("Default value not supported for local tables", ecp);
                    ret = RDB_ERROR;
                    goto cleanup;
                }
                if (dflp->seqp == NULL) {
                    RDB_object seqname;
                    RDB_init_obj(&seqname);

                    if (RDB_seq_container_name(RDB_table_name(tbp),
                                tuptyp->def.tuple.attrv[i].name, &seqname,
                                ecp) != RDB_OK) {
                        RDB_destroy_obj(&seqname, ecp);
                        ret = RDB_ERROR;
                        goto cleanup;
                    }

                    ret = RDB_open_sequence(RDB_obj_string(&seqname),
                            RDB_DATAFILE, RDB_db_env(RDB_tx_db(txp)), txp->txid, &dflp->seqp);
                    RDB_destroy_obj(&seqname, ecp);
                    if (ret != 0) {
                        RDB_handle_errcode(ret, ecp, txp);
                        ret = RDB_ERROR;
                        goto cleanup;
                    }
                }
                ret = RDB_sequence_next(dflp->seqp, txp->txid, &nextval);
                if (ret != 0) {
                    RDB_handle_errcode(ret, ecp, txp);
                    ret = RDB_ERROR;
                    goto cleanup;
                }
                RDB_int_to_obj(&serial_val, nextval);
                valp = &serial_val;
            } else {
                valp = RDB_expr_obj(dflp->exp);
                if (valp == NULL) {
                    RDB_raise_internal("invalid default value", ecp);
                    ret = RDB_ERROR;
                    goto cleanup;
                }
            }
        } else {
            if (dflp != NULL && RDB_expr_is_serial(dflp->exp)) {
                RDB_raise_invalid_argument("explicit value not permitted", ecp);
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
            if (!RDB_is_subtype(valp->typ, attrtyp)) {
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
    ret = RDB_insert_rec(tbp->val.tbp->stp->recmapp, fvp,
            RDB_table_is_persistent(tbp) ? txp->txid : NULL);
    if (ret == DB_KEYEXIST) {
        /* check if the tuple is an element of the table */
        if (RDB_contains_rec(tbp->val.tbp->stp->recmapp, fvp,
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
        tbp->val.tbp->stp->est_cardinality++;
    }

cleanup:
    RDB_destroy_obj(&serial_val, ecp);
    RDB_free(fvp);
    return ret;
}
