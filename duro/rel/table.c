/*
 * $Id$
 *
 * Copyright (C) 2003-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "catalog.h"
#include "qresult.h"
#include "insert.h"
#include "optimize.h"
#include "internal.h"

#include <gen/strfns.h>
#include <string.h>

static RDB_string_vec *
dup_keyv(int keyc, const RDB_string_vec keyv[])
{
    return _RDB_dup_rename_keys(keyc, keyv, NULL);
}

int
_RDB_init_table(RDB_object *tbp, const char *name, RDB_bool persistent,
        RDB_type *reltyp, int keyc, const RDB_string_vec keyv[], RDB_bool usr,
        RDB_expression *exp, RDB_exec_context *ecp)
{
    int i;
    RDB_string_vec allkey; /* Used if keyv is NULL */
    int attrc = reltyp->var.basetyp->var.tuple.attrc;

    tbp->kind = RDB_OB_TABLE;
    tbp->var.tb.is_user = usr;
    tbp->var.tb.is_persistent = persistent;
    tbp->var.tb.keyv = NULL;
    tbp->var.tb.stp = NULL;

    if (name != NULL) {
        tbp->var.tb.name = RDB_dup_str(name);
        if (tbp->var.tb.name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
    	tbp->var.tb.name = NULL;
    }

    allkey.strv = NULL;
    if (exp != NULL) {
        /* Key is inferred from exp 'on demand' */
        tbp->var.tb.keyv = NULL;
    } else {
        if (keyv == NULL) {
            /* Create key for all-key table */
            allkey.strc = attrc;
            allkey.strv = malloc(sizeof (char *) * attrc);
            if (allkey.strv == NULL) {
                RDB_raise_no_memory(ecp);
                goto error;
            }
            for (i = 0; i < attrc; i++)
                allkey.strv[i] = reltyp->var.basetyp->var.tuple.attrv[i].name;
            keyc = 1;
            keyv = &allkey;
        }

        /* Copy candidate keys */
        tbp->var.tb.keyv = dup_keyv(keyc, keyv);
        if (tbp->var.tb.keyv == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        tbp->var.tb.keyc = keyc;
    }
    tbp->var.tb.exp = exp;

    tbp->typ = reltyp;

    free(allkey.strv);

    return RDB_OK;

error:
    /* Clean up */
    if (tbp != NULL) {
        free(tbp->var.tb.name);
        if (tbp->var.tb.keyv != NULL)
            _RDB_free_keys(tbp->var.tb.keyc, tbp->var.tb.keyv);
    }
    free(allkey.strv);
    return RDB_ERROR;
}

int
RDB_init_table(RDB_object *tbp, const char *name,
        RDB_type *reltyp, int keyc, const RDB_string_vec keyv[],
        RDB_exec_context *ecp)
{
    return _RDB_init_table(tbp, name, RDB_FALSE, reltyp, keyc, keyv,
            RDB_TRUE, NULL, ecp);
}

/*
 * Creates a stored table, but not the recmap and the indexes
 * and does not insert the table into the catalog.
 * reltyp is consumed on success (must not be freed by caller).
 */
static RDB_object *
new_rtable(const char *name, RDB_bool persistent,
           RDB_type *reltyp,
           int keyc, const RDB_string_vec keyv[], RDB_bool usr,
           RDB_exec_context *ecp)
{
	int ret;
    RDB_object *tbp = _RDB_new_obj(ecp);
    if (tbp == NULL)
        return NULL;

    ret = _RDB_init_table(tbp, name, persistent, reltyp, keyc, keyv, usr,
            NULL, ecp);
    if (ret != RDB_OK) {
    	free(tbp);
    	return NULL;
    }
    return tbp;
}

/*
 * Like new_rtable(), but uses attrc and heading instead of reltype.
 */
RDB_object *
_RDB_new_rtable(const char *name, RDB_bool persistent,
                int attrc, const RDB_attr heading[],
                int keyc, const RDB_string_vec keyv[], RDB_bool usr,
                RDB_exec_context *ecp)
{
    int ret;
    int i;
    RDB_object *tbp;
    RDB_type *reltyp = RDB_create_relation_type(attrc, heading, ecp);
    if (reltyp == NULL) {
        return NULL;
    }

    for (i = 0; i < attrc; i++) {
        if (heading[i].defaultp != NULL) {
            RDB_type *tuptyp = reltyp->var.basetyp;

            tuptyp->var.tuple.attrv[i].defaultp = malloc(sizeof (RDB_object));
            if (tuptyp->var.tuple.attrv[i].defaultp == NULL) {
                RDB_drop_type(reltyp, ecp, NULL);
                RDB_raise_no_memory(ecp);
                return NULL;
            }
            RDB_init_obj(tuptyp->var.tuple.attrv[i].defaultp);
            ret = RDB_copy_obj(tuptyp->var.tuple.attrv[i].defaultp,
                    heading[i].defaultp, ecp);
            if (ret != RDB_OK) {
                RDB_drop_type(reltyp, ecp, NULL);
                return NULL;
            }                
        }
    }

    tbp = new_rtable(name, persistent, reltyp, keyc, keyv, usr, ecp);
    if (tbp == NULL) {
        RDB_drop_type(reltyp, ecp, NULL);
        return NULL;
    }
    return tbp;
}

int
RDB_table_keys(RDB_object *tbp, RDB_exec_context *ecp, RDB_string_vec **keyvp)
{
    RDB_bool freekey;

    if (tbp->var.tb.keyv == NULL) {
        int keyc;
        RDB_string_vec *keyv;

        keyc = _RDB_infer_keys(tbp->var.tb.exp, ecp, &keyv, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;
        if (freekey) {
            tbp->var.tb.keyv = keyv;
        } else {
            tbp->var.tb.keyv = dup_keyv(keyc, keyv);
            if (tbp->var.tb.keyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
        }
        tbp->var.tb.keyc = keyc;
    }

    if (keyvp != NULL)
        *keyvp = tbp->var.tb.keyv;

    return tbp->var.tb.keyc;
}

int
RDB_drop_table_index(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    int xi;
    char *tbname;
    RDB_object *tbp;

    if (!_RDB_legal_name(name)) {
        RDB_raise_not_found("invalid index name", ecp);
        return RDB_ERROR;
    }

    ret = _RDB_cat_index_tablename(name, &tbname, ecp, txp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    tbp = RDB_get_table(tbname, ecp, txp);
    free(tbname);
    if (tbp == NULL)
        return RDB_ERROR;

    for (i = 0; i < tbp->var.tb.stp->indexc
            && strcmp(tbp->var.tb.stp->indexv[i].name, name) != 0;
            i++);
    if (i >= tbp->var.tb.stp->indexc) {
        /* Index not found, so reread indexes */
        for (i = 0; i < tbp->var.tb.stp->indexc; i++)
            _RDB_free_tbindex(&tbp->var.tb.stp->indexv[i]);
        free(tbp->var.tb.stp->indexv);
        ret = _RDB_cat_get_indexes(tbp->var.tb.name, txp->dbp->dbrootp, ecp, txp,
                &tbp->var.tb.stp->indexv);
        if (ret != RDB_OK)
            return RDB_ERROR;

        /* Search again */
        for (i = 0; i < tbp->var.tb.stp->indexc
                && strcmp(tbp->var.tb.stp->indexv[i].name, name) != 0;
                i++);
        if (i >= tbp->var.tb.stp->indexc) {
            RDB_raise_internal("invalid index", ecp);
            return RDB_ERROR;
        }
    }        
    xi = i;

    /* Destroy index */
    ret = _RDB_del_index(txp, tbp->var.tb.stp->indexv[i].idxp, ecp);
    if (ret != RDB_OK)
        return ret;

    /* Delete index from catalog */
    ret = _RDB_cat_delete_index(name, ecp, txp);
    if (ret != RDB_OK)
        return ret;

    /*
     * Delete index entry
     */

    _RDB_free_tbindex(&tbp->var.tb.stp->indexv[xi]);

    tbp->var.tb.stp->indexc--;
    for (i = xi; i < tbp->var.tb.stp->indexc; i++) {
        tbp->var.tb.stp->indexv[i] = tbp->var.tb.stp->indexv[i + 1];
    }

    realloc(tbp->var.tb.stp->indexv,
            sizeof(_RDB_tbindex) * tbp->var.tb.stp->indexc);

    return RDB_OK;
}

char *
RDB_table_name(const RDB_object *tbp)
{
    return tbp->var.tb.name;
}

/*
 * Copy all tuples from source table into the destination table.
 * The destination table must be a real table.
 */
int
_RDB_move_tuples(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;
    RDB_qresult *qrp = NULL;
    RDB_expression *texp = _RDB_optimize(srcp, 0, NULL, ecp, txp);
    if (texp == NULL)
        return RDB_ERROR;

    qrp = _RDB_expr_qresult(texp, ecp, txp);
    if (qrp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Eliminate duplicates, if necessary */
    ret = _RDB_duprem(qrp, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    RDB_init_obj(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        if (!dstp->var.tb.is_persistent)
            ret = _RDB_insert_real(dstp, &tpl, ecp, NULL);
        else
            ret = _RDB_insert_real(dstp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        ret = RDB_OK;
    }

cleanup:
    if (qrp != NULL)
        _RDB_drop_qresult(qrp, ecp, txp);
    RDB_drop_expr(texp, ecp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

int
RDB_copy_table(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_ma_copy cpy;

    cpy.dstp = dstp;
    cpy.srcp = srcp;

    return RDB_multi_assign(0, NULL, 0, NULL, 0, NULL, 1, &cpy, ecp, txp);
}

int
RDB_all(RDB_object *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }
    if (attrtyp != &RDB_BOOLEAN) {
        RDB_raise_type_mismatch("attribute type must be BOOLEAN", ecp);
        return RDB_ERROR;
    }

    /* initialize result */
    *resultp = RDB_TRUE;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (!RDB_tuple_get_bool(tplp, attrname))
            *resultp = RDB_FALSE;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_any(RDB_object *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }
    if (attrtyp != &RDB_BOOLEAN) {
        RDB_raise_type_mismatch("attribute type must be BOOLEAN", ecp);
        return RDB_ERROR;
    }

    /* initialize result */
    *resultp = RDB_FALSE;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (RDB_tuple_get_bool(tplp, attrname))
            *resultp = RDB_TRUE;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_max(RDB_object *tbp, const char *attrname, RDB_exec_context *ecp,
       RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int i;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = RDB_INT_MIN;
    else if (attrtyp == &RDB_FLOAT)
        resultp->var.float_val = RDB_FLOAT_MIN;
    else if (attrtyp == &RDB_DOUBLE)
        resultp->var.double_val = RDB_DOUBLE_MIN;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    if (RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp) != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(tplp, attrname);
             
            if (val > resultp->var.int_val)
                 resultp->var.int_val = val;
        } else {
            RDB_double val = RDB_tuple_get_double(tplp, attrname);
             
            if (val > resultp->var.double_val)
                resultp->var.double_val = val;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_min(RDB_object *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = RDB_INT_MAX;
    else if (attrtyp == &RDB_FLOAT)
        resultp->var.double_val = RDB_FLOAT_MAX;
    else if (attrtyp == &RDB_DOUBLE)
        resultp->var.double_val = RDB_DOUBLE_MAX;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(tplp, attrname);
             
            if (val < resultp->var.int_val)
                 resultp->var.int_val = val;
        } else {
            RDB_double val = RDB_tuple_get_double(tplp, attrname);
             
            if (val < resultp->var.double_val)
                resultp->var.double_val = val;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_sum(RDB_object *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    /* initialize result */
    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = 0;
    else if (attrtyp == &RDB_FLOAT)
        resultp->var.float_val = 0.0;
    else if (attrtyp == &RDB_DOUBLE)
        resultp->var.double_val = 0.0;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER)
            resultp->var.int_val += RDB_tuple_get_int(tplp, attrname);
        else
            resultp->var.double_val
                            += RDB_tuple_get_double(tplp, attrname);
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);

    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_avg(RDB_object *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_double *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;
    unsigned long count;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    if (!RDB_type_is_numeric(attrtyp)) {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }
    count = 0;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    *resultp = 0.0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        count++;
        if (attrtyp == &RDB_INTEGER)
            *resultp += RDB_tuple_get_int(tplp, attrname);
        else
            *resultp += RDB_tuple_get_double(tplp, attrname);
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);

    if (count == 0) {
        RDB_raise_aggregate_undefined(ecp);
        return RDB_ERROR;
    }
    *resultp /= count;

    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_extract_tuple(RDB_object *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *tplp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_type *errtyp;
    RDB_expression *texp = _RDB_optimize(tbp, 0, NULL, ecp, txp);
    if (texp == NULL)
        return RDB_ERROR;

    qrp = _RDB_expr_qresult(texp, ecp, txp);
    if (qrp == NULL) {
        RDB_drop_expr(texp, ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    /* Get tuple */
    ret = _RDB_next_tuple(qrp, tplp, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Check if there are more tuples */
    for(;;) {
        RDB_bool is_equal;
    
        ret = _RDB_next_tuple(qrp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            errtyp = RDB_obj_type(RDB_get_err(ecp));
            if (errtyp == &RDB_NOT_FOUND_ERROR) {
                RDB_clear_err(ecp);
                ret = RDB_OK;
            }
            break;
        }

        ret = _RDB_tuple_equals(tplp, &tpl, ecp, txp, &is_equal);
        if (ret != RDB_OK)
            goto cleanup;

        if (!is_equal) {
            RDB_raise_invalid_argument("table contains more than one tuple", ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);

    _RDB_drop_qresult(qrp, ecp, txp);
    RDB_drop_expr(texp, ecp);
    return RDB_get_err(ecp) == NULL ? RDB_OK : RDB_ERROR;
}

RDB_bool
RDB_table_is_persistent(const RDB_object *tbp)
{
	return tbp->var.tb.is_persistent;
}

int
RDB_table_is_empty(RDB_object *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_expression *exp, *argp, *nexp;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Project all attributes away, then optimize
     */
    exp = RDB_ro_op("PROJECT", 1, ecp);
    if (exp == NULL)
    	return RDB_ERROR;
    argp = RDB_table_ref_to_expr(tbp, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        RDB_drop_expr(argp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    nexp = _RDB_optimize_expr(exp, 0, NULL, ecp, txp);

    /* Remove projection */
    RDB_drop_expr(exp, ecp);

    if (nexp == NULL) {
        return RDB_ERROR;
    }

    qrp = _RDB_expr_qresult(nexp, ecp, txp);
    if (qrp == NULL) {
        RDB_drop_expr(nexp, ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    /*
     * Read first tuple
     */
    if (_RDB_next_tuple(qrp, &tpl, ecp, txp) != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
            RDB_destroy_obj(&tpl, ecp);
            _RDB_drop_qresult(qrp, ecp, txp);
            RDB_drop_expr(nexp, ecp);
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);
        *resultp = RDB_TRUE;
    } else {
        *resultp = RDB_FALSE;
    }

    RDB_destroy_obj(&tpl, ecp);
    if (_RDB_drop_qresult(qrp, ecp, txp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_drop_expr(nexp, ecp);
}

RDB_int
RDB_cardinality(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_int count;
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_expression *texp;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    texp = _RDB_optimize(tbp, 0, NULL, ecp, txp);
    if (texp == NULL)
        return RDB_ERROR;

    qrp = _RDB_expr_qresult(texp, ecp, txp);
    if (qrp == NULL) {
        RDB_drop_expr(texp, ecp);
        return RDB_ERROR;
    }

    /* Duplicates must be removed */
    ret = _RDB_duprem(qrp, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_expr(texp, ecp);
        _RDB_drop_qresult(qrp, ecp, txp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    count = 0;
    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        count++;
    }
    RDB_destroy_obj(&tpl, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        _RDB_drop_qresult(qrp, ecp, txp);
        goto error;
    }
    RDB_clear_err(ecp);

    ret = _RDB_drop_qresult(qrp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    if (texp->kind == RDB_EX_TBP
            && texp->var.tbref.tbp->var.tb.stp != NULL)
        texp->var.tbref.tbp->var.tb.stp->est_cardinality = count;

    if (RDB_drop_expr(texp, ecp) != RDB_OK)
        return RDB_ERROR;

    return count;

error:
    RDB_drop_expr(texp, ecp);
    return RDB_ERROR;
}

int
RDB_subset(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    if (!RDB_type_equals(tb1p->typ, tb2p->typ)) {
        RDB_raise_type_mismatch("argument types must be equal", ecp);
        return RDB_ERROR;
    }

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    qrp = _RDB_table_qresult(tb1p, ecp, txp);
    if (qrp == NULL) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    *resultp = RDB_TRUE;
    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, ecp, txp, resultp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            _RDB_drop_qresult(qrp, ecp, txp);
            goto error;
        }
        if (!*resultp) {
            break;
        }
    }

    RDB_destroy_obj(&tpl, ecp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
            _RDB_drop_qresult(qrp, ecp, txp);
            goto error;
        }
        RDB_clear_err(ecp);
    }
    ret = _RDB_drop_qresult(qrp, ecp, txp);
    if (ret != RDB_OK)
        goto error;
    return RDB_OK;

error:
    return RDB_ERROR;
}

int
_RDB_table_equals(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;
    int cnt;

    /* Check if types of the two tables match */
    if (!RDB_type_equals(tb1p->typ, tb2p->typ)) {
        RDB_raise_type_mismatch("argument types must be equal", ecp);
        return RDB_ERROR;
    }

    /*
     * Check if both tables have same cardinality
     */
    cnt = RDB_cardinality(tb1p, ecp, txp);
    if (cnt < 0)
        return cnt;

    ret =  RDB_cardinality(tb2p, ecp, txp);
    if (ret < 0)
        return ret;
    if (ret != cnt) {
        *resp = RDB_FALSE;
        return RDB_OK;
    }

    /*
     * Check if all tuples from table #1 are in table #2
     * (The implementation is quite inefficient if table #2
     * is a SUMMARIZE PER or GROUP table)
     */
    qrp = _RDB_table_qresult(tb1p, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, ecp, txp, resp);
        if (ret != RDB_OK) {
            goto error;
        }
        if (!*resp) {
            RDB_destroy_obj(&tpl, ecp);
            return _RDB_drop_qresult(qrp, ecp, txp);
        }
    }

    *resp = RDB_TRUE;
    RDB_destroy_obj(&tpl, ecp);
    return _RDB_drop_qresult(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    _RDB_drop_qresult(qrp, ecp, txp);
    return ret;
}

/*
 * If the tuples are sorted by an ordered index when read using
 * a table qresult, return the index, otherwise NULL.
 */
_RDB_tbindex *
_RDB_expr_sortindex (RDB_expression *exp)
{
    if (exp->kind == RDB_EX_TBP) {
        if (exp->var.tbref.tbp->var.tb.exp != NULL)
            return _RDB_expr_sortindex(exp->var.tbref.tbp->var.tb.exp);
        return exp->var.tbref.indexp;
    }
    if (exp->kind != RDB_EX_RO_OP)
        return NULL;
    if (strcmp(exp->var.op.name, "WHERE") == 0) {
        return _RDB_expr_sortindex(exp->var.op.argv[0]);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        return _RDB_expr_sortindex(exp->var.op.argv[0]);
    }
    if (strcmp(exp->var.op.name, "SEMIMINUS") == 0
            || strcmp(exp->var.op.name, "MINUS") == 0
            || strcmp(exp->var.op.name, "SEMIJOIN") == 0
            || strcmp(exp->var.op.name, "INTERSECT") == 0
            || strcmp(exp->var.op.name, "JOIN") == 0
            || strcmp(exp->var.op.name, "EXTEND") == 0
            || strcmp(exp->var.op.name, "SDIVIDE") == 0) {
        return _RDB_expr_sortindex(exp->var.op.argv[0]);
    }
    /* !! RENAME, SUMMARIZE, WRAP, UNWRAP, GROUP, UNGROUP */

    return NULL;
}
