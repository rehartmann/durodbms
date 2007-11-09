/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "catalog.h"
#include "stable.h"
#include "qresult.h"
#include "insert.h"
#include "optimize.h"
#include "internal.h"
#include <gen/strfns.h>

#include <string.h>

#include <dli/tabletostr.h>

static RDB_string_vec *
dup_keyv(int keyc, const RDB_string_vec keyv[], RDB_exec_context *ecp)
{
    return _RDB_dup_rename_keys(keyc, keyv, NULL, ecp);
}

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

/**
 * Creates a stored table, but not the recmap and the indexes
 * and does not insert the table into the catalog.
 * reltyp is consumed on success (must not be freed by caller).
 */
RDB_object *
_RDB_new_rtable(const char *name, RDB_bool persistent,
           RDB_type *reltyp,
           int keyc, const RDB_string_vec keyv[], RDB_bool usr,
           RDB_exec_context *ecp)
{
    RDB_object *tbp = _RDB_new_obj(ecp);
    if (tbp == NULL)
        return NULL;

    if (_RDB_init_table(tbp, name, persistent, reltyp, keyc, keyv,
            usr, NULL, ecp) != RDB_OK) {
        RDB_free(tbp);
        return NULL;
    }
    return tbp;
}

/**
 * Copy all tuples from source table into the destination table.
 * The destination table must be a real table.
 */
RDB_int
_RDB_move_tuples(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;
    int count = 0;
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
    if (_RDB_duprem(qrp, ecp, txp) != RDB_OK)
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
        count++;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        ret = count;
    }

cleanup:
    if (qrp != NULL)
        _RDB_drop_qresult(qrp, ecp, txp);
    RDB_drop_expr(texp, ecp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

int
_RDB_init_table(RDB_object *tbp, const char *name, RDB_bool persistent,
        RDB_type *reltyp, int keyc, const RDB_string_vec keyv[], RDB_bool usr,
        RDB_expression *exp, RDB_exec_context *ecp)
{
    int i;
    RDB_string_vec allkey; /* Used if keyv is NULL */
    int attrc = reltyp->var.basetyp->var.tuple.attrc;

    if (keyv != NULL) {
        int j;

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
                if (_RDB_tuple_type_attr(reltyp->var.basetyp, keyv[i].strv[j])
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
    }

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
            allkey.strv = RDB_alloc(sizeof (char *) * attrc, ecp);
            if (allkey.strv == NULL) {
                goto error;
            }
            for (i = 0; i < attrc; i++)
                allkey.strv[i] = reltyp->var.basetyp->var.tuple.attrv[i].name;
            keyc = 1;
            keyv = &allkey;
        }

        /* Copy candidate keys */
        tbp->var.tb.keyv = dup_keyv(keyc, keyv, ecp);
        if (tbp->var.tb.keyv == NULL) {
            goto error;
        }
        tbp->var.tb.keyc = keyc;
    }
    tbp->var.tb.exp = exp;

    tbp->typ = reltyp;

    RDB_free(allkey.strv);

    return RDB_OK;

error:
    /* Clean up */
    if (tbp != NULL) {
        RDB_free(tbp->var.tb.name);
        if (tbp->var.tb.keyv != NULL)
            _RDB_free_keys(tbp->var.tb.keyc, tbp->var.tb.keyv);
    }
    RDB_free(allkey.strv);
    return RDB_ERROR;
}

/** @addtogroup table
 * @{
 */

/** Like RDB_init_table(), but uses a relation type instead
 * of a RDB_type argument instead of attribute arguments.
 */
int
RDB_init_table_from_type(RDB_object *tbp, const char *name, RDB_type *reltyp,
        int keyc, const RDB_string_vec keyv[], RDB_exec_context *ecp)
{
    return _RDB_init_table(tbp, name, RDB_FALSE, reltyp,
            keyc, keyv, RDB_TRUE, NULL, ecp);
}

/** Turn *<var>tbp</var> into a transient table.
 *<var>tbp</var> should have been initialized using
RDB_init_obj().

For <var>name</var>, <var>attrc</var>, <var>attrv</var>,
<var>keyc</var>, <var>keyv</var>,
and <var>ecp</var>, the same rules apply as for
RDB_create_table().

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of a default value does not match the type of the corresponding
attribute.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd>One or more of the arguments are incorrect. For example, a key attribute
does not appear in *<var>attrv</var>, etc.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_init_table(RDB_object *tbp, const char *name,
        int attrc, const RDB_attr attrv[],
        int keyc, const RDB_string_vec keyv[],
        RDB_exec_context *ecp)
{
    RDB_type *reltyp = RDB_create_relation_type(attrc, attrv, ecp);
    if (reltyp == NULL)
        return RDB_ERROR;

    if (_RDB_init_table(tbp, name, RDB_FALSE, reltyp, keyc, keyv, RDB_TRUE,
            NULL, ecp) != RDB_OK) {
        RDB_drop_type(reltyp, ecp, NULL);
        return RDB_ERROR;
    }
    return RDB_OK;   
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
            tbp->var.tb.keyv = dup_keyv(keyc, keyv, ecp);
            if (tbp->var.tb.keyv == NULL) {
                return RDB_ERROR;
            }
        }
        tbp->var.tb.keyc = keyc;
    }

    if (keyvp != NULL)
        *keyvp = tbp->var.tb.keyv;

    return tbp->var.tb.keyc;
}

/**
 * RDB_table_name returns a pointer to the name of a table.

@returns

A pointer to the name of the table, or NULL if the table has no name.
 */
char *
RDB_table_name(const RDB_object *tbp)
{
    return tbp->var.tb.name;
}

/**
RDB_copy_table assigns the table specified
by <var>srcp</var> to the value of the table specified by <var>dstp</var>.
The two tables must have the same heading.

Currently, virtual target tables are not supported.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The types of the two tables differ.
<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>The definition of the table specified by <var>srcp</var>
refers to a non-existing operator.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
int
RDB_copy_table(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_ma_copy cpy;

    cpy.dstp = dstp;
    cpy.srcp = srcp;

    return RDB_multi_assign(0, NULL, 0, NULL, 0, NULL, 1, &cpy, ecp, txp);
}

/**
 * RDB_all computes a logical AND over the attribute
<var>attrname</var> of the table specified by <var>tbp</var>
and stores the result at the location pointed to by <var>resultp</var>.

If the table has only one attribute, <var>attrname</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The attribute <var>attrname</var> must be of type BOOLEAN.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>NAME_ERROR
<dd>The table does not have an attribute <var>attrname</var>.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of the attribute is not BOOLEAN.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd><var>attrname</var> is NULL and the table has more than one
attribute.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
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
        RDB_raise_name(attrname, ecp);
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

/**
RDB_any computes a logical OR over the attribute
<var>attrname</var> of the table specified by <var>tbp</var>
and stores the result at the location
pointed to by <var>resultp</var>.

If the table has only one attribute, <var>attrname</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The attribute <var>attrname</var> must be of
type BOOLEAN.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>NAME_ERROR
<dd>The table does not have an attribute <var>attrname</var>.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of the attribute is not BOOLEAN.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd><var>attrname</var> is NULL and the table has more than one
attribute.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
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
        RDB_raise_name(attrname, ecp);
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

/**
 * RDB_max computes the maximum over the attribute
<var>attrname</var> of the table specified by <var>tbp</var>
and stores the result at the location pointed to by <var>resultp</var>.

If the table has only one attribute, <var>attrname</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The attribute <var>attrname</var> must be numeric
and the result is of the same type as the attribute.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>NAME_ERROR
<dd>The table does not have an attribute <var>attrname</var>.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of the attribute is not numeric.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd><var>attrname</var> is NULL and the table has more than one
attribute.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
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
        RDB_raise_name(attrname, ecp);
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

/**
 * RDB_min computes the minimum over the attribute
<var>attrname</var> of the table specified by <var>tbp</var>
and stores the result at the location pointed to by <var>resultp</var>.

If the table has only one attribute, <var>attrname</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The attribute <var>attrname</var> must be numeric
and the result is of the same type as the attribute.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>NAME_ERROR
<dd>The table does not have an attribute <var>attrname</var>.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of the attribute is not numeric.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd><var>attrname</var> is NULL and the table has more than one
attribute.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
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
        RDB_raise_name(attrname, ecp);
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

/**
 * RDB_sum computes the sum over the attribute
<var>attrname</var> of the table pointed to by <var>tbp</var>
and stores the result at the location pointed to by <var>resultp</var>.

If the table has only one attribute, <var>attrname</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The attribute <var>attrname</var> must be numeric
and the result is of the same type as the attribute.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>NAME_ERROR
<dd>The table does not have an attribute <var>attrname</var>.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of the attribute is not numeric.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd><var>attrname</var> is NULL and the table has more than one
attribute.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
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
        RDB_raise_name(attrname, ecp);
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

/**
 * RDB_avg computes the average over the attribute
<var>attrname</var> of the table specified by <var>tbp</var>
and stores the result at the location
pointed to by <var>resultp</var>.

If the table has only one attribute, <var>attrname</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The attribute <var>attrname</var> must be numeric.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>NAME_ERROR
<dd>The table does not have an attribute <var>attrname</var>.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of the attribute is not numeric.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd><var>attrname</var> is NULL and the table has more than one
attribute.
<dt>RDB_AGGREGATE_UNDEFINED_ERROR
<dd>The table is empty.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
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
        RDB_raise_name(attrname, ecp);
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

/**
 * RDB_extract_tuple extracts a single tuple from a table which contains
only one tuple and stores its value in the variable pointed to by <var>tplp</var>.

If an error occurs, the tuple value of the variable pointed to by <var>tplp</var>
is undefined and an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_NOT_FOUND_ERROR
<dd>The table is empty.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd>The table contains more than one tuple.
<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
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

/**
 * RDB_table_is_persistent returns if the table *<var>tbp</var>
is persistent.

@returns

RDB_TRUE if *<var>tbp</var> is persistent, RDB_FALSE if it
is transient.
 */
RDB_bool
RDB_table_is_persistent(const RDB_object *tbp)
{
	return tbp->var.tb.is_persistent;
}

/**
 * RDB_table_is_real returns if the table *<var>tbp</var>
is real.

@returns

RDB_TRUE if *<var>tbp</var> is real, RDB_FALSE if it
is virtual.
 */
RDB_bool
RDB_table_is_real(const RDB_object *tbp)
{
    return (RDB_bool) (tbp->var.tb.exp == NULL);
}

/**
 *
RDB_table_is_empty checks if the table specified by <var>tbp</var>
is empty and stores the result of
the check at the location pointed to by <var>resultp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.
 */
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
    exp = RDB_ro_op("PROJECT", ecp);
    if (exp == NULL)
    	return RDB_ERROR;
    argp = RDB_table_ref(tbp, ecp);
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

/**
RDB_cardinality returns the number of tuples in
the table pointed to by <var>tbp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

On success, the number of tuples is returned.
On failure, (RDB_int)RDB_ERROR is returned.
(RDB_int)RDB_ERROR is guaranteed to be lower than zero.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
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

/**
 * RDB_subset checks if the table specified by <var>tb1p</var> is a subset
of the table specified by <var>tb2p</var> and stores the result at the
location pointed to by <var>resultp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The types of the two tables differ.
<dt>RDB_OPERATOR_NOT_FOUND_ERROR
<dd>The definition of one of the tables
refers to a non-existing operator.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
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

/*@}*/

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

/**
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
        return _RDB_expr_sortindex(exp->var.op.args.firstp);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
        return _RDB_expr_sortindex(exp->var.op.args.firstp);
    }
    if (strcmp(exp->var.op.name, "SEMIMINUS") == 0
            || strcmp(exp->var.op.name, "MINUS") == 0
            || strcmp(exp->var.op.name, "SEMIJOIN") == 0
            || strcmp(exp->var.op.name, "INTERSECT") == 0
            || strcmp(exp->var.op.name, "JOIN") == 0
            || strcmp(exp->var.op.name, "EXTEND") == 0
            || strcmp(exp->var.op.name, "DIVIDE") == 0) {
        return _RDB_expr_sortindex(exp->var.op.args.firstp);
    }
    /* !! RENAME, SUMMARIZE, WRAP, UNWRAP, GROUP, UNGROUP */

    return NULL;
}

int
_RDB_set_defvals(RDB_type *tbtyp, int attrc, const RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    int i;

    for (i = 0; i < attrc; i++) {
        if (attrv[i].defaultp != NULL) {
            RDB_type *tpltyp = tbtyp->var.basetyp;

            tpltyp->var.tuple.attrv[i].defaultp = RDB_alloc(sizeof (RDB_object), ecp);
            if (tpltyp->var.tuple.attrv[i].defaultp == NULL) {
                return RDB_ERROR;
            }
            RDB_init_obj(tpltyp->var.tuple.attrv[i].defaultp);
            if (RDB_copy_obj(tpltyp->var.tuple.attrv[i].defaultp,
                    attrv[i].defaultp, ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }
    return RDB_OK;
}

/**
 * @defgroup index Index functions
 * \#include <rel/rdb.h>
 * @{
 */

/**
 * RDB_create_table_index creates an index with name <var>name</var>
for the table specified by <var>tbp</var> over the attributes
specified by <var>idxcompc</var> and <var>idxcompv</var>.
The <var>flags</var> argument must be either 0 or RDB_ORDERED.
If <var>flags</var> is 0, a hash index is created.
If <var>flags</var> is RDB_ORDERED, a B-tree index is created.

Functions which read tuples from a table (like
@ref RDB_table_to_array
and @ref RDB_extract_tuple) try to use available
indexes to achieve better performance.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd><var>name</var> is not a valid index name.
<dt>RDB_ELEMENT_EXIST_ERROR
<dd>An index with name <var>name</var> already exists.
<dt>NAME_ERROR
<dd>An attribute specified by <var>idxcompv</var> does not appear in the
table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_table_index(const char *name, RDB_object *tbp, int idxcompc,
        const RDB_seq_item idxcompv[], int flags, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;
    _RDB_tbindex *indexp;

    if (!_RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid index name", ecp);
        return RDB_ERROR;
    }

    if (tbp->var.tb.is_persistent) {
        /* Insert index into catalog */
        ret = _RDB_cat_insert_index(name, idxcompc, idxcompv,
                (RDB_bool) ((RDB_UNIQUE & flags) != 0),
                (RDB_bool) ((RDB_ORDERED & flags) != 0), RDB_table_name(tbp),
                ecp, txp);
        if (ret != RDB_OK)
            goto error;
    }

    if (tbp->var.tb.stp != NULL) {
        tbp->var.tb.stp->indexv = realloc(tbp->var.tb.stp->indexv,
                (tbp->var.tb.stp->indexc + 1) * sizeof (_RDB_tbindex));
        if (tbp->var.tb.stp->indexv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        indexp = &tbp->var.tb.stp->indexv[tbp->var.tb.stp->indexc++];

        indexp->name = RDB_dup_str(name);
        if (indexp->name == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        indexp->attrc = idxcompc;
        indexp->attrv = RDB_alloc(sizeof (RDB_seq_item) * idxcompc, ecp);
        if (indexp->attrv == NULL) {
            RDB_free(indexp->name);
            return RDB_ERROR;
        }

        for (i = 0; i < idxcompc; i++) {
            indexp->attrv[i].attrname = RDB_dup_str(idxcompv[i].attrname);
            if (indexp->attrv[i].attrname == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            indexp->attrv[i].asc = idxcompv[i].asc;
        }
        indexp->unique = (RDB_bool) (RDB_UNIQUE & flags);
        indexp->ordered = (RDB_bool) (RDB_ORDERED & flags);

        /* Create index */
        if (_RDB_create_tbindex(tbp, RDB_db_env(RDB_tx_db(txp)), ecp, txp,
                indexp, flags) != RDB_OK) {
            goto error;
        }
    }

    return RDB_OK;

error:
    if (tbp->var.tb.stp != NULL) {
        /* Remove index entry */
        void *ivp = realloc(tbp->var.tb.stp->indexv,
                (--tbp->var.tb.stp->indexc) * sizeof (_RDB_tbindex));
        if (ivp != NULL)
            tbp->var.tb.stp->indexv = ivp;
    }
    return RDB_ERROR;
}

/**
 * RDB_drop_table_index drops the index specified by <var>name</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_NOT_FOUND_ERROR
<dd>An index with name <var>name</var> does not exist.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
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
    RDB_free(tbname);
    if (tbp == NULL)
        return RDB_ERROR;

    for (i = 0; i < tbp->var.tb.stp->indexc
            && strcmp(tbp->var.tb.stp->indexv[i].name, name) != 0;
            i++);
    if (i >= tbp->var.tb.stp->indexc) {
        /* Index not found, so reread indexes */
        for (i = 0; i < tbp->var.tb.stp->indexc; i++)
            _RDB_free_tbindex(&tbp->var.tb.stp->indexv[i]);
        RDB_free(tbp->var.tb.stp->indexv);
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

/*@}*/
