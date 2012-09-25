/*
 * aggrf.c
 *
 *  Created on: 23.09.2012
 *      Author: Rene Hartmann
 *
 *  Functions for aggregate operators and IS_EMPTY
 */

#include "rdb.h"
#include "internal.h"
#include "optimize.h"

/** @addtogroup table
 * @{
 */

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
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>name_error
<dd>The table does not have an attribute <var>attrname</var>.
<dt>type_mismatch_error
<dd>The type of the attribute is not BOOLEAN.
<dt>invalid_argument_error
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
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->def.basetyp->def.tuple.attrv[0].name;
    }

    attrtyp = RDB_tuple_type_attr(tbp->typ->def.basetyp, attrname)->typ;
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

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, RDB_UNBUFFERED, ecp, txp);
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
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>name_error
<dd>The table does not have an attribute <var>attrname</var>.
<dt>type_mismatch_error
<dd>The type of the attribute is not BOOLEAN.
<dt>invalid_argument_error
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
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->def.basetyp->def.tuple.attrv[0].name;
    }

    attrtyp = RDB_tuple_type_attr(tbp->typ->def.basetyp, attrname)->typ;
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

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, RDB_UNBUFFERED, ecp, txp);
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
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>name_error
<dd>The table does not have an attribute <var>attrname</var>.
<dt>type_mismatch_error
<dd>The type of the attribute is not numeric.
<dt>invalid_argument_error
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
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->def.basetyp->def.tuple.attrv[0].name;
    }

    attrtyp = RDB_tuple_type_attr(tbp->typ->def.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_name(attrname, ecp);
        return RDB_ERROR;
    }

    RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->val.int_val = RDB_INT_MIN;
    else if (attrtyp == &RDB_FLOAT)
        resultp->val.float_val = RDB_FLOAT_MIN;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    if (RDB_table_to_array(&arr, tbp, 0, NULL, RDB_UNBUFFERED, ecp, txp) != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(tplp, attrname);

            if (val > resultp->val.int_val)
                 resultp->val.int_val = val;
        } else {
            RDB_float val = RDB_tuple_get_float(tplp, attrname);

            if (val > resultp->val.float_val)
                resultp->val.float_val = val;
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
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>name_error
<dd>The table does not have an attribute <var>attrname</var>.
<dt>type_mismatch_error
<dd>The type of the attribute is not numeric.
<dt>invalid_argument_error
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
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->def.basetyp->def.tuple.attrv[0].name;
    }

    attrtyp = RDB_tuple_type_attr(tbp->typ->def.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_name(attrname, ecp);
        return RDB_ERROR;
    }

    RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->val.int_val = RDB_INT_MAX;
    else if (attrtyp == &RDB_FLOAT)
        resultp->val.float_val = RDB_FLOAT_MAX;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, RDB_UNBUFFERED, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(tplp, attrname);

            if (val < resultp->val.int_val)
                 resultp->val.int_val = val;
        } else {
            RDB_float val = RDB_tuple_get_float(tplp, attrname);

            if (val < resultp->val.float_val)
                resultp->val.float_val = val;
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
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>name_error
<dd>The table does not have an attribute <var>attrname</var>.
<dt>type_mismatch_error
<dd>The type of the attribute is not numeric.
<dt>invalid_argument_error
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
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->def.basetyp->def.tuple.attrv[0].name;
    }

    attrtyp = RDB_tuple_type_attr(tbp->typ->def.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_name(attrname, ecp);
        return RDB_ERROR;
    }

    RDB_set_obj_type(resultp, attrtyp);

    /* initialize result */
    if (attrtyp == &RDB_INTEGER)
        resultp->val.int_val = 0;
    else if (attrtyp == &RDB_FLOAT)
        resultp->val.float_val = 0.0;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, RDB_UNBUFFERED, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER)
            resultp->val.int_val += RDB_tuple_get_int(tplp, attrname);
        else
            resultp->val.float_val
                            += RDB_tuple_get_float(tplp, attrname);
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
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>name_error
<dd>The table does not have an attribute <var>attrname</var>.
<dt>type_mismatch_error
<dd>The type of the attribute is not numeric.
<dt>invalid_argument_error
<dd><var>attrname</var> is NULL and the table has more than one
attribute.
<dt>aggregate_undefined_error
<dd>The table is empty.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_avg(RDB_object *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_float *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;
    unsigned long count;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->def.basetyp->def.tuple.attrv[0].name;
    }

    attrtyp = RDB_tuple_type_attr(tbp->typ->def.basetyp, attrname)->typ;
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

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, RDB_UNBUFFERED, ecp, txp);
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
            *resultp += RDB_tuple_get_float(tplp, attrname);
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
    int ret;
    RDB_object result;
    RDB_expression *argp;
    RDB_expression *exp = RDB_ro_op("is_empty", ecp);
    if (exp == NULL)
        return RDB_ERROR;

    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    RDB_init_obj(&result);
    ret = RDB_evaluate(exp, NULL, NULL, NULL, ecp, txp, &result);
    RDB_del_expr(exp, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&result, ecp);
        return RDB_ERROR;
    }

    *resultp = RDB_obj_bool(&result);
    return RDB_destroy_obj(&result, ecp);
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
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>operator_not_found_error
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
    RDB_int card;
    RDB_object result;
    RDB_expression *argp;
    RDB_expression *exp = RDB_ro_op("count", ecp);
    if (exp == NULL)
        return (RDB_int) RDB_ERROR;

    argp = RDB_table_ref(tbp, ecp);
    if (argp == NULL) {
        RDB_del_expr(exp, ecp);
        return (RDB_int) RDB_ERROR;
    }
    RDB_add_arg(exp, argp);

    RDB_init_obj(&result);
    ret = RDB_evaluate(exp, NULL, NULL, NULL, ecp, txp, &result);
    RDB_del_expr(exp, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&result, ecp);
        return (RDB_int) RDB_ERROR;
    }

    card = RDB_obj_int(&result);
    RDB_destroy_obj(&result, ecp);
    return card;
}

/*@}*/
