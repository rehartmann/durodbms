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
 * RDB_all computes a logical OR over the expression
*<var>exp</var> of the table specified by <var>tbp</var>
and stores the result at the location
pointed to by <var>resultp</var>.

If the table has only one attribute, <var>exp</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The type of *<var>exp</var> must be BOOLEAN.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>tbp</var> is persistent and *<var>txp</var> is not a running transaction.
<dt>name_error
<dd>*<var>exp</var> refers to a non-existing attribute.
<dt>type_mismatch_error
<dd>The type of the attribute is not BOOLEAN.
<dt>invalid_argument_error
<dd><var>exp</var> is NULL and the table has more than one
attribute.
<dt>
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_all(RDB_object *tbp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_object tpl;
    RDB_object hobj;
    RDB_qresult *qrp = NULL;
    RDB_bool del_over = RDB_FALSE;

    if (exp == NULL) {
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("second argument required", ecp);
            return RDB_ERROR;
        }
        exp = RDB_var_ref(tbp->typ->def.basetyp->def.tuple.attrv[0].name, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        del_over = RDB_TRUE;
    }

    attrtyp = RDB_expr_type(exp, RDB_get_tuple_attr_type, tbp->typ->def.basetyp, NULL, ecp, txp);
    if (attrtyp == NULL) {
        return RDB_ERROR;
    }
    if (attrtyp != &RDB_BOOLEAN) {
        RDB_raise_type_mismatch("type must be boolean", ecp);
        return RDB_ERROR;
    }

    /* Initialize result */
    *resultp = RDB_TRUE;

    /*
     * Perform aggregation
     */

    qrp = RDB_table_iterator(tbp, 0, NULL, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&hobj);

    while (RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        if (RDB_evaluate(exp, &RDB_tpl_get, &tpl, NULL, ecp, txp, &hobj)
                != RDB_OK) {
            goto error;
        }
        if (!RDB_obj_bool(&hobj))
            *resultp = RDB_FALSE;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_del_table_iterator(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (qrp != NULL) {
        RDB_del_table_iterator(qrp, ecp, txp);
    }

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_ERROR;
}

/**
RDB_any computes a logical OR over the expression
*<var>exp</var> of the table specified by <var>tbp</var>
and stores the result at the location
pointed to by <var>resultp</var>.

If the table has only one attribute, <var>exp</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The type of *<var>exp</var> must be BOOLEAN.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>tbp</var> is persistent and *<var>txp</var> is not a running transaction.
<dt>name_error
<dd>*<var>exp</var> refers to a non-existing attribute.
<dt>type_mismatch_error
<dd>The type of the expression is not BOOLEAN.
<dt>invalid_argument_error
<dd><var>exp</var> is NULL and the table has more than one
attribute.
<dt>
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
int
RDB_any(RDB_object *tbp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_object tpl;
    RDB_object hobj;
    RDB_qresult *qrp = NULL;
    RDB_bool del_over = RDB_FALSE;

    if (exp == NULL) {
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("second argument required", ecp);
            return RDB_ERROR;
        }
        exp = RDB_var_ref(tbp->typ->def.basetyp->def.tuple.attrv[0].name, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        del_over = RDB_TRUE;
    }

    attrtyp = RDB_expr_type(exp, RDB_get_tuple_attr_type, tbp->typ->def.basetyp, NULL, ecp, txp);
    if (attrtyp == NULL) {
        return RDB_ERROR;
    }
    if (attrtyp != &RDB_BOOLEAN) {
        RDB_raise_type_mismatch("type must be boolean", ecp);
        return RDB_ERROR;
    }

    /* Initialize result */
    *resultp = RDB_FALSE;

    /*
     * Perform aggregation
     */

    qrp = RDB_table_iterator(tbp, 0, NULL, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&hobj);

    while (RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        if (RDB_evaluate(exp, &RDB_tpl_get, &tpl, NULL, ecp, txp, &hobj)
                != RDB_OK) {
            goto error;
        }
        if (RDB_obj_bool(&hobj))
            *resultp = RDB_TRUE;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_del_table_iterator(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (qrp != NULL) {
        RDB_del_table_iterator(qrp, ecp, txp);
    }

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_ERROR;
}

/**
 * RDB_max computes the maximum over the expression
<var>exp</var> of the table specified by <var>tbp</var>
and stores the result at the location pointed to by <var>resultp</var>.

If the table has only one attribute, <var>exp</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The type of <var>exp</var> must be numeric
and the result is of the same type as the attribute.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>tbp</var> is persistent and *<var>txp</var> is not a running transaction.
<dt>name_error
<dd>*<var>exp</var> refers to a non-existing attribute.
<dt>type_mismatch_error
<dd>The type of the attribute is not numeric.
<dt>invalid_argument_error
<dd><var>exp</var> is NULL and the table has more than one
attribute.
<dt>
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_max(RDB_object *tbp, RDB_expression *exp, RDB_exec_context *ecp,
       RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp = NULL;
    RDB_object tpl;
    RDB_object hobj;
    RDB_bool del_over = RDB_FALSE;

    if (exp == NULL) {
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("second argument required", ecp);
            return RDB_ERROR;
        }
        exp = RDB_var_ref(tbp->typ->def.basetyp->def.tuple.attrv[0].name, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        del_over = RDB_TRUE;
    }

    attrtyp = RDB_expr_type(exp, RDB_get_tuple_attr_type, tbp->typ->def.basetyp, NULL, ecp, txp);
    if (attrtyp == NULL) {
        return RDB_ERROR;
    }

    /* Initialize result */
    if (attrtyp == &RDB_INTEGER)
        resultp->val.int_val = RDB_INT_MIN;
    else if (attrtyp == &RDB_FLOAT)
        resultp->val.float_val = RDB_FLOAT_MIN;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    RDB_set_obj_type(resultp, attrtyp);

    /*
     * Perform aggregation
     */

    qrp = RDB_table_iterator(tbp, 0, NULL, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&hobj);

    while (RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        if (RDB_evaluate(exp, &RDB_tpl_get, &tpl, NULL, ecp, txp, &hobj)
                != RDB_OK) {
            goto error;
        }
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_obj_int(&hobj);

            if (val > resultp->val.int_val)
                 resultp->val.int_val = val;
        } else {
            RDB_float val = RDB_obj_float(&hobj);

            if (val > resultp->val.float_val)
                resultp->val.float_val = val;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_del_table_iterator(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (qrp != NULL) {
        RDB_del_table_iterator(qrp, ecp, txp);
    }

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_ERROR;
}

/**
 * RDB_min computes the minimum over the expression
<var>exp</var> of the table specified by <var>tbp</var>
and stores the result at the location pointed to by <var>resultp</var>.

If the table has only one attribute, <var>exp</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The type of *<var>exp</var> must be numeric
and the result is of the same type as the attribute.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>tbp</var> is persistent and *<var>txp</var> is not a running transaction.
<dt>name_error
<dd>*<var>exp</var> refers to a non-existing attribute.
<dt>type_mismatch_error
<dd>The type of the attribute is not numeric.
<dt>invalid_argument_error
<dd><var>exp</var> is NULL and the table has more than one
attribute.
<dt>
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_min(RDB_object *tbp, RDB_expression *exp, RDB_exec_context *ecp,
       RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp = NULL;
    RDB_object tpl;
    RDB_object hobj;
    RDB_bool del_over = RDB_FALSE;

    if (exp == NULL) {
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("second argument required", ecp);
            return RDB_ERROR;
        }
        exp = RDB_var_ref(tbp->typ->def.basetyp->def.tuple.attrv[0].name, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        del_over = RDB_TRUE;
    }

    attrtyp = RDB_expr_type(exp, RDB_get_tuple_attr_type, tbp->typ->def.basetyp, NULL, ecp, txp);
    if (attrtyp == NULL) {
        return RDB_ERROR;
    }

    /* Initialize result */
    if (attrtyp == &RDB_INTEGER)
        resultp->val.int_val = RDB_INT_MAX;
    else if (attrtyp == &RDB_FLOAT)
        resultp->val.float_val = RDB_FLOAT_MAX;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    RDB_set_obj_type(resultp, attrtyp);

    /*
     * Perform aggregation
     */

    qrp = RDB_table_iterator(tbp, 0, NULL, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&hobj);

    while (RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        if (RDB_evaluate(exp, &RDB_tpl_get, &tpl, NULL, ecp, txp, &hobj)
                != RDB_OK) {
            goto error;
        }
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_obj_int(&hobj);

            if (val < resultp->val.int_val)
                 resultp->val.int_val = val;
        } else {
            RDB_float val = RDB_obj_float(&hobj);

            if (val < resultp->val.float_val)
                resultp->val.float_val = val;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_del_table_iterator(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (qrp != NULL) {
        RDB_del_table_iterator(qrp, ecp, txp);
    }

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_ERROR;
}

/**
 * RDB_sum computes the sum over the expression
<var>exp</var> of the table pointed to by <var>tbp</var>
and stores the result at the location pointed to by <var>resultp</var>.

If the table has only one attribute, <var>exp</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The type of <var>exp</var> must be numeric
and the result is of the same type as the attribute.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>tbp</var> is persistent and *<var>txp</var> is not a running transaction.
<dt>name_error
<dd>*<var>exp</var> refers to a non-existing attribute.
<dt>type_mismatch_error
<dd>The type of the attribute is not numeric.
<dt>invalid_argument_error
<dd><var>exp</var> is NULL and the table has more than one
attribute.
<dt>
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_sum(RDB_object *tbp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_qresult *qrp = NULL;
    RDB_object tpl;
    RDB_object hobj;
    RDB_bool del_over = RDB_FALSE;

    if (exp == NULL) {
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("second argument required", ecp);
            return RDB_ERROR;
        }
        exp = RDB_var_ref(tbp->typ->def.basetyp->def.tuple.attrv[0].name, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        del_over = RDB_TRUE;
    }

    attrtyp = RDB_expr_type(exp, RDB_get_tuple_attr_type, tbp->typ->def.basetyp, NULL, ecp, txp);
    if (attrtyp == NULL) {
        return RDB_ERROR;
    }

    /* Initialize result */
    if (attrtyp == &RDB_INTEGER)
        resultp->val.int_val = 0;
    else if (attrtyp == &RDB_FLOAT)
        resultp->val.float_val = 0.0;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    RDB_set_obj_type(resultp, attrtyp);

    /*
     * Perform aggregation
     */

    qrp = RDB_table_iterator(tbp, 0, NULL, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&hobj);

    while (RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        if (RDB_evaluate(exp, &RDB_tpl_get, &tpl, NULL, ecp, txp, &hobj)
                != RDB_OK) {
            goto error;
        }
        if (attrtyp == &RDB_INTEGER) {
            int a = RDB_obj_int(&hobj);
            if (a > 0) {
                if (resultp->val.int_val > RDB_INT_MAX - a) {
                    RDB_raise_type_constraint_violation("integer overflow", ecp);
                    goto error;
                }
            } else {
                if (resultp->val.int_val < RDB_INT_MIN - a) {
                    RDB_raise_type_constraint_violation("integer overflow", ecp);
                    goto error;
                }
            }
            resultp->val.int_val += a;
        } else
            resultp->val.float_val += RDB_obj_float(&hobj);
    }

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_del_table_iterator(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (qrp != NULL) {
        RDB_del_table_iterator(qrp, ecp, txp);
    }

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_ERROR;
}

/**
 * Computes the average over the expression
*<var>exp</var> of the table specified by <var>tbp</var>
and stores the result at the location
pointed to by <var>resultp</var>.

If the table has only one attribute, <var>exp</var>
may be NULL.

If an error occurs, an error value is left in *<var>ecp</var>.

The type of *<var>exp</var> must be numeric.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>tbp</var> is persistent and *<var>txp</var> is not a running transaction.
<dt>name_error
<dd>*<var>exp</var> refers to a non-existing attribute.
<dt>type_mismatch_error
<dd>The type of the attribute is not numeric.
<dt>invalid_argument_error
<dd><var>exp</var> is NULL and the table has more than one
attribute.
<dt>
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
<dt>aggregate_undefined_error
<dd>The table is empty.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_avg(RDB_object *tbp, RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_float *resultp)
{
    RDB_type *attrtyp;
    RDB_object tpl;
    RDB_object hobj;
    unsigned long count;
    RDB_qresult *qrp = NULL;
    RDB_bool del_over = RDB_FALSE;

    if (exp == NULL) {
        if (tbp->typ->def.basetyp->def.tuple.attrc != 1) {
            RDB_raise_invalid_argument("second argument required", ecp);
            return RDB_ERROR;
        }
        exp = RDB_var_ref(tbp->typ->def.basetyp->def.tuple.attrv[0].name, ecp);
        if (exp == NULL)
            return RDB_ERROR;
        del_over = RDB_TRUE;
    }

    attrtyp = RDB_expr_type(exp, RDB_get_tuple_attr_type, tbp->typ->def.basetyp, NULL, ecp, txp);
    if (attrtyp == NULL) {
        return RDB_ERROR;
    }

    /* Initialize result */
    if (!RDB_type_is_numeric(attrtyp)) {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    qrp = RDB_table_iterator(tbp, 0, NULL, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    RDB_init_obj(&hobj);

    count = 0;
    *resultp = 0.0;
    while (RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        count++;
        if (RDB_evaluate(exp, &RDB_tpl_get, &tpl, NULL, ecp, txp, &hobj)
                != RDB_OK) {
            goto error;
        }
        if (attrtyp == &RDB_INTEGER)
            *resultp += RDB_obj_int(&hobj);
        else
            *resultp += RDB_obj_float(&hobj);
    }

    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        goto error;
    }

    if (count == 0) {
        RDB_raise_aggregate_undefined(ecp);
        goto error;
    }
    *resultp /= count;

    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_del_table_iterator(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_destroy_obj(&hobj, ecp);

    if (qrp != NULL) {
        RDB_del_table_iterator(qrp, ecp, txp);
    }

    if (del_over) {
        RDB_del_expr(exp, ecp);
    }

    return RDB_ERROR;
}

/**
 *
RDB_table_is_empty checks if the table specified by <var>tbp</var>
is empty and stores the result of
the check at the location pointed to by <var>resultp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>tbp</var> is persistent and *<var>txp</var> is not a running transaction.
<dt>operator_not_found_error
<dd>The definition of *<var>tbp</var> refers to a non-existing operator.
<dt>invalid_argument_error
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.

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
<dd>*<var>tbp</var> is persistent and *<var>txp</var> is not a running transaction.
<dt>operator_not_found_error
<dd>The definition of *<var>tbp</var> refers to a non-existing operator.
<dt>invalid_argument_error
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
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
