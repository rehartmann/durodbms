/*
 * $Id$
 *
 * Copyright (C) 2003-2008 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "qresult.h"
#include "internal.h"
#include "stable.h"
#include "optimize.h"

static int
enlarge_buf(RDB_object *arrp, RDB_int len, RDB_exec_context *ecp)
{
    int i;
    void *vp = RDB_realloc(arrp->var.arr.elemv, sizeof (RDB_object) * len, ecp);
    if (vp == NULL)
        return RDB_ERROR;

    arrp->var.arr.elemv = vp;
    for (i = arrp->var.arr.elemc; i < len; i++)
        RDB_init_obj(&arrp->var.arr.elemv[i]);
    arrp->var.arr.elemc = len;
    return RDB_OK;
}

static int
init_expr_array(RDB_object *arrp, RDB_expression *texp,
                   int seqitc, const RDB_seq_item seqitv[], int flags,
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    _RDB_tbindex *indexp = NULL;

    if (arrp->kind == RDB_OB_ARRAY) {
        if (RDB_destroy_obj(arrp, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    if (seqitc > 0) {
        indexp = _RDB_expr_sortindex(texp);
        if (indexp == NULL || !_RDB_index_sorts(indexp, seqitc, seqitv)) {
            /* Create sorter */
            if (_RDB_sorter(texp, &qrp, ecp, txp, seqitc, seqitv) != RDB_OK)
                goto error;
        }
    }
    if (qrp == NULL) {
        qrp = _RDB_expr_qresult(texp, ecp, txp);
        if (qrp == NULL) {
            goto error;
        }
        /* Add duplicate remover, if necessary */
        if (_RDB_duprem(qrp, ecp, txp) != RDB_OK)
            goto error;
    }

    arrp->kind = RDB_OB_ARRAY;
    arrp->var.arr.elemv = NULL;
    arrp->var.arr.tplp = NULL;
    if (RDB_UNBUFFERED & flags) {
        arrp->var.arr.pos = 0;
        arrp->var.arr.texp = texp;
        arrp->var.arr.txp = txp;
        arrp->var.arr.length = -1;
        arrp->var.arr.qrp = qrp;

        return RDB_OK;
    }

    arrp->var.arr.texp = NULL;
    arrp->var.arr.length = 0;
    arrp->var.arr.tplp = NULL;
    arrp->var.arr.qrp = NULL;
    arrp->var.arr.elemc = 0;

    for(;;) {
        /* Extend elemv if necessary to make room for the next element */
        if (arrp->var.arr.elemc <= arrp->var.arr.length) {
            if (enlarge_buf(arrp, arrp->var.arr.elemc + 256, ecp) != RDB_OK) {
                goto error;
            }
        }

        /* Get next tuple */
        if (_RDB_next_tuple(qrp, &arrp->var.arr.elemv[arrp->var.arr.length],
                ecp, txp) != RDB_OK)
            break;
        arrp->var.arr.length++;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        goto error;
    RDB_clear_err(ecp);

    if (_RDB_drop_qresult(qrp, ecp, txp) != RDB_OK) {
        qrp = NULL;
        goto error;
    }

    return RDB_drop_expr(texp, ecp);

error:
    if (qrp != NULL)
        _RDB_drop_qresult(qrp, ecp, txp);
    RDB_drop_expr(texp, ecp);
    return RDB_ERROR;
}

/** @defgroup array Array functions 
 * @{
 */

/** @struct RDB_seq_item rdb.h <rel/rdb.h>
 * This struct is used to specify an attribute and a direction
 * for tuple ordering.
 */

/**
 * RDB_table_to_array creates an array which contains
all tuples from the table specified by <var>tbp</var>.
If <var>seqitc</var> is zero, the order of the tuples is undefined.
If <var>seqitc</var> is greater than zero, the order of the tuples
is specified by <var>seqitv</var>.

@param flags    Must be 0 or RDB_UNBUFFERED. If flags is set to RDB_UNBUFFERED,
an array access will result in an access to *<var>tbp</var>
with the following consequences:
<ul>
<li>*<var>tbp</var> must not be deleted before the array is destroyed.
<li>*<var>arrp</var> will become inaccessible if the transaction
is committed or rolled back.
<li>Write access to *<var>arrp</var> is not supported.
</ul>

@returns
RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:
<dl>
<dt>NO_RUNNING_TX_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>OPERATOR_NOT_FOUND_ERROR
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
<dt>INVALID_ARGUMENT
<dd>*<var>arrp</var> is neither newly initialized nor an array.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_table_to_array(RDB_object *arrp, RDB_object *tbp,
                   int seqitc, const RDB_seq_item seqitv[], int flags,
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *texp;

    if (arrp->kind != RDB_OB_INITIAL && arrp->kind != RDB_OB_ARRAY) {
        RDB_raise_invalid_argument("no array", ecp);
        return RDB_ERROR;
    }

    texp = _RDB_optimize(tbp, seqitc, seqitv, ecp, txp);
    if (texp == NULL)
        return RDB_ERROR;

    return init_expr_array(arrp, texp, seqitc, seqitv, flags, ecp, txp);
}

/**
 * Get next element from qresult
 */
static int
next_tuple(RDB_object *arrp, RDB_bool mustread, RDB_exec_context *ecp)
{
    RDB_object *tplp = mustread ? arrp->var.arr.tplp : NULL;

    return _RDB_next_tuple(arrp->var.arr.qrp, tplp,
                ecp, arrp->var.arr.txp);
}

/**
 * RDB_array_get returns a pointer to the RDB_object at index <var>idx</var>.
This pointer may become invalid after the next invocation of RDB_array_get().
The pointer will become invalid when the array is destroyed.

@returns
A pointer to the array element, or NULL if an error occurred.

@par Errors:
<dl>
<dt>NOT_FOUND_ERROR
<dd><var>idx</var> exceeds the array length.
<dt>OPERATOR_NOT_FOUND_ERROR
<dd>The array was created from a table which refers to a non-existing
operator.
</dl>

The call may also fail for a @ref system-errors "system error".
*/
RDB_object *
RDB_array_get(RDB_object *arrp, RDB_int idx, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *tplp;

    if (arrp->kind == RDB_OB_INITIAL ||
            (arrp->var.arr.length != -1 && idx >= arrp->var.arr.length)) {
        RDB_raise_not_found("array index out of bounds", ecp);
        return NULL;
    }

    if (arrp->var.arr.texp == NULL) {
        return &arrp->var.arr.elemv[idx];
    }

    /* Reset qresult to start, if necessary */
    if (arrp->var.arr.pos > idx) {
        ret = _RDB_reset_qresult(arrp->var.arr.qrp, ecp, arrp->var.arr.txp);
        arrp->var.arr.pos = 0;
        if (ret != RDB_OK)
            return NULL;
    }

    if (arrp->var.arr.tplp == NULL) {
        arrp->var.arr.tplp = RDB_alloc(sizeof (RDB_object), ecp);
        if (arrp->var.arr.tplp == NULL) {
            return NULL;
        }
        RDB_init_obj(arrp->var.arr.tplp);
    }
         
    /*
     * Move forward until the right position is reached
     */
    while (arrp->var.arr.pos < idx) {
        ret = next_tuple(arrp, RDB_FALSE, ecp);
        if (ret != RDB_OK) {
            return NULL;
        }
        ++arrp->var.arr.pos;
    }

    /* Read next element */
    ret = next_tuple(arrp, RDB_TRUE, ecp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            arrp->var.arr.length = arrp->var.arr.pos;
            if (arrp->var.arr.texp->kind == RDB_EX_TBP
                    && arrp->var.arr.texp->var.tbref.tbp->var.tb.stp != NULL) {
                arrp->var.arr.texp->var.tbref.tbp->var.tb.stp->est_cardinality =
                        arrp->var.arr.length;
            }
        }
        return NULL;
    }

    tplp = arrp->var.arr.tplp;
    ++arrp->var.arr.pos;

    return tplp;
}

/**
 * RDB_array_length returns the length of an array.

@returns
The length of the array. A return code lower than zero
indicates an error.

@par Errors:
<dl>
<dt>OPERATOR_NOT_FOUND_ERROR
<dd>The array was created from a table which refers to a non-existing
operator.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
RDB_int
RDB_array_length(RDB_object *arrp, RDB_exec_context *ecp)
{
    if (arrp->kind == RDB_OB_INITIAL)
        return 0;

    if (arrp->var.arr.length == -1) {
        int i = arrp->var.arr.pos;
        RDB_type *errtyp;

        while (RDB_array_get(arrp, (RDB_int) i, ecp) != NULL)
            i++;
        errtyp = RDB_obj_type(RDB_get_err(ecp));
        if (errtyp != &RDB_NOT_FOUND_ERROR)
            return (RDB_int) RDB_ERROR;
        RDB_clear_err(ecp);
    }
    return arrp->var.arr.length;
}

/**
 * RDB_set_array_length sets the length of the array specified by
<var>arrp</var>.

This function is not supported for arrays which have been created
using RDB_table_to_array with the RDB_UNBUFFERED flag.

@returns
RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:
<dl>
<dt>NOT_SUPPORTED_ERROR
<dd>The array has been created using RDB_table_to_array with the RDB_UNBUFFERED flag.
</dl>
 */
int
RDB_set_array_length(RDB_object *arrp, RDB_int len, RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (arrp->kind == RDB_OB_INITIAL) {
        arrp->kind = RDB_OB_ARRAY;

        arrp->var.arr.elemv = RDB_alloc(sizeof(RDB_object) * len, ecp);
        if (arrp->var.arr.elemv == NULL) {
            return RDB_ERROR;
        }
        for (i = 0; i < len; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);

        arrp->var.arr.texp = NULL;
        arrp->var.arr.length = arrp->var.arr.elemc = len;
        
        return RDB_OK;
    }
    if (arrp->var.arr.texp != NULL) {
        RDB_raise_not_supported(
                "cannot set length of array created from table", ecp);
        return RDB_ERROR;
    }

    if (len < arrp->var.arr.length) {
        void *vp;
        /* Shrink array */
        for (i = len; i < arrp->var.arr.length; i++) {
            ret = RDB_destroy_obj(&arrp->var.arr.elemv[i], ecp);
            if (ret != RDB_OK)
                return ret;
        }
        vp = RDB_realloc(arrp->var.arr.elemv, sizeof (RDB_object) * len, ecp);
        if (vp == NULL)
            return RDB_ERROR;
        arrp->var.arr.elemv = vp;
    } else if (len < arrp->var.arr.length) {
        /* Enlarge array */
        if (enlarge_buf(arrp, len, ecp) != RDB_OK)
            return RDB_ERROR;
    }
    arrp->var.arr.length = len;
        
    return RDB_OK;
}

/**
 * RDB_array_set copies the RDB_object pointed to by tplp
into the RDB_object at index <var>idx</var>.

RDB_array_set is not supported for arrays which have been created
using RDB_table_to_array with the RDB_UNBUFFERED flag.

@returns
RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:
<dl>
<dt>NOT_FOUND_ERROR
<dd><var>idx</var> exceeds the array length.
<dt>NOT_SUPPORTED_ERROR
<dd>The table has been created using RDB_table_to_array with the RDB_UNBUFFERED flag.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_array_set(RDB_object *arrp, RDB_int idx, const RDB_object *objp,
        RDB_exec_context *ecp)
{
    if (arrp->var.arr.texp != NULL) {
        RDB_raise_not_supported("setting array element is not permitted", ecp);
        return RDB_ERROR;
    }

    if (idx >= arrp->var.arr.length) {
        RDB_raise_not_found("index out of bounds", ecp);
        return RDB_ERROR;
    }

    return RDB_copy_obj(&arrp->var.arr.elemv[idx], objp, ecp);
}

/*@}*/

int
_RDB_copy_array(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp)
{
    int i;
    RDB_object *objp;
    int len = RDB_array_length((RDB_object *) srcp, ecp);

    if (len == -1) {
        RDB_raise_not_supported("invalid source array", ecp);
        return RDB_ERROR;
    }

    if (RDB_set_array_length(dstp, len, ecp) != RDB_OK)
        return RDB_ERROR;

    for (i = 0; i < len; i++) {
        objp = RDB_array_get((RDB_object *) srcp, (RDB_int) i, ecp);
        if (objp == NULL)
            return RDB_ERROR;
        if (RDB_array_set(dstp, (RDB_int) i, objp, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    return RDB_OK;
}

int
_RDB_array_equals(RDB_object *arr1p, RDB_object *arr2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_object *obj1p, *obj2p;
    int i = 0;

    do {
        obj1p = RDB_array_get(arr1p, (RDB_int) i, ecp);
        if (obj1p == NULL) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                return RDB_ERROR;
            RDB_clear_err(ecp);
        }
        obj2p = RDB_array_get(arr2p, (RDB_int) i, ecp);
        if (obj2p == NULL) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                return RDB_ERROR;
            RDB_clear_err(ecp);
        }
        if ((obj1p == NULL && obj2p != NULL)
                || (obj1p != NULL && obj2p == NULL)) {
            *resp = RDB_FALSE;
            return RDB_OK;
        }

        if (obj1p == NULL) {
            /* At end of both arrays, which means they are equal */
            *resp = RDB_TRUE;
            return RDB_OK;
        }
        i++;
        ret = RDB_obj_equals(obj1p, obj2p, ecp, txp, resp);
    } while (ret == RDB_OK && *resp);
    return ret;
}
