/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "qresult.h"
#include "internal.h"
#include "stable.h"
#include "optimize.h"
#include <obj/objinternal.h>

static int
enlarge_buf(RDB_object *arrp, RDB_int len, RDB_exec_context *ecp)
{
    int i;
    void *vp = RDB_realloc(arrp->val.arr.elemv, sizeof (RDB_object) * len, ecp);
    if (vp == NULL)
        return RDB_ERROR;

    arrp->val.arr.elemv = vp;
    for (i = arrp->val.arr.elemc; i < len; i++)
        RDB_init_obj(&arrp->val.arr.elemv[i]);
    arrp->val.arr.elemc = len;
    return RDB_OK;
}

static int
cleanup_qrarr(RDB_object *arrp, RDB_exec_context *ecp)
{
    return RDB_del_qresult(arrp->val.arr.qrp, ecp,
                arrp->val.arr.txp);
}

static int
init_expr_array(RDB_object *arrp, RDB_expression *texp,
                   int seqitc, const RDB_seq_item seqitv[], int flags,
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    RDB_tbindex *indexp = NULL;

    if (seqitc > 0) {
        indexp = RDB_expr_sortindex(texp);
        if (indexp == NULL || !RDB_index_sorts(indexp, seqitc, seqitv)) {
            /* Create sorter */
            if (RDB_sorter(texp, &qrp, ecp, txp, seqitc, seqitv) != RDB_OK)
                goto error;
        }
    }
    if (qrp == NULL) {
        qrp = RDB_expr_qresult(texp, ecp, txp);
        if (qrp == NULL) {
            goto error;
        }
        /* Add duplicate remover, if necessary */
        if (RDB_duprem(qrp, ecp, txp) != RDB_OK)
            goto error;
    }

    if (arrp->kind == RDB_OB_ARRAY) {
        if (RDB_destroy_obj(arrp, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    arrp->kind = RDB_OB_ARRAY;
    arrp->val.arr.elemv = NULL;
    arrp->val.arr.tplp = NULL;
    if (RDB_UNBUFFERED & flags) {
        arrp->val.arr.pos = 0;
        arrp->val.arr.texp = texp;
        arrp->val.arr.txp = txp;
        arrp->val.arr.length = -1;
        arrp->val.arr.qrp = qrp;
        arrp->cleanup_fp = &cleanup_qrarr;

        return RDB_OK;
    }

    arrp->val.arr.texp = NULL;
    arrp->val.arr.length = 0;
    arrp->val.arr.tplp = NULL;
    arrp->val.arr.qrp = NULL;
    arrp->val.arr.elemc = 0;

    for(;;) {
        /* Extend elemv if necessary to make room for the next element */
        if (arrp->val.arr.elemc <= arrp->val.arr.length) {
            if (enlarge_buf(arrp, arrp->val.arr.elemc + 256, ecp) != RDB_OK) {
                goto error;
            }
        }

        /* Get next tuple */
        if (RDB_next_tuple(qrp, &arrp->val.arr.elemv[arrp->val.arr.length],
                ecp, txp) != RDB_OK)
            break;
        arrp->val.arr.length++;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        goto error;
    RDB_clear_err(ecp);

    if (RDB_del_qresult(qrp, ecp, txp) != RDB_OK) {
        qrp = NULL;
        goto error;
    }

    return RDB_del_expr(texp, ecp);

error:
    if (qrp != NULL)
        RDB_del_qresult(qrp, ecp, txp);
    RDB_del_expr(texp, ecp);
    return RDB_ERROR;
}

/**@defgroup array Array functions
 * @{
 * \#include <rel/rdb.h>
 *
 */

/**
 * Create an array which contains
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
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>operator_not_found_error
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
<dt>invalid_argument_error
<dd>*<var>arrp</var> is neither newly initialized nor an array.
<dd>The table represented by *<var>tbp</var> does not exist. (e.g. after a rollback)
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

    texp = RDB_optimize(tbp, seqitc, seqitv, ecp, txp);
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
    RDB_object *tplp = mustread ? arrp->val.arr.tplp : NULL;

    return RDB_next_tuple(arrp->val.arr.qrp, tplp,
                ecp, arrp->val.arr.txp);
}

/**
 * RDB_array_get returns a pointer to the RDB_object at index <var>idx</var>.
This pointer may become invalid after the next invocation of RDB_array_get().
The pointer will become invalid when the array is destroyed.

@returns
A pointer to the array element, or NULL if an error occurred.

@par Errors:
<dl>
<dt>not_found_error
<dd><var>idx</var> exceeds the array length.
<dt>operator_not_found_error
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
            (arrp->val.arr.length != -1 && idx >= arrp->val.arr.length)) {
        RDB_raise_not_found("array index out of bounds", ecp);
        return NULL;
    }

    if (arrp->val.arr.texp == NULL) {
        return &arrp->val.arr.elemv[idx];
    }

    /* Reset qresult to start, if necessary */
    if (arrp->val.arr.pos > idx) {
        ret = RDB_reset_qresult(arrp->val.arr.qrp, ecp, arrp->val.arr.txp);
        arrp->val.arr.pos = 0;
        if (ret != RDB_OK)
            return NULL;
    }

    if (arrp->val.arr.tplp == NULL) {
        arrp->val.arr.tplp = RDB_alloc(sizeof (RDB_object), ecp);
        if (arrp->val.arr.tplp == NULL) {
            return NULL;
        }
        RDB_init_obj(arrp->val.arr.tplp);
    }
         
    /*
     * Move forward until the right position is reached
     */
    while (arrp->val.arr.pos < idx) {
        ret = next_tuple(arrp, RDB_FALSE, ecp);
        if (ret != RDB_OK) {
            return NULL;
        }
        ++arrp->val.arr.pos;
    }

    /* Read next element */
    ret = next_tuple(arrp, RDB_TRUE, ecp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            arrp->val.arr.length = arrp->val.arr.pos;
            if (arrp->val.arr.texp->kind == RDB_EX_TBP
                    && arrp->val.arr.texp->def.tbref.tbp->val.tb.stp != NULL) {
                arrp->val.arr.texp->def.tbref.tbp->val.tb.stp->est_cardinality =
                        arrp->val.arr.length;
            }
        }
        return NULL;
    }

    tplp = arrp->val.arr.tplp;
    ++arrp->val.arr.pos;

    return tplp;
}

/**
 * RDB_array_length returns the length of an array.

@returns
The length of the array. A return code lower than zero
indicates an error.

@par Errors:
<dl>
<dt>operator_not_found_error
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

    if (arrp->val.arr.length == -1) {
        int i = arrp->val.arr.pos;
        RDB_type *errtyp;

        while (RDB_array_get(arrp, (RDB_int) i, ecp) != NULL)
            i++;
        errtyp = RDB_obj_type(RDB_get_err(ecp));
        if (errtyp != &RDB_NOT_FOUND_ERROR)
            return (RDB_int) RDB_ERROR;
        RDB_clear_err(ecp);
    }
    return arrp->val.arr.length;
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
<dt>not_supported_error
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

        arrp->val.arr.elemv = RDB_alloc(sizeof(RDB_object) * len, ecp);
        if (arrp->val.arr.elemv == NULL) {
            return RDB_ERROR;
        }
        for (i = 0; i < len; i++)
            RDB_init_obj(&arrp->val.arr.elemv[i]);

        arrp->val.arr.texp = NULL;
        arrp->val.arr.length = arrp->val.arr.elemc = len;
        
        return RDB_OK;
    }
    if (arrp->val.arr.texp != NULL) {
        RDB_raise_not_supported(
                "cannot set length of array created from table", ecp);
        return RDB_ERROR;
    }

    if (len < arrp->val.arr.length) {
        void *vp;
        /* Shrink array */
        for (i = len; i < arrp->val.arr.length; i++) {
            ret = RDB_destroy_obj(&arrp->val.arr.elemv[i], ecp);
            if (ret != RDB_OK)
                return ret;
        }
        if (len > 0) {
            vp = RDB_realloc(arrp->val.arr.elemv, sizeof (RDB_object) * len, ecp);
            if (vp == NULL)
                return RDB_ERROR;
        } else {
            RDB_free(arrp->val.arr.elemv);
            vp = NULL;
        }
        arrp->val.arr.elemv = vp;
    } else if (len < arrp->val.arr.length) {
        /* Enlarge array */
        if (enlarge_buf(arrp, len, ecp) != RDB_OK)
            return RDB_ERROR;
    }
    arrp->val.arr.length = len;
        
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
<dt>not_found_error
<dd><var>idx</var> exceeds the array length.
<dt>not_supported_error
<dd>The table has been created using RDB_table_to_array with the RDB_UNBUFFERED flag.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_array_set(RDB_object *arrp, RDB_int idx, const RDB_object *objp,
        RDB_exec_context *ecp)
{
    if (arrp->val.arr.texp != NULL) {
        RDB_raise_not_supported("setting array element is not permitted", ecp);
        return RDB_ERROR;
    }

    if (idx >= arrp->val.arr.length) {
        RDB_raise_not_found("index out of bounds", ecp);
        return RDB_ERROR;
    }

    return RDB_copy_obj(&arrp->val.arr.elemv[idx], objp, ecp);
}

/*@}*/

int
RDB_copy_array(RDB_object *dstp, const RDB_object *srcp,
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
RDB_array_equals(RDB_object *arr1p, RDB_object *arr2p, RDB_exec_context *ecp,
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
