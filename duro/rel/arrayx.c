/*
 * Copyright (C) 2009, 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "qresult.h"
#include "internal.h"
#include "stable.h"
#include "optimize.h"
#include <obj/objinternal.h>

#include <limits.h>

/*
 * Number of entries by which the buffer will be extended
 * when more space is needed
 */
enum {
    BUF_INCREMENT = 256
};

static int
init_expr_array(RDB_object *arrp, RDB_expression *texp,
                   int seqitc, const RDB_seq_item seqitv[], RDB_int limit,
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    RDB_tbindex *indexp = NULL;

    if (seqitc > 0) {
        indexp = RDB_expr_sortindex(texp);
        if (indexp == NULL || !RDB_index_sorts(indexp, seqitc, seqitv)
                || (txp != NULL && RDB_env_queries(txp->envp))) {
            /* Create sorter */
            if (txp != NULL && RDB_env_trace(RDB_db_env(RDB_tx_db(txp))) > 0) {
                fputs("Creating sorter\n", stderr);
            }
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

    arrp->val.arr.length = 0;
    arrp->val.arr.capacity = 0;

    for (;;) {
        /* Extend elemv if necessary to make room for the next element */
        if (arrp->val.arr.capacity <= arrp->val.arr.length) {
            if (RDB_enlarge_array_buf(arrp, arrp->val.arr.capacity + BUF_INCREMENT, ecp)
                    != RDB_OK) {
                goto error;
            }
        }

        /* Get next tuple */
        if (RDB_next_tuple(qrp, &arrp->val.arr.elemv[arrp->val.arr.length],
                ecp, txp) != RDB_OK) {
            break;
        }
        if (arrp->val.arr.length == limit) {
            break;
        }
        arrp->val.arr.length++;
    }
    if (RDB_get_err(ecp) != NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
            goto error;
        RDB_clear_err(ecp);
    }

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

int
RDB_table_to_array_limit(RDB_object *arrp, RDB_object *tbp,
                   int seqitc, const RDB_seq_item seqitv[], int flags,
                   RDB_int limit, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *texp;

    if (arrp->kind != RDB_OB_INITIAL && arrp->kind != RDB_OB_ARRAY) {
        RDB_raise_invalid_argument("no array", ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < seqitc; i++) {
        RDB_type *attrtyp = RDB_type_attr_type(RDB_obj_type(tbp),
                seqitv[i].attrname);
        if (attrtyp == NULL) {
            RDB_raise_invalid_argument("attribute not found", ecp);
            return RDB_ERROR;
        }
        if (!RDB_type_is_ordered(attrtyp)) {
            RDB_raise_invalid_argument("attribute type is not ordered", ecp);
            return RDB_ERROR;
        }
    }

    texp = RDB_optimize(tbp, seqitc, seqitv, ecp, txp);
    if (texp == NULL)
        return RDB_ERROR;

    return init_expr_array(arrp, texp, seqitc, seqitv, limit, ecp, txp);
}

/**
 * Create an array which contains
all tuples from the table specified by <var>tbp</var>.
If <var>seqitc</var> is zero, the order of the tuples is undefined.
If <var>seqitc</var> is greater than zero, the order of the tuples
is specified by <var>seqitv</var>.

@param flags    Currently unused and should be set to zero

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
<dd>One of the attributes in seqitv does not exist or is not of an ordered type.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_table_to_array(RDB_object *arrp, RDB_object *tbp,
                   int seqitc, const RDB_seq_item seqitv[], int flags,
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    return RDB_table_to_array_limit(arrp, tbp, seqitc, seqitv, flags, RDB_INT_MAX, ecp, txp);
}

/*@}*/

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
