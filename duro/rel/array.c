/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"

int
RDB_table_to_array(RDB_object *arrp, RDB_table *tbp,
                   int seqitc, const RDB_seq_item seqitv[],
                   RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_table *ntbp;
    _RDB_tbindex *indexp = NULL;

    ret = RDB_destroy_obj(arrp, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = _RDB_optimize(tbp, seqitc, seqitv, ecp, txp, &ntbp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    arrp->kind = RDB_OB_ARRAY;
    arrp->var.arr.tbp = ntbp;
    arrp->var.arr.txp = txp;
    arrp->var.arr.qrp = NULL;
    arrp->var.arr.length = -1;
    arrp->var.arr.tplp = NULL;
    arrp->var.arr.elemv = NULL;
    arrp->var.arr.pos = 0;

    if (seqitc > 0) {
        indexp = _RDB_sortindex(arrp->var.arr.tbp);
        if (indexp == NULL || !_RDB_index_sorts(indexp, seqitc, seqitv)) {
            /* Create sorter */
            ret = _RDB_sorter(arrp->var.arr.tbp, &arrp->var.arr.qrp, ecp, txp,
                    seqitc, seqitv);
            if (ret != RDB_OK)
                goto error;
        }
    }
    if (arrp->var.arr.qrp == NULL) {
        arrp->var.arr.qrp = _RDB_table_qresult(arrp->var.arr.tbp, ecp,
                arrp->var.arr.txp);
        if (arrp->var.arr.qrp == NULL) {
            goto error;
        }
        /* Add duplicate remover, if necessary */
        ret = _RDB_duprem(arrp->var.arr.qrp, ecp);
        if (ret != RDB_OK)
            goto error;
    }
    
    return RDB_OK;

error:
    RDB_drop_table(arrp->var.arr.tbp, ecp, txp);
    return RDB_ERROR;
}

/*
 * Get next element from qresult
 */

static int
next_tuple(RDB_object *arrp, RDB_bool mustread, RDB_exec_context *ecp)
{
    RDB_object *tplp;

    if (arrp->var.arr.pos < arrp->var.arr.elemc) {
        tplp = &arrp->var.arr.elemv[arrp->var.arr.pos];
        if (tplp->kind == RDB_OB_TUPLE) {
            /* Don't read same tuple again */
            tplp = NULL;
        }
    } else {
        tplp = mustread ? arrp->var.arr.tplp : NULL;
    }

    return _RDB_next_tuple(arrp->var.arr.qrp, tplp,
                ecp, arrp->var.arr.txp);
}

/* !! should be made configurable */
enum {
    ARRAY_BUFLEN_MIN = 256,
    ARRAY_BUFLEN_MAX = 32768
};

RDB_object *
RDB_array_get(RDB_object *arrp, RDB_int idx, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *tplp;

    if (arrp->var.arr.length != -1 && idx >= arrp->var.arr.length) {
        RDB_raise_not_found("array index out of bounds", ecp);
        return NULL;
    }

    if (arrp->var.arr.tbp == NULL) {
        return &arrp->var.arr.elemv[idx];
    }

    /* If the element is in buffer, return it */
    if (idx < arrp->var.arr.elemc && idx < arrp->var.arr.pos) {
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
        arrp->var.arr.tplp = malloc(sizeof (RDB_object));
        if (arrp->var.arr.tplp == NULL) {
            RDB_raise_no_memory(ecp);
            return NULL;
        }
        RDB_init_obj(arrp->var.arr.tplp);
    }

    /*
     * Allocate buffer, not already present
     */
    if (arrp->var.arr.elemv == NULL) {
        int i;
        
        arrp->var.arr.elemc = ARRAY_BUFLEN_MIN;
        if (idx > arrp->var.arr.elemc)
            arrp->var.arr.elemc = idx + 1;
        if (arrp->var.arr.elemc > ARRAY_BUFLEN_MAX)
            arrp->var.arr.elemc = ARRAY_BUFLEN_MAX;
        arrp->var.arr.elemv = malloc (sizeof(RDB_object) * arrp->var.arr.elemc);
        if (arrp->var.arr.elemv == NULL) {
            RDB_raise_no_memory(ecp);
            return NULL;
        }

        for (i = 0; i < arrp->var.arr.elemc; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);
    }

    /* Enlarge buffer if necessary and permitted */
    if (idx >= arrp->var.arr.elemc && idx < ARRAY_BUFLEN_MAX) {
        int oldelemc = arrp->var.arr.elemc;
        int elemc = oldelemc;
        int i;

        do {
            elemc *= 2;
        } while (elemc < idx);

        arrp->var.arr.elemv = realloc(arrp->var.arr.elemv,
                 sizeof(RDB_object) * elemc);
        if (arrp->var.arr.elemv == NULL) {
            RDB_raise_no_memory(ecp);
            return NULL;
        }
        for (i = oldelemc; i < elemc; i++)
           RDB_init_obj(&arrp->var.arr.elemv[i]);
        arrp->var.arr.elemc = elemc;
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
            if (arrp->var.arr.tbp->kind == RDB_TB_REAL
                    && arrp->var.arr.tbp->stp != NULL) {
                arrp->var.arr.tbp->stp->est_cardinality =
                        arrp->var.arr.length;
            }
        }
        return NULL;
    }

    if (arrp->var.arr.pos < arrp->var.arr.elemc) {
        tplp = &arrp->var.arr.elemv[arrp->var.arr.pos];
    } else {
        tplp = arrp->var.arr.tplp;
    }
    ++arrp->var.arr.pos;

    return tplp;
}

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
            return RDB_ERROR;
        RDB_clear_err(ecp);
    }
    return arrp->var.arr.length;
}

int
RDB_set_array_length(RDB_object *arrp, RDB_int len, RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (arrp->kind == RDB_OB_INITIAL) {
        arrp->kind = RDB_OB_ARRAY;

        arrp->var.arr.elemv = malloc(sizeof(RDB_object) * len);
        if (arrp->var.arr.elemv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        for (i = 0; i < len; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);

        arrp->var.arr.tbp = NULL;
        arrp->var.arr.length = arrp->var.arr.elemc = len;
        
        return RDB_OK;
    }
    if (arrp->var.arr.tbp != NULL) {
        RDB_raise_not_supported(
                "cannot set length of array created from table", ecp);
        return RDB_ERROR;
    }

    if (len < arrp->var.arr.length) {
        /* Shrink array */
        for (i = len; i < arrp->var.arr.length; i++) {
            ret = RDB_destroy_obj(&arrp->var.arr.elemv[i], ecp);
            if (ret != RDB_OK)
                return ret;
        }            
        arrp->var.arr.elemv = realloc(arrp->var.arr.elemv,
                sizeof (RDB_object) * len);
    } else if (len < arrp->var.arr.length) {
        /* Enlarge array */
        arrp->var.arr.elemv = realloc(arrp->var.arr.elemv,
                sizeof (RDB_object) * len);
        for (i = arrp->var.arr.length; i < len; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);
    }
    arrp->var.arr.length = len;
        
    return RDB_OK;
}

int
RDB_array_set(RDB_object *arrp, RDB_int idx, const RDB_object *objp,
        RDB_exec_context *ecp)
{
    if (arrp->var.arr.tbp != NULL) {
        RDB_raise_not_supported("setting array element is not permitted", ecp);
        return RDB_ERROR;
    }

    if (idx >= arrp->var.arr.length) {
        RDB_raise_not_found("index out of bounds", ecp);
        return RDB_ERROR;
    }

    return RDB_copy_obj(&arrp->var.arr.elemv[idx], objp, ecp);
}

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
