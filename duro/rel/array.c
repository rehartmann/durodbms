/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"

int
RDB_table_to_array(RDB_object *arrp, RDB_table *tbp,
                   int seqitc, const RDB_seq_item seqitv[],
                   RDB_transaction *txp)
{
    int ret;

    ret = RDB_destroy_obj(arrp);
    if (ret != RDB_OK)
        return ret;

    arrp->kind = RDB_OB_ARRAY;
    arrp->var.arr.tbp = tbp;
    arrp->var.arr.txp = txp;
    arrp->var.arr.qrp = NULL;
    arrp->var.arr.length = -1;
    arrp->var.arr.tplp = NULL;
    arrp->var.arr.elemv = NULL;
    arrp->var.arr.pos = 0;

    if (seqitc > 0) {
        /* Create sorter */
        ret = _RDB_sorter(tbp, &arrp->var.arr.qrp, txp, seqitc, seqitv);
        if (ret != RDB_OK)
            return ret;
    }
    
    return RDB_OK;
}    

/*
 * Get next element from qresult
 */

static int
next_tuple(RDB_object *arrp, RDB_bool mustread)
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
                arrp->var.arr.txp);
}

/* !! should be made configurable */
enum {
    ARRAY_BUFLEN_MIN = 256,
    ARRAY_BUFLEN_MAX = 32768
};

int
RDB_array_get(RDB_object *arrp, RDB_int idx, RDB_object **tplpp)
{
    int ret;

    if (arrp->var.arr.length != -1 && idx >= arrp->var.arr.length)
        return RDB_NOT_FOUND;

    if (arrp->var.arr.tbp == NULL) {
        *tplpp = &arrp->var.arr.elemv[idx];
        return RDB_OK;
    }

    /* If the element is in buffer, return it */
    if (idx < arrp->var.arr.elemc && idx < arrp->var.arr.pos) {
        *tplpp = &arrp->var.arr.elemv[idx];
        return RDB_OK;
    }

    /* If there is no qresult, create it */
    if (arrp->var.arr.qrp == NULL) {
        ret = _RDB_table_qresult(arrp->var.arr.tbp, arrp->var.arr.txp,
                &arrp->var.arr.qrp);
        if (ret != RDB_OK) {
            arrp->var.arr.qrp = NULL;
            if (RDB_is_syserr(ret))
                RDB_rollback_all(arrp->var.arr.txp);
            return ret;
        }
        arrp->var.arr.pos = 0;
    }

    /* Reset qresult to start, if necessary */
    if (arrp->var.arr.pos > idx) {
        ret = _RDB_reset_qresult(arrp->var.arr.qrp, arrp->var.arr.txp);
        arrp->var.arr.pos = 0;
        if (ret != RDB_OK)
            return ret;
    }

    if (arrp->var.arr.tplp == NULL) {
        arrp->var.arr.tplp = malloc(sizeof (RDB_object));
        if (arrp->var.arr.tplp == NULL)
            return RDB_NO_MEMORY;
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
        if (arrp->var.arr.elemv == NULL)
            return RDB_NO_MEMORY;

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
         if (arrp->var.arr.elemv == NULL)
             return RDB_NO_MEMORY;
         for (i = oldelemc; i < elemc; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);
         arrp->var.arr.elemc = elemc;
    }
         
    /*
     * Move forward until the right position is reached
     */
    while (arrp->var.arr.pos < idx) {
        ret = next_tuple(arrp, RDB_FALSE);
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret)) {
                _RDB_drop_qresult(arrp->var.arr.qrp, arrp->var.arr.txp);
                arrp->var.arr.qrp = NULL;
                RDB_rollback_all(arrp->var.arr.txp);
            }
            return ret;
        }
        ++arrp->var.arr.pos;
    }

    /* Read next element */
    ret = next_tuple(arrp, RDB_TRUE);
    if (ret != RDB_OK) {
        if (ret == RDB_NOT_FOUND) {
            arrp->var.arr.length = arrp->var.arr.pos;
            if (arrp->var.arr.tbp->kind == RDB_TB_REAL) {
                arrp->var.arr.tbp->var.real.est_cardinality =
                        arrp->var.arr.length;
            }
        } else if (RDB_is_syserr(ret)) {
            _RDB_drop_qresult(arrp->var.arr.qrp, arrp->var.arr.txp);
            arrp->var.arr.qrp = NULL;
            RDB_rollback_all(arrp->var.arr.txp);
        }
        return ret;
    }

    if (arrp->var.arr.pos < arrp->var.arr.elemc) {
        *tplpp = &arrp->var.arr.elemv[arrp->var.arr.pos];
    } else {
        *tplpp = arrp->var.arr.tplp;
    }
    ++arrp->var.arr.pos;

    return ret;
}

RDB_int
RDB_array_length(RDB_object *arrp)
{
    int ret;

    if (arrp->kind == RDB_OB_INITIAL)
        return 0;

    if (arrp->var.arr.length == -1) {
        RDB_object *tplp;
        int i = arrp->var.arr.pos;

        while ((ret = RDB_array_get(arrp, (RDB_int) i, &tplp)) == RDB_OK)
            i++;
        if (ret != RDB_NOT_FOUND)
            return ret;
    }
    return arrp->var.arr.length;
}

/* currently unused
static int
detach_array(RDB_object *arrp)
{
    int ret;

    if (arrp->var.arr.tbp == NULL)
        return RDB_OK;

    if (arrp->var.arr.length == -1
            || arrp->var.arr.length > arrp->var.arr.elemc)
        return RDB_NOT_SUPPORTED;

    if (arrp->var.arr.qrp != NULL) {
        ret = _RDB_drop_qresult(arrp->var.arr.qrp,
                arrp->var.arr.txp);
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret) && arrp->var.arr.txp != NULL)
                RDB_rollback_all(arrp->var.arr.txp);
            return ret;
        }
        arrp->var.arr.qrp = NULL;
    }

    arrp->var.arr.tbp = NULL;
    arrp->var.arr.txp = NULL;
    return RDB_OK;
}
*/

int
RDB_set_array_length(RDB_object *arrp, RDB_int len)
{
    int i;
    int ret;

    if (arrp->kind == RDB_OB_INITIAL) {
        arrp->kind = RDB_OB_ARRAY;

        arrp->var.arr.elemv = malloc(sizeof(RDB_object) * len);
        if (arrp->var.arr.elemv == NULL)
            return RDB_NO_MEMORY;
        for (i = 0; i < len; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);

        arrp->var.arr.tbp = NULL;
        arrp->var.arr.length = arrp->var.arr.elemc = len;
        
        return RDB_OK;
    }
    if (arrp->var.arr.tbp != NULL) {
        return RDB_NOT_SUPPORTED;
    }

    if (len < arrp->var.arr.length) {
        /* Shrink array */
        for (i = len; i < arrp->var.arr.length; i++) {
            ret = RDB_destroy_obj(&arrp->var.arr.elemv[i]);
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
RDB_array_set(RDB_object *arrp, RDB_int idx, const RDB_object *objp)
{
    if (arrp->var.arr.tbp != NULL) {
        return RDB_NOT_SUPPORTED;
    }

    if (idx >= arrp->var.arr.length)
        return RDB_NOT_FOUND;

    return RDB_copy_obj(&arrp->var.arr.elemv[idx], objp);
}

int
_RDB_copy_array(RDB_object *dstp, const RDB_object *srcp)
{
    int ret;
    int i;
    RDB_object *objp;
    int len = RDB_array_length((RDB_object *) srcp);

    if (len == -1)
        return RDB_NOT_SUPPORTED;

    ret = RDB_set_array_length(dstp, len);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < len; i++) {
        ret = RDB_array_get((RDB_object *) srcp, (RDB_int) i, &objp);
        if (ret != RDB_OK)
            return ret;
        ret = RDB_array_set(dstp, (RDB_int) i, objp);
        if (ret != RDB_OK)
            return ret;
    }

    return RDB_OK;
}

int
_RDB_array_equals(RDB_object *arr1p, RDB_object *arr2p, RDB_bool *resp)
{
    int ret, ret2;
    RDB_object *obj1p, *obj2p;
    int i = 0;

    do {
        ret = RDB_array_get(arr1p, (RDB_int) i, &obj1p);
        if (ret != RDB_OK && ret != RDB_NOT_FOUND)
            return ret;
        ret2 = RDB_array_get(arr2p, (RDB_int) i, &obj2p);
        if (ret2 != RDB_OK && ret2 != RDB_NOT_FOUND)
            return ret2;
        if (ret != ret2) {
            *resp = RDB_FALSE;
            return RDB_OK;
        }
        if (ret == RDB_NOT_FOUND) {
            *resp = RDB_TRUE;
            return ret;
        }
        i++;
        ret = RDB_obj_equals(obj1p, obj2p, resp);
    } while (ret == RDB_OK && *resp);
    return ret;
}
