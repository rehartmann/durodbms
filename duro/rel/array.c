/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"

int
RDB_table_to_array(RDB_object *arrp, RDB_table *tbp,
                   int seqitc, RDB_seq_item seqitv[],
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

    if (seqitc > 0) {
        /* Create sorter */
        ret = _RDB_sorter(tbp, &arrp->var.arr.qrp, txp, seqitc, seqitv);
        if (ret != RDB_OK)
            return ret;
        arrp->var.arr.pos = 0;
    }
    
    return RDB_OK;
}    

enum {
    ARRAY_BUFLEN_MIN = 256,
    ARRAY_BUFLEN_MAX = 32768
};

/* !! elemc bzw. capacity kann entfallen (?) */

int
RDB_array_get(RDB_object *arrp, RDB_int idx, RDB_object **tplpp)
{
    int ret;

    if (arrp->var.arr.elemv != NULL && idx < arrp->var.arr.elemc) {
        *tplpp = &arrp->var.arr.elemv[idx];
        return RDB_OK;
    }

    if (arrp->var.arr.tbp == NULL) {
        /* lement is not in buffer and no table available */
        return RDB_NOT_FOUND;
    }
    
    /* Reset qresult to start */
    if (arrp->var.arr.pos > idx && arrp->var.arr.qrp != NULL) {
        ret = _RDB_reset_qresult(arrp->var.arr.qrp, arrp->var.arr.txp);
        arrp->var.arr.pos = 0;
        if (ret != RDB_OK)
            return ret;
    }

    /* If there is no qresult, create it */
    if (arrp->var.arr.qrp == NULL) {
        ret = _RDB_table_qresult(arrp->var.arr.tbp, arrp->var.arr.txp, &arrp->var.arr.qrp);
        if (ret != RDB_OK) {
            arrp->var.arr.qrp = NULL;
            return ret;
        }
        arrp->var.arr.pos = 0;
    }

    if (arrp->var.arr.tplp == NULL) {
        arrp->var.arr.tplp = malloc(sizeof (RDB_object));
        if (arrp->var.arr.tplp == NULL)
            return RDB_NO_MEMORY;
        RDB_init_obj(arrp->var.arr.tplp);
    }

    if (arrp->var.arr.elemv == NULL) {
        int i;

        /*
         * Allocate buffer
         */
        
        arrp->var.arr.capacity = ARRAY_BUFLEN_MIN;
        if (idx > arrp->var.arr.capacity)
            arrp->var.arr.capacity = idx + 1;
        if (arrp->var.arr.capacity > ARRAY_BUFLEN_MAX)
            arrp->var.arr.capacity = ARRAY_BUFLEN_MAX;
        arrp->var.arr.elemv = malloc (sizeof(RDB_object) * arrp->var.arr.capacity);
        if (arrp->var.arr.elemv == NULL)
            return RDB_NO_MEMORY;

        arrp->var.arr.elemc = idx;
        if (arrp->var.arr.elemc > arrp->var.arr.capacity)
            arrp->var.arr.elemc = arrp->var.arr.capacity;

        for (i = 0; i < arrp->var.arr.capacity; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);
    }

    /* Enlarge buffer is necessary and permtted */
    if (idx >= arrp->var.arr.capacity && idx < ARRAY_BUFLEN_MAX) {
         int oldcapacity = arrp->var.arr.capacity;
         int capacity = oldcapacity;
         int i;

         do {
             capacity *= 2;
         } while (capacity < idx);

         arrp->var.arr.elemv = realloc (arrp->var.arr.elemv,
                 sizeof(RDB_object) * capacity);
         if (arrp->var.arr.elemv == NULL)
             return RDB_NO_MEMORY;
         for (i = oldcapacity; i < capacity; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);
         arrp->var.arr.capacity = capacity;
    }
         
    /*
     * Move forward until the right position is reached
     */
    while (arrp->var.arr.pos < idx) {
        if (arrp->var.arr.pos < arrp->var.arr.capacity)
            ret = _RDB_next_tuple(arrp->var.arr.qrp,
                    &arrp->var.arr.elemv[arrp->var.arr.pos],
                    arrp->var.arr.txp);
        else
            ret = _RDB_next_tuple(arrp->var.arr.qrp, arrp->var.arr.tplp,
                    arrp->var.arr.txp);
        if (ret != RDB_OK)
            return ret;
        ++arrp->var.arr.pos;
    }

    if (arrp->var.arr.pos < arrp->var.arr.capacity) {
        *tplpp = &arrp->var.arr.elemv[arrp->var.arr.pos];
        arrp->var.arr.elemc = arrp->var.arr.pos + 1;
    } else {
        *tplpp = arrp->var.arr.tplp;
        arrp->var.arr.elemc = arrp->var.arr.capacity;
    }

    /* Read next element */
    if (arrp->var.arr.pos < arrp->var.arr.capacity)
        ret = _RDB_next_tuple(arrp->var.arr.qrp,
                &arrp->var.arr.elemv[arrp->var.arr.pos], arrp->var.arr.txp);
    else
        ret = _RDB_next_tuple(arrp->var.arr.qrp, arrp->var.arr.tplp, arrp->var.arr.txp);
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
        RDB_object tpl;

        RDB_init_obj(&tpl);
        if (arrp->var.arr.qrp == NULL) {
            ret = _RDB_table_qresult(arrp->var.arr.tbp, arrp->var.arr.txp, &arrp->var.arr.qrp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);            
                return ret;
            }
            arrp->var.arr.pos = 0;
        }

        do {
            ret = _RDB_next_tuple(arrp->var.arr.qrp, &tpl, arrp->var.arr.txp);
            if (ret == RDB_OK) {
                arrp->var.arr.pos++;
            }
        } while (ret == RDB_OK);
        RDB_destroy_obj(&tpl);
        arrp->var.arr.length = arrp->var.arr.pos;
    }
    return arrp->var.arr.length;
}

int
RDB_set_array_length(RDB_object *arrp, RDB_int len)
{
    int i;

    if (arrp->kind == RDB_OB_INITIAL) {
        arrp->kind = RDB_OB_ARRAY;

        arrp->var.arr.elemv = malloc(sizeof(RDB_object) * len);
        if (arrp->var.arr.elemv == NULL)
            return RDB_NO_MEMORY;
        for (i = 0; i < len; i++)
            RDB_init_obj(&arrp->var.arr.elemv[i]);

        arrp->var.arr.tbp = NULL;
        arrp->var.arr.length = arrp->var.arr.capacity =
                arrp->var.arr.elemc = len;
        
        return RDB_OK;
    }
    return RDB_NOT_SUPPORTED;
}

int
RDB_array_set(RDB_object *arrp, RDB_int idx, const RDB_object *objp)
{
    if (arrp->var.arr.tbp != NULL)
        return RDB_NOT_SUPPORTED;

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

RDB_bool
_RDB_array_equals(RDB_object *arr1p, RDB_object *arr2p)
{
    int ret, ret2;
    RDB_object *obj1p, *obj2p;
    int i = 0;

    do {
        ret = RDB_array_get(arr1p, (RDB_int) i, &obj1p);
        if (ret != RDB_OK && ret != RDB_NOT_FOUND)
            return RDB_FALSE;
        ret2 = RDB_array_get(arr2p, (RDB_int) i, &obj2p);
        if (ret2 != RDB_OK && ret2 != RDB_NOT_FOUND)
            return RDB_FALSE;
        if (ret != ret2)
            return RDB_FALSE;
        if (ret == RDB_NOT_FOUND)
            return RDB_TRUE;
        i++;
    } while (RDB_obj_equals(obj1p, obj2p));
    return RDB_FALSE;
}
