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

    if (seqitc > 0) {
        /* Create sorter */
        ret = _RDB_sorter(tbp, &arrp->var.arr.qrp, txp, seqitc, seqitv);
        if (ret != RDB_OK)
            return ret;
        arrp->var.arr.pos = 0;
    }
    
    return RDB_OK;
}    

int
RDB_array_get(RDB_object *arrp, RDB_int idx, RDB_object **tplpp)
{
    int ret;

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

    /* Move forward until the right position is reached */
    while (arrp->var.arr.pos < idx) {
        ret = _RDB_next_tuple(arrp->var.arr.qrp, arrp->var.arr.tplp, arrp->var.arr.txp);
        if (ret != RDB_OK)
            return ret;
        ++arrp->var.arr.pos;
    }

    ++arrp->var.arr.pos;
    *tplpp = arrp->var.arr.tplp;
    return _RDB_next_tuple(arrp->var.arr.qrp, arrp->var.arr.tplp, arrp->var.arr.txp);
}

RDB_int
RDB_array_length(RDB_object *arrp)
{
    int ret;

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
