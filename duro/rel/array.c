/* $Id$ */

#include "rdb.h"
#include "internal.h"

void
RDB_init_array(RDB_array *arrp) {
    arrp->tbp = NULL;
}

int
RDB_destroy_array(RDB_array *arrp)
{
    if (arrp->tbp == NULL)
        return RDB_OK;
    
    if (arrp->qrp != NULL) {
        int ret = _RDB_drop_qresult(arrp->qrp, arrp->txp);

        if (RDB_is_syserr(ret))
            RDB_rollback(arrp->txp);
        return ret;
    }
    return RDB_OK;
}

int
RDB_table_to_array(RDB_table *tbp, RDB_array *arrp,
                   int seqitc, RDB_seq_item seqitv[],
                   RDB_transaction *txp)
{
    int ret;

    if (seqitc > 0)
        return RDB_NOT_SUPPORTED;

    ret = RDB_destroy_array(arrp);
    if (ret != RDB_OK)
        return ret;

    arrp->tbp = tbp;
    arrp->txp = txp;
    arrp->qrp = NULL;
    arrp->length = -1;
    
    return RDB_OK;
}    

int
RDB_array_get_tuple(RDB_array *arrp, RDB_int idx, RDB_tuple *tup)
{
    int ret;

    if (arrp->pos > idx && arrp->qrp != NULL) {
        ret = _RDB_drop_qresult(arrp->qrp, arrp->txp);
        arrp->qrp = NULL;
        if (ret != RDB_OK)
            return ret;
    }

    if (arrp->qrp == NULL) {
        ret = _RDB_table_qresult(arrp->tbp, &arrp->qrp, arrp->txp);
        if (ret != RDB_OK)
            return ret;
        arrp->pos = 0;
    }
    while (arrp->pos < idx) {
        ret = _RDB_next_tuple(arrp->qrp, tup, arrp->txp);
        if (ret != RDB_OK)
            return ret;
        ++arrp->pos;
    }

    ++arrp->pos;
    return _RDB_next_tuple(arrp->qrp, tup, arrp->txp);
}

int
RDB_array_length(RDB_array *arrp)
{
    int ret;

    if (arrp->length == -1) {    
        RDB_tuple tpl;

        RDB_init_tuple(&tpl);
        if (arrp->qrp == NULL) {
            ret = _RDB_table_qresult(arrp->tbp, &arrp->qrp, arrp->txp);
            if (ret != RDB_OK) {
                RDB_destroy_tuple(&tpl);            
                return ret;
            }
            arrp->pos = 0;
        }

        do {
            ret = _RDB_next_tuple(arrp->qrp, &tpl, arrp->txp);
            if (ret == RDB_OK) {
                arrp->pos++;
            }
        } while (ret == RDB_OK);
        RDB_destroy_tuple(&tpl);
        _RDB_drop_qresult(arrp->qrp, arrp->txp);
        arrp->qrp = NULL;
        if (ret != RDB_NOT_FOUND)
            return ret;
        arrp->length = arrp->pos;
    }
    return arrp->length;
}
