/* $Id$ */

#include "rdb.h"
#include "internal.h"

void
RDB_init_array(RDB_array *arrp) {
    arrp->tbp = NULL;
}

void
RDB_destroy_array(RDB_array *arrp)
{
    /* Nothing to do, because the qresult is dropped by
     * RDB_tx_commit()/RDB_tx_abort() */
}

int
RDB_table_to_array(RDB_table *tbp, RDB_array *arrp,
                   int seqitc, RDB_seq_item seqitv[],
                   RDB_transaction *txp)
{
    if (seqitc > 0)
        return RDB_NOT_SUPPORTED;

    RDB_destroy_array(arrp);

    arrp->tbp = tbp;
    arrp->txp = txp;
    arrp->qrp = NULL;
    arrp->pos = -1;
    arrp->length = -1;
    
    return RDB_OK;
}    

static int
add_qresult(RDB_array *arrp)
{
    int res;

    res = _RDB_table_qresult(arrp->tbp, &arrp->qrp, arrp->txp);
    if (res != RDB_OK)
       return res;

    /* insert qresult into list */
    arrp->qrp->nextp = arrp->txp->first_qrp;
    arrp->txp->first_qrp = arrp->qrp;
    
    return RDB_OK;
}

int
RDB_array_get_tuple(RDB_array *arrp, int idx, RDB_tuple *tup)
{
    int res;

    if (arrp->pos > idx) {
        arrp->pos = -1;
    }

    if (arrp->pos == -1) {
        res = add_qresult(arrp);
        if (res != RDB_OK)
            return res;
        arrp->pos = 0;
    }
    while (arrp->pos < idx) {
        res = _RDB_next_tuple(arrp->qrp, tup);
        if (res != RDB_OK)
            return res;
        ++arrp->pos;
    }

    ++arrp->pos;
    return _RDB_next_tuple(arrp->qrp, tup);
}

int
RDB_array_length(RDB_array *arrp)
{
    int res;

    if (arrp->length == -1) {    
        RDB_tuple tpl;

        RDB_init_tuple(&tpl);
        if (arrp->pos == -1) {
            res = add_qresult(arrp);
            if (res != RDB_OK) {
                RDB_destroy_tuple(&tpl);            
                return res;
            }
            arrp->pos = 0;
        }

        do {
            res = _RDB_next_tuple(arrp->qrp, &tpl);
            if (res == RDB_OK) {
                arrp->pos++;
            }
        } while (res == RDB_OK);
        RDB_destroy_tuple(&tpl);
        if (res != RDB_NOT_FOUND)
            return res;
        arrp->length = arrp->pos;
    }
    return arrp->length;
}
