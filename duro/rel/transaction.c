/* $Id$ */

#include "rdb.h"
#include "internal.h"

typedef struct RDB_rmlink {
    RDB_recmap *rmp;
    struct RDB_rmlink *nextp;
} RDB_rmlink;

int
RDB_begin_tx(RDB_transaction *txp, RDB_database *dbp,
        RDB_transaction *parent)
{
    DB_TXN *partxid = parent != NULL ? parent->txid : NULL;
    int res;

    txp->dbp = dbp;
    txp->parentp = parent;
    txp->first_qrp = NULL;
    res = dbp->envp->envp->txn_begin(dbp->envp->envp, partxid, &txp->txid, 0);
    if (res != 0) {
        return res;
    }
    return RDB_OK;
}

static int
drop_qresults(RDB_transaction *txp) {
    RDB_qresult *qrp;
    RDB_qresult *nextqrp;
    int res = RDB_OK;
    int hres;

    for (qrp = txp->first_qrp; qrp != NULL; qrp = nextqrp) {
        nextqrp = qrp->nextp;
        hres = _RDB_drop_qresult(qrp);
        if (hres != RDB_OK)
            res = hres;
    }
    return res;
}

int
RDB_commit(RDB_transaction *txp)
{
    int res;

    if (txp->txid == NULL)
        return RDB_INVALID_TRANSACTION;

    /* Drop qresults */
    res = drop_qresults(txp);
    if (res != RDB_OK) {
        if (txp->txid != NULL)
            txp->txid->abort(txp->txid);
        return res;
    }
        
    res = txp->txid->commit(txp->txid, 0);
    txp->txid = NULL;
    
    return res;
}   

int
RDB_rollback(RDB_transaction *txp)
{
    int res;

    if (txp->txid == NULL)
        return RDB_INVALID_TRANSACTION;

    /* Drop qresults */
    res = drop_qresults(txp);
    if (res != RDB_OK) {
        if (txp->txid != NULL)
            txp->txid->abort(txp->txid);
        return res;
    }

    res = txp->txid->abort(txp->txid);
    txp->txid = NULL;

    return res;
}
