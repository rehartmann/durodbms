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
    int ret;

    txp->dbp = dbp;
    txp->parentp = parent;
    ret = dbp->dbrootp->envp->envp->txn_begin(dbp->dbrootp->envp->envp,
            partxid, &txp->txid, 0);
    if (ret != 0) {
        return ret;
    }
    return RDB_OK;
}

int
RDB_commit(RDB_transaction *txp)
{
    int ret;

    if (txp->txid == NULL)
        return RDB_INVALID_TRANSACTION;

    ret = txp->txid->commit(txp->txid, 0);
    txp->txid = NULL;
    
    return ret;
}   

int
RDB_rollback(RDB_transaction *txp)
{
    int ret;

    if (txp->txid == NULL)
        return RDB_INVALID_TRANSACTION;

    ret = txp->txid->abort(txp->txid);
    txp->txid = NULL;

    return ret;
}

RDB_bool
RDB_tx_is_running(RDB_transaction *txp)
{
    return (RDB_bool)(txp->txid != NULL);
}
