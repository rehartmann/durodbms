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
    txp->delrmp = NULL;
    return RDB_OK;
}

static int del_recmaps(RDB_transaction *txp)
{
    RDB_rmlink *linkp, *hlinkp;
    int ret = RDB_OK;

    for (linkp = txp->delrmp;
         (linkp != NULL) && (ret == RDB_OK);
         linkp = linkp->nextp) {
        ret = RDB_delete_recmap(linkp->rmp, txp->dbp->dbrootp->envp, NULL);
    }

    /* Delete list */
    linkp = txp->delrmp;
    while (linkp != NULL) {
        hlinkp = linkp->nextp;
        free(linkp);
        linkp = hlinkp;
    }

    txp->delrmp = NULL;
    
    return ret;
}

int
RDB_commit(RDB_transaction *txp)
{
    int ret;

    if (txp->txid == NULL)
        return RDB_INVALID_TRANSACTION;

    /* Delete recmaps scheduled for deletion */
    ret = del_recmaps(txp);
    if (ret != RDB_OK)
        return ret;

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

    /*
     * Delete recmaps scheduled for deletion after abort,
     * because if they were deleted before the abort the abort would fail.
     */
    ret = del_recmaps(txp);
    if (ret != RDB_OK)
        return ret;

    txp->txid = NULL;

    return ret;
}

RDB_bool
RDB_tx_is_running(RDB_transaction *txp)
{
    return (RDB_bool)(txp->txid != NULL);
}

int
_RDB_del_recmap(RDB_transaction *txp, RDB_recmap *rmp)
{
    RDB_rmlink *linkp = malloc(sizeof (RDB_rmlink));

    if (linkp == NULL)
        return RDB_NO_MEMORY;
    linkp->rmp = rmp;
    linkp->nextp = txp->delrmp;
    txp->delrmp = linkp;
    
    return RDB_OK;
}
