/*
 * $Id$
 *
 * Copyright (C) 2003-2005 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>

typedef struct RDB_rmlink {
    RDB_recmap *rmp;
    struct RDB_rmlink *nextp;
} RDB_rmlink;

typedef struct RDB_ixlink {
    RDB_index *ixp;
    struct RDB_ixlink *nextp;
} RDB_ixlink;

int
_RDB_begin_tx(RDB_exec_context *ecp, RDB_transaction *txp, RDB_environment *envp,
        RDB_transaction *parentp)
{
    DB_TXN *partxid = parentp != NULL ? parentp->txid : NULL;
    int ret;

    txp->parentp = parentp;
    txp->envp = envp;
    ret = envp->envp->txn_begin(envp->envp, partxid, &txp->txid, 0);
    if (ret != 0) {
        RDB_raise_system("too many transactions", ecp);
        return RDB_ERROR;
    }
    txp->delrmp = NULL;
    txp->delixp = NULL;
    return RDB_OK;
}

static void
cleanup_storage(RDB_transaction *txp)
{
    RDB_rmlink *rmlinkp, *hrmlinkp;
    RDB_ixlink *ixlinkp, *hixlinkp;

    /*
     * Delete lists
     */
    rmlinkp = txp->delrmp;
    while (rmlinkp != NULL) {
        hrmlinkp = rmlinkp->nextp;
        free(rmlinkp);
        rmlinkp = hrmlinkp;
    }
    txp->delrmp = NULL;

    ixlinkp = txp->delixp;
    while (ixlinkp != NULL) {
        hixlinkp = ixlinkp->nextp;
        free(ixlinkp);
        ixlinkp = hixlinkp;
    }
    txp->delixp = NULL;
}

static int
del_storage(RDB_transaction *txp)
{
    RDB_rmlink *rmlinkp;
    RDB_ixlink *ixlinkp;
    int ret = RDB_OK;

    for (ixlinkp = txp->delixp;
         (ixlinkp != NULL) && (ret == RDB_OK);
         ixlinkp = ixlinkp->nextp) {
        ret = RDB_delete_index(ixlinkp->ixp, txp->envp, NULL);
    }

    for (rmlinkp = txp->delrmp;
         (rmlinkp != NULL) && (ret == RDB_OK);
         rmlinkp = rmlinkp->nextp) {
        ret = RDB_delete_recmap(rmlinkp->rmp, NULL);
    }

    cleanup_storage(txp);
    
    return ret;
}

static int
close_storage(RDB_transaction *txp)
{
    RDB_rmlink *rmlinkp;
    RDB_ixlink *ixlinkp;
    int ret = RDB_OK;

    for (ixlinkp = txp->delixp;
         (ixlinkp != NULL) && (ret == RDB_OK);
         ixlinkp = ixlinkp->nextp) {
        ret = RDB_close_index(ixlinkp->ixp);
    }

    for (rmlinkp = txp->delrmp;
         (rmlinkp != NULL) && (ret == RDB_OK);
         rmlinkp = rmlinkp->nextp) {
        ret = RDB_close_recmap(rmlinkp->rmp);
    }

    cleanup_storage(txp);
    
    return ret;
}

/** @defgroup tx Transaction functions 
 * @{
 */

/**
 * RDB_begin_tx starts a transaction which interacts with the
database specified by <var>dbp</var>.

If <var>parentp</var> is not NULL, the new transaction is
a subtransaction of the transaction specified by <var>parentp</var>.

The execution of a parent transaction is suspended while a child
transaction executes. It is an error to perform an operation under
the control of a transaction which has a running child transaction.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

The call may fail for a @ref system-errors "system error".
 */
int
RDB_begin_tx(RDB_exec_context *ecp, RDB_transaction *txp, RDB_database *dbp,
        RDB_transaction *parentp)
{
    txp->dbp = dbp;
    return _RDB_begin_tx(ecp, txp, dbp->dbrootp->envp, parentp);
}

/**
 * RDB_commit commits the transaction pointed to by <var>txp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_commit(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (txp->txid == NULL) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    ret = txp->txid->commit(txp->txid, 0);
    if (ret != 0) {
        _RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }

    if (txp->parentp != NULL) {
        /* Move recmaps and indexes to parent tx */
        RDB_rmlink *rmlinkp;
        RDB_ixlink *ixlinkp;
        
        rmlinkp = txp->delrmp;
        if (rmlinkp != NULL) {
             /* Find last link */
             while (rmlinkp->nextp != NULL)
                 rmlinkp = rmlinkp->nextp;

             /* Concatenate lists */
             rmlinkp->nextp = txp->parentp->delrmp;
             txp->parentp->delrmp = txp->delrmp;
        }

        ixlinkp = txp->delixp;
        if (ixlinkp != NULL) {
             /* Find last link */
             while (ixlinkp->nextp != NULL)
                 ixlinkp = ixlinkp->nextp;

             /* Concatenate lists */
             ixlinkp->nextp = txp->parentp->delixp;
             txp->parentp->delixp = txp->delixp;
        }   
    } else {
        /* Delete recmaps and indexes scheduled for deletion */
        ret = del_storage(txp);
        if (ret != RDB_OK) {
            return ret;
        }
    }

    txp->txid = NULL;
    
    return RDB_OK;
}

/**
 * RDB_rollback terminates the transaction pointed to by <var>txp</var>
and rolls back all changes made by this transaction and its subtransactions.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_rollback(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (txp->txid == NULL) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    ret = txp->txid->abort(txp->txid);
    if (ret != 0) {
        _RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }

    /*
     * Delete recmap list
     */
    ret = close_storage(txp);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        ret = RDB_ERROR;
    }

    txp->txid = NULL;

    return ret;
}

int
RDB_rollback_all(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    do {
        ret = RDB_rollback(ecp, txp);
        if (ret != RDB_OK)
            return ret;
        txp = txp->parentp;
    } while (txp != NULL);

    return RDB_OK;
}

/**
 * RDB_tx_is_running returns if <var>txp</var> points to a running
transaction.

@returns

RDB_TRUE if the transaction is running, RDB_FALSE otherwise.
 */
RDB_bool
RDB_tx_is_running(RDB_transaction *txp)
{
    return (RDB_bool)(txp->txid != NULL);
}

/*@}*/

int
_RDB_del_recmap(RDB_transaction *txp, RDB_recmap *rmp, RDB_exec_context *ecp)
{
    RDB_rmlink *linkp = malloc(sizeof (RDB_rmlink));

    if (linkp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    linkp->rmp = rmp;
    linkp->nextp = txp->delrmp;
    txp->delrmp = linkp;
    
    return RDB_OK;
}

int
_RDB_del_index(RDB_transaction *txp, RDB_index *ixp, RDB_exec_context *ecp)
{
    RDB_ixlink *linkp = malloc(sizeof (RDB_ixlink));

    if (linkp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    linkp->ixp = ixp;
    linkp->nextp = txp->delixp;
    txp->delixp = linkp;
    
    return RDB_OK;
}
