/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <gen/strfns.h>

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
    return RDB_OK;
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
<dt>RDB_NO_RUNNING_TX_ERROR
<dd><var>txp</var> does not point to a running transaction.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_commit(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (txp->txid == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    ret = txp->txid->commit(txp->txid, 0);
    if (ret != 0) {
        _RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
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
<dt>RDB_NO_RUNNING_TX_ERROR
<dd><var>txp</var> does not point to a running transaction.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_rollback(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (txp->txid == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    ret = txp->txid->abort(txp->txid);
    if (ret != 0) {
        _RDB_handle_errcode(ret, ecp, txp);
        return RDB_ERROR;
    }

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
