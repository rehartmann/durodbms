/*
 * Transaction functions.
 *
 * Copyright (C) 2003-2009, 2012-2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <rec/env.h>
#include <rec/index.h>
#include <gen/strfns.h>

#include <errno.h>
#include <stdio.h>

typedef struct RDB_rmlink {
    RDB_recmap *rmp;
    struct RDB_rmlink *nextp;
} RDB_rmlink;

typedef struct RDB_ixlink {
    RDB_index *ixp;
    struct RDB_ixlink *nextp;
} RDB_ixlink;

int
RDB_begin_tx_env(RDB_exec_context *ecp, RDB_transaction *txp, RDB_environment *envp,
        RDB_transaction *parentp)
{
    RDB_rec_transaction *partxid = parentp != NULL ? parentp->tx : NULL;

    if (parentp != NULL && partxid == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }
    txp->parentp = parentp;
    txp->envp = envp;
    txp->tx = RDB_begin_rec_tx(envp, parentp != NULL ? parentp->tx : NULL, ecp);
    if (txp->tx == NULL) {
        return RDB_ERROR;
    }
    if (RDB_env_trace(envp) > 0) {
        fprintf(stderr, "Transaction started, ID=%x\n",
                (unsigned) RDB_rec_tx_id(txp->tx, envp));
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
        RDB_free(rmlinkp);
        rmlinkp = hrmlinkp;
    }
    txp->delrmp = NULL;

    ixlinkp = txp->delixp;
    while (ixlinkp != NULL) {
        hixlinkp = ixlinkp->nextp;
        RDB_free(ixlinkp);
        ixlinkp = hixlinkp;
    }
    txp->delixp = NULL;
}

static int
del_storage(RDB_transaction *txp, RDB_exec_context *ecp)
{
    RDB_rmlink *rmlinkp;
    RDB_ixlink *ixlinkp;
    int ret = RDB_OK;

    for (ixlinkp = txp->delixp;
        (ixlinkp != NULL) && (ret == RDB_OK);
        ixlinkp = ixlinkp->nextp) {
        ret = RDB_delete_index(ixlinkp->ixp, NULL, ecp);
        /* If the index was not found, ignore it */
        if (ret != RDB_OK
                && RDB_obj_type(RDB_get_err(ecp)) == &RDB_RESOURCE_NOT_FOUND_ERROR) {
            ret = RDB_OK;
        }
    }

    for (rmlinkp = txp->delrmp;
         (rmlinkp != NULL) && (ret == RDB_OK);
         rmlinkp = rmlinkp->nextp) {
        /*
         * Cannot pass txp->tx because it has been closed by the caller -
         * BDB transaction handles must be closed before DB handles are closed
         */
        ret = RDB_delete_recmap(rmlinkp->rmp, NULL, ecp);
    }

    cleanup_storage(txp);
    
    return ret;
}

/** @defgroup tx Transaction functions 
 * @{
 */

/**
 * Start a transaction which interacts with the
database *<var>dbp</var>.

If <var>parentp</var> is not NULL, the new transaction is
a subtransaction of the transaction specified by <var>parentp</var>.

The execution of a parent transaction is suspended while a child
transaction executes. It is an error to perform an operation under
the control of a transaction which has a running child transaction.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>not_supported_error
<dd><var>parentp</var> is not NULL and the underlying storage system
does not support nested transactions.
</dl>

The call may also fail for a @ref system-errors "system error".

 */
int
RDB_begin_tx(RDB_exec_context *ecp, RDB_transaction *txp, RDB_database *dbp,
        RDB_transaction *parentp)
{
    txp->dbp = dbp;
    return RDB_begin_tx_env(ecp, txp, dbp->dbrootp->envp, parentp);
}

/**
 * RDB_commit commits the transaction pointed to by <var>txp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_commit(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (txp->tx == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    ret = RDB_commit_rec_tx(txp->tx, txp->envp, ecp);
    if (ret == RDB_ERROR) {
        RDB_handle_err(ecp, txp);
        return RDB_ERROR;
    }

    /* Delete recmaps and indexes scheduled for deletion */
    ret = del_storage(txp, ecp);
    if (ret != 0) {
        RDB_handle_err(ecp, NULL);
        return ret;
    }

    txp->tx = NULL;
    
    return RDB_OK;
}

/**
 * RDB_rollback terminates the transaction pointed to by <var>txp</var>
and rolls back all changes made by this transaction and its subtransactions.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_rollback(RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (txp->tx == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    ret = RDB_abort_rec_tx(txp->tx, txp->envp, ecp);
    if (ret != 0) {
        RDB_handle_err(ecp, txp);
        return RDB_ERROR;
    }

    txp->tx = NULL;

    cleanup_storage(txp);

    /*
     * Close all user tables because to enforce consistency
     * because creation or deletion of a recmap or index may have
     * been undone
     */
    if (txp->dbp != NULL) {
        RDB_set_user_tables_check(txp->dbp, ecp);
    }

    return RDB_OK;
}

/**
 * Aborts the transaction *<var>txp</var> and all parent transactions.
 */
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
    return (RDB_bool) (txp->tx != NULL);
}

/**
 * Return the database the transaction pointed
 * to by <var>txp</var> interacts with.

@returns

A pointer to the RDB_database structure that represents the database.
 */
RDB_database *
RDB_tx_db(RDB_transaction *txp)
{
    return txp->dbp;
}

/*@}*/

int
RDB_add_del_recmap(RDB_transaction *txp, RDB_recmap *rmp, RDB_exec_context *ecp)
{
    RDB_rmlink *linkp = RDB_alloc(sizeof (RDB_rmlink), ecp);

    if (linkp == NULL) {
        return RDB_ERROR;
    }
    linkp->rmp = rmp;
    linkp->nextp = txp->delrmp;
    txp->delrmp = linkp;

    return RDB_OK;
}

int
RDB_add_del_index(RDB_transaction *txp, RDB_index *ixp, RDB_exec_context *ecp)
{
    RDB_ixlink *linkp = RDB_alloc(sizeof (RDB_ixlink), ecp);

    if (linkp == NULL) {
        return RDB_ERROR;
    }
    linkp->ixp = ixp;
    linkp->nextp = txp->delixp;
    txp->delixp = linkp;
    
    return RDB_OK;
}

