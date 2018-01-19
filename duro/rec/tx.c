/*
 * Internal transaction
 *
 * Copyright (C) 2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "tx.h"
#include "envimpl.h"

#include <db.h>

int
RDB_begin_rec_tx(RDB_rec_transaction **rtxpp, RDB_environment *envp,
        RDB_rec_transaction *parent_rtxp)
{
    return ((DB_ENV *)envp->envp)->txn_begin(envp->envp, (DB_TXN *) parent_rtxp,
            (DB_TXN **) rtxpp, 0);
}

int
RDB_commit_rec_tx(RDB_rec_transaction *rtxp)
{
    return ((DB_TXN *) rtxp)->commit((DB_TXN *) rtxp, 0);
}

int
RDB_abort_rec_tx(RDB_rec_transaction *rtxp)
{
    return ((DB_TXN *) rtxp)->abort((DB_TXN *) rtxp);
}

int
RDB_rec_tx_id(RDB_rec_transaction *rtxp) {
    return ((DB_TXN *) rtxp)->id((DB_TXN *) rtxp);
}
