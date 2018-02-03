/*
 * Internal transaction
 *
 * Copyright (C) 2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "tx.h"
#include "envimpl.h"

RDB_rec_transaction *
RDB_begin_rec_tx(RDB_environment *envp,
        RDB_rec_transaction *parent_rtxp, RDB_exec_context *ecp)
{
    return (*envp->begin_tx_fn)(envp, parent_rtxp, ecp);
}

int
RDB_commit_rec_tx(RDB_rec_transaction *rtxp, RDB_environment *envp,
        RDB_exec_context *ecp)
{
    return (*envp->commit_fn)(rtxp, ecp);
}

int
RDB_abort_rec_tx(RDB_rec_transaction *rtxp, RDB_environment *envp,
        RDB_exec_context *ecp)
{
    return (*envp->abort_fn)(rtxp, ecp);
}

int
RDB_rec_tx_id(RDB_rec_transaction *rtxp, RDB_environment *envp)
{
    return (*envp->tx_id_fn)(rtxp);
}
